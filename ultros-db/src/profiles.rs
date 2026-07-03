use crate::UltrosDb;
use crate::entity::*;
use anyhow::Result;
use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionTrait, TryIntoModel,
};

fn default_arbitrage_settings(profile_id: i32) -> profile_arbitrage_settings::ActiveModel {
    profile_arbitrage_settings::ActiveModel {
        profile_id: Set(profile_id),
        min_net_profit: Set(0),
        velocity_threshold: Set(0.0),
        travel_cost_rate_per_min: Set(0),
        min_profit_total: Set(0),
        category_blocklist: Set(None),
        category_allowlist: Set(None),
        world_exclusion_list: Set(None),
        excluded_item_ids: Set(None),
        max_listing_age_hours: Set(4),
        show_stale_panel: Set(false),
        require_home_world_sell_target: Set(true),
        source_world_scope: Set("SAME_DC".to_string()),
        max_price_jump_ratio: Set(1.30),
        min_recent_cluster_confirmations: Set(5),
        volatility_action: Set("DEMOTE_TO_REVIEW".to_string()),
        require_ask_confirmation: Set(true),
        max_ask_vs_sale_gap_percent: Set(15.0),
        preset_name: Set("BALANCED".to_string()),
        destination_world_scope: Set("HOME_WORLD".to_string()),
        seller_world_ids: Set(None),
        weekly_velocity_threshold: Set(0.0),
        same_dc_travel_minutes: Set(2),
        cross_dc_travel_minutes: Set(8),
        reference_price_scope: Set("DESTINATION_DC".to_string()),
        sell_price_strategy: Set("LOWER_OF_ASK_AND_MEDIAN".to_string()),
        min_markdown_pct: Set(0.0),
        digest_format: Set("CARDS".to_string()),
        digest_changed_only: Set(true),
        digest_max_clean: Set(8),
        digest_max_review: Set(4),
        digest_include_review: Set(true),
        digest_include_universalis_links: Set(true),
        digest_include_ultros_links: Set(true),
        table_grouping_strategy: Set("BEST_PLUS_SAME_DC".to_string()),
        table_max_rows_per_item: Set(2),
        table_include_same_dc_best: Set(true),
        table_show_theoretical: Set(false),
        alert_grouping_strategy: Set("BEST_PLUS_SAME_DC".to_string()),
        alert_max_rows_per_item: Set(2),
        alert_include_same_dc_best: Set(true),
        alert_show_theoretical: Set(false),
        alert_profit_improvement_threshold_gil: Set(1),
        alert_profit_improvement_threshold_pct: Set(0.0),
        alert_frequency_mode: Set("DIGEST_INTERVAL".to_string()),
        alert_digest_interval_minutes: Set(60),
        alert_schedule_cron: Set(None),
        alert_send_empty_digest: Set(false),
        alert_immediate_threshold_enabled: Set(true),
        alert_immediate_min_net_profit: Set(500_000),
        alert_immediate_min_markdown_pct: Set(0.0),
        alert_immediate_min_velocity: Set(0.0),
        alert_immediate_max_per_hour: Set(3),
    }
}

impl UltrosDb {
    pub async fn get_profiles_for_user(
        &self,
        discord_user_id: u64,
    ) -> Result<Vec<player_profile::Model>> {
        let profiles = player_profile::Entity::find()
            .filter(player_profile::Column::DiscordUserId.eq(discord_user_id as i64))
            .all(&self.db)
            .await?;
        Ok(profiles)
    }

    pub async fn get_profile_by_id(
        &self,
        profile_id: i32,
    ) -> Result<Option<player_profile::Model>> {
        let profile = player_profile::Entity::find_by_id(profile_id)
            .one(&self.db)
            .await?;
        Ok(profile)
    }

    pub async fn create_profile(
        &self,
        discord_user_id: u64,
        display_name: String,
    ) -> Result<player_profile::Model> {
        let now = Utc::now().naive_utc();

        // Start a transaction so all default tables are created atomically
        let txn = self.db.begin().await?;

        let profile = player_profile::ActiveModel {
            id: ActiveValue::NotSet,
            discord_user_id: Set(discord_user_id as i64),
            display_name: Set(display_name),
            home_world_id: Set(None),
            active_datacenter_id: Set(None),
            grand_company: Set(None),
            gil_balance: Set(0),
            alert_channel_webhook: Set(None),
            alert_channel_dm: Set(false),
            alert_item_cooldown_minutes: Set(30),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&txn)
        .await?;

        // Initialize default settings for this profile
        default_arbitrage_settings(profile.id).insert(&txn).await?;

        profile_crafting_settings::ActiveModel {
            profile_id: Set(profile.id),
            min_net_profit: Set(0),
            hq_only: Set(false),
        }
        .insert(&txn)
        .await?;

        profile_gathering_settings::ActiveModel {
            profile_id: Set(profile.id),
            show_all_levels: Set(false),
            class_filter: Set(None),
            min_unit_price: Set(None),
        }
        .insert(&txn)
        .await?;

        txn.commit().await?;
        Ok(profile)
    }

    pub async fn update_profile(
        &self,
        profile_id: i32,
        mut active_model: player_profile::ActiveModel,
    ) -> Result<player_profile::Model> {
        active_model.id = Set(profile_id);
        active_model.updated_at = Set(Utc::now().naive_utc());
        let updated = active_model.update(&self.db).await?;
        Ok(updated)
    }

    pub async fn delete_profile(&self, profile_id: i32) -> Result<()> {
        player_profile::Entity::delete_by_id(profile_id)
            .exec(&self.db)
            .await?;
        Ok(())
    }

    // --- Job Levels ---
    pub async fn get_job_levels(&self, profile_id: i32) -> Result<Vec<profile_job_level::Model>> {
        let levels = profile_job_level::Entity::find()
            .filter(profile_job_level::Column::ProfileId.eq(profile_id))
            .all(&self.db)
            .await?;
        Ok(levels)
    }

    pub async fn save_job_levels(
        &self,
        profile_id: i32,
        levels: Vec<profile_job_level::Model>,
    ) -> Result<()> {
        let txn = self.db.begin().await?;

        // Remove existing job levels
        profile_job_level::Entity::delete_many()
            .filter(profile_job_level::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;

        if !levels.is_empty() {
            let active_models: Vec<profile_job_level::ActiveModel> = levels
                .into_iter()
                .map(|l| profile_job_level::ActiveModel {
                    profile_id: Set(profile_id),
                    job_id: Set(l.job_id),
                    level: Set(l.level),
                    kind: Set(l.kind),
                })
                .collect();

            profile_job_level::Entity::insert_many(active_models)
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }

    // --- Arbitrage Settings ---
    pub async fn get_arbitrage_settings(
        &self,
        profile_id: i32,
    ) -> Result<profile_arbitrage_settings::Model> {
        let settings = profile_arbitrage_settings::Entity::find_by_id(profile_id)
            .one(&self.db)
            .await?;

        if let Some(s) = settings {
            Ok(s)
        } else {
            // Fallback just in case setting record doesn't exist
            let new_settings = default_arbitrage_settings(profile_id)
                .insert(&self.db)
                .await?;
            Ok(new_settings)
        }
    }

    pub async fn update_arbitrage_settings(
        &self,
        profile_id: i32,
        mut active_model: profile_arbitrage_settings::ActiveModel,
    ) -> Result<profile_arbitrage_settings::Model> {
        active_model.profile_id = Set(profile_id);
        let updated = active_model.save(&self.db).await?;
        Ok(updated.try_into_model()?)
    }

    pub async fn get_arbitrage_destination_endpoint_ids(
        &self,
        profile_id: i32,
    ) -> Result<Vec<i32>> {
        Ok(arbitrage_notification_destination::Entity::find()
            .filter(arbitrage_notification_destination::Column::ProfileId.eq(profile_id))
            .order_by_asc(arbitrage_notification_destination::Column::EndpointId)
            .all(&self.db)
            .await?
            .into_iter()
            .map(|row| row.endpoint_id)
            .collect())
    }

    pub async fn set_arbitrage_destination_endpoint_ids(
        &self,
        profile_id: i32,
        endpoint_ids: &[i32],
    ) -> Result<()> {
        let txn = self.db.begin().await?;
        arbitrage_notification_destination::Entity::delete_many()
            .filter(arbitrage_notification_destination::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;

        let active_models: Vec<arbitrage_notification_destination::ActiveModel> = endpoint_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .map(
                |endpoint_id| arbitrage_notification_destination::ActiveModel {
                    profile_id: Set(profile_id),
                    endpoint_id: Set(endpoint_id),
                },
            )
            .collect();

        if !active_models.is_empty() {
            arbitrage_notification_destination::Entity::insert_many(active_models)
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }

    pub async fn list_arbitrage_delivery_endpoints(
        &self,
        profile_id: i32,
        owner: i64,
    ) -> Result<Vec<notification_endpoint::Model>> {
        let endpoint_ids = self
            .get_arbitrage_destination_endpoint_ids(profile_id)
            .await?;

        if endpoint_ids.is_empty() {
            return self.list_endpoints(owner).await;
        }

        Ok(notification_endpoint::Entity::find()
            .filter(notification_endpoint::Column::UserId.eq(owner))
            .filter(notification_endpoint::Column::Id.is_in(endpoint_ids))
            .order_by_asc(notification_endpoint::Column::Id)
            .all(&self.db)
            .await?)
    }

    pub async fn get_arbitrage_item_alert_states(
        &self,
        profile_id: i32,
    ) -> Result<Vec<arbitrage_item_alert_state::Model>> {
        Ok(arbitrage_item_alert_state::Entity::find()
            .filter(arbitrage_item_alert_state::Column::ProfileId.eq(profile_id))
            .all(&self.db)
            .await?)
    }

    pub async fn upsert_arbitrage_item_alert_states(
        &self,
        states: Vec<arbitrage_item_alert_state::Model>,
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let active_models: Vec<arbitrage_item_alert_state::ActiveModel> = states
            .into_iter()
            .map(|state| arbitrage_item_alert_state::ActiveModel {
                id: ActiveValue::NotSet,
                profile_id: Set(state.profile_id),
                item_id: Set(state.item_id),
                hq: Set(state.hq),
                best_alerted_net_profit: Set(state.best_alerted_net_profit),
                best_alerted_snapshot_hash: Set(state.best_alerted_snapshot_hash),
                last_alerted_at: Set(state.last_alerted_at),
            })
            .collect();

        arbitrage_item_alert_state::Entity::insert_many(active_models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    arbitrage_item_alert_state::Column::ProfileId,
                    arbitrage_item_alert_state::Column::ItemId,
                    arbitrage_item_alert_state::Column::Hq,
                ])
                .update_columns([
                    arbitrage_item_alert_state::Column::BestAlertedNetProfit,
                    arbitrage_item_alert_state::Column::BestAlertedSnapshotHash,
                    arbitrage_item_alert_state::Column::LastAlertedAt,
                ])
                .to_owned(),
            )
            .exec(&self.db)
            .await?;

        Ok(())
    }

    pub async fn get_arbitrage_schedule_state(
        &self,
        profile_id: i32,
    ) -> Result<arbitrage_alert_schedule_state::Model> {
        if let Some(state) = arbitrage_alert_schedule_state::Entity::find_by_id(profile_id)
            .one(&self.db)
            .await?
        {
            return Ok(state);
        }

        Ok(arbitrage_alert_schedule_state::ActiveModel {
            profile_id: Set(profile_id),
            last_digest_sent_at: Set(None),
            last_immediate_sent_at: Set(None),
            immediate_sent_count_window_start: Set(None),
            immediate_sent_count: Set(0),
        }
        .insert(&self.db)
        .await?)
    }

    pub async fn save_arbitrage_schedule_state(
        &self,
        state: arbitrage_alert_schedule_state::Model,
    ) -> Result<()> {
        arbitrage_alert_schedule_state::Entity::insert(
            arbitrage_alert_schedule_state::ActiveModel {
                profile_id: Set(state.profile_id),
                last_digest_sent_at: Set(state.last_digest_sent_at),
                last_immediate_sent_at: Set(state.last_immediate_sent_at),
                immediate_sent_count_window_start: Set(state.immediate_sent_count_window_start),
                immediate_sent_count: Set(state.immediate_sent_count),
            },
        )
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(
                arbitrage_alert_schedule_state::Column::ProfileId,
            )
            .update_columns([
                arbitrage_alert_schedule_state::Column::LastDigestSentAt,
                arbitrage_alert_schedule_state::Column::LastImmediateSentAt,
                arbitrage_alert_schedule_state::Column::ImmediateSentCountWindowStart,
                arbitrage_alert_schedule_state::Column::ImmediateSentCount,
            ])
            .to_owned(),
        )
        .exec(&self.db)
        .await?;
        Ok(())
    }

    pub async fn insert_arbitrage_delivery_attempt(
        &self,
        attempt: arbitrage_delivery_attempt::Model,
    ) -> Result<()> {
        arbitrage_delivery_attempt::Entity::insert(arbitrage_delivery_attempt::ActiveModel {
            id: ActiveValue::NotSet,
            profile_id: Set(attempt.profile_id),
            endpoint_id: Set(attempt.endpoint_id),
            delivery_kind: Set(attempt.delivery_kind),
            snapshot_batch_hash: Set(attempt.snapshot_batch_hash),
            success: Set(attempt.success),
            error_message: Set(attempt.error_message),
            attempted_at: Set(attempt.attempted_at),
        })
        .exec(&self.db)
        .await?;
        Ok(())
    }

    pub async fn get_arbitrage_pending_digests(
        &self,
        profile_id: i32,
    ) -> Result<Vec<arbitrage_pending_digest::Model>> {
        Ok(arbitrage_pending_digest::Entity::find()
            .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
            .order_by_desc(arbitrage_pending_digest::Column::NetProfit)
            .all(&self.db)
            .await?)
    }

    pub async fn upsert_arbitrage_pending_digests(
        &self,
        pending: Vec<arbitrage_pending_digest::Model>,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin().await?;
        let item_keys = pending
            .iter()
            .map(|row| (row.profile_id, row.item_id, row.hq))
            .collect::<std::collections::HashSet<_>>();
        for (profile_id, item_id, hq) in item_keys {
            arbitrage_pending_digest::Entity::delete_many()
                .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
                .filter(arbitrage_pending_digest::Column::ItemId.eq(item_id))
                .filter(arbitrage_pending_digest::Column::Hq.eq(hq))
                .exec(&txn)
                .await?;
        }

        let active_models: Vec<arbitrage_pending_digest::ActiveModel> = pending
            .into_iter()
            .map(|row| arbitrage_pending_digest::ActiveModel {
                id: ActiveValue::NotSet,
                profile_id: Set(row.profile_id),
                item_id: Set(row.item_id),
                hq: Set(row.hq),
                source_world_id: Set(row.source_world_id),
                dest_world_id: Set(row.dest_world_id),
                snapshot_hash: Set(row.snapshot_hash),
                net_profit: Set(row.net_profit),
                section: Set(row.section),
                queued_at: Set(row.queued_at),
                updated_at: Set(row.updated_at),
            })
            .collect();

        arbitrage_pending_digest::Entity::insert_many(active_models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    arbitrage_pending_digest::Column::ProfileId,
                    arbitrage_pending_digest::Column::ItemId,
                    arbitrage_pending_digest::Column::Hq,
                    arbitrage_pending_digest::Column::SourceWorldId,
                    arbitrage_pending_digest::Column::DestWorldId,
                ])
                .update_columns([
                    arbitrage_pending_digest::Column::SnapshotHash,
                    arbitrage_pending_digest::Column::NetProfit,
                    arbitrage_pending_digest::Column::Section,
                    arbitrage_pending_digest::Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn delete_arbitrage_pending_digest_keys(
        &self,
        profile_id: i32,
        keys: &[(i32, bool, i32, i32)],
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin().await?;
        for (item_id, hq, source_world_id, dest_world_id) in keys {
            arbitrage_pending_digest::Entity::delete_many()
                .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
                .filter(arbitrage_pending_digest::Column::ItemId.eq(*item_id))
                .filter(arbitrage_pending_digest::Column::Hq.eq(*hq))
                .filter(arbitrage_pending_digest::Column::SourceWorldId.eq(*source_world_id))
                .filter(arbitrage_pending_digest::Column::DestWorldId.eq(*dest_world_id))
                .exec(&txn)
                .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub async fn delete_arbitrage_pending_digest_item_keys(
        &self,
        profile_id: i32,
        keys: &[(i32, bool)],
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin().await?;
        for (item_id, hq) in keys {
            arbitrage_pending_digest::Entity::delete_many()
                .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
                .filter(arbitrage_pending_digest::Column::ItemId.eq(*item_id))
                .filter(arbitrage_pending_digest::Column::Hq.eq(*hq))
                .exec(&txn)
                .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub async fn get_arbitrage_pending_digest_count(&self, profile_id: i32) -> Result<i64> {
        Ok(arbitrage_pending_digest::Entity::find()
            .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
            .count(&self.db)
            .await? as i64)
    }

    pub async fn get_recent_arbitrage_delivery_attempts(
        &self,
        profile_id: i32,
        limit: u64,
    ) -> Result<Vec<arbitrage_delivery_attempt::Model>> {
        Ok(arbitrage_delivery_attempt::Entity::find()
            .filter(arbitrage_delivery_attempt::Column::ProfileId.eq(profile_id))
            .order_by_desc(arbitrage_delivery_attempt::Column::AttemptedAt)
            .limit(limit)
            .all(&self.db)
            .await?)
    }

    pub async fn reset_arbitrage_delivery_state(&self, profile_id: i32) -> Result<()> {
        let txn = self.db.begin().await?;
        arbitrage_digest_state::Entity::delete_many()
            .filter(arbitrage_digest_state::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;
        arbitrage_item_alert_state::Entity::delete_many()
            .filter(arbitrage_item_alert_state::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;
        arbitrage_pending_digest::Entity::delete_many()
            .filter(arbitrage_pending_digest::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;
        arbitrage_alert_schedule_state::Entity::delete_by_id(profile_id)
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    // --- Crafting Settings ---
    pub async fn get_crafting_settings(
        &self,
        profile_id: i32,
    ) -> Result<(
        profile_crafting_settings::Model,
        Vec<profile_crafting_subcraft_threshold::Model>,
    )> {
        let settings = profile_crafting_settings::Entity::find_by_id(profile_id)
            .one(&self.db)
            .await?;

        let settings_model = if let Some(s) = settings {
            s
        } else {
            profile_crafting_settings::ActiveModel {
                profile_id: Set(profile_id),
                min_net_profit: Set(0),
                hq_only: Set(false),
            }
            .insert(&self.db)
            .await?
        };

        let thresholds = profile_crafting_subcraft_threshold::Entity::find()
            .filter(profile_crafting_subcraft_threshold::Column::ProfileId.eq(profile_id))
            .all(&self.db)
            .await?;

        Ok((settings_model, thresholds))
    }

    pub async fn update_crafting_settings(
        &self,
        profile_id: i32,
        mut settings_active: profile_crafting_settings::ActiveModel,
        thresholds: Vec<profile_crafting_subcraft_threshold::Model>,
    ) -> Result<(
        profile_crafting_settings::Model,
        Vec<profile_crafting_subcraft_threshold::Model>,
    )> {
        let txn = self.db.begin().await?;

        settings_active.profile_id = Set(profile_id);
        let updated_settings = settings_active.save(&txn).await?.try_into_model()?;

        // Replace thresholds
        profile_crafting_subcraft_threshold::Entity::delete_many()
            .filter(profile_crafting_subcraft_threshold::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;

        let mut inserted_thresholds = Vec::new();
        if !thresholds.is_empty() {
            let active_thresholds: Vec<profile_crafting_subcraft_threshold::ActiveModel> =
                thresholds
                    .into_iter()
                    .map(|t| profile_crafting_subcraft_threshold::ActiveModel {
                        profile_id: Set(profile_id),
                        crafting_class_id: Set(t.crafting_class_id),
                        savings_pct_threshold: Set(t.savings_pct_threshold),
                        savings_gil_threshold: Set(t.savings_gil_threshold),
                    })
                    .collect();

            profile_crafting_subcraft_threshold::Entity::insert_many(active_thresholds)
                .exec(&txn)
                .await?;

            inserted_thresholds = profile_crafting_subcraft_threshold::Entity::find()
                .filter(profile_crafting_subcraft_threshold::Column::ProfileId.eq(profile_id))
                .all(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok((updated_settings, inserted_thresholds))
    }

    // --- Gathering Settings ---
    pub async fn get_gathering_settings(
        &self,
        profile_id: i32,
    ) -> Result<profile_gathering_settings::Model> {
        let settings = profile_gathering_settings::Entity::find_by_id(profile_id)
            .one(&self.db)
            .await?;

        if let Some(s) = settings {
            Ok(s)
        } else {
            let new_settings = profile_gathering_settings::ActiveModel {
                profile_id: Set(profile_id),
                show_all_levels: Set(false),
                class_filter: Set(None),
                min_unit_price: Set(None),
            }
            .insert(&self.db)
            .await?;
            Ok(new_settings)
        }
    }

    pub async fn update_gathering_settings(
        &self,
        profile_id: i32,
        mut active_model: profile_gathering_settings::ActiveModel,
    ) -> Result<profile_gathering_settings::Model> {
        active_model.profile_id = Set(profile_id);
        let updated = active_model.save(&self.db).await?;
        Ok(updated.try_into_model()?)
    }

    // --- Tax Rate Cache ---
    pub async fn get_cached_tax_rate(
        &self,
        world_id: i32,
    ) -> Result<Option<tax_rate_cache::Model>> {
        let cache = tax_rate_cache::Entity::find_by_id(world_id)
            .one(&self.db)
            .await?;
        Ok(cache)
    }

    pub async fn upsert_tax_rate(
        &self,
        world_id: i32,
        tax_rate: f64,
    ) -> Result<tax_rate_cache::Model> {
        let now = Utc::now().naive_utc();
        let cache = tax_rate_cache::ActiveModel {
            world_id: Set(world_id),
            tax_rate: Set(tax_rate),
            fetched_at: Set(now),
        };

        tax_rate_cache::Entity::insert(cache)
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(tax_rate_cache::Column::WorldId)
                    .update_columns([
                        tax_rate_cache::Column::TaxRate,
                        tax_rate_cache::Column::FetchedAt,
                    ])
                    .to_owned(),
            )
            .exec(&self.db)
            .await?;

        Ok(tax_rate_cache::Model {
            world_id,
            tax_rate,
            fetched_at: now,
        })
    }

    // --- Arbitrage Opportunities ---
    pub async fn get_arbitrage_opportunities(
        &self,
        profile_id: i32,
    ) -> Result<Vec<arbitrage_opportunity::Model>> {
        let opportunities = arbitrage_opportunity::Entity::find()
            .filter(arbitrage_opportunity::Column::ProfileId.eq(profile_id))
            .order_by_desc(arbitrage_opportunity::Column::NetProfit)
            .all(&self.db)
            .await?;
        Ok(opportunities)
    }

    pub async fn save_arbitrage_opportunities(
        &self,
        profile_id: i32,
        opportunities: Vec<arbitrage_opportunity::Model>,
    ) -> Result<()> {
        let txn = self.db.begin().await?;

        // Remove existing cached arbitrage opportunities for this profile
        arbitrage_opportunity::Entity::delete_many()
            .filter(arbitrage_opportunity::Column::ProfileId.eq(profile_id))
            .exec(&txn)
            .await?;

        if !opportunities.is_empty() {
            let active_models: Vec<arbitrage_opportunity::ActiveModel> = opportunities
                .into_iter()
                .map(|opp| arbitrage_opportunity::ActiveModel {
                    id: ActiveValue::NotSet,
                    profile_id: Set(profile_id),
                    item_id: Set(opp.item_id),
                    hq: Set(opp.hq),
                    source_world_id: Set(opp.source_world_id),
                    dest_world_id: Set(opp.dest_world_id),
                    gross_profit: Set(opp.gross_profit),
                    net_profit: Set(opp.net_profit),
                    velocity_score: Set(opp.velocity_score),
                    weekly_avg_velocity: Set(opp.weekly_avg_velocity),
                    units_sold_48h: Set(opp.units_sold_48h),
                    units_sold_7d: Set(opp.units_sold_7d),
                    median_sale_price: Set(opp.median_sale_price),
                    latest_sale_timestamp: Set(opp.latest_sale_timestamp),
                    listing_age_seconds: Set(opp.listing_age_seconds),
                    total_cost: Set(opp.total_cost),
                    quantity_available: Set(opp.quantity_available),
                    over_budget: Set(opp.over_budget),
                    travel_tier: Set(opp.travel_tier),
                    volatility_flag: Set(opp.volatility_flag),
                    regime_recent_window_count: Set(opp.regime_recent_window_count),
                    recent_cluster_avg_price: Set(opp.recent_cluster_avg_price),
                    prior_cluster_avg_price: Set(opp.prior_cluster_avg_price),
                    price_jump_ratio: Set(opp.price_jump_ratio),
                    within_cluster_cv_recent: Set(opp.within_cluster_cv_recent),
                    within_cluster_cv_prior: Set(opp.within_cluster_cv_prior),
                    recent_cluster_sales_count: Set(opp.recent_cluster_sales_count),
                    prior_cluster_sales_count: Set(opp.prior_cluster_sales_count),
                    current_ask_cluster_avg: Set(opp.current_ask_cluster_avg),
                    ask_vs_recent_sale_gap_pct: Set(opp.ask_vs_recent_sale_gap_pct),
                    dest_low_ask_price: Set(opp.dest_low_ask_price),
                    selected_sell_reference_price: Set(opp.selected_sell_reference_price),
                    source_ask_avg: Set(opp.source_ask_avg),
                    dest_ask_avg: Set(opp.dest_ask_avg),
                    reference_min_price: Set(opp.reference_min_price),
                    reference_avg_price: Set(opp.reference_avg_price),
                    markdown_pct: Set(opp.markdown_pct),
                    execution_status: Set(opp.execution_status),
                    travel_minutes: Set(opp.travel_minutes),
                    computed_at: Set(opp.computed_at),
                })
                .collect();

            arbitrage_opportunity::Entity::insert_many(active_models)
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }

    pub async fn get_arbitrage_digest_states(
        &self,
        profile_id: i32,
    ) -> Result<Vec<arbitrage_digest_state::Model>> {
        let states = arbitrage_digest_state::Entity::find()
            .filter(arbitrage_digest_state::Column::ProfileId.eq(profile_id))
            .all(&self.db)
            .await?;
        Ok(states)
    }

    pub async fn upsert_arbitrage_digest_states(
        &self,
        states: Vec<arbitrage_digest_state::Model>,
    ) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let active_models: Vec<arbitrage_digest_state::ActiveModel> = states
            .into_iter()
            .map(|state| arbitrage_digest_state::ActiveModel {
                id: ActiveValue::NotSet,
                profile_id: Set(state.profile_id),
                item_id: Set(state.item_id),
                hq: Set(state.hq),
                source_world_id: Set(state.source_world_id),
                dest_world_id: Set(state.dest_world_id),
                snapshot_hash: Set(state.snapshot_hash),
                source_price: Set(state.source_price),
                dest_price: Set(state.dest_price),
                quantity_available: Set(state.quantity_available),
                net_profit: Set(state.net_profit),
                volatility_flag: Set(state.volatility_flag),
                latest_sale_timestamp: Set(state.latest_sale_timestamp),
                units_sold_48h: Set(state.units_sold_48h),
                units_sold_7d: Set(state.units_sold_7d),
                median_sale_price: Set(state.median_sale_price),
                recent_cluster_avg_price: Set(state.recent_cluster_avg_price),
                prior_cluster_avg_price: Set(state.prior_cluster_avg_price),
                weekly_avg_velocity: Set(state.weekly_avg_velocity),
                dest_low_ask_price: Set(state.dest_low_ask_price),
                selected_sell_reference_price: Set(state.selected_sell_reference_price),
                source_ask_avg: Set(state.source_ask_avg),
                dest_ask_avg: Set(state.dest_ask_avg),
                reference_min_price: Set(state.reference_min_price),
                reference_avg_price: Set(state.reference_avg_price),
                markdown_pct: Set(state.markdown_pct),
                execution_status: Set(state.execution_status),
                delivered_at: Set(state.delivered_at),
                created_at: Set(state.created_at),
                updated_at: Set(state.updated_at),
            })
            .collect();

        arbitrage_digest_state::Entity::insert_many(active_models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    arbitrage_digest_state::Column::ProfileId,
                    arbitrage_digest_state::Column::ItemId,
                    arbitrage_digest_state::Column::Hq,
                    arbitrage_digest_state::Column::SourceWorldId,
                    arbitrage_digest_state::Column::DestWorldId,
                ])
                .update_columns([
                    arbitrage_digest_state::Column::SnapshotHash,
                    arbitrage_digest_state::Column::SourcePrice,
                    arbitrage_digest_state::Column::DestPrice,
                    arbitrage_digest_state::Column::QuantityAvailable,
                    arbitrage_digest_state::Column::NetProfit,
                    arbitrage_digest_state::Column::VolatilityFlag,
                    arbitrage_digest_state::Column::LatestSaleTimestamp,
                    arbitrage_digest_state::Column::UnitsSold48h,
                    arbitrage_digest_state::Column::UnitsSold7d,
                    arbitrage_digest_state::Column::MedianSalePrice,
                    arbitrage_digest_state::Column::RecentClusterAvgPrice,
                    arbitrage_digest_state::Column::PriorClusterAvgPrice,
                    arbitrage_digest_state::Column::WeeklyAvgVelocity,
                    arbitrage_digest_state::Column::DestLowAskPrice,
                    arbitrage_digest_state::Column::SelectedSellReferencePrice,
                    arbitrage_digest_state::Column::SourceAskAvg,
                    arbitrage_digest_state::Column::DestAskAvg,
                    arbitrage_digest_state::Column::ReferenceMinPrice,
                    arbitrage_digest_state::Column::ReferenceAvgPrice,
                    arbitrage_digest_state::Column::MarkdownPct,
                    arbitrage_digest_state::Column::ExecutionStatus,
                    arbitrage_digest_state::Column::DeliveredAt,
                    arbitrage_digest_state::Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(&self.db)
            .await?;

        Ok(())
    }
}
