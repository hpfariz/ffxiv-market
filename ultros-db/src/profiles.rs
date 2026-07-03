use crate::UltrosDb;
use crate::entity::*;
use anyhow::Result;
use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Set,
    TransactionTrait, TryIntoModel,
};

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
        profile_arbitrage_settings::ActiveModel {
            profile_id: Set(profile.id),
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
        }
        .insert(&txn)
        .await?;

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
            let new_settings = profile_arbitrage_settings::ActiveModel {
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
            }
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
