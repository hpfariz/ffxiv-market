use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::alerts::delivery::{
    deliver_non_discord_endpoint, deliver_to_endpoint, get_serenity_ctx, send_dm, send_webhook,
};
use chrono::{NaiveDateTime, Utc};
use sea_orm::{ColumnTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter, Statement};
use serde::Serialize;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use ultros_db::UltrosDb;
use ultros_db::entity::{
    arbitrage_digest_state, arbitrage_opportunity, datacenter, player_profile, world,
};

pub struct ArbitrageDaemon {
    db: UltrosDb,
    trigger: Arc<Notify>,
    status: ArbitrageScanStatusTracker,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArbitrageScanStatus {
    pub phase: String,
    pub message: String,
    pub progress_percent: u8,
    pub profiles_scanned: i32,
    pub profiles_total: i32,
    pub queued_at: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
}

impl Default for ArbitrageScanStatus {
    fn default() -> Self {
        Self {
            phase: "idle".to_string(),
            message: "Scanner has not run yet".to_string(),
            progress_percent: 0,
            profiles_scanned: 0,
            profiles_total: 0,
            queued_at: None,
            started_at: None,
            completed_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct ArbitrageScanStatusTracker {
    status: Arc<RwLock<ArbitrageScanStatus>>,
}

impl ArbitrageScanStatusTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self) -> ArbitrageScanStatus {
        self.status.read().await.clone()
    }

    pub async fn mark_queued(&self, message: impl Into<String>) {
        let mut status = self.status.write().await;
        status.phase = "queued".to_string();
        status.message = message.into();
        status.progress_percent = 5;
        status.queued_at = Some(Utc::now().to_rfc3339());
        status.completed_at = None;
        status.last_error = None;
    }

    async fn mark_scanning(&self, profiles_total: usize) {
        let mut status = self.status.write().await;
        status.phase = "scanning".to_string();
        status.message = "Scanning arbitrage opportunities".to_string();
        status.progress_percent = 10;
        status.profiles_scanned = 0;
        status.profiles_total = profiles_total as i32;
        status.started_at = Some(Utc::now().to_rfc3339());
        status.completed_at = None;
        status.last_error = None;
    }

    async fn mark_profile_progress(&self, profiles_scanned: usize, profiles_total: usize) {
        let mut status = self.status.write().await;
        status.phase = "scanning".to_string();
        status.profiles_scanned = profiles_scanned as i32;
        status.profiles_total = profiles_total as i32;
        status.progress_percent = if profiles_total == 0 {
            90
        } else {
            let profile_progress = (profiles_scanned.saturating_mul(80) / profiles_total) as u8;
            10u8.saturating_add(profile_progress).min(90)
        };
        status.message = format!("Scanning profile {profiles_scanned} of {profiles_total}");
    }

    async fn mark_complete(&self, profiles_total: usize) {
        let mut status = self.status.write().await;
        status.phase = "complete".to_string();
        status.message = "Arbitrage scan complete".to_string();
        status.progress_percent = 100;
        status.profiles_scanned = profiles_total as i32;
        status.profiles_total = profiles_total as i32;
        status.completed_at = Some(Utc::now().to_rfc3339());
        status.last_error = None;
    }

    async fn mark_failed(&self, error: &anyhow::Error) {
        let mut status = self.status.write().await;
        status.phase = "failed".to_string();
        status.message = "Arbitrage scan failed".to_string();
        status.progress_percent = 100;
        status.completed_at = Some(Utc::now().to_rfc3339());
        status.last_error = Some(error.to_string());
    }
}

#[derive(FromQueryResult)]
struct CandidateOpportunity {
    item_id: i32,
    hq: bool,
    source_world_id: i32,
    dest_world_id: i32,
    source_price: i32,
    dest_price: i32,
    source_qty: i32,
    source_timestamp: NaiveDateTime,
    dest_active_count: i64,
    units_sold_48h: i64,
    units_sold_7d: i64,
    median_sale_price: i32,
    latest_sale_timestamp: Option<NaiveDateTime>,
    regime_recent_window_count: i32,
    recent_cluster_avg_price: Option<f64>,
    prior_cluster_avg_price: Option<f64>,
    price_jump_ratio: Option<f64>,
    within_cluster_cv_recent: Option<f64>,
    within_cluster_cv_prior: Option<f64>,
    recent_cluster_sales_count: i32,
    prior_cluster_sales_count: i32,
    current_ask_cluster_avg: Option<f64>,
    ask_vs_recent_sale_gap_pct: Option<f64>,
}

#[derive(Default)]
struct ScanStats {
    candidates: usize,
    setup_skipped: bool,
    world_excluded: usize,
    item_excluded: usize,
    static_missing: usize,
    not_marketable: usize,
    category_rejected: usize,
    velocity_rejected: usize,
    gross_profit_rejected: usize,
    net_profit_rejected: usize,
    volatility_suppressed: usize,
}

impl ArbitrageDaemon {
    pub fn new(db: UltrosDb, trigger: Arc<Notify>, status: ArbitrageScanStatusTracker) -> Self {
        Self {
            db,
            trigger,
            status,
        }
    }

    pub fn start(self, token: CancellationToken) {
        let db = self.db.clone();
        let trigger = self.trigger.clone();
        let status = self.status.clone();
        tokio::spawn(async move {
            info!("Starting ArbitrageDaemon");
            loop {
                tokio::select! {
                    _ = trigger.notified() => {
                        status.mark_queued("Scanner queued; waiting for market updates to settle").await;
                        // Debounce trigger: wait 30s to let batches settle
                        sleep(Duration::from_secs(30)).await;

                        // Limit frequency: run at most once every 2 minutes
                        let start_time = tokio::time::Instant::now();

                        if let Err(e) = run_arbitrage_scan(&db, status.clone()).await {
                            error!(error = ?e, "Arbitrage scan failed");
                            status.mark_failed(&e).await;
                        }

                        let elapsed = start_time.elapsed();
                        if elapsed < Duration::from_secs(120) {
                            sleep(Duration::from_secs(120) - elapsed).await;
                        }
                    }
                    _ = token.cancelled() => {
                        info!("ArbitrageDaemon cancelled");
                        break;
                    }
                }
            }
        });
    }
}

#[instrument(skip(db, status))]
async fn run_arbitrage_scan(
    db: &UltrosDb,
    status: ArbitrageScanStatusTracker,
) -> Result<(), anyhow::Error> {
    let scan_started = tokio::time::Instant::now();
    info!("Running DC-wide arbitrage scan for all profiles");

    let profiles = player_profile::Entity::find()
        .all(db.get_connection())
        .await?;
    let profiles_len = profiles.len();
    status.mark_scanning(profiles_len).await;

    let marketable_item_ids: Vec<i32> = xiv_gen_db::data()
        .items
        .values()
        .filter(|item| is_market_board_candidate(item))
        .map(|item| item.key_id.0)
        .collect();

    for (profile_index, profile) in profiles.into_iter().enumerate() {
        let profile_started = tokio::time::Instant::now();
        let settings = db.get_arbitrage_settings(profile.id).await?;
        let active_dc_id = match profile.active_datacenter_id {
            Some(id) => id,
            None => {
                status
                    .mark_profile_progress(profile_index + 1, profiles_len)
                    .await;
                continue;
            }
        };
        let home_world_id = profile.home_world_id;

        let dc_worlds = world::Entity::find()
            .filter(world::Column::DatacenterId.eq(active_dc_id))
            .all(db.get_connection())
            .await?;
        let dc_world_ids: Vec<i32> = dc_worlds.into_iter().map(|w| w.id).collect();

        if dc_world_ids.is_empty() {
            status
                .mark_profile_progress(profile_index + 1, profiles_len)
                .await;
            continue;
        }

        let (source_world_ids, _default_dest_world_ids, home_dc_world_ids) =
            resolve_execution_worlds(
                db,
                &settings.source_world_scope,
                home_world_id,
                active_dc_id,
            )
            .await?;

        if settings.require_home_world_sell_target && home_world_id.is_none() {
            let stats = ScanStats {
                setup_skipped: true,
                ..ScanStats::default()
            };
            info!(
                profile_id = profile.id,
                profile = %profile.display_name,
                setup_skipped = stats.setup_skipped,
                "Skipped arbitrage scan because safe sell-target mode requires a home world"
            );
            status
                .mark_profile_progress(profile_index + 1, profiles_len)
                .await;
            continue;
        }

        let dest_world_ids = if settings.require_home_world_sell_target {
            match home_world_id {
                Some(id) => vec![id],
                None => {
                    status
                        .mark_profile_progress(profile_index + 1, profiles_len)
                        .await;
                    continue;
                }
            }
        } else {
            dc_world_ids.clone()
        };

        if source_world_ids.is_empty() || dest_world_ids.is_empty() {
            status
                .mark_profile_progress(profile_index + 1, profiles_len)
                .await;
            continue;
        }

        let listing_world_ids: Vec<i32> = source_world_ids
            .iter()
            .chain(dest_world_ids.iter())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let excluded_worlds: Vec<i32> = settings
            .world_exclusion_list
            .as_ref()
            .and_then(|val| serde_json::from_value(val.clone()).ok())
            .unwrap_or_default();
        let blocklisted_categories: Vec<i32> = settings
            .category_blocklist
            .as_ref()
            .and_then(|val| serde_json::from_value(val.clone()).ok())
            .unwrap_or_default();
        let allowlisted_categories: Vec<i32> = settings
            .category_allowlist
            .as_ref()
            .and_then(|val| serde_json::from_value(val.clone()).ok())
            .unwrap_or_default();
        let excluded_item_ids: Vec<i32> = settings
            .excluded_item_ids
            .as_ref()
            .and_then(|val| serde_json::from_value(val.clone()).ok())
            .unwrap_or_default();

        let max_age_seconds = settings.max_listing_age_hours as i64 * 3600;
        let staleness_cutoff = Utc::now().naive_utc() - chrono::Duration::seconds(max_age_seconds);
        let sales_cutoff = Utc::now().naive_utc() - chrono::Duration::hours(48);
        let sales_7d_cutoff = Utc::now().naive_utc() - chrono::Duration::days(7);

        let sql = r#"
            WITH fresh_listings AS (
                SELECT item_id, hq, world_id, price_per_unit, quantity, timestamp
                FROM active_listing
                WHERE world_id = ANY($1)
                  AND item_id = ANY($4)
                  AND timestamp > $2
                  AND price_per_unit > 0
            ),
            min_prices AS (
                SELECT item_id, hq, world_id, price_per_unit, quantity, timestamp,
                       ROW_NUMBER() OVER(PARTITION BY item_id, hq, world_id ORDER BY price_per_unit ASC, timestamp DESC) as rn
                FROM fresh_listings
            ),
            active_counts AS (
                SELECT item_id, hq, world_id, COUNT(*)::bigint AS active_count
                FROM fresh_listings
                GROUP BY item_id, hq, world_id
            ),
            ask_ranked AS (
                SELECT item_id, hq, world_id, price_per_unit,
                       ROW_NUMBER() OVER(PARTITION BY item_id, hq, world_id ORDER BY price_per_unit ASC, timestamp DESC) as ask_rn
                FROM fresh_listings
            ),
            ask_clusters AS (
                SELECT item_id,
                       hq,
                       world_id,
                       (AVG(price_per_unit) FILTER (WHERE ask_rn <= 3))::double precision AS current_ask_cluster_avg
                FROM ask_ranked
                GROUP BY item_id, hq, world_id
            ),
            sales_ranked AS (
                SELECT sold_item_id AS item_id,
                       hq,
                       world_id,
                       quantity,
                       price_per_item,
                       sold_date,
                       ROW_NUMBER() OVER(PARTITION BY sold_item_id, hq, world_id ORDER BY sold_date DESC) AS sale_rn,
                       COUNT(*) OVER(PARTITION BY sold_item_id, hq, world_id) AS sale_count
                FROM sale_history
                WHERE world_id = ANY($6)
                  AND sold_item_id = ANY($4)
                  AND sold_date > $3
                  AND price_per_item > 0
            ),
            sales_48h AS (
                SELECT sold_item_id AS item_id,
                       hq,
                       world_id,
                       SUM(quantity)::bigint AS units_sold,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_item)::integer AS median_sale_price
                FROM sale_history
                WHERE world_id = ANY($6)
                  AND sold_item_id = ANY($4)
                  AND sold_date > $3
                  AND price_per_item > 0
                GROUP BY sold_item_id, hq, world_id
            ),
            sales_7d AS (
                SELECT sold_item_id AS item_id,
                       hq,
                       world_id,
                       SUM(quantity)::bigint AS units_sold_7d,
                       MAX(sold_date) AS latest_sale_timestamp
                FROM sale_history
                WHERE world_id = ANY($6)
                  AND sold_item_id = ANY($4)
                  AND sold_date > $7
                  AND price_per_item > 0
                GROUP BY sold_item_id, hq, world_id
            ),
            sales_clusters AS (
                SELECT item_id,
                       hq,
                       world_id,
                       MAX(GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer)) AS regime_recent_window_count,
                       COUNT(*) FILTER (WHERE sale_rn <= GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer))::integer AS recent_cluster_sales_count,
                       COUNT(*) FILTER (WHERE sale_rn > GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer))::integer AS prior_cluster_sales_count,
                       (AVG(price_per_item) FILTER (WHERE sale_rn <= GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer)))::double precision AS recent_cluster_avg_price,
                       (AVG(price_per_item) FILTER (WHERE sale_rn > GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer)))::double precision AS prior_cluster_avg_price,
                       (STDDEV_SAMP(price_per_item) FILTER (WHERE sale_rn <= GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer)))::double precision AS recent_cluster_stddev,
                       (STDDEV_SAMP(price_per_item) FILTER (WHERE sale_rn > GREATEST(3, CEIL(sale_count::double precision * 0.30)::integer)))::double precision AS prior_cluster_stddev
                FROM sales_ranked
                GROUP BY item_id, hq, world_id
            )
            SELECT s.item_id, s.hq, s.world_id as source_world_id, d.world_id as dest_world_id,
                   s.price_per_unit as source_price,
                   LEAST(d.price_per_unit, sales.median_sale_price) as dest_price,
                   s.quantity as source_qty,
                   s.timestamp as source_timestamp,
                   active.active_count as dest_active_count,
                   sales.units_sold as units_sold_48h,
                   COALESCE(sales7.units_sold_7d, 0)::bigint AS units_sold_7d,
                   sales.median_sale_price AS median_sale_price,
                   sales7.latest_sale_timestamp,
                   COALESCE(clusters.regime_recent_window_count, 0)::integer AS regime_recent_window_count,
                   clusters.recent_cluster_avg_price,
                   clusters.prior_cluster_avg_price,
                   CASE
                       WHEN clusters.prior_cluster_avg_price IS NOT NULL AND clusters.prior_cluster_avg_price > 0
                       THEN clusters.recent_cluster_avg_price / clusters.prior_cluster_avg_price
                       ELSE NULL
                   END AS price_jump_ratio,
                   CASE
                       WHEN clusters.recent_cluster_avg_price IS NOT NULL AND clusters.recent_cluster_avg_price > 0
                       THEN clusters.recent_cluster_stddev / clusters.recent_cluster_avg_price
                       ELSE NULL
                   END AS within_cluster_cv_recent,
                   CASE
                       WHEN clusters.prior_cluster_avg_price IS NOT NULL AND clusters.prior_cluster_avg_price > 0
                       THEN clusters.prior_cluster_stddev / clusters.prior_cluster_avg_price
                       ELSE NULL
                   END AS within_cluster_cv_prior,
                   COALESCE(clusters.recent_cluster_sales_count, 0)::integer AS recent_cluster_sales_count,
                   COALESCE(clusters.prior_cluster_sales_count, 0)::integer AS prior_cluster_sales_count,
                   asks.current_ask_cluster_avg,
                   CASE
                       WHEN clusters.recent_cluster_avg_price IS NOT NULL
                         AND clusters.recent_cluster_avg_price > 0
                         AND asks.current_ask_cluster_avg IS NOT NULL
                       THEN ABS(asks.current_ask_cluster_avg - clusters.recent_cluster_avg_price) / clusters.recent_cluster_avg_price * 100.0
                       ELSE NULL
                   END AS ask_vs_recent_sale_gap_pct
            FROM min_prices s
            JOIN min_prices d ON s.item_id = d.item_id AND s.hq = d.hq
            JOIN active_counts active ON active.item_id = d.item_id
                                    AND active.hq = d.hq
                                    AND active.world_id = d.world_id
            JOIN sales_48h sales ON sales.item_id = d.item_id
                                AND sales.hq = d.hq
                                AND sales.world_id = d.world_id
            LEFT JOIN sales_7d sales7 ON sales7.item_id = d.item_id
                                     AND sales7.hq = d.hq
                                     AND sales7.world_id = d.world_id
            LEFT JOIN sales_clusters clusters ON clusters.item_id = d.item_id
                                             AND clusters.hq = d.hq
                                             AND clusters.world_id = d.world_id
            LEFT JOIN ask_clusters asks ON asks.item_id = d.item_id
                                       AND asks.hq = d.hq
                                       AND asks.world_id = d.world_id
            WHERE s.world_id = ANY($5) AND d.world_id = ANY($6)
              AND s.world_id != d.world_id
              AND s.rn = 1 AND d.rn = 1
              AND s.price_per_unit < LEAST(d.price_per_unit, sales.median_sale_price)
        "#;

        let candidates = CandidateOpportunity::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            vec![
                listing_world_ids.into(),
                staleness_cutoff.into(),
                sales_cutoff.into(),
                marketable_item_ids.clone().into(),
                source_world_ids.clone().into(),
                dest_world_ids.clone().into(),
                sales_7d_cutoff.into(),
            ],
        ))
        .all(db.get_connection())
        .await?;

        let mut opportunities = Vec::new();
        let mut stats = ScanStats {
            candidates: candidates.len(),
            ..ScanStats::default()
        };

        for cand in candidates {
            if excluded_worlds.contains(&cand.source_world_id)
                || excluded_worlds.contains(&cand.dest_world_id)
            {
                stats.world_excluded += 1;
                continue;
            }

            if excluded_item_ids.contains(&cand.item_id) {
                stats.item_excluded += 1;
                continue;
            }

            let item = match xiv_gen_db::data().items.get(&xiv_gen::ItemId(cand.item_id)) {
                Some(i) => i,
                None => {
                    stats.static_missing += 1;
                    continue;
                }
            };

            if !is_market_board_candidate(item) || (cand.hq && !item.can_be_hq) {
                stats.not_marketable += 1;
                continue;
            }

            let search_category = item.item_search_category;
            if !allowlisted_categories.is_empty()
                && !allowlisted_categories.contains(&search_category)
            {
                stats.category_rejected += 1;
                continue;
            }
            if blocklisted_categories.contains(&search_category) {
                stats.category_rejected += 1;
                continue;
            }

            if cand.dest_active_count <= 0 {
                stats.velocity_rejected += 1;
                continue;
            }

            let velocity_score = cand.units_sold_48h as f64 / cand.dest_active_count as f64;
            let weekly_avg_velocity = weekly_avg_velocity(cand.units_sold_7d);

            if velocity_score < settings.velocity_threshold {
                stats.velocity_rejected += 1;
                continue;
            }

            let qty_to_buy = cand.source_qty;
            let gross_profit = (cand.dest_price - cand.source_price) as i64 * qty_to_buy as i64;
            let total_cost = cand.source_price as i64 * qty_to_buy as i64;

            if gross_profit < settings.min_profit_total {
                stats.gross_profit_rejected += 1;
                continue;
            }

            let travel_minutes = estimate_travel_time(
                home_world_id.unwrap_or(0),
                cand.source_world_id,
                cand.dest_world_id,
                &home_dc_world_ids,
            );
            let travel_tier = travel_tier(
                home_world_id.unwrap_or(0),
                cand.source_world_id,
                &home_dc_world_ids,
            );
            let travel_deduction = travel_minutes * settings.travel_cost_rate_per_min;
            let net_profit = gross_profit - travel_deduction;

            if net_profit < settings.min_net_profit {
                stats.net_profit_rejected += 1;
                continue;
            }

            let volatility_flag = volatility_flag(
                cand.price_jump_ratio,
                cand.recent_cluster_sales_count,
                cand.ask_vs_recent_sale_gap_pct,
                settings.max_price_jump_ratio,
                settings.min_recent_cluster_confirmations,
                settings.require_ask_confirmation,
                settings.max_ask_vs_sale_gap_percent,
            );

            if volatility_flag != "NONE" && settings.volatility_action == "SUPPRESS" {
                stats.volatility_suppressed += 1;
                continue;
            }

            let over_budget = total_cost > profile.gil_balance;
            let listing_age_seconds = Utc::now()
                .naive_utc()
                .signed_duration_since(cand.source_timestamp)
                .num_seconds();

            opportunities.push(arbitrage_opportunity::Model {
                id: 0,
                profile_id: profile.id,
                item_id: cand.item_id,
                hq: cand.hq,
                source_world_id: cand.source_world_id,
                dest_world_id: cand.dest_world_id,
                gross_profit,
                net_profit,
                velocity_score,
                weekly_avg_velocity,
                units_sold_48h: cand.units_sold_48h,
                units_sold_7d: cand.units_sold_7d,
                median_sale_price: cand.median_sale_price,
                latest_sale_timestamp: cand.latest_sale_timestamp,
                listing_age_seconds,
                total_cost,
                quantity_available: qty_to_buy,
                over_budget,
                travel_tier: travel_tier.to_string(),
                volatility_flag: volatility_flag.to_string(),
                regime_recent_window_count: cand.regime_recent_window_count,
                recent_cluster_avg_price: cand.recent_cluster_avg_price,
                prior_cluster_avg_price: cand.prior_cluster_avg_price,
                price_jump_ratio: cand.price_jump_ratio,
                within_cluster_cv_recent: cand.within_cluster_cv_recent,
                within_cluster_cv_prior: cand.within_cluster_cv_prior,
                recent_cluster_sales_count: cand.recent_cluster_sales_count,
                prior_cluster_sales_count: cand.prior_cluster_sales_count,
                current_ask_cluster_avg: cand.current_ask_cluster_avg,
                ask_vs_recent_sale_gap_pct: cand.ask_vs_recent_sale_gap_pct,
                computed_at: Utc::now().naive_utc(),
            });
        }

        let saved = opportunities.len();
        let digest_opportunities = opportunities.clone();
        db.save_arbitrage_opportunities(profile.id, opportunities)
            .await?;
        if let Err(e) = deliver_arbitrage_digest(db, &profile, &digest_opportunities).await {
            error!(
                profile_id = profile.id,
                profile = %profile.display_name,
                error = ?e,
                "Failed to deliver arbitrage digest"
            );
        }
        info!(
            profile_id = profile.id,
            profile = %profile.display_name,
            candidates = stats.candidates,
            saved,
            world_excluded = stats.world_excluded,
            item_excluded = stats.item_excluded,
            static_missing = stats.static_missing,
            not_marketable = stats.not_marketable,
            category_rejected = stats.category_rejected,
            velocity_rejected = stats.velocity_rejected,
            gross_profit_rejected = stats.gross_profit_rejected,
            net_profit_rejected = stats.net_profit_rejected,
            volatility_suppressed = stats.volatility_suppressed,
            elapsed_ms = profile_started.elapsed().as_millis(),
            "Saved arbitrage opportunities"
        );
        status
            .mark_profile_progress(profile_index + 1, profiles_len)
            .await;
    }
    status.mark_complete(profiles_len).await;

    info!(
        profiles_scanned = profiles_len,
        elapsed_ms = scan_started.elapsed().as_millis(),
        "Completed DC-wide arbitrage scan"
    );
    Ok(())
}

fn is_market_board_candidate(item: &xiv_gen::Item) -> bool {
    item.item_search_category > 1 && !item.name.trim().is_empty() && item.stack_size > 0
}

fn volatility_flag(
    price_jump_ratio: Option<f64>,
    recent_cluster_sales_count: i32,
    ask_vs_recent_sale_gap_pct: Option<f64>,
    max_price_jump_ratio: f64,
    min_recent_cluster_confirmations: i32,
    require_ask_confirmation: bool,
    max_ask_vs_sale_gap_percent: f64,
) -> &'static str {
    let Some(price_jump_ratio) = price_jump_ratio else {
        return "NONE";
    };

    if price_jump_ratio < max_price_jump_ratio {
        return "NONE";
    }

    let mut flag = if recent_cluster_sales_count < min_recent_cluster_confirmations {
        "UNCONFIRMED_SPIKE"
    } else {
        "CONFIRMED_REGIME_CHANGE"
    };

    if require_ask_confirmation
        && ask_vs_recent_sale_gap_pct
            .map(|gap| gap > max_ask_vs_sale_gap_percent)
            .unwrap_or(true)
    {
        flag = "UNCONFIRMED_SPIKE";
    }

    flag
}

type DigestKey = (i32, i32, bool, i32, i32);

#[derive(Clone)]
struct DigestDeliveryCandidate {
    opportunity: arbitrage_opportunity::Model,
    state: arbitrage_digest_state::Model,
}

async fn deliver_arbitrage_digest(
    db: &UltrosDb,
    profile: &player_profile::Model,
    opportunities: &[arbitrage_opportunity::Model],
) -> Result<(), anyhow::Error> {
    if opportunities.is_empty() {
        return Ok(());
    }

    let now = Utc::now().naive_utc();
    let previous_states = db.get_arbitrage_digest_states(profile.id).await?;
    let previous_by_key: HashMap<DigestKey, arbitrage_digest_state::Model> = previous_states
        .into_iter()
        .map(|state| (digest_state_key(&state), state))
        .collect();

    let mut changed = Vec::new();
    for opportunity in opportunities {
        let key = opportunity_digest_key(opportunity);
        let previous = previous_by_key.get(&key);
        let snapshot_hash = digest_snapshot_hash(opportunity);
        if !digest_snapshot_changed(
            previous.map(|state| state.snapshot_hash.as_str()),
            &snapshot_hash,
        ) {
            continue;
        }

        if digest_on_cooldown(previous, profile.alert_item_cooldown_minutes, now) {
            continue;
        }

        changed.push(DigestDeliveryCandidate {
            opportunity: opportunity.clone(),
            state: digest_state_from_opportunity(profile.id, opportunity, snapshot_hash, now),
        });
    }

    if changed.is_empty() {
        return Ok(());
    }

    let world_names = load_digest_world_names(db, &changed).await?;
    let title = format!("Arbitrage Digest: {}", profile.display_name);
    let body = build_arbitrage_digest_body(&changed, &world_names);

    if deliver_arbitrage_digest_message(db, profile, &title, &body).await? {
        let delivered_states = changed
            .into_iter()
            .map(|candidate| candidate.state)
            .collect();
        db.upsert_arbitrage_digest_states(delivered_states).await?;
    }

    Ok(())
}

async fn load_digest_world_names(
    db: &UltrosDb,
    changed: &[DigestDeliveryCandidate],
) -> Result<HashMap<i32, String>, anyhow::Error> {
    let world_ids: Vec<i32> = changed
        .iter()
        .flat_map(|candidate| {
            [
                candidate.opportunity.source_world_id,
                candidate.opportunity.dest_world_id,
            ]
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if world_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let worlds = world::Entity::find()
        .filter(world::Column::Id.is_in(world_ids))
        .all(db.get_connection())
        .await?;

    Ok(worlds
        .into_iter()
        .map(|world| (world.id, world.name))
        .collect())
}

async fn deliver_arbitrage_digest_message(
    db: &UltrosDb,
    profile: &player_profile::Model,
    title: &str,
    body: &str,
) -> Result<bool, anyhow::Error> {
    let ctx_opt = get_serenity_ctx();
    let mut attempted = false;
    let mut success = false;
    let mut last_error: Option<anyhow::Error> = None;

    match db.list_endpoints(profile.discord_user_id).await {
        Ok(endpoints) => {
            attempted = !endpoints.is_empty();
            for endpoint in endpoints {
                let delivered = if let Some(ctx) = &ctx_opt {
                    deliver_to_endpoint(&endpoint, title, body, db, ctx.as_ref()).await
                } else {
                    deliver_non_discord_endpoint(&endpoint, title, body, db).await
                };

                if let Err(e) = delivered {
                    error!(
                        profile_id = profile.id,
                        endpoint_id = endpoint.id,
                        error = ?e,
                        "Failed to send arbitrage digest to notification endpoint"
                    );
                    last_error = Some(e);
                } else {
                    success = true;
                }
            }
        }
        Err(e) => {
            error!(
                profile_id = profile.id,
                error = ?e,
                "Failed to load notification endpoints for arbitrage digest"
            );
            last_error = Some(e);
        }
    }

    if let Some(webhook_url) = &profile.alert_channel_webhook
        && !webhook_url.trim().is_empty()
    {
        attempted = true;
        if let Err(e) = send_webhook(webhook_url, title, body).await {
            error!(
                profile_id = profile.id,
                error = ?e,
                "Failed to send arbitrage digest webhook"
            );
            last_error = Some(e);
        } else {
            success = true;
        }
    }

    if profile.alert_channel_dm {
        attempted = true;
        if let Some(ctx) = &ctx_opt {
            if let Err(e) = send_dm(profile.discord_user_id, title, body, ctx.as_ref()).await {
                error!(
                    profile_id = profile.id,
                    error = ?e,
                    "Failed to send arbitrage digest DM"
                );
                last_error = Some(e);
            } else {
                success = true;
            }
        } else {
            let message = format!(
                "Serenity context not available for arbitrage digest DM profile {}",
                profile.id
            );
            error!("{message}");
            last_error = Some(anyhow::Error::msg(message));
        }
    }

    if success {
        return Ok(true);
    }

    if !attempted {
        if let Some(e) = last_error {
            return Err(e);
        }
        info!(
            profile_id = profile.id,
            "No arbitrage digest notification destinations configured"
        );
        return Ok(false);
    }

    Err(last_error
        .unwrap_or_else(|| anyhow::Error::msg("arbitrage digest delivery did not succeed")))
}

fn build_arbitrage_digest_body(
    changed: &[DigestDeliveryCandidate],
    world_names: &HashMap<i32, String>,
) -> String {
    let mut body = String::from(
        "Changed opportunities only. Previously delivered rows are omitted until ask prices or sale-history summaries change.\n",
    );
    let mut clean: Vec<&DigestDeliveryCandidate> = changed
        .iter()
        .filter(|candidate| !is_review_volatility_flag(&candidate.opportunity.volatility_flag))
        .collect();
    let mut review: Vec<&DigestDeliveryCandidate> = changed
        .iter()
        .filter(|candidate| is_review_volatility_flag(&candidate.opportunity.volatility_flag))
        .collect();

    clean.sort_by(|a, b| b.opportunity.net_profit.cmp(&a.opportunity.net_profit));
    review.sort_by(|a, b| b.opportunity.net_profit.cmp(&a.opportunity.net_profit));

    let mut omitted = 0usize;
    append_digest_section(
        &mut body,
        "Clean Opportunities",
        &clean,
        world_names,
        &mut omitted,
    );
    append_digest_section(
        &mut body,
        "Review: Volatile Opportunities",
        &review,
        world_names,
        &mut omitted,
    );

    if omitted > 0 {
        let suffix = format!(
            "\n{} additional changed rows omitted from this digest.",
            omitted
        );
        if body.len() + suffix.len() <= 3900 {
            body.push_str(&suffix);
        }
    }

    body
}

fn append_digest_section(
    body: &mut String,
    title: &str,
    candidates: &[&DigestDeliveryCandidate],
    world_names: &HashMap<i32, String>,
    omitted: &mut usize,
) {
    if candidates.is_empty() {
        return;
    }

    let header = format!("\n**{title}**\n");
    if body.len() + header.len() <= 3900 {
        body.push_str(&header);
    }

    for candidate in candidates {
        let line = digest_line(candidate, world_names);
        if body.len() + line.len() <= 3900 {
            body.push_str(&line);
        } else {
            *omitted += 1;
        }
    }
}

fn digest_line(candidate: &DigestDeliveryCandidate, world_names: &HashMap<i32, String>) -> String {
    let opportunity = &candidate.opportunity;
    let state = &candidate.state;
    let item_name = xiv_gen_db::data()
        .items
        .get(&xiv_gen::ItemId(opportunity.item_id))
        .map(|item| item.name.as_str())
        .unwrap_or("Unknown Item");
    let source_world = digest_world_name(world_names, opportunity.source_world_id);
    let dest_world = digest_world_name(world_names, opportunity.dest_world_id);
    let quality = if opportunity.hq { "HQ" } else { "NQ" };
    let risk = if opportunity.volatility_flag == "NONE" {
        "Clean"
    } else {
        opportunity.volatility_flag.as_str()
    };

    format!(
        "- {} ({} #{}) {} -> {}: buy {} / sell {}, qty {}, net {} gil, vel {:.2} / {:.1}/day, risk {}\n",
        item_name,
        quality,
        opportunity.item_id,
        source_world,
        dest_world,
        format_i64_with_commas(state.source_price as i64),
        format_i64_with_commas(state.dest_price as i64),
        state.quantity_available,
        format_i64_with_commas(state.net_profit),
        opportunity.velocity_score,
        opportunity.weekly_avg_velocity,
        risk
    )
}

fn digest_world_name(world_names: &HashMap<i32, String>, world_id: i32) -> String {
    world_names
        .get(&world_id)
        .cloned()
        .unwrap_or_else(|| format!("World #{world_id}"))
}

fn digest_state_from_opportunity(
    profile_id: i32,
    opportunity: &arbitrage_opportunity::Model,
    snapshot_hash: String,
    now: NaiveDateTime,
) -> arbitrage_digest_state::Model {
    arbitrage_digest_state::Model {
        id: 0,
        profile_id,
        item_id: opportunity.item_id,
        hq: opportunity.hq,
        source_world_id: opportunity.source_world_id,
        dest_world_id: opportunity.dest_world_id,
        snapshot_hash,
        source_price: source_unit_price(opportunity),
        dest_price: competing_unit_price(opportunity),
        quantity_available: opportunity.quantity_available,
        net_profit: opportunity.net_profit,
        volatility_flag: opportunity.volatility_flag.clone(),
        latest_sale_timestamp: opportunity.latest_sale_timestamp,
        units_sold_48h: opportunity.units_sold_48h,
        units_sold_7d: opportunity.units_sold_7d,
        median_sale_price: opportunity.median_sale_price,
        recent_cluster_avg_price: opportunity.recent_cluster_avg_price,
        prior_cluster_avg_price: opportunity.prior_cluster_avg_price,
        weekly_avg_velocity: opportunity.weekly_avg_velocity,
        delivered_at: now,
        created_at: now,
        updated_at: now,
    }
}

fn digest_snapshot_hash(opportunity: &arbitrage_opportunity::Model) -> String {
    format!(
        "source_price={}|dest_price={}|quantity={}|net_profit={}|volatility_flag={}|latest_sale={}|units_48h={}|units_7d={}|median_sale={}|recent_sales={}|prior_sales={}|recent_avg={}|prior_avg={}|current_ask_avg={}|ask_gap={}|weekly={:.4}",
        source_unit_price(opportunity),
        competing_unit_price(opportunity),
        opportunity.quantity_available,
        opportunity.net_profit,
        opportunity.volatility_flag,
        opportunity
            .latest_sale_timestamp
            .map(|timestamp| timestamp.to_string())
            .unwrap_or_else(|| "none".to_string()),
        opportunity.units_sold_48h,
        opportunity.units_sold_7d,
        opportunity.median_sale_price,
        opportunity.recent_cluster_sales_count,
        opportunity.prior_cluster_sales_count,
        snapshot_opt_f64(opportunity.recent_cluster_avg_price),
        snapshot_opt_f64(opportunity.prior_cluster_avg_price),
        snapshot_opt_f64(opportunity.current_ask_cluster_avg),
        snapshot_opt_f64(opportunity.ask_vs_recent_sale_gap_pct),
        opportunity.weekly_avg_velocity,
    )
}

fn snapshot_opt_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "none".to_string())
}

fn digest_snapshot_changed(previous_hash: Option<&str>, current_hash: &str) -> bool {
    previous_hash != Some(current_hash)
}

fn digest_on_cooldown(
    previous: Option<&arbitrage_digest_state::Model>,
    cooldown_minutes: i32,
    now: NaiveDateTime,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if cooldown_minutes <= 0 {
        return false;
    }

    now.signed_duration_since(previous.delivered_at)
        < chrono::Duration::minutes(cooldown_minutes as i64)
}

fn opportunity_digest_key(opportunity: &arbitrage_opportunity::Model) -> DigestKey {
    (
        opportunity.profile_id,
        opportunity.item_id,
        opportunity.hq,
        opportunity.source_world_id,
        opportunity.dest_world_id,
    )
}

fn digest_state_key(state: &arbitrage_digest_state::Model) -> DigestKey {
    (
        state.profile_id,
        state.item_id,
        state.hq,
        state.source_world_id,
        state.dest_world_id,
    )
}

fn source_unit_price(opportunity: &arbitrage_opportunity::Model) -> i32 {
    if opportunity.quantity_available <= 0 {
        return 0;
    }
    clamp_i64_to_i32(opportunity.total_cost / opportunity.quantity_available as i64)
}

fn competing_unit_price(opportunity: &arbitrage_opportunity::Model) -> i32 {
    if opportunity.quantity_available <= 0 {
        return 0;
    }
    let gross_unit_profit = opportunity.gross_profit / opportunity.quantity_available as i64;
    clamp_i64_to_i32(source_unit_price(opportunity) as i64 + gross_unit_profit)
}

fn clamp_i64_to_i32(value: i64) -> i32 {
    value.clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn weekly_avg_velocity(units_sold_7d: i64) -> f64 {
    units_sold_7d as f64 / 7.0
}

fn is_review_volatility_flag(flag: &str) -> bool {
    flag != "NONE"
}

fn format_i64_with_commas(value: i64) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let digits = (value as i128).abs().to_string();
    let mut reversed = String::new();

    for (index, ch) in digits.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            reversed.push(',');
        }
        reversed.push(ch);
    }

    format!("{sign}{}", reversed.chars().rev().collect::<String>())
}

async fn resolve_execution_worlds(
    db: &UltrosDb,
    source_world_scope: &str,
    home_world_id: Option<i32>,
    active_dc_id: i32,
) -> Result<(Vec<i32>, Vec<i32>, HashSet<i32>), anyhow::Error> {
    let active_dc_worlds = world::Entity::find()
        .filter(world::Column::DatacenterId.eq(active_dc_id))
        .all(db.get_connection())
        .await?;
    let active_dc_world_ids: Vec<i32> = active_dc_worlds.iter().map(|w| w.id).collect();

    let Some(home_world_id) = home_world_id else {
        let active_dc_world_set = active_dc_world_ids.iter().copied().collect();
        return Ok((
            active_dc_world_ids.clone(),
            active_dc_world_ids,
            active_dc_world_set,
        ));
    };

    let Some(home_world) = world::Entity::find_by_id(home_world_id)
        .one(db.get_connection())
        .await?
    else {
        let active_dc_world_set = active_dc_world_ids.iter().copied().collect();
        return Ok((
            active_dc_world_ids.clone(),
            active_dc_world_ids,
            active_dc_world_set,
        ));
    };

    let home_dc_worlds = world::Entity::find()
        .filter(world::Column::DatacenterId.eq(home_world.datacenter_id))
        .all(db.get_connection())
        .await?;
    let home_dc_world_ids: Vec<i32> = home_dc_worlds.iter().map(|w| w.id).collect();
    let home_dc_world_set = home_dc_world_ids.iter().copied().collect::<HashSet<_>>();

    let source_world_ids = match source_world_scope {
        "CURRENT_WORLD" => vec![home_world_id],
        "SAME_REGION" => {
            let Some(home_dc) = datacenter::Entity::find_by_id(home_world.datacenter_id)
                .one(db.get_connection())
                .await?
            else {
                return Ok((
                    home_dc_world_ids.clone(),
                    home_dc_world_ids,
                    home_dc_world_set,
                ));
            };
            let region_dcs = datacenter::Entity::find()
                .filter(datacenter::Column::RegionId.eq(home_dc.region_id))
                .all(db.get_connection())
                .await?;
            let region_dc_ids: Vec<i32> = region_dcs.into_iter().map(|dc| dc.id).collect();
            world::Entity::find()
                .filter(world::Column::DatacenterId.is_in(region_dc_ids))
                .all(db.get_connection())
                .await?
                .into_iter()
                .map(|w| w.id)
                .collect()
        }
        _ => home_dc_world_ids.clone(),
    };

    Ok((source_world_ids, home_dc_world_ids, home_dc_world_set))
}

fn travel_tier(
    home_world: i32,
    source_world: i32,
    home_dc_world_ids: &HashSet<i32>,
) -> &'static str {
    if source_world == home_world {
        "HOME"
    } else if home_dc_world_ids.contains(&source_world) {
        "SAME_DC_VISIT"
    } else {
        "CROSS_DC_TRAVEL"
    }
}

fn estimate_travel_time(
    home_world: i32,
    source_world: i32,
    dest_world: i32,
    home_dc_world_ids: &HashSet<i32>,
) -> i64 {
    if source_world == dest_world {
        0
    } else {
        match travel_tier(home_world, source_world, home_dc_world_ids) {
            "HOME" => 0,
            "SAME_DC_VISIT" => 2,
            _ => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weekly_velocity_is_units_sold_over_seven_days() {
        assert_eq!(weekly_avg_velocity(0), 0.0);
        assert_eq!(weekly_avg_velocity(14), 2.0);
        assert!((weekly_avg_velocity(10) - 1.428_571).abs() < 0.000_01);
    }

    #[test]
    fn digest_snapshot_changes_only_when_fingerprint_differs() {
        assert!(!digest_snapshot_changed(Some("same"), "same"));
        assert!(digest_snapshot_changed(Some("old"), "new"));
        assert!(digest_snapshot_changed(None, "new"));
    }

    #[test]
    fn volatile_flags_are_routed_to_review() {
        assert!(!is_review_volatility_flag("NONE"));
        assert!(is_review_volatility_flag("UNCONFIRMED_SPIKE"));
        assert!(is_review_volatility_flag("CONFIRMED_REGIME_CHANGE"));
    }
}
