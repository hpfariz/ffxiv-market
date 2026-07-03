use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use crate::alerts::delivery::{
    DeliveryEmbed, DeliveryEmbedField, deliver_embeds_non_discord_endpoint,
    deliver_embeds_to_endpoint, get_serenity_ctx, send_dm, send_webhook,
};
use chrono::{Datelike, NaiveDateTime, Timelike, Utc};
use sea_orm::{ColumnTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter, Statement};
use serde::Serialize;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use ultros_db::UltrosDb;
use ultros_db::entity::{
    arbitrage_alert_schedule_state, arbitrage_delivery_attempt, arbitrage_digest_state,
    arbitrage_item_alert_state, arbitrage_opportunity, datacenter, player_profile,
    profile_arbitrage_settings, world,
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
    dest_low_ask_price: i32,
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
    source_ask_avg: Option<f64>,
    dest_ask_avg: Option<f64>,
    reference_min_price: Option<i32>,
    reference_avg_price: Option<f64>,
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
    markdown_rejected: usize,
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

        let world_sets =
            resolve_arbitrage_world_sets(db, &settings, home_world_id, active_dc_id).await?;
        let source_world_ids = world_sets.source_world_ids.clone();
        let dest_world_ids = world_sets.dest_world_ids.clone();
        let reference_world_ids = world_sets.reference_world_ids.clone();
        let home_dc_world_ids = world_sets.home_dc_world_ids.clone();
        let world_dc_map = world_sets.world_dc_map.clone();

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

        if source_world_ids.is_empty()
            || dest_world_ids.is_empty()
            || reference_world_ids.is_empty()
        {
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
            reference_listings AS (
                SELECT item_id, hq, price_per_unit
                FROM active_listing
                WHERE world_id = ANY($8)
                  AND item_id = ANY($4)
                  AND timestamp > $2
                  AND price_per_unit > 0
            ),
            reference_prices AS (
                SELECT item_id,
                       hq,
                       MIN(price_per_unit)::integer AS reference_min_price,
                       AVG(price_per_unit)::double precision AS reference_avg_price
                FROM reference_listings
                GROUP BY item_id, hq
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
                   d.price_per_unit as dest_low_ask_price,
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
                   END AS ask_vs_recent_sale_gap_pct,
                   source_asks.current_ask_cluster_avg AS source_ask_avg,
                   dest_asks.current_ask_cluster_avg AS dest_ask_avg,
                   ref.reference_min_price,
                   ref.reference_avg_price
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
            LEFT JOIN ask_clusters source_asks ON source_asks.item_id = s.item_id
                                              AND source_asks.hq = s.hq
                                              AND source_asks.world_id = s.world_id
            LEFT JOIN ask_clusters dest_asks ON dest_asks.item_id = d.item_id
                                            AND dest_asks.hq = d.hq
                                            AND dest_asks.world_id = d.world_id
            LEFT JOIN reference_prices ref ON ref.item_id = d.item_id
                                          AND ref.hq = d.hq
            WHERE s.world_id = ANY($5) AND d.world_id = ANY($6)
              AND s.world_id != d.world_id
              AND s.rn = 1 AND d.rn = 1
              AND s.price_per_unit < GREATEST(d.price_per_unit, sales.median_sale_price)
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
                reference_world_ids.clone().into(),
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
            if weekly_avg_velocity < settings.weekly_velocity_threshold {
                stats.velocity_rejected += 1;
                continue;
            }

            let qty_to_buy = cand.source_qty;
            let selected_sell_reference_price = selected_sell_reference_price(
                &settings.sell_price_strategy,
                cand.dest_low_ask_price,
                cand.median_sale_price,
            );
            if selected_sell_reference_price <= cand.source_price {
                stats.gross_profit_rejected += 1;
                continue;
            }

            let markdown_pct = markdown_pct(cand.source_price, selected_sell_reference_price);
            if markdown_pct.unwrap_or(0.0) < settings.min_markdown_pct {
                stats.markdown_rejected += 1;
                continue;
            }

            let gross_profit =
                (selected_sell_reference_price - cand.source_price) as i64 * qty_to_buy as i64;
            let total_cost = cand.source_price as i64 * qty_to_buy as i64;

            if gross_profit < settings.min_profit_total {
                stats.gross_profit_rejected += 1;
                continue;
            }

            let travel_minutes = configured_travel_minutes(
                home_world_id.unwrap_or(0),
                cand.source_world_id,
                cand.dest_world_id,
                &home_dc_world_ids,
                settings.same_dc_travel_minutes as i64,
                settings.cross_dc_travel_minutes as i64,
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
            let execution_status = execution_status(
                home_world_id,
                cand.source_world_id,
                cand.dest_world_id,
                &world_dc_map,
            );

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
                dest_low_ask_price: cand.dest_low_ask_price,
                selected_sell_reference_price,
                source_ask_avg: cand.source_ask_avg,
                dest_ask_avg: cand.dest_ask_avg,
                reference_min_price: cand.reference_min_price,
                reference_avg_price: cand.reference_avg_price,
                markdown_pct,
                execution_status: execution_status.to_string(),
                travel_minutes,
                computed_at: Utc::now().naive_utc(),
            });
        }

        let table_opportunities = group_opportunities_for_surface(
            &opportunities,
            &settings.table_grouping_strategy,
            settings.table_max_rows_per_item,
            settings.table_include_same_dc_best,
            settings.table_show_theoretical,
            &world_dc_map,
        );
        let digest_opportunities = group_opportunities_for_surface(
            &opportunities,
            &settings.alert_grouping_strategy,
            settings.alert_max_rows_per_item,
            settings.alert_include_same_dc_best,
            settings.alert_show_theoretical,
            &world_dc_map,
        );
        let saved = table_opportunities.len();
        db.save_arbitrage_opportunities(profile.id, table_opportunities)
            .await?;
        if let Err(e) =
            deliver_arbitrage_digest(db, &profile, &settings, &digest_opportunities).await
        {
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
            markdown_rejected = stats.markdown_rejected,
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
type ItemAlertKey = (i32, i32, bool);

#[derive(Clone)]
struct DigestDeliveryCandidate {
    opportunity: arbitrage_opportunity::Model,
    state: arbitrage_digest_state::Model,
}

async fn deliver_arbitrage_digest(
    db: &UltrosDb,
    profile: &player_profile::Model,
    settings: &profile_arbitrage_settings::Model,
    opportunities: &[arbitrage_opportunity::Model],
) -> Result<(), anyhow::Error> {
    if opportunities.is_empty() && !settings.alert_send_empty_digest {
        return Ok(());
    }

    let now = Utc::now().naive_utc();
    let previous_states = db.get_arbitrage_digest_states(profile.id).await?;
    let previous_by_key: HashMap<DigestKey, arbitrage_digest_state::Model> = previous_states
        .into_iter()
        .map(|state| (digest_state_key(&state), state))
        .collect();
    let item_alert_states = db.get_arbitrage_item_alert_states(profile.id).await?;
    let item_alert_by_key: HashMap<ItemAlertKey, arbitrage_item_alert_state::Model> =
        item_alert_states
            .into_iter()
            .map(|state| (item_alert_state_key(&state), state))
            .collect();

    let mut changed = Vec::new();
    for opportunity in opportunities {
        let key = opportunity_digest_key(opportunity);
        let previous = previous_by_key.get(&key);
        let snapshot_hash = digest_snapshot_hash(opportunity);
        if settings.digest_changed_only
            && !digest_snapshot_changed(
                previous.map(|state| state.snapshot_hash.as_str()),
                &snapshot_hash,
            )
        {
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

    let improved_item_keys = changed
        .iter()
        .filter_map(|candidate| {
            let key = opportunity_item_alert_key(&candidate.opportunity);
            let previous = item_alert_by_key.get(&key);
            alert_profit_improved(previous, &candidate.opportunity, settings).then_some(key)
        })
        .collect::<HashSet<_>>();
    changed.retain(|candidate| {
        improved_item_keys.contains(&opportunity_item_alert_key(&candidate.opportunity))
    });

    if changed.is_empty() && !settings.alert_send_empty_digest {
        return Ok(());
    }

    let world_names = load_digest_world_names(db, &changed).await?;
    let mut schedule_state = db.get_arbitrage_schedule_state(profile.id).await?;
    let mut delivered_states = Vec::new();
    let mut delivered_item_states = Vec::new();
    let mut delivered_any = false;

    let (immediate_candidates, digest_candidates) =
        split_immediate_candidates(&changed, settings, &mut schedule_state, now);
    let mut delivered_pending_item_keys = Vec::new();

    if !immediate_candidates.is_empty() {
        let title = format!("Immediate Arbitrage Alert: {}", profile.display_name);
        let body = build_arbitrage_digest_body(
            &immediate_candidates,
            &world_names,
            settings,
            "Immediate threshold matches",
        );
        let embeds = build_arbitrage_digest_embeds(&immediate_candidates, &world_names, settings);
        let batch_hash = digest_batch_hash(&immediate_candidates);
        if deliver_arbitrage_digest_message(
            db,
            profile,
            &title,
            &body,
            &embeds,
            "immediate",
            &batch_hash,
        )
        .await?
        {
            delivered_any = true;
            schedule_state.last_immediate_sent_at = Some(now);
            delivered_states.extend(
                immediate_candidates
                    .iter()
                    .map(|candidate| candidate.state.clone()),
            );
            delivered_item_states.extend(item_alert_states_from_candidates(
                profile.id,
                &immediate_candidates,
                now,
            ));
            delivered_pending_item_keys
                .extend(pending_item_keys_from_candidates(&immediate_candidates));
        }
    }

    let digest_due = should_send_digest(settings, &schedule_state, now);
    if digest_due && (!digest_candidates.is_empty() || settings.alert_send_empty_digest) {
        let title = format!("Arbitrage Digest: {}", profile.display_name);
        let body = if digest_candidates.is_empty() {
            "No changed arbitrage opportunities qualified for this digest.".to_string()
        } else {
            build_arbitrage_digest_body(
                &digest_candidates,
                &world_names,
                settings,
                "Changed opportunities with higher item profit only",
            )
        };
        let embeds = build_arbitrage_digest_embeds(&digest_candidates, &world_names, settings);
        let batch_hash = digest_batch_hash(&digest_candidates);
        if deliver_arbitrage_digest_message(
            db,
            profile,
            &title,
            &body,
            &embeds,
            "digest",
            &batch_hash,
        )
        .await?
        {
            delivered_any = true;
            schedule_state.last_digest_sent_at = Some(now);
            delivered_states.extend(
                digest_candidates
                    .iter()
                    .map(|candidate| candidate.state.clone()),
            );
            delivered_item_states.extend(item_alert_states_from_candidates(
                profile.id,
                &digest_candidates,
                now,
            ));
            delivered_pending_item_keys
                .extend(pending_item_keys_from_candidates(&digest_candidates));
        }
    } else if !digest_candidates.is_empty() {
        db.upsert_arbitrage_pending_digests(pending_rows_from_candidates(
            profile.id,
            &digest_candidates,
            now,
        ))
        .await?;
    }

    if delivered_any {
        db.delete_arbitrage_pending_digest_item_keys(profile.id, &delivered_pending_item_keys)
            .await?;
        db.upsert_arbitrage_digest_states(delivered_states).await?;
        db.upsert_arbitrage_item_alert_states(merge_item_alert_states(delivered_item_states))
            .await?;
    }
    db.save_arbitrage_schedule_state(schedule_state).await?;

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
    embeds: &[DeliveryEmbed],
    delivery_kind: &str,
    batch_hash: &str,
) -> Result<bool, anyhow::Error> {
    let ctx_opt = get_serenity_ctx();
    let mut attempted = false;
    let mut success = false;
    let mut last_error: Option<anyhow::Error> = None;

    match db
        .list_arbitrage_delivery_endpoints(profile.id, profile.discord_user_id)
        .await
    {
        Ok(endpoints) => {
            attempted = !endpoints.is_empty();
            for endpoint in endpoints {
                let delivered = if let Some(ctx) = &ctx_opt {
                    deliver_embeds_to_endpoint(&endpoint, title, body, embeds, db, ctx.as_ref())
                        .await
                } else {
                    deliver_embeds_non_discord_endpoint(&endpoint, title, body, embeds, db).await
                };

                if let Err(e) = delivered {
                    record_delivery_attempt(
                        db,
                        profile.id,
                        Some(endpoint.id),
                        delivery_kind,
                        batch_hash,
                        false,
                        Some(e.to_string()),
                    )
                    .await;
                    error!(
                        profile_id = profile.id,
                        endpoint_id = endpoint.id,
                        error = ?e,
                        "Failed to send arbitrage digest to notification endpoint"
                    );
                    last_error = Some(e);
                } else {
                    record_delivery_attempt(
                        db,
                        profile.id,
                        Some(endpoint.id),
                        delivery_kind,
                        batch_hash,
                        true,
                        None,
                    )
                    .await;
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
            record_delivery_attempt(
                db,
                profile.id,
                None,
                delivery_kind,
                batch_hash,
                false,
                Some(e.to_string()),
            )
            .await;
            error!(
                profile_id = profile.id,
                error = ?e,
                "Failed to send arbitrage digest webhook"
            );
            last_error = Some(e);
        } else {
            record_delivery_attempt(db, profile.id, None, delivery_kind, batch_hash, true, None)
                .await;
            success = true;
        }
    }

    if profile.alert_channel_dm {
        attempted = true;
        if let Some(ctx) = &ctx_opt {
            if let Err(e) = send_dm(profile.discord_user_id, title, body, ctx.as_ref()).await {
                record_delivery_attempt(
                    db,
                    profile.id,
                    None,
                    delivery_kind,
                    batch_hash,
                    false,
                    Some(e.to_string()),
                )
                .await;
                error!(
                    profile_id = profile.id,
                    error = ?e,
                    "Failed to send arbitrage digest DM"
                );
                last_error = Some(e);
            } else {
                record_delivery_attempt(
                    db,
                    profile.id,
                    None,
                    delivery_kind,
                    batch_hash,
                    true,
                    None,
                )
                .await;
                success = true;
            }
        } else {
            let message = format!(
                "Serenity context not available for arbitrage digest DM profile {}",
                profile.id
            );
            error!("{message}");
            record_delivery_attempt(
                db,
                profile.id,
                None,
                delivery_kind,
                batch_hash,
                false,
                Some(message.clone()),
            )
            .await;
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

fn split_immediate_candidates(
    changed: &[DigestDeliveryCandidate],
    settings: &profile_arbitrage_settings::Model,
    schedule_state: &mut arbitrage_alert_schedule_state::Model,
    now: NaiveDateTime,
) -> (Vec<DigestDeliveryCandidate>, Vec<DigestDeliveryCandidate>) {
    reset_immediate_window_if_needed(schedule_state, now);

    if !settings.alert_immediate_threshold_enabled || settings.alert_immediate_max_per_hour <= 0 {
        return (Vec::new(), changed.to_vec());
    }

    let remaining = (settings.alert_immediate_max_per_hour - schedule_state.immediate_sent_count)
        .max(0) as usize;
    if remaining == 0 {
        return (Vec::new(), changed.to_vec());
    }

    let mut eligible = changed
        .iter()
        .filter(|candidate| immediate_threshold_met(&candidate.opportunity, settings))
        .cloned()
        .collect::<Vec<_>>();
    eligible.sort_by(|a, b| b.opportunity.net_profit.cmp(&a.opportunity.net_profit));
    eligible.truncate(remaining);

    let sent_keys = eligible
        .iter()
        .map(|candidate| opportunity_digest_key(&candidate.opportunity))
        .collect::<HashSet<_>>();
    let digest_candidates = changed
        .iter()
        .filter(|candidate| !sent_keys.contains(&opportunity_digest_key(&candidate.opportunity)))
        .cloned()
        .collect::<Vec<_>>();

    schedule_state.immediate_sent_count += eligible.len() as i32;
    if schedule_state.immediate_sent_count_window_start.is_none() {
        schedule_state.immediate_sent_count_window_start = Some(now);
    }

    (eligible, digest_candidates)
}

fn reset_immediate_window_if_needed(
    schedule_state: &mut arbitrage_alert_schedule_state::Model,
    now: NaiveDateTime,
) {
    let should_reset = schedule_state
        .immediate_sent_count_window_start
        .map(|start| now.signed_duration_since(start) >= chrono::Duration::hours(1))
        .unwrap_or(true);
    if should_reset {
        schedule_state.immediate_sent_count_window_start = Some(now);
        schedule_state.immediate_sent_count = 0;
    }
}

fn immediate_threshold_met(
    opportunity: &arbitrage_opportunity::Model,
    settings: &profile_arbitrage_settings::Model,
) -> bool {
    opportunity.net_profit >= settings.alert_immediate_min_net_profit
        && opportunity.markdown_pct.unwrap_or(0.0) >= settings.alert_immediate_min_markdown_pct
        && opportunity.velocity_score >= settings.alert_immediate_min_velocity
}

fn should_send_digest(
    settings: &profile_arbitrage_settings::Model,
    schedule_state: &arbitrage_alert_schedule_state::Model,
    now: NaiveDateTime,
) -> bool {
    match settings.alert_frequency_mode.as_str() {
        "IMMEDIATE" => false,
        "SCANNER_COMPLETE" => true,
        "SCHEDULED" => settings
            .alert_schedule_cron
            .as_deref()
            .map(|cron| cron_due(cron, schedule_state.last_digest_sent_at, now))
            .unwrap_or_else(|| interval_digest_due(settings, schedule_state, now)),
        "DIGEST_INTERVAL" => interval_digest_due(settings, schedule_state, now),
        _ => true,
    }
}

fn interval_digest_due(
    settings: &profile_arbitrage_settings::Model,
    schedule_state: &arbitrage_alert_schedule_state::Model,
    now: NaiveDateTime,
) -> bool {
    schedule_state
        .last_digest_sent_at
        .map(|last| {
            now.signed_duration_since(last)
                >= chrono::Duration::minutes(settings.alert_digest_interval_minutes.max(1) as i64)
        })
        .unwrap_or(true)
}

fn cron_due(expr: &str, last_sent_at: Option<NaiveDateTime>, now: NaiveDateTime) -> bool {
    if !cron_matches_now(expr, now) {
        return false;
    }
    last_sent_at
        .map(|last| now.signed_duration_since(last) >= chrono::Duration::minutes(1))
        .unwrap_or(true)
}

fn cron_matches_now(expr: &str, now: NaiveDateTime) -> bool {
    let fields = expr.split_whitespace().collect::<Vec<_>>();
    let (second, minute, hour, day, month, weekday) = match fields.as_slice() {
        [minute, hour, day, month, weekday] => ("0", *minute, *hour, *day, *month, *weekday),
        [second, minute, hour, day, month, weekday] => {
            (*second, *minute, *hour, *day, *month, *weekday)
        }
        _ => return false,
    };

    cron_field_matches(second, now.second(), 0, 59, false)
        && cron_field_matches(minute, now.minute(), 0, 59, false)
        && cron_field_matches(hour, now.hour(), 0, 23, false)
        && cron_field_matches(day, now.day(), 1, 31, false)
        && cron_field_matches(month, now.month(), 1, 12, false)
        && cron_field_matches(weekday, now.weekday().num_days_from_sunday(), 0, 7, true)
}

fn cron_field_matches(field: &str, value: u32, min: u32, max: u32, sunday_alias: bool) -> bool {
    field.split(',').any(|part| {
        let part = part.trim();
        if part == "*" {
            return true;
        }

        let (base, step) = match part.split_once('/') {
            Some((base, step)) => (base, step.parse::<u32>().ok().filter(|step| *step > 0)),
            None => (part, None),
        };
        let step = step.unwrap_or(1);
        let range: Option<(u32, u32)> = if base == "*" {
            Some((min, max))
        } else if let Some((start, end)) = base.split_once('-') {
            match (start.parse::<u32>().ok(), end.parse::<u32>().ok()) {
                (Some(start), Some(end)) => Some((start, end)),
                _ => None,
            }
        } else {
            base.parse::<u32>().ok().map(|single| (single, single))
        };

        let Some((start, end)) = range else {
            return false;
        };
        let matches_value = |candidate: u32| {
            candidate >= start && candidate <= end && (candidate - start).is_multiple_of(step)
        };

        if sunday_alias && value == 0 {
            matches_value(0) || matches_value(7)
        } else {
            matches_value(value)
        }
    })
}

fn alert_profit_improved(
    previous: Option<&arbitrage_item_alert_state::Model>,
    opportunity: &arbitrage_opportunity::Model,
    settings: &profile_arbitrage_settings::Model,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };
    let gil_threshold = settings.alert_profit_improvement_threshold_gil.max(0);
    let pct_threshold = ((previous.best_alerted_net_profit as f64)
        * settings.alert_profit_improvement_threshold_pct.max(0.0)
        / 100.0)
        .ceil() as i64;
    let threshold = gil_threshold.max(pct_threshold);
    opportunity.net_profit > previous.best_alerted_net_profit.saturating_add(threshold)
}

fn item_alert_states_from_candidates(
    profile_id: i32,
    candidates: &[DigestDeliveryCandidate],
    now: NaiveDateTime,
) -> Vec<arbitrage_item_alert_state::Model> {
    let mut best_by_item: HashMap<ItemAlertKey, &DigestDeliveryCandidate> = HashMap::new();
    for candidate in candidates {
        let key = opportunity_item_alert_key(&candidate.opportunity);
        let should_replace = best_by_item
            .get(&key)
            .map(|existing| candidate.opportunity.net_profit > existing.opportunity.net_profit)
            .unwrap_or(true);
        if should_replace {
            best_by_item.insert(key, candidate);
        }
    }

    best_by_item
        .into_iter()
        .map(
            |((_profile_id, item_id, hq), candidate)| arbitrage_item_alert_state::Model {
                id: 0,
                profile_id,
                item_id,
                hq,
                best_alerted_net_profit: candidate.opportunity.net_profit,
                best_alerted_snapshot_hash: candidate.state.snapshot_hash.clone(),
                last_alerted_at: now,
            },
        )
        .collect()
}

fn merge_item_alert_states(
    states: Vec<arbitrage_item_alert_state::Model>,
) -> Vec<arbitrage_item_alert_state::Model> {
    let mut by_key: HashMap<ItemAlertKey, arbitrage_item_alert_state::Model> = HashMap::new();
    for state in states {
        let key = item_alert_state_key(&state);
        let should_replace = by_key
            .get(&key)
            .map(|existing| state.best_alerted_net_profit > existing.best_alerted_net_profit)
            .unwrap_or(true);
        if should_replace {
            by_key.insert(key, state);
        }
    }
    by_key.into_values().collect()
}

fn pending_rows_from_candidates(
    profile_id: i32,
    candidates: &[DigestDeliveryCandidate],
    now: NaiveDateTime,
) -> Vec<ultros_db::entity::arbitrage_pending_digest::Model> {
    candidates
        .iter()
        .map(|candidate| {
            let opportunity = &candidate.opportunity;
            ultros_db::entity::arbitrage_pending_digest::Model {
                id: 0,
                profile_id,
                item_id: opportunity.item_id,
                hq: opportunity.hq,
                source_world_id: opportunity.source_world_id,
                dest_world_id: opportunity.dest_world_id,
                snapshot_hash: candidate.state.snapshot_hash.clone(),
                net_profit: opportunity.net_profit,
                section: pending_section(opportunity).to_string(),
                queued_at: now,
                updated_at: now,
            }
        })
        .collect()
}

fn pending_section(opportunity: &arbitrage_opportunity::Model) -> &'static str {
    if opportunity.execution_status != "EXECUTABLE" {
        "THEORETICAL"
    } else if is_review_volatility_flag(&opportunity.volatility_flag) {
        "REVIEW"
    } else {
        "CLEAN"
    }
}

fn pending_item_keys_from_candidates(candidates: &[DigestDeliveryCandidate]) -> Vec<(i32, bool)> {
    candidates
        .iter()
        .map(|candidate| {
            let opportunity = &candidate.opportunity;
            (opportunity.item_id, opportunity.hq)
        })
        .collect()
}

fn digest_batch_hash(candidates: &[DigestDeliveryCandidate]) -> String {
    let mut hashes = candidates
        .iter()
        .map(|candidate| candidate.state.snapshot_hash.as_str())
        .collect::<Vec<_>>();
    hashes.sort_unstable();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hashes.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

async fn record_delivery_attempt(
    db: &UltrosDb,
    profile_id: i32,
    endpoint_id: Option<i32>,
    delivery_kind: &str,
    batch_hash: &str,
    success: bool,
    error_message: Option<String>,
) {
    if let Err(e) = db
        .insert_arbitrage_delivery_attempt(arbitrage_delivery_attempt::Model {
            id: 0,
            profile_id,
            endpoint_id,
            delivery_kind: delivery_kind.to_string(),
            snapshot_batch_hash: batch_hash.to_string(),
            success,
            error_message,
            attempted_at: Utc::now().naive_utc(),
        })
        .await
    {
        error!(
            profile_id,
            error = ?e,
            "Failed to record arbitrage delivery attempt"
        );
    }
}

fn build_arbitrage_digest_body(
    changed: &[DigestDeliveryCandidate],
    world_names: &HashMap<i32, String>,
    settings: &profile_arbitrage_settings::Model,
    summary: &str,
) -> String {
    let mut body = format!(
        "{summary}. Previously delivered items are omitted until ask prices or sale-history summaries change and the item profit improves.\n"
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
        settings,
        settings.digest_max_clean.max(1) as usize,
        &mut omitted,
    );
    if settings.digest_include_review {
        append_digest_section(
            &mut body,
            "Review: Volatile Opportunities",
            &review,
            world_names,
            settings,
            settings.digest_max_review.max(0) as usize,
            &mut omitted,
        );
    } else {
        omitted += review.len();
    }

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

fn build_arbitrage_digest_embeds(
    candidates: &[DigestDeliveryCandidate],
    world_names: &HashMap<i32, String>,
    settings: &profile_arbitrage_settings::Model,
) -> Vec<DeliveryEmbed> {
    let mut sorted = candidates.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| b.opportunity.net_profit.cmp(&a.opportunity.net_profit));
    sorted
        .into_iter()
        .map(|candidate| build_arbitrage_opportunity_embed(candidate, world_names, settings))
        .collect()
}

fn build_arbitrage_opportunity_embed(
    candidate: &DigestDeliveryCandidate,
    world_names: &HashMap<i32, String>,
    settings: &profile_arbitrage_settings::Model,
) -> DeliveryEmbed {
    let opportunity = &candidate.opportunity;
    let state = &candidate.state;
    let item_name = xiv_gen_db::data()
        .items
        .get(&xiv_gen::ItemId(opportunity.item_id))
        .map(|item| item.name.as_str())
        .unwrap_or("Unknown Item");
    let quality = if opportunity.hq { "HQ" } else { "NQ" };
    let source_world = digest_world_name(world_names, opportunity.source_world_id);
    let dest_world = digest_world_name(world_names, opportunity.dest_world_id);
    let risk = if opportunity.volatility_flag == "NONE" {
        "Clean"
    } else {
        opportunity.volatility_flag.as_str()
    };
    let markdown = opportunity
        .markdown_pct
        .map(|pct| format!("{pct:.1}% below sell reference"))
        .unwrap_or_else(|| "n/a".to_string());
    let color = if is_review_volatility_flag(&opportunity.volatility_flag) {
        0xff7a18
    } else if opportunity.execution_status != "EXECUTABLE" {
        0xf0b429
    } else {
        0x00c850
    };

    let mut description = format!(
        "{} -> {} | {} | qty {}",
        source_world, dest_world, opportunity.execution_status, state.quantity_available
    );
    if settings.digest_include_ultros_links {
        description.push_str(&format!("\n/market/item/{}", opportunity.item_id));
    }

    DeliveryEmbed {
        title: format!(
            "Deal Alert: {item_name} ({quality} #{})",
            opportunity.item_id
        ),
        description,
        color,
        url: settings
            .digest_include_universalis_links
            .then(|| format!("https://universalis.app/market/{}", opportunity.item_id)),
        fields: vec![
            DeliveryEmbedField {
                name: "Profit potential".to_string(),
                value: format!("{} Gil", format_i64_with_commas(state.net_profit)),
                inline: true,
            },
            DeliveryEmbedField {
                name: "Markdown".to_string(),
                value: markdown,
                inline: true,
            },
            DeliveryEmbedField {
                name: "Sale velocity".to_string(),
                value: format!(
                    "{:.2} current / {:.1} per day",
                    opportunity.velocity_score, opportunity.weekly_avg_velocity
                ),
                inline: true,
            },
            DeliveryEmbedField {
                name: format!("Source market: {source_world}"),
                value: format!(
                    "Min: {} Gil / Avg ask: {}",
                    format_i64_with_commas(state.source_price as i64),
                    state
                        .source_ask_avg
                        .map(|price| format!("{price:.0}"))
                        .unwrap_or_else(|| "n/a".to_string())
                ),
                inline: false,
            },
            DeliveryEmbedField {
                name: format!("{dest_world} target"),
                value: format!(
                    "Ask: {} Gil / Sell ref: {} Gil / Median sale: {} Gil",
                    format_i64_with_commas(state.dest_low_ask_price as i64),
                    format_i64_with_commas(state.selected_sell_reference_price as i64),
                    format_i64_with_commas(state.median_sale_price as i64)
                ),
                inline: false,
            },
            DeliveryEmbedField {
                name: "Reference".to_string(),
                value: format!(
                    "Min: {} / Avg: {}",
                    state
                        .reference_min_price
                        .map(|price| format!("{} Gil", format_i64_with_commas(price as i64)))
                        .unwrap_or_else(|| "n/a".to_string()),
                    state
                        .reference_avg_price
                        .map(|price| format!("{price:.0} Gil"))
                        .unwrap_or_else(|| "n/a".to_string())
                ),
                inline: false,
            },
            DeliveryEmbedField {
                name: "Risk".to_string(),
                value: risk.to_string(),
                inline: true,
            },
            DeliveryEmbedField {
                name: "Travel".to_string(),
                value: format!("{} min", opportunity.travel_minutes),
                inline: true,
            },
        ],
        footer: Some("Changed-only; same item/HQ re-alerts only after profit improves".to_string()),
    }
}

fn append_digest_section(
    body: &mut String,
    title: &str,
    candidates: &[&DigestDeliveryCandidate],
    world_names: &HashMap<i32, String>,
    settings: &profile_arbitrage_settings::Model,
    limit: usize,
    omitted: &mut usize,
) {
    if candidates.is_empty() || limit == 0 {
        return;
    }

    let header = format!("\n**{title}**\n");
    if body.len() + header.len() <= 3900 {
        body.push_str(&header);
    }

    for candidate in candidates.iter().take(limit) {
        let line = digest_line(candidate, world_names, settings);
        if body.len() + line.len() <= 3900 {
            body.push_str(&line);
        } else {
            *omitted += 1;
        }
    }
    *omitted += candidates.len().saturating_sub(limit);
}

fn digest_line(
    candidate: &DigestDeliveryCandidate,
    world_names: &HashMap<i32, String>,
    settings: &profile_arbitrage_settings::Model,
) -> String {
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
    let markdown = opportunity
        .markdown_pct
        .map(|pct| format!("{pct:.1}% below sell reference"))
        .unwrap_or_else(|| "markdown n/a".to_string());
    let mut links = Vec::new();
    if settings.digest_include_universalis_links {
        links.push(format!(
            "Universalis: https://universalis.app/market/{}",
            opportunity.item_id
        ));
    }
    if settings.digest_include_ultros_links {
        links.push(format!("Ultros: /market/item/{}", opportunity.item_id));
    }
    let links = if links.is_empty() {
        String::new()
    } else {
        format!("\n  {}", links.join(" | "))
    };

    format!(
        "- **{}** ({} #{}) {} -> {}: buy {} / sell {}, net **{} gil**, {}, vel {:.2} / {:.1}/day, qty {}, risk {}, {}{}\n",
        item_name,
        quality,
        opportunity.item_id,
        source_world,
        dest_world,
        format_i64_with_commas(state.source_price as i64),
        format_i64_with_commas(state.selected_sell_reference_price as i64),
        format_i64_with_commas(state.net_profit),
        markdown,
        opportunity.velocity_score,
        opportunity.weekly_avg_velocity,
        state.quantity_available,
        risk,
        opportunity.execution_status,
        links
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
        dest_price: opportunity.selected_sell_reference_price,
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
        dest_low_ask_price: opportunity.dest_low_ask_price,
        selected_sell_reference_price: opportunity.selected_sell_reference_price,
        source_ask_avg: opportunity.source_ask_avg,
        dest_ask_avg: opportunity.dest_ask_avg,
        reference_min_price: opportunity.reference_min_price,
        reference_avg_price: opportunity.reference_avg_price,
        markdown_pct: opportunity.markdown_pct,
        execution_status: opportunity.execution_status.clone(),
        delivered_at: now,
        created_at: now,
        updated_at: now,
    }
}

fn digest_snapshot_hash(opportunity: &arbitrage_opportunity::Model) -> String {
    format!(
        "source_price={}|dest_price={}|dest_low_ask={}|sell_ref={}|quantity={}|net_profit={}|volatility_flag={}|execution={}|latest_sale={}|units_48h={}|units_7d={}|median_sale={}|recent_sales={}|prior_sales={}|recent_avg={}|prior_avg={}|current_ask_avg={}|source_ask_avg={}|dest_ask_avg={}|ask_gap={}|ref_min={}|ref_avg={}|markdown={}|weekly={:.4}",
        source_unit_price(opportunity),
        competing_unit_price(opportunity),
        opportunity.dest_low_ask_price,
        opportunity.selected_sell_reference_price,
        opportunity.quantity_available,
        opportunity.net_profit,
        opportunity.volatility_flag,
        opportunity.execution_status,
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
        snapshot_opt_f64(opportunity.source_ask_avg),
        snapshot_opt_f64(opportunity.dest_ask_avg),
        snapshot_opt_f64(opportunity.ask_vs_recent_sale_gap_pct),
        opportunity
            .reference_min_price
            .map(|price| price.to_string())
            .unwrap_or_else(|| "none".to_string()),
        snapshot_opt_f64(opportunity.reference_avg_price),
        snapshot_opt_f64(opportunity.markdown_pct),
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

fn opportunity_item_alert_key(opportunity: &arbitrage_opportunity::Model) -> ItemAlertKey {
    (opportunity.profile_id, opportunity.item_id, opportunity.hq)
}

fn item_alert_state_key(state: &arbitrage_item_alert_state::Model) -> ItemAlertKey {
    (state.profile_id, state.item_id, state.hq)
}

fn source_unit_price(opportunity: &arbitrage_opportunity::Model) -> i32 {
    if opportunity.quantity_available <= 0 {
        return 0;
    }
    clamp_i64_to_i32(opportunity.total_cost / opportunity.quantity_available as i64)
}

fn competing_unit_price(opportunity: &arbitrage_opportunity::Model) -> i32 {
    if opportunity.selected_sell_reference_price > 0 {
        return opportunity.selected_sell_reference_price;
    }
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

#[derive(Clone)]
struct ArbitrageWorldSets {
    source_world_ids: Vec<i32>,
    dest_world_ids: Vec<i32>,
    reference_world_ids: Vec<i32>,
    home_dc_world_ids: HashSet<i32>,
    world_dc_map: HashMap<i32, i32>,
}

async fn resolve_arbitrage_world_sets(
    db: &UltrosDb,
    settings: &profile_arbitrage_settings::Model,
    home_world_id: Option<i32>,
    active_dc_id: i32,
) -> Result<ArbitrageWorldSets, anyhow::Error> {
    let active_dc_worlds = world::Entity::find()
        .filter(world::Column::DatacenterId.eq(active_dc_id))
        .all(db.get_connection())
        .await?;
    let active_dc_world_ids: Vec<i32> = active_dc_worlds.iter().map(|w| w.id).collect();

    let home_world = match home_world_id {
        Some(id) => {
            world::Entity::find_by_id(id)
                .one(db.get_connection())
                .await?
        }
        None => None,
    };
    let home_dc_id = home_world
        .as_ref()
        .map(|world| world.datacenter_id)
        .unwrap_or(active_dc_id);

    let home_dc_worlds = world_ids_for_datacenter(db, home_dc_id).await?;
    let home_dc_world_ids: Vec<i32> = home_dc_worlds.iter().map(|w| w.id).collect();
    let home_dc_world_set = home_dc_world_ids.iter().copied().collect::<HashSet<_>>();

    let source_world_ids = match settings.source_world_scope.as_str() {
        "CURRENT_WORLD" => home_world_id.map(|id| vec![id]).unwrap_or_default(),
        "SAME_REGION" => world_ids_for_region_of_datacenter(db, home_dc_id).await?,
        _ => home_dc_world_ids.clone(),
    };

    let dest_world_ids = if settings.require_home_world_sell_target {
        home_world_id.map(|id| vec![id]).unwrap_or_default()
    } else {
        match settings.destination_world_scope.as_str() {
            "HOME_WORLD" => home_world_id.map(|id| vec![id]).unwrap_or_default(),
            "SAME_REGION" => world_ids_for_region_of_datacenter(db, home_dc_id).await?,
            "CUSTOM" => json_i32_list(&settings.seller_world_ids),
            _ => active_dc_world_ids.clone(),
        }
    };

    let reference_world_ids = match settings.reference_price_scope.as_str() {
        "DESTINATION_WORLD" => dest_world_ids.clone(),
        "ACTIVE_REGION" => world_ids_for_region_of_datacenter(db, active_dc_id).await?,
        "SOURCE_AND_DESTINATION" => unique_world_ids(
            source_world_ids
                .iter()
                .chain(dest_world_ids.iter())
                .copied()
                .collect(),
        ),
        _ => {
            let mut datacenter_ids = dest_world_ids
                .iter()
                .filter_map(|world_id| {
                    active_dc_worlds
                        .iter()
                        .chain(home_dc_worlds.iter())
                        .find(|world| world.id == *world_id)
                        .map(|world| world.datacenter_id)
                })
                .collect::<HashSet<_>>();
            if datacenter_ids.is_empty() {
                datacenter_ids.insert(active_dc_id);
            }
            let mut ids = Vec::new();
            for datacenter_id in datacenter_ids {
                ids.extend(
                    world_ids_for_datacenter(db, datacenter_id)
                        .await?
                        .into_iter()
                        .map(|world| world.id),
                );
            }
            unique_world_ids(ids)
        }
    };

    let all_world_ids = unique_world_ids(
        source_world_ids
            .iter()
            .chain(dest_world_ids.iter())
            .chain(reference_world_ids.iter())
            .chain(home_dc_world_ids.iter())
            .copied()
            .collect(),
    );
    let world_dc_map = load_world_dc_map(db, all_world_ids).await?;

    Ok(ArbitrageWorldSets {
        source_world_ids: unique_world_ids(source_world_ids),
        dest_world_ids: unique_world_ids(dest_world_ids),
        reference_world_ids: unique_world_ids(reference_world_ids),
        home_dc_world_ids: home_dc_world_set,
        world_dc_map,
    })
}

async fn world_ids_for_datacenter(
    db: &UltrosDb,
    datacenter_id: i32,
) -> Result<Vec<world::Model>, anyhow::Error> {
    Ok(world::Entity::find()
        .filter(world::Column::DatacenterId.eq(datacenter_id))
        .all(db.get_connection())
        .await?)
}

async fn world_ids_for_region_of_datacenter(
    db: &UltrosDb,
    datacenter_id: i32,
) -> Result<Vec<i32>, anyhow::Error> {
    let Some(dc) = datacenter::Entity::find_by_id(datacenter_id)
        .one(db.get_connection())
        .await?
    else {
        return Ok(Vec::new());
    };
    let region_dcs = datacenter::Entity::find()
        .filter(datacenter::Column::RegionId.eq(dc.region_id))
        .all(db.get_connection())
        .await?;
    let region_dc_ids: Vec<i32> = region_dcs.into_iter().map(|dc| dc.id).collect();

    Ok(world::Entity::find()
        .filter(world::Column::DatacenterId.is_in(region_dc_ids))
        .all(db.get_connection())
        .await?
        .into_iter()
        .map(|world| world.id)
        .collect())
}

async fn load_world_dc_map(
    db: &UltrosDb,
    world_ids: Vec<i32>,
) -> Result<HashMap<i32, i32>, anyhow::Error> {
    if world_ids.is_empty() {
        return Ok(HashMap::new());
    }

    Ok(world::Entity::find()
        .filter(world::Column::Id.is_in(world_ids))
        .all(db.get_connection())
        .await?
        .into_iter()
        .map(|world| (world.id, world.datacenter_id))
        .collect())
}

fn json_i32_list(value: &Option<sea_orm::JsonValue>) -> Vec<i32> {
    value
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

fn unique_world_ids(ids: Vec<i32>) -> Vec<i32> {
    ids.into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
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

fn configured_travel_minutes(
    home_world: i32,
    source_world: i32,
    dest_world: i32,
    home_dc_world_ids: &HashSet<i32>,
    same_dc_minutes: i64,
    cross_dc_minutes: i64,
) -> i64 {
    if source_world == dest_world {
        0
    } else {
        match travel_tier(home_world, source_world, home_dc_world_ids) {
            "HOME" => 0,
            "SAME_DC_VISIT" => same_dc_minutes,
            _ => cross_dc_minutes,
        }
    }
}

fn selected_sell_reference_price(strategy: &str, dest_low_ask: i32, median_sale_price: i32) -> i32 {
    match strategy {
        "DESTINATION_LOW_ASK" => dest_low_ask,
        "MEDIAN_SALE" => median_sale_price,
        _ => dest_low_ask.min(median_sale_price),
    }
}

fn markdown_pct(source_price: i32, selected_sell_reference_price: i32) -> Option<f64> {
    if selected_sell_reference_price <= 0 {
        return None;
    }
    Some(
        (selected_sell_reference_price - source_price) as f64
            / selected_sell_reference_price as f64
            * 100.0,
    )
}

fn execution_status(
    home_world_id: Option<i32>,
    source_world_id: i32,
    dest_world_id: i32,
    world_dc_map: &HashMap<i32, i32>,
) -> &'static str {
    if Some(dest_world_id) == home_world_id {
        "EXECUTABLE"
    } else if same_datacenter(source_world_id, dest_world_id, world_dc_map) {
        "THEORETICAL_SAME_DC_SELL"
    } else {
        "THEORETICAL_CROSS_DC_SELL"
    }
}

fn same_datacenter(
    source_world_id: i32,
    dest_world_id: i32,
    world_dc_map: &HashMap<i32, i32>,
) -> bool {
    match (
        world_dc_map.get(&source_world_id),
        world_dc_map.get(&dest_world_id),
    ) {
        (Some(source_dc), Some(dest_dc)) => source_dc == dest_dc,
        _ => false,
    }
}

fn group_opportunities_for_surface(
    opportunities: &[arbitrage_opportunity::Model],
    strategy: &str,
    max_rows_per_item: i32,
    include_same_dc_best: bool,
    show_theoretical: bool,
    world_dc_map: &HashMap<i32, i32>,
) -> Vec<arbitrage_opportunity::Model> {
    let max_rows_per_item = max_rows_per_item.max(1) as usize;
    let mut grouped: HashMap<(i32, bool), Vec<&arbitrage_opportunity::Model>> = HashMap::new();

    for opportunity in opportunities {
        if !show_theoretical && opportunity.execution_status != "EXECUTABLE" {
            continue;
        }
        grouped
            .entry((opportunity.item_id, opportunity.hq))
            .or_default()
            .push(opportunity);
    }

    let mut selected = Vec::new();
    for mut group in grouped.into_values() {
        group.sort_by(|a, b| b.net_profit.cmp(&a.net_profit));

        if strategy == "ALL" {
            selected.extend(group.into_iter().take(max_rows_per_item).cloned());
            continue;
        }

        let best = group.first().copied();
        if let Some(best) = best {
            selected.push(best.clone());
        }

        if strategy == "BEST_ONLY" || !include_same_dc_best || max_rows_per_item == 1 {
            continue;
        }

        if best
            .map(|opportunity| {
                same_datacenter(
                    opportunity.source_world_id,
                    opportunity.dest_world_id,
                    world_dc_map,
                )
            })
            .unwrap_or(false)
        {
            continue;
        }

        if let Some(same_dc_best) = group.into_iter().find(|opportunity| {
            same_datacenter(
                opportunity.source_world_id,
                opportunity.dest_world_id,
                world_dc_map,
            ) && !selected
                .iter()
                .any(|selected: &arbitrage_opportunity::Model| {
                    selected.item_id == opportunity.item_id
                        && selected.hq == opportunity.hq
                        && selected.source_world_id == opportunity.source_world_id
                        && selected.dest_world_id == opportunity.dest_world_id
                })
        }) {
            selected.push(same_dc_best.clone());
        }
    }

    selected.sort_by(|a, b| b.net_profit.cmp(&a.net_profit));
    selected
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
