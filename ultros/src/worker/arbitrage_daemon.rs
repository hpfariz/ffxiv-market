use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use sea_orm::{
    ColumnTrait, DbBackend, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter, QuerySelect,
    Statement,
};
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use ultros_db::UltrosDb;
use ultros_db::entity::{
    active_listing, arbitrage_opportunity, player_profile, sale_history, world,
};

pub struct ArbitrageDaemon {
    db: UltrosDb,
    trigger: Arc<Notify>,
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
}

#[derive(FromQueryResult)]
struct UnitsSold {
    total: Option<i64>,
}

impl ArbitrageDaemon {
    pub fn new(db: UltrosDb, trigger: Arc<Notify>) -> Self {
        Self { db, trigger }
    }

    pub fn start(self, token: CancellationToken) {
        let db = self.db.clone();
        let trigger = self.trigger.clone();
        tokio::spawn(async move {
            info!("Starting ArbitrageDaemon");
            loop {
                tokio::select! {
                    _ = trigger.notified() => {
                        // Debounce trigger: wait 30s to let batches settle
                        sleep(Duration::from_secs(30)).await;

                        // Limit frequency: run at most once every 2 minutes
                        let start_time = tokio::time::Instant::now();

                        if let Err(e) = run_arbitrage_scan(&db).await {
                            error!(error = ?e, "Arbitrage scan failed");
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

#[instrument(skip(db))]
async fn run_arbitrage_scan(db: &UltrosDb) -> Result<(), anyhow::Error> {
    info!("Running DC-wide arbitrage scan for all profiles");
    let profiles = player_profile::Entity::find()
        .all(db.get_connection())
        .await?;

    for profile in profiles {
        let settings = db.get_arbitrage_settings(profile.id).await?;
        let active_dc_id = match profile.active_datacenter_id {
            Some(id) => id,
            None => continue, // Scopes all market queries by active DC
        };

        // Get all worlds in the active DC
        let dc_worlds = world::Entity::find()
            .filter(world::Column::DatacenterId.eq(active_dc_id))
            .all(db.get_connection())
            .await?;
        let dc_world_ids: Vec<i32> = dc_worlds.into_iter().map(|w| w.id).collect();

        if dc_world_ids.is_empty() {
            continue;
        }

        // Apply world exclusion list
        let excluded_worlds: Vec<i32> = if let Some(val) = &settings.world_exclusion_list {
            serde_json::from_value(val.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Category allowlist/blocklist
        let blocklisted_categories: Vec<i32> = if let Some(val) = &settings.category_blocklist {
            serde_json::from_value(val.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        let allowlisted_categories: Vec<i32> = if let Some(val) = &settings.category_allowlist {
            serde_json::from_value(val.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Manual item exclusions
        let excluded_item_ids: Vec<i32> = if let Some(val) = &settings.excluded_item_ids {
            serde_json::from_value(val.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        let max_age_seconds = settings.max_listing_age_hours as i64 * 3600;
        let staleness_cutoff = Utc::now().naive_utc() - chrono::Duration::seconds(max_age_seconds);

        // Find candidate opportunities by matching cheapest listing in source world
        // to destination world cheapest listing (dest is either home_world or DC-wide cheapest).
        // To do this efficiently at scale, we use a custom SQL query joining active_listing with itself.
        // We filter out stale source listings (Gate 0).
        let sql = r#"
            WITH min_prices AS (
                SELECT item_id, hq, world_id, price_per_unit, quantity, timestamp,
                       ROW_NUMBER() OVER(PARTITION BY item_id, hq, world_id ORDER BY price_per_unit ASC, timestamp DESC) as rn
                FROM active_listing
            )
            SELECT s.item_id, s.hq, s.world_id as source_world_id, d.world_id as dest_world_id,
                   s.price_per_unit as source_price, d.price_per_unit as dest_price,
                   s.quantity as source_qty, s.timestamp as source_timestamp
            FROM min_prices s
            JOIN min_prices d ON s.item_id = d.item_id AND s.hq = d.hq
            WHERE s.world_id = ANY($1) AND d.world_id = ANY($2)
              AND s.world_id != d.world_id
              AND s.rn = 1 AND d.rn = 1
              AND s.price_per_unit < d.price_per_unit
              AND s.timestamp > $3
        "#;

        let candidates = CandidateOpportunity::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            vec![
                dc_world_ids.clone().into(),
                dc_world_ids.clone().into(),
                staleness_cutoff.into(),
            ],
        ))
        .all(db.get_connection())
        .await?;

        let mut opportunities = Vec::new();

        for cand in candidates {
            // Apply world exclusions
            if excluded_worlds.contains(&cand.source_world_id)
                || excluded_worlds.contains(&cand.dest_world_id)
            {
                continue;
            }

            // Apply item exclusions
            if excluded_item_ids.contains(&cand.item_id) {
                continue;
            }

            // Lookup item details from static xiv-gen-db
            let item = match xiv_gen_db::data().items.get(&xiv_gen::ItemId(cand.item_id)) {
                Some(i) => i,
                None => continue,
            };

            // Category filters
            let search_category = item.item_search_category;
            if !allowlisted_categories.is_empty()
                && !allowlisted_categories.contains(&search_category)
            {
                continue;
            }
            if blocklisted_categories.contains(&search_category) {
                continue;
            }

            // Gate 1 — Velocity Filter
            // Fetch total active listings for this item on the source world
            let active_count = active_listing::Entity::find()
                .filter(active_listing::Column::ItemId.eq(cand.item_id))
                .filter(active_listing::Column::WorldId.eq(cand.source_world_id))
                .filter(active_listing::Column::Hq.eq(cand.hq))
                .count(db.get_connection())
                .await?;

            if active_count == 0 {
                continue;
            }

            let sales_cutoff = Utc::now().naive_utc() - chrono::Duration::hours(48);
            let sales_res = sale_history::Entity::find()
                .select_only()
                .column_as(sale_history::Column::Quantity.sum(), "total")
                .filter(sale_history::Column::SoldItemId.eq(cand.item_id))
                .filter(sale_history::Column::WorldId.eq(cand.source_world_id))
                .filter(sale_history::Column::Hq.eq(cand.hq))
                .filter(sale_history::Column::SoldDate.gt(sales_cutoff))
                .into_model::<UnitsSold>()
                .one(db.get_connection())
                .await?;

            let units_sold_48h = sales_res.and_then(|r| r.total).unwrap_or(0);
            let velocity_score = units_sold_48h as f64 / active_count as f64;

            if velocity_score < settings.velocity_threshold {
                continue; // Drop below-threshold items
            }

            // Gate 2 — Minimum Quantity Filter
            let qty_to_buy = cand.source_qty;
            let gross_profit = (cand.dest_price - cand.source_price) as i64 * qty_to_buy as i64;
            let total_cost = cand.source_price as i64 * qty_to_buy as i64;

            if gross_profit < settings.min_profit_total {
                continue; // Drop if gross profit below the minimum floor
            }

            // Gate 3 — Travel Cost Deduction
            let travel_minutes = estimate_travel_time(
                profile.home_world_id.unwrap_or(0),
                cand.source_world_id,
                cand.dest_world_id,
            );
            let travel_deduction = travel_minutes * settings.travel_cost_rate_per_min;
            let net_profit = gross_profit - travel_deduction;

            if net_profit < settings.min_net_profit {
                continue; // Drop if net profit below threshold
            }

            // Gate 4 — Capital Check
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
                listing_age_seconds,
                total_cost,
                quantity_available: qty_to_buy,
                over_budget,
                computed_at: Utc::now().naive_utc(),
            });
        }

        // Save opportunities cache to database
        db.save_arbitrage_opportunities(profile.id, opportunities)
            .await?;
        info!(
            "Saved arbitrage opportunities for profile {}",
            profile.display_name
        );
    }

    Ok(())
}

fn estimate_travel_time(home_world: i32, source_world: i32, dest_world: i32) -> i64 {
    if source_world == dest_world {
        if source_world == home_world { 0 } else { 2 }
    } else {
        if source_world == home_world || dest_world == home_world {
            2
        } else {
            4
        }
    }
}
