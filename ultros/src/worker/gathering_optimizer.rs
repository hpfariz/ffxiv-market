use anyhow::Result;
use chrono::Utc;
use sea_orm::{
    ColumnTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter, QuerySelect,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ultros_db::UltrosDb;
use ultros_db::entity::{active_listing, sale_history};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GatheringNodeDetail {
    pub item_id: i32,
    pub name: String,
    pub level: i32,
    pub unit_price: i64,
    pub velocity: f64,
    pub node_score: f64,
    pub class_kind: String, // "Miner" or "Botanist" or "Fisher"
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TimedNodeDetail {
    pub item_id: i32,
    pub name: String,
    pub level: i32,
    pub unit_price: i64,
    pub velocity: f64,
    pub node_score: f64,
    pub class_kind: String,
    pub spawn_hours: Vec<i32>,    // Eorzea hours (e.g. 0, 12)
    pub duration_hours: i32,      // Eorzea hours (e.g. 2)
    pub next_spawn_local: String, // ISO timestamp of next real-world spawn
}

pub struct GatheringOptimizer {
    db: UltrosDb,
}

struct StaticTimedNode {
    item_id: i32,
    name: &'static str,
    level: i32,
    class_kind: &'static str,
    spawn_hours: Vec<i32>,
    duration_hours: i32,
}

impl GatheringOptimizer {
    pub fn new(db: UltrosDb) -> Self {
        Self { db }
    }

    pub async fn optimize_gathering_routes(
        &self,
        profile_id: i32,
        show_all_levels: bool,
    ) -> Result<(Vec<GatheringNodeDetail>, Vec<TimedNodeDetail>)> {
        let settings = self.db.get_gathering_settings(profile_id).await?;
        let profile = self
            .db
            .get_profile_by_id(profile_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Profile not found"))?;

        let active_dc_id = match profile.active_datacenter_id {
            Some(id) => id,
            None => return Ok((Vec::new(), Vec::new())),
        };

        // Get DC world IDs
        let dc_worlds = ultros_db::entity::world::Entity::find()
            .filter(ultros_db::entity::world::Column::DatacenterId.eq(active_dc_id))
            .all(self.db.get_connection())
            .await?;
        let dc_world_ids: Vec<i32> = dc_worlds.into_iter().map(|w| w.id).collect();

        if dc_world_ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Get gatherer levels
        let job_levels = self.db.get_job_levels(profile_id).await?;
        let gatherer_levels: HashMap<String, i32> = job_levels
            .iter()
            .filter(|jl| jl.kind == "Gatherer")
            .map(|jl| {
                let name = match jl.job_id {
                    16 => "Miner".to_string(),
                    17 => "Botanist".to_string(),
                    18 => "Fisher".to_string(),
                    _ => "Miner".to_string(),
                };
                (name, jl.level)
            })
            .collect();

        let data = xiv_gen_db::data();

        // 1. Static list of timed node materials (popular Endwalker/Dawntrail materials)
        let timed_metadata = vec![
            StaticTimedNode {
                item_id: 38830,
                name: "Rutilated Quartz",
                level: 90,
                class_kind: "Miner",
                spawn_hours: vec![0, 12],
                duration_hours: 2,
            },
            StaticTimedNode {
                item_id: 38831,
                name: "Tungsten Ore",
                level: 90,
                class_kind: "Miner",
                spawn_hours: vec![2, 14],
                duration_hours: 2,
            },
            StaticTimedNode {
                item_id: 38853,
                name: "Bayberry Log",
                level: 90,
                class_kind: "Botanist",
                spawn_hours: vec![4, 16],
                duration_hours: 2,
            },
            StaticTimedNode {
                item_id: 38854,
                name: "Acacia Log",
                level: 90,
                class_kind: "Botanist",
                spawn_hours: vec![6, 18],
                duration_hours: 2,
            },
            StaticTimedNode {
                item_id: 41250,
                name: "Dawntrail Ore",
                level: 95,
                class_kind: "Miner",
                spawn_hours: vec![8, 20],
                duration_hours: 2,
            },
            StaticTimedNode {
                item_id: 41260,
                name: "Dawntrail Log",
                level: 95,
                class_kind: "Botanist",
                spawn_hours: vec![10, 22],
                duration_hours: 2,
            },
        ];

        // Gather all gatherable item IDs (both from static list and xiv-gen gathering items)
        let mut gatherable_item_ids: Vec<i32> =
            data.gathering_items.values().map(|g| g.item).collect();
        for meta in &timed_metadata {
            gatherable_item_ids.push(meta.item_id);
        }
        gatherable_item_ids.sort();
        gatherable_item_ids.dedup();

        // Batch-fetch lowest DC prices
        let lowest_prices = self
            .fetch_dc_lowest_prices(&dc_world_ids, &gatherable_item_ids)
            .await?;

        // 2. Compute Always-Available routes
        let mut always_available = Vec::new();
        for g_item in data.gathering_items.values() {
            let item_id = g_item.item;

            // Skip if this is a timed node item
            if timed_metadata.iter().any(|m| m.item_id == item_id) {
                continue;
            }

            let item = match data.items.get(&xiv_gen::ItemId(item_id)) {
                Some(i) => i,
                None => continue,
            };

            // Map ClassJob to kind
            // MIN = 16, BTN = 17, FSH = 18
            let class_kind =
                if item.item_search_category >= 14 && item.item_search_category <= 16 {
                    ("Miner".to_string(), 16)
                } else if item.item_search_category >= 17 && item.item_search_category <= 20 {
                    ("Botanist".to_string(), 17)
                } else if item.item_search_category >= 22 && item.item_search_category <= 23 {
                    ("Fisher".to_string(), 18)
                } else {
                    ("Miner".to_string(), 16)
                }
                .0;

            // Apply Class filters
            if let Some(ref cf) = settings.class_filter
                && cf != &class_kind
            {
                continue;
            }

            // Apply Level filters
            if !show_all_levels {
                let player_level = gatherer_levels.get(&class_kind).cloned().unwrap_or(0);
                if player_level < g_item.gathering_item_level {
                    continue;
                }
            }

            let unit_price = lowest_prices.get(&item_id).cloned().unwrap_or(0);
            if unit_price == 0 {
                continue;
            }

            if let Some(min_price) = settings.min_unit_price
                && unit_price < min_price
            {
                continue;
            }

            // Compute velocity score
            let velocity = self.fetch_velocity(item_id, &dc_world_ids).await?;
            let node_score = unit_price as f64 * velocity;

            always_available.push(GatheringNodeDetail {
                item_id,
                name: item.name.clone(),
                level: g_item.gathering_item_level,
                unit_price,
                velocity,
                node_score,
                class_kind,
            });
        }

        // Sort by Node Score descending
        always_available.sort_by(|a, b| {
            b.node_score
                .partial_cmp(&a.node_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 3. Compute Timed Nodes
        let mut timed_nodes = Vec::new();
        for meta in timed_metadata {
            // Apply Class filters
            if let Some(ref cf) = settings.class_filter
                && cf != meta.class_kind
            {
                continue;
            }

            // Apply Level filters
            if !show_all_levels {
                let player_level = gatherer_levels.get(meta.class_kind).cloned().unwrap_or(0);
                if player_level < meta.level {
                    continue;
                }
            }

            let unit_price = lowest_prices.get(&meta.item_id).cloned().unwrap_or(0);
            if unit_price == 0 {
                continue;
            }

            if let Some(min_price) = settings.min_unit_price
                && unit_price < min_price
            {
                continue;
            }

            let velocity = self.fetch_velocity(meta.item_id, &dc_world_ids).await?;
            let node_score = unit_price as f64 * velocity;

            // Calculate next spawn local time
            let next_spawn_local = get_next_spawn_time(meta.spawn_hours.clone());

            timed_nodes.push(TimedNodeDetail {
                item_id: meta.item_id,
                name: meta.name.to_string(),
                level: meta.level,
                unit_price,
                velocity,
                node_score,
                class_kind: meta.class_kind.to_string(),
                spawn_hours: meta.spawn_hours,
                duration_hours: meta.duration_hours,
                next_spawn_local,
            });
        }

        // Sort by Node Score descending
        timed_nodes.sort_by(|a, b| {
            b.node_score
                .partial_cmp(&a.node_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok((always_available, timed_nodes))
    }

    async fn fetch_dc_lowest_prices(
        &self,
        worlds: &[i32],
        items: &[i32],
    ) -> Result<HashMap<i32, i64>> {
        #[derive(FromQueryResult)]
        struct MinPriceRow {
            item_id: i32,
            min_price: i32,
        }

        let query = sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"
                SELECT item_id, MIN(price_per_unit) as min_price
                FROM active_listing
                WHERE world_id = ANY($1) AND item_id = ANY($2)
                GROUP BY item_id
            "#,
            vec![worlds.to_vec().into(), items.to_vec().into()],
        );

        let rows = MinPriceRow::find_by_statement(query)
            .all(self.db.get_connection())
            .await?;

        let mut prices = HashMap::new();
        for r in rows {
            prices.insert(r.item_id, r.min_price as i64);
        }

        Ok(prices)
    }

    async fn fetch_velocity(&self, item_id: i32, worlds: &[i32]) -> Result<f64> {
        // Query active listing count
        let active_count = active_listing::Entity::find()
            .filter(active_listing::Column::ItemId.eq(item_id))
            .filter(active_listing::Column::WorldId.is_in(worlds.to_vec()))
            .count(self.db.get_connection())
            .await?;

        if active_count == 0 {
            return Ok(0.0);
        }

        // Query sales in past 48h
        #[derive(FromQueryResult)]
        struct UnitsSold {
            total: Option<i64>,
        }

        let sales_cutoff = Utc::now().naive_utc() - chrono::Duration::hours(48);
        let sales_res = sale_history::Entity::find()
            .select_only()
            .column_as(sale_history::Column::Quantity.sum(), "total")
            .filter(sale_history::Column::SoldItemId.eq(item_id))
            .filter(sale_history::Column::WorldId.is_in(worlds.to_vec()))
            .filter(sale_history::Column::SoldDate.gt(sales_cutoff))
            .into_model::<UnitsSold>()
            .one(self.db.get_connection())
            .await?;

        let units_sold_48h = sales_res.and_then(|r| r.total).unwrap_or(0);
        Ok(units_sold_48h as f64 / active_count as f64)
    }
}

fn get_next_spawn_time(spawn_hours: Vec<i32>) -> String {
    let eorzea_multiplier = 3600.0 / 175.0; // 20.57142857142857
    let real_now_ms = Utc::now().timestamp_millis() as f64;
    let eorzea_now_ms = real_now_ms * eorzea_multiplier;

    let current_eorzea_hour = ((eorzea_now_ms / 3_600_000.0) % 24.0) as i32;

    // Find the next spawn hour
    let mut next_spawn_hour = spawn_hours[0];
    for &hour in &spawn_hours {
        if hour > current_eorzea_hour {
            next_spawn_hour = hour;
            break;
        }
    }

    // Compute diff in Eorzea hours
    let hour_diff = if next_spawn_hour > current_eorzea_hour {
        next_spawn_hour - current_eorzea_hour
    } else {
        (24 - current_eorzea_hour) + next_spawn_hour
    };

    // Convert Eorzea hour diff to real-world minutes/seconds
    // 1 Eorzea hour = 175 real-world seconds
    let real_diff_seconds = hour_diff as f64 * 175.0;

    let next_spawn_time = Utc::now() + chrono::Duration::seconds(real_diff_seconds as i64);
    next_spawn_time.to_rfc3339()
}
