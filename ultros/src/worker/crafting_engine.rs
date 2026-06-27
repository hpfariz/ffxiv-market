use std::collections::HashMap;
use anyhow::Result;
use sea_orm::{EntityTrait, ColumnTrait, PaginatorTrait, FromQueryResult, QuerySelect, QueryFilter};
use ultros_db::UltrosDb;
use ultros_db::entity::{profile_crafting_settings, profile_crafting_subcraft_threshold};
use xiv_gen::{Recipe, Item};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubcraftIngredientDetail {
    pub item_id: i32,
    pub name: String,
    pub quantity: i32,
    pub cost_per_unit: i64,
    pub total_cost: i64,
    pub path: String, // "Buy" or "Craft"
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CraftingOpportunity {
    pub recipe_id: i32,
    pub item_id: i32,
    pub name: String,
    pub craft_type: i32,
    pub level: i32,
    pub material_cost: i64,
    pub sell_price: i64,
    pub net_profit: i64,
    pub flags: Vec<String>,
    pub ingredients: Vec<SubcraftIngredientDetail>,
}

pub struct CraftingEngine {
    db: UltrosDb,
}

impl CraftingEngine {
    pub fn new(db: UltrosDb) -> Self {
        Self { db }
    }

    pub async fn compute_crafting_opportunities(
        &self,
        profile_id: i32,
        show_all_levels: bool,
    ) -> Result<Vec<CraftingOpportunity>> {
        let (settings, thresholds) = self.db.get_crafting_settings(profile_id).await?;
        let profile = self.db.get_profile_by_id(profile_id).await?
            .ok_or_else(|| anyhow::anyhow!("Profile not found"))?;
        
        let active_dc_id = match profile.active_datacenter_id {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        // Get all worlds in the active DC
        let dc_worlds = ultros_db::entity::world::Entity::find()
            .filter(ultros_db::entity::world::Column::DatacenterId.eq(active_dc_id))
            .all(self.db.get_connection())
            .await?;
        let dc_world_ids: Vec<i32> = dc_worlds.into_iter().map(|w| w.id).collect();

        if dc_world_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Get profile's job levels
        let job_levels = self.db.get_job_levels(profile_id).await?;
        let crafter_levels: HashMap<i32, i32> = job_levels
            .iter()
            .filter(|jl| jl.kind == "Crafter")
            .map(|jl| (jl.job_id, jl.level))
            .collect();

        // Get tax rate for the profile's home world
        let tax_rate = if let Some(home_world) = profile.home_world_id {
            if let Some(cache) = self.db.get_cached_tax_rate(home_world).await? {
                cache.tax_rate
            } else {
                0.05
            }
        } else {
            0.05
        };

        // Create threshold map
        let threshold_map: HashMap<i32, &profile_crafting_subcraft_threshold::Model> = thresholds
            .iter()
            .map(|t| (t.crafting_class_id, t))
            .collect();

        let data = xiv_gen_db::data();
        
        // Group recipes by item_result
        let mut recipes_by_result: HashMap<i32, &Recipe> = HashMap::new();
        for recipe in data.recipes.values() {
            // Carpenter = 8, Blacksmith = 9, Armorer = 10, Goldsmith = 11, Leatherworker = 12, Weaver = 13, Alchemist = 14, Culinarian = 15
            recipes_by_result.insert(recipe.item_result, recipe);
        }

        // Gather all relevant item IDs to batch-fetch lowest prices
        // Since doing it one by one is slow, we will fetch all active listings for DC in one query
        let all_item_ids: Vec<i32> = data.items.keys().map(|id| id.0).collect();
        
        // Fetch all lowest prices in the DC
        // To do this efficiently, we query the database for the lowest price of each item in the DC worlds
        let lowest_prices = self.fetch_dc_lowest_prices(&dc_world_ids, &all_item_ids).await?;

        let mut opportunities = Vec::new();

        for recipe in data.recipes.values() {
            // Level and Class Job filter
            let craft_type = recipe.craft_type; // Carpenter is 0, Blacksmith is 1, etc., mapped to xiv-gen craft classes
            
            // Map craft type to class job id
            // CRP = 8, BSM = 9, ARM = 10, GSM = 11, LTW = 12, WVR = 13, ALC = 14, CUL = 15
            let class_job_id = craft_type + 8; 

            if !show_all_levels {
                let player_level = crafter_levels.get(&class_job_id).cloned().unwrap_or(0);
                
                // Lookup recipe level requirement from recipe_level_tables
                let recipe_level = data.recipe_level_tables
                    .get(&xiv_gen::RecipeLevelTableId(recipe.recipe_level_table))
                    .map(|r| r.class_job_level as i32)
                    .unwrap_or(1);

                if player_level < recipe_level {
                    continue; // Skip recipes above level
                }
            }

            let result_item = match data.items.get(&xiv_gen::ItemId(recipe.item_result)) {
                Some(i) => i,
                None => continue,
            };

            if settings.hq_only && !result_item.can_be_hq {
                continue; // Skip if NQ only and settings require HQ only
            }

            // Resolve ingredients recursively
            let mut visited = std::collections::HashSet::new();
            let mut ingredients_detail = Vec::new();
            let mut total_material_cost = 0;

            for idx in 0..8 {
                let ingredient_id = recipe.ingredient[idx];
                let qty = recipe.amount_ingredient[idx];
                if ingredient_id == 0 || qty == 0 {
                    continue;
                }

                let cost = self.resolve_cost(
                    ingredient_id,
                    &recipes_by_result,
                    &lowest_prices,
                    &threshold_map,
                    &mut visited,
                    &mut ingredients_detail,
                    qty,
                    data,
                );
                
                total_material_cost += cost;
            }

            // Lookup sell price
            let sell_price = lowest_prices.get(&recipe.item_result).cloned().unwrap_or(0) as i64;
            if sell_price == 0 {
                continue; // Skip if no market data available
            }

            // Compute net profit
            let gross_revenue = (sell_price as f64 * (1.0 - tax_rate)) as i64;
            let net_profit = gross_revenue - total_material_cost;

            if net_profit < settings.min_net_profit {
                continue; // Filter below threshold
            }

            // Flags
            let mut flags = Vec::new();
            
            // Stub for HQ_TURNIN: Grand Company turn ins can be inferred from leve items or static check
            let is_gc_turnin = data.leves.values().any(|l| l.name.contains(&result_item.name));
            if is_gc_turnin {
                flags.push("HQ_TURNIN".to_string());
            }

            if result_item.description.to_lowercase().contains("glamour") || result_item.name.to_lowercase().contains("glamour") {
                flags.push("GLAMOUR".to_string());
            }

            opportunities.push(CraftingOpportunity {
                recipe_id: recipe.key_id.0,
                item_id: recipe.item_result,
                name: result_item.name.clone(),
                craft_type,
                level: data.recipe_level_tables
                    .get(&xiv_gen::RecipeLevelTableId(recipe.recipe_level_table))
                    .map(|r| r.class_job_level as i32)
                    .unwrap_or(1),
                material_cost: total_material_cost,
                sell_price,
                net_profit,
                flags,
                ingredients: ingredients_detail,
            });
        }

        // Sort by net profit descending
        opportunities.sort_by(|a, b| b.net_profit.cmp(&a.net_profit));

        Ok(opportunities)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_cost(
        &self,
        item_id: i32,
        recipes_by_result: &HashMap<i32, &Recipe>,
        lowest_prices: &HashMap<i32, i64>,
        threshold_map: &HashMap<i32, &profile_crafting_subcraft_threshold::Model>,
        visited: &mut std::collections::HashSet<i32>,
        details: &mut Vec<SubcraftIngredientDetail>,
        qty: i32,
        data: &xiv_gen::Data,
    ) -> i64 {
        let name = data.items.get(&xiv_gen::ItemId(item_id))
            .map(|i| i.name.clone())
            .unwrap_or_else(|| format!("Item #{}", item_id));

        let buy_price = lowest_prices.get(&item_id).cloned().unwrap_or(999_999_999) as i64;
        
        if visited.contains(&item_id) {
            // Loop prevention
            let total_cost = buy_price * qty as i64;
            details.push(SubcraftIngredientDetail {
                item_id,
                name,
                quantity: qty,
                cost_per_unit: buy_price,
                total_cost,
                path: "Buy".to_string(),
            });
            return total_cost;
        }

        visited.insert(item_id);

        let sub_recipe = recipes_by_result.get(&item_id);

        if let Some(recipe) = sub_recipe {
            let class_job_id = recipe.craft_type + 8;
            
            // Recursively resolve materials cost for sub-craft
            let mut sub_details = Vec::new();
            let mut craft_cost_sum = 0;
            
            for idx in 0..8 {
                let ingredient_id = recipe.ingredient[idx];
                let sub_qty = recipe.amount_ingredient[idx];
                if ingredient_id == 0 || sub_qty == 0 {
                    continue;
                }
                craft_cost_sum += self.resolve_cost(
                    ingredient_id,
                    recipes_by_result,
                    lowest_prices,
                    threshold_map,
                    visited,
                    &mut sub_details,
                    sub_qty,
                    data,
                );
            }

            // The recipe might yield multiple result units
            let yield_qty = recipe.amount_result.max(1) as i64;
            let craft_price_per_unit = craft_cost_sum / yield_qty;

            // Apply per-class savings thresholds
            let mut choose_craft = false;

            if let Some(t) = threshold_map.get(&class_job_id) {
                let savings_pct = if buy_price > 0 {
                    (buy_price - craft_price_per_unit) as f64 / buy_price as f64
                } else {
                    0.0
                };
                let savings_gil = buy_price - craft_price_per_unit;

                if let Some(pct_t) = t.savings_pct_threshold {
                    if savings_pct >= pct_t {
                        choose_craft = true;
                    }
                }
                if let Some(gil_t) = t.savings_gil_threshold {
                    if savings_gil >= gil_t {
                        choose_craft = true;
                    }
                }
            } else {
                // If no threshold configured, choose the cheaper path
                if craft_price_per_unit < buy_price {
                    choose_craft = true;
                }
            }

            visited.remove(&item_id);

            if choose_craft {
                let total_cost = craft_price_per_unit * qty as i64;
                details.push(SubcraftIngredientDetail {
                    item_id,
                    name,
                    quantity: qty,
                    cost_per_unit: craft_price_per_unit,
                    total_cost,
                    path: "Craft".to_string(),
                });
                return total_cost;
            }
        }

        visited.remove(&item_id);

        let total_cost = buy_price * qty as i64;
        details.push(SubcraftIngredientDetail {
            item_id,
            name,
            quantity: qty,
            cost_per_unit: buy_price,
            total_cost,
            path: "Buy".to_string(),
        });
        
        total_cost
    }

    async fn fetch_dc_lowest_prices(
        &self,
        worlds: &[i32],
        items: &[i32],
    ) -> Result<HashMap<i32, i64>> {
        // Query the database for the minimum price per unit of each item across all DC worlds
        // Standard SeaORM query grouping by item_id
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
}
