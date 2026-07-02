#![allow(dead_code)]

use crate::alerts::delivery::{
    deliver_non_discord_endpoint, deliver_to_endpoint, send_dm, send_webhook,
};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::error;
use ultros_db::UltrosDb;

pub(crate) struct ArbitrageSignal {
    pub gross_profit: i64,
    pub net_profit: i64,
    pub velocity_score: f64,
    pub total_cost: i64,
    pub quantity: i32,
    pub over_budget: bool,
    pub source_world: String,
    pub dest_world: String,
}

pub(crate) struct CraftingSignal {
    pub recipe_id: i32,
    pub material_cost: i64,
    pub sell_price: i64,
    pub net_profit: i64,
    pub flags: Vec<String>,
}

pub(crate) struct PendingAlertEntry {
    pub arbitrage: Option<ArbitrageSignal>,
    pub crafting: Option<CraftingSignal>,
    #[allow(unused)]
    pub spawned_at: Instant,
}

pub(crate) struct MergedAlertManager {
    db: UltrosDb,
    pending: Mutex<HashMap<(i32, i32, bool), PendingAlertEntry>>, // Key: (profile_id, item_id, hq)
    cooldowns: Mutex<HashMap<(i32, i32, bool), Instant>>,         // Key: (profile_id, item_id, hq)
}

impl MergedAlertManager {
    pub fn new(db: UltrosDb) -> Self {
        Self {
            db,
            pending: Mutex::new(HashMap::new()),
            cooldowns: Mutex::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
    pub async fn signal_arbitrage(
        self: &Arc<Self>,
        profile_id: i32,
        item_id: i32,
        hq: bool,
        signal: ArbitrageSignal,
    ) {
        let key = (profile_id, item_id, hq);

        // Enforce cooldown
        if self.is_on_cooldown(profile_id, item_id, hq).await {
            return;
        }

        let mut pending = self.pending.lock().unwrap();
        if let Some(entry) = pending.get_mut(&key) {
            entry.arbitrage = Some(signal);
        } else {
            let entry = PendingAlertEntry {
                arbitrage: Some(signal),
                crafting: None,
                spawned_at: Instant::now(),
            };
            pending.insert(key, entry);

            // Spawn dispatch timer
            let manager = self.clone();
            tokio::spawn(async move {
                sleep(Duration::from_secs(5)).await;
                manager.dispatch(profile_id, item_id, hq).await;
            });
        }
    }

    #[allow(dead_code)]
    pub async fn signal_crafting(
        self: &Arc<Self>,
        profile_id: i32,
        item_id: i32,
        hq: bool,
        signal: CraftingSignal,
    ) {
        let key = (profile_id, item_id, hq);

        // Enforce cooldown
        if self.is_on_cooldown(profile_id, item_id, hq).await {
            return;
        }

        let mut pending = self.pending.lock().unwrap();
        if let Some(entry) = pending.get_mut(&key) {
            entry.crafting = Some(signal);
        } else {
            let entry = PendingAlertEntry {
                arbitrage: None,
                crafting: Some(signal),
                spawned_at: Instant::now(),
            };
            pending.insert(key, entry);

            // Spawn dispatch timer
            let manager = self.clone();
            tokio::spawn(async move {
                sleep(Duration::from_secs(5)).await;
                manager.dispatch(profile_id, item_id, hq).await;
            });
        }
    }

    async fn is_on_cooldown(&self, profile_id: i32, item_id: i32, hq: bool) -> bool {
        let profile = match self.db.get_profile_by_id(profile_id).await {
            Ok(Some(p)) => p,
            _ => return false,
        };
        let cooldown_duration =
            Duration::from_secs(profile.alert_item_cooldown_minutes as u64 * 60);
        let cooldowns = self.cooldowns.lock().unwrap();
        if let Some(&last_sent) = cooldowns.get(&(profile_id, item_id, hq))
            && last_sent.elapsed() < cooldown_duration
        {
            return true;
        }
        false
    }

    async fn dispatch(self: &Arc<Self>, profile_id: i32, item_id: i32, hq: bool) {
        let entry = {
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&(profile_id, item_id, hq))
        };

        let entry = match entry {
            Some(e) => e,
            None => return,
        };

        // Fetch profile
        let profile = match self.db.get_profile_by_id(profile_id).await {
            Ok(Some(p)) => p,
            _ => return,
        };

        // Resolve item details
        let item_name = xiv_gen_db::data()
            .items
            .get(&xiv_gen::ItemId(item_id))
            .map(|i| i.name.clone())
            .unwrap_or_else(|| format!("Item #{}", item_id));

        let quality_str = if hq { "HQ" } else { "NQ" };

        let title = format!("Market Alert: {} ({})", item_name, quality_str);
        let mut body = String::new();

        let mut send_alert = false;

        if let Some(arb) = &entry.arbitrage {
            body.push_str(&format!(
                "**Arbitrage Flip:**\n• Source: {}\n• Destination: {}\n• Net Profit: **{} Gil**\n• Velocity: {:.2}\n• Cost: {} Gil\n• Quantity: {}\n• Over Budget: {}\n\n",
                arb.source_world, arb.dest_world, arb.net_profit, arb.velocity_score, arb.total_cost, arb.quantity, arb.over_budget
            ));
            send_alert = true;
        }

        if let Some(craft) = &entry.crafting {
            let flags_str = if craft.flags.is_empty() {
                "None".to_string()
            } else {
                craft.flags.join(", ")
            };
            body.push_str(&format!(
                "**Crafting Opportunity:**\n• Material Cost: **{} Gil**\n• Sell Price: **{} Gil**\n• Net Profit: **{} Gil**\n• Flags: {}\n\n",
                craft.material_cost, craft.sell_price, craft.net_profit, flags_str
            ));
            send_alert = true;
        }

        if !send_alert {
            return;
        }

        // Send notification
        let ctx_opt = crate::alerts::delivery::get_serenity_ctx();
        let mut success = false;

        match self.db.list_endpoints(profile.discord_user_id).await {
            Ok(endpoints) => {
                for endpoint in endpoints {
                    let delivered = if let Some(ctx) = &ctx_opt {
                        deliver_to_endpoint(&endpoint, &title, &body, &self.db, ctx).await
                    } else {
                        deliver_non_discord_endpoint(&endpoint, &title, &body, &self.db).await
                    };

                    if let Err(e) = delivered {
                        error!(
                            "Failed to send endpoint alert for profile {} endpoint {}: {}",
                            profile.id, endpoint.id, e
                        );
                    } else {
                        success = true;
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to load notification endpoints for profile {}: {}",
                    profile.id, e
                );
            }
        }

        if let Some(webhook_url) = &profile.alert_channel_webhook
            && !webhook_url.trim().is_empty()
        {
            if let Err(e) = send_webhook(webhook_url, &title, &body).await {
                error!(
                    "Failed to send webhook alert for profile {}: {}",
                    profile.id, e
                );
            } else {
                success = true;
            }
        }

        if profile.alert_channel_dm {
            if let Some(ctx) = ctx_opt {
                if let Err(e) = send_dm(profile.discord_user_id, &title, &body, &ctx).await {
                    error!("Failed to send DM alert for profile {}: {}", profile.id, e);
                } else {
                    success = true;
                }
            } else {
                error!(
                    "Serenity context not available to send DM alert for profile {}",
                    profile.id
                );
            }
        }

        if success {
            let mut cooldowns = self.cooldowns.lock().unwrap();
            cooldowns.insert((profile_id, item_id, hq), Instant::now());
        }
    }
}

static MERGED_ALERT_MANAGER: OnceLock<Arc<MergedAlertManager>> = OnceLock::new();

pub(crate) fn init_merged_alert_manager(db: UltrosDb) {
    let _ = MERGED_ALERT_MANAGER.set(Arc::new(MergedAlertManager::new(db)));
}

#[allow(dead_code)]
pub(crate) fn get_merged_alert_manager() -> Option<Arc<MergedAlertManager>> {
    MERGED_ALERT_MANAGER.get().cloned()
}
