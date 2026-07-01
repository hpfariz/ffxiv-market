use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use ultros_db::UltrosDb;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct UniversalisTaxRates {
    #[serde(default)]
    limsa_lominsa: Option<i32>,
    #[serde(default)]
    gridania: Option<i32>,
    #[serde(default)]
    uldah: Option<i32>,
    #[serde(default)]
    ishgard: Option<i32>,
    #[serde(default)]
    kugane: Option<i32>,
    #[serde(default)]
    crystarium: Option<i32>,
    #[serde(default)]
    old_sharlayan: Option<i32>,
    #[serde(default)]
    radz_at_han: Option<i32>,
    #[serde(default)]
    tuliyollal: Option<i32>,
}

pub struct TaxRateWorker {
    db: UltrosDb,
    client: reqwest::Client,
}

impl TaxRateWorker {
    pub fn new(db: UltrosDb) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        Self { db, client }
    }

    pub fn start(self, token: CancellationToken) {
        tokio::spawn(async move {
            info!("Starting TaxRateWorker");
            loop {
                if token.is_cancelled() {
                    break;
                }

                if let Err(e) = self.refresh_tax_rates().await {
                    error!(error = ?e, "Failed to refresh tax rates");
                }

                // Sleep for 24 hours or until cancellation
                tokio::select! {
                    _ = sleep(Duration::from_secs(24 * 60 * 60)) => {}
                    _ = token.cancelled() => {
                        info!("TaxRateWorker cancelled");
                        break;
                    }
                }
            }
        });
    }

    #[instrument(skip(self))]
    async fn refresh_tax_rates(&self) -> Result<(), anyhow::Error> {
        let worlds = self.db.list_worlds().await?;
        info!("Refreshing tax rates for {} worlds", worlds.len());

        for world in worlds {
            // Respect rate limits: small delay between requests
            sleep(Duration::from_millis(150)).await;

            let url = format!(
                "https://universalis.app/api/v2/tax-rates?world={}",
                world.name
            );

            match self.client.get(&url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(rates) = resp.json::<UniversalisTaxRates>().await {
                            // Calculate average tax rate or default to 0.05
                            let mut sum = 0.0;
                            let mut count = 0;

                            let fields = [
                                rates.limsa_lominsa,
                                rates.gridania,
                                rates.uldah,
                                rates.ishgard,
                                rates.kugane,
                                rates.crystarium,
                                rates.old_sharlayan,
                                rates.radz_at_han,
                                rates.tuliyollal,
                            ];

                            for val in fields.into_iter().flatten() {
                                sum += val as f64;
                                count += 1;
                            }

                            let tax_rate = if count > 0 {
                                (sum / count as f64) / 100.0
                            } else {
                                0.05
                            };

                            self.db.upsert_tax_rate(world.id, tax_rate).await?;
                            info!(
                                "Updated tax rate for world {}: {:.2}%",
                                world.name,
                                tax_rate * 100.0
                            );
                        }
                    } else {
                        error!(
                            "Failed to fetch tax rates for world {}, status: {}",
                            world.name,
                            resp.status()
                        );
                    }
                }
                Err(e) => {
                    error!(error = ?e, "HTTP error fetching tax rates for world {}", world.name);
                }
            }
        }

        info!("Finished refreshing tax rates");
        Ok(())
    }
}
