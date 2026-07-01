use chrono::{NaiveDateTime, Utc};
use sea_orm::{ColumnTrait, EntityTrait, FromQueryResult, QuerySelect};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};
use ultros_db::UltrosDb;
use ultros_db::entity::active_listing;

#[derive(Clone, Debug, serde::Serialize)]
pub enum FeedEvent {
    Healthy,
    Lagging { lag_seconds: i64 },
    Disconnected,
}

pub struct WsHealthMonitor {
    db: UltrosDb,
    event_tx: broadcast::Sender<FeedEvent>,
    is_socket_connected: std::sync::atomic::AtomicBool,
}

#[derive(FromQueryResult)]
struct MaxTimestamp {
    max_time: Option<NaiveDateTime>,
}

impl WsHealthMonitor {
    pub fn new(db: UltrosDb) -> (Arc<Self>, broadcast::Receiver<FeedEvent>) {
        let (event_tx, event_rx) = broadcast::channel(16);
        let monitor = Arc::new(Self {
            db,
            event_tx,
            is_socket_connected: std::sync::atomic::AtomicBool::new(true),
        });
        (monitor, event_rx)
    }

    pub fn set_connected(&self, connected: bool) {
        self.is_socket_connected
            .store(connected, std::sync::atomic::Ordering::Relaxed);
        let event = if connected {
            FeedEvent::Healthy
        } else {
            FeedEvent::Disconnected
        };
        let _ = self.event_tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FeedEvent> {
        self.event_tx.subscribe()
    }

    pub fn get_db(&self) -> &UltrosDb {
        &self.db
    }

    pub fn start(self: Arc<Self>, token: CancellationToken) {
        let this = self.clone();
        tokio::spawn(async move {
            info!("Starting WsHealthMonitor");
            loop {
                if token.is_cancelled() {
                    break;
                }

                if let Err(e) = this.check_health().await {
                    error!(error = ?e, "Error checking WebSocket health");
                }

                tokio::select! {
                    _ = sleep(Duration::from_secs(60)) => {}
                    _ = token.cancelled() => {
                        info!("WsHealthMonitor cancelled");
                        break;
                    }
                }
            }
        });
    }

    #[instrument(skip(self))]
    async fn check_health(&self) -> Result<(), anyhow::Error> {
        // If the socket itself is disconnected, report that immediately
        if !self
            .is_socket_connected
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let _ = self.event_tx.send(FeedEvent::Disconnected);
            return Ok(());
        }

        // Query the database for the most recent active listing timestamp
        let query_res = active_listing::Entity::find()
            .select_only()
            .column_as(active_listing::Column::Timestamp.max(), "max_time")
            .into_model::<MaxTimestamp>()
            .one(self.db.get_connection())
            .await?;

        let max_time = query_res.and_then(|r| r.max_time);

        if let Some(timestamp) = max_time {
            let now = Utc::now().naive_utc();
            let duration = now.signed_duration_since(timestamp);
            let lag_seconds = duration.num_seconds();

            info!("WebSocket lag check: {} seconds", lag_seconds);

            if lag_seconds > 10 * 60 {
                // Lag exceeds 10 minutes
                let _ = self.event_tx.send(FeedEvent::Lagging { lag_seconds });
            } else {
                let _ = self.event_tx.send(FeedEvent::Healthy);
            }
        } else {
            // No listings exist in database, treat as healthy but warn
            info!("No active listings found in database for lag check");
            let _ = self.event_tx.send(FeedEvent::Healthy);
        }

        Ok(())
    }
}
