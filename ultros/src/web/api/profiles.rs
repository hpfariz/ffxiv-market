use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use ultros_db::{UltrosDb, entity::*, lists::ListError};

use crate::web::error::ApiError;
use crate::web::oauth::AuthDiscordUser;

// Payload structs
#[derive(Deserialize)]
pub struct ProfileCreatePayload {
    pub display_name: String,
}

#[derive(Deserialize)]
pub struct ProfileUpdatePayload {
    pub display_name: Option<String>,
    pub home_world_id: Option<Option<i32>>,
    pub active_datacenter_id: Option<Option<i32>>,
    pub grand_company: Option<Option<String>>,
    pub gil_balance: Option<i64>,
    pub alert_channel_webhook: Option<Option<String>>,
    pub alert_channel_dm: Option<bool>,
    pub alert_item_cooldown_minutes: Option<i32>,
}

#[derive(Deserialize)]
pub struct ArbitrageSettingsPayload {
    pub min_net_profit: i64,
    pub velocity_threshold: f64,
    pub travel_cost_rate_per_min: i64,
    pub min_profit_total: i64,
    pub category_blocklist: Option<serde_json::Value>,
    pub category_allowlist: Option<serde_json::Value>,
    pub world_exclusion_list: Option<serde_json::Value>,
    pub excluded_item_ids: Option<serde_json::Value>,
    pub max_listing_age_hours: i32,
    pub show_stale_panel: bool,
}

#[derive(Deserialize)]
pub struct CraftingSettingsPayload {
    pub min_net_profit: i64,
    pub hq_only: bool,
    pub thresholds: Vec<profile_crafting_subcraft_threshold::Model>,
}

#[derive(Deserialize)]
pub struct GatheringSettingsPayload {
    pub show_all_levels: bool,
    pub class_filter: Option<String>,
    pub min_unit_price: Option<i64>,
}

#[derive(Deserialize)]
pub struct JobLevelsPayload {
    pub levels: Vec<profile_job_level::Model>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub healthy: bool,
    pub status: String,
    pub lag_seconds: Option<i64>,
}

#[derive(Serialize)]
pub struct ProfileSetupStatus {
    pub complete: bool,
    pub missing: Vec<String>,
}

#[derive(Deserialize)]
pub struct OpportunityQuery {
    #[serde(default)]
    pub show_all_levels: bool,
}

// Helpers
async fn check_profile_owner(
    db: &UltrosDb,
    profile_id: i32,
    user_id: i64,
) -> Result<player_profile::Model, ApiError> {
    let profile = db
        .get_profile_by_id(profile_id)
        .await?
        .ok_or_else(|| anyhow::Error::new(ListError::NotFound))?;
    if profile.discord_user_id != user_id {
        return Err(anyhow::Error::new(ListError::Forbidden(
            "Profile does not belong to this user",
        ))
        .into());
    }
    Ok(profile)
}

async fn profile_setup_status(
    db: &UltrosDb,
    profile: &player_profile::Model,
) -> Result<ProfileSetupStatus, ApiError> {
    let mut missing = Vec::new();

    if profile.home_world_id.is_none() {
        missing.push("home_world".to_string());
    }
    if profile.active_datacenter_id.is_none() {
        missing.push("active_datacenter".to_string());
    }
    if profile.gil_balance <= 0 {
        missing.push("gil_balance".to_string());
    }

    let arbitrage = db.get_arbitrage_settings(profile.id).await?;
    if arbitrage.min_net_profit <= 0 {
        missing.push("arbitrage_min_net_profit".to_string());
    }
    if arbitrage.velocity_threshold <= 0.0 {
        missing.push("arbitrage_velocity_threshold".to_string());
    }
    if arbitrage.min_profit_total <= 0 {
        missing.push("arbitrage_min_profit_total".to_string());
    }

    Ok(ProfileSetupStatus {
        complete: missing.is_empty(),
        missing,
    })
}

async fn require_profile_setup(
    db: &UltrosDb,
    profile: &player_profile::Model,
) -> Result<(), ApiError> {
    let status = profile_setup_status(db, profile).await?;
    if !status.complete {
        return Err(anyhow::Error::new(ListError::BadRequest(
            "Profile setup must be completed before recommendations are available",
        ))
        .into());
    }
    Ok(())
}

// Route Handlers
pub async fn list_profiles(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
) -> Result<Json<Vec<player_profile::Model>>, ApiError> {
    let profiles = db.get_profiles_for_user(user.id).await?;
    Ok(Json(profiles))
}

pub async fn create_profile(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Json(payload): Json<ProfileCreatePayload>,
) -> Result<Json<player_profile::Model>, ApiError> {
    let profile = db.create_profile(user.id, payload.display_name).await?;
    Ok(Json(profile))
}

pub async fn get_profile(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<player_profile::Model>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    Ok(Json(profile))
}

pub async fn get_profile_setup_status(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<ProfileSetupStatus>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    let status = profile_setup_status(&db, &profile).await?;
    Ok(Json(status))
}

pub async fn update_profile(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_trigger): State<Arc<tokio::sync::Notify>>,
    Path(id): Path<i32>,
    Json(payload): Json<ProfileUpdatePayload>,
) -> Result<Json<player_profile::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;

    let mut active_model = player_profile::ActiveModel {
        id: sea_orm::ActiveValue::Set(id),
        ..Default::default()
    };
    if let Some(val) = payload.display_name {
        active_model.display_name = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.home_world_id {
        active_model.home_world_id = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.active_datacenter_id {
        active_model.active_datacenter_id = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.grand_company {
        active_model.grand_company = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.gil_balance {
        active_model.gil_balance = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.alert_channel_webhook {
        active_model.alert_channel_webhook = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.alert_channel_dm {
        active_model.alert_channel_dm = sea_orm::ActiveValue::Set(val);
    }
    if let Some(val) = payload.alert_item_cooldown_minutes {
        active_model.alert_item_cooldown_minutes = sea_orm::ActiveValue::Set(val);
    }

    let updated = db.update_profile(id, active_model).await?;
    arbitrage_trigger.notify_one();
    Ok(Json(updated))
}

pub async fn delete_profile(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<()>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    db.delete_profile(id).await?;
    Ok(Json(()))
}

pub async fn get_job_levels(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<Vec<profile_job_level::Model>>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let levels = db.get_job_levels(id).await?;
    Ok(Json(levels))
}

pub async fn save_job_levels(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Json(payload): Json<JobLevelsPayload>,
) -> Result<Json<()>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    db.save_job_levels(id, payload.levels).await?;
    Ok(Json(()))
}

pub async fn get_arbitrage_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<profile_arbitrage_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let settings = db.get_arbitrage_settings(id).await?;
    Ok(Json(settings))
}

pub async fn update_arbitrage_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_trigger): State<Arc<tokio::sync::Notify>>,
    Path(id): Path<i32>,
    Json(payload): Json<ArbitrageSettingsPayload>,
) -> Result<Json<profile_arbitrage_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;

    let active_model = profile_arbitrage_settings::ActiveModel {
        profile_id: sea_orm::ActiveValue::Set(id),
        min_net_profit: sea_orm::ActiveValue::Set(payload.min_net_profit),
        velocity_threshold: sea_orm::ActiveValue::Set(payload.velocity_threshold),
        travel_cost_rate_per_min: sea_orm::ActiveValue::Set(payload.travel_cost_rate_per_min),
        min_profit_total: sea_orm::ActiveValue::Set(payload.min_profit_total),
        category_blocklist: sea_orm::ActiveValue::Set(payload.category_blocklist),
        category_allowlist: sea_orm::ActiveValue::Set(payload.category_allowlist),
        world_exclusion_list: sea_orm::ActiveValue::Set(payload.world_exclusion_list),
        excluded_item_ids: sea_orm::ActiveValue::Set(payload.excluded_item_ids),
        max_listing_age_hours: sea_orm::ActiveValue::Set(payload.max_listing_age_hours),
        show_stale_panel: sea_orm::ActiveValue::Set(payload.show_stale_panel),
    };

    let updated = db.update_arbitrage_settings(id, active_model).await?;
    arbitrage_trigger.notify_one();
    Ok(Json(updated))
}

pub async fn get_crafting_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<
    Json<(
        profile_crafting_settings::Model,
        Vec<profile_crafting_subcraft_threshold::Model>,
    )>,
    ApiError,
> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let settings = db.get_crafting_settings(id).await?;
    Ok(Json(settings))
}

pub async fn update_crafting_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Json(payload): Json<CraftingSettingsPayload>,
) -> Result<
    Json<(
        profile_crafting_settings::Model,
        Vec<profile_crafting_subcraft_threshold::Model>,
    )>,
    ApiError,
> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;

    let settings_active = profile_crafting_settings::ActiveModel {
        profile_id: sea_orm::ActiveValue::Set(id),
        min_net_profit: sea_orm::ActiveValue::Set(payload.min_net_profit),
        hq_only: sea_orm::ActiveValue::Set(payload.hq_only),
    };

    let updated = db
        .update_crafting_settings(id, settings_active, payload.thresholds)
        .await?;
    Ok(Json(updated))
}

pub async fn get_gathering_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<profile_gathering_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let settings = db.get_gathering_settings(id).await?;
    Ok(Json(settings))
}

pub async fn update_gathering_settings(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Json(payload): Json<GatheringSettingsPayload>,
) -> Result<Json<profile_gathering_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;

    let active_model = profile_gathering_settings::ActiveModel {
        profile_id: sea_orm::ActiveValue::Set(id),
        show_all_levels: sea_orm::ActiveValue::Set(payload.show_all_levels),
        class_filter: sea_orm::ActiveValue::Set(payload.class_filter),
        min_unit_price: sea_orm::ActiveValue::Set(payload.min_unit_price),
    };

    let updated = db.update_gathering_settings(id, active_model).await?;
    Ok(Json(updated))
}

pub async fn get_arbitrage_opportunities(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<Vec<arbitrage_opportunity::Model>>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    require_profile_setup(&db, &profile).await?;
    let opportunities = db.get_arbitrage_opportunities(id).await?;
    Ok(Json(opportunities))
}

pub async fn get_crafting_opportunities(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Query(query): Query<OpportunityQuery>,
) -> Result<Json<Vec<crate::worker::crafting_engine::CraftingOpportunity>>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    require_profile_setup(&db, &profile).await?;
    let engine = crate::worker::crafting_engine::CraftingEngine::new(db.clone());
    let opportunities = engine
        .compute_crafting_opportunities(id, query.show_all_levels)
        .await?;
    Ok(Json(opportunities))
}

pub async fn get_gathering_routes(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Query(query): Query<OpportunityQuery>,
) -> Result<
    Json<(
        Vec<crate::worker::gathering_optimizer::GatheringNodeDetail>,
        Vec<crate::worker::gathering_optimizer::TimedNodeDetail>,
    )>,
    ApiError,
> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    require_profile_setup(&db, &profile).await?;
    let optimizer = crate::worker::gathering_optimizer::GatheringOptimizer::new(db.clone());
    let routes = optimizer
        .optimize_gathering_routes(id, query.show_all_levels)
        .await?;
    Ok(Json(routes))
}

pub async fn get_health(
    State(health_monitor): State<Arc<crate::worker::ws_health_monitor::WsHealthMonitor>>,
) -> Result<Json<HealthResponse>, ApiError> {
    // We can query the health directly via a fast DB read for max active_listing timestamp:

    // We can query the health directly or rely on the broadcast's current event.
    // However, since it doesn't store the last event inside the struct directly,
    // we can run a quick check of the lag like in ws_health_monitor:
    use chrono::{NaiveDateTime, Utc};
    use sea_orm::{ColumnTrait, EntityTrait, FromQueryResult, QuerySelect};

    #[derive(FromQueryResult)]
    struct MaxTimestamp {
        max_time: Option<NaiveDateTime>,
    }

    let query_res = active_listing::Entity::find()
        .select_only()
        .column_as(active_listing::Column::Timestamp.max(), "max_time")
        .into_model::<MaxTimestamp>()
        .one(health_monitor.get_db().get_connection())
        .await?;

    let max_time = query_res.and_then(|r| r.max_time);

    let (healthy, status, lag_seconds) = if let Some(timestamp) = max_time {
        let now = Utc::now().naive_utc();
        let duration = now.signed_duration_since(timestamp);
        let lag = duration.num_seconds();
        if lag > 10 * 60 {
            (false, "Lagging".to_string(), Some(lag))
        } else {
            (true, "Healthy".to_string(), Some(lag))
        }
    } else {
        (true, "Healthy (No Data)".to_string(), None)
    };

    Ok(Json(HealthResponse {
        healthy,
        status,
        lag_seconds,
    }))
}

pub async fn get_events(
    State(health_monitor): State<Arc<crate::worker::ws_health_monitor::WsHealthMonitor>>,
) -> impl IntoResponse {
    let rx = health_monitor.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|res| match res {
        Ok(event) => {
            if let Ok(json) = serde_json::to_string(&event) {
                let event_res: Result<Event, std::convert::Infallible> =
                    Ok(Event::default().data(json));
                Some(event_res)
            } else {
                None
            }
        }
        Err(_) => None,
    });
    (
        [("x-accel-buffering", "no"), ("cache-control", "no-cache")],
        Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default()),
    )
}
