use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use ultros_db::{UltrosDb, entity::*, lists::ListError};

use crate::alerts::delivery::{
    DeliveryEmbed, DeliveryEmbedField, deliver_embeds_non_discord_endpoint,
    deliver_embeds_to_endpoint, get_serenity_ctx,
};
use crate::web::error::ApiError;
use crate::web::oauth::AuthDiscordUser;
use crate::worker::arbitrage_daemon::{ArbitrageScanStatus, ArbitrageScanStatusTracker};

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
    #[serde(default = "default_require_home_world_sell_target")]
    pub require_home_world_sell_target: bool,
    #[serde(default = "default_source_world_scope")]
    pub source_world_scope: String,
    #[serde(default = "default_max_price_jump_ratio")]
    pub max_price_jump_ratio: f64,
    #[serde(default = "default_min_recent_cluster_confirmations")]
    pub min_recent_cluster_confirmations: i32,
    #[serde(default = "default_volatility_action")]
    pub volatility_action: String,
    #[serde(default = "default_require_ask_confirmation")]
    pub require_ask_confirmation: bool,
    #[serde(default = "default_max_ask_vs_sale_gap_percent")]
    pub max_ask_vs_sale_gap_percent: f64,
    #[serde(default = "default_preset_name")]
    pub preset_name: String,
    #[serde(default = "default_destination_world_scope")]
    pub destination_world_scope: String,
    pub seller_world_ids: Option<serde_json::Value>,
    #[serde(default)]
    pub weekly_velocity_threshold: f64,
    #[serde(default = "default_same_dc_travel_minutes")]
    pub same_dc_travel_minutes: i32,
    #[serde(default = "default_cross_dc_travel_minutes")]
    pub cross_dc_travel_minutes: i32,
    #[serde(default = "default_reference_price_scope")]
    pub reference_price_scope: String,
    #[serde(default = "default_sell_price_strategy")]
    pub sell_price_strategy: String,
    #[serde(default)]
    pub min_markdown_pct: f64,
    #[serde(default = "default_digest_format")]
    pub digest_format: String,
    #[serde(default = "default_true")]
    pub digest_changed_only: bool,
    #[serde(default = "default_digest_max_clean")]
    pub digest_max_clean: i32,
    #[serde(default = "default_digest_max_review")]
    pub digest_max_review: i32,
    #[serde(default = "default_true")]
    pub digest_include_review: bool,
    #[serde(default = "default_true")]
    pub digest_include_universalis_links: bool,
    #[serde(default = "default_true")]
    pub digest_include_ultros_links: bool,
    #[serde(default = "default_grouping_strategy")]
    pub table_grouping_strategy: String,
    #[serde(default = "default_max_rows_per_item")]
    pub table_max_rows_per_item: i32,
    #[serde(default = "default_true")]
    pub table_include_same_dc_best: bool,
    #[serde(default)]
    pub table_show_theoretical: bool,
    #[serde(default = "default_grouping_strategy")]
    pub alert_grouping_strategy: String,
    #[serde(default = "default_max_rows_per_item")]
    pub alert_max_rows_per_item: i32,
    #[serde(default = "default_true")]
    pub alert_include_same_dc_best: bool,
    #[serde(default)]
    pub alert_show_theoretical: bool,
    #[serde(default = "default_profit_improvement_gil")]
    pub alert_profit_improvement_threshold_gil: i64,
    #[serde(default)]
    pub alert_profit_improvement_threshold_pct: f64,
    #[serde(default = "default_alert_frequency_mode")]
    pub alert_frequency_mode: String,
    #[serde(default = "default_alert_digest_interval_minutes")]
    pub alert_digest_interval_minutes: i32,
    pub alert_schedule_cron: Option<String>,
    #[serde(default)]
    pub alert_send_empty_digest: bool,
    #[serde(default = "default_true")]
    pub alert_immediate_threshold_enabled: bool,
    #[serde(default = "default_immediate_min_net_profit")]
    pub alert_immediate_min_net_profit: i64,
    #[serde(default)]
    pub alert_immediate_min_markdown_pct: f64,
    #[serde(default)]
    pub alert_immediate_min_velocity: f64,
    #[serde(default = "default_immediate_max_per_hour")]
    pub alert_immediate_max_per_hour: i32,
}

#[derive(Deserialize)]
pub struct ArbitrageDestinationsPayload {
    pub endpoint_ids: Vec<i32>,
}

#[derive(Deserialize)]
pub struct ApplyArbitragePresetPayload {
    pub preset_name: String,
}

#[derive(Serialize)]
pub struct ArbitrageDigestPreview {
    pub title: String,
    pub body: String,
    pub embeds: Vec<DeliveryEmbed>,
}

#[derive(Serialize)]
pub struct ArbitrageTestSendResponse {
    pub delivered: bool,
    pub attempted: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

#[derive(Serialize)]
pub struct ArbitrageDeliveryAttemptSummary {
    pub endpoint_id: Option<i32>,
    pub delivery_kind: String,
    pub success: bool,
    pub error_message: Option<String>,
    pub attempted_at: String,
}

#[derive(Serialize)]
pub struct ArbitrageAlertStatusResponse {
    pub scan: ArbitrageScanStatus,
    pub pending_digest_count: i64,
    pub last_digest_sent_at: Option<String>,
    pub last_immediate_sent_at: Option<String>,
    pub immediate_sent_count_window_start: Option<String>,
    pub immediate_sent_count: i32,
    pub next_digest_hint: Option<String>,
    pub recent_delivery_attempts: Vec<ArbitrageDeliveryAttemptSummary>,
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

fn default_require_home_world_sell_target() -> bool {
    true
}

fn default_source_world_scope() -> String {
    "SAME_DC".to_string()
}

fn default_max_price_jump_ratio() -> f64 {
    1.30
}

fn default_min_recent_cluster_confirmations() -> i32 {
    5
}

fn default_volatility_action() -> String {
    "DEMOTE_TO_REVIEW".to_string()
}

fn default_require_ask_confirmation() -> bool {
    true
}

fn default_max_ask_vs_sale_gap_percent() -> f64 {
    15.0
}

fn default_true() -> bool {
    true
}

fn default_preset_name() -> String {
    "CUSTOM".to_string()
}

fn default_destination_world_scope() -> String {
    "HOME_WORLD".to_string()
}

fn default_same_dc_travel_minutes() -> i32 {
    2
}

fn default_cross_dc_travel_minutes() -> i32 {
    8
}

fn default_reference_price_scope() -> String {
    "DESTINATION_DC".to_string()
}

fn default_sell_price_strategy() -> String {
    "LOWER_OF_ASK_AND_MEDIAN".to_string()
}

fn default_digest_format() -> String {
    "CARDS".to_string()
}

fn default_digest_max_clean() -> i32 {
    8
}

fn default_digest_max_review() -> i32 {
    4
}

fn default_grouping_strategy() -> String {
    "BEST_PLUS_SAME_DC".to_string()
}

fn default_max_rows_per_item() -> i32 {
    2
}

fn default_profit_improvement_gil() -> i64 {
    1
}

fn default_alert_frequency_mode() -> String {
    "DIGEST_INTERVAL".to_string()
}

fn default_alert_digest_interval_minutes() -> i32 {
    60
}

fn default_immediate_min_net_profit() -> i64 {
    500_000
}

fn default_immediate_max_per_hour() -> i32 {
    3
}

fn normalize_source_world_scope(scope: &str) -> anyhow::Result<String> {
    let normalized = scope.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "CURRENT_WORLD" | "SAME_DC" | "SAME_REGION" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid source_world_scope")),
    }
}

fn normalize_volatility_action(action: &str) -> anyhow::Result<String> {
    let normalized = action.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "SUPPRESS" | "DEMOTE_TO_REVIEW" | "ALERT_WITH_WARNING" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid volatility_action")),
    }
}

fn normalize_destination_world_scope(scope: &str) -> anyhow::Result<String> {
    let normalized = scope.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "HOME_WORLD" | "ACTIVE_DC" | "SAME_REGION" | "CUSTOM" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid destination_world_scope")),
    }
}

fn normalize_reference_price_scope(scope: &str) -> anyhow::Result<String> {
    let normalized = scope.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "DESTINATION_WORLD" | "DESTINATION_DC" | "ACTIVE_REGION" | "SOURCE_AND_DESTINATION" => {
            Ok(normalized)
        }
        _ => Err(anyhow::anyhow!("invalid reference_price_scope")),
    }
}

fn normalize_sell_price_strategy(strategy: &str) -> anyhow::Result<String> {
    let normalized = strategy.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "LOWER_OF_ASK_AND_MEDIAN" | "DESTINATION_LOW_ASK" | "MEDIAN_SALE" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid sell_price_strategy")),
    }
}

fn normalize_digest_format(format: &str) -> anyhow::Result<String> {
    let normalized = format.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "CARDS" | "COMPACT" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid digest_format")),
    }
}

fn normalize_grouping_strategy(strategy: &str) -> anyhow::Result<String> {
    let normalized = strategy.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "BEST_PLUS_SAME_DC" | "BEST_ONLY" | "ALL" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid grouping_strategy")),
    }
}

fn normalize_alert_frequency_mode(mode: &str) -> anyhow::Result<String> {
    let normalized = mode.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "IMMEDIATE" | "DIGEST_INTERVAL" | "SCANNER_COMPLETE" | "SCHEDULED" => Ok(normalized),
        _ => Err(anyhow::anyhow!("invalid alert_frequency_mode")),
    }
}

fn validate_alert_schedule(mode: &str, cron: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(cron) = cron
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        if mode == "SCHEDULED" {
            return Err(anyhow::anyhow!(
                "alert_schedule_cron is required for SCHEDULED mode"
            ));
        }
        return Ok(None);
    };

    if validate_cron_expression(&cron) {
        Ok(Some(cron))
    } else {
        Err(anyhow::anyhow!(
            "alert_schedule_cron must be a valid 5- or 6-field cron expression"
        ))
    }
}

fn validate_cron_expression(expr: &str) -> bool {
    let fields = expr.split_whitespace().collect::<Vec<_>>();
    match fields.as_slice() {
        [minute, hour, day, month, weekday] => {
            validate_cron_field(minute, 0, 59)
                && validate_cron_field(hour, 0, 23)
                && validate_cron_field(day, 1, 31)
                && validate_cron_field(month, 1, 12)
                && validate_cron_field(weekday, 0, 7)
        }
        [second, minute, hour, day, month, weekday] => {
            validate_cron_field(second, 0, 59)
                && validate_cron_field(minute, 0, 59)
                && validate_cron_field(hour, 0, 23)
                && validate_cron_field(day, 1, 31)
                && validate_cron_field(month, 1, 12)
                && validate_cron_field(weekday, 0, 7)
        }
        _ => false,
    }
}

fn validate_cron_field(field: &str, min: u32, max: u32) -> bool {
    !field.trim().is_empty()
        && field.split(',').all(|part| {
            let part = part.trim();
            if part.is_empty() {
                return false;
            }

            let (base, _step) = match part.split_once('/') {
                Some((base, step)) => {
                    let Some(step) = step.parse::<u32>().ok().filter(|step| *step > 0) else {
                        return false;
                    };
                    (base, step)
                }
                None => (part, 1),
            };

            if base.is_empty() {
                return false;
            }

            let range = if base == "*" {
                Some((min, max))
            } else if let Some((start, end)) = base.split_once('-') {
                match (start.parse::<u32>(), end.parse::<u32>()) {
                    (Ok(start), Ok(end)) => Some((start, end)),
                    _ => None,
                }
            } else {
                base.parse::<u32>().ok().map(|value| (value, value))
            };

            let Some((start, end)) = range else {
                return false;
            };
            start >= min && end <= max && start <= end
        })
}

fn validate_seller_world_ids(
    destination_world_scope: &str,
    seller_world_ids: Option<serde_json::Value>,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(value) = seller_world_ids else {
        if destination_world_scope == "CUSTOM" {
            return Err(anyhow::anyhow!(
                "seller_world_ids is required when destination_world_scope is CUSTOM"
            ));
        }
        return Ok(None);
    };

    let ids: Vec<i32> = serde_json::from_value(value)
        .map_err(|_| anyhow::anyhow!("seller_world_ids must be an array of world ids"))?;
    if ids.iter().any(|id| *id <= 0) {
        return Err(anyhow::anyhow!(
            "seller_world_ids must contain positive world ids"
        ));
    }
    if destination_world_scope == "CUSTOM" && ids.is_empty() {
        return Err(anyhow::anyhow!(
            "seller_world_ids cannot be empty when destination_world_scope is CUSTOM"
        ));
    }
    Ok((!ids.is_empty()).then(|| serde_json::json!(ids)))
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
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
    Json(payload): Json<ArbitrageSettingsPayload>,
) -> Result<Json<profile_arbitrage_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let source_world_scope = normalize_source_world_scope(&payload.source_world_scope)?;
    let volatility_action = normalize_volatility_action(&payload.volatility_action)?;
    let destination_world_scope =
        normalize_destination_world_scope(&payload.destination_world_scope)?;
    let reference_price_scope = normalize_reference_price_scope(&payload.reference_price_scope)?;
    let sell_price_strategy = normalize_sell_price_strategy(&payload.sell_price_strategy)?;
    let digest_format = normalize_digest_format(&payload.digest_format)?;
    let table_grouping_strategy = normalize_grouping_strategy(&payload.table_grouping_strategy)?;
    let alert_grouping_strategy = normalize_grouping_strategy(&payload.alert_grouping_strategy)?;
    let alert_frequency_mode = normalize_alert_frequency_mode(&payload.alert_frequency_mode)?;
    let alert_schedule_cron =
        validate_alert_schedule(&alert_frequency_mode, payload.alert_schedule_cron)?;
    let seller_world_ids =
        validate_seller_world_ids(&destination_world_scope, payload.seller_world_ids)?;

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
        require_home_world_sell_target: sea_orm::ActiveValue::Set(
            payload.require_home_world_sell_target,
        ),
        source_world_scope: sea_orm::ActiveValue::Set(source_world_scope),
        max_price_jump_ratio: sea_orm::ActiveValue::Set(payload.max_price_jump_ratio.max(1.0)),
        min_recent_cluster_confirmations: sea_orm::ActiveValue::Set(
            payload.min_recent_cluster_confirmations.max(1),
        ),
        volatility_action: sea_orm::ActiveValue::Set(volatility_action),
        require_ask_confirmation: sea_orm::ActiveValue::Set(payload.require_ask_confirmation),
        max_ask_vs_sale_gap_percent: sea_orm::ActiveValue::Set(
            payload.max_ask_vs_sale_gap_percent.max(0.0),
        ),
        preset_name: sea_orm::ActiveValue::Set(payload.preset_name.trim().to_string()),
        destination_world_scope: sea_orm::ActiveValue::Set(destination_world_scope),
        seller_world_ids: sea_orm::ActiveValue::Set(seller_world_ids),
        weekly_velocity_threshold: sea_orm::ActiveValue::Set(
            payload.weekly_velocity_threshold.max(0.0),
        ),
        same_dc_travel_minutes: sea_orm::ActiveValue::Set(payload.same_dc_travel_minutes.max(0)),
        cross_dc_travel_minutes: sea_orm::ActiveValue::Set(payload.cross_dc_travel_minutes.max(0)),
        reference_price_scope: sea_orm::ActiveValue::Set(reference_price_scope),
        sell_price_strategy: sea_orm::ActiveValue::Set(sell_price_strategy),
        min_markdown_pct: sea_orm::ActiveValue::Set(payload.min_markdown_pct.max(0.0)),
        digest_format: sea_orm::ActiveValue::Set(digest_format),
        digest_changed_only: sea_orm::ActiveValue::Set(payload.digest_changed_only),
        digest_max_clean: sea_orm::ActiveValue::Set(payload.digest_max_clean.clamp(1, 20)),
        digest_max_review: sea_orm::ActiveValue::Set(payload.digest_max_review.clamp(0, 20)),
        digest_include_review: sea_orm::ActiveValue::Set(payload.digest_include_review),
        digest_include_universalis_links: sea_orm::ActiveValue::Set(
            payload.digest_include_universalis_links,
        ),
        digest_include_ultros_links: sea_orm::ActiveValue::Set(payload.digest_include_ultros_links),
        table_grouping_strategy: sea_orm::ActiveValue::Set(table_grouping_strategy),
        table_max_rows_per_item: sea_orm::ActiveValue::Set(
            payload.table_max_rows_per_item.clamp(1, 20),
        ),
        table_include_same_dc_best: sea_orm::ActiveValue::Set(payload.table_include_same_dc_best),
        table_show_theoretical: sea_orm::ActiveValue::Set(payload.table_show_theoretical),
        alert_grouping_strategy: sea_orm::ActiveValue::Set(alert_grouping_strategy),
        alert_max_rows_per_item: sea_orm::ActiveValue::Set(
            payload.alert_max_rows_per_item.clamp(1, 20),
        ),
        alert_include_same_dc_best: sea_orm::ActiveValue::Set(payload.alert_include_same_dc_best),
        alert_show_theoretical: sea_orm::ActiveValue::Set(payload.alert_show_theoretical),
        alert_profit_improvement_threshold_gil: sea_orm::ActiveValue::Set(
            payload.alert_profit_improvement_threshold_gil.max(0),
        ),
        alert_profit_improvement_threshold_pct: sea_orm::ActiveValue::Set(
            payload.alert_profit_improvement_threshold_pct.max(0.0),
        ),
        alert_frequency_mode: sea_orm::ActiveValue::Set(alert_frequency_mode),
        alert_digest_interval_minutes: sea_orm::ActiveValue::Set(
            payload.alert_digest_interval_minutes.max(1),
        ),
        alert_schedule_cron: sea_orm::ActiveValue::Set(alert_schedule_cron),
        alert_send_empty_digest: sea_orm::ActiveValue::Set(payload.alert_send_empty_digest),
        alert_immediate_threshold_enabled: sea_orm::ActiveValue::Set(
            payload.alert_immediate_threshold_enabled,
        ),
        alert_immediate_min_net_profit: sea_orm::ActiveValue::Set(
            payload.alert_immediate_min_net_profit.max(0),
        ),
        alert_immediate_min_markdown_pct: sea_orm::ActiveValue::Set(
            payload.alert_immediate_min_markdown_pct.max(0.0),
        ),
        alert_immediate_min_velocity: sea_orm::ActiveValue::Set(
            payload.alert_immediate_min_velocity.max(0.0),
        ),
        alert_immediate_max_per_hour: sea_orm::ActiveValue::Set(
            payload.alert_immediate_max_per_hour.max(0),
        ),
    };

    let updated = db.update_arbitrage_settings(id, active_model).await?;
    arbitrage_status
        .mark_queued("Arbitrage settings changed; scanner queued")
        .await;
    arbitrage_trigger.notify_one();
    Ok(Json(updated))
}

pub async fn apply_arbitrage_preset(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_trigger): State<Arc<tokio::sync::Notify>>,
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
    Json(payload): Json<ApplyArbitragePresetPayload>,
) -> Result<Json<profile_arbitrage_settings::Model>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let preset = payload.preset_name.trim().to_ascii_uppercase();
    let active_model = arbitrage_preset_active_model(id, &preset)?;
    let updated = db.update_arbitrage_settings(id, active_model).await?;
    arbitrage_status
        .mark_queued(format!("Arbitrage {preset} preset applied; scanner queued"))
        .await;
    arbitrage_trigger.notify_one();
    Ok(Json(updated))
}

fn arbitrage_preset_active_model(
    profile_id: i32,
    preset: &str,
) -> anyhow::Result<profile_arbitrage_settings::ActiveModel> {
    let mut active = profile_arbitrage_settings::ActiveModel {
        profile_id: sea_orm::ActiveValue::Set(profile_id),
        preset_name: sea_orm::ActiveValue::Set(preset.to_string()),
        require_home_world_sell_target: sea_orm::ActiveValue::Set(true),
        destination_world_scope: sea_orm::ActiveValue::Set("HOME_WORLD".to_string()),
        source_world_scope: sea_orm::ActiveValue::Set("SAME_DC".to_string()),
        reference_price_scope: sea_orm::ActiveValue::Set("DESTINATION_DC".to_string()),
        sell_price_strategy: sea_orm::ActiveValue::Set("LOWER_OF_ASK_AND_MEDIAN".to_string()),
        volatility_action: sea_orm::ActiveValue::Set("DEMOTE_TO_REVIEW".to_string()),
        digest_format: sea_orm::ActiveValue::Set("CARDS".to_string()),
        digest_changed_only: sea_orm::ActiveValue::Set(true),
        digest_include_review: sea_orm::ActiveValue::Set(true),
        digest_include_universalis_links: sea_orm::ActiveValue::Set(true),
        digest_include_ultros_links: sea_orm::ActiveValue::Set(true),
        table_grouping_strategy: sea_orm::ActiveValue::Set("BEST_PLUS_SAME_DC".to_string()),
        table_max_rows_per_item: sea_orm::ActiveValue::Set(2),
        table_include_same_dc_best: sea_orm::ActiveValue::Set(true),
        table_show_theoretical: sea_orm::ActiveValue::Set(false),
        alert_grouping_strategy: sea_orm::ActiveValue::Set("BEST_PLUS_SAME_DC".to_string()),
        alert_max_rows_per_item: sea_orm::ActiveValue::Set(2),
        alert_include_same_dc_best: sea_orm::ActiveValue::Set(true),
        alert_show_theoretical: sea_orm::ActiveValue::Set(false),
        alert_frequency_mode: sea_orm::ActiveValue::Set("DIGEST_INTERVAL".to_string()),
        alert_immediate_threshold_enabled: sea_orm::ActiveValue::Set(true),
        same_dc_travel_minutes: sea_orm::ActiveValue::Set(2),
        cross_dc_travel_minutes: sea_orm::ActiveValue::Set(8),
        max_price_jump_ratio: sea_orm::ActiveValue::Set(1.30),
        min_recent_cluster_confirmations: sea_orm::ActiveValue::Set(5),
        require_ask_confirmation: sea_orm::ActiveValue::Set(true),
        max_ask_vs_sale_gap_percent: sea_orm::ActiveValue::Set(15.0),
        ..Default::default()
    };

    match preset {
        "CONSERVATIVE" => {
            active.min_net_profit = sea_orm::ActiveValue::Set(500_000);
            active.velocity_threshold = sea_orm::ActiveValue::Set(1.5);
            active.weekly_velocity_threshold = sea_orm::ActiveValue::Set(1.0);
            active.travel_cost_rate_per_min = sea_orm::ActiveValue::Set(10_000);
            active.min_profit_total = sea_orm::ActiveValue::Set(500_000);
            active.min_markdown_pct = sea_orm::ActiveValue::Set(20.0);
            active.digest_max_clean = sea_orm::ActiveValue::Set(5);
            active.digest_max_review = sea_orm::ActiveValue::Set(3);
            active.alert_digest_interval_minutes = sea_orm::ActiveValue::Set(120);
            active.alert_profit_improvement_threshold_gil = sea_orm::ActiveValue::Set(100_000);
            active.alert_profit_improvement_threshold_pct = sea_orm::ActiveValue::Set(10.0);
            active.alert_immediate_min_net_profit = sea_orm::ActiveValue::Set(1_000_000);
            active.alert_immediate_min_markdown_pct = sea_orm::ActiveValue::Set(25.0);
            active.alert_immediate_min_velocity = sea_orm::ActiveValue::Set(1.5);
            active.alert_immediate_max_per_hour = sea_orm::ActiveValue::Set(2);
        }
        "BALANCED" => {
            active.min_net_profit = sea_orm::ActiveValue::Set(100_000);
            active.velocity_threshold = sea_orm::ActiveValue::Set(1.0);
            active.weekly_velocity_threshold = sea_orm::ActiveValue::Set(0.4);
            active.travel_cost_rate_per_min = sea_orm::ActiveValue::Set(10_000);
            active.min_profit_total = sea_orm::ActiveValue::Set(100_000);
            active.min_markdown_pct = sea_orm::ActiveValue::Set(10.0);
            active.digest_max_clean = sea_orm::ActiveValue::Set(8);
            active.digest_max_review = sea_orm::ActiveValue::Set(4);
            active.alert_digest_interval_minutes = sea_orm::ActiveValue::Set(60);
            active.alert_profit_improvement_threshold_gil = sea_orm::ActiveValue::Set(1);
            active.alert_profit_improvement_threshold_pct = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_min_net_profit = sea_orm::ActiveValue::Set(500_000);
            active.alert_immediate_min_markdown_pct = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_min_velocity = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_max_per_hour = sea_orm::ActiveValue::Set(3);
        }
        "AGGRESSIVE" => {
            active.min_net_profit = sea_orm::ActiveValue::Set(50_000);
            active.velocity_threshold = sea_orm::ActiveValue::Set(0.5);
            active.weekly_velocity_threshold = sea_orm::ActiveValue::Set(0.0);
            active.travel_cost_rate_per_min = sea_orm::ActiveValue::Set(5_000);
            active.min_profit_total = sea_orm::ActiveValue::Set(50_000);
            active.min_markdown_pct = sea_orm::ActiveValue::Set(0.0);
            active.digest_max_clean = sea_orm::ActiveValue::Set(10);
            active.digest_max_review = sea_orm::ActiveValue::Set(6);
            active.alert_digest_interval_minutes = sea_orm::ActiveValue::Set(30);
            active.alert_profit_improvement_threshold_gil = sea_orm::ActiveValue::Set(1);
            active.alert_profit_improvement_threshold_pct = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_min_net_profit = sea_orm::ActiveValue::Set(250_000);
            active.alert_immediate_min_markdown_pct = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_min_velocity = sea_orm::ActiveValue::Set(0.0);
            active.alert_immediate_max_per_hour = sea_orm::ActiveValue::Set(5);
        }
        _ => return Err(anyhow::anyhow!("unknown arbitrage preset")),
    }

    Ok(active)
}

pub async fn get_arbitrage_destinations(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<Vec<i32>>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let endpoint_ids = db.get_arbitrage_destination_endpoint_ids(id).await?;
    Ok(Json(endpoint_ids))
}

pub async fn update_arbitrage_destinations(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
    Json(payload): Json<ArbitrageDestinationsPayload>,
) -> Result<Json<Vec<i32>>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let owned_endpoint_ids: std::collections::HashSet<i32> = db
        .list_endpoints(user.id as i64)
        .await?
        .into_iter()
        .map(|endpoint| endpoint.id)
        .collect();

    if !payload
        .endpoint_ids
        .iter()
        .all(|endpoint_id| owned_endpoint_ids.contains(endpoint_id))
    {
        return Err(anyhow::Error::new(ListError::Forbidden(
            "Arbitrage destination endpoint does not belong to this user",
        ))
        .into());
    }

    db.set_arbitrage_destination_endpoint_ids(id, &payload.endpoint_ids)
        .await?;
    Ok(Json(db.get_arbitrage_destination_endpoint_ids(id).await?))
}

pub async fn reset_arbitrage_delivery_state(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_trigger): State<Arc<tokio::sync::Notify>>,
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
) -> Result<Json<()>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    db.reset_arbitrage_delivery_state(id).await?;
    arbitrage_status
        .mark_queued("Arbitrage alert memory reset; scanner queued")
        .await;
    arbitrage_trigger.notify_one();
    Ok(Json(()))
}

pub async fn preview_arbitrage_digest(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<ArbitrageDigestPreview>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    let settings = db.get_arbitrage_settings(id).await?;
    let opportunities = db.get_arbitrage_opportunities(id).await?;
    Ok(Json(build_arbitrage_preview(
        &profile,
        &settings,
        &opportunities,
    )))
}

pub async fn test_arbitrage_digest(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    Path(id): Path<i32>,
) -> Result<Json<ArbitrageTestSendResponse>, ApiError> {
    let profile = check_profile_owner(&db, id, user.id as i64).await?;
    let endpoints = db
        .list_arbitrage_delivery_endpoints(profile.id, profile.discord_user_id)
        .await?;
    let title = format!("Arbitrage Test: {}", profile.display_name);
    let body = "Test arbitrage alert delivery. This does not mark any opportunity delivered.";
    let embeds = vec![DeliveryEmbed {
        title: "Test Arbitrage Deal Card".to_string(),
        description: "Selected endpoint delivery test for arbitrage alerts.".to_string(),
        color: 0x00c850,
        url: None,
        fields: vec![
            DeliveryEmbedField {
                name: "Profit potential".to_string(),
                value: "500,000 Gil".to_string(),
                inline: true,
            },
            DeliveryEmbedField {
                name: "Markdown".to_string(),
                value: "25.0% below reference".to_string(),
                inline: true,
            },
            DeliveryEmbedField {
                name: "Sale velocity".to_string(),
                value: "1.50 current / 2.0 per day".to_string(),
                inline: true,
            },
        ],
        footer: Some("Synthetic test card; no opportunity state changed".to_string()),
    }];
    let batch_hash = format!("test-{}", Utc::now().timestamp());
    let ctx_opt = get_serenity_ctx();
    let mut attempted = 0usize;
    let mut failed = 0usize;
    let mut errors = Vec::new();

    for endpoint in endpoints {
        attempted += 1;
        let result = if let Some(ctx) = &ctx_opt {
            deliver_embeds_to_endpoint(&endpoint, &title, body, &embeds, &db, ctx.as_ref()).await
        } else {
            deliver_embeds_non_discord_endpoint(&endpoint, &title, body, &embeds, &db).await
        };
        let (success, error_message) = match result {
            Ok(()) => (true, None),
            Err(e) => {
                let message = e.to_string();
                errors.push(format!("{}: {message}", endpoint.name));
                failed += 1;
                (false, Some(message))
            }
        };
        db.insert_arbitrage_delivery_attempt(arbitrage_delivery_attempt::Model {
            id: 0,
            profile_id: profile.id,
            endpoint_id: Some(endpoint.id),
            delivery_kind: "TEST".to_string(),
            snapshot_batch_hash: batch_hash.clone(),
            success,
            error_message,
            attempted_at: Utc::now().naive_utc(),
        })
        .await?;
    }

    Ok(Json(ArbitrageTestSendResponse {
        delivered: attempted > 0 && failed < attempted,
        attempted,
        failed,
        errors,
    }))
}

pub async fn get_arbitrage_alert_status(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
) -> Result<Json<ArbitrageAlertStatusResponse>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    let settings = db.get_arbitrage_settings(id).await?;
    let schedule = db.get_arbitrage_schedule_state(id).await?;
    let pending_digest_count = db.get_arbitrage_pending_digest_count(id).await?;
    let recent_delivery_attempts = db
        .get_recent_arbitrage_delivery_attempts(id, 10)
        .await?
        .into_iter()
        .map(|attempt| ArbitrageDeliveryAttemptSummary {
            endpoint_id: attempt.endpoint_id,
            delivery_kind: attempt.delivery_kind,
            success: attempt.success,
            error_message: attempt.error_message,
            attempted_at: attempt.attempted_at.to_string(),
        })
        .collect();

    Ok(Json(ArbitrageAlertStatusResponse {
        scan: arbitrage_status.get().await,
        pending_digest_count,
        last_digest_sent_at: schedule.last_digest_sent_at.map(|value| value.to_string()),
        last_immediate_sent_at: schedule
            .last_immediate_sent_at
            .map(|value| value.to_string()),
        immediate_sent_count_window_start: schedule
            .immediate_sent_count_window_start
            .map(|value| value.to_string()),
        immediate_sent_count: schedule.immediate_sent_count,
        next_digest_hint: next_digest_hint(&settings, &schedule),
        recent_delivery_attempts,
    }))
}

fn next_digest_hint(
    settings: &profile_arbitrage_settings::Model,
    schedule: &arbitrage_alert_schedule_state::Model,
) -> Option<String> {
    match settings.alert_frequency_mode.as_str() {
        "IMMEDIATE" => Some("Normal digests disabled; immediate threshold alerts only".to_string()),
        "SCANNER_COMPLETE" => Some("Next completed scan with eligible rows".to_string()),
        "SCHEDULED" => settings
            .alert_schedule_cron
            .as_ref()
            .map(|cron| format!("Next scan matching cron `{cron}`")),
        _ => schedule
            .last_digest_sent_at
            .map(|last| {
                (last
                    + chrono::Duration::minutes(
                        settings.alert_digest_interval_minutes.max(1) as i64
                    ))
                .to_string()
            })
            .or_else(|| Some("Next eligible scan".to_string())),
    }
}

fn build_arbitrage_preview(
    profile: &player_profile::Model,
    settings: &profile_arbitrage_settings::Model,
    opportunities: &[arbitrage_opportunity::Model],
) -> ArbitrageDigestPreview {
    let title = format!("Arbitrage Digest Preview: {}", profile.display_name);
    let max_rows = (settings.digest_max_clean.max(1) + settings.digest_max_review.max(0)) as usize;
    let mut rows = opportunities.to_vec();
    rows.sort_by(|a, b| b.net_profit.cmp(&a.net_profit));
    rows.truncate(max_rows.max(1));
    let embeds = rows
        .iter()
        .map(|opportunity| preview_embed_from_opportunity(opportunity, settings))
        .collect::<Vec<_>>();
    let body = format!(
        "{} preview rows. Changed-only and profit-improvement delivery rules still apply to real sends.",
        embeds.len()
    );
    ArbitrageDigestPreview {
        title,
        body,
        embeds,
    }
}

fn preview_embed_from_opportunity(
    opportunity: &arbitrage_opportunity::Model,
    settings: &profile_arbitrage_settings::Model,
) -> DeliveryEmbed {
    let item_name = xiv_gen_db::data()
        .items
        .get(&xiv_gen::ItemId(opportunity.item_id))
        .map(|item| item.name.as_str())
        .unwrap_or("Unknown Item");
    let quality = if opportunity.hq { "HQ" } else { "NQ" };
    let buy_price = if opportunity.quantity_available > 0 {
        opportunity.total_cost / opportunity.quantity_available as i64
    } else {
        0
    };

    DeliveryEmbed {
        title: format!(
            "Deal Alert: {item_name} ({quality} #{})",
            opportunity.item_id
        ),
        description: format!(
            "World #{} -> World #{} | {} | qty {}",
            opportunity.source_world_id,
            opportunity.dest_world_id,
            opportunity.execution_status,
            opportunity.quantity_available
        ),
        color: if opportunity.volatility_flag == "NONE" {
            0x00c850
        } else {
            0xff7a18
        },
        url: settings
            .digest_include_universalis_links
            .then(|| format!("https://universalis.app/market/{}", opportunity.item_id)),
        fields: vec![
            DeliveryEmbedField {
                name: "Profit potential".to_string(),
                value: format!("{} Gil", format_i64_for_preview(opportunity.net_profit)),
                inline: true,
            },
            DeliveryEmbedField {
                name: "Markdown".to_string(),
                value: opportunity
                    .markdown_pct
                    .map(|pct| format!("{pct:.1}%"))
                    .unwrap_or_else(|| "n/a".to_string()),
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
                name: "Source market".to_string(),
                value: format!("Min: {} Gil", format_i64_for_preview(buy_price)),
                inline: false,
            },
            DeliveryEmbedField {
                name: "Target market".to_string(),
                value: format!(
                    "Ask: {} Gil / Sell ref: {} Gil / Median sale: {} Gil",
                    format_i64_for_preview(opportunity.dest_low_ask_price as i64),
                    format_i64_for_preview(opportunity.selected_sell_reference_price as i64),
                    format_i64_for_preview(opportunity.median_sale_price as i64),
                ),
                inline: false,
            },
        ],
        footer: Some("Preview only; no delivery state changed".to_string()),
    }
}

fn format_i64_for_preview(value: i64) -> String {
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

pub async fn get_arbitrage_scan_status(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
) -> Result<Json<ArbitrageScanStatus>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    Ok(Json(arbitrage_status.get().await))
}

pub async fn trigger_arbitrage_scan(
    user: AuthDiscordUser,
    State(db): State<UltrosDb>,
    State(arbitrage_trigger): State<Arc<tokio::sync::Notify>>,
    State(arbitrage_status): State<ArbitrageScanStatusTracker>,
    Path(id): Path<i32>,
) -> Result<Json<ArbitrageScanStatus>, ApiError> {
    let _ = check_profile_owner(&db, id, user.id as i64).await?;
    arbitrage_status
        .mark_queued("Manual refresh requested; scanner queued")
        .await;
    arbitrage_trigger.notify_one();
    Ok(Json(arbitrage_status.get().await))
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
