use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "profile_arbitrage_settings")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_id: i32,
    pub min_net_profit: i64,
    pub velocity_threshold: f64,
    pub travel_cost_rate_per_min: i64,
    pub min_profit_total: i64,
    pub category_blocklist: Option<Json>,
    pub category_allowlist: Option<Json>,
    pub world_exclusion_list: Option<Json>,
    pub excluded_item_ids: Option<Json>,
    pub max_listing_age_hours: i32,
    pub show_stale_panel: bool,
    pub require_home_world_sell_target: bool,
    pub source_world_scope: String,
    pub max_price_jump_ratio: f64,
    pub min_recent_cluster_confirmations: i32,
    pub volatility_action: String,
    pub require_ask_confirmation: bool,
    pub max_ask_vs_sale_gap_percent: f64,
    pub preset_name: String,
    pub destination_world_scope: String,
    pub seller_world_ids: Option<Json>,
    pub weekly_velocity_threshold: f64,
    pub same_dc_travel_minutes: i32,
    pub cross_dc_travel_minutes: i32,
    pub reference_price_scope: String,
    pub sell_price_strategy: String,
    pub min_markdown_pct: f64,
    pub digest_format: String,
    pub digest_changed_only: bool,
    pub digest_max_clean: i32,
    pub digest_max_review: i32,
    pub digest_include_review: bool,
    pub digest_include_universalis_links: bool,
    pub digest_include_ultros_links: bool,
    pub table_grouping_strategy: String,
    pub table_max_rows_per_item: i32,
    pub table_include_same_dc_best: bool,
    pub table_show_theoretical: bool,
    pub alert_grouping_strategy: String,
    pub alert_max_rows_per_item: i32,
    pub alert_include_same_dc_best: bool,
    pub alert_show_theoretical: bool,
    pub alert_profit_improvement_threshold_gil: i64,
    pub alert_profit_improvement_threshold_pct: f64,
    pub alert_frequency_mode: String,
    pub alert_digest_interval_minutes: i32,
    pub alert_schedule_cron: Option<String>,
    pub alert_send_empty_digest: bool,
    pub alert_immediate_threshold_enabled: bool,
    pub alert_immediate_min_net_profit: i64,
    pub alert_immediate_min_markdown_pct: f64,
    pub alert_immediate_min_velocity: f64,
    pub alert_immediate_max_per_hour: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::player_profile::Entity",
        from = "Column::ProfileId",
        to = "super::player_profile::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    PlayerProfile,
}

impl Related<super::player_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PlayerProfile.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
