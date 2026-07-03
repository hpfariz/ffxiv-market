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
