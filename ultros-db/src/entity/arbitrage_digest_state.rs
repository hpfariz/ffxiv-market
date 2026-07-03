use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "arbitrage_digest_state")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub profile_id: i32,
    pub item_id: i32,
    pub hq: bool,
    pub source_world_id: i32,
    pub dest_world_id: i32,
    pub snapshot_hash: String,
    pub source_price: i32,
    pub dest_price: i32,
    pub quantity_available: i32,
    pub net_profit: i64,
    pub volatility_flag: String,
    pub latest_sale_timestamp: Option<DateTime>,
    pub units_sold_48h: i64,
    pub units_sold_7d: i64,
    pub median_sale_price: i32,
    pub recent_cluster_avg_price: Option<f64>,
    pub prior_cluster_avg_price: Option<f64>,
    pub weekly_avg_velocity: f64,
    pub delivered_at: DateTime,
    pub created_at: DateTime,
    pub updated_at: DateTime,
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
