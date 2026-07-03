use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "arbitrage_opportunity")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub profile_id: i32,
    pub item_id: i32,
    pub hq: bool,
    pub source_world_id: i32,
    pub dest_world_id: i32,
    pub gross_profit: i64,
    pub net_profit: i64,
    pub velocity_score: f64,
    pub listing_age_seconds: i64,
    pub total_cost: i64,
    pub quantity_available: i32,
    pub over_budget: bool,
    pub travel_tier: String,
    pub computed_at: DateTime,
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
    #[sea_orm(
        belongs_to = "super::world::Entity",
        from = "Column::SourceWorldId",
        to = "super::world::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    SourceWorld,
    #[sea_orm(
        belongs_to = "super::world::Entity",
        from = "Column::DestWorldId",
        to = "super::world::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    DestWorld,
}

impl Related<super::player_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PlayerProfile.def()
    }
}

// Custom related bindings for world (source and destination)
// Note: Normally Related<World> is implemented, but since there are two, we can implement them manually or just define the relations.

impl ActiveModelBehavior for ActiveModel {}
