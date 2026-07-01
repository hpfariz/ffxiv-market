use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "profile_crafting_subcraft_threshold")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_id: i32,
    #[sea_orm(primary_key, auto_increment = false)]
    pub crafting_class_id: i32,
    pub savings_pct_threshold: Option<f64>,
    pub savings_gil_threshold: Option<i64>,
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
