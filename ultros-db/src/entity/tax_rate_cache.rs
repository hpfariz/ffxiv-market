use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "tax_rate_cache")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub world_id: i32,
    pub tax_rate: f64,
    pub fetched_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::world::Entity",
        from = "Column::WorldId",
        to = "super::world::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    World,
}

impl Related<super::world::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::World.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
