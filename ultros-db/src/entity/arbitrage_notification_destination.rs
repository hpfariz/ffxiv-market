use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "arbitrage_notification_destination")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_id: i32,
    #[sea_orm(primary_key, auto_increment = false)]
    pub endpoint_id: i32,
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
        belongs_to = "super::notification_endpoint::Entity",
        from = "Column::EndpointId",
        to = "super::notification_endpoint::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    NotificationEndpoint,
}

impl Related<super::player_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PlayerProfile.def()
    }
}

impl Related<super::notification_endpoint::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::NotificationEndpoint.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
