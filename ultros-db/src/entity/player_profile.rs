use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "player_profile")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub discord_user_id: i64,
    pub display_name: String,
    pub home_world_id: Option<i32>,
    pub active_datacenter_id: Option<i32>,
    pub grand_company: Option<String>,
    pub gil_balance: i64,
    pub alert_channel_webhook: Option<String>,
    pub alert_channel_dm: bool,
    pub alert_item_cooldown_minutes: i32,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::discord_user::Entity",
        from = "Column::DiscordUserId",
        to = "super::discord_user::Column::Id",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    DiscordUser,
    #[sea_orm(has_many = "super::profile_job_level::Entity")]
    ProfileJobLevel,
    #[sea_orm(has_one = "super::profile_arbitrage_settings::Entity")]
    ProfileArbitrageSettings,
    #[sea_orm(has_one = "super::profile_crafting_settings::Entity")]
    ProfileCraftingSettings,
    #[sea_orm(has_many = "super::profile_crafting_subcraft_threshold::Entity")]
    ProfileCraftingSubcraftThreshold,
    #[sea_orm(has_one = "super::profile_gathering_settings::Entity")]
    ProfileGatheringSettings,
    #[sea_orm(has_many = "super::arbitrage_opportunity::Entity")]
    ArbitrageOpportunity,
}

impl Related<super::discord_user::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DiscordUser.def()
    }
}

impl Related<super::profile_job_level::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileJobLevel.def()
    }
}

impl Related<super::profile_arbitrage_settings::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileArbitrageSettings.def()
    }
}

impl Related<super::profile_crafting_settings::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileCraftingSettings.def()
    }
}

impl Related<super::profile_crafting_subcraft_threshold::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileCraftingSubcraftThreshold.def()
    }
}

impl Related<super::profile_gathering_settings::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileGatheringSettings.def()
    }
}

impl Related<super::arbitrage_opportunity::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ArbitrageOpportunity.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
