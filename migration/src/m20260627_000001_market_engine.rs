use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. player_profile table
        manager
            .create_table(
                Table::create()
                    .table(PlayerProfile::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PlayerProfile::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::DiscordUserId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::DisplayName)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(PlayerProfile::HomeWorldId).integer().null())
                    .col(
                        ColumnDef::new(PlayerProfile::ActiveDatacenterId)
                            .integer()
                            .null(),
                    )
                    .col(ColumnDef::new(PlayerProfile::GrandCompany).string().null())
                    .col(
                        ColumnDef::new(PlayerProfile::GilBalance)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::AlertChannelWebhook)
                            .string()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::AlertChannelDm)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::AlertItemCooldownMinutes)
                            .integer()
                            .not_null()
                            .default(30),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(PlayerProfile::UpdatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Foreign Key from PlayerProfile to DiscordUser
        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(PlayerProfile::Table, PlayerProfile::DiscordUserId)
                    .to(Alias::new("discord_user"), Alias::new("id"))
                    .to_owned(),
            )
            .await?;

        // 2. profile_job_level table
        manager
            .create_table(
                Table::create()
                    .table(ProfileJobLevel::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileJobLevel::ProfileId)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ProfileJobLevel::JobId).integer().not_null())
                    .col(ColumnDef::new(ProfileJobLevel::Level).integer().not_null())
                    .col(ColumnDef::new(ProfileJobLevel::Kind).string().not_null())
                    .primary_key(
                        Index::create()
                            .col(ProfileJobLevel::ProfileId)
                            .col(ProfileJobLevel::JobId),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(ProfileJobLevel::Table, ProfileJobLevel::ProfileId)
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // 3. profile_arbitrage_settings table
        manager
            .create_table(
                Table::create()
                    .table(ProfileArbitrageSettings::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::ProfileId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::MinNetProfit)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::VelocityThreshold)
                            .double()
                            .not_null()
                            .default(0.0),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::TravelCostRatePerMin)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::MinProfitTotal)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::CategoryBlocklist)
                            .json()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::CategoryAllowlist)
                            .json()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::WorldExclusionList)
                            .json()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::ExcludedItemIds)
                            .json()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::MaxListingAgeHours)
                            .integer()
                            .not_null()
                            .default(4),
                    )
                    .col(
                        ColumnDef::new(ProfileArbitrageSettings::ShowStalePanel)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(
                        ProfileArbitrageSettings::Table,
                        ProfileArbitrageSettings::ProfileId,
                    )
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // 4. profile_crafting_settings table
        manager
            .create_table(
                Table::create()
                    .table(ProfileCraftingSettings::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileCraftingSettings::ProfileId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProfileCraftingSettings::MinNetProfit)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(ProfileCraftingSettings::HqOnly)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(
                        ProfileCraftingSettings::Table,
                        ProfileCraftingSettings::ProfileId,
                    )
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // 5. profile_crafting_subcraft_threshold table
        manager
            .create_table(
                Table::create()
                    .table(ProfileCraftingSubcraftThreshold::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileCraftingSubcraftThreshold::ProfileId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProfileCraftingSubcraftThreshold::CraftingClassId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ProfileCraftingSubcraftThreshold::SavingsPctThreshold)
                            .double()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileCraftingSubcraftThreshold::SavingsGilThreshold)
                            .big_integer()
                            .null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(ProfileCraftingSubcraftThreshold::ProfileId)
                            .col(ProfileCraftingSubcraftThreshold::CraftingClassId),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(
                        ProfileCraftingSubcraftThreshold::Table,
                        ProfileCraftingSubcraftThreshold::ProfileId,
                    )
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // 6. profile_gathering_settings table
        manager
            .create_table(
                Table::create()
                    .table(ProfileGatheringSettings::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileGatheringSettings::ProfileId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ProfileGatheringSettings::ShowAllLevels)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(ProfileGatheringSettings::ClassFilter)
                            .string()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ProfileGatheringSettings::MinUnitPrice)
                            .big_integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(
                        ProfileGatheringSettings::Table,
                        ProfileGatheringSettings::ProfileId,
                    )
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // 7. tax_rate_cache table
        manager
            .create_table(
                Table::create()
                    .table(TaxRateCache::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TaxRateCache::WorldId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TaxRateCache::TaxRate).double().not_null())
                    .col(
                        ColumnDef::new(TaxRateCache::FetchedAt)
                            .date_time()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(TaxRateCache::Table, TaxRateCache::WorldId)
                    .to(Alias::new("world"), Alias::new("id"))
                    .to_owned(),
            )
            .await?;

        // 8. arbitrage_opportunity table
        manager
            .create_table(
                Table::create()
                    .table(ArbitrageOpportunity::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::ProfileId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::ItemId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::Hq)
                            .boolean()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::SourceWorldId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::DestWorldId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::GrossProfit)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::NetProfit)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::VelocityScore)
                            .double()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::ListingAgeSeconds)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::TotalCost)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::QuantityAvailable)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::OverBudget)
                            .boolean()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ArbitrageOpportunity::ComputedAt)
                            .date_time()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_foreign_key(
                ForeignKey::create()
                    .on_delete(ForeignKeyAction::Cascade)
                    .from(ArbitrageOpportunity::Table, ArbitrageOpportunity::ProfileId)
                    .to(PlayerProfile::Table, PlayerProfile::Id)
                    .to_owned(),
            )
            .await?;

        // Index on (profile_id, net_profit DESC)
        manager
            .create_index(
                Index::create()
                    .name("idx_arbitrage_opp_profile_net_profit")
                    .table(ArbitrageOpportunity::Table)
                    .col(ArbitrageOpportunity::ProfileId)
                    .col(ArbitrageOpportunity::NetProfit)
                    .to_owned(),
            )
            .await?;

        // 9. Add indexes to active_listing for performance (Gate 0)
        manager
            .create_index(
                Index::create()
                    .name("idx_active_listing_item_world_timestamp")
                    .table(Alias::new("active_listing"))
                    .col(Alias::new("item_id"))
                    .col(Alias::new("world_id"))
                    .col(Alias::new("timestamp"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_active_listing_item_world_timestamp")
                    .table(Alias::new("active_listing"))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ArbitrageOpportunity::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(TaxRateCache::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ProfileGatheringSettings::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ProfileCraftingSubcraftThreshold::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ProfileCraftingSettings::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ProfileArbitrageSettings::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(ProfileJobLevel::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(PlayerProfile::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum PlayerProfile {
    Table,
    Id,
    DiscordUserId,
    DisplayName,
    HomeWorldId,
    ActiveDatacenterId,
    GrandCompany,
    GilBalance,
    AlertChannelWebhook,
    AlertChannelDm,
    AlertItemCooldownMinutes,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
enum ProfileJobLevel {
    Table,
    ProfileId,
    JobId,
    Level,
    Kind,
}

#[derive(Iden)]
enum ProfileArbitrageSettings {
    Table,
    ProfileId,
    MinNetProfit,
    VelocityThreshold,
    TravelCostRatePerMin,
    MinProfitTotal,
    CategoryBlocklist,
    CategoryAllowlist,
    WorldExclusionList,
    ExcludedItemIds,
    MaxListingAgeHours,
    ShowStalePanel,
}

#[derive(Iden)]
enum ProfileCraftingSettings {
    Table,
    ProfileId,
    MinNetProfit,
    HqOnly,
}

#[derive(Iden)]
enum ProfileCraftingSubcraftThreshold {
    Table,
    ProfileId,
    CraftingClassId,
    SavingsPctThreshold,
    SavingsGilThreshold,
}

#[derive(Iden)]
enum ProfileGatheringSettings {
    Table,
    ProfileId,
    ShowAllLevels,
    ClassFilter,
    MinUnitPrice,
}

#[derive(Iden)]
enum TaxRateCache {
    Table,
    WorldId,
    TaxRate,
    FetchedAt,
}

#[derive(Iden)]
enum ArbitrageOpportunity {
    Table,
    Id,
    ProfileId,
    ItemId,
    Hq,
    SourceWorldId,
    DestWorldId,
    GrossProfit,
    NetProfit,
    VelocityScore,
    ListingAgeSeconds,
    TotalCost,
    QuantityAvailable,
    OverBudget,
    ComputedAt,
}
