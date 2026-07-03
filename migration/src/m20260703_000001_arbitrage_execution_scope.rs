use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(ProfileArbitrageSettings::Table)
                    .add_column(
                        ColumnDef::new(ProfileArbitrageSettings::RequireHomeWorldSellTarget)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .add_column(
                        ColumnDef::new(ProfileArbitrageSettings::SourceWorldScope)
                            .string()
                            .not_null()
                            .default("SAME_DC"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(ArbitrageOpportunity::Table)
                    .add_column(
                        ColumnDef::new(ArbitrageOpportunity::TravelTier)
                            .string()
                            .not_null()
                            .default("SAME_DC_VISIT"),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(ArbitrageOpportunity::Table)
                    .drop_column(ArbitrageOpportunity::TravelTier)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(ProfileArbitrageSettings::Table)
                    .drop_column(ProfileArbitrageSettings::SourceWorldScope)
                    .drop_column(ProfileArbitrageSettings::RequireHomeWorldSellTarget)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum ProfileArbitrageSettings {
    Table,
    RequireHomeWorldSellTarget,
    SourceWorldScope,
}

#[derive(Iden)]
enum ArbitrageOpportunity {
    Table,
    TravelTier,
}
