use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_opportunity RENAME COLUMN units_sold48h TO units_sold_48h",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_opportunity RENAME COLUMN units_sold7d TO units_sold_7d",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_digest_state RENAME COLUMN units_sold48h TO units_sold_48h",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_digest_state RENAME COLUMN units_sold7d TO units_sold_7d",
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_digest_state RENAME COLUMN units_sold_7d TO units_sold7d",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_digest_state RENAME COLUMN units_sold_48h TO units_sold48h",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_opportunity RENAME COLUMN units_sold_7d TO units_sold7d",
            )
            .await?;
        manager
            .get_connection()
            .execute_unprepared(
                "ALTER TABLE arbitrage_opportunity RENAME COLUMN units_sold_48h TO units_sold48h",
            )
            .await?;

        Ok(())
    }
}
