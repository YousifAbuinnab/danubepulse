"""Create initial operational and analytics schema."""

from alembic import op

from danubepulse.models import Base

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)
    op.execute(
        """
        CREATE VIEW gold_daily_observations AS
        SELECT DATE(event_time) AS observation_date, 'river' AS domain, station_id AS location,
               COUNT(*) AS observation_count, AVG(water_level_m) AS average_primary,
               AVG(discharge_m3_s) AS average_secondary
          FROM river_observations WHERE quality_status = 'valid'
         GROUP BY DATE(event_time), station_id
        UNION ALL
        SELECT DATE(event_time), 'weather', station_id, COUNT(*), AVG(temperature_c),
               AVG(precipitation_mm)
          FROM weather_observations WHERE quality_status = 'valid'
         GROUP BY DATE(event_time), station_id
        UNION ALL
        SELECT DATE(event_time), 'energy', region, COUNT(*), AVG(generation_mw),
               AVG(price_eur_mwh)
          FROM energy_observations WHERE quality_status = 'valid'
         GROUP BY DATE(event_time), region
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold_daily_observations")
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
