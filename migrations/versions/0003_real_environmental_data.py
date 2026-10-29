"""Add real hydrology/weather provenance and analytics."""

import sqlalchemy as sa
from alembic import op

revision = "0003_real_data"
down_revision = "0002_incidents"
branch_labels = None
depends_on = None


def _columns(table_name: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    additions = {
        "ingestion_runs": (
            sa.Column("request_key", sa.String(240), nullable=True),
            sa.Column("request_parameters", sa.JSON(), nullable=True),
            sa.Column("accepted_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        ),
        "river_observations": (
            sa.Column("measurement_type", sa.String(24), nullable=True),
            sa.Column("source_value", sa.Float(), nullable=True),
            sa.Column("source_unit", sa.String(40), nullable=True),
            sa.Column("measurement_reference", sa.String(80), nullable=True),
        ),
        "weather_observations": (
            sa.Column("relative_humidity_percent", sa.Float(), nullable=True),
            sa.Column("wind_speed_m_s", sa.Float(), nullable=True),
            sa.Column("source_model", sa.String(80), nullable=True),
            sa.Column("source_units", sa.JSON(), nullable=True),
        ),
    }
    for table, columns in additions.items():
        existing = _columns(table)
        missing = [column for column in columns if column.name not in existing]
        if missing:
            with op.batch_alter_table(table) as batch:
                for column in missing:
                    batch.add_column(column)

    indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("ingestion_runs")}
    if "ix_ingestion_runs_request_key" not in indexes:
        op.create_index(
            "ix_ingestion_runs_request_key", "ingestion_runs", ["request_key"], unique=False
        )

    op.execute("DROP VIEW IF EXISTS gold_daily_observations")
    op.execute(
        """
        CREATE VIEW gold_daily_observations AS
        SELECT DATE(r.event_time) AS observation_date, 'river' AS domain,
               r.station_id AS location, COUNT(*) AS observation_count,
               AVG(r.water_level_m) AS average_primary,
               AVG(r.discharge_m3_s) AS average_secondary
          FROM river_observations r
          JOIN data_sources s ON s.id = r.source_id
         WHERE r.quality_status = 'valid' AND s.provenance = 'synthetic'
         GROUP BY DATE(r.event_time), r.station_id
        UNION ALL
        SELECT DATE(w.event_time), 'weather', w.station_id, COUNT(*),
               AVG(w.temperature_c), AVG(w.precipitation_mm)
          FROM weather_observations w
          JOIN data_sources s ON s.id = w.source_id
         WHERE w.quality_status = 'valid' AND s.provenance = 'synthetic'
         GROUP BY DATE(w.event_time), w.station_id
        UNION ALL
        SELECT DATE(e.event_time), 'energy', e.region, COUNT(*),
               AVG(e.generation_mw), AVG(e.price_eur_mwh)
          FROM energy_observations e
          JOIN data_sources s ON s.id = e.source_id
         WHERE e.quality_status = 'valid' AND s.provenance = 'synthetic'
         GROUP BY DATE(e.event_time), e.region
        """
    )
    op.execute(
        """
        CREATE VIEW gold_real_river_daily AS
        SELECT DATE(r.event_time) AS observation_date, r.station_id AS location,
               r.measurement_type, r.source_unit, COUNT(*) AS observation_count,
               AVG(r.water_level_m) AS average_water_level_m,
               MIN(r.water_level_m) AS minimum_water_level_m,
               MAX(r.water_level_m) AS maximum_water_level_m,
               AVG(r.discharge_m3_s) AS average_discharge_m3_s,
               MIN(r.discharge_m3_s) AS minimum_discharge_m3_s,
               MAX(r.discharge_m3_s) AS maximum_discharge_m3_s,
               MAX(r.event_time) AS latest_event_time
          FROM river_observations r
          JOIN data_sources s ON s.id = r.source_id
         WHERE r.quality_status = 'valid' AND s.provenance = 'real'
         GROUP BY DATE(r.event_time), r.station_id, r.measurement_type, r.source_unit
        """
    )
    op.execute(
        """
        CREATE VIEW gold_real_weather_hourly AS
        SELECT w.event_time, w.station_id AS location, w.temperature_c,
               w.precipitation_mm, w.relative_humidity_percent, w.wind_speed_m_s,
               w.source_model, w.source_units, w.quality_status
          FROM weather_observations w
          JOIN data_sources s ON s.id = w.source_id
         WHERE s.provenance = 'real'
        """
    )
    op.execute(
        """
        CREATE VIEW gold_real_weather_daily AS
        SELECT DATE(w.event_time) AS observation_date, w.station_id AS location,
               COUNT(*) AS observation_count,
               COUNT(w.temperature_c) AS temperature_observation_count,
               AVG(w.temperature_c) AS average_temperature_c,
               SUM(w.precipitation_mm) AS precipitation_total_mm,
               COUNT(w.relative_humidity_percent) AS humidity_observation_count,
               AVG(w.relative_humidity_percent) AS average_relative_humidity_percent,
               COUNT(w.wind_speed_m_s) AS wind_observation_count,
               AVG(w.wind_speed_m_s) AS average_wind_speed_m_s,
               MAX(w.event_time) AS latest_event_time
          FROM weather_observations w
          JOIN data_sources s ON s.id = w.source_id
         WHERE w.quality_status = 'valid' AND s.provenance = 'real'
         GROUP BY DATE(w.event_time), w.station_id
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold_real_weather_daily")
    op.execute("DROP VIEW IF EXISTS gold_real_weather_hourly")
    op.execute("DROP VIEW IF EXISTS gold_real_river_daily")
    op.execute("DROP VIEW IF EXISTS gold_daily_observations")
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
    op.drop_index("ix_ingestion_runs_request_key", table_name="ingestion_runs")
