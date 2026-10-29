"""Separate Austrian A65 day-ahead demand forecasts from generation and prices."""

import sqlalchemy as sa
from alembic import op

revision = "0007_load_forecast"
down_revision = "0006_energy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("energy_observations")}
    if "load_forecast_mw" not in columns:
        op.add_column("energy_observations", sa.Column("load_forecast_mw", sa.Float()))
    day = (
        "(e.event_time AT TIME ZONE 'UTC')::date"
        if op.get_bind().dialect.name == "postgresql"
        else "DATE(e.event_time)"
    )
    op.execute(f"""
        CREATE VIEW gold_real_load_forecast_daily AS
        SELECT {day} AS observation_date, e.region, e.metric, e.production_type,
               COUNT(*) AS observation_count, SUM(e.duration_hours) AS coverage_hours,
               SUM(e.load_forecast_mw * e.duration_hours) AS forecast_demand_mwh,
               SUM(e.load_forecast_mw * e.duration_hours) /
                   NULLIF(SUM(e.duration_hours), 0) AS average_load_forecast_mw,
               MIN(e.load_forecast_mw) AS minimum_load_forecast_mw,
               MAX(e.load_forecast_mw) AS maximum_load_forecast_mw,
               MAX(e.event_time) AS latest_event_time
        FROM energy_observations e JOIN data_sources s ON s.id=e.source_id
        WHERE s.provenance='real' AND e.source_id='entsoe-load-forecast'
              AND e.metric='load_forecast' AND e.quality_status='valid'
        GROUP BY {day}, e.region, e.metric, e.production_type
    """)
    # Keep the existing view's columns and legacy semantics, excluding the new product.
    op.execute("DROP VIEW gold_real_energy_daily")
    op.execute(f"""
        CREATE VIEW gold_real_energy_daily AS
        SELECT {day} AS observation_date, e.region, e.metric, e.production_type,
               COUNT(*) AS observation_count,
               SUM(e.duration_hours) AS coverage_hours,
               SUM(CASE WHEN e.generation_mw IS NOT NULL THEN e.duration_hours ELSE 0 END)
                   AS generation_coverage_hours,
               SUM(CASE WHEN e.price_eur_mwh IS NOT NULL THEN e.duration_hours ELSE 0 END)
                   AS price_coverage_hours,
               SUM(e.generation_mw * e.duration_hours) AS generation_mwh,
               SUM(e.generation_mw * e.duration_hours) /
                   NULLIF(SUM(CASE WHEN e.generation_mw IS NOT NULL
                                  THEN e.duration_hours ELSE 0 END), 0) AS average_generation_mw,
               SUM(e.price_eur_mwh * e.duration_hours) /
                   NULLIF(SUM(CASE WHEN e.price_eur_mwh IS NOT NULL
                                  THEN e.duration_hours ELSE 0 END), 0) AS average_price_eur_mwh,
               MIN(e.price_eur_mwh) AS minimum_price_eur_mwh,
               MAX(e.price_eur_mwh) AS maximum_price_eur_mwh,
               MAX(e.event_time) AS latest_event_time
        FROM energy_observations e JOIN data_sources s ON s.id=e.source_id
        WHERE s.provenance='real' AND e.quality_status='valid'
              AND e.metric IN ('generation', 'price')
        GROUP BY {day}, e.region, e.metric, e.production_type
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold_real_load_forecast_daily")
    # The generation/price view does not depend on the added nullable column.
    with op.batch_alter_table("energy_observations") as batch:
        batch.drop_column("load_forecast_mw")
