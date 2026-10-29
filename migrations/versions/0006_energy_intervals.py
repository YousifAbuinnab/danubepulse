"""Preserve energy revisions and expose duration-weighted real energy analytics."""

import sqlalchemy as sa
from alembic import op

revision = "0006_energy"
down_revision = "0005_pipelines"
branch_labels = None
depends_on = None


def upgrade() -> None:
    existing = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("energy_observations")
    }
    columns = (
        sa.Column("interval_end", sa.DateTime(timezone=True)),
        sa.Column("duration_hours", sa.Float()),
        sa.Column("metric", sa.String(24)),
        sa.Column("production_type", sa.String(8)),
        sa.Column("document_id", sa.String(160)),
        sa.Column("revision", sa.Integer()),
        sa.Column("source_unit", sa.String(20)),
    )
    for column in columns:
        if column.name not in existing:
            op.add_column("energy_observations", column)
    # Interval boundaries are UTC, never server-local calendar dates.
    day = (
        "(e.event_time AT TIME ZONE 'UTC')::date"
        if op.get_bind().dialect.name == "postgresql"
        else "DATE(e.event_time)"
    )
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
        GROUP BY {day}, e.region, e.metric, e.production_type
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold_real_energy_daily")
    with op.batch_alter_table("energy_observations") as batch:
        for name in (
            "interval_end",
            "duration_hours",
            "metric",
            "production_type",
            "document_id",
            "revision",
            "source_unit",
        ):
            batch.drop_column(name)
