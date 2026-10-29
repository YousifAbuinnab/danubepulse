"""Add incident investigation, quarantine, and replay verification history.

Revision ID: 0002_incidents
Revises: 0001_initial
"""

import sqlalchemy as sa
from alembic import op

revision = "0002_incidents"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def _columns(table_name: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table_name)}


def upgrade() -> None:
    quality_columns = _columns("quality_results")
    with op.batch_alter_table("quality_results") as batch:
        if "phase" not in quality_columns:
            batch.add_column(sa.Column("phase", sa.String(20), nullable=True))
        if "incident_id" not in quality_columns:
            batch.add_column(
                sa.Column(
                    "incident_id",
                    sa.String(36),
                    sa.ForeignKey("incidents.id"),
                    nullable=True,
                )
            )

    incident_columns = _columns("incidents")
    for column, type_ in (
        ("scenario", sa.String(40)),
        ("affected_records", sa.JSON()),
        ("before_metrics", sa.JSON()),
        ("after_metrics", sa.JSON()),
    ):
        if column not in incident_columns:
            op.add_column("incidents", sa.Column(column, type_, nullable=True))

    replay_columns = _columns("replay_history")
    for column, type_ in (
        ("before_metrics", sa.JSON()),
        ("after_metrics", sa.JSON()),
        ("validation_results", sa.JSON()),
    ):
        if column not in replay_columns:
            op.add_column("replay_history", sa.Column(column, type_, nullable=True))

    table_names = set(sa.inspect(op.get_bind()).get_table_names())
    if "quarantined_records" not in table_names:
        op.create_table(
            "quarantined_records",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id"), nullable=False),
            sa.Column("source_record_id", sa.String(120), nullable=False),
            sa.Column("raw_record", sa.JSON(), nullable=False),
            sa.Column("reason", sa.Text(), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index(
            "ix_quarantined_records_incident_id",
            "quarantined_records",
            ["incident_id"],
        )
    if "incident_events" not in table_names:
        op.create_table(
            "incident_events",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("incident_id", sa.String(36), sa.ForeignKey("incidents.id"), nullable=False),
            sa.Column("event_type", sa.String(40), nullable=False),
            sa.Column("from_status", sa.String(20), nullable=True),
            sa.Column("to_status", sa.String(20), nullable=True),
            sa.Column("details", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_incident_events_incident_id", "incident_events", ["incident_id"])
    op.execute("UPDATE incidents SET status = 'detected' WHERE status = 'open'")
    op.execute("UPDATE incidents SET status = 'recovered' WHERE status = 'resolved'")
    op.execute("UPDATE incidents SET scenario = category WHERE scenario IS NULL")
    op.execute("UPDATE quality_results SET phase = 'detection' WHERE phase IS NULL")
    op.execute(
        """
        UPDATE quality_results
           SET incident_id = (
               SELECT incidents.id FROM incidents
                WHERE incidents.run_id = quality_results.run_id
                ORDER BY incidents.detected_at DESC
                LIMIT 1
           )
         WHERE incident_id IS NULL
           AND EXISTS (SELECT 1 FROM incidents WHERE incidents.run_id = quality_results.run_id)
        """
    )


def downgrade() -> None:
    op.drop_index("ix_incident_events_incident_id", table_name="incident_events")
    op.drop_table("incident_events")
    op.drop_index("ix_quarantined_records_incident_id", table_name="quarantined_records")
    op.drop_table("quarantined_records")
    for column in ("before_metrics", "after_metrics", "validation_results"):
        op.drop_column("replay_history", column)
    for column in ("scenario", "affected_records", "before_metrics", "after_metrics"):
        op.drop_column("incidents", column)
    op.drop_column("quality_results", "incident_id")
    op.drop_column("quality_results", "phase")
