"""Audit automated ingestion and independent data verification."""

import sqlalchemy as sa
from alembic import op

revision = "0005_pipelines"
down_revision = "0004_ehyd_terms"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "pipeline_executions" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "pipeline_executions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("domain", sa.String(20), nullable=False),
        sa.Column("orchestrator_run_id", sa.String(300), nullable=False),
        sa.Column("target_date", sa.DateTime(timezone=True)),
        sa.Column("ingestion_run_id", sa.String(36), sa.ForeignKey("ingestion_runs.id")),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text),
        sa.Column("verification", sa.JSON),
    )
    op.create_index("ix_pipeline_executions_domain", "pipeline_executions", ["domain"])
    op.create_index("ix_pipeline_executions_started_at", "pipeline_executions", ["started_at"])


def downgrade() -> None:
    op.drop_table("pipeline_executions")
