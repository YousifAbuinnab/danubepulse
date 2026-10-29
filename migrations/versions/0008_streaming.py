"""Durable notification outbox, consumer inbox and monitoring; no historical backfill."""

from alembic import op

from danubepulse.models import (
    StreamingDeadLetter,
    StreamingOutbox,
    StreamingReceipt,
    StreamingWorker,
)

revision = "0008_streaming"
down_revision = "0007_load_forecast"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for model in (StreamingOutbox, StreamingReceipt, StreamingDeadLetter, StreamingWorker):
        model.__table__.create(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for model in (StreamingWorker, StreamingDeadLetter, StreamingReceipt, StreamingOutbox):
        model.__table__.drop(op.get_bind())
