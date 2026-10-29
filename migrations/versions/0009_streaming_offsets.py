"""Use the Kafka protocol's 64-bit offset range in durable dead letters."""

import sqlalchemy as sa
from alembic import op

revision = "0009_streaming_offsets"
down_revision = "0008_streaming"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("streaming_dead_letters") as batch:
        batch.alter_column("offset", existing_type=sa.Integer(), type_=sa.BigInteger())


def downgrade() -> None:
    # Refuse lossy narrowing if any retained Kafka position exceeds PostgreSQL INTEGER.
    maximum = op.get_bind().scalar(sa.text('SELECT MAX("offset") FROM streaming_dead_letters'))
    if maximum is not None and maximum > 2**31 - 1:
        raise ValueError("Cannot narrow retained 64-bit Kafka offsets without losing history.")
    with op.batch_alter_table("streaming_dead_letters") as batch:
        batch.alter_column("offset", existing_type=sa.BigInteger(), type_=sa.Integer())
