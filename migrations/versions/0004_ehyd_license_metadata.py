"""Clarify eHYD collection licensing metadata."""

import sqlalchemy as sa
from alembic import op

revision = "0004_ehyd_terms"
down_revision = "0003_real_data"
branch_labels = None
depends_on = None

_PREVIOUS = "Source terms published by provider; attribution: Datenquelle: ehyd.gv.at"
_CURRENT = (
    "No redistribution license is declared in the collection metadata; verify provider terms."
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE data_sources SET license = :current "
            "WHERE id = 'ehyd-ogc-api' AND license = :previous"
        ).bindparams(current=_CURRENT, previous=_PREVIOUS)
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE data_sources SET license = :previous "
            "WHERE id = 'ehyd-ogc-api' AND license = :current"
        ).bindparams(previous=_PREVIOUS, current=_CURRENT)
    )
