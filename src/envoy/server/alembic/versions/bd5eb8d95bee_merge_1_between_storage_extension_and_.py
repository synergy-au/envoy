"""merge 1 between storage extension and main

Revision ID: bd5eb8d95bee
Revises: 54595fc3f784, fe565b4c1a9e
Create Date: 2025-06-18 09:11:20.078506

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "bd5eb8d95bee"
down_revision = ("54595fc3f784", "fe565b4c1a9e")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
