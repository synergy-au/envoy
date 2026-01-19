"""merge point 1 storage extension

Revision ID: 5a6908457f70
Revises: 5a615c22e7ad, fe565b4c1a9e
Create Date: 2025-06-23 11:28:08.653150

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "5a6908457f70"
down_revision = ("5a615c22e7ad", "fe565b4c1a9e")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
