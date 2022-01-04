"""empty message

Revision ID: 9f3cd6878a65
Revises: f5312239d602
Create Date: 2022-01-03 21:14:53.060774

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9f3cd6878a65'
down_revision = 'f5312239d602'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('user', sa.Column('access_type', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('user', 'access_type')
    # ### end Alembic commands ###