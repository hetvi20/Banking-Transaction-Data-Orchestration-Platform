"""002_add_fraud_score_to_transactions

Add fraud_score and risk_tier directly to transactions table
for faster reporting queries (denormalized for Power BI).

Revision ID: 002
Down Revision: 001
"""
from alembic import op
import sqlalchemy as sa

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add fraud columns to transactions for denormalized reporting
    op.add_column('transactions', sa.Column('fraud_score',   sa.Numeric(6, 4), nullable=True))
    op.add_column('transactions', sa.Column('risk_tier',     sa.String(10),    nullable=True))
    op.add_column('transactions', sa.Column('is_fraud_flagged', sa.Boolean,    nullable=False, server_default='0'))
    op.add_column('transactions', sa.Column('requires_review',  sa.Boolean,    nullable=False, server_default='0'))

    # Index for fraud dashboard queries
    op.create_index('ix_txn_fraud_flagged', 'transactions', ['is_fraud_flagged', 'transaction_date'])
    op.create_index('ix_txn_risk_tier',     'transactions', ['risk_tier'])


def downgrade() -> None:
    op.drop_index('ix_txn_risk_tier',     table_name='transactions')
    op.drop_index('ix_txn_fraud_flagged', table_name='transactions')
    op.drop_column('transactions', 'requires_review')
    op.drop_column('transactions', 'is_fraud_flagged')
    op.drop_column('transactions', 'risk_tier')
    op.drop_column('transactions', 'fraud_score')
