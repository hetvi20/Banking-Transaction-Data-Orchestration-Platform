"""003_add_compliance_flags

Add AML and compliance columns to support regulatory reporting.
Adds compliance_reports table for audit trail.

Revision ID: 003
Down Revision: 002
"""
from alembic import op
import sqlalchemy as sa

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # AML flags on transactions
    op.add_column('transactions', sa.Column('aml_flag',          sa.Boolean,    nullable=False, server_default='0'))
    op.add_column('transactions', sa.Column('compliance_notes',  sa.Text,       nullable=True))
    op.add_column('transactions', sa.Column('reviewed_by',       sa.String(100), nullable=True))
    op.add_column('transactions', sa.Column('reviewed_at',       sa.DateTime,   nullable=True))

    # Compliance reports audit table
    op.create_table(
        'compliance_reports',
        sa.Column('report_id',         sa.String(36),   primary_key=True),
        sa.Column('report_type',       sa.String(50),   nullable=False),   # CTR, SAR, etc.
        sa.Column('transaction_id',    sa.String(36),   sa.ForeignKey('transactions.transaction_id'), nullable=False),
        sa.Column('account_id',        sa.String(36),   nullable=False),
        sa.Column('amount_usd',        sa.Numeric(18,2), nullable=False),
        sa.Column('reason',            sa.Text,         nullable=True),
        sa.Column('status',            sa.String(20),   nullable=False, default='pending'),
        sa.Column('submitted_at',      sa.DateTime,     nullable=True),
        sa.Column('created_at',        sa.DateTime,     server_default=sa.func.now()),
    )

    op.create_index('ix_compliance_account', 'compliance_reports', ['account_id'])
    op.create_index('ix_txn_aml',           'transactions',        ['aml_flag', 'transaction_date'])


def downgrade() -> None:
    op.drop_index('ix_txn_aml',           table_name='transactions')
    op.drop_index('ix_compliance_account', table_name='compliance_reports')
    op.drop_table('compliance_reports')
    op.drop_column('transactions', 'reviewed_at')
    op.drop_column('transactions', 'reviewed_by')
    op.drop_column('transactions', 'compliance_notes')
    op.drop_column('transactions', 'aml_flag')
