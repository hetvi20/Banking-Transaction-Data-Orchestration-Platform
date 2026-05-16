"""001_create_transactions_table

Initial schema: transactions, fraud_signals, and pipeline_runs tables.

Revision ID: 001
Create Date: 2025-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Transactions table ────────────────────────────────────────────────────
    op.create_table(
        'transactions',
        sa.Column('transaction_id',     sa.String(36),      primary_key=True),
        sa.Column('account_id',         sa.String(36),      nullable=False, index=True),
        sa.Column('merchant_id',        sa.String(36),      nullable=True),
        sa.Column('amount_usd',         sa.Numeric(18, 2),  nullable=False),
        sa.Column('currency_code',      sa.String(3),       nullable=False),
        sa.Column('transaction_type',   sa.String(20),      nullable=False),
        sa.Column('transaction_status', sa.String(20),      nullable=False),
        sa.Column('region',             sa.String(20),      nullable=True),
        sa.Column('channel',            sa.String(20),      nullable=True),
        sa.Column('transaction_at',     sa.DateTime,        nullable=False, index=True),
        sa.Column('transaction_date',   sa.Date,            nullable=False, index=True),
        sa.Column('is_large_transaction', sa.Boolean,       nullable=False, default=False),
        sa.Column('ingested_at',        sa.DateTime,        nullable=False),
        sa.Column('created_at',         sa.DateTime,        server_default=sa.func.now()),
        sa.Column('updated_at',         sa.DateTime,        server_default=sa.func.now(), onupdate=sa.func.now()),
    )
    op.create_index('ix_txn_date_region', 'transactions', ['transaction_date', 'region'])
    op.create_index('ix_txn_account_date', 'transactions', ['account_id', 'transaction_date'])

    # ── Fraud signals table ───────────────────────────────────────────────────
    op.create_table(
        'fraud_signals',
        sa.Column('id',              sa.Integer,       primary_key=True, autoincrement=True),
        sa.Column('transaction_id',  sa.String(36),    sa.ForeignKey('transactions.transaction_id'), nullable=False, index=True),
        sa.Column('fraud_score',     sa.Numeric(6, 4), nullable=False),
        sa.Column('is_flagged',      sa.Boolean,       nullable=False, default=False),
        sa.Column('risk_tier',       sa.String(10),    nullable=True),
        sa.Column('fraud_reasons',   sa.Text,          nullable=True),
        sa.Column('model_version',   sa.String(20),    nullable=True),
        sa.Column('scored_at',       sa.DateTime,      nullable=False),
        sa.Column('created_at',      sa.DateTime,      server_default=sa.func.now()),
    )

    # ── Pipeline runs audit table ─────────────────────────────────────────────
    op.create_table(
        'pipeline_runs',
        sa.Column('run_id',            sa.String(36),   primary_key=True),
        sa.Column('pipeline_name',     sa.String(100),  nullable=False),
        sa.Column('partition_date',    sa.Date,         nullable=False),
        sa.Column('status',            sa.String(20),   nullable=False),
        sa.Column('rows_ingested',     sa.Integer,      nullable=True),
        sa.Column('rows_failed',       sa.Integer,      nullable=True),
        sa.Column('duration_seconds',  sa.Numeric(8,2), nullable=True),
        sa.Column('error_message',     sa.Text,         nullable=True),
        sa.Column('started_at',        sa.DateTime,     nullable=False),
        sa.Column('completed_at',      sa.DateTime,     nullable=True),
    )


def downgrade() -> None:
    op.drop_table('pipeline_runs')
    op.drop_table('fraud_signals')
    op.drop_table('transactions')
