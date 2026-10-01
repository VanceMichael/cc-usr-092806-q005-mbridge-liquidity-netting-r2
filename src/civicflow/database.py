"""SQLite 连接、事务和数据库初始化。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, version);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);
CREATE TABLE IF NOT EXISTS clearing_corridors (
    corridor_id TEXT PRIMARY KEY,
    corridor_code TEXT NOT NULL UNIQUE,
    currency_pay TEXT NOT NULL,
    currency_receive TEXT NOT NULL,
    window_duration_minutes INTEGER NOT NULL,
    cutoff_offset_minutes INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS clearing_participants (
    corridor_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    state TEXT NOT NULL,
    authorized_counterparties_json TEXT NOT NULL,
    added_at TEXT NOT NULL,
    added_by TEXT NOT NULL,
    PRIMARY KEY(corridor_id, organization_id)
);
CREATE TABLE IF NOT EXISTS clearing_limits (
    limit_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    version INTEGER NOT NULL,
    effective_at TEXT NOT NULL,
    registered_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS limits_lookup ON clearing_limits(corridor_id, organization_id, currency, version);
CREATE TABLE IF NOT EXISTS clearing_windows (
    window_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL,
    window_index INTEGER NOT NULL,
    opens_at TEXT NOT NULL,
    closes_at TEXT NOT NULL,
    cutoff_at TEXT NOT NULL,
    state TEXT NOT NULL,
    hold_reason TEXT NOT NULL DEFAULT '',
    fx_rate_id TEXT,
    approved_by TEXT,
    approved_at TEXT,
    closed_by TEXT,
    closed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(corridor_id, window_index)
);
CREATE INDEX IF NOT EXISTS windows_order ON clearing_windows(corridor_id, opens_at, closes_at);
CREATE TABLE IF NOT EXISTS clearing_fx_rates (
    rate_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL,
    window_id TEXT,
    rate_value TEXT NOT NULL,
    version INTEGER NOT NULL,
    effective_at TEXT NOT NULL,
    registered_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fx_window ON clearing_fx_rates(window_id, version);
CREATE TABLE IF NOT EXISTS clearing_batches (
    batch_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    settle_currency TEXT NOT NULL,
    status TEXT NOT NULL,
    hold_reason TEXT NOT NULL DEFAULT '',
    confirmed_sequence INTEGER,
    confirmed_at TEXT,
    settled_entry_ids_json TEXT NOT NULL DEFAULT '[]',
    reversal_entry_ids_json TEXT NOT NULL DEFAULT '[]',
    settlement_detail_json TEXT NOT NULL DEFAULT '[]',
    settled_at TEXT,
    settlement_window_id TEXT,
    adjusted_by_batch_id TEXT UNIQUE,
    reverses_batch_id TEXT,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS batch_window_net ON clearing_batches(window_id, settle_currency, COALESCE(reverses_batch_id, ''));
CREATE TABLE IF NOT EXISTS clearing_batch_items (
    item_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    obligation_key TEXT NOT NULL,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    organization_id TEXT NOT NULL,
    counterparty_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    fx_rate_id TEXT NOT NULL,
    net_amount_minor INTEGER NOT NULL,
    leg TEXT NOT NULL,
    instruction_id TEXT NOT NULL,
    hold_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS batch_items_batch ON clearing_batch_items(batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS batch_items_live_obligation ON clearing_batch_items(obligation_key) WHERE leg='gross';
CREATE UNIQUE INDEX IF NOT EXISTS batch_items_live_sequence ON clearing_batch_items(source, source_key, sequence) WHERE leg='gross';
CREATE TABLE IF NOT EXISTS clearing_positions (
    batch_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    gross_pay_minor INTEGER NOT NULL,
    gross_receive_minor INTEGER NOT NULL,
    net_minor INTEGER NOT NULL,
    net_converted_minor INTEGER NOT NULL,
    net_direction TEXT NOT NULL,
    hold_ids_json TEXT NOT NULL,
    PRIMARY KEY(batch_id, organization_id, currency)
);
CREATE TABLE IF NOT EXISTS clearing_instructions (
    instruction_id TEXT PRIMARY KEY,
    obligation_key TEXT NOT NULL,
    corridor_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    counterparty_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    bridge_phase TEXT NOT NULL DEFAULT 'admitted',
    effective_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    late_admitted INTEGER NOT NULL DEFAULT 0,
    window_id TEXT,
    batch_id TEXT,
    settled_window_id TEXT,
    received_at TEXT NOT NULL,
    processed_at TEXT,
    UNIQUE(source, source_key, sequence)
);
CREATE INDEX IF NOT EXISTS instructions_window ON clearing_instructions(window_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS instructions_live_obligation ON clearing_instructions(obligation_key) WHERE state IN ('received','frozen','settled');
CREATE TABLE IF NOT EXISTS clearing_holds (
    hold_id TEXT PRIMARY KEY,
    corridor_id TEXT NOT NULL,
    window_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    status TEXT NOT NULL,
    instruction_id TEXT NOT NULL,
    batch_id TEXT,
    item_id TEXT,
    released_at TEXT,
    created_at TEXT NOT NULL,
    settlement_entry_id TEXT,
    UNIQUE(instruction_id, currency)
);
CREATE INDEX IF NOT EXISTS holds_window_org ON clearing_holds(corridor_id, window_id, organization_id, currency, status);
CREATE TABLE IF NOT EXISTS clearing_source_cursors (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_confirmed_sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(source, source_key)
);
CREATE TABLE IF NOT EXISTS clearing_bridge_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    obligation_key TEXT,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    UNIQUE(source, source_key, sequence)
);
CREATE INDEX IF NOT EXISTS bridge_events_obligation ON clearing_bridge_events(obligation_key, sequence);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
