"""应用装配。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .audit import AuditLog
from .clearing import ClearingHouse
from .database import Database
from .idempotency import IdempotencyStore
from .inbox import Inbox
from .jobs import JobQueue
from .ledger import Ledger
from .netting import BridgeNetting
from .outbox import Outbox
from .repository import EntityRepository
from .reservations import ReservationBook
from .timeutil import Clock


@dataclass(frozen=True)
class CivicFlow:
    database: Database
    clock: Clock
    repository: EntityRepository
    inbox: Inbox
    outbox: Outbox
    ledger: Ledger
    reservations: ReservationBook
    jobs: JobQueue
    clearing: ClearingHouse
    bridge: BridgeNetting

    @classmethod
    def open(cls, path: str | Path, *, fixed_now: str | None = None) -> "CivicFlow":
        database = Database(path); database.initialize(); clock = Clock(fixed_now)
        audit = AuditLog(clock); idempotency = IdempotencyStore(clock)
        repository = EntityRepository(database, clock, audit, idempotency)
        inbox = Inbox(database, clock)
        ledger = Ledger(database, clock)
        clearing = ClearingHouse(database, clock, audit)
        bridge = BridgeNetting(database, clock, clearing, inbox, ledger, audit)
        return cls(database, clock, repository, inbox, Outbox(database, clock), ledger,
                   ReservationBook(database), JobQueue(database, clock), clearing, bridge)

    def verify(self) -> dict:
        with self.database.connect() as connection:
            audit_count = AuditLog(self.clock).verify(connection)
            entity_count = connection.execute("SELECT COUNT(*) AS n FROM entities").fetchone()["n"]
            conflict_count = connection.execute("SELECT COUNT(*) AS n FROM inbox_conflicts").fetchone()["n"]
            duplicated = connection.execute(
                "SELECT COUNT(*) AS n FROM (SELECT obligation_key FROM clearing_batch_items WHERE leg='gross' GROUP BY obligation_key HAVING COUNT(*)>1)").fetchone()["n"]
            double_hold = connection.execute(
                "SELECT COUNT(*) AS n FROM (SELECT obligation_key FROM clearing_instructions WHERE state IN ('received','frozen') GROUP BY obligation_key HAVING COUNT(*)>1)").fetchone()["n"]
        if duplicated or double_hold:
            from .errors import InvariantViolation
            raise InvariantViolation("同一义务同时出现在多个有效批次/指令中")
        return {"audit_entries": audit_count, "entities": entity_count, "inbox_conflicts": conflict_count,
                "duplicated_obligations": duplicated + double_hold}
