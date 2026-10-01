"""数字货币桥清算窗口与净额结算。

桥侧事件（受理、匹配、结算、退回、撤销）按 ``(来源, 来源键, 序号)`` 严格有序推进；
指令在窗口内冻结可用流动性并汇入可追溯的净额批次。已结算记录不可抹除，
只能通过反向分录在新窗口中调整。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .jsonutil import canonical_json, digest_json
from .ledger import to_minor
from .security import AccessContext
from .timeutil import Clock, canonical_instant, parse_instant

EVENT_RECEIVED = "received"
EVENT_MATCHED = "matched"
EVENT_SETTLED = "settled"
EVENT_RETURNED = "returned"
EVENT_CANCELLED = "cancelled"
EVENT_TYPES = (EVENT_RECEIVED, EVENT_MATCHED, EVENT_SETTLED, EVENT_RETURNED, EVENT_CANCELLED)

# 批次状态
BATCH_GATHERING = "gathering"
BATCH_PAUSED = "paused"
BATCH_CLOSED = "closed"
BATCH_SETTLED = "settled"
# 指令状态
INSTR_FROZEN = "frozen"
INSTR_MATCHED = "matched"
INSTR_SETTLED = "settled"
INSTR_RETURNED = "returned"
INSTR_CANCELLED = "cancelled"
ACTIVE_INSTRUCTION_STATES = (INSTR_FROZEN, INSTR_MATCHED)
TERMINAL_BATCH_STATES = (BATCH_CLOSED, BATCH_SETTLED)

PAUSE_LIMIT = "limit_changed"
PAUSE_RATE = "rate_revised"
PAUSE_COMPLIANCE = "compliance_hit"


@dataclass(frozen=True)
class ClearingService:
    database: Database
    clock: Clock
    audit: AuditLog

    # ------------------------------------------------------------------ 走廊

    def register_corridor(self, context: AccessContext, *, corridor_id: str, name: str, base_currency: str, quote_currency: str) -> dict:
        context.require("write:clearing")
        require_safe(corridor_id, "走廊标识")
        for currency in (base_currency, quote_currency):
            require_safe(currency, "币种")
        if not name.strip():
            raise ValidationError("走廊名称不能为空")
        with self.database.transaction() as conn:
            if conn.execute("SELECT 1 FROM cl_corridors WHERE corridor_id=?", (corridor_id,)).fetchone():
                raise ConflictError("走廊已经登记")
            now = self.clock.now()
            conn.execute("INSERT INTO cl_corridors(corridor_id,name,base_currency,quote_currency,status,created_at,created_by) VALUES(?,?,?,?,?,?,?)", (corridor_id, name.strip(), base_currency, quote_currency, "active", now, context.actor_id))
            self._audit(conn, context, "register_corridor", corridor_id, {"name": name, "base_currency": base_currency, "quote_currency": quote_currency})
            return self._corridor(conn, corridor_id)

    def add_participant(self, context: AccessContext, *, corridor_id: str, org_id: str, authorized_counterparties: Iterable[str] = ()) -> dict:
        context.require("write:clearing")
        require_safe(org_id, "机构标识")
        counterparties = sorted({require_safe(str(c), "对手方机构") for c in authorized_counterparties})
        with self.database.transaction() as conn:
            self._require_corridor(conn, corridor_id)
            if org_id in counterparties:
                raise ValidationError("机构不能把自己授权为对手方")
            if conn.execute("SELECT 1 FROM cl_participants WHERE corridor_id=? AND org_id=?", (corridor_id, org_id)).fetchone():
                raise ConflictError("机构已经加入走廊")
            now = self.clock.now()
            conn.execute("INSERT INTO cl_participants(corridor_id,org_id,status,authorized_counterparties_json,added_at,added_by) VALUES(?,?,?,?,?,?)", (corridor_id, org_id, "active", canonical_json(counterparties), now, context.actor_id))
            self._audit(conn, context, "add_participant", corridor_id, {"org_id": org_id, "authorized_counterparties": counterparties})
            return {"corridor_id": corridor_id, "org_id": org_id, "status": "active", "authorized_counterparties": counterparties}

    def update_counterparties(self, context: AccessContext, *, corridor_id: str, org_id: str, authorized_counterparties: Iterable[str]) -> dict:
        context.require("write:clearing")
        counterparties = sorted({require_safe(str(c), "对手方机构") for c in authorized_counterparties})
        if org_id in counterparties:
            raise ValidationError("机构不能把自己授权为对手方")
        with self.database.transaction() as conn:
            row = conn.execute("SELECT status FROM cl_participants WHERE corridor_id=? AND org_id=?", (corridor_id, org_id)).fetchone()
            if not row:
                raise NotFoundError("参与方不存在")
            conn.execute("UPDATE cl_participants SET authorized_counterparties_json=? WHERE corridor_id=? AND org_id=?", (canonical_json(counterparties), corridor_id, org_id))
            self._audit(conn, context, "update_counterparties", corridor_id, {"org_id": org_id, "authorized_counterparties": counterparties})
            return {"corridor_id": corridor_id, "org_id": org_id, "authorized_counterparties": counterparties}

    # ------------------------------------------------------------------ 限额

    def set_limit(self, context: AccessContext, *, corridor_id: str, org_id: str, currency: str, amount: str) -> dict:
        context.require("write:clearing")
        minor = to_minor(amount)
        if minor < 0:
            raise ValidationError("限额不能为负数")
        with self.database.transaction() as conn:
            self._require_participant(conn, corridor_id, org_id)
            row = conn.execute("SELECT COALESCE(MAX(version),0) AS v FROM cl_limits WHERE corridor_id=? AND org_id=? AND currency=?", (corridor_id, org_id, currency)).fetchone()
            version = int(row["v"]) + 1
            now = self.clock.now()
            conn.execute("INSERT INTO cl_limits(corridor_id,org_id,currency,amount_minor,version,changed_by,changed_at) VALUES(?,?,?,?,?,?,?)", (corridor_id, org_id, currency, minor, version, context.actor_id, now))
            self._audit(conn, context, "set_limit", corridor_id, {"org_id": org_id, "currency": currency, "amount_minor": minor, "version": version})
            # 关账前的限额变化：暂停该机构该币种尚未关账的批次
            if version > 1:
                self._pause_affected(conn, context, PAUSE_LIMIT,
                    "SELECT b.* FROM cl_batches b JOIN cl_instructions i ON i.batch_id=b.batch_id "
                    "WHERE b.corridor_id=? AND i.payer_org=? AND i.currency=? AND i.status IN ('frozen','matched') "
                    "AND b.status IN ('gathering','paused')",
                    (corridor_id, org_id, currency),
                    detail={"org_id": org_id, "currency": currency, "limit_version": version})
            return {"corridor_id": corridor_id, "org_id": org_id, "currency": currency, "amount_minor": minor, "version": version}

    def get_limit(self, context: AccessContext, *, corridor_id: str, org_id: str, currency: str, window_id: str | None = None) -> dict:
        context.require("read:clearing")
        self._require_own_org(context, org_id)
        with self.database.connect() as conn:
            row = conn.execute("SELECT * FROM cl_limits WHERE corridor_id=? AND org_id=? AND currency=? ORDER BY version DESC LIMIT 1", (corridor_id, org_id, currency)).fetchone()
            if not row:
                raise NotFoundError("限额尚未设置")
            if window_id is None:
                window = conn.execute("SELECT window_id FROM cl_windows WHERE corridor_id=? AND status='open' ORDER BY opens_at DESC LIMIT 1", (corridor_id,)).fetchone()
                window_id = window["window_id"] if window else None
            used = self._window_limit_used(conn, corridor_id, window_id, org_id, currency) if window_id else 0
            return {"corridor_id": corridor_id, "org_id": org_id, "currency": currency, "window_id": window_id, "amount_minor": row["amount_minor"], "version": row["version"], "occupied_minor": used, "available_minor": int(row["amount_minor"]) - used}

    # ------------------------------------------------------------------ 流动性

    def fund_liquidity(self, context: AccessContext, *, corridor_id: str, org_id: str, currency: str, amount: str, reference: str) -> dict:
        context.require("fund:clearing")
        self._require_own_org(context, org_id)
        minor = to_minor(amount)
        if minor <= 0:
            raise ValidationError("注资金额必须大于零")
        require_safe(reference, "参考号")
        with self.database.transaction() as conn:
            self._require_participant(conn, corridor_id, org_id)
            dup = conn.execute("SELECT 1 FROM cl_liquidity_entries WHERE corridor_id=? AND reference=? AND kind='fund'", (corridor_id, reference)).fetchone()
            if dup:
                raise ConflictError("相同参考号已经注资")
            entry_id = new_id("liq")
            conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (entry_id, corridor_id, org_id, currency, minor, "fund", reference, self.clock.now(), context.actor_id))
            return {"entry_id": entry_id, "balance_minor": self._balance(conn, corridor_id, org_id, currency)}

    def liquidity_position(self, context: AccessContext, *, corridor_id: str, org_id: str, currency: str) -> dict:
        context.require("read:clearing")
        self._require_own_org(context, org_id)
        with self.database.connect() as conn:
            return {"corridor_id": corridor_id, "org_id": org_id, "currency": currency, "balance_minor": self._balance(conn, corridor_id, org_id, currency), "occupied_minor": self._occupied(conn, corridor_id, org_id, currency)}

    # ------------------------------------------------------------------ 窗口

    def open_window(self, context: AccessContext, *, corridor_id: str, opens_at: str | None = None, closes_at: str) -> dict:
        context.require("write:clearing")
        opens_at = canonical_instant(opens_at) if opens_at else self.clock.now()
        closes_at = canonical_instant(closes_at)
        if parse_instant(opens_at) >= parse_instant(closes_at):
            raise ValidationError("窗口截止时间必须晚于开启时间")
        with self.database.transaction() as conn:
            self._require_corridor(conn, corridor_id, active=True)
            seq = int(conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS n FROM cl_windows WHERE corridor_id=?", (corridor_id,)).fetchone()["n"])
            window_id = new_id("window")
            now = self.clock.now()
            conn.execute("INSERT INTO cl_windows(window_id,corridor_id,seq,opens_at,closes_at,status,created_at,created_by) VALUES(?,?,?,?,?,?,?,?)", (window_id, corridor_id, seq, opens_at, closes_at, "open", now, context.actor_id))
            self._audit(conn, context, "open_window", corridor_id, {"window_id": window_id, "seq": seq, "opens_at": opens_at, "closes_at": closes_at})
            return self._window(conn, window_id)

    def get_window(self, context: AccessContext, window_id: str) -> dict:
        context.require("read:clearing")
        with self.database.connect() as conn:
            window = self._window(conn, window_id)
            return self._window_view(conn, window, context)

    def list_windows(self, context: AccessContext, *, corridor_id: str) -> list[dict]:
        context.require("read:clearing")
        with self.database.connect() as conn:
            windows = [self._window(conn, row["window_id"]) for row in conn.execute("SELECT window_id FROM cl_windows WHERE corridor_id=? ORDER BY seq", (corridor_id,))]
            return [self._window_view(conn, window, context) for window in windows]

    def enter_rate(self, context: AccessContext, *, window_id: str, rate: str, reason: str = "") -> dict:
        """录入或修订窗口汇率快照。关账后不允许修订。"""
        context.require("write:rate")
        try:
            number = Decimal(str(rate))
        except InvalidOperation as exc:
            raise ValidationError("汇率格式错误") from exc
        if not number.is_finite() or number <= 0:
            raise ValidationError("汇率必须是大于零的有限数")
        with self.database.transaction() as conn:
            window = self._require_window_row(conn, window_id)
            if window["status"] != "open":
                raise ConflictError("窗口已经关账，汇率修订必须在新窗口处理")
            version = int(conn.execute("SELECT COALESCE(MAX(version),0)+1 AS n FROM cl_rates WHERE corridor_id=? AND window_id=?", (window["corridor_id"], window_id)).fetchone()["n"])
            rate_id = new_id("rate")
            now = self.clock.now()
            conn.execute("UPDATE cl_rates SET status='superseded' WHERE corridor_id=? AND window_id=? AND status='current'", (window["corridor_id"], window_id))
            conn.execute("INSERT INTO cl_rates(rate_id,corridor_id,window_id,version,rate,status,entered_by,reason,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (rate_id, window["corridor_id"], window_id, version, format(number, "f"), "current", context.actor_id, reason.strip(), now))
            conn.execute("UPDATE cl_windows SET rate_id=? WHERE window_id=?", (rate_id, window_id))
            self._audit(conn, context, "enter_rate", window["corridor_id"], {"window_id": window_id, "rate_id": rate_id, "version": version, "rate": format(number, "f"), "revision": version > 1})
            if version > 1:
                # 汇率修订：暂停该窗口内所有尚未关账的批次
                self._pause_affected(conn, context, PAUSE_RATE,
                    "SELECT * FROM cl_batches WHERE window_id=? AND status IN ('gathering','paused')",
                    (window_id,),
                    detail={"window_id": window_id, "rate_id": rate_id, "version": version})
            return {"window_id": window_id, "rate_id": rate_id, "version": version, "rate": format(number, "f"), "entered_by": context.actor_id}

    # ------------------------------------------------------- 合规命中与暂停

    def flag_compliance(self, context: AccessContext, *, instruction_id: str, reason: str) -> dict:
        context.require("compliance:clearing")
        if not reason.strip():
            raise ValidationError("合规命中必须说明原因")
        with self.database.transaction() as conn:
            row = conn.execute("SELECT batch_id FROM cl_instructions WHERE instruction_id=? AND status IN ('frozen','matched')", (instruction_id,)).fetchone()
            if not row:
                raise NotFoundError("找不到有效指令，无法标记合规命中")
            self._pause_batch_row(conn, context, row["batch_id"], PAUSE_COMPLIANCE, reason.strip(), {"instruction_id": instruction_id})
            return self._batch(conn, row["batch_id"])

    def pause_batch(self, context: AccessContext, *, batch_id: str, reason: str) -> dict:
        context.require("pause:clearing")
        if reason not in (PAUSE_LIMIT, PAUSE_RATE, PAUSE_COMPLIANCE):
            raise ValidationError("暂停原因不合法")
        with self.database.transaction() as conn:
            self._pause_batch_row(conn, context, batch_id, reason, "人工暂停", {})
            return self._batch(conn, batch_id)

    def resume_batch(self, context: AccessContext, *, batch_id: str, note: str = "") -> dict:
        context.require("resume:clearing")
        with self.database.transaction() as conn:
            batch = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError("批次不存在")
            if batch["status"] != BATCH_PAUSED:
                raise ConflictError("批次当前不处于暂停状态")
            reason = batch["pause_reason"]
            if reason == PAUSE_RATE:
                window = self._require_window_row(conn, batch["window_id"])
                if not window["rate_id"]:
                    raise ConflictError("窗口尚未录入汇率")
                conn.execute("UPDATE cl_batches SET rate_id=? WHERE batch_id=?", (window["rate_id"], batch_id))
                conn.execute("UPDATE cl_instructions SET rate_id=? WHERE batch_id=? AND status IN ('frozen','matched')", (window["rate_id"], batch_id))
            elif reason == PAUSE_LIMIT:
                for payer in conn.execute("SELECT payer_org,currency,SUM(amount_minor) AS used FROM cl_instructions WHERE batch_id=? AND status IN ('frozen','matched') GROUP BY payer_org,currency", (batch_id,)):
                    limit = conn.execute("SELECT amount_minor,version FROM cl_limits WHERE corridor_id=? AND org_id=? AND currency=? ORDER BY version DESC LIMIT 1", (batch["corridor_id"], payer["payer_org"], payer["currency"])).fetchone()
                    window_used = self._window_limit_used(conn, batch["corridor_id"], batch["window_id"], payer["payer_org"], payer["currency"])
                    if not limit or int(limit["amount_minor"]) < window_used:
                        raise ConflictError(f"机构 {payer['payer_org']} 的最新限额仍不足以恢复批次")
                    conn.execute("UPDATE cl_instructions SET limit_version=?,usage_at_freeze_minor=? WHERE batch_id=? AND payer_org=? AND currency=? AND status IN ('frozen','matched')", (limit["version"], window_used, batch_id, payer["payer_org"], payer["currency"]))
            conn.execute("UPDATE cl_batches SET status='gathering',pause_reason='',paused_by='',paused_at=NULL WHERE batch_id=?", (batch_id,))
            self._audit(conn, context, "resume_batch", batch["corridor_id"], {"batch_id": batch_id, "previous_reason": reason, "note": note.strip()})
            return self._batch(conn, batch_id)

    # ------------------------------------------------------------ 桥侧事件

    def bridge_event(self, context: AccessContext, *, source: str, source_key: str, sequence: int, event_type: str, payload: dict | None = None, occurred_at: str | None = None) -> dict:
        context.require("bridge:clearing")
        require_safe(source, "来源"); require_safe(source_key, "来源键")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        if event_type not in EVENT_TYPES:
            raise ValidationError("未知桥侧事件类型")
        payload = payload or {}
        occurred_at = canonical_instant(occurred_at) if occurred_at else self.clock.now()
        digest = digest_json(payload)
        # 异文隔离必须独立持久化：不能随被拒绝的主事务一起回滚
        with self.database.connect() as probe:
            existing = probe.execute("SELECT * FROM cl_bridge_events WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
        if existing:
            return self._handle_existing(existing, digest, event_type, source, source_key, sequence)
        with self.database.transaction() as conn:
            # 事务内二次确认（防并发）
            raced = conn.execute("SELECT * FROM cl_bridge_events WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            if raced:
                result = self._handle_existing(raced, digest, event_type, source, source_key, sequence, conn=conn)
                if result is not None:
                    return result
            # 序号必须按来源键连续推进
            last = conn.execute("SELECT COALESCE(MAX(sequence),-1) AS s FROM cl_bridge_events WHERE source=? AND source_key=? AND status!='waiting'", (source, source_key)).fetchone()["s"]
            if sequence != int(last) + 1:
                if sequence <= int(last):
                    raise ConflictError("来源序号倒退")
                conn.execute("INSERT INTO cl_bridge_events(source,source_key,sequence,event_type,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,?,?)", (source, source_key, sequence, event_type, digest, canonical_json(payload), occurred_at, self.clock.now(), "waiting"))
                return {"status": "waiting_gap", "source": source, "source_key": source_key, "sequence": sequence, "expected_sequence": int(last) + 1}
            result = self._apply_event(conn, context, source, source_key, sequence, event_type, payload, digest, occurred_at)
        # 当前事件提交后，再按序独立推进缓存事件：任一事件无法应用时停在该缺口，不回滚已确认的补缺
        self._drain_waiting(context, source, source_key)
        return result

    def _handle_existing(self, existing, digest: str, event_type: str, source: str, source_key: str, sequence: int, *, conn=None) -> dict | None:
        if existing["status"] == "waiting":
            return {"status": "waiting_gap", "source": source, "source_key": source_key, "sequence": sequence, "buffered": True}
        if existing["payload_digest"] != digest or existing["event_type"] != event_type:
            if conn is None:
                # 独立事务持久化隔离记录，避免随被拒绝的主事务回滚
                with self.database.transaction() as conflict_conn:
                    conflict_conn.execute("INSERT INTO cl_bridge_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES(?,?,?,?,?,?)", (source, source_key, sequence, existing["payload_digest"], digest, self.clock.now()))
            else:
                conn.execute("INSERT INTO cl_bridge_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES(?,?,?,?,?,?)", (source, source_key, sequence, existing["payload_digest"], digest, self.clock.now()))
            raise ConflictError("相同来源序号出现不同内容，事件已隔离")
        if conn is not None:
            return self._replay_result(conn, existing)
        with self.database.connect() as read_conn:
            return self._replay_result(read_conn, existing)

    def conflicts(self, context: AccessContext, *, source: str | None = None) -> list[dict]:
        context.require("read:clearing")
        with self.database.connect() as conn:
            if source:
                rows = conn.execute("SELECT * FROM cl_bridge_conflicts WHERE source=? ORDER BY conflict_id", (source,))
            else:
                rows = conn.execute("SELECT * FROM cl_bridge_conflicts ORDER BY conflict_id")
            return [dict(row) for row in rows]

    def checkpoint(self, context: AccessContext, *, source: str) -> dict:
        """服务恢复点：最后确认的来源序列、等待缺口与当前流动性占用。"""
        context.require("read:clearing")
        with self.database.connect() as conn:
            cursor = conn.execute("SELECT * FROM cl_bridge_cursors WHERE source=?", (source,)).fetchone()
            keys = []
            for row in conn.execute(
                "SELECT source_key,"
                "MAX(CASE WHEN status!='waiting' THEN sequence ELSE -1 END) AS last_applied,"
                "MAX(CASE WHEN status='waiting' THEN sequence ELSE -1 END) AS first_waiting "
                "FROM cl_bridge_events WHERE source=? GROUP BY source_key ORDER BY source_key", (source,)):
                keys.append({"source_key": row["source_key"], "last_confirmed_sequence": int(row["last_applied"]), "next_expected_sequence": int(row["last_applied"]) + 1, "has_gap": int(row["first_waiting"]) >= 0})
            holdings = [dict(row) for row in conn.execute(
                "SELECT corridor_id,payer_org AS org_id,currency,SUM(amount_minor) AS held_minor "
                "FROM cl_instructions WHERE source=? AND status IN ('frozen','matched') GROUP BY corridor_id,payer_org,currency", (source,))]
            return {
                "source": source,
                "last_sequence": int(cursor["last_sequence"]) if cursor else -1,
                "last_confirmed_sequence": int(cursor["last_confirmed_sequence"]) if cursor else -1,
                "sources": keys,
                "liquidity_held": holdings,
            }

    # ------------------------------------------------------------- 批次关账

    def close_batch(self, context: AccessContext, *, batch_id: str, net_currency: str | None = None) -> dict:
        """批准窗口关账并放行批次；录入汇率的人不能批准。"""
        context.require("approve:clearing")
        with self.database.transaction() as conn:
            batch = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError("批次不存在")
            if batch["status"] == BATCH_PAUSED:
                raise ConflictError("批次处于暂停状态，不能关账")
            if batch["status"] in TERMINAL_BATCH_STATES:
                raise ConflictError("批次已经关账")
            window = self._require_window_row(conn, batch["window_id"])
            if parse_instant(self.clock.now()) < parse_instant(window["closes_at"]):
                raise ConflictError("窗口尚未到截止时间")
            rate_entered_by = {row["entered_by"] for row in conn.execute("SELECT entered_by FROM cl_rates WHERE window_id=?", (batch["window_id"],))}
            if context.actor_id in rate_entered_by:
                raise PermissionDenied("录入汇率的人不能批准窗口关账")
            positions = self._compute_positions(conn, batch_id)
            now = self.clock.now()
            conn.execute("UPDATE cl_batches SET status='closed',net_positions_json=?,closed_by=?,closed_at=? WHERE batch_id=?", (canonical_json(positions), context.actor_id, now, batch_id))
            conn.execute("UPDATE cl_instructions SET released_by=? WHERE batch_id=? AND status IN ('frozen','matched')", (context.actor_id, batch_id))
            self._audit(conn, context, "close_batch", batch["corridor_id"], {"batch_id": batch_id, "window_id": batch["window_id"], "net_positions": positions, "rate_id": batch["rate_id"]})
            self._maybe_close_window(conn, context, window["window_id"])
            return self._batch(conn, batch_id)

    def get_batch(self, context: AccessContext, batch_id: str) -> dict:
        context.require("read:clearing")
        with self.database.connect() as conn:
            batch = self._batch(conn, batch_id)
            return self._batch_view(conn, batch, context)

    def list_instructions(self, context: AccessContext, *, corridor_id: str, window_id: str | None = None) -> list[dict]:
        context.require("read:clearing")
        sql = "SELECT * FROM cl_instructions WHERE corridor_id=?"
        params: list[object] = [corridor_id]
        if window_id:
            sql += " AND window_id=?"; params.append(window_id)
        sql += " ORDER BY frozen_at,source_sequence"
        with self.database.connect() as conn:
            rows = [dict(row) for row in conn.execute(sql, params)]
            result = []
            for row in rows:
                if not self._sees_all(context) and context.org_id not in (row["payer_org"], row["payee_org"]):
                    continue
                result.append(self._instruction_view(conn, row, context))
            return result

    # ------------------------------------------------------- 已结算的调整

    def reverse_settled_batch(self, context: AccessContext, *, batch_id: str, new_window_id: str, reason: str) -> dict:
        """已结算批次不可抹除：在新窗口用反向分录调整。"""
        context.require("adjust:clearing")
        if not reason.strip():
            raise ValidationError("调整必须说明原因")
        with self.database.transaction() as conn:
            batch = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError("批次不存在")
            if batch["status"] != BATCH_SETTLED:
                raise ConflictError("只有已结算批次需要反向调整")
            new_window = self._require_window_row(conn, new_window_id)
            if new_window["corridor_id"] != batch["corridor_id"]:
                raise ValidationError("新窗口必须属于同一走廊")
            if new_window["status"] != "open":
                raise ConflictError("新窗口不可用")
            if not new_window["rate_id"]:
                raise ConflictError("新窗口尚未录入汇率快照，不能承接调整")
            dup = conn.execute("SELECT 1 FROM cl_adjustments WHERE original_batch_id=? AND status!='cancelled'", (batch_id,)).fetchone()
            if dup:
                raise ConflictError("该批次已经存在调整单")
            settled = conn.execute("SELECT * FROM cl_instructions WHERE batch_id=? AND status='settled' ORDER BY source_sequence", (batch_id,)).fetchall()
            reversal_entries: list[dict] = []
            new_instructions: list[str] = []
            adjustment_id = new_id("adjust")
            rate_id = new_window["rate_id"]
            for original in settled:
                for entry_column, leg in (("debit_entry_id", "debit"), ("credit_entry_id", "credit")):
                    original_entry = original[entry_column]
                    if not original_entry:
                        continue
                    reversal = self._ledger_reverse(conn, original_entry, reference=f"{adjustment_id}:{leg}", actor=context.actor_id)
                    reversal_entries.append(reversal)
                # 在新窗口重新冻结同一笔义务，净额批次重新计算
                new_instruction = self._freeze(
                    conn, context,
                    corridor_id=batch["corridor_id"], window_id=new_window_id,
                    obligation_ref=f"{original['obligation_ref']}#adj:{adjustment_id}",
                    payer_org=original["payee_org"], payee_org=original["payer_org"],
                    currency=original["currency"], amount_minor=original["amount_minor"],
                    source=original["source"], source_key=f"{original['source_key']}:adj:{adjustment_id}",
                    source_sequence=original["source_sequence"], rate_id=rate_id)
                new_instructions.append(new_instruction["instruction_id"])
            now = self.clock.now()
            conn.execute("INSERT INTO cl_adjustments(adjustment_id,corridor_id,original_batch_id,original_window_id,new_window_id,reason,status,reversal_entries_json,created_at,created_by) VALUES(?,?,?,?,?,?,?,?,?,?)", (adjustment_id, batch["corridor_id"], batch_id, batch["window_id"], new_window_id, reason.strip(), "reversed", canonical_json(reversal_entries), now, context.actor_id))
            self._audit(conn, context, "reverse_settled_batch", batch["corridor_id"], {"adjustment_id": adjustment_id, "original_batch_id": batch_id, "new_window_id": new_window_id, "reversed_instructions": len(settled)})
            return {"adjustment_id": adjustment_id, "status": "reversed", "reversal_entries": reversal_entries, "new_window_id": new_window_id, "new_instruction_ids": new_instructions}

    # ------------------------------------------------------------- 解释查询

    def explain(self, context: AccessContext, *, obligation_ref: str) -> dict:
        """解释某笔义务落入哪个窗口、采用哪份汇率、占用多少额度、由谁放行。"""
        context.require("read:clearing")
        with self.database.connect() as conn:
            rows = conn.execute("SELECT * FROM cl_instructions WHERE obligation_ref=? OR obligation_ref LIKE ? ORDER BY frozen_at", (obligation_ref, obligation_ref + "#adj:%")).fetchall()
            if not rows:
                raise NotFoundError("找不到该义务的指令")
            explanations = []
            for row in rows:
                instruction = dict(row)
                window = conn.execute("SELECT * FROM cl_windows WHERE window_id=?", (row["window_id"],)).fetchone()
                rate = conn.execute("SELECT * FROM cl_rates WHERE rate_id=?", (row["rate_id"],)).fetchone() if row["rate_id"] else None
                batch = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (row["batch_id"],)).fetchone() if row["batch_id"] else None
                limit_row = conn.execute("SELECT * FROM cl_limits WHERE corridor_id=? AND org_id=? AND currency=? AND version=? ", (row["corridor_id"], row["payer_org"], row["currency"], row["limit_version"])).fetchone() if row["limit_version"] is not None else None
                explanations.append({
                    "instruction_id": row["instruction_id"],
                    "obligation_ref": row["obligation_ref"],
                    "window_id": row["window_id"],
                    "window_seq": window["seq"] if window else None,
                    "window_opens_at": window["opens_at"] if window else None,
                    "window_closes_at": window["closes_at"] if window else None,
                    "batch_id": row["batch_id"],
                    "batch_status": batch["status"] if batch else None,
                    "rate_id": row["rate_id"],
                    "rate_version": rate["version"] if rate else None,
                    "rate": rate["rate"] if rate else None,
                    "currency": row["currency"],
                    "amount_minor": row["amount_minor"],
                    "limit_version": row["limit_version"],
                    "limit_amount_minor": limit_row["amount_minor"] if limit_row else None,
                    "occupied_after_freeze_minor": row["usage_at_freeze_minor"],
                    "status": row["status"],
                    "frozen_by": row["frozen_by"],
                    "released_by": row["released_by"],
                    "settled_by": batch["settled_by"] if batch else "",
                    "debit_entry_id": row["debit_entry_id"],
                    "credit_entry_id": row["credit_entry_id"],
                })
            visible = []
            for r in rows:
                if self._sees_all(context) or context.org_id in (r["payer_org"], r["payee_org"]):
                    visible.append(self._instruction_view(conn, dict(r), context))
                else:
                    visible.append({"instruction_id": r["instruction_id"], "visible": False})
            if self._sees_all(context):
                trace = [dict(e, viewer=v) for e, v in zip(explanations, visible)]
            else:
                # 参与机构视角：窗口/汇率/本方额度占用可解释，对手方限额与未授权机构字段遮蔽
                trace = []
                for explanation, view in zip(explanations, visible):
                    if view.get("visible") is False:
                        trace.append(view); continue
                    item = {k: explanation[k] for k in (
                        "instruction_id", "obligation_ref", "window_id", "window_seq", "window_opens_at", "window_closes_at",
                        "batch_id", "batch_status", "rate_id", "rate_version", "rate", "currency", "amount_minor",
                        "occupied_after_freeze_minor", "status", "frozen_by", "released_by", "settled_by",
                        "debit_entry_id", "credit_entry_id")}
                    item["payer_org"] = view["payer_org"]
                    item["payee_org"] = view["payee_org"]
                    # 限额是付款方本方数据：仅付款方可见，收款方只见遮蔽
                    item["limit_version"] = explanation["limit_version"]
                    item["limit_amount_minor"] = explanation["limit_amount_minor"] if context.org_id == view.get("payer_org") else "***"
                    trace.append(item)
            return {"obligation_ref": obligation_ref, "trace": trace}

    # ================================================================ 内部

    def _apply_event(self, conn, context: AccessContext, source: str, source_key: str, sequence: int, event_type: str, payload: dict, digest: str, occurred_at: str) -> dict:
        now = self.clock.now()
        if event_type == EVENT_RECEIVED:
            effect = self._apply_received(conn, context, source, source_key, sequence, payload, occurred_at)
        else:
            instr = conn.execute("SELECT * FROM cl_instructions WHERE source=? AND source_key=?", (source, source_key)).fetchone()
            if not instr:
                raise ConflictError(f"{event_type} 事件缺少已受理的指令")
            effect = self._apply_lifecycle(conn, context, instr, event_type, payload, occurred_at)
        conn.execute("INSERT INTO cl_bridge_events(source,source_key,sequence,event_type,payload_digest,payload_json,occurred_at,received_at,status,instruction_id,window_id,batch_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (source, source_key, sequence, event_type, digest, canonical_json(payload), occurred_at, now, "applied", effect.get("instruction_id"), effect.get("window_id"), effect.get("batch_id")))
        self._advance_cursor(conn, source, sequence)
        effect.update({"status": "applied", "source": source, "source_key": source_key, "sequence": sequence})
        return effect

    def _apply_received(self, conn, context: AccessContext, source: str, source_key: str, sequence: int, payload: dict, occurred_at: str) -> dict:
        required = ("corridor_id", "obligation_ref", "payer_org", "payee_org", "currency", "amount")
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValidationError("受理事件缺少字段: " + ", ".join(missing))
        corridor_id = require_safe(str(payload["corridor_id"]), "走廊标识")
        obligation_ref = require_safe(str(payload["obligation_ref"]), "合同义务号")
        payer = require_safe(str(payload["payer_org"]), "付款机构")
        payee = require_safe(str(payload["payee_org"]), "收款机构")
        currency = require_safe(str(payload["currency"]), "币种")
        if payer == payee:
            raise ValidationError("付款方与收款方不能相同")
        amount_minor = to_minor(payload["amount"])
        if amount_minor <= 0:
            raise ValidationError("结算金额必须大于零")
        self._require_corridor(conn, corridor_id, active=True)
        self._require_counterparties(conn, corridor_id, payer, payee)
        # 同一笔义务不能重复计入：活跃指令拒绝重复受理，已结算只能反向调整
        prior = conn.execute("SELECT status,batch_id FROM cl_instructions WHERE corridor_id=? AND obligation_ref=?", (corridor_id, obligation_ref)).fetchone()
        if prior and prior["status"] in ACTIVE_INSTRUCTION_STATES:
            raise ConflictError("该合同义务已经冻结在窗口中，不能重复受理")
        if prior and prior["status"] == INSTR_SETTLED:
            raise ConflictError("该义务已经结算，纠正只能使用反向调整")
        window_id, late = self._choose_window(conn, corridor_id, occurred_at)
        window = self._require_window_row(conn, window_id)
        if not window["rate_id"]:
            raise ConflictError("窗口汇率快照尚未录入，无法冻结")
        frozen = self._freeze(conn, context, corridor_id=corridor_id, window_id=window_id, obligation_ref=obligation_ref, payer_org=payer, payee_org=payee, currency=currency, amount_minor=amount_minor, source=source, source_key=source_key, source_sequence=sequence, rate_id=window["rate_id"])
        self._audit(conn, context, "freeze_instruction", corridor_id, {"instruction_id": frozen["instruction_id"], "obligation_ref": obligation_ref, "window_id": window_id, "batch_id": frozen["batch_id"], "late": late, "amount_minor": amount_minor})
        frozen["late"] = late
        return frozen

    def _apply_lifecycle(self, conn, context: AccessContext, instr_row, event_type: str, payload: dict, occurred_at: str) -> dict:
        instruction = dict(instr_row)
        instruction_id = instruction["instruction_id"]
        batch = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (instruction["batch_id"],)).fetchone()
        if event_type in (EVENT_MATCHED, EVENT_SETTLED, EVENT_RETURNED) and batch["status"] == BATCH_PAUSED:
            raise ConflictError("批次已暂停，桥侧事件必须等待恢复")
        current = instruction["status"]
        if event_type == EVENT_MATCHED:
            if current != INSTR_FROZEN:
                raise ConflictError("只有已冻结指令可以匹配")
            if batch["status"] not in (BATCH_GATHERING,):
                raise ConflictError("批次已关账或暂停，不能再匹配新指令")
            conn.execute("UPDATE cl_instructions SET status='matched',matched_at=? WHERE instruction_id=?", (self.clock.now(), instruction_id))
        elif event_type == EVENT_SETTLED:
            if current != INSTR_MATCHED:
                raise ConflictError("只有已匹配指令可以结算")
            if batch["status"] != BATCH_CLOSED:
                raise ConflictError("批次未经关账放行，不能结算")
            self._settle_instruction(conn, context, instruction, batch)
        elif event_type in (EVENT_RETURNED, EVENT_CANCELLED):
            if batch["status"] in (BATCH_CLOSED, BATCH_SETTLED):
                raise ConflictError("批次已经关账放行，退回或撤销只能通过反向调整")
            if event_type == EVENT_RETURNED:
                if current not in (INSTR_FROZEN, INSTR_MATCHED):
                    raise ConflictError("只有未结算指令可以退回")
                self._release_instruction(conn, context, instruction, INSTR_RETURNED, "bridge_returned")
            else:
                if current != INSTR_FROZEN:
                    raise ConflictError("只有冻结未匹配的指令可以撤销")
                self._release_instruction(conn, context, instruction, INSTR_CANCELLED, "bridge_cancelled")
        return {"instruction_id": instruction_id, "window_id": instruction["window_id"], "batch_id": instruction["batch_id"], "instruction_status": conn.execute("SELECT status FROM cl_instructions WHERE instruction_id=?", (instruction_id,)).fetchone()["status"]}

    def _drain_waiting(self, context: AccessContext, source: str, source_key: str) -> None:
        # 缺口补齐后，缓存事件逐个在独立事务中按序推进；
        # 某个事件暂时无法应用时停在该缺口，已确认的补缺与前序事件不受影响。
        while True:
            with self.database.connect() as probe:
                last = probe.execute("SELECT COALESCE(MAX(sequence),-1) AS s FROM cl_bridge_events WHERE source=? AND source_key=? AND status!='waiting'", (source, source_key)).fetchone()["s"]
                waiting = probe.execute("SELECT * FROM cl_bridge_events WHERE source=? AND source_key=? AND status='waiting' AND sequence=? ORDER BY sequence LIMIT 1", (source, source_key, int(last) + 1)).fetchone()
            if not waiting:
                return
            payload = json.loads(waiting["payload_json"])
            try:
                with self.database.transaction() as conn:
                    removed = conn.execute("DELETE FROM cl_bridge_events WHERE source=? AND source_key=? AND sequence=? AND status='waiting'", (source, source_key, waiting["sequence"])).rowcount
                    if not removed:
                        return
                    self._apply_event(conn, context, source, source_key, waiting["sequence"], waiting["event_type"], payload, waiting["payload_digest"], waiting["occurred_at"])
            except (ConflictError, ValidationError):
                # 缓存事件暂时无法应用：事务回滚后其 waiting 记录保留，停在此缺口等待人工核查
                return

    def _freeze(self, conn, context: AccessContext, *, corridor_id: str, window_id: str, obligation_ref: str, payer_org: str, payee_org: str, currency: str, amount_minor: int, source: str, source_key: str, source_sequence: int, rate_id: str) -> dict:
        balance = self._balance(conn, corridor_id, payer_org, currency)
        if balance < amount_minor:
            raise ConflictError(f"机构 {payer_org} 可用流动性不足")
        # 限额按窗口计量：每个清算窗口有独立的参与方额度
        window_used = self._window_limit_used(conn, corridor_id, window_id, payer_org, currency)
        limit = conn.execute("SELECT amount_minor,version FROM cl_limits WHERE corridor_id=? AND org_id=? AND currency=? ORDER BY version DESC LIMIT 1", (corridor_id, payer_org, currency)).fetchone()
        if not limit:
            raise ConflictError(f"机构 {payer_org} 尚未设置 {currency} 限额")
        if window_used + amount_minor > int(limit["amount_minor"]):
            raise ConflictError(f"机构 {payer_org} 在本窗口限额不足：已占用 {window_used}，申请 {amount_minor}，限额 {limit['amount_minor']}")
        batch = self._get_or_create_batch(conn, context, corridor_id, window_id, currency, rate_id)
        instruction_id = new_id("instr")
        hold_id = new_id("liq")
        now = self.clock.now()
        conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (hold_id, corridor_id, payer_org, currency, -amount_minor, "hold", instruction_id, now, context.actor_id))
        conn.execute("INSERT INTO cl_instructions(instruction_id,corridor_id,window_id,batch_id,obligation_ref,payer_org,payee_org,currency,amount_minor,status,source,source_key,source_sequence,rate_id,limit_version,usage_at_freeze_minor,frozen_at,frozen_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (instruction_id, corridor_id, window_id, batch["batch_id"], obligation_ref, payer_org, payee_org, currency, amount_minor, INSTR_FROZEN, source, source_key, source_sequence, rate_id, limit["version"], window_used + amount_minor, now, context.actor_id))
        conn.execute("INSERT INTO cl_batch_members(member_id,batch_id,window_id,instruction_id,source_sequence,amount_minor,status,role,added_at) VALUES(?,?,?,?,?,?,?,?,?)", (new_id("member"), batch["batch_id"], window_id, instruction_id, source_sequence, amount_minor, "active", "leg", now))
        self._refresh_batch_totals(conn, batch["batch_id"])
        return {"instruction_id": instruction_id, "window_id": window_id, "batch_id": batch["batch_id"], "amount_minor": amount_minor, "occupied_minor": window_used + amount_minor, "limit_version": limit["version"], "rate_id": rate_id, "instruction_status": INSTR_FROZEN}

    def _release_instruction(self, conn, context: AccessContext, instruction: dict, new_status: str, reason: str) -> None:
        now = self.clock.now()
        conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (new_id("liq"), instruction["corridor_id"], instruction["payer_org"], instruction["currency"], instruction["amount_minor"], "release", instruction["instruction_id"], now, context.actor_id))
        timestamp_column = "returned_at" if new_status == INSTR_RETURNED else "cancelled_at"
        conn.execute(f"UPDATE cl_instructions SET status=?,{timestamp_column}=? WHERE instruction_id=?", (new_status, now, instruction["instruction_id"]))
        conn.execute("UPDATE cl_batch_members SET status=?,removed_at=?,removal_reason=? WHERE instruction_id=? AND status='active'", (new_status, now, reason, instruction["instruction_id"]))
        self._refresh_batch_totals(conn, instruction["batch_id"])
        self._audit(conn, context, new_status, instruction["corridor_id"], {"instruction_id": instruction["instruction_id"], "batch_id": instruction["batch_id"]})

    def _settle_instruction(self, conn, context: AccessContext, instruction: dict, batch) -> None:
        now = self.clock.now()
        reference_base = instruction["instruction_id"]
        # 不可变资金分录：一借一贷，reference 唯一保证重放不重复入账
        debit_id = self._ledger_post(conn, journal_key=instruction["corridor_id"], account=f"liquidity:{instruction['payer_org']}", currency=instruction["currency"], amount_minor=instruction["amount_minor"], direction="debit", reference=f"{reference_base}:debit", actor=batch["closed_by"] or context.actor_id)
        credit_id = self._ledger_post(conn, journal_key=instruction["corridor_id"], account=f"liquidity:{instruction['payee_org']}", currency=instruction["currency"], amount_minor=instruction["amount_minor"], direction="credit", reference=f"{reference_base}:credit", actor=batch["closed_by"] or context.actor_id)
        # 冻结占用转为最终扣划：付款方释放 hold 后记 settle 扣划，收款方同额入账
        conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (new_id("liq"), instruction["corridor_id"], instruction["payer_org"], instruction["currency"], instruction["amount_minor"], "release", reference_base, now, context.actor_id))
        conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (new_id("liq"), instruction["corridor_id"], instruction["payer_org"], instruction["currency"], -instruction["amount_minor"], "settle", reference_base, now, context.actor_id))
        conn.execute("INSERT INTO cl_liquidity_entries(entry_id,corridor_id,org_id,currency,amount_minor,kind,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (new_id("liq"), instruction["corridor_id"], instruction["payee_org"], instruction["currency"], instruction["amount_minor"], "settle", reference_base, now, context.actor_id))
        conn.execute("UPDATE cl_instructions SET status='settled',settled_at=?,debit_entry_id=?,credit_entry_id=? WHERE instruction_id=?", (now, debit_id, credit_id, instruction["instruction_id"]))
        conn.execute("UPDATE cl_batch_members SET status='settled' WHERE instruction_id=? AND status='active'", (instruction["instruction_id"],))
        self._refresh_batch_totals(conn, instruction["batch_id"])
        remaining = conn.execute("SELECT COUNT(*) AS n FROM cl_batch_members WHERE batch_id=? AND status='active'", (instruction["batch_id"],)).fetchone()["n"]
        if remaining == 0:
            conn.execute("UPDATE cl_batches SET status='settled',settled_by=?,settled_at=? WHERE batch_id=? AND status='closed'", (context.actor_id, now, instruction["batch_id"]))
            conn.execute("UPDATE cl_windows SET settled_by=?,settled_at=? WHERE window_id=?", (context.actor_id, now, batch["window_id"]))
            self._audit(conn, context, "settle_batch", instruction["corridor_id"], {"batch_id": instruction["batch_id"], "window_id": batch["window_id"]})

    def _choose_window(self, conn, corridor_id: str, occurred_at: str) -> tuple[str, bool]:
        """选择当前可用窗口；迟到事件落入下一可用窗口。返回 (window_id, 是否迟到)。"""
        intended = conn.execute("SELECT * FROM cl_windows WHERE corridor_id=? AND opens_at<=? AND closes_at>? AND status='open' ORDER BY closes_at LIMIT 1", (corridor_id, occurred_at, occurred_at)).fetchone()
        now = self.clock.now()
        if intended:
            if parse_instant(now) < parse_instant(intended["closes_at"]):
                return intended["window_id"], False
            late = True
        else:
            late = parse_instant(occurred_at) < parse_instant(now)
        target = conn.execute("SELECT * FROM cl_windows WHERE corridor_id=? AND status='open' AND closes_at>? ORDER BY opens_at LIMIT 1", (corridor_id, now)).fetchone()
        if not target:
            raise ConflictError("没有可用的清算窗口，事件只能等待下一窗口")
        return target["window_id"], late

    def _get_or_create_batch(self, conn, context: AccessContext, corridor_id: str, window_id: str, currency: str, rate_id: str) -> dict:
        row = conn.execute("SELECT * FROM cl_batches WHERE window_id=? AND currency=?", (window_id, currency)).fetchone()
        if row:
            if row["status"] == BATCH_PAUSED:
                raise ConflictError("该币种批次已暂停，新指令必须等待恢复")
            return dict(row)
        seq = int(conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS n FROM cl_batches WHERE corridor_id=?", (corridor_id,)).fetchone()["n"])
        batch_id = new_id("batch")
        conn.execute("INSERT INTO cl_batches(batch_id,corridor_id,window_id,currency,seq,status,rate_id,total_amount_minor,net_positions_json,created_at,created_by) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (batch_id, corridor_id, window_id, currency, seq, BATCH_GATHERING, rate_id, 0, "{}", self.clock.now(), context.actor_id))
        return dict(conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone())

    def _refresh_batch_totals(self, conn, batch_id: str) -> None:
        row = conn.execute("SELECT COALESCE(SUM(amount_minor),0) AS total FROM cl_batch_members WHERE batch_id=? AND status='active'", (batch_id,)).fetchone()
        conn.execute("UPDATE cl_batches SET total_amount_minor=? WHERE batch_id=?", (int(row["total"]), batch_id))

    def _compute_positions(self, conn, batch_id: str) -> dict:
        positions: dict[str, int] = {}
        for row in conn.execute("SELECT i.payer_org AS payer,i.payee_org AS payee,m.amount_minor AS amount FROM cl_batch_members m JOIN cl_instructions i ON i.instruction_id=m.instruction_id WHERE m.batch_id=? AND m.status='active'", (batch_id,)):
            positions[row["payer"]] = positions.get(row["payer"], 0) - int(row["amount"])
            positions[row["payee"]] = positions.get(row["payee"], 0) + int(row["amount"])
        return positions

    def _maybe_close_window(self, conn, context: AccessContext, window_id: str) -> None:
        window = self._require_window_row(conn, window_id)
        pending = conn.execute("SELECT COUNT(*) AS n FROM cl_batches WHERE window_id=? AND status IN ('gathering','paused')", (window_id,)).fetchone()["n"]
        if pending == 0 and parse_instant(self.clock.now()) >= parse_instant(window["closes_at"]):
            conn.execute("UPDATE cl_windows SET status='closed',closed_by=?,closed_at=?,approved_by=?,approved_at=? WHERE window_id=? AND status='open'", (context.actor_id, self.clock.now(), context.actor_id, self.clock.now(), window_id))

    def _pause_affected(self, conn, context: AccessContext, reason: str, sql: str, params: tuple, *, detail: dict) -> None:
        for row in conn.execute(sql, params):
            self._pause_batch_row(conn, context, row["batch_id"], reason, reason, detail)

    def _pause_batch_row(self, conn, context: AccessContext, batch_id: str, reason: str, label: str, detail: dict) -> None:
        row = conn.execute("SELECT status FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if not row:
            raise NotFoundError("批次不存在")
        if row["status"] in (BATCH_CLOSED, BATCH_SETTLED):
            raise ConflictError("已关账或已结算批次不能暂停，只能反向调整")
        changed = conn.execute("UPDATE cl_batches SET status='paused',pause_reason=?,paused_by=?,paused_at=? WHERE batch_id=? AND status!='paused'", (reason, context.actor_id, self.clock.now(), batch_id)).rowcount
        if changed:
            corridor = conn.execute("SELECT corridor_id FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()["corridor_id"]
            self._audit(conn, context, "pause_batch", corridor, {"batch_id": batch_id, "reason": reason, "label": label, **detail})

    # -------------------------------------------------------------- 只读组装

    def _corridor(self, conn, corridor_id: str) -> dict:
        row = conn.execute("SELECT * FROM cl_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
        if not row:
            raise NotFoundError("走廊不存在")
        return dict(row)

    def _require_corridor(self, conn, corridor_id: str, *, active: bool = False) -> None:
        row = conn.execute("SELECT status FROM cl_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
        if not row:
            raise NotFoundError("走廊不存在")
        if active and row["status"] != "active":
            raise ConflictError("走廊未处于启用状态")

    def _require_participant(self, conn, corridor_id: str, org_id: str) -> None:
        row = conn.execute("SELECT status FROM cl_participants WHERE corridor_id=? AND org_id=?", (corridor_id, org_id)).fetchone()
        if not row:
            raise NotFoundError(f"机构 {org_id} 未加入走廊")
        if row["status"] != "active":
            raise ConflictError(f"机构 {org_id} 未处于参与状态")

    def _require_counterparties(self, conn, corridor_id: str, payer: str, payee: str) -> None:
        self._require_participant(conn, corridor_id, payer)
        self._require_participant(conn, corridor_id, payee)
        row = conn.execute("SELECT authorized_counterparties_json FROM cl_participants WHERE corridor_id=? AND org_id=?", (corridor_id, payer)).fetchone()
        if payee not in json.loads(row["authorized_counterparties_json"]):
            raise PermissionDenied(f"机构 {payer} 未被授权与 {payee} 交易")

    def _require_window_row(self, conn, window_id: str):
        row = conn.execute("SELECT * FROM cl_windows WHERE window_id=?", (window_id,)).fetchone()
        if not row:
            raise NotFoundError("清算窗口不存在")
        return row

    def _window(self, conn, window_id: str) -> dict:
        row = self._require_window_row(conn, window_id)
        result = dict(row)
        result["batches"] = [self._batch(conn, b["batch_id"]) for b in conn.execute("SELECT batch_id FROM cl_batches WHERE window_id=? ORDER BY currency", (window_id,))]
        return result

    def _batch(self, conn, batch_id: str) -> dict:
        row = conn.execute("SELECT * FROM cl_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if not row:
            raise NotFoundError("批次不存在")
        result = dict(row)
        result["net_positions"] = json.loads(row["net_positions_json"])
        result.pop("net_positions_json", None)
        return result

    def _window_view(self, conn, window: dict, context: AccessContext) -> dict:
        result = dict(window)
        if self._sees_all(context):
            return result
        result["batches"] = [self._batch_view(conn, b, context) for b in window["batches"]]
        return result

    @staticmethod
    def _sees_all(context: AccessContext) -> bool:
        # 平台运营方（通配权限或不带机构身份的清算操作角色）可见全部；携带机构身份者按本方过滤
        return context.is_superuser or context.org_id is None

    def _batch_view(self, conn, batch: dict, context: AccessContext) -> dict:
        if self._sees_all(context):
            return batch
        result = dict(batch)
        if context.org_id:
            authorized = set()
            row = conn.execute("SELECT authorized_counterparties_json FROM cl_participants WHERE corridor_id=? AND org_id=?", (batch["corridor_id"], context.org_id)).fetchone()
            if row:
                authorized = set(json.loads(row["authorized_counterparties_json"]))
            positions = {org: value for org, value in batch.get("net_positions", {}).items() if org == context.org_id or org in authorized}
            result["net_positions"] = positions
        return result

    def _instruction_view(self, conn, instruction: dict, context: AccessContext) -> dict:
        result = dict(instruction)
        if self._sees_all(context):
            return result
        viewer = context.org_id
        if viewer not in (instruction["payer_org"], instruction["payee_org"]):
            raise PermissionDenied("只能查看本方参与的指令")
        own_is_payer = viewer == instruction["payer_org"]
        other_side = instruction["payee_org"] if own_is_payer else instruction["payer_org"]
        row = conn.execute("SELECT authorized_counterparties_json FROM cl_participants WHERE corridor_id=? AND org_id=?", (instruction["corridor_id"], viewer)).fetchone()
        authorized = set(json.loads(row["authorized_counterparties_json"])) if row else set()
        if other_side not in authorized:
            if own_is_payer:
                result["payee_org"] = "***"
            else:
                result["payer_org"] = "***"
        return result

    def _replay_result(self, conn, event_row) -> dict:
        result = {"status": "duplicate", "replayed": True, "source": event_row["source"], "source_key": event_row["source_key"], "sequence": event_row["sequence"], "window_id": event_row["window_id"], "batch_id": event_row["batch_id"], "instruction_id": event_row["instruction_id"]}
        if event_row["instruction_id"]:
            instr = conn.execute("SELECT status FROM cl_instructions WHERE instruction_id=?", (event_row["instruction_id"],)).fetchone()
            result["instruction_status"] = instr["status"] if instr else None
        return result

    def _advance_cursor(self, conn, source: str, sequence: int) -> None:
        conn.execute("INSERT INTO cl_bridge_cursors(source,last_sequence,last_confirmed_sequence,updated_at) VALUES(?,? ,?,?) ON CONFLICT(source) DO UPDATE SET last_sequence=MAX(excluded.last_sequence,cl_bridge_cursors.last_sequence),last_confirmed_sequence=MAX(excluded.last_confirmed_sequence,cl_bridge_cursors.last_confirmed_sequence),updated_at=excluded.updated_at", (source, sequence, sequence, self.clock.now()))

    def _balance(self, conn, corridor_id: str, org_id: str, currency: str) -> int:
        row = conn.execute("SELECT COALESCE(SUM(amount_minor),0) AS v FROM cl_liquidity_entries WHERE corridor_id=? AND org_id=? AND currency=?", (corridor_id, org_id, currency)).fetchone()
        return int(row["v"])

    def _occupied(self, conn, corridor_id: str, org_id: str, currency: str) -> int:
        row = conn.execute("SELECT COALESCE(SUM(amount_minor),0) AS v FROM cl_instructions WHERE corridor_id=? AND payer_org=? AND currency=? AND status IN ('frozen','matched')", (corridor_id, org_id, currency)).fetchone()
        return int(row["v"])

    def _window_limit_used(self, conn, corridor_id: str, window_id: str, org_id: str, currency: str) -> int:
        """某机构在指定窗口内对限额的占用（冻结+匹配）。"""
        row = conn.execute("SELECT COALESCE(SUM(amount_minor),0) AS v FROM cl_instructions WHERE corridor_id=? AND window_id=? AND payer_org=? AND currency=? AND status IN ('frozen','matched')", (corridor_id, window_id, org_id, currency)).fetchone()
        return int(row["v"])

    def _ledger_post(self, conn, *, journal_key: str, account: str, currency: str, amount_minor: int, direction: str, reference: str, actor: str) -> str:
        entry_id = new_id("entry")
        conn.execute("INSERT INTO journal_entries(entry_id,journal_key,account,currency,amount_minor,direction,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (entry_id, journal_key, account, currency, amount_minor, direction, reference, self.clock.now(), actor))
        return entry_id

    def _ledger_reverse(self, conn, entry_id: str, *, reference: str, actor: str) -> dict:
        row = conn.execute("SELECT * FROM journal_entries WHERE entry_id=?", (entry_id,)).fetchone()
        existing = conn.execute("SELECT entry_id FROM journal_entries WHERE reversed_entry_id=?", (entry_id,)).fetchone()
        if existing:
            return {"entry_id": existing["entry_id"], "replayed": True, "original_entry_id": entry_id}
        reversal = new_id("entry")
        direction = "credit" if row["direction"] == "debit" else "debit"
        conn.execute("INSERT INTO journal_entries(entry_id,journal_key,account,currency,amount_minor,direction,reference,reversed_entry_id,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?,?)", (reversal, row["journal_key"], row["account"], row["currency"], row["amount_minor"], direction, reference, entry_id, self.clock.now(), actor))
        return {"entry_id": reversal, "replayed": False, "original_entry_id": entry_id}

    @staticmethod
    def _require_own_org(context: AccessContext, org_id: str) -> None:
        if context.is_superuser:
            return
        if not context.org_id or context.org_id != org_id:
            raise PermissionDenied("参与机构只能查看或操作本方记录")

    def _audit(self, conn, context: AccessContext, action: str, corridor_id: str, detail: dict) -> None:
        self.audit.append(conn, actor_id=context.actor_id, action=action, entity_type="clearing", entity_id=corridor_id, version=1, detail=detail)
