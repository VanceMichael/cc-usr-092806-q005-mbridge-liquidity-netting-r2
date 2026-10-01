"""清算走廊登记、限额与汇率版本、窗口生命周期和可解释查询。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, InvariantViolation, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .jsonutil import canonical_json
from .ledger import to_minor
from .timeutil import Clock, canonical_instant, parse_instant

WINDOW_STATES = ("open", "on_hold", "ready", "closed")
BATCH_STATUSES = ("building", "on_hold", "ready", "settled", "returned", "cancelled", "adjusted")
HOLD_STATUSES = ("held", "settled", "released", "cancelled")
INSTRUCTION_STATES = ("received", "frozen", "settled", "returned", "cancelled")
COMPLIANCE_REASONS = ("limit_change", "fx_revision", "compliance_hit")


def parse_rate(value: str) -> Decimal:
    try:
        rate = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValidationError("汇率格式错误") from exc
    if not rate.is_finite() or rate <= 0:
        raise ValidationError("汇率必须是有限正数")
    return rate


def convert_minor(amount_minor: int, rate: Decimal, *, exponent: int = 2) -> int:
    quantum = Decimal(1).scaleb(-exponent)
    return int((Decimal(amount_minor) * rate).quantize(quantum).scaleb(exponent))


@dataclass(frozen=True)
class ClearingHouse:
    """走廊、窗口、限额与汇率的登记册，并提供机构可见的解释视图。"""

    database: Database
    clock: Clock
    audit: AuditLog

    # ------------------------------------------------------------------ 走廊

    def register_corridor(self, context, *, corridor_code: str, currency_pay: str,
                          currency_receive: str, window_duration_minutes: int,
                          cutoff_offset_minutes: int = 0) -> dict:
        context.require("write:corridors")
        require_safe(corridor_code, "走廊编码")
        require_safe(currency_pay, "币种")
        require_safe(currency_receive, "币种")
        if currency_pay == currency_receive:
            raise ValidationError("走廊两端币种不能相同")
        if window_duration_minutes < 1 or window_duration_minutes > 24 * 60 * 7:
            raise ValidationError("窗口时长必须在 1 分钟到 7 天之间")
        if cutoff_offset_minutes < 0 or cutoff_offset_minutes >= window_duration_minutes:
            raise ValidationError("截止提前量必须在 0 到窗口时长之间")
        corridor_id = new_id("corridor")
        now = self.clock.now()
        with self.database.transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO clearing_corridors(corridor_id,corridor_code,currency_pay,currency_receive,window_duration_minutes,cutoff_offset_minutes,state,created_at,created_by) VALUES(?,?,?,?,?,?, 'active',?,?)",
                    (corridor_id, corridor_code, currency_pay, currency_receive,
                     window_duration_minutes, cutoff_offset_minutes, now, context.actor_id))
            except Exception as exc:  # 唯一编码冲突
                raise ConflictError("走廊编码已存在") from exc
            self.audit.append(connection, actor_id=context.actor_id, action="corridor.register",
                              entity_type="clearing_corridors", entity_id=corridor_id, version=1,
                              detail={"corridor_code": corridor_code, "currency_pay": currency_pay,
                                      "currency_receive": currency_receive,
                                      "window_duration_minutes": window_duration_minutes})
        return self.get_corridor(context, corridor_id)

    def add_participant(self, context, *, corridor_id: str, organization_id: str,
                        authorized_counterparties: list[str]) -> dict:
        context.require("write:corridors")
        self._require_corridor(corridor_id)
        counterparties = sorted({require_safe(c, "对手方标识") for c in authorized_counterparties})
        if organization_id in counterparties:
            raise ValidationError("参与方不能把自己登记为对手方")
        now = self.clock.now()
        with self.database.transaction() as connection:
            changed = connection.execute(
                "INSERT OR IGNORE INTO clearing_participants(corridor_id,organization_id,state,authorized_counterparties_json,added_at,added_by) VALUES(?,?, 'active',?,?,?)",
                (corridor_id, organization_id, canonical_json(counterparties), now, context.actor_id)).rowcount
            if not changed:
                connection.execute(
                    "UPDATE clearing_participants SET state='active',authorized_counterparties_json=?,added_at=?,added_by=? WHERE corridor_id=? AND organization_id=?",
                    (canonical_json(counterparties), now, context.actor_id, corridor_id, organization_id))
            self.audit.append(connection, actor_id=context.actor_id, action="participant.add",
                              entity_type="clearing_participants", entity_id=organization_id, version=1,
                              detail={"corridor_id": corridor_id, "counterparties": counterparties})
        return self.get_participant(context, corridor_id, organization_id)

    def get_corridor(self, context, corridor_id: str) -> dict:
        context.require("read:corridors")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
            if not row:
                raise NotFoundError("走廊不存在")
            return dict(row)

    def get_participant(self, context, corridor_id: str, organization_id: str) -> dict:
        context.require("read:corridors")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_participants WHERE corridor_id=? AND organization_id=?",
                                     (corridor_id, organization_id)).fetchone()
            if not row:
                raise NotFoundError("参与方未登记")
            result = dict(row)
            result["authorized_counterparties"] = json.loads(result.pop("authorized_counterparties_json"))
            return result

    # ------------------------------------------------------------------ 限额

    def set_limit(self, context, *, corridor_id: str, organization_id: str, currency: str,
                  amount: str, effective_at: str | None = None) -> dict:
        context.require("write:limits")
        corridor = self._require_corridor(corridor_id)
        if currency not in (corridor["currency_pay"], corridor["currency_receive"]):
            raise ValidationError("限额币种不在走廊币种范围内")
        self._require_participant(corridor_id, organization_id)
        minor = to_minor(amount)
        if minor <= 0:
            raise ValidationError("限额必须大于零")
        effective_at = canonical_instant(effective_at or self.clock.now())
        limit_id = new_id("limit")
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(version),0) AS v FROM clearing_limits WHERE corridor_id=? AND organization_id=? AND currency=?",
                (corridor_id, organization_id, currency)).fetchone()
            version = int(row["v"]) + 1
            now = self.clock.now()
            connection.execute(
                "INSERT INTO clearing_limits(limit_id,corridor_id,organization_id,currency,amount_minor,version,effective_at,registered_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (limit_id, corridor_id, organization_id, currency, minor, version, effective_at, context.actor_id, now))
            self.audit.append(connection, actor_id=context.actor_id, action="limit.register",
                              entity_type="clearing_limits", entity_id=limit_id, version=version,
                              detail={"corridor_id": corridor_id, "organization_id": organization_id,
                                      "currency": currency, "amount_minor": minor, "effective_at": effective_at})
            # 关账前生效的限额变化暂停已占用该限额的开放批次
            held_batches = connection.execute(
                "SELECT DISTINCT h.batch_id FROM clearing_holds h JOIN clearing_windows w ON w.window_id=h.window_id "
                "WHERE h.corridor_id=? AND h.organization_id=? AND h.currency=? AND h.status='held' AND w.state IN ('open','on_hold')",
                (corridor_id, organization_id, currency)).fetchall()
            for held in held_batches:
                self._hold_batch(connection, held["batch_id"], "limit_change", context.actor_id)
        return self.get_limit(context, limit_id)

    def get_limit(self, context, limit_id: str) -> dict:
        context.require("read:limits")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_limits WHERE limit_id=?", (limit_id,)).fetchone()
            if not row:
                raise NotFoundError("限额版本不存在")
            limit = dict(row)
        # 参与机构只能看到本方限额
        if context.org_id is not None and limit["organization_id"] != context.org_id:
            raise PermissionDenied("只能查看本方限额")
        return limit

    def current_limit(self, corridor_id: str, organization_id: str, currency: str, *, as_of: str) -> dict | None:
        instant = canonical_instant(as_of)
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM clearing_limits WHERE corridor_id=? AND organization_id=? AND currency=? AND effective_at<=? ORDER BY version DESC LIMIT 1",
                (corridor_id, organization_id, currency, instant)).fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ 窗口

    def open_window(self, context, *, corridor_id: str, opens_at: str,
                    fx_rate: str | None = None) -> dict:
        context.require("write:windows")
        corridor = self._require_corridor(corridor_id)
        opens = parse_instant(opens_at)
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(window_index),-1) AS i FROM clearing_windows WHERE corridor_id=?",
                (corridor_id,)).fetchone()
            last_index = int(row["i"])
            if last_index >= 0:
                last = connection.execute("SELECT closes_at FROM clearing_windows WHERE corridor_id=? AND window_index=?",
                                          (corridor_id, last_index)).fetchone()
                if parse_instant(last["closes_at"]) > opens:
                    raise ConflictError("新窗口不能早于上一窗口关账时间")
            index = last_index + 1
            closes = opens + timedelta(minutes=corridor["window_duration_minutes"])
            cutoff = closes - timedelta(minutes=corridor["cutoff_offset_minutes"])
            window_id = new_id("window")
            rate_id = None
            if fx_rate is not None:
                rate_id = self._insert_rate(connection, corridor_id, window_id, fx_rate, 1, context.actor_id)
            now = self.clock.now()
            connection.execute(
                "INSERT INTO clearing_windows(window_id,corridor_id,window_index,opens_at,closes_at,cutoff_at,state,fx_rate_id,created_at) VALUES(?,?,?,?,?,?, 'open',?,?)",
                (window_id, corridor_id, index,
                 opens.isoformat().replace("+00:00", "Z"),
                 closes.isoformat().replace("+00:00", "Z"),
                 cutoff.isoformat().replace("+00:00", "Z"), rate_id, now))
            self.audit.append(connection, actor_id=context.actor_id, action="window.open",
                              entity_type="clearing_windows", entity_id=window_id, version=1,
                              detail={"corridor_id": corridor_id, "window_index": index})
        return self.get_window(context, window_id)

    def revise_window_fx(self, context, *, window_id: str, rate_value: str, reason: str) -> dict:
        """录入/修订窗口汇率；关账前修订会暂停窗口批次。录入人不能是关账批准人。"""
        context.require("write:fx")
        if not reason.strip():
            raise ValidationError("汇率修订必须说明原因")
        with self.database.transaction() as connection:
            window = self._window_row(connection, window_id)
            if window["state"] in ("ready", "closed"):
                raise ConflictError("窗口已关账，汇率不可修订")
            row = connection.execute("SELECT COALESCE(MAX(version),0) AS v FROM clearing_fx_rates WHERE window_id=?",
                                     (window_id,)).fetchone()
            rate_id = self._insert_rate(connection, window["corridor_id"], window_id, rate_value,
                                        int(row["v"]) + 1, context.actor_id)
            connection.execute("UPDATE clearing_windows SET fx_rate_id=? WHERE window_id=?", (rate_id, window_id))
            if int(row["v"]) >= 1:
                # 关账前修订：暂停窗口内全部批次
                for held in connection.execute("SELECT batch_id FROM clearing_batches WHERE window_id=? AND status IN ('building','on_hold')", (window_id,)).fetchall():
                    self._hold_batch(connection, held["batch_id"], "fx_revision", context.actor_id)
            self.audit.append(connection, actor_id=context.actor_id, action="fx.revise",
                              entity_type="clearing_windows", entity_id=window_id,
                              version=int(row["v"]) + 1,
                              detail={"rate_id": rate_id, "reason": reason.strip()})
        return self.get_fx(context, rate_id)

    def mark_compliance_hit(self, context, *, batch_id: str, reason: str) -> dict:
        context.require("write:windows")
        if not reason.strip():
            raise ValidationError("合规命中必须说明原因")
        with self.database.transaction() as connection:
            batch = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError("批次不存在")
            if batch["status"] in ("settled", "cancelled", "adjusted"):
                raise ConflictError("终态批次不能再标记合规命中")
            self._hold_batch(connection, batch_id, "compliance_hit", context.actor_id, note=reason.strip())
        return self.get_batch(context, batch_id)

    def resume_batch(self, context, *, batch_id: str, reason: str) -> dict:
        context.require("write:windows")
        if not reason.strip():
            raise ValidationError("恢复批次必须说明原因")
        with self.database.transaction() as connection:
            batch = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError("批次不存在")
            if batch["status"] != "on_hold":
                raise ConflictError("只有暂停中的批次可以恢复")
            connection.execute("UPDATE clearing_batches SET status='building',hold_reason='' WHERE batch_id=?", (batch_id,))
            window = connection.execute("SELECT state FROM clearing_windows WHERE window_id=?", (batch["window_id"],)).fetchone()
            if window["state"] == "on_hold":
                still_held = connection.execute(
                    "SELECT COUNT(*) AS n FROM clearing_batches WHERE window_id=? AND status='on_hold'",
                    (batch["window_id"],)).fetchone()["n"]
                if still_held == 0:
                    connection.execute("UPDATE clearing_windows SET state='open',hold_reason='' WHERE window_id=?", (batch["window_id"],))
            self.audit.append(connection, actor_id=context.actor_id, action="batch.resume",
                              entity_type="clearing_batches", entity_id=batch_id, version=1,
                              detail={"reason": reason.strip()})
        return self.get_batch(context, batch_id)

    def approve_window_close(self, context, *, window_id: str, expected_fx_rate_id: str) -> dict:
        """批准窗口关账：批准人不能是该窗口任一汇率版本的录入人。"""
        context.require("close:windows")
        with self.database.transaction() as connection:
            window = self._window_row(connection, window_id)
            if window["state"] == "closed":
                raise ConflictError("窗口已经关账")
            if not window["fx_rate_id"]:
                raise ConflictError("窗口还没有汇率快照")
            if window["fx_rate_id"] != expected_fx_rate_id:
                raise ConflictError("关账汇率与当前汇率快照不一致")
            registrars = {r["registered_by"] for r in connection.execute(
                "SELECT registered_by FROM clearing_fx_rates WHERE window_id=?", (window_id,)).fetchall()}
            if context.actor_id in registrars:
                raise PermissionDenied("汇率录入人不能批准窗口关账")
            held = connection.execute(
                "SELECT COUNT(*) AS n FROM clearing_batches WHERE window_id=? AND status='on_hold'",
                (window_id,)).fetchone()["n"]
            if held:
                raise ConflictError("窗口内仍有暂停批次，不能关账")
            now = self.clock.now()
            connection.execute(
                "UPDATE clearing_windows SET state='ready',approved_by=?,approved_at=? WHERE window_id=?",
                (context.actor_id, now, window_id))
            connection.execute("UPDATE clearing_batches SET status='ready' WHERE window_id=? AND status='building'",
                               (window_id,))
            self.audit.append(connection, actor_id=context.actor_id, action="window.approve",
                              entity_type="clearing_windows", entity_id=window_id, version=1,
                              detail={"fx_rate_id": window["fx_rate_id"]})
        return self.get_window(context, window_id)

    def get_window(self, context, window_id: str) -> dict:
        context.require("read:corridors")
        with self.database.connect() as connection:
            return dict(self._window_row(connection, window_id))

    def find_window(self, corridor_id: str, at: str) -> dict:
        """事件时间落入的窗口；超过截止时间则返回下一可用窗口（迟到语义）。"""
        instant = canonical_instant(at)
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM clearing_windows WHERE corridor_id=? AND opens_at<=? AND closes_at>? ORDER BY opens_at LIMIT 1",
                (corridor_id, instant, instant)).fetchone()
            if row and instant <= parse_instant(row["cutoff_at"]):
                return dict(row)
            nxt = connection.execute(
                "SELECT * FROM clearing_windows WHERE corridor_id=? AND opens_at>? ORDER BY opens_at LIMIT 1",
                (corridor_id, instant)).fetchone()
            if nxt:
                return dict(nxt)
            return None

    # ------------------------------------------------------------------ 汇率

    def get_fx(self, context, rate_id: str) -> dict:
        context.require("read:corridors")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_fx_rates WHERE rate_id=?", (rate_id,)).fetchone()
            if not row:
                raise NotFoundError("汇率版本不存在")
            return dict(row)

    # ------------------------------------------------------------------ 批次

    def get_batch(self, context, batch_id: str) -> dict:
        context.require("read:batches")
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row:
                raise NotFoundError("批次不存在")
            batch = dict(row)
        return self._scoped_batch(batch, context)

    def list_window_batches(self, context, *, window_id: str) -> list[dict]:
        context.require("read:batches")
        with self.database.connect() as connection:
            rows = [dict(r) for r in connection.execute(
                "SELECT * FROM clearing_batches WHERE window_id=? ORDER BY settle_currency,batch_id", (window_id,))]
        return [self._scoped_batch(b, context) for b in rows]

    def batch_positions(self, context, batch_id: str) -> list[dict]:
        context.require("read:batches")
        with self.database.connect() as connection:
            rows = [dict(r) for r in connection.execute(
                "SELECT * FROM clearing_positions WHERE batch_id=? ORDER BY organization_id,currency", (batch_id,))]
        for row in rows:
            row["hold_ids"] = json.loads(row.pop("hold_ids_json"))
        if context.org_id is not None:
            rows = [r for r in rows if r["organization_id"] == context.org_id]
        return rows

    def batch_items(self, context, batch_id: str) -> list[dict]:
        context.require("read:batches")
        with self.database.connect() as connection:
            rows = [dict(r) for r in connection.execute(
                "SELECT * FROM clearing_batch_items WHERE batch_id=? ORDER BY sequence,item_id", (batch_id,))]
        if context.org_id is not None:
            visible = self._authorized_peers(context, rows)
            rows = [r for r in rows if r["organization_id"] == context.org_id or r["item_id"] in visible]
        return rows

    # --------------------------------------------------------------- 可解释性

    def explain_obligation(self, context, obligation_key: str) -> dict:
        """解释一笔义务落入哪个窗口、哪份汇率、占用多少额度、由谁放行。"""
        context.require("read:batches")
        with self.database.connect() as connection:
            instruction = connection.execute("SELECT * FROM clearing_instructions WHERE obligation_key=?",
                                             (obligation_key,)).fetchone()
            if not instruction:
                raise NotFoundError("义务指令不存在")
            instruction = dict(instruction)
            instruction["payload"] = json.loads(instruction.pop("payload_json"))
            answer = {
                "obligation_key": obligation_key,
                "instruction_state": instruction["state"],
                "source": instruction["source"], "source_key": instruction["source_key"],
                "sequence": instruction["sequence"],
                "effective_at": instruction["effective_at"],
                "received_at": instruction["received_at"],
                "window_id": instruction["window_id"],
                "batch_id": instruction["batch_id"],
            }
            if instruction["window_id"]:
                window = connection.execute("SELECT * FROM clearing_windows WHERE window_id=?",
                                            (instruction["window_id"],)).fetchone()
                answer["window"] = {"window_index": window["window_index"], "opens_at": window["opens_at"],
                                    "closes_at": window["closes_at"], "cutoff_at": window["cutoff_at"],
                                    "state": window["state"]}
                answer["late_to_previous_window"] = bool(instruction["late_admitted"])
                rate = connection.execute("SELECT * FROM clearing_fx_rates WHERE rate_id=?",
                                          (window["fx_rate_id"],)).fetchone() if window["fx_rate_id"] else None
                answer["fx"] = dict(rate) if rate else None
                answer["released_by"] = window["approved_by"]
            hold = connection.execute("SELECT * FROM clearing_holds WHERE instruction_id=? ORDER BY created_at LIMIT 1",
                                      (instruction["instruction_id"],)).fetchone()
            if hold:
                answer["liquidity_hold"] = {"hold_id": hold["hold_id"], "currency": hold["currency"],
                                            "amount_minor": hold["amount_minor"], "status": hold["status"]}
                limit = self._limit_as_of(connection, hold["corridor_id"], hold["organization_id"],
                                          hold["currency"], hold["created_at"])
                answer["limit"] = limit
                used = connection.execute(
                    "SELECT COALESCE(SUM(amount_minor),0) AS n FROM clearing_holds WHERE corridor_id=? AND window_id=? AND organization_id=? AND currency=? AND status='held'",
                    (hold["corridor_id"], hold["window_id"], hold["organization_id"], hold["currency"])).fetchone()["n"]
                answer["limit"]["used_minor_at_freeze"] = used if limit else None
            item = connection.execute("SELECT * FROM clearing_batch_items WHERE obligation_key=?",
                                      (obligation_key,)).fetchone()
            if item:
                answer["netting"] = {"net_amount_minor": item["net_amount_minor"], "leg": item["leg"],
                                     "fx_rate_id": item["fx_rate_id"]}
                adjustment = connection.execute(
                    "SELECT b.batch_id,b.window_id,b.reversal_entry_ids_json,b.created_at FROM clearing_batches b WHERE b.reverses_batch_id=?",
                    (item["batch_id"],)).fetchone()
                if adjustment:
                    answer["adjustment"] = {"batch_id": adjustment["batch_id"],
                                            "window_id": adjustment["window_id"],
                                            "reversal_entry_ids": json.loads(adjustment["reversal_entry_ids_json"] or "[]"),
                                            "adjusted_at": adjustment["created_at"]}
        if context.org_id is not None and instruction["organization_id"] != context.org_id:
            raise PermissionDenied("只能解释本方义务")
        return answer

    def recovery_position(self, source: str, source_key: str = "default") -> dict:
        """服务中断后，从最后确认的来源序列和流动性占用继续。"""
        with self.database.connect() as connection:
            cursor = connection.execute("SELECT * FROM clearing_source_cursors WHERE source=? AND source_key=?",
                                        (source, source_key)).fetchone()
            if not cursor:
                return {"source": source, "source_key": source_key, "last_sequence": -1,
                        "last_confirmed_sequence": -1, "outstanding_holds": [], "pending_instructions": [],
                        "next_window": None}
            holds = [dict(r) for r in connection.execute(
                "SELECT hold_id,window_id,organization_id,currency,amount_minor,instruction_id,batch_id FROM clearing_holds WHERE status='held' ORDER BY created_at")]
            pending = [dict(r) for r in connection.execute(
                "SELECT instruction_id,obligation_key,sequence,window_id,batch_id,state FROM clearing_instructions WHERE source=? AND source_key=? AND state IN ('received','frozen') ORDER BY sequence",
                (source, source_key))]
            next_window = connection.execute(
                "SELECT window_id,window_index,opens_at,closes_at,cutoff_at FROM clearing_windows w WHERE state='open' AND opens_at<=? ORDER BY opens_at LIMIT 1",
                (self.clock.now(),)).fetchone()
            return {"source": source, "last_sequence": cursor["last_sequence"],
                    "last_confirmed_sequence": cursor["last_confirmed_sequence"],
                    "updated_at": cursor["updated_at"], "outstanding_holds": holds,
                    "pending_instructions": pending,
                    "next_window": dict(next_window) if next_window else None}

    # ------------------------------------------------------------------ 内部

    def _require_corridor(self, corridor_id: str) -> dict:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM clearing_corridors WHERE corridor_id=?", (corridor_id,)).fetchone()
            if not row:
                raise NotFoundError("走廊不存在")
            return dict(row)

    def _require_participant(self, corridor_id: str, organization_id: str) -> None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT state,authorized_counterparties_json FROM clearing_participants WHERE corridor_id=? AND organization_id=?",
                (corridor_id, organization_id)).fetchone()
            if not row or row["state"] != "active":
                raise ValidationError("机构不是该走廊的有效参与方")

    def _counterparties(self, corridor_id: str, organization_id: str) -> set[str]:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT authorized_counterparties_json FROM clearing_participants WHERE corridor_id=? AND organization_id=?",
                (corridor_id, organization_id)).fetchone()
            return set(json.loads(row["authorized_counterparties_json"])) if row else set()

    def _authorized_peers(self, context, rows: list[dict]) -> set[str]:
        """机构行可见；对手方行只有在本方授权名单中才可见，返回可见 item_id 集合。"""
        visible: set[str] = set()
        for row in rows:
            if row["organization_id"] == context.org_id:
                continue
            peers = self._counterparties(row.get("corridor_id") or self._corridor_of_batch(row["batch_id"]),
                                        context.org_id) if context.org_id else set()
            if row["counterparty_id"] == context.org_id and row["organization_id"] in peers:
                visible.add(row["item_id"])
        return visible

    def _corridor_of_batch(self, batch_id: str) -> str:
        with self.database.connect() as connection:
            row = connection.execute("SELECT corridor_id FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
            return row["corridor_id"] if row else ""

    def _scoped_batch(self, batch: dict, context) -> dict:
        if context.org_id is None:
            return batch
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT item_id,organization_id,counterparty_id FROM clearing_batch_items WHERE batch_id=?",
                (batch["batch_id"],)).fetchall()
        peers = self._counterparties(batch["corridor_id"], context.org_id)
        own_items = sum(1 for r in rows if r["organization_id"] == context.org_id)
        peer_items = sum(1 for r in rows if r["counterparty_id"] == context.org_id and r["organization_id"] in peers)
        if own_items == 0 and peer_items == 0:
            raise PermissionDenied("批次不含本方或被授权对手方的指令")
        return {**batch, "visible_item_count": own_items + peer_items, "total_item_count": len(rows)}

    @staticmethod
    def _window_row(connection, window_id: str):
        row = connection.execute("SELECT * FROM clearing_windows WHERE window_id=?", (window_id,)).fetchone()
        if not row:
            raise NotFoundError("窗口不存在")
        return row

    @staticmethod
    def _limit_as_of(connection, corridor_id, organization_id, currency, as_of):
        row = connection.execute(
            "SELECT limit_id,amount_minor,version,effective_at,registered_by FROM clearing_limits WHERE corridor_id=? AND organization_id=? AND currency=? AND effective_at<=? ORDER BY version DESC LIMIT 1",
            (corridor_id, organization_id, currency, as_of)).fetchone()
        return dict(row) if row else None

    def _insert_rate(self, connection, corridor_id, window_id, rate_value, version, actor) -> str:
        rate = parse_rate(rate_value)
        rate_id = new_id("fx")
        connection.execute(
            "INSERT INTO clearing_fx_rates(rate_id,corridor_id,window_id,rate_value,version,effective_at,registered_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (rate_id, corridor_id, window_id, str(rate), version, self.clock.now(), actor, self.clock.now()))
        return rate_id

    def _hold_batch(self, connection, batch_id: str, reason: str, actor: str, *, note: str = "") -> None:
        batch = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if not batch or batch["status"] in ("settled", "cancelled", "adjusted", "returned"):
            return
        connection.execute("UPDATE clearing_batches SET status='on_hold',hold_reason=? WHERE batch_id=?",
                           (reason, batch_id))
        connection.execute("UPDATE clearing_windows SET state='on_hold',hold_reason=? WHERE window_id=? AND state='open'",
                           (reason, batch["window_id"]))
        self.audit.append(connection, actor_id=actor, action="batch.hold",
                          entity_type="clearing_batches", entity_id=batch_id, version=1,
                          detail={"reason": reason, "note": note})

    # 供净额引擎使用的不变量校验
    def verify_integrity(self) -> dict:
        with self.database.connect() as connection:
            duplicated = connection.execute(
                "SELECT obligation_key,COUNT(*) AS n FROM clearing_batch_items GROUP BY obligation_key HAVING n>1").fetchall()
            if duplicated:
                raise InvariantViolation("同一义务出现在多个批次")
            return {"batches": connection.execute("SELECT COUNT(*) AS n FROM clearing_batches").fetchone()["n"],
                    "instructions": connection.execute("SELECT COUNT(*) AS n FROM clearing_instructions").fetchone()["n"],
                    "open_holds": connection.execute("SELECT COUNT(*) AS n FROM clearing_holds WHERE status='held'").fetchone()["n"]}
