"""桥侧序列事件机：受理、匹配、冻结净额、关账结算、退回、撤销与反向调整。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal

from .audit import AuditLog
from .clearing import ClearingHouse, convert_minor, parse_rate
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .inbox import Inbox
from .jsonutil import canonical_json
from .ledger import Ledger, to_minor
from .timeutil import Clock, canonical_instant, parse_instant

# 桥侧事件按来源序号推进的生命周期。
EVENT_KINDS = ("instruction", "accept", "match", "settle", "return", "cancel")
# 事件允许的前置阶段（None 表示不需要既有指令）。
EVENT_ORDER = {
    "instruction": None,
    "accept": "admitted",
    "match": "accepted",
    "settle": "matched",
    "return": None,   # 结算前任何阶段都可退回
    "cancel": None,   # 结算前任何阶段都可撤销
}
TERMINAL_STATES = {"settled", "returned", "cancelled"}
SETTLE_DIRECTION = {"pay": "debit", "receive": "credit"}
REVERSE_DIRECTION = {"debit": "credit", "credit": "debit"}


@dataclass(frozen=True)
class BridgeNetting:
    """按来源序列推进桥侧事件。

    不变量：
    - 同一 (source, source_key, sequence) 只能应用一次，异文进入收件箱冲突表隔离；
    - 序号必须连续，断序拒绝，迟到事件落入下一可用窗口；
    - 一笔有效义务同一时刻只存在于一个净额批次（部分唯一索引保证）；
    - 已结算批次只能由新窗口的反向批次冲销，原分录不抹除。
    """

    database: Database
    clock: Clock
    house: ClearingHouse
    inbox: Inbox
    ledger: Ledger
    audit: AuditLog
    journal_key: str = "bridge"

    # -------------------------------------------------------------- 受理入口

    def admit_event(self, context, *, source: str, source_key: str, sequence: int,
                    event_kind: str, payload: dict, occurred_at: str) -> dict:
        context.require("write:bridge")
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        if event_kind not in EVENT_KINDS:
            raise ValidationError(f"未知桥侧事件类型: {event_kind}")
        occurred_at = canonical_instant(occurred_at)
        envelope = {"event_kind": event_kind, "payload": payload, "occurred_at": occurred_at}
        # 同序同文幂等、同序异文隔离到 inbox_conflicts。
        inbox_result = self.inbox.receive(source=source, source_key=source_key, sequence=sequence,
                                          payload=envelope, occurred_at=occurred_at)
        with self.database.transaction() as connection:
            duplicate = connection.execute(
                "SELECT * FROM clearing_bridge_events WHERE source=? AND source_key=? AND sequence=?",
                (source, source_key, sequence)).fetchone()
            if duplicate:
                view = self._event_view(dict(duplicate), replayed=True)
                if duplicate["obligation_key"]:
                    instruction_row = connection.execute(
                        "SELECT * FROM clearing_instructions WHERE obligation_key=? ORDER BY received_at DESC,sequence DESC LIMIT 1",
                        (duplicate["obligation_key"],)).fetchone()
                    if instruction_row:
                        instruction = dict(instruction_row)
                        view.update({k: instruction[k] for k in
                                     ("instruction_id", "state", "bridge_phase", "window_id", "batch_id")})
                return view
            self._require_contiguous(connection, source, source_key, sequence)
            self._advance_cursor(connection, source, source_key, sequence, confirmed=False)
            result = self._apply(connection, context, source=source, source_key=source_key,
                                 sequence=sequence, event_kind=event_kind, payload=payload,
                                 occurred_at=occurred_at)
            event_id = new_id("evt")
            connection.execute(
                "INSERT INTO clearing_bridge_events(event_id,source,source_key,sequence,event_kind,obligation_key,payload_json,occurred_at,applied_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (event_id, source, source_key, sequence, event_kind, result.get("obligation_key"),
                 canonical_json(payload), occurred_at, self.clock.now()))
            result["event_id"] = event_id
            result["inbox_status"] = inbox_result["status"]
            return result

    # -------------------------------------------------------------- 事件应用

    def _apply(self, connection, context, *, source, source_key, sequence,
               event_kind, payload, occurred_at) -> dict:
        if event_kind == "instruction":
            return self._apply_instruction(connection, context, source=source, source_key=source_key,
                                           sequence=sequence, payload=payload, occurred_at=occurred_at)
        obligation_key = self._require_obligation(payload)
        instruction = self._live_instruction(connection, obligation_key)
        required = EVENT_ORDER[event_kind]
        if event_kind in ("accept", "match", "settle"):
            if instruction["state"] == "settled":
                return self._instruction_view(instruction, replayed=True)
            if required and instruction["bridge_phase"] != required:
                raise ConflictError(
                    f"事件 {event_kind} 要求阶段 {required}，当前为 {instruction['bridge_phase']}")
        if event_kind == "accept":
            connection.execute("UPDATE clearing_instructions SET bridge_phase='accepted' WHERE instruction_id=?",
                               (instruction["instruction_id"],))
            self._audit(connection, context, "bridge.accept", instruction, sequence)
            return self._instruction_view(self._reload(connection, instruction))
        if event_kind == "match":
            view = self._freeze(connection, context, instruction, sequence)
            return view
        if event_kind == "settle":
            if instruction["state"] == "settled":
                self._advance_cursor(connection, source, source_key, sequence, confirmed=True)
                return self._instruction_view(instruction, replayed=True)
            if instruction["state"] != "frozen":
                raise ConflictError("结算事件要求指令已匹配冻结")
            batch_id = instruction["batch_id"]
            settled = self._settle_batch(connection, context, batch_id)
            self._advance_cursor(connection, source, source_key, sequence, confirmed=True)
            result = self._instruction_view(self._reload(connection, instruction))
            result["settlement"] = settled
            return result
        if event_kind == "return":
            return self._return(connection, context, instruction, sequence, str(payload.get("reason", "")))
        if event_kind == "cancel":
            return self._cancel(connection, context, instruction, sequence, str(payload.get("reason", "")))
        raise ValidationError(f"未知桥侧事件类型: {event_kind}")

    def _apply_instruction(self, connection, context, *, source, source_key, sequence,
                           payload, occurred_at) -> dict:
        parsed = self._parse_instruction(connection, payload)
        # 跨日重试可能使用新的来源序号重发同一义务：直接回到原指令/原批次，绝不二次占用。
        existing = connection.execute(
            "SELECT * FROM clearing_instructions WHERE obligation_key=? AND state IN ('received','frozen','settled') ORDER BY received_at DESC,sequence DESC LIMIT 1",
            (parsed["obligation_key"],)).fetchone()
        if existing:
            view = self._instruction_view(dict(existing), replayed=True)
            view["duplicate_retry"] = True
            return view
        window, late = self._resolve_window(connection, parsed["corridor_id"], occurred_at)
        instruction_id = new_id("ins")
        now = self.clock.now()
        connection.execute(
            "INSERT INTO clearing_instructions(instruction_id,obligation_key,corridor_id,organization_id,counterparty_id,currency,amount_minor,source,source_key,sequence,event_kind,bridge_phase,effective_at,payload_json,state,late_admitted,window_id,received_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?, 'instruction','admitted',?,?, 'received', ?, ?, ?)",
            (instruction_id, parsed["obligation_key"], parsed["corridor_id"], parsed["organization_id"],
             parsed["counterparty_id"], parsed["currency"], parsed["amount_minor"], source, source_key,
             sequence, occurred_at, canonical_json(payload), 1 if late else 0, window["window_id"], now))
        self.audit.append(connection, actor_id=context.actor_id, action="bridge.admit",
                          entity_type="clearing_instructions", entity_id=instruction_id, version=1,
                          detail={"source": source, "sequence": sequence,
                                  "obligation_key": parsed["obligation_key"], "window_id": window["window_id"],
                                  "late": parse_instant(occurred_at) > parse_instant(window["cutoff_at"])})
        view = self._instruction_view(self._reload(connection, {"instruction_id": instruction_id}))
        view["late"] = late
        return view

    # -------------------------------------------------------------- 冻结净额

    def freeze(self, context, *, instruction_id: str) -> dict:
        """平台侧直接触发匹配冻结（桥侧 match 事件的同义入口，便于批处理恢复）。"""
        context.require("write:bridge")
        with self.database.transaction() as connection:
            instruction = connection.execute("SELECT * FROM clearing_instructions WHERE instruction_id=?",
                                             (instruction_id,)).fetchone()
            if not instruction:
                raise NotFoundError("指令不存在")
            instruction = dict(instruction)
            if instruction["state"] == "frozen":
                return self._instruction_view(instruction, replayed=True)
            if instruction["bridge_phase"] != "accepted":
                raise ConflictError("指令必须先受理（accept）才能匹配冻结")
            return self._freeze(connection, context, instruction, instruction["sequence"])

    def _freeze(self, connection, context, instruction: dict, sequence: int) -> dict:
        instruction = dict(instruction)
        if instruction["state"] == "frozen":
            return self._instruction_view(instruction, replayed=True)
        if instruction["state"] in TERMINAL_STATES:
            raise ConflictError(f"指令已 {instruction['state']}，不能冻结")
        corridor = connection.execute("SELECT * FROM clearing_corridors WHERE corridor_id=?",
                                      (instruction["corridor_id"],)).fetchone()
        participant = connection.execute(
            "SELECT state,authorized_counterparties_json FROM clearing_participants WHERE corridor_id=? AND organization_id=?",
            (instruction["corridor_id"], instruction["organization_id"])).fetchone()
        if not participant or participant["state"] != "active":
            raise PermissionDenied("付款方不是走廊有效参与方")
        peers = set(json.loads(participant["authorized_counterparties_json"]))
        if instruction["counterparty_id"] not in peers:
            raise PermissionDenied("对手方不在本方授权名单内")
        window = connection.execute("SELECT * FROM clearing_windows WHERE window_id=?",
                                    (instruction["window_id"],)).fetchone()
        window = dict(window)
        if window["state"] in ("ready", "closed"):
            # 窗口已关账：迟到匹配只能滚入下一可用窗口，绝不挤进已关账批次。
            window = self._next_open_window(connection, instruction["corridor_id"], window["window_index"])
            connection.execute("UPDATE clearing_instructions SET window_id=?,late_admitted=1 WHERE instruction_id=?",
                               (window["window_id"], instruction["instruction_id"]))
        if not window["fx_rate_id"]:
            raise ConflictError("窗口缺少汇率快照，不能冻结")
        fx = connection.execute("SELECT * FROM clearing_fx_rates WHERE rate_id=?",
                                (window["fx_rate_id"],)).fetchone()
        rate = parse_rate(fx["rate_value"])

        limit = self.house.current_limit(instruction["corridor_id"], instruction["organization_id"],
                                         instruction["currency"], as_of=self.clock.now())
        if not limit:
            raise ConflictError("付款方没有生效限额")
        used = connection.execute(
            "SELECT COALESCE(SUM(amount_minor),0) AS n FROM clearing_holds WHERE corridor_id=? AND window_id=? AND organization_id=? AND currency=? AND status='held'",
            (instruction["corridor_id"], window["window_id"], instruction["organization_id"],
             instruction["currency"])).fetchone()["n"]
        if int(used) + instruction["amount_minor"] > int(limit["amount_minor"]):
            raise ConflictError("可用流动性不足，冻结会突破窗口限额")

        hold_id = new_id("hold")
        connection.execute(
            "INSERT INTO clearing_holds(hold_id,corridor_id,window_id,organization_id,currency,amount_minor,status,instruction_id,created_at) "
            "VALUES(?,?,?,?,?,?, 'held', ?, ?)",
            (hold_id, instruction["corridor_id"], window["window_id"], instruction["organization_id"],
             instruction["currency"], instruction["amount_minor"], instruction["instruction_id"],
             self.clock.now()))

        batch = self._get_or_create_batch(connection, corridor["corridor_id"], window["window_id"],
                                          instruction["currency"], context.actor_id)
        if batch["status"] == "on_hold":
            raise ConflictError("批次处于暂停状态，暂停解除前不能并入指令")
        net_converted = convert_minor(instruction["amount_minor"], rate)
        item_id = new_id("item")
        connection.execute(
            "INSERT INTO clearing_batch_items(item_id,batch_id,obligation_key,source,source_key,sequence,organization_id,counterparty_id,direction,currency,amount_minor,fx_rate_id,net_amount_minor,leg,instruction_id,hold_id) "
            "VALUES(?,?,?,?,?,?,?,?, 'pay', ?,?,?,?, 'gross', ?, ?)",
            (item_id, batch["batch_id"], instruction["obligation_key"], instruction["source"],
             instruction["source_key"], sequence, instruction["organization_id"],
             instruction["counterparty_id"], instruction["currency"], instruction["amount_minor"],
             fx["rate_id"], net_converted, instruction["instruction_id"], hold_id))
        connection.execute("UPDATE clearing_holds SET batch_id=?,item_id=? WHERE hold_id=?",
                           (batch["batch_id"], item_id, hold_id))
        self._rebuild_positions(connection, batch["batch_id"], rate)
        connection.execute(
            "UPDATE clearing_instructions SET state='frozen',bridge_phase='matched',batch_id=?,processed_at=? WHERE instruction_id=?",
            (batch["batch_id"], self.clock.now(), instruction["instruction_id"]))
        self._advance_cursor(connection, instruction["source"], instruction["source_key"],
                             sequence, confirmed=True)
        self.audit.append(connection, actor_id=context.actor_id, action="bridge.match",
                          entity_type="clearing_batches", entity_id=batch["batch_id"], version=1,
                          detail={"instruction_id": instruction["instruction_id"], "hold_id": hold_id,
                                  "obligation_key": instruction["obligation_key"],
                                  "window_id": window["window_id"],
                                  "net_converted_minor": net_converted, "fx_rate_id": fx["rate_id"]})
        view = self._instruction_view(self._reload(connection, instruction))
        view["batch_id"] = batch["batch_id"]
        view["hold_id"] = hold_id
        return view

    # ---------------------------------------------------------------- 结算

    def settle_batch(self, context, *, batch_id: str) -> dict:
        """对关账批准后的批次结算：按头寸写不可变分录并推进确认序列。"""
        context.require("settle:bridge")
        with self.database.transaction() as connection:
            return self._settle_batch(connection, context, batch_id)

    def _settle_batch(self, connection, context, batch_id: str) -> dict:
        batch = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if not batch:
            raise NotFoundError("批次不存在")
        batch = dict(batch)
        if batch["status"] == "settled":
            return self._settled_view(connection, batch, replayed=True)
        if batch["reverses_batch_id"]:
            raise ConflictError("反向调整批次在创建时即已结算")
        if batch["status"] != "ready":
            raise ConflictError(f"批次状态为 {batch['status']}，只有 ready 批次可以结算")
        window = connection.execute("SELECT * FROM clearing_windows WHERE window_id=?",
                                    (batch["window_id"],)).fetchone()
        if window["state"] != "ready":
            raise ConflictError("窗口尚未完成关账批准")
        fx = connection.execute("SELECT * FROM clearing_fx_rates WHERE rate_id=?",
                                (window["fx_rate_id"],)).fetchone()
        detail: list[dict] = []
        positions = connection.execute(
            "SELECT * FROM clearing_positions WHERE batch_id=? ORDER BY organization_id,currency",
            (batch_id,)).fetchall()
        for position in positions:
            net = abs(int(position["net_minor"]))
            if net == 0:
                continue
            direction = SETTLE_DIRECTION[position["net_direction"]]
            reference = f"{batch_id}:{position['organization_id']}:{position['currency']}"
            entry = self.ledger.post_on(
                connection, journal_key=self.journal_key, account=position["organization_id"],
                currency=position["currency"], amount_minor=net, direction=direction,
                reference=reference, actor=context.actor_id)
            detail.append({"entry_id": entry["entry_id"], "organization_id": position["organization_id"],
                           "currency": position["currency"], "amount_minor": net,
                           "net_direction": position["net_direction"]})
        hold_entries = {d["organization_id"]: d["entry_id"] for d in detail}
        for hold in connection.execute("SELECT * FROM clearing_holds WHERE batch_id=?", (batch_id,)).fetchall():
            connection.execute(
                "UPDATE clearing_holds SET status='settled',released_at=?,settlement_entry_id=? WHERE hold_id=?",
                (self.clock.now(), hold_entries.get(hold["organization_id"]), hold["hold_id"]))
        connection.execute(
            "UPDATE clearing_instructions SET state='settled',bridge_phase='settled',settled_window_id=? WHERE batch_id=? AND state='frozen'",
            (window["window_id"], batch_id))
        last_sequence = self._batch_last_sequence(connection, batch_id)
        now = self.clock.now()
        entry_ids = [d["entry_id"] for d in detail]
        connection.execute(
            "UPDATE clearing_batches SET status='settled',confirmed_sequence=?,confirmed_at=?,settled_entry_ids_json=?,settlement_detail_json=?,settled_at=? WHERE batch_id=?",
            (last_sequence, now, canonical_json(entry_ids), canonical_json(detail), now, batch_id))
        for row in connection.execute(
                "SELECT source,source_key,COALESCE(MAX(sequence),0) AS s FROM clearing_batch_items WHERE batch_id=? AND leg='gross' GROUP BY source,source_key",
                (batch_id,)).fetchall():
            self._mark_settled_cursor(connection, row["source"], row["source_key"], int(row["s"]))
        # 窗口内全部净额批次结清后，窗口正式关账。
        pending = connection.execute(
            "SELECT COUNT(*) AS n FROM clearing_batches WHERE window_id=? AND status IN ('building','on_hold','ready')",
            (batch["window_id"],)).fetchone()["n"]
        if pending == 0:
            connection.execute(
                "UPDATE clearing_windows SET state='closed',closed_by=?,closed_at=? WHERE window_id=? AND state='ready'",
                (context.actor_id, now, batch["window_id"]))
        self.audit.append(connection, actor_id=context.actor_id, action="bridge.settle",
                          entity_type="clearing_batches", entity_id=batch_id, version=1,
                          detail={"window_id": batch["window_id"], "entries": detail,
                                  "fx_rate_id": fx["rate_id"], "confirmed_sequence": last_sequence})
        return self._settled_view(connection, dict(connection.execute(
            "SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()))

    # ------------------------------------------------------- 退回与撤销

    def return_instruction(self, context, *, instruction_id: str, reason: str) -> dict:
        context.require("write:bridge")
        if not reason.strip():
            raise ValidationError("退回必须说明原因")
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM clearing_instructions WHERE instruction_id=?",
                                     (instruction_id,)).fetchone()
            if not row:
                raise NotFoundError("指令不存在")
            return self._return(connection, context, dict(row), row["sequence"], reason)

    def cancel_instruction(self, context, *, instruction_id: str, reason: str) -> dict:
        context.require("write:bridge")
        if not reason.strip():
            raise ValidationError("撤销必须说明原因")
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM clearing_instructions WHERE instruction_id=?",
                                     (instruction_id,)).fetchone()
            if not row:
                raise NotFoundError("指令不存在")
            return self._cancel(connection, context, dict(row), row["sequence"], reason)

    def _return(self, connection, context, instruction: dict, sequence: int, reason: str) -> dict:
        instruction = dict(instruction)
        if instruction["state"] == "returned":
            return self._instruction_view(instruction, replayed=True)
        if instruction["state"] == "settled":
            raise ConflictError("已结算指令只能通过反向批次调整")
        self._detach_from_batch(connection, instruction)
        connection.execute("UPDATE clearing_holds SET status='released',released_at=? WHERE instruction_id=? AND status='held'",
                           (self.clock.now(), instruction["instruction_id"]))
        connection.execute("UPDATE clearing_instructions SET state='returned',processed_at=? WHERE instruction_id=?",
                           (self.clock.now(), instruction["instruction_id"]))
        self._audit(connection, context, "bridge.return", instruction, sequence, reason=reason.strip())
        return self._instruction_view(self._reload(connection, instruction))

    def _cancel(self, connection, context, instruction: dict, sequence: int, reason: str) -> dict:
        instruction = dict(instruction)
        if instruction["state"] == "cancelled":
            return self._instruction_view(instruction, replayed=True)
        if instruction["state"] == "settled":
            raise ConflictError("已结算指令不能撤销，只能用反向分录调整")
        self._detach_from_batch(connection, instruction)
        connection.execute("UPDATE clearing_holds SET status='cancelled',released_at=? WHERE instruction_id=?",
                           (self.clock.now(), instruction["instruction_id"]))
        connection.execute("UPDATE clearing_instructions SET state='cancelled',processed_at=? WHERE instruction_id=?",
                           (self.clock.now(), instruction["instruction_id"]))
        self._audit(connection, context, "bridge.cancel", instruction, sequence, reason=reason.strip())
        return self._instruction_view(self._reload(connection, instruction))

    def _detach_from_batch(self, connection, instruction: dict) -> None:
        if not instruction["batch_id"]:
            return
        batch = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?",
                                   (instruction["batch_id"],)).fetchone()
        if batch["status"] in ("ready", "settled", "adjusted"):
            raise ConflictError(f"批次已 {batch['status']}，不能撤出指令")
        connection.execute("DELETE FROM clearing_batch_items WHERE instruction_id=? AND leg='gross'",
                           (instruction["instruction_id"],))
        connection.execute("UPDATE clearing_holds SET batch_id=NULL,item_id=NULL WHERE instruction_id=? AND status IN ('held')",
                           (instruction["instruction_id"],))
        window = connection.execute("SELECT fx_rate_id FROM clearing_windows WHERE window_id=?",
                                    (batch["window_id"],)).fetchone()
        fx = connection.execute("SELECT rate_value FROM clearing_fx_rates WHERE rate_id=?",
                                (window["fx_rate_id"],)).fetchone()
        self._rebuild_positions(connection, batch["batch_id"], parse_rate(fx["rate_value"]))
        connection.execute("UPDATE clearing_instructions SET batch_id=NULL WHERE instruction_id=?",
                           (instruction["instruction_id"],))

    # ------------------------------------------------------- 反向调整批次

    def adjust_settled_batch(self, context, *, batch_id: str, target_window_id: str, reason: str) -> dict:
        """已结算批次不抹账：在新窗口生成反向批次并写反向分录。"""
        context.require("settle:bridge")
        if not reason.strip():
            raise ValidationError("调整必须说明原因")
        with self.database.transaction() as connection:
            original = connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not original:
                raise NotFoundError("原批次不存在")
            original = dict(original)
            if original["status"] != "settled":
                raise ConflictError("只有已结算批次可以反向调整")
            target = connection.execute("SELECT * FROM clearing_windows WHERE window_id=?",
                                        (target_window_id,)).fetchone()
            if not dict(target)["fx_rate_id"]:
                raise ConflictError("调整窗口缺少汇率快照")
            if target["state"] == "closed":
                raise ConflictError("调整窗口已经关账")
            if target["window_id"] == original["window_id"]:
                raise ConflictError("调整必须使用新窗口")
            if connection.execute("SELECT 1 FROM clearing_batches WHERE reverses_batch_id=?",
                                  (batch_id,)).fetchone():
                raise ConflictError("原批次已经存在反向调整批次")
            now = self.clock.now()
            adjustment_id = new_id("batch")
            connection.execute(
                "INSERT INTO clearing_batches(batch_id,corridor_id,window_id,settle_currency,status,confirmed_sequence,confirmed_at,reverses_batch_id,created_at,created_by) "
                "VALUES(?,?,?,?,'building',?,?,?,?,?)",
                (adjustment_id, original["corridor_id"], target_window_id, original["settle_currency"],
                 original["confirmed_sequence"], now, batch_id, now, context.actor_id))
            reversal_detail: list[dict] = []
            for entry in json.loads(original["settlement_detail_json"]):
                reversed_entry = self.ledger.reverse_on(
                    connection, entry["entry_id"],
                    reference=f"{adjustment_id}:{entry['organization_id']}:{entry['currency']}",
                    actor=context.actor_id)
                reversal_detail.append({**entry, "reversal_entry_id": reversed_entry["entry_id"],
                                        "original_entry_id": entry["entry_id"],
                                        "direction": REVERSE_DIRECTION[SETTLE_DIRECTION[entry["net_direction"]]]})
            connection.execute(
                "UPDATE clearing_batches SET status='settled',settled_at=?,settled_entry_ids_json=?,settlement_detail_json=?,reversal_entry_ids_json=? WHERE batch_id=?",
                (now, canonical_json([d["reversal_entry_id"] for d in reversal_detail]),
                 canonical_json(reversal_detail),
                 canonical_json([d["reversal_entry_id"] for d in reversal_detail]), adjustment_id))
            connection.execute("UPDATE clearing_batches SET status='adjusted' WHERE batch_id=?", (batch_id,))
            self.audit.append(connection, actor_id=context.actor_id, action="bridge.adjust",
                              entity_type="clearing_batches", entity_id=adjustment_id, version=1,
                              detail={"reverses_batch_id": batch_id, "target_window_id": target_window_id,
                                      "reason": reason.strip(), "reversal_detail": reversal_detail})
            return self._settled_view(connection, dict(connection.execute(
                "SELECT * FROM clearing_batches WHERE batch_id=?", (adjustment_id,)).fetchone()))

    # -------------------------------------------------------------- 恢复

    def recover(self, source: str, source_key: str = "default") -> dict:
        """从最后确认的序列和流动性占用继续；重放返回原批次。"""
        with self.database.connect() as connection:
            cursor = connection.execute(
                "SELECT * FROM clearing_source_cursors WHERE source=? AND source_key=?",
                (source, source_key)).fetchone()
            base = {"source": source, "source_key": source_key,
                    "last_sequence": cursor["last_sequence"] if cursor else -1,
                    "last_confirmed_sequence": cursor["last_confirmed_sequence"] if cursor else -1}
            base["resumable"] = [dict(r) for r in connection.execute(
                "SELECT instruction_id,obligation_key,sequence,bridge_phase,state,window_id,batch_id FROM clearing_instructions WHERE source=? AND source_key=? AND state='received' ORDER BY sequence",
                (source, source_key))]
            base["frozen_unsettled"] = [dict(r) for r in connection.execute(
                "SELECT instruction_id,obligation_key,sequence,window_id,batch_id FROM clearing_instructions WHERE source=? AND source_key=? AND state='frozen' ORDER BY sequence",
                (source, source_key))]
            base["outstanding_holds"] = [dict(r) for r in connection.execute(
                "SELECT hold_id,window_id,organization_id,currency,amount_minor,instruction_id,batch_id FROM clearing_holds WHERE status='held' ORDER BY created_at")]
            base["recent_events"] = [dict(r) for r in connection.execute(
                "SELECT sequence,event_kind,obligation_key,applied_at FROM clearing_bridge_events WHERE source=? AND source_key=? ORDER BY sequence DESC LIMIT 10",
                (source, source_key))]
            return base

    # -------------------------------------------------------------- 内部

    def _require_contiguous(self, connection, source: str, source_key: str, sequence: int) -> None:
        row = connection.execute(
            "SELECT MAX(sequence) AS s FROM clearing_bridge_events WHERE source=? AND source_key=?",
            (source, source_key)).fetchone()
        value = row["s"]
        expected = (int(value) if value is not None else -1) + 1
        if sequence != expected:
            if sequence < expected:
                raise ConflictError(f"序号 {sequence} 已应用，请按原事件重放")
            raise ConflictError(f"来源断序：期望序号 {expected}，收到 {sequence}")

    @staticmethod
    def _require_obligation(payload: dict) -> str:
        obligation_key = payload.get("obligation_key")
        if not obligation_key:
            raise ValidationError("事件必须携带 obligation_key")
        return require_safe(str(obligation_key), "义务标识")

    def _live_instruction(self, connection, obligation_key: str) -> dict:
        row = connection.execute(
            "SELECT * FROM clearing_instructions WHERE obligation_key=? AND state IN ('received','frozen','settled') ORDER BY received_at DESC,sequence DESC LIMIT 1",
            (obligation_key,)).fetchone()
        if not row:
            raise NotFoundError(f"义务 {obligation_key} 没有有效指令")
        return dict(row)

    @staticmethod
    def _reload(connection, instruction: dict) -> dict:
        row = connection.execute("SELECT * FROM clearing_instructions WHERE instruction_id=?",
                                 (instruction["instruction_id"],)).fetchone()
        return dict(row)

    def _parse_instruction(self, connection, payload: dict) -> dict:
        required = ("corridor_id", "organization_id", "counterparty_id", "currency", "amount", "obligation_key")
        missing = [f for f in required if f not in payload]
        if missing:
            raise ValidationError("指令缺少字段: " + ", ".join(missing))
        for field_name in ("corridor_id", "organization_id", "counterparty_id", "currency", "obligation_key"):
            require_safe(str(payload[field_name]), field_name)
        amount = to_minor(payload["amount"])
        if amount <= 0:
            raise ValidationError("指令金额必须大于零")
        if not connection.execute("SELECT 1 FROM clearing_corridors WHERE corridor_id=?",
                                  (payload["corridor_id"],)).fetchone():
            raise NotFoundError("走廊不存在")
        return {"corridor_id": payload["corridor_id"], "organization_id": payload["organization_id"],
                "counterparty_id": payload["counterparty_id"], "currency": payload["currency"],
                "amount_minor": amount, "obligation_key": payload["obligation_key"]}

    def _resolve_window(self, connection, corridor_id: str, occurred_at: str) -> tuple[dict, bool]:
        """返回承接窗口与迟到标志。

        事件时间落在窗口 [opens, closes) 且未过该窗截止时间 -> 本窗、非迟到；
        超过截止时间（或处于两窗间隙/上窗已关账）-> 下一可用窗口、迟到。
        """
        instant = parse_instant(occurred_at)
        row = connection.execute(
            "SELECT * FROM clearing_windows WHERE corridor_id=? AND opens_at<=? AND closes_at>? ORDER BY opens_at LIMIT 1",
            (corridor_id, occurred_at, occurred_at)).fetchone()
        if row and instant <= parse_instant(row["cutoff_at"]):
            return dict(row), False
        nxt = connection.execute(
            "SELECT * FROM clearing_windows WHERE corridor_id=? AND closes_at>=? ORDER BY opens_at LIMIT 1",
            (corridor_id, occurred_at)).fetchone()
        if not nxt or nxt["window_id"] == (row["window_id"] if row else None):
            nxt = connection.execute(
                "SELECT * FROM clearing_windows WHERE corridor_id=? AND opens_at>? ORDER BY opens_at LIMIT 1",
                (corridor_id, occurred_at)).fetchone()
        if not nxt:
            raise ConflictError("没有下一可用窗口承接迟到事件")
        return dict(nxt), True

    def _next_open_window(self, connection, corridor_id: str, after_index: int) -> dict:
        row = connection.execute(
            "SELECT * FROM clearing_windows WHERE corridor_id=? AND window_index>? AND state='open' ORDER BY window_index LIMIT 1",
            (corridor_id, after_index)).fetchone()
        if not row:
            raise ConflictError("没有下一可用窗口承接迟到事件")
        return dict(row)

    def _get_or_create_batch(self, connection, corridor_id: str, window_id: str,
                             settle_currency: str, actor: str) -> dict:
        row = connection.execute(
            "SELECT * FROM clearing_batches WHERE window_id=? AND settle_currency=? AND reverses_batch_id IS NULL",
            (window_id, settle_currency)).fetchone()
        if row:
            return dict(row)
        batch_id = new_id("batch")
        connection.execute(
            "INSERT INTO clearing_batches(batch_id,corridor_id,window_id,settle_currency,status,created_at,created_by) "
            "VALUES(?,?,?,?,'building',?,?)",
            (batch_id, corridor_id, window_id, settle_currency, self.clock.now(), actor))
        return dict(connection.execute("SELECT * FROM clearing_batches WHERE batch_id=?", (batch_id,)).fetchone())

    def _rebuild_positions(self, connection, batch_id: str, rate: Decimal) -> None:
        connection.execute("DELETE FROM clearing_positions WHERE batch_id=?", (batch_id,))
        items = connection.execute(
            "SELECT organization_id,currency,SUM(amount_minor) AS gross FROM clearing_batch_items WHERE batch_id=? AND leg='gross' GROUP BY organization_id,currency",
            (batch_id,)).fetchall()
        orgs = {(r["organization_id"], r["currency"]) for r in items}
        for org_id, currency in sorted(orgs):
            gross_pay = connection.execute(
                "SELECT COALESCE(SUM(amount_minor),0) AS n FROM clearing_batch_items WHERE batch_id=? AND leg='gross' AND organization_id=? AND currency=?",
                (batch_id, org_id, currency)).fetchone()["n"]
            gross_receive = connection.execute(
                "SELECT COALESCE(SUM(amount_minor),0) AS n FROM clearing_batch_items WHERE batch_id=? AND leg='gross' AND counterparty_id=? AND currency=? AND organization_id<>?",
                (batch_id, org_id, currency, org_id)).fetchone()["n"]
            net = int(gross_pay) - int(gross_receive)
            hold_rows = connection.execute(
                "SELECT hold_id FROM clearing_holds WHERE batch_id=? AND organization_id=? AND currency=? AND status='held' ORDER BY hold_id",
                (batch_id, org_id, currency)).fetchall()
            converted = convert_minor(abs(net), rate)
            connection.execute(
                "INSERT INTO clearing_positions(batch_id,organization_id,currency,gross_pay_minor,gross_receive_minor,net_minor,net_converted_minor,net_direction,hold_ids_json) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (batch_id, org_id, currency, int(gross_pay), int(gross_receive), net,
                 converted if net >= 0 else -converted,
                 "pay" if net > 0 else ("receive" if net < 0 else "flat"),
                 canonical_json([r["hold_id"] for r in hold_rows])))

    def _batch_last_sequence(self, connection, batch_id: str) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence),0) AS n FROM clearing_batch_items WHERE batch_id=? AND leg='gross'",
            (batch_id,)).fetchone()
        return int(row["n"])

    def _advance_cursor(self, connection, source: str, source_key: str, sequence: int, *, confirmed: bool) -> None:
        row = connection.execute("SELECT * FROM clearing_source_cursors WHERE source=? AND source_key=?",
                                 (source, source_key)).fetchone()
        now = self.clock.now()
        if row:
            connection.execute(
                "UPDATE clearing_source_cursors SET last_sequence=?,last_confirmed_sequence=?,updated_at=? WHERE source=? AND source_key=?",
                (max(int(row["last_sequence"]), sequence),
                 max(int(row["last_confirmed_sequence"]), sequence) if confirmed else int(row["last_confirmed_sequence"]),
                 now, source, source_key))
        else:
            connection.execute(
                "INSERT INTO clearing_source_cursors(source,source_key,last_sequence,last_confirmed_sequence,updated_at) VALUES(?,?,?,?,?)",
                (source, source_key, sequence, sequence if confirmed else -1, now))

    def _mark_settled_cursor(self, connection, source: str, source_key: str, sequence: int) -> None:
        row = connection.execute("SELECT * FROM clearing_source_cursors WHERE source=? AND source_key=?",
                                 (source, source_key)).fetchone()
        if row:
            connection.execute(
                "UPDATE clearing_source_cursors SET last_confirmed_sequence=MAX(last_confirmed_sequence,?),updated_at=? WHERE source=? AND source_key=?",
                (sequence, self.clock.now(), source, source_key))

    def _audit(self, connection, context, action: str, instruction: dict, sequence: int, *, reason: str = "") -> None:
        detail = {"obligation_key": instruction["obligation_key"], "sequence": sequence}
        if reason:
            detail["reason"] = reason
        self.audit.append(connection, actor_id=context.actor_id, action=action,
                          entity_type="clearing_instructions", entity_id=instruction["instruction_id"],
                          version=1, detail=detail)

    def _instruction_view(self, instruction: dict, *, replayed: bool = False) -> dict:
        result = dict(instruction)
        result["replayed"] = replayed
        return result

    def _event_view(self, event: dict, *, replayed: bool = False) -> dict:
        result = dict(event)
        result["payload"] = json.loads(result.pop("payload_json"))
        result["replayed"] = replayed
        return result

    def _settled_view(self, connection, batch: dict, *, replayed: bool = False) -> dict:
        result = dict(batch)
        result["settled_entry_ids"] = json.loads(result.get("settled_entry_ids_json") or "[]")
        result["reversal_entry_ids"] = json.loads(result.get("reversal_entry_ids_json") or "[]")
        result["settlement_detail"] = json.loads(result.get("settlement_detail_json") or "[]")
        result["replayed"] = replayed
        return result
