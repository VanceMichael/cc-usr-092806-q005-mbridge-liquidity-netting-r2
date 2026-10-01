from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.clearing import PAUSE_COMPLIANCE, PAUSE_LIMIT, PAUSE_RATE
from civicflow.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext
from civicflow.timeutil import parse_instant


def iso(moment) -> str:
    return moment.isoformat().replace("+00:00", "Z")


class ClearingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "clearing.sqlite3"
        self.base = parse_instant("2026-09-28T12:00:00+08:00")
        self.app = CivicFlow.open(self.db_path, fixed_now=iso(self.base))
        self.op = AccessContext.system("operator")
        self.rc = AccessContext(actor_id="rate-clerk", permissions=frozenset({"write:rate", "read:clearing", "approve:clearing"}))
        self.ap = AccessContext(actor_id="approver", permissions=frozenset({
            "approve:clearing", "read:clearing", "bridge:clearing", "write:clearing", "resume:clearing", "pause:clearing", "adjust:clearing", "compliance:clearing"}))
        self._build_corridor()

    def tearDown(self):
        self.temp.cleanup()

    def _build_corridor(self):
        c = self.app.clearing
        c.register_corridor(self.op, corridor_id="cor:1", name="横琴-澳门桥", base_currency="CNY", quote_currency="MOP")
        c.add_participant(self.op, corridor_id="cor:1", org_id="org:A", authorized_counterparties=["org:B"])
        c.add_participant(self.op, corridor_id="cor:1", org_id="org:B", authorized_counterparties=["org:A", "org:C"])
        c.add_participant(self.op, corridor_id="cor:1", org_id="org:C", authorized_counterparties=["org:B"])
        c.set_limit(self.op, corridor_id="cor:1", org_id="org:A", currency="CNY", amount="1000.00")
        c.set_limit(self.op, corridor_id="cor:1", org_id="org:B", currency="CNY", amount="1000.00")
        c.set_limit(self.op, corridor_id="cor:1", org_id="org:C", currency="CNY", amount="1000.00")
        # A、B 资金充足以区分限额与流动性约束；C 资金较少用于触发流动性不足
        for org, ref, amount in (("org:A", "fund-a", "2000.00"), ("org:B", "fund-b", "2000.00"), ("org:C", "fund-c", "500.00")):
            c.fund_liquidity(self.op, corridor_id="cor:1", org_id=org, currency="CNY", amount=amount, reference=ref)

    def _window(self, *, start_minute=0, end_minute=30, app=None, rate=True):
        app = app or self.app
        window = app.clearing.open_window(self.op, corridor_id="cor:1",
            opens_at=iso(self.base + timedelta(minutes=start_minute)),
            closes_at=iso(self.base + timedelta(minutes=end_minute)))
        if rate:
            app.clearing.enter_rate(self.rc, window_id=window["window_id"], rate="1.08")
        return window

    def _at(self, minute: int) -> str:
        return iso(self.base + timedelta(minutes=minute))

    def _receive(self, key, *, amount="100.00", payer="org:A", payee="org:B", seq=0, occurred_minute=5, app=None):
        app = app or self.app
        return app.clearing.bridge_event(self.ap, source="bridge", source_key=key, sequence=seq, event_type="received",
            payload={"corridor_id": "cor:1", "obligation_ref": key, "payer_org": payer, "payee_org": payee, "currency": "CNY", "amount": amount},
            occurred_at=self._at(occurred_minute))

    def _app_at(self, minute: int) -> CivicFlow:
        return CivicFlow.open(self.db_path, fixed_now=self._at(minute))

    # -------------------------------------------------------------- 冻结与额度

    def test_freeze_locks_liquidity_and_window_limit(self):
        window = self._window()
        frozen = self._receive("obl-1")
        self.assertEqual(frozen["instruction_status"], "frozen")
        self.assertEqual(frozen["occupied_minor"], 10000)
        # 流动性被占用：余额 2000 - 冻结 100
        position = self.app.clearing.liquidity_position(self._org_ctx("org:A"), corridor_id="cor:1", org_id="org:A", currency="CNY")
        self.assertEqual(position["balance_minor"], 190000)
        self.assertEqual(position["occupied_minor"], 10000)
        # 本方限额可见，占用按窗口计量
        limit = self.app.clearing.get_limit(self._org_ctx("org:A"), corridor_id="cor:1", org_id="org:A", currency="CNY", window_id=window["window_id"])
        self.assertEqual(limit["available_minor"], 90000)

    def test_freeze_rejects_when_limit_or_liquidity_exceeded(self):
        self._window()
        with self.assertRaises(ConflictError):
            self._receive("big-limit", amount="1200.00")  # 超过窗口限额 1000
        with self.assertRaises(ConflictError):
            self._receive("big-liq", payer="org:C", payee="org:B", amount="600.00")  # 超过流动性 500

    def test_window_limits_are_independent_across_windows(self):
        w1 = self._window(end_minute=20)
        self._receive("obl-a", amount="900.00")
        # 同一窗口再冻 200 会超额
        with self.assertRaises(ConflictError):
            self._receive("obl-b", amount="200.00", occurred_minute=6)
        # 下一窗口额度重新可用
        self._window(start_minute=20, end_minute=40, app=self._app_at(20))
        late_app = self._app_at(21)
        frozen = late_app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-c", sequence=0, event_type="received",
            payload={"corridor_id": "cor:1", "obligation_ref": "obl-c", "payer_org": "org:A", "payee_org": "org:B", "currency": "CNY", "amount": "200.00"},
            occurred_at=self._at(21))
        self.assertNotEqual(frozen["window_id"], w1["window_id"])
        self.assertEqual(frozen["occupied_minor"], 20000)

    # -------------------------------------------------------------- 来源序列

    def test_sequence_gap_is_buffered_then_drained(self):
        self._window()
        self._receive("obl-1")
        # 序号 2 先到形成缺口，缓存为 waiting
        gap = self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=2, event_type="returned")
        self.assertEqual(gap["status"], "waiting_gap")
        self.assertTrue(self.app.clearing.checkpoint(self.op, source="bridge")["sources"][0]["has_gap"])
        # 补缺序号 1（匹配），缓存的序号 2（退回）随后按序自动推进
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        instr = self.app.clearing.list_instructions(self.op, corridor_id="cor:1")
        self.assertEqual(instr[0]["status"], "returned")
        checkpoint = self.app.clearing.checkpoint(self.op, source="bridge")
        self.assertEqual(checkpoint["sources"][0]["last_confirmed_sequence"], 2)
        self.assertFalse(checkpoint["sources"][0]["has_gap"])

    def test_sequence_must_not_move_backwards(self):
        self._window()
        self._receive("obl-1", seq=0)
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        with self.assertRaises(ConflictError):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=0, event_type="matched")

    def test_same_sequence_divergent_payload_is_quarantined(self):
        self._window()
        self._receive("obl-1")
        with self.assertRaises(ConflictError):
            self._receive("obl-1", amount="999.00")  # 同序号异文
        conflicts = self.app.clearing.conflicts(self.op, source="bridge")
        self.assertEqual(len(conflicts), 1)
        # 完全相同的重放返回原批次
        replay = self._receive("obl-1")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["status"], "duplicate")

    # -------------------------------------------------------------- 迟到与唯一批次

    def test_late_event_goes_to_next_available_window(self):
        self._window(end_minute=20)
        self._window(start_minute=20, end_minute=40)
        app = self._app_at(25)  # 第一窗口已关
        result = app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-late", sequence=0, event_type="received",
            payload={"corridor_id": "cor:1", "obligation_ref": "obl-late", "payer_org": "org:A", "payee_org": "org:B", "currency": "CNY", "amount": "10.00"},
            occurred_at=self._at(5))
        self.assertTrue(result["late"])
        windows = app.clearing.list_windows(self.op, corridor_id="cor:1")
        self.assertEqual(result["window_id"], windows[1]["window_id"])

    def test_instruction_cannot_be_in_two_active_batches(self):
        self._window(end_minute=20)
        first = self._receive("obl-1")
        # 不同来源事件重复承载同一义务：业务层拒绝，避免同一义务占用两笔流动性
        with self.assertRaises(ConflictError):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1-dup", sequence=0, event_type="received",
                payload={"corridor_id": "cor:1", "obligation_ref": "obl-1", "payer_org": "org:A", "payee_org": "org:B", "currency": "CNY", "amount": "100.00"},
                occurred_at=self._at(6))
        # 同一来源键的重放则幂等返回原批次
        replay = self._receive("obl-1", occurred_minute=6)
        self.assertEqual(replay["status"], "duplicate")
        self.assertEqual(replay["batch_id"], first["batch_id"])
        # 撤销后释放占用，义务可在新窗口用新来源键重新受理
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="cancelled")
        self._window(start_minute=20, end_minute=40)
        app = self._app_at(21)
        second = app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1-retry", sequence=0, event_type="received",
            payload={"corridor_id": "cor:1", "obligation_ref": "obl-1", "payer_org": "org:A", "payee_org": "org:B", "currency": "CNY", "amount": "100.00"},
            occurred_at=self._at(21))
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        # 旧成员已移除、新成员有效：活跃成员关系全局唯一
        with self.app.database.connect() as conn:
            active = conn.execute("SELECT COUNT(*) AS n FROM cl_batch_members m JOIN cl_instructions i ON i.instruction_id=m.instruction_id WHERE i.obligation_ref='obl-1' AND m.status='active'").fetchone()["n"]
        self.assertEqual(active, 1)

    # -------------------------------------------------------------- 生命周期顺序

    def test_lifecycle_requires_order_and_closed_batch(self):
        self._window()
        self._receive("obl-1")
        with self.assertRaises(ConflictError):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="settled")  # 未匹配
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        with self.assertRaises(ConflictError):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=2, event_type="settled")  # 未关账

    def test_closed_batch_cannot_be_returned(self):
        window = self._window(end_minute=20)
        self._receive("obl-1")
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        app = self._app_at(21)
        batch = app.clearing.list_windows(self.op, corridor_id="cor:1")[0]["batches"][0]
        app.clearing.close_batch(self.ap, batch_id=batch["batch_id"])
        with self.assertRaises(ConflictError):
            app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=2, event_type="returned")

    # -------------------------------------------------------------- 暂停

    def test_limit_change_pauses_then_resumes(self):
        window = self._window()
        self._receive("obl-1", amount="200.00")
        # 限额降到不足 → 批次暂停
        self.app.clearing.set_limit(self.op, corridor_id="cor:1", org_id="org:A", currency="CNY", amount="50.00")
        with self.app.database.connect() as conn:
            batch_id = conn.execute("SELECT batch_id FROM cl_instructions WHERE obligation_ref='obl-1'").fetchone()["batch_id"]
        self.assertEqual(self.app.clearing.get_batch(self.op, batch_id)["status"], "paused")
        # 暂停期间桥侧事件被拒
        with self.assertRaises(ConflictError):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        # 限额仍不足不能恢复
        with self.assertRaises(ConflictError):
            self.app.clearing.resume_batch(self.ap, batch_id=batch_id)
        # 限额恢复充足后放行
        self.app.clearing.set_limit(self.op, corridor_id="cor:1", org_id="org:A", currency="CNY", amount="1000.00")
        resumed = self.app.clearing.resume_batch(self.ap, batch_id=batch_id)
        self.assertEqual(resumed["status"], "gathering")

    def test_rate_revision_pauses_and_adopts_new_rate(self):
        window = self._window()
        frozen = self._receive("obl-1")
        self.app.clearing.enter_rate(self.rc, window_id=window["window_id"], rate="1.09", reason="盘中修订")
        batch = self.app.clearing.get_batch(self.op, frozen["batch_id"])
        self.assertEqual(batch["status"], "paused")
        self.assertEqual(batch["pause_reason"], PAUSE_RATE)
        self.app.clearing.resume_batch(self.ap, batch_id=frozen["batch_id"])
        instr = self.app.clearing.list_instructions(self.op, corridor_id="cor:1")[0]
        window_after = self.app.clearing.get_window(self.op, window["window_id"])
        self.assertEqual(instr["rate_id"], window_after["rate_id"])

    def test_compliance_hit_pauses_batch(self):
        self._window()
        frozen = self._receive("obl-1")
        self.app.clearing.flag_compliance(self.ap, instruction_id=frozen["instruction_id"], reason="命中制裁名单复核")
        batch = self.app.clearing.get_batch(self.op, frozen["batch_id"])
        self.assertEqual(batch["status"], "paused")
        self.assertEqual(batch["pause_reason"], PAUSE_COMPLIANCE)

    # -------------------------------------------------------------- 关账职责分离

    def test_rate_entrant_cannot_approve_close(self):
        window = self._window(end_minute=20)
        self._receive("obl-1")
        app = self._app_at(21)
        batch_id = app.clearing.list_windows(self.op, corridor_id="cor:1")[0]["batches"][0]["batch_id"]
        with self.assertRaises(PermissionDenied):
            app.clearing.close_batch(self.rc, batch_id=batch_id)
        closed = app.clearing.close_batch(self.ap, batch_id=batch_id)
        self.assertEqual(closed["status"], "closed")

    def test_cannot_close_before_deadline(self):
        self._window(end_minute=30)
        self._receive("obl-1")
        batch_id = self.app.clearing.list_windows(self.op, corridor_id="cor:1")[0]["batches"][0]["batch_id"]
        with self.assertRaises(ConflictError):
            self.app.clearing.close_batch(self.ap, batch_id=batch_id)

    def test_net_positions_and_full_settlement(self):
        window = self._window(end_minute=20)
        self._receive("obl-1", amount="120.00", payer="org:A", payee="org:B")
        self._receive("obl-2", amount="80.00", payer="org:B", payee="org:A", occurred_minute=6)
        for key in ("obl-1", "obl-2"):
            self.app.clearing.bridge_event(self.ap, source="bridge", source_key=key, sequence=1, event_type="matched")
        app = self._app_at(21)
        windows = app.clearing.list_windows(self.op, corridor_id="cor:1")
        batch_id = windows[0]["batches"][0]["batch_id"]
        closed = app.clearing.close_batch(self.ap, batch_id=batch_id)
        # 净额：A -40, B +40
        self.assertEqual(closed["net_positions"], {"org:A": -4000, "org:B": 4000})
        for key in ("obl-1", "obl-2"):
            app.clearing.bridge_event(self.ap, source="bridge", source_key=key, sequence=2, event_type="settled")
        batch = app.clearing.get_batch(self.op, batch_id)
        self.assertEqual(batch["status"], "settled")
        # 付款方最终净流出 40：2000 - 120 + 80（结算守恒）
        pos_a = app.clearing.liquidity_position(self._org_ctx("org:A"), corridor_id="cor:1", org_id="org:A", currency="CNY")
        self.assertEqual(pos_a["balance_minor"], 196000)

    # -------------------------------------------------------------- 反向调整

    def test_settled_batch_only_reversed_in_new_window(self):
        window = self._window(end_minute=20)
        self._receive("obl-1", amount="120.00")
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        app = self._app_at(21)
        batch_id = app.clearing.list_windows(self.op, corridor_id="cor:1")[0]["batches"][0]["batch_id"]
        app.clearing.close_batch(self.ap, batch_id=batch_id)
        app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=2, event_type="settled")
        # 原结算记录不可抹除
        with self.assertRaises(ConflictError):
            app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=3, event_type="returned")
        # 新窗口做反向调整
        w2 = app.clearing.open_window(self.op, corridor_id="cor:1", opens_at=self._at(22), closes_at=self._at(40))
        app.clearing.enter_rate(self.rc, window_id=w2["window_id"], rate="1.085")
        adjustment = app.clearing.reverse_settled_batch(self.ap, batch_id=batch_id, new_window_id=w2["window_id"], reason="金额多划")
        self.assertEqual(adjustment["status"], "reversed")
        self.assertEqual(len(adjustment["reversal_entries"]), 2)  # 借、贷各一笔反向
        # 重复调整被拒
        with self.assertRaises(ConflictError):
            app.clearing.reverse_settled_batch(self.ap, batch_id=batch_id, new_window_id=w2["window_id"], reason="再次")
        # 原批次仍为已结算，原分录仍保留；新窗口出现反向义务指令
        self.assertEqual(app.clearing.get_batch(self.op, batch_id)["status"], "settled")
        new_instructions = app.clearing.list_instructions(self.op, corridor_id="cor:1", window_id=w2["window_id"])
        self.assertEqual(len(new_instructions), 1)
        self.assertEqual(new_instructions[0]["payer_org"], "org:B")
        self.assertEqual(new_instructions[0]["payee_org"], "org:A")

    # -------------------------------------------------------------- 可见性

    def _org_ctx(self, org: str) -> AccessContext:
        return AccessContext(actor_id=f"user-{org}", permissions=frozenset({"read:clearing", "fund:clearing"}), org_id=org)

    def test_org_sees_only_own_limit_and_authorized_fields(self):
        self._window()
        self._receive("obl-1", payer="org:A", payee="org:B")
        ctx_a = self._org_ctx("org:A")
        # 只能看本方限额
        self.app.clearing.get_limit(ctx_a, corridor_id="cor:1", org_id="org:A", currency="CNY")
        with self.assertRaises(PermissionDenied):
            self.app.clearing.get_limit(ctx_a, corridor_id="cor:1", org_id="org:B", currency="CNY")
        # 授权对手方字段可见
        row = self.app.clearing.list_instructions(ctx_a, corridor_id="cor:1")[0]
        self.assertEqual(row["payee_org"], "org:B")
        # 撤销授权后对手方字段被遮蔽
        self.app.clearing.update_counterparties(self.op, corridor_id="cor:1", org_id="org:A", authorized_counterparties=[])
        row = self.app.clearing.list_instructions(ctx_a, corridor_id="cor:1")[0]
        self.assertEqual(row["payee_org"], "***")
        # 机构不能给别家注资
        with self.assertRaises(PermissionDenied):
            self.app.clearing.fund_liquidity(ctx_a, corridor_id="cor:1", org_id="org:B", currency="CNY", amount="1.00", reference="x")

    def test_uninvolved_org_sees_no_instructions(self):
        self._window()
        self._receive("obl-1", payer="org:A", payee="org:B")
        rows = self.app.clearing.list_instructions(self._org_ctx("org:C"), corridor_id="cor:1")
        self.assertEqual(rows, [])

    # -------------------------------------------------------------- 解释与恢复

    def test_explain_obligation_trace(self):
        window = self._window(end_minute=20)
        self._receive("obl-1", amount="120.00")
        self.app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=1, event_type="matched")
        app = self._app_at(21)
        batch_id = app.clearing.list_windows(self.op, corridor_id="cor:1")[0]["batches"][0]["batch_id"]
        app.clearing.close_batch(self.ap, batch_id=batch_id)
        app.clearing.bridge_event(self.ap, source="bridge", source_key="obl-1", sequence=2, event_type="settled")
        trace = app.clearing.explain(self.op, obligation_ref="obl-1")["trace"][0]
        self.assertEqual(trace["window_seq"], 1)
        self.assertEqual(trace["rate"], "1.08")
        self.assertEqual(trace["occupied_after_freeze_minor"], 12000)
        self.assertEqual(trace["limit_amount_minor"], 100000)
        self.assertTrue(trace["released_by"])
        self.assertTrue(trace["debit_entry_id"])
        with self.assertRaises(NotFoundError):
            app.clearing.explain(self.op, obligation_ref="missing")

    def test_explain_org_view_redacts_counterparty_limit(self):
        self._window(end_minute=20)
        self._receive("obl-1", amount="120.00", payer="org:A", payee="org:B")
        # 收款方 B 能解释义务，但看不到付款方 A 的限额金额
        trace_b = self.app.clearing.explain(self._org_ctx("org:B"), obligation_ref="obl-1")["trace"][0]
        self.assertEqual(trace_b["rate"], "1.08")
        self.assertEqual(trace_b["payer_org"], "org:A")  # B 已被 A 授权为对手方
        self.assertEqual(trace_b["limit_amount_minor"], "***")
        # 付款方 A 可见本方限额
        trace_a = self.app.clearing.explain(self._org_ctx("org:A"), obligation_ref="obl-1")["trace"][0]
        self.assertEqual(trace_a["limit_amount_minor"], 100000)
        # 无关机构只看到不可见占位
        trace_c = self.app.clearing.explain(self._org_ctx("org:C"), obligation_ref="obl-1")["trace"][0]
        self.assertFalse(trace_c.get("visible", True))

    def test_checkpoint_resumes_from_sequence_and_holdings(self):
        self._window()
        self._receive("obl-1", amount="100.00")
        cp = self.app.clearing.checkpoint(self.op, source="bridge")
        self.assertEqual(cp["last_confirmed_sequence"], 0)
        self.assertEqual(cp["liquidity_held"][0]["held_minor"], 10000)
        # 服务中断后重新打开库，占用仍在
        reopened = self._app_at(5)
        cp2 = reopened.clearing.checkpoint(self.op, source="bridge")
        self.assertEqual(cp2["liquidity_held"][0]["held_minor"], 10000)


if __name__ == "__main__":
    unittest.main()
