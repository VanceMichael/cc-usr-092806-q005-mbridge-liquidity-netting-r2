from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, InvariantViolation, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext


class ClearingTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = CivicFlow.open(Path(self.temp.name) / "clearing.sqlite3",
                                  fixed_now="2026-10-01T12:00:00+08:00")
        self.operator = AccessContext.system("operator")
        self.fx_clerk = AccessContext(actor_id="fx-clerk",
                                      permissions=frozenset({"write:corridors", "write:windows", "write:fx",
                                                             "read:corridors", "read:limits", "read:batches"}))
        self.closer = AccessContext(actor_id="closer",
                                    permissions=frozenset({"close:windows", "read:corridors",
                                                           "read:limits", "read:batches"}))
        self.settler = AccessContext(actor_id="settler",
                                     permissions=frozenset({"settle:bridge", "read:corridors",
                                                            "read:limits", "read:batches", "write:bridge"}))
        self.corridor = self.app.clearing.register_corridor(
            self.operator, corridor_code="HQMO", currency_pay="MOP", currency_receive="CNH",
            window_duration_minutes=60, cutoff_offset_minutes=5)
        self.cid = self.corridor["corridor_id"]
        self.app.clearing.add_participant(self.operator, corridor_id=self.cid,
                                          organization_id="org:hq", authorized_counterparties=["org:mo"])
        self.app.clearing.add_participant(self.operator, corridor_id=self.cid,
                                          organization_id="org:mo", authorized_counterparties=["org:hq"])
        self.app.clearing.set_limit(self.operator, corridor_id=self.cid, organization_id="org:hq",
                                    currency="MOP", amount="1000.00")
        self.app.clearing.set_limit(self.operator, corridor_id=self.cid, organization_id="org:mo",
                                    currency="MOP", amount="1000.00")
        self.w1 = self.app.clearing.open_window(self.fx_clerk, corridor_id=self.cid,
                                                opens_at="2026-10-01T09:00:00+08:00", fx_rate="0.8900")
        self.w2 = self.app.clearing.open_window(self.fx_clerk, corridor_id=self.cid,
                                                opens_at="2026-10-01T10:00:00+08:00", fx_rate="0.8920")
        self.w3 = self.app.clearing.open_window(self.fx_clerk, corridor_id=self.cid,
                                                opens_at="2026-10-01T11:00:00+08:00", fx_rate="0.8910")

    def tearDown(self):
        self.temp.cleanup()

    def _instruction(self, seq, obl, payer="org:hq", payee="org:mo", amount="100.00",
                     at="2026-10-01T09:10:00+08:00"):
        payload = {"corridor_id": self.cid, "organization_id": payer, "counterparty_id": payee,
                   "currency": "MOP", "amount": amount, "obligation_key": obl}
        return self.app.bridge.admit_event(self.operator, source="bridge", source_key="water",
                                           sequence=seq, event_kind="instruction", payload=payload,
                                           occurred_at=at)

    def _lifecycle(self, start_seq, obl, payer="org:hq", payee="org:mo", amount="100.00",
                   at="2026-10-01T09:10:00+08:00"):
        self._instruction(start_seq, obl, payer, payee, amount, at)
        self.app.bridge.admit_event(self.operator, source="bridge", source_key="water",
                                    sequence=start_seq + 1, event_kind="accept",
                                    payload={"obligation_key": obl}, occurred_at=at)
        matched = self.app.bridge.admit_event(self.operator, source="bridge", source_key="water",
                                              sequence=start_seq + 2, event_kind="match",
                                              payload={"obligation_key": obl}, occurred_at=at)
        return matched

    def _close_and_settle(self, window):
        self.app.clearing.approve_window_close(self.closer, window_id=window["window_id"],
                                               expected_fx_rate_id=window["fx_rate_id"])
        batches = self.app.clearing.list_window_batches(self.operator, window_id=window["window_id"])
        return self.app.bridge.settle_batch(self.settler, batch_id=batches[0]["batch_id"])


class CorridorRegistrationTest(ClearingTestBase):
    def test_same_currency_rejected(self):
        with self.assertRaises(ValidationError):
            self.app.clearing.register_corridor(
                self.operator, corridor_code="SAME", currency_pay="MOP", currency_receive="MOP",
                window_duration_minutes=60)

    def test_duplicate_corridor_code(self):
        with self.assertRaises(ConflictError):
            self.app.clearing.register_corridor(
                self.operator, corridor_code="HQMO", currency_pay="MOP", currency_receive="CNH",
                window_duration_minutes=60)

    def test_window_boundaries_and_cutoff(self):
        self.assertEqual(self.w1["opens_at"], "2026-10-01T01:00:00Z")
        self.assertEqual(self.w1["closes_at"], "2026-10-01T02:00:00Z")
        self.assertEqual(self.w1["cutoff_at"], "2026-10-01T01:55:00Z")

    def test_window_cannot_overlap(self):
        with self.assertRaises(ConflictError):
            self.app.clearing.open_window(self.fx_clerk, corridor_id=self.cid,
                                          opens_at="2026-10-01T09:30:00+08:00", fx_rate="0.9")

    def test_limit_currency_must_belong_to_corridor(self):
        with self.assertRaises(ValidationError):
            self.app.clearing.set_limit(self.operator, corridor_id=self.cid,
                                        organization_id="org:hq", currency="USD", amount="10")


class NettingFlowTest(ClearingTestBase):
    def test_multiple_instructions_net_to_single_batch(self):
        m1 = self._lifecycle(0, "obl:a", amount="100.00")
        m2 = self._lifecycle(3, "obl:b", amount="40.00")
        m3 = self._lifecycle(6, "obl:c", payer="org:mo", payee="org:hq", amount="30.00",
                             at="2026-10-01T09:40:00+08:00")
        self.assertEqual(m1["batch_id"], m2["batch_id"])
        self.assertEqual(m1["batch_id"], m3["batch_id"])
        positions = self.app.clearing.batch_positions(self.operator, m1["batch_id"])
        hq = next(p for p in positions if p["organization_id"] == "org:hq")
        mo = next(p for p in positions if p["organization_id"] == "org:mo")
        self.assertEqual((hq["gross_pay_minor"], hq["gross_receive_minor"], hq["net_minor"]), (14000, 3000, 11000))
        self.assertEqual(hq["net_direction"], "pay")
        self.assertEqual(mo["net_minor"], -11000)
        self.assertEqual(mo["net_direction"], "receive")

    def test_settlement_posts_immutable_entries_and_releases_holds(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")
        settled = self._close_and_settle(self.w1)
        self.assertEqual(settled["status"], "settled")
        self.assertEqual(len(settled["settlement_detail"]), 1)
        with self.app.database.connect() as conn:
            holds = [dict(r) for r in conn.execute("SELECT status FROM clearing_holds WHERE batch_id=?",
                                                   (matched["batch_id"],))]
        self.assertTrue(holds and all(h["status"] == "settled" for h in holds))

    def test_settled_batch_cannot_settle_again(self):
        self._lifecycle(0, "obl:a", amount="10.00")
        settled = self._close_and_settle(self.w1)
        again = self.app.bridge.settle_batch(self.settler, batch_id=settled["batch_id"])
        self.assertTrue(again["replayed"])
        self.assertEqual(len(again["settlement_detail"]), 1)

    def test_obligation_cannot_appear_in_two_batches(self):
        matched = self._lifecycle(0, "obl:a", amount="10.00")
        # 用全新来源序号重发同一义务（跨日重试场景）：回到原批次，不二次占用。
        payload = {"corridor_id": self.cid, "organization_id": "org:hq", "counterparty_id": "org:mo",
                   "currency": "MOP", "amount": "10.00", "obligation_key": "obl:a"}
        replayed = self.app.bridge.admit_event(
            self.operator, source="bridge-next-day", source_key="water", sequence=0,
            event_kind="instruction", payload=payload, occurred_at="2026-10-02T09:00:00+08:00")
        self.assertTrue(replayed["replayed"])
        self.assertTrue(replayed.get("duplicate_retry"))
        self.assertEqual(replayed["batch_id"], matched["batch_id"])
        self.app.verify()  # 不变量校验通过

    def test_liquidity_limit_blocks_freeze(self):
        self._lifecycle(0, "obl:big", amount="900.00")
        with self.assertRaises(ConflictError):
            self._lifecycle(3, "obl-over", amount="200.00")

    def test_unauthorized_counterparty_rejected(self):
        with self.assertRaises(PermissionDenied):
            self._lifecycle(0, "obl:x", payee="org:stranger")


class SequenceAndWindowTest(ClearingTestBase):
    def test_gap_in_sequence_rejected(self):
        self._instruction(0, "obl:a")
        with self.assertRaises(ConflictError):
            self._instruction(2, "obl:b")

    def test_duplicate_sequence_same_payload_is_idempotent(self):
        first = self._instruction(0, "obl:a")
        second = self._instruction(0, "obl:a")
        self.assertTrue(second["replayed"])
        self.assertEqual(first["instruction_id"], second["instruction_id"])

    def test_same_sequence_divergent_payload_quarantined(self):
        self._instruction(0, "obl:a")
        with self.assertRaises(ConflictError):
            self._instruction(0, "obl:different")
        with self.app.database.connect() as conn:
            conflicts = conn.execute("SELECT COUNT(*) AS n FROM inbox_conflicts").fetchone()["n"]
        self.assertEqual(conflicts, 1)

    def test_late_event_goes_to_next_window(self):
        # 09:58 晚于 09:55 截止时间 -> 进入窗口二。
        late = self._instruction(0, "obl:late", at="2026-10-01T09:58:00+08:00")
        self.assertEqual(late["window_id"], self.w2["window_id"])
        self.assertTrue(late["late"])

    def test_event_after_window_closed_rolls_forward_on_match(self):
        admitted = self._instruction(0, "obl:a", at="2026-10-01T09:50:00+08:00")
        self.assertEqual(admitted["window_id"], self.w1["window_id"])
        self.app.bridge.admit_event(self.operator, source="bridge", source_key="water", sequence=1,
                                    event_kind="accept", payload={"obligation_key": "obl:a"},
                                    occurred_at="2026-10-01T09:50:00+08:00")
        # 窗口一关账，未匹配指令在匹配时滚入下一窗口。
        self.app.clearing.approve_window_close(self.closer, window_id=self.w1["window_id"],
                                               expected_fx_rate_id=self.w1["fx_rate_id"])
        matched = self.app.bridge.admit_event(self.operator, source="bridge", source_key="water", sequence=2,
                                              event_kind="match", payload={"obligation_key": "obl:a"},
                                              occurred_at="2026-10-01T10:05:00+08:00")
        self.assertEqual(matched["window_id"], self.w2["window_id"])
        self.assertEqual(matched["batch_id"],
                         self.app.clearing.list_window_batches(self.operator,
                                                               window_id=self.w2["window_id"])[0]["batch_id"])


class HoldAndClosePolicyTest(ClearingTestBase):
    def _one_frozen(self, obl="obl:a", amount="100.00"):
        return self._lifecycle(0, obl, amount=amount)

    def test_fx_clerk_cannot_approve_close(self):
        self._one_frozen()
        with self.assertRaises(PermissionDenied):
            self.app.clearing.approve_window_close(self.fx_clerk, window_id=self.w1["window_id"],
                                                   expected_fx_rate_id=self.w1["fx_rate_id"])

    def test_fx_revision_before_close_holds_batch(self):
        matched = self._one_frozen()
        self.app.clearing.revise_window_fx(self.fx_clerk, window_id=self.w1["window_id"],
                                           rate_value="0.9100", reason="盘口修订")
        batch = self.app.clearing.get_batch(self.operator, matched["batch_id"])
        self.assertEqual(batch["status"], "on_hold")
        self.assertEqual(batch["hold_reason"], "fx_revision")
        with self.assertRaises(ConflictError):
            self.app.clearing.approve_window_close(self.closer, window_id=self.w1["window_id"],
                                                   expected_fx_rate_id=self.w1["fx_rate_id"])
        # 恢复后可关账；关账必须匹配新的汇率快照。
        self.app.clearing.resume_batch(self.operator, batch_id=matched["batch_id"], reason="修订确认")
        window = self.app.clearing.get_window(self.operator, self.w1["window_id"])
        approved = self.app.clearing.approve_window_close(self.closer, window_id=self.w1["window_id"],
                                                          expected_fx_rate_id=window["fx_rate_id"])
        self.assertEqual(approved["state"], "ready")

    def test_limit_change_before_close_holds_affected_batch(self):
        matched = self._one_frozen()
        self.app.clearing.set_limit(self.operator, corridor_id=self.cid, organization_id="org:hq",
                                    currency="MOP", amount="50.00")
        batch = self.app.clearing.get_batch(self.operator, matched["batch_id"])
        self.assertEqual(batch["status"], "on_hold")
        self.assertEqual(batch["hold_reason"], "limit_change")

    def test_compliance_hit_holds_batch_and_resume(self):
        matched = self._one_frozen()
        held = self.app.clearing.mark_compliance_hit(self.operator, batch_id=matched["batch_id"],
                                                     reason="名单命中待核")
        self.assertEqual(held["status"], "on_hold")
        with self.assertRaises(ValidationError):
            self.app.clearing.resume_batch(self.operator, batch_id=matched["batch_id"], reason="  ")
        resumed = self.app.clearing.resume_batch(self.operator, batch_id=matched["batch_id"], reason="已排除")
        self.assertEqual(resumed["status"], "building")

    def test_cannot_freeze_into_held_batch(self):
        self._one_frozen("obl:a")
        self.app.clearing.mark_compliance_hit(self.operator,
                                              batch_id=self.app.clearing.list_window_batches(
                                                  self.operator, window_id=self.w1["window_id"])[0]["batch_id"],
                                              reason="名单命中")
        with self.assertRaises(ConflictError):
            self._lifecycle(3, "obl:b")


class ReturnCancelAdjustTest(ClearingTestBase):
    def test_return_releases_liquidity(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")
        self.app.bridge.return_instruction(self.operator, instruction_id=matched["instruction_id"], reason="要素有误")
        explanation = self.app.clearing.explain_obligation(self.operator, "obl:a")
        self.assertEqual(explanation["instruction_state"], "returned")
        self.assertEqual(explanation["liquidity_hold"]["status"], "released")
        # 退回后不影响其它义务结算。
        self._lifecycle(3, "obl:b", amount="20.00")

    def test_cancel_before_freeze(self):
        admitted = self._instruction(0, "obl:a")
        self.app.bridge.cancel_instruction(self.operator, instruction_id=admitted["instruction_id"], reason="撤单")
        explanation = self.app.clearing.explain_obligation(self.operator, "obl:a")
        self.assertEqual(explanation["instruction_state"], "cancelled")

    def test_settled_obligation_cannot_return_or_cancel(self):
        matched = self._lifecycle(0, "obl:a", amount="10.00")
        self._close_and_settle(self.w1)
        with self.assertRaises(ConflictError):
            self.app.bridge.return_instruction(self.operator, instruction_id=matched["instruction_id"], reason="x")
        with self.assertRaises(ConflictError):
            self.app.bridge.cancel_instruction(self.operator, instruction_id=matched["instruction_id"], reason="x")

    def test_settled_batch_adjusted_only_by_reversal_in_new_window(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")
        self._close_and_settle(self.w1)
        with self.assertRaises(ConflictError):
            # 不能在原窗口调整。
            self.app.bridge.adjust_settled_batch(self.settler, batch_id=matched["batch_id"],
                                                 target_window_id=self.w1["window_id"], reason="冲销")
        adjustment = self.app.bridge.adjust_settled_batch(
            self.settler, batch_id=matched["batch_id"], target_window_id=self.w3["window_id"], reason="单价修订")
        self.assertEqual(adjustment["reverses_batch_id"], matched["batch_id"])
        self.assertEqual(adjustment["status"], "settled")
        self.assertEqual(len(adjustment["reversal_entry_ids"]), 1)
        # 原批次保留并变为 adjusted，原分录仍在。
        original = self.app.clearing.get_batch(self.operator, matched["batch_id"])
        self.assertEqual(original["status"], "adjusted")
        with self.app.database.connect() as conn:
            original_entries = conn.execute(
                "SELECT COUNT(*) AS n FROM journal_entries WHERE reversed_entry_id IS NOT NULL").fetchone()["n"]
            gross_entries = conn.execute(
                "SELECT COUNT(*) AS n FROM journal_entries WHERE reference LIKE ?",
                (matched["batch_id"] + "%",)).fetchone()["n"]
        self.assertEqual(original_entries, 1)
        self.assertGreaterEqual(gross_entries, 1)
        # 同一原批次只能调整一次。
        with self.assertRaises(ConflictError):
            self.app.bridge.adjust_settled_batch(self.settler, batch_id=matched["batch_id"],
                                                 target_window_id=self.w3["window_id"], reason="再次")


class VisibilityTest(ClearingTestBase):
    def test_participant_sees_only_own_limits(self):
        limits = self.app.clearing.list_window_batches  # 仅为可读性占位
        hq_ctx = AccessContext(actor_id="hq-user", org_id="org:hq",
                               permissions=frozenset({"read:limits", "read:corridors", "read:batches"}))
        mo_ctx = AccessContext(actor_id="mo-user", org_id="org:mo",
                               permissions=frozenset({"read:limits", "read:corridors", "read:batches"}))
        with self.app.database.connect() as conn:
            hq_limit_id = conn.execute(
                "SELECT limit_id FROM clearing_limits WHERE organization_id='org:hq' ORDER BY version DESC LIMIT 1").fetchone()["limit_id"]
            mo_limit_id = conn.execute(
                "SELECT limit_id FROM clearing_limits WHERE organization_id='org:mo' ORDER BY version DESC LIMIT 1").fetchone()["limit_id"]
        self.assertEqual(self.app.clearing.get_limit(hq_ctx, hq_limit_id)["organization_id"], "org:hq")
        with self.assertRaises(PermissionDenied):
            self.app.clearing.get_limit(hq_ctx, mo_limit_id)

    def test_participant_batch_items_scoped_to_authorization(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")
        self.app.clearing.get_batch  # noop
        hq_ctx = AccessContext(actor_id="hq-user", org_id="org:hq",
                               permissions=frozenset({"read:batches", "read:corridors", "read:limits"}))
        stranger_ctx = AccessContext(actor_id="x-user", org_id="org:stranger",
                                     permissions=frozenset({"read:batches", "read:corridors", "read:limits"}))
        items = self.app.clearing.batch_items(hq_ctx, matched["batch_id"])
        self.assertEqual({i["obligation_key"] for i in items}, {"obl:a"})
        with self.assertRaises(PermissionDenied):
            self.app.clearing.get_batch(stranger_ctx, matched["batch_id"])

    def test_explain_own_obligation_only(self):
        self._lifecycle(0, "obl:a")
        mo_ctx = AccessContext(actor_id="mo-user", org_id="org:mo",
                               permissions=frozenset({"read:batches", "read:corridors", "read:limits"}))
        # 澳门是该笔的对手方且在授权名单中：可通过批次视图看到，但不能以他人义务名义解释。
        with self.assertRaises(PermissionDenied):
            self.app.clearing.explain_obligation(mo_ctx, "obl:a")
        hq_ctx = AccessContext(actor_id="hq-user", org_id="org:hq",
                               permissions=frozenset({"read:batches", "read:corridors", "read:limits"}))
        answer = self.app.clearing.explain_obligation(hq_ctx, "obl:a")
        self.assertEqual(answer["window"]["window_index"], 0)
        self.assertEqual(answer["fx"]["rate_id"], self.w1["fx_rate_id"])
        self.assertEqual(answer["liquidity_hold"]["amount_minor"], 10000)
        self.assertEqual(answer["limit"]["amount_minor"], 100000)


class RecoveryAndExplainTest(ClearingTestBase):
    def test_explain_reports_window_fx_hold_and_releaser(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")
        self._close_and_settle(self.w1)
        answer = self.app.clearing.explain_obligation(self.operator, "obl:a")
        self.assertEqual(answer["window"]["window_index"], 0)
        self.assertEqual(answer["fx"]["rate_id"], self.w1["fx_rate_id"])
        self.assertEqual(answer["liquidity_hold"]["amount_minor"], 10000)
        self.assertEqual(answer["limit"]["amount_minor"], 100000)
        self.assertEqual(answer["released_by"], "closer")
        self.assertEqual(answer["netting"]["leg"], "gross")

    def test_recover_from_last_confirmed_sequence_and_holds(self):
        matched = self._lifecycle(0, "obl:a", amount="100.00")  # 序号 0,1,2
        self._instruction(3, "obl:b", at="2026-10-01T09:30:00+08:00")  # 已受理未冻结
        position = self.app.bridge.recover("bridge", "water")
        self.assertEqual(position["last_sequence"], 3)
        self.assertEqual(position["last_confirmed_sequence"], 2)
        self.assertEqual(len(position["outstanding_holds"]), 1)
        self.assertEqual([i["obligation_key"] for i in position["resumable"]], ["obl:b"])
        # 重放已确认事件返回原批次。
        replay = self.app.bridge.admit_event(self.operator, source="bridge", source_key="water", sequence=2,
                                             event_kind="match", payload={"obligation_key": "obl:a"},
                                             occurred_at="2026-10-01T09:10:00+08:00")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["batch_id"], matched["batch_id"])

    def test_recovery_restarts_with_fresh_app_instance(self):
        self._lifecycle(0, "obl:a", amount="100.00")
        reopened = CivicFlow.open(Path(self.temp.name) / "clearing.sqlite3",
                                  fixed_now="2026-10-01T12:00:00+08:00")
        position = reopened.bridge.recover("bridge", "water")
        self.assertEqual(position["last_confirmed_sequence"], 2)
        self.assertEqual(len(position["outstanding_holds"]), 1)


if __name__ == "__main__":
    unittest.main()
