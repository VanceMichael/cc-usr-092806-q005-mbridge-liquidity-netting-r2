"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .application import CivicFlow
from .cases import CaseService
from .errors import PermissionDenied
from .security import AccessContext


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def demo(app: CivicFlow) -> dict:
    context = AccessContext.system("demo-operator")
    cases = CaseService(app.repository)
    created = cases.create(context, {"case_type": "协同事项", "subject": "示例联合处置", "owner_org": "org:demo", "priority": "high", "opened_at": app.clock.now()}, request_key="demo-case")
    accepted = app.inbox.receive(source="demo", source_key=created["entity_id"], sequence=1, payload={"kind": "opened"}, occurred_at=app.clock.now())
    reservation = app.reservations.reserve(resource_id="room:joint", subject_id=created["entity_id"], quantity=2, capacity=10, start_at="2026-09-28T10:00:00+08:00", end_at="2026-09-28T11:00:00+08:00", actor=context.actor_id)
    debit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="debit", reference="demo-debit", actor=context.actor_id)
    credit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="credit", reference="demo-credit", actor=context.actor_id)
    message = app.outbox.enqueue(topic="case.opened", aggregate_id=created["entity_id"], payload={"case_id": created["entity_id"]})
    return {"case": created, "inbox": accepted, "reservation": reservation, "entries": [debit, credit], "balance": app.ledger.balance("demo", currency="CNY"), "message_id": message, "verification": app.verify()}


def clearing_demo(app: CivicFlow) -> dict:
    """横琴-澳门数字货币桥小额公共服务结算的端到端演示。"""
    fx_clerk = AccessContext.system("fx-clerk")
    close_officer = AccessContext.system("close-officer")
    operator = AccessContext.system("bridge-operator")

    corridor = app.clearing.register_corridor(
        operator, corridor_code="HQ-MO-MOPCNH", currency_pay="MOP", currency_receive="CNH",
        window_duration_minutes=60, cutoff_offset_minutes=5)
    app.clearing.add_participant(operator, corridor_id=corridor["corridor_id"],
                                 organization_id="org:hq", authorized_counterparties=["org:mo"])
    app.clearing.add_participant(operator, corridor_id=corridor["corridor_id"],
                                 organization_id="org:mo", authorized_counterparties=["org:hq"])
    app.clearing.set_limit(operator, corridor_id=corridor["corridor_id"], organization_id="org:hq",
                           currency="MOP", amount="1000.00", effective_at="2026-10-01T08:00:00+08:00")
    app.clearing.set_limit(operator, corridor_id=corridor["corridor_id"], organization_id="org:mo",
                           currency="MOP", amount="1000.00", effective_at="2026-10-01T08:00:00+08:00")

    windows = []
    for opens in ("2026-10-01T09:00:00+08:00", "2026-10-01T10:00:00+08:00",
                  "2026-10-01T11:00:00+08:00"):
        windows.append(app.clearing.open_window(fx_clerk, corridor_id=corridor["corridor_id"],
                                                opens_at=opens, fx_rate="0.8900"))

    # 窗口一：三笔指令（两笔横琴付澳门、一笔澳门付横琴），按来源序列推进。
    instructions = [
        ("obl:water-1", "org:hq", "org:mo", "100.00", "2026-10-01T09:10:00+08:00"),
        ("obl:water-2", "org:hq", "org:mo", "40.00", "2026-10-01T09:20:00+08:00"),
        ("obl:transit-1", "org:mo", "org:hq", "30.00", "2026-10-01T09:40:00+08:00"),
        # 09:58 超过 09:55 截止时间：迟到事件只能进入下一可用窗口。
        ("obl:water-3", "org:hq", "org:mo", "25.00", "2026-10-01T09:58:00+08:00"),
    ]
    events = []
    seq = 0
    for index, (obl, payer, payee, amount, at) in enumerate(instructions):
        payload = {"corridor_id": corridor["corridor_id"], "organization_id": payer,
                   "counterparty_id": payee, "currency": "MOP", "amount": amount,
                   "obligation_key": obl}
        events.append(app.bridge.admit_event(operator, source="bridge-hqmo", source_key="water",
                                             sequence=seq, event_kind="instruction",
                                             payload=payload, occurred_at=at)); seq += 1
        events.append(app.bridge.admit_event(operator, source="bridge-hqmo", source_key="water",
                                             sequence=seq, event_kind="accept",
                                             payload={"obligation_key": obl}, occurred_at=at)); seq += 1
        events.append(app.bridge.admit_event(operator, source="bridge-hqmo", source_key="water",
                                             sequence=seq, event_kind="match",
                                             payload={"obligation_key": obl}, occurred_at=at)); seq += 1

    first_batch = app.clearing.list_window_batches(operator, window_id=windows[0]["window_id"])[0]
    # 汇率录入人不能批准关账。
    fx_clerk_close_denied = False
    try:
        app.clearing.approve_window_close(fx_clerk, window_id=windows[0]["window_id"],
                                          expected_fx_rate_id=windows[0]["fx_rate_id"])
    except PermissionDenied:
        fx_clerk_close_denied = True
    app.clearing.approve_window_close(close_officer, window_id=windows[0]["window_id"],
                                      expected_fx_rate_id=windows[0]["fx_rate_id"])
    settled = app.bridge.settle_batch(close_officer, batch_id=first_batch["batch_id"])

    # 已结算批次不能抹账：在窗口三用反向分录调整。
    adjustment = app.bridge.adjust_settled_batch(
        operator, batch_id=first_batch["batch_id"], target_window_id=windows[2]["window_id"],
        reason="水费单价修订，冲销窗口一净额")

    explanation = app.clearing.explain_obligation(operator, "obl:water-3")
    recovery = app.bridge.recover("bridge-hqmo", "water")
    return {
        "corridor": corridor,
        "windows": [{"window_id": w["window_id"], "window_index": w["window_index"],
                     "opens_at": w["opens_at"], "cutoff_at": w["cutoff_at"]} for w in windows],
        "events": [{"sequence": e["sequence"] if "sequence" in e else None,
                    "state": e.get("state"), "window_id": e.get("window_id"),
                    "late": e.get("late")} for e in events],
        "fx_clerk_close_denied": fx_clerk_close_denied,
        "window_one_settlement": {"batch_id": settled["batch_id"], "status": settled["status"],
                                  "detail": settled["settlement_detail"]},
        "adjustment": {"batch_id": adjustment["batch_id"],
                       "reverses": adjustment["reverses_batch_id"],
                       "reversal_entry_ids": adjustment["reversal_entry_ids"]},
        "late_obligation_explanation": {
            "window_index": explanation["window"]["window_index"],
            "late_to_previous_window": explanation["late_to_previous_window"],
            "fx_rate_id": explanation["netting"]["fx_rate_id"],
            "hold": explanation["liquidity_hold"], "limit": explanation["limit"],
            "released_by": explanation["released_by"]},
        "recovery": {"last_sequence": recovery["last_sequence"],
                     "last_confirmed_sequence": recovery["last_confirmed_sequence"],
                     "resumable": len(recovery["resumable"]),
                     "outstanding_holds": len(recovery["outstanding_holds"])},
        "verification": app.verify(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协同事务平台")
    parser.add_argument("--db", default=os.getenv("CIVICFLOW_DB", "civicflow.sqlite3"))
    parser.add_argument("--now", default=None, help="测试或演示使用的固定时间")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    commands.add_parser("clearing-demo")
    commands.add_parser("verify")
    commands.add_parser("list-cases")
    args = parser.parse_args(argv)
    app = CivicFlow.open(Path(args.db), fixed_now=args.now)
    if args.command == "demo": emit(demo(app))
    elif args.command == "clearing-demo": emit(clearing_demo(app))
    elif args.command == "verify": emit(app.verify())
    elif args.command == "list-cases": emit(CaseService(app.repository).list_current(AccessContext.system("cli")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
