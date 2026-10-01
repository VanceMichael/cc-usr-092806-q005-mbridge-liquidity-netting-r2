"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .application import CivicFlow
from .cases import CaseService
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
    """横琴—澳门数字货币桥：登记走廊、冻结两笔义务到窗口、净额放行并结算。"""
    from datetime import timedelta
    from civicflow.timeutil import parse_instant

    operator = AccessContext.system("demo-operator")
    rate_clerk = AccessContext(actor_id="demo-rate-clerk", permissions=frozenset({"write:rate", "read:clearing"}))
    approver = AccessContext(actor_id="demo-approver", permissions=frozenset({"approve:clearing", "read:clearing", "bridge:clearing", "write:clearing"}))

    base = parse_instant(app.clock.now())
    opens = (base + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    closes = (base + timedelta(minutes=31)).isoformat().replace("+00:00", "Z")
    after = (base + timedelta(minutes=32)).isoformat().replace("+00:00", "Z")
    t1 = (base + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    t2 = (base + timedelta(minutes=6)).isoformat().replace("+00:00", "Z")

    app.clearing.register_corridor(operator, corridor_id="corridor:hqmz", name="横琴-澳门数字货币桥", base_currency="CNY", quote_currency="MOP")
    app.clearing.add_participant(operator, corridor_id="corridor:hqmz", org_id="org:hengqin", authorized_counterparties=["org:macau"])
    app.clearing.add_participant(operator, corridor_id="corridor:hqmz", org_id="org:macau", authorized_counterparties=["org:hengqin"])
    app.clearing.set_limit(operator, corridor_id="corridor:hqmz", org_id="org:hengqin", currency="CNY", amount="100000.00")
    app.clearing.set_limit(operator, corridor_id="corridor:hqmz", org_id="org:macau", currency="CNY", amount="100000.00")
    app.clearing.fund_liquidity(operator, corridor_id="corridor:hqmz", org_id="org:hengqin", currency="CNY", amount="50000.00", reference="fund-hq-1")
    app.clearing.fund_liquidity(operator, corridor_id="corridor:hqmz", org_id="org:macau", currency="CNY", amount="50000.00", reference="fund-mo-1")

    window = app.clearing.open_window(operator, corridor_id="corridor:hqmz", opens_at=opens, closes_at=closes)
    rate = app.clearing.enter_rate(rate_clerk, window_id=window["window_id"], rate="1.0812", reason="窗口开盘快照")

    first = app.clearing.bridge_event(approver, source="bridge-hqmz", source_key="obl-001", sequence=0, event_type="received",
        payload={"corridor_id": "corridor:hqmz", "obligation_ref": "obl-001", "payer_org": "org:hengqin", "payee_org": "org:macau", "currency": "CNY", "amount": "120.00"}, occurred_at=t1)
    second = app.clearing.bridge_event(approver, source="bridge-hqmz", source_key="obl-002", sequence=0, event_type="received",
        payload={"corridor_id": "corridor:hqmz", "obligation_ref": "obl-002", "payer_org": "org:macau", "payee_org": "org:hengqin", "currency": "CNY", "amount": "80.00"}, occurred_at=t2)
    for key in ("obl-001", "obl-002"):
        app.clearing.bridge_event(approver, source="bridge-hqmz", source_key=key, sequence=1, event_type="matched")

    later = CivicFlow.open(Path(app.database.path), fixed_now=after)
    batch_id = first["batch_id"]
    closed = later.clearing.close_batch(approver, batch_id=batch_id)
    for key in ("obl-001", "obl-002"):
        later.clearing.bridge_event(approver, source="bridge-hqmz", source_key=key, sequence=2, event_type="settled")
    explanation = later.clearing.explain(approver, obligation_ref="obl-001")
    checkpoint = later.clearing.checkpoint(approver, source="bridge-hqmz")
    return {"window": window, "rate": rate, "frozen": [first, second], "closed_batch": closed, "explanation": explanation, "checkpoint": checkpoint, "verification": later.verify()}


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
