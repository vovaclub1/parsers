import json

from storage.execution_store import ExecutionStore
from research.legacy_gate_import import import_gate_intents


def test_import_gate_intents_is_idempotent(tmp_path):
    p=tmp_path/"signal_events.jsonl"
    p.write_text(json.dumps({"k":"open","ts":1700000000,"coin":"A","src":"TOA","venue":"GATE","outcome":"opened","amount":12})+"\n")
    s=ExecutionStore(tmp_path/"e.db")
    assert import_gate_intents(s, [(p,"listing")]) == 1
    assert import_gate_intents(s, [(p,"listing")]) == 0
    row=s.list_intents(venue="gate",symbol="A_USDT")[0]
    assert row["signal_id"].startswith("listing:A:")
    assert row["side"] == "Sell"
