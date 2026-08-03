from storage.execution_store import ExecutionStore
from api.bybit_ws_private import BybitWsPrivate


def make_client(store):
    client = BybitWsPrivate.__new__(BybitWsPrivate)
    client.api_key = ""
    client.api_secret = ""
    import threading
    client._pos_lock = threading.Lock()
    client._positions = {}
    client._pos_cond = threading.Condition(client._pos_lock)
    client._ord_lock = threading.Lock()
    client._orders = {}
    client._ord_cond = threading.Condition(client._ord_lock)
    client._execution_store = store
    return client


def test_order_push_is_persisted(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    client = make_client(store)
    client._dispatch({
        "topic": "order",
        "creationTime": 123,
        "data": [{
            "orderLinkId": "link-1", "orderId": "order-1",
            "orderStatus": "Filled", "avgPrice": "2.5",
            "cumExecQty": "4",
        }],
    })
    assert store.get_order("link-1")["status"] == "Filled"


def test_execution_push_is_deduplicated_and_persisted(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    client = make_client(store)
    msg = {
        "topic": "execution",
        "creationTime": 123,
        "data": [{
            "execId": "exec-1", "orderLinkId": "link-1",
            "orderId": "order-1", "symbol": "AAAUSDT", "side": "Sell",
            "execPrice": "2.5", "execQty": "4", "execFee": "0.01",
            "feeCurrency": "USDT", "execTime": "456",
        }],
    }
    client._dispatch(msg)
    client._dispatch(msg)
    assert store.count_fills("link-1") == 1


def test_position_push_is_persisted(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    client = make_client(store)
    client._dispatch({
        "topic": "position",
        "creationTime": 123,
        "data": [{
            "symbol": "AAAUSDT", "positionIdx": 2, "side": "Sell",
            "size": "4", "avgPrice": "2.5", "trailingStop": "0.02",
        }],
    })
    pos = store.get_position("bybit", "AAAUSDT", 2)
    assert pos["size"] == 4
    assert pos["side"] == "Sell"
