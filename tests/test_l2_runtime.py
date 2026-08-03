from storage import runtime


class DummyStore:
    pass


def test_l2_runtime_initializes_once(monkeypatch):
    runtime.reset_for_tests()
    monkeypatch.setattr(runtime, "get_store", lambda: DummyStore())
    first = runtime.init_l2(lambda symbol, limit: {})
    second = runtime.init_l2(lambda symbol, limit: {"different": True})
    assert first is second
    assert runtime.get_l2_worker() is first
    runtime.reset_for_tests()
