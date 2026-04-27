from __future__ import annotations

import sys
import types
from importlib import import_module


def test_collect_browser_sources_forwards_source_config(monkeypatch) -> None:
    structlog_stub = types.ModuleType("structlog")
    structlog_stub.get_logger = lambda *a, **k: types.SimpleNamespace(
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        info=lambda *a, **k: None,
    )
    sys.modules.setdefault("structlog", structlog_stub)

    module = import_module("jobradar.browser_collector")
    source = import_module("jobradar.models").Source(
        name="Kakao Careers",
        type="browser",
        url="https://careers.kakao.com/jobs?company=ALL&part=TECHNOLOGY",
        config={"wait_for": "body"},
    )
    captured: dict[str, object] = {}

    def fake_collect(*, sources, category, timeout, health_db_path):
        captured["sources"] = sources
        captured["category"] = category
        return [], []

    monkeypatch.setattr(module, "_BROWSER_COLLECTION_AVAILABLE", True)
    monkeypatch.setattr(module, "_core_collect", fake_collect)

    articles, errors = module.collect_browser_sources([source], "job")

    assert articles == []
    assert errors == []
    assert captured["category"] == "job"
    assert captured["sources"] == [
        {
            "name": "Kakao Careers",
            "type": "browser",
            "url": "https://careers.kakao.com/jobs?company=ALL&part=TECHNOLOGY",
            "config": {"wait_for": "body"},
        }
    ]
