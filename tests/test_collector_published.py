"""feedparser *_parsed is UTC. time.mktime must not shift it under TZ=Asia/Seoul."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from jobradar.collector import _extract_published
from jobradar.models import Article, Source
from jobradar.storage import RadarStorage


def test_feedparser_struct_stores_utc_under_asia_seoul(
    monkeypatch, tmp_path: Path
) -> None:
    """2026-10-28 15:00 UTC stays 15:00 UTC when the process zone is Asia/Seoul."""
    monkeypatch.setenv("TZ", "Asia/Seoul")
    time.tzset()
    try:
        published_parsed = time.gmtime(
            datetime(2026, 10, 28, 15, 0, tzinfo=UTC).timestamp()
        )
        shifted = datetime.fromtimestamp(time.mktime(published_parsed), tz=UTC)
        assert shifted == datetime(2026, 10, 28, 6, 0, tzinfo=UTC)

        source = Source(name="Example", type="rss", url="https://example.com/feed.xml")
        published, raw = _extract_published(
            {"published_parsed": published_parsed}, source
        )
        assert raw is None
        assert published == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
        updated, updated_raw = _extract_published(
            {"updated_parsed": published_parsed}, source
        )
        assert updated_raw is None
        assert updated == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)

        storage = RadarStorage(tmp_path / "radar.duckdb")
        try:
            storage.upsert_articles(
                [
                    Article(
                        title="백엔드",
                        link="https://example.com/jobs/1",
                        summary="",
                        published=published,
                        source=source.name,
                        category="job",
                        published_raw=raw,
                    )
                ]
            )
            row = storage.conn.execute("SELECT published FROM articles").fetchone()
            assert row is not None
            stored = row[0]
            assert isinstance(stored, datetime)
            assert stored.tzinfo is None
            assert (stored.year, stored.month, stored.day, stored.hour, stored.minute) == (
                2026,
                10,
                28,
                15,
                0,
            )
            loaded = storage.recent_articles("job", days=3650)
            assert loaded[0].published == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
        finally:
            storage.close()
    finally:
        monkeypatch.undo()
        time.tzset()


def test_offset_string_keeps_its_offset() -> None:
    source = Source(name="HR Dive", type="rss", url="https://www.hrdive.com/feeds/news")
    published, raw = _extract_published(
        {"published": "Thu, 29 Oct 2026 09:00:00 +0900"}, source
    )
    assert raw == "Thu, 29 Oct 2026 09:00:00 +0900"
    assert published is not None
    assert published.utcoffset() == timedelta(hours=9)
    assert published.astimezone(UTC) == datetime(2026, 10, 29, 0, 0, tzinfo=UTC)


def test_zoneless_string_uses_source_or_host_default() -> None:
    korean = Source(name="한국경제", type="rss", url="https://www.hankyung.co.kr/feed")
    published, raw = _extract_published({"published": "2026-10-29 18:00:00"}, korean)
    assert raw == "2026-10-29 18:00:00"
    assert published == datetime(2026, 10, 29, 18, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    other = Source(name="Example", type="rss", url="https://example.com/feed.xml")
    utc_published, _ = _extract_published({"published": "2026-10-29 18:00:00"}, other)
    assert utc_published == datetime(2026, 10, 29, 18, 0, tzinfo=UTC)

    configured = Source(
        name="Example",
        type="rss",
        url="https://example.com/feed.xml",
        config={"default_timezone": "Asia/Seoul"},
    )
    seoul_published, _ = _extract_published(
        {"published": "2026-10-29 18:00:00"}, configured
    )
    assert seoul_published == datetime(2026, 10, 29, 18, 0, tzinfo=ZoneInfo("Asia/Seoul"))
