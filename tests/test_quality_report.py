from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from jobradar.models import Article, CategoryConfig, Source
from jobradar.quality_report import build_quality_report, write_quality_report


def _article(
    source: str,
    *,
    published: datetime,
    matched_entities: dict[str, list[str]] | None = None,
) -> Article:
    return Article(
        title=f"{source} article",
        link=f"https://example.com/{source}",
        summary="",
        published=published,
        source=source,
        category="job",
        matched_entities=matched_entities or {},
    )


def test_build_quality_report_tracks_job_source_statuses() -> None:
    generated_at = datetime(2026, 4, 13, tzinfo=UTC)
    category = CategoryConfig(
        category_name="job",
        display_name="Job",
        sources=[
            Source(
                name="Kakao Careers",
                type="browser",
                url="https://careers.kakao.com/jobs",
                content_type="job_posting",
                producer_role="employer",
                info_purpose=["official_job_posting"],
            ),
            Source(
                name="HR Dive",
                type="rss",
                url="https://www.hrdive.com/feeds/news",
                info_purpose=["labor_market", "employment_compliance"],
            ),
            Source(name="한국경제", type="rss", url="https://example.com/economy"),
        ],
        entities=[],
    )
    articles = [
        _article(
            "Kakao Careers",
            published=generated_at - timedelta(days=1),
            matched_entities={
                "Company": ["카카오"],
                "SourceSignal": ["official_job_posting"],
            },
        ),
        _article(
            "HR Dive",
            published=generated_at - timedelta(days=2),
            matched_entities={"SourceSignal": ["labor_market"]},
        ),
    ]

    report = build_quality_report(
        category=category,
        articles=articles,
        quality_config={
            "data_quality": {
                "quality_outputs": {
                    "tracked_event_models": [
                        "official_job_posting",
                        "labor_market_signal",
                    ]
                },
                "freshness_sla": {
                    "official_job_posting_days": 3,
                    "labor_market_signal_days": 7,
                },
                "event_models": {
                    "official_job_posting": {
                        "required_fields": ["company_id", "job_title", "location", "source_url"]
                    },
                    "labor_market_signal": {
                        "required_fields": ["topic", "source_url", "source_name"]
                    },
                },
            }
        },
        generated_at=generated_at,
    )

    sources = {row["source"]: row for row in report["sources"]}
    assert report["summary"]["fresh_sources"] == 2
    assert report["summary"]["official_job_posting_events"] == 1
    assert report["summary"]["labor_market_signal_events"] == 1
    assert report["summary"]["job_signal_event_count"] == 2
    assert report["summary"]["company_present_count"] == 1
    assert report["summary"]["event_required_field_gap_count"] >= 1
    assert report["summary"]["daily_review_item_count"] >= 1
    assert report["events"][0]["canonical_key"]
    assert "required_field_gaps" in report["events"][0]
    assert sources["Kakao Careers"]["status"] == "fresh"
    assert sources["HR Dive"]["status"] == "fresh"
    assert sources["한국경제"]["status"] == "not_tracked"


def test_community_reddit_source_is_context_not_job_board_tracking() -> None:
    generated_at = datetime(2026, 4, 13, tzinfo=UTC)
    category = CategoryConfig(
        category_name="job",
        display_name="Job",
        sources=[
            Source(
                name="r/jobs",
                type="rss",
                url="https://www.reddit.com/r/jobs/.rss",
                content_type="community",
                collection_tier="C3_reddit",
            ),
        ],
        entities=[],
    )

    report = build_quality_report(
        category=category,
        articles=[],
        quality_config={
            "data_quality": {
                "quality_outputs": {"tracked_event_models": ["job_board_posting"]},
                "freshness_sla": {"job_board_posting_days": 3},
            }
        },
        generated_at=generated_at,
    )

    row = report["sources"][0]
    assert row["tracked"] is False
    assert row["status"] == "not_tracked"
    assert report["summary"]["tracked_sources"] == 0


def test_disabled_tracked_source_is_skipped_not_counted_as_active_tracked() -> None:
    generated_at = datetime(2026, 4, 13, tzinfo=UTC)
    category = CategoryConfig(
        category_name="job",
        display_name="Job",
        sources=[
            Source(
                name="Dormant Feed",
                type="rss",
                url="https://example.com/feed.xml",
                enabled=False,
                config={
                    "event_model": "labor_market_signal",
                    "skip_reason": "Endpoint is dormant.",
                    "reenable_gate": "Feed publishes current posts again.",
                },
            ),
        ],
        entities=[],
    )

    report = build_quality_report(
        category=category,
        articles=[
            _article(
                "Dormant Feed",
                published=generated_at,
                matched_entities={"SourceSignal": ["labor_market"]},
            )
        ],
        quality_config={
            "data_quality": {
                "quality_outputs": {"tracked_event_models": ["labor_market_signal"]},
                "freshness_sla": {"labor_market_signal_days": 7},
            }
        },
        generated_at=generated_at,
    )

    row = report["sources"][0]
    assert row["tracked"] is False
    assert row["status"] == "skipped_disabled"
    assert row["skip_reason"] == "Endpoint is dormant."
    assert report["summary"]["tracked_sources"] == 0
    assert report["summary"]["skipped_disabled_sources"] == 1
    assert report["summary"]["labor_market_signal_events"] == 0


def test_write_quality_report_writes_latest_and_dated_json(tmp_path) -> None:
    report = {
        "category": "job",
        "generated_at": "2026-04-13T00:00:00+00:00",
        "summary": {},
        "sources": [],
    }

    paths = write_quality_report(report, output_dir=tmp_path, category_name="job")

    assert paths["latest"] == tmp_path / "job_quality.json"
    assert paths["dated"] == tmp_path / "job_20260413_quality.json"
    assert json.loads(paths["latest"].read_text(encoding="utf-8"))["category"] == "job"
