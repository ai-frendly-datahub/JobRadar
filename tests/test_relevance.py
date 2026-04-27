from __future__ import annotations

from jobradar.models import Article, Source
from jobradar.relevance import apply_source_context_entities, filter_relevant_articles


def _article(
    source: str,
    *,
    matched_entities: dict[str, list[str]] | None = None,
) -> Article:
    return Article(
        title="sample",
        link=f"https://example.com/{source}",
        summary="",
        published=None,
        source=source,
        category="job",
        matched_entities=matched_entities or {},
    )


def test_apply_source_context_entities_adds_curated_source_signal() -> None:
    source = Source(
        name="HR Dive",
        type="rss",
        url="https://www.hrdive.com/feeds/news",
        info_purpose=["labor_market", "employment_compliance"],
    )

    [article] = apply_source_context_entities([_article("HR Dive")], [source])

    assert article.matched_entities["SourceSignal"] == [
        "employment_compliance",
        "labor_market",
    ]


def test_filter_relevant_articles_drops_broad_company_only_noise() -> None:
    sources = [
        Source(name="한국경제", type="rss", url="https://example.com/economy"),
        Source(
            name="Kakao Careers",
            type="browser",
            url="https://careers.kakao.com/jobs",
            content_type="job_posting",
            producer_role="employer",
        ),
    ]
    articles = [
        _article("한국경제", matched_entities={"Company": ["쿠팡"]}),
        _article("한국경제", matched_entities={"EmploymentTrend": ["채용"]}),
        _article("Kakao Careers"),
        _article("JobPost", matched_entities={"EmploymentTrend": ["채용"]}),
    ]

    filtered = filter_relevant_articles(articles, sources)

    assert [article.source for article in filtered] == ["한국경제", "Kakao Careers"]
