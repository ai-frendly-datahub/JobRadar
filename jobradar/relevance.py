from __future__ import annotations

from collections.abc import Iterable

from .models import Article, Source


CONTEXT_PURPOSES = {
    "employment_compliance",
    "enterprise_hiring",
    "labor_market",
    "official_hiring_signal",
    "official_job_posting",
    "recruiting_ops",
    "salary_signal",
    "skill_trend",
    "talent_strategy",
    "tech_hiring",
    "workplace",
}
STRONG_ENTITY_NAMES = {
    "EmploymentTrend",
    "EmploymentType",
    "JobGeneral",
    "JobTitle",
    "TechStack",
}


def apply_source_context_entities(
    articles: Iterable[Article],
    sources: Iterable[Source],
) -> list[Article]:
    """Attach source taxonomy tags so curated job sources remain classified."""
    source_map = {source.name: source for source in sources if source.enabled}
    classified: list[Article] = []
    for article in articles:
        source = source_map.get(article.source)
        if source is not None:
            tags = _source_context_tags(source)
            if tags:
                existing = article.matched_entities.get("SourceSignal", [])
                merged = sorted({str(value) for value in existing} | set(tags))
                article.matched_entities["SourceSignal"] = merged
        classified.append(article)
    return classified


def filter_relevant_articles(
    articles: Iterable[Article],
    sources: Iterable[Source],
) -> list[Article]:
    """Keep only articles from configured sources that carry job-domain evidence."""
    source_map = {source.name: source for source in sources if source.enabled}
    filtered: list[Article] = []
    for article in articles:
        source = source_map.get(article.source)
        if source is None:
            continue
        if _source_context_tags(source) or _has_strong_job_signal(article):
            filtered.append(article)
    return filtered


def _has_strong_job_signal(article: Article) -> bool:
    return any(entity_name in STRONG_ENTITY_NAMES for entity_name in article.matched_entities)


def _source_context_tags(source: Source) -> list[str]:
    tags = {tag for tag in source.info_purpose if tag in CONTEXT_PURPOSES}
    if source.content_type == "job_posting":
        tags.add("official_job_posting")
    if source.producer_role == "employer":
        tags.add("employer")
    if source.producer_role == "vendor_research":
        tags.add("vendor_research")
    return sorted(tags)
