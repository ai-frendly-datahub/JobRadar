from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import Article, CategoryConfig, Source


TRACKED_EVENT_MODEL_ORDER = [
    "official_job_posting",
    "job_board_posting",
    "labor_market_signal",
    "skill_demand",
    "salary_signal",
]
TRACKED_EVENT_MODELS = set(TRACKED_EVENT_MODEL_ORDER)
SUMMARY_LABELS = [
    "Company ID",
    "Company",
    "Job title",
    "Role",
    "Location",
    "Region",
    "Salary range",
    "Salary",
    "Skill",
    "Demand count",
    "Topic",
]


def build_quality_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    errors: Iterable[str] | None = None,
    quality_config: Mapping[str, object] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _as_utc(generated_at or datetime.now(UTC))
    article_rows = list(articles)
    error_rows = [str(error) for error in (errors or [])]
    quality = _dict(quality_config or {}, "data_quality")
    event_model_config = _dict(quality, "event_models")
    freshness_sla = _dict(quality, "freshness_sla")
    tracked_models = _tracked_event_models(quality)

    events = _build_events(
        articles=article_rows,
        sources=category.sources,
        tracked_models=tracked_models,
        event_model_config=event_model_config,
    )
    source_rows = [
        _build_source_row(
            source=source,
            articles=article_rows,
            events=events,
            errors=error_rows,
            freshness_sla=freshness_sla,
            tracked_models=tracked_models,
            generated_at=generated,
        )
        for source in category.sources
    ]

    status_counts = Counter(str(row["status"]) for row in source_rows)
    event_counts = Counter(str(row["event_model"]) for row in events)
    summary: dict[str, Any] = {
        "total_sources": len(source_rows),
        "enabled_sources": sum(1 for row in source_rows if row["enabled"]),
        "tracked_sources": sum(1 for row in source_rows if row["tracked"]),
        "fresh_sources": status_counts.get("fresh", 0),
        "stale_sources": status_counts.get("stale", 0),
        "missing_sources": status_counts.get("missing", 0),
        "missing_event_sources": status_counts.get("missing_event", 0),
        "unknown_event_date_sources": status_counts.get("unknown_event_date", 0),
        "not_tracked_sources": status_counts.get("not_tracked", 0),
        "skipped_disabled_sources": status_counts.get("skipped_disabled", 0),
        "collection_error_count": len(error_rows),
    }
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        summary[f"{event_model}_events"] = event_counts.get(event_model, 0)
    summary.update(
        _event_quality_summary(
            events=events,
            source_rows=source_rows,
            quality_config=quality_config or {},
            tracked_models=tracked_models,
        )
    )
    daily_review_items = _daily_review_items(
        events=events,
        source_rows=source_rows,
        quality_config=quality_config or {},
        tracked_models=tracked_models,
    )
    summary["daily_review_item_count"] = len(daily_review_items)

    return {
        "category": category.category_name,
        "generated_at": generated.isoformat(),
        "scope_note": (
            "Job quality rows separate official employer boards, job-board postings, "
            "labor-market signals, skill demand, and salary signals from broad HR or "
            "community context. Proxy rows stay review items until company, role, "
            "location, salary, or skill identifiers are collected."
        ),
        "summary": summary,
        "sources": source_rows,
        "events": events,
        "daily_review_items": daily_review_items,
        "source_backlog": (quality_config or {}).get("source_backlog", {}),
        "errors": error_rows,
    }


def write_quality_report(
    report: Mapping[str, object],
    *,
    output_dir: Path,
    category_name: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _parse_datetime(str(report.get("generated_at") or "")) or datetime.now(UTC)
    date_stamp = _as_utc(generated_at).strftime("%Y%m%d")
    latest_path = output_dir / f"{category_name}_quality.json"
    dated_path = output_dir / f"{category_name}_{date_stamp}_quality.json"
    encoded = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    latest_path.write_text(encoded + "\n", encoding="utf-8")
    dated_path.write_text(encoded + "\n", encoding="utf-8")
    return {"latest": latest_path, "dated": dated_path}


def _build_events(
    *,
    articles: list[Article],
    sources: list[Source],
    tracked_models: list[str],
    event_model_config: Mapping[str, object],
) -> list[dict[str, Any]]:
    source_map = {source.name: source for source in sources}
    rows: list[dict[str, Any]] = []
    for article in articles:
        source = source_map.get(_article_source(article))
        if source is None or not source.enabled:
            continue
        event_model = _article_event_model(article, source, tracked_models)
        if event_model not in tracked_models:
            continue
        event_at = _event_datetime(article)
        rows.append(_event_row(article, source, event_model, event_at, event_model_config))
    return rows


def _event_row(
    article: Article,
    source: Source,
    event_model: str,
    event_at: datetime | None,
    event_model_config: Mapping[str, object],
) -> dict[str, Any]:
    company_name = _company_name(article, source)
    job_title = _job_title(article)
    location = _location(article, source)
    skills = _skills(article)
    row: dict[str, Any] = {
        "source": source.name,
        "source_type": source.type,
        "trust_tier": source.trust_tier,
        "content_type": source.content_type,
        "collection_tier": source.collection_tier,
        "producer_role": source.producer_role,
        "info_purpose": source.info_purpose,
        "event_model": event_model,
        "title": _article_title(article),
        "url": _article_link(article),
        "source_url": _article_link(article) or source.url,
        "event_at": event_at.isoformat() if event_at else None,
        "matched_entities": _article_entities(article),
        "signal_basis": _signal_basis(article, source, event_model),
        "company_id": _company_id(article, source, company_name),
        "company_name": company_name,
        "job_title": job_title,
        "role": _role(article, job_title),
        "location": location,
        "region": location,
        "skills": skills,
        "skill": _first(skills),
        "employment_type": _matches(article, "EmploymentType"),
        "topic": _topic(article),
        "salary_range": _salary_range(article),
        "demand_count": _demand_count(article),
        "source_signal": _matches(article, "SourceSignal"),
    }
    canonical_key, canonical_key_status = _canonical_key(row)
    row["canonical_key"] = canonical_key
    row["canonical_key_status"] = canonical_key_status
    row["event_key"] = _event_key(row, event_at)
    row["required_field_proxy"] = _required_field_proxy(row, event_model, event_model_config)
    row["required_field_gaps"] = _required_field_gaps(row, event_model, event_model_config)
    return row


def _build_source_row(
    *,
    source: Source,
    articles: list[Article],
    events: list[dict[str, Any]],
    errors: list[str],
    freshness_sla: Mapping[str, object],
    tracked_models: list[str],
    generated_at: datetime,
) -> dict[str, Any]:
    source_articles = [article for article in articles if _article_source(article) == source.name]
    event_model = _source_event_model(source, tracked_models)
    source_events = [
        row
        for row in events
        if row["source"] == source.name and row["event_model"] == event_model
    ]
    latest_event = _latest_event(source_events)
    latest_event_at = (
        _parse_datetime(str(latest_event.get("event_at") or "")) if latest_event else None
    )
    sla_days = _source_sla_days(source, event_model, freshness_sla)
    age_days = _age_days(generated_at, latest_event_at) if latest_event_at else None
    source_errors = [
        error
        for error in errors
        if error.startswith(f"{source.name}:") or error.startswith(f"[{source.name}]")
    ]
    tracked = source.enabled and event_model in tracked_models
    status = _source_status(
        source=source,
        tracked=tracked,
        article_count=len(source_articles),
        event_count=len(source_events),
        latest_event_at=latest_event_at,
        sla_days=sla_days,
        age_days=age_days,
    )

    return {
        "source": source.name,
        "source_type": source.type,
        "enabled": source.enabled,
        "trust_tier": source.trust_tier,
        "content_type": source.content_type,
        "collection_tier": source.collection_tier,
        "producer_role": source.producer_role,
        "info_purpose": source.info_purpose,
        "tracked": tracked,
        "event_model": event_model,
        "freshness_sla_days": sla_days,
        "status": status,
        "article_count": len(source_articles),
        "event_count": len(source_events),
        "latest_event_at": latest_event_at.isoformat() if latest_event_at else None,
        "age_days": round(age_days, 2) if age_days is not None else None,
        "latest_title": str(latest_event.get("title", "")) if latest_event else "",
        "latest_url": str(latest_event.get("url", "")) if latest_event else "",
        "latest_canonical_key": str(latest_event.get("canonical_key", "")) if latest_event else "",
        "latest_required_field_gaps": (
            latest_event.get("required_field_gaps", []) if latest_event else []
        ),
        "skip_reason": source.config.get("skip_reason"),
        "reenable_gate": source.config.get("reenable_gate"),
        "errors": source_errors,
    }


def _tracked_event_models(quality: Mapping[str, object]) -> list[str]:
    outputs = _dict(quality, "quality_outputs")
    raw = outputs.get("tracked_event_models")
    if isinstance(raw, list):
        values = [str(item).strip() for item in raw if str(item).strip()]
        values = [value for value in values if value in TRACKED_EVENT_MODELS]
        if values:
            return values
    return list(TRACKED_EVENT_MODEL_ORDER)


def _source_event_model(source: Source, tracked_models: list[str]) -> str:
    raw = source.config.get("event_model")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()

    text = _source_context(source)
    if source.content_type == "job_posting" or source.producer_role == "employer":
        return "official_job_posting" if "official_job_posting" in tracked_models else ""
    if "salary_signal" in text or "salary" in text or "compensation" in text:
        return "salary_signal" if "salary_signal" in tracked_models else ""
    if "skill_trend" in text or "skill" in text:
        return "skill_demand" if "skill_demand" in tracked_models else ""
    if _has_any(
        text,
        [
            "labor_market",
            "official_hiring_signal",
            "recruiting_ops",
            "talent_strategy",
            "workplace",
            "employment_compliance",
        ],
    ):
        return "labor_market_signal" if "labor_market_signal" in tracked_models else ""
    return ""


def _article_event_model(article: Article, source: Source, tracked_models: list[str]) -> str:
    configured = _source_event_model(source, tracked_models)
    if configured in {"official_job_posting", "job_board_posting"}:
        return configured
    if configured and _has_article_evidence(article, configured):
        return configured

    text = _article_text(article)
    if _salary_range(article) and "salary_signal" in tracked_models:
        return "salary_signal"
    if _skills(article) and _has_any(text, ["skill", "demand", "hiring", "job", "engineer"]):
        return "skill_demand" if "skill_demand" in tracked_models else ""
    if _company_name(article, source) and _job_title(article):
        return "job_board_posting" if "job_board_posting" in tracked_models else ""
    if _topic(article) or _matches(article, "EmploymentTrend"):
        return "labor_market_signal" if "labor_market_signal" in tracked_models else ""
    return configured if configured in tracked_models else ""


def _has_article_evidence(article: Article, event_model: str) -> bool:
    text = _article_text(article)
    if event_model == "salary_signal":
        return bool(_salary_range(article)) or _has_any(text, ["salary", "compensation", "pay"])
    if event_model == "skill_demand":
        return bool(_skills(article))
    if event_model == "labor_market_signal":
        return bool(_topic(article)) or _has_any(text, ["labor market", "hiring", "layoff"])
    return True


def _source_sla_days(
    source: Source,
    event_model: str,
    freshness_sla: Mapping[str, object],
) -> float | None:
    raw_source_sla = source.config.get("freshness_sla_days")
    parsed_source_sla = _as_float(raw_source_sla)
    if parsed_source_sla is not None:
        return parsed_source_sla
    days = _as_float(freshness_sla.get(f"{event_model}_days"))
    if days is not None:
        return days
    hours = _as_float(freshness_sla.get(f"{event_model}_hours"))
    if hours is not None:
        return hours / 24.0
    return None


def _source_status(
    *,
    source: Source,
    tracked: bool,
    article_count: int,
    event_count: int,
    latest_event_at: datetime | None,
    sla_days: float | None,
    age_days: float | None,
) -> str:
    if not source.enabled:
        return "skipped_disabled"
    if not tracked:
        return "not_tracked"
    if article_count == 0:
        return "missing"
    if event_count == 0:
        return "missing_event"
    if latest_event_at is None or age_days is None:
        return "unknown_event_date"
    if sla_days is not None and age_days > sla_days:
        return "stale"
    return "fresh"


def _event_quality_summary(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_models: list[str],
) -> dict[str, int]:
    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    return {
        "job_signal_event_count": sum(event_counts.get(model, 0) for model in tracked_models),
        "official_or_operational_event_count": sum(
            1
            for row in events
            if str(row.get("trust_tier") or "").startswith("T1_")
            or str(row.get("producer_role") or "").lower() == "employer"
            or str(row.get("content_type") or "").lower() == "job_posting"
        ),
        "community_proxy_event_count": sum(
            1 for row in events if str(row.get("content_type") or "").lower() == "community"
        ),
        "complete_canonical_key_count": sum(
            1 for row in events if row.get("canonical_key_status") == "complete"
        ),
        "proxy_canonical_key_count": sum(
            1 for row in events if str(row.get("canonical_key_status") or "").endswith("_proxy")
        ),
        "missing_canonical_key_count": sum(1 for row in events if not row.get("canonical_key")),
        "company_present_count": sum(1 for row in events if row.get("company_name")),
        "job_title_present_count": sum(1 for row in events if row.get("job_title")),
        "location_present_count": sum(1 for row in events if row.get("location")),
        "salary_range_present_count": sum(1 for row in events if row.get("salary_range")),
        "skill_present_count": sum(1 for row in events if row.get("skill") or row.get("skills")),
        "demand_count_present_count": sum(
            1 for row in events if row.get("demand_count") is not None
        ),
        "event_required_field_gap_count": sum(
            len(row.get("required_field_gaps") or []) for row in events
        ),
        "tracked_source_gap_count": sum(
            1
            for row in source_rows
            if row.get("tracked")
            and row.get("status") in {"missing", "missing_event", "unknown_event_date", "stale"}
        ),
        "missing_event_model_count": sum(
            1 for model in tracked_models if event_counts.get(model, 0) == 0
        ),
        "source_backlog_candidate_count": len(_source_backlog_items(quality_config)),
    }


def _daily_review_items(
    *,
    events: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    quality_config: Mapping[str, object],
    tracked_models: list[str],
) -> list[dict[str, Any]]:
    review: list[dict[str, Any]] = []
    for row in events:
        gaps = [str(value) for value in row.get("required_field_gaps") or []]
        if gaps:
            review.append(
                {
                    "reason": "missing_required_fields",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key": row.get("canonical_key"),
                    "required_field_gaps": gaps,
                }
            )
        if not row.get("canonical_key"):
            review.append(
                {
                    "reason": "missing_canonical_key",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "event_key": row.get("event_key"),
                }
            )
        if str(row.get("canonical_key_status") or "").endswith("_proxy"):
            review.append(
                {
                    "reason": "proxy_canonical_key",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "canonical_key": row.get("canonical_key"),
                    "canonical_key_status": row.get("canonical_key_status"),
                }
            )
        if str(row.get("content_type") or "").lower() == "community":
            review.append(
                {
                    "reason": "community_proxy_source",
                    "event_model": row.get("event_model"),
                    "source": row.get("source"),
                    "title": row.get("title"),
                    "signal_basis": row.get("signal_basis"),
                }
            )

    for source in source_rows:
        if not source.get("tracked"):
            continue
        if source.get("status") in {"missing", "missing_event", "unknown_event_date", "stale"}:
            review.append(
                {
                    "reason": f"source_{source.get('status')}",
                    "source": source.get("source"),
                    "event_model": source.get("event_model"),
                    "age_days": source.get("age_days"),
                    "latest_title": source.get("latest_title"),
                }
            )

    event_counts = Counter(str(row.get("event_model") or "") for row in events)
    for event_model in TRACKED_EVENT_MODEL_ORDER:
        if event_model in tracked_models and event_counts.get(event_model, 0) == 0:
            review.append({"reason": "missing_event_model", "event_model": event_model})

    for item in _source_backlog_items(quality_config):
        review.append(
            {
                "reason": "source_backlog_pending",
                "source": item.get("name") or item.get("id"),
                "signal_type": item.get("signal_type"),
                "activation_gate": item.get("activation_gate"),
            }
        )
    return review[:50]


def _source_backlog_items(quality_config: Mapping[str, object]) -> list[Mapping[str, object]]:
    backlog = _dict(quality_config, "source_backlog")
    candidates = backlog.get("operational_candidates")
    if not isinstance(candidates, list):
        return []
    return [item for item in candidates if isinstance(item, Mapping)]


def _required_field_proxy(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> dict[str, bool]:
    event_config = _dict(event_model_config, event_model)
    raw_fields = event_config.get("required_fields")
    if not isinstance(raw_fields, list):
        return {}
    return {str(field): _field_present(row, str(field)) for field in raw_fields if str(field).strip()}


def _required_field_gaps(
    row: Mapping[str, Any],
    event_model: str,
    event_model_config: Mapping[str, object],
) -> list[str]:
    return [
        field
        for field, present in _required_field_proxy(row, event_model, event_model_config).items()
        if not present
    ]


def _field_present(row: Mapping[str, Any], field: str) -> bool:
    aliases = {
        "company_id": ("company_id", "company_name"),
        "company_name": ("company_name",),
        "job_title": ("job_title", "role"),
        "location": ("location", "region"),
        "source_url": ("source_url", "url"),
        "source_name": ("source",),
        "topic": ("topic",),
        "role": ("role", "job_title"),
        "region": ("region", "location"),
        "salary_range": ("salary_range",),
        "skill": ("skill", "skills"),
        "demand_count": ("demand_count",),
    }
    for alias in aliases.get(field.lower(), (field.lower(),)):
        value = row.get(alias)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list) and not value:
            continue
        return True
    return False


def _canonical_key(row: Mapping[str, Any]) -> tuple[str, str]:
    event_model = str(row.get("event_model") or "")
    company_id = _slug(row.get("company_id") or "")
    company_name = _slug(row.get("company_name") or "")
    job_title = _slug(row.get("job_title") or "")
    location = _slug(row.get("location") or "")
    role = _slug(row.get("role") or "")
    skill = _slug(row.get("skill") or "")
    salary = _slug(row.get("salary_range") or "")
    topic = _slug(row.get("topic") or "")
    source = _slug(row.get("source") or "")

    company = company_id or company_name
    if event_model in {"official_job_posting", "job_board_posting"}:
        if company and job_title and location:
            return f"job_posting:{company}:{job_title}:{location}", "complete"
        if company and job_title:
            return f"job_posting:{company}:{job_title}", "posting_proxy"
        if company:
            return f"job_posting:employer:{company}", "employer_proxy"
        if job_title and source:
            return f"job_posting:source:{source}:{job_title}", "source_proxy"
        if source:
            return f"job_posting:source:{source}", "source_proxy"
        return "", "missing"
    if event_model == "salary_signal":
        if role and location and salary:
            return f"salary:{role}:{location}:{_digest(salary)}", "complete"
        if role and location:
            return f"salary:{role}:{location}", "role_proxy"
        if role:
            return f"salary:{role}", "role_proxy"
        return "", "missing"
    if event_model == "skill_demand":
        if skill and role and location:
            return f"skill_demand:{skill}:{role}:{location}", "complete"
        if skill and role:
            return f"skill_demand:{skill}:{role}", "skill_proxy"
        if skill:
            return f"skill_demand:{skill}", "skill_proxy"
        return "", "missing"
    if event_model == "labor_market_signal":
        if topic and location:
            return f"labor_market:{topic}:{location}", "complete"
        if topic:
            return f"labor_market:{topic}", "topic_proxy"
        if source:
            return f"labor_market:source:{source}", "source_proxy"
        return "", "missing"
    return "", "missing"


def _event_key(row: Mapping[str, Any], event_at: datetime | None) -> str:
    observed = _as_utc(event_at).strftime("%Y%m%d") if event_at else "undated"
    basis = row.get("canonical_key") or row.get("source_url") or row.get("title") or ""
    return f"{row.get('event_model')}:{_digest(basis)}:{observed}"


def _signal_basis(article: Article, source: Source, event_model: str) -> str:
    source_model = _source_event_model(source, list(TRACKED_EVENT_MODEL_ORDER))
    if source_model == event_model and (
        source.type.lower() in {"api", "mcp", "browser"} or source.producer_role == "employer"
    ):
        return "operational_source"
    if source_model == event_model:
        return "source_context_signal"
    if _has_article_evidence(article, event_model):
        return "article_entity_signal"
    return "proxy_signal"


def _company_id(article: Article, source: Source, company_name: str) -> str:
    configured = _first_non_empty(source.config.get("company_id"), source.config.get("employer_id"))
    if configured:
        return _slug(configured)
    labeled = _summary_value(article, "Company ID")
    if labeled:
        return _slug(labeled)
    return _slug(company_name) if source.producer_role == "employer" else ""


def _company_name(article: Article, source: Source) -> str:
    labeled = _summary_value(article, "Company")
    if labeled:
        return labeled
    matches = _matches(article, "Company")
    if matches:
        return matches[0]
    if source.producer_role == "employer":
        return source.name.removesuffix(" Careers").strip()
    return ""


def _job_title(article: Article) -> str:
    labeled = _summary_value(article, "Job title")
    if labeled:
        return labeled
    matches = _matches(article, "JobTitle")
    return matches[0] if matches else ""


def _role(article: Article, job_title: str) -> str:
    labeled = _summary_value(article, "Role")
    if labeled:
        return labeled
    return job_title or _first(_matches(article, "EmploymentTrend"))


def _location(article: Article, source: Source) -> str:
    labeled = _summary_value(article, "Location", "Region")
    if labeled:
        return labeled
    matches = _matches(article, "Location")
    if matches:
        return matches[0]
    return _first_non_empty(source.region, source.country)


def _skills(article: Article) -> list[str]:
    return _matches(article, "TechStack")


def _topic(article: Article) -> str:
    labeled = _summary_value(article, "Topic")
    if labeled:
        return labeled
    for key in ("EmploymentTrend", "SourceSignal", "JobGeneral"):
        matches = _matches(article, key)
        if matches:
            return matches[0]
    return ""


def _salary_range(article: Article) -> str:
    labeled = _summary_value(article, "Salary range", "Salary")
    if labeled:
        return labeled
    match = re.search(
        r"((?:USD|\$|KRW)?\s*\d[\d,]*(?:\.\d+)?\s*(?:k|K|million|M|won)?\s*(?:-|to|~)\s*(?:USD|\$|KRW)?\s*\d[\d,]*(?:\.\d+)?\s*(?:k|K|million|M|won)?)",
        _article_text(article),
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else ""


def _demand_count(article: Article) -> int | None:
    labeled = _summary_value(article, "Demand count")
    if labeled:
        number = _first_number(labeled)
        return int(number) if number is not None else None
    return None


def _latest_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    dated: list[tuple[datetime, dict[str, Any]]] = []
    undated: list[dict[str, Any]] = []
    for row in events:
        parsed = _parse_datetime(str(row.get("event_at") or ""))
        if parsed is None:
            undated.append(row)
        else:
            dated.append((parsed, row))
    if dated:
        return max(dated, key=lambda row: row[0])[1]
    return undated[0] if undated else None


def _event_datetime(article: Article) -> datetime | None:
    value = article.published if isinstance(article.published, datetime) else article.collected_at
    return _as_utc(value) if isinstance(value, datetime) else None


def _article_source(article: Article) -> str:
    return str(getattr(article, "source", "") or "")


def _article_title(article: Article) -> str:
    return str(getattr(article, "title", "") or "")


def _article_link(article: Article) -> str:
    return str(getattr(article, "link", "") or "")


def _article_summary(article: Article) -> str:
    return str(getattr(article, "summary", "") or "")


def _article_entities(article: Article) -> dict[str, Any]:
    raw = getattr(article, "matched_entities", {})
    return raw if isinstance(raw, dict) else {}


def _matches(article: Article, key: str) -> list[str]:
    raw = _article_entities(article).get(key, [])
    if isinstance(raw, list):
        return [str(value).strip() for value in raw if str(value).strip()]
    if raw:
        return [str(raw).strip()]
    return []


def _summary_value(article: Article, *labels: str) -> str:
    text = " ".join(_article_text(article).split())
    for label in labels:
        match = re.search(rf"\b{re.escape(label)}\s*[:=]\s*", text, flags=re.IGNORECASE)
        if not match:
            continue
        start = match.end()
        end = len(text)
        for next_label in SUMMARY_LABELS:
            next_match = re.search(
                rf"\b{re.escape(next_label)}\s*[:=]\s*",
                text[start:],
                flags=re.IGNORECASE,
            )
            if next_match:
                end = min(end, start + next_match.start())
        return text[start:end].strip(" \t\r\n.;,")
    return ""


def _article_text(article: Article) -> str:
    return f"{_article_title(article)} {_article_summary(article)} {_article_link(article)}"


def _source_context(source: Source) -> str:
    return " ".join(
        [
            source.name,
            source.type,
            source.content_type,
            source.collection_tier,
            source.producer_role,
            " ".join(source.info_purpose),
            " ".join(str(value) for value in source.config.values()),
        ]
    ).lower()


def _has_any(text: str, needles: list[str]) -> bool:
    lowered = text.lower()
    return any(needle.lower() in lowered for needle in needles)


def _dict(mapping: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = mapping.get(key)
    if isinstance(value, Mapping):
        return {str(k): v for k, v in value.items()}
    return {}


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime | None:
    normalized = value.strip()
    if not normalized or normalized == "None":
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _age_days(generated_at: datetime, event_at: datetime) -> float:
    return max(0.0, (_as_utc(generated_at) - _as_utc(event_at)).total_seconds() / 86400)


def _first(values: list[str]) -> str:
    return values[0] if values else ""


def _first_non_empty(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _first_number(text: str) -> float | None:
    match = re.search(r"\d[\d,]*(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:120]


def _digest(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
