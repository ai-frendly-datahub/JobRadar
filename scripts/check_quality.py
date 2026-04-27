from __future__ import annotations

import sys
from datetime import UTC, datetime
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (PROJECT_ROOT, WORKSPACE_ROOT / "radar-core"):
    path_value = str(path)
    if path_value not in sys.path:
        sys.path.insert(0, path_value)

from jobradar.config_loader import load_category_config, load_category_quality_config  # noqa: E402
from jobradar.quality_report import build_quality_report, write_quality_report  # noqa: E402
from jobradar.relevance import apply_source_context_entities, filter_relevant_articles  # noqa: E402
from jobradar.storage import RadarStorage  # noqa: E402
from radar_core.common import quality_checks as shared_quality_checks  # noqa: E402


def _project_path(project_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _load_runtime_config(project_root: Path) -> dict[str, Any]:
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        return {}
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
    return None


def _latest_article_date(db_path: Path, category_name: str) -> date | None:
    if not db_path.exists():
        return None
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            row = con.execute(
                """
                SELECT MAX(COALESCE(published, collected_at))
                FROM articles
                WHERE category = ?
                """,
                [category_name],
            ).fetchone()
    except duckdb.Error:
        return None
    if not row:
        return None
    return _coerce_date(row[0])


def _lookback_days(target_date: date | None, *, minimum_days: int = 14) -> int:
    if target_date is None:
        return minimum_days
    age_days = (datetime.now(UTC).date() - target_date).days + 1
    return max(minimum_days, age_days)


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def _run_storage_checks(con: duckdb.DuckDBPyConnection) -> None:
    table_name = "articles"
    columns = _table_columns(con, table_name)
    total = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    total_records = int(total[0]) if total else 0
    print(f"Total records: {total_records}")

    shared_quality_checks.check_missing_fields(
        con,
        table_name=table_name,
        null_conditions={
            "title": "title IS NULL OR title = ''",
            "link": "link IS NULL OR link = ''",
            "summary": "summary IS NULL OR summary = ''",
            "published": "published IS NULL",
        },
    )
    shared_quality_checks.check_duplicate_urls(con, table_name=table_name, url_column="link")
    shared_quality_checks.check_text_lengths(
        con,
        table_name=table_name,
        text_columns=["title", "summary"],
    )
    if "language" in columns:
        shared_quality_checks.check_language_values(
            con,
            table_name=table_name,
            language_column="language",
        )
    else:
        print("\nSkipping language check: missing column 'language'")

    if "published" in columns:
        shared_quality_checks.check_dates(con, table_name=table_name, date_column="published")
    else:
        print("\nSkipping date check: missing column 'published'")


def generate_quality_artifacts(
    project_root: Path = PROJECT_ROOT,
    *,
    category_name: str = "job",
) -> tuple[dict[str, Path], dict[str, Any]]:
    runtime_config = _load_runtime_config(project_root)
    db_path = _project_path(
        project_root,
        str(runtime_config.get("database_path", "data/radar_data.duckdb")),
    )
    report_dir = _project_path(
        project_root,
        str(runtime_config.get("report_dir", "reports")),
    )
    categories_dir = project_root / "config" / "categories"
    category_cfg = load_category_config(category_name, categories_dir=categories_dir)
    quality_cfg = load_category_quality_config(category_name, categories_dir=categories_dir)
    lookback_days = _lookback_days(_latest_article_date(db_path, category_cfg.category_name))

    with RadarStorage(db_path) as storage:
        recent_articles = storage.recent_articles(
            category_cfg.category_name,
            days=lookback_days,
            limit=1000,
        )

    scoped_articles = filter_relevant_articles(
        apply_source_context_entities(recent_articles, category_cfg.sources),
        category_cfg.sources,
    )
    report = build_quality_report(
        category=category_cfg,
        articles=scoped_articles or recent_articles,
        quality_config=quality_cfg,
        generated_at=datetime.now(UTC),
    )
    paths = write_quality_report(
        report,
        output_dir=report_dir,
        category_name=category_cfg.category_name,
    )
    return paths, report


def main() -> int:
    runtime_config = _load_runtime_config(PROJECT_ROOT)
    db_path = _project_path(
        PROJECT_ROOT,
        str(runtime_config.get("database_path", "data/radar_data.duckdb")),
    )
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1

    with duckdb.connect(str(db_path), read_only=True) as con:
        _run_storage_checks(con)

    paths, report = generate_quality_artifacts(PROJECT_ROOT)
    summary = report["summary"]
    print(f"quality_report={paths['latest']}")
    print(f"tracked_sources={summary['tracked_sources']}")
    print(f"fresh_sources={summary['fresh_sources']}")
    print(f"stale_sources={summary['stale_sources']}")
    print(f"missing_sources={summary['missing_sources']}")
    print(f"not_tracked_sources={summary['not_tracked_sources']}")
    print(f"job_signal_event_count={summary['job_signal_event_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
