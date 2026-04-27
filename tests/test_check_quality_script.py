from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from jobradar.models import Article
from jobradar.storage import RadarStorage


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_quality.py"
    spec = importlib.util.spec_from_file_location("jobradar_check_quality_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generate_quality_artifacts_uses_latest_stored_checkpoint(
    tmp_path: Path,
    capsys,
) -> None:
    project_root = tmp_path
    (project_root / "config" / "categories").mkdir(parents=True)

    (project_root / "config" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "database_path": "data/radar_data.duckdb",
                "report_dir": "reports",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (project_root / "config" / "categories" / "job.yaml").write_text(
        yaml.safe_dump(
            {
                "category_name": "job",
                "display_name": "Job Radar",
                "sources": [
                    {
                        "id": "kakao_careers",
                        "name": "Kakao Careers",
                        "type": "browser",
                        "url": "https://example.com/jobs",
                        "content_type": "job_posting",
                        "producer_role": "employer",
                        "enabled": True,
                        "info_purpose": ["official_job_posting"],
                        "config": {
                            "freshness_sla_days": 7,
                        },
                    }
                ],
                "entities": [],
                "data_quality": {
                    "quality_outputs": {
                        "tracked_event_models": ["official_job_posting"],
                    }
                },
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    article_time = datetime.now(UTC) - timedelta(days=30)
    db_path = project_root / "data" / "radar_data.duckdb"
    with RadarStorage(db_path) as storage:
        storage.upsert_articles(
            [
                Article(
                    title="Backend engineer hiring",
                    link="https://example.com/jobs/1",
                    summary="Hiring backend engineer in Seoul.",
                    published=article_time,
                    collected_at=article_time,
                    source="Kakao Careers",
                    category="job",
                    matched_entities={
                        "Company": ["Kakao"],
                        "JobTitle": ["Backend Engineer"],
                    },
                )
            ]
        )

    module = _load_script_module()
    paths, report = module.generate_quality_artifacts(project_root)

    assert Path(paths["latest"]).exists()
    assert Path(paths["dated"]).exists()
    assert report["summary"]["tracked_sources"] == 1
    assert report["summary"]["official_job_posting_events"] == 1

    module.PROJECT_ROOT = project_root
    module.main()
    captured = capsys.readouterr()
    assert "quality_report=" in captured.out
    assert "tracked_sources=1" in captured.out
    assert "job_signal_event_count=1" in captured.out
