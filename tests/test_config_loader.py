from __future__ import annotations

from jobradar.config_loader import load_category_config, load_category_quality_config


def test_load_category_config_preserves_source_metadata(tmp_path) -> None:
    categories_dir = tmp_path / "categories"
    categories_dir.mkdir()
    (categories_dir / "job.yaml").write_text(
        """
category_name: job
display_name: Job
sources:
  - name: Kakao Careers
    id: kakao_careers
    type: browser
    url: https://careers.kakao.com/jobs?company=ALL&part=TECHNOLOGY
    enabled: true
    language: ko
    country: KR
    trust_tier: T1_authoritative
    weight: 2.0
    content_type: job_posting
    collection_tier: C3_html_js
    producer_role: employer
    info_purpose:
      - official_job_posting
      - tech_hiring
    notes: official company hiring board
    config:
      wait_for: body
entities: []
""",
        encoding="utf-8",
    )

    config = load_category_config("job", categories_dir=categories_dir)
    source = config.sources[0]

    assert source.id == "kakao_careers"
    assert source.language == "ko"
    assert source.country == "KR"
    assert source.trust_tier == "T1_authoritative"
    assert source.weight == 2.0
    assert source.content_type == "job_posting"
    assert source.collection_tier == "C3_html_js"
    assert source.producer_role == "employer"
    assert source.info_purpose == ["official_job_posting", "tech_hiring"]
    assert source.notes == "official company hiring board"
    assert source.config == {"wait_for": "body"}


def test_load_category_quality_config_exposes_job_quality_contract(tmp_path) -> None:
    categories_dir = tmp_path / "categories"
    categories_dir.mkdir()
    (categories_dir / "job.yaml").write_text(
        """
category_name: job
display_name: Job
data_quality:
  priority: P2
  quality_outputs:
    freshness_report: reports/job_quality.json
    tracked_event_models:
      - official_job_posting
      - labor_market_signal
source_backlog:
  operational_candidates:
    - id: greenhouse_public_jobs
sources: []
entities: []
""",
        encoding="utf-8",
    )

    metadata = load_category_quality_config("job", categories_dir=categories_dir)
    data_quality = metadata["data_quality"]
    source_backlog = metadata["source_backlog"]

    assert isinstance(data_quality, dict)
    assert data_quality["priority"] == "P2"
    assert data_quality["quality_outputs"]["freshness_report"] == "reports/job_quality.json"
    assert set(data_quality["quality_outputs"]["tracked_event_models"]) == {
        "official_job_posting",
        "labor_market_signal",
    }
    assert isinstance(source_backlog, dict)
    assert source_backlog["operational_candidates"][0]["id"] == "greenhouse_public_jobs"
