from __future__ import annotations

from dataclasses import dataclass

from jobradar.skill_cluster import (
    build_skill_cooccurrence,
    classify_role,
    extract_for_jobs,
    extract_skills,
    role_distribution,
)


def test_extract_skills_finds_lexicon_hits() -> None:
    text = "Senior Python engineer, fastapi + redis. PostgreSQL + AWS."
    skills = extract_skills(text)
    assert "python" in skills
    assert "fastapi" in skills
    assert "redis" in skills
    assert "postgresql" in skills
    assert "aws" in skills


def test_extract_skills_dedupes() -> None:
    text = "python python python"
    assert extract_skills(text) == ("python",)


def test_classify_role_backend_python() -> None:
    assert classify_role(["python", "fastapi", "postgresql"]) == "backend-python"


def test_classify_role_data_engineer() -> None:
    assert classify_role(["python", "airflow", "spark"]) == "data-engineer"


def test_classify_role_single_skill_is_other() -> None:
    # Only one matching skill is below the 2-overlap floor.
    assert classify_role(["python"]) == "other"


def test_classify_role_empty_is_other() -> None:
    assert classify_role([]) == "other"


@dataclass
class _Job:
    title: str
    summary: str
    link: str


def test_extract_for_jobs_returns_per_job_records() -> None:
    jobs = [
        _Job("Senior Python Backend", "FastAPI, PostgreSQL, Redis", "j1"),
        _Job("Frontend Engineer", "React + TypeScript", "j2"),
        _Job("Data Engineer", "Airflow, Spark, Python", "j3"),
        _Job("Office Manager", "schedule meetings", "j4"),
    ]
    extractions = extract_for_jobs(jobs)
    by_id = {e.job_id: e for e in extractions}
    assert by_id["j1"].role == "backend-python"
    assert by_id["j2"].role == "frontend-web"
    assert by_id["j3"].role == "data-engineer"
    assert by_id["j4"].role == "other"


def test_cooccurrence_filters_low_count_pairs() -> None:
    jobs = [
        _Job("J1", "python fastapi postgresql", "j1"),
        _Job("J2", "python fastapi redis", "j2"),
        _Job("J3", "java spring boot", "j3"),
    ]
    extractions = extract_for_jobs(jobs)
    edges = build_skill_cooccurrence(extractions, min_pair_count=2)
    assert ("fastapi", "python") in edges
    # ("python", "fastapi") and ("fastapi", "python") sorted to alphabetical
    assert edges[("fastapi", "python")] == 2
    # The java pair appears once — below threshold, should be dropped.
    assert ("java", "spring boot") not in edges


def test_role_distribution_counts() -> None:
    jobs = [
        _Job("Senior Python Backend", "FastAPI, PostgreSQL, Redis", "j1"),
        _Job("Backend Engineer", "Spring Boot, Kotlin, MySQL", "j2"),
        _Job("Frontend Engineer", "React + TypeScript", "j3"),
    ]
    extractions = extract_for_jobs(jobs)
    dist = role_distribution(extractions)
    assert dist["backend-python"] == 1
    assert dist["backend-java"] == 1
    assert dist["frontend-web"] == 1
