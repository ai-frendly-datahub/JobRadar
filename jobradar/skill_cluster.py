"""Skill graph + role clustering for JobRadar.

Reads job postings (objects with title + summary) and:

1. Extracts skill tokens from a curated multilingual lexicon
   (python, java, react, nlp, etc.).
2. Builds a co-occurrence weighted graph between skills.
3. Clusters jobs into roles by majority-skill overlap with a small
   set of canonical role templates.

No external ML dependencies — TF-IDF + cosine on a fixed skill vocabulary
keeps this runnable inside the existing JobRadar pytest matrix.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import combinations


# Curated lexicon — extend in priceradar/config as needed.
_SKILL_LEXICON: tuple[str, ...] = (
    "python",
    "java",
    "kotlin",
    "javascript",
    "typescript",
    "go",
    "rust",
    "scala",
    "ruby",
    "swift",
    "react",
    "vue",
    "angular",
    "node.js",
    "nodejs",
    "express",
    "flask",
    "fastapi",
    "django",
    "spring",
    "spring boot",
    "sql",
    "mysql",
    "postgresql",
    "redis",
    "mongodb",
    "elasticsearch",
    "duckdb",
    "kafka",
    "spark",
    "airflow",
    "dbt",
    "snowflake",
    "bigquery",
    "docker",
    "kubernetes",
    "terraform",
    "aws",
    "gcp",
    "azure",
    "tensorflow",
    "pytorch",
    "scikit-learn",
    "nlp",
    "llm",
    "huggingface",
    "transformers",
    "ai",
    "ml",
    "데이터엔지니어",
    "데이터분석",
    "백엔드",
    "프론트엔드",
    "풀스택",
    "ios",
    "android",
)

# Canonical role -> required skills (minimum overlap = 2).
_ROLE_TEMPLATES: dict[str, set[str]] = {
    "data-engineer": {"python", "sql", "kafka", "spark", "airflow", "dbt"},
    "ml-engineer": {"python", "pytorch", "tensorflow", "huggingface", "transformers", "ml"},
    "backend-python": {"python", "fastapi", "django", "flask", "postgresql", "redis"},
    "backend-java": {"java", "kotlin", "spring", "spring boot", "mysql", "postgresql"},
    "frontend-web": {"react", "vue", "angular", "typescript", "javascript"},
    "devops-platform": {"kubernetes", "docker", "terraform", "aws", "gcp", "azure"},
    "mobile-ios": {"swift", "ios"},
    "mobile-android": {"kotlin", "android"},
}


@dataclass(frozen=True)
class SkillExtraction:
    job_id: str
    skills: tuple[str, ...]
    role: str


def extract_skills(text: str | None) -> tuple[str, ...]:
    if not text:
        return ()
    lowered = text.lower()
    found: list[str] = []
    for skill in _SKILL_LEXICON:
        if skill in lowered:
            found.append(skill)
    # de-dup while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for s in found:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return tuple(out)


def classify_role(skills: Iterable[str]) -> str:
    skill_set = set(skills)
    if not skill_set:
        return "other"
    best_role = "other"
    best_score = 0
    for role, required in _ROLE_TEMPLATES.items():
        overlap = len(skill_set & required)
        if overlap > best_score:
            best_score = overlap
            best_role = role
    return best_role if best_score >= 2 else "other"


def extract_for_jobs(
    jobs: Iterable[object],
    *,
    id_attr: str = "link",
) -> list[SkillExtraction]:
    out: list[SkillExtraction] = []
    for job in jobs:
        text = " ".join(
            str(getattr(job, attr, "") or "") for attr in ("title", "summary")
        )
        skills = extract_skills(text)
        role = classify_role(skills)
        job_id = str(getattr(job, id_attr, "") or "")
        out.append(SkillExtraction(job_id, skills, role))
    return out


def build_skill_cooccurrence(
    extractions: Iterable[SkillExtraction],
    *,
    min_pair_count: int = 2,
) -> dict[tuple[str, str], int]:
    """Co-occurrence weighted edges, only keeping pairs that appear >= min_pair_count."""
    counter: Counter[tuple[str, str]] = Counter()
    for ext in extractions:
        for a, b in combinations(sorted(set(ext.skills)), 2):
            counter[(a, b)] += 1
    return {pair: count for pair, count in counter.items() if count >= min_pair_count}


def role_distribution(extractions: Iterable[SkillExtraction]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for ext in extractions:
        counts[ext.role] += 1
    return dict(counts)


__all__ = [
    "SkillExtraction",
    "extract_skills",
    "classify_role",
    "extract_for_jobs",
    "build_skill_cooccurrence",
    "role_distribution",
]
