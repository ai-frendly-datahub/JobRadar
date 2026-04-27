from __future__ import annotations

from jobradar.analyzer import apply_entity_rules
from jobradar.models import Article, EntityDefinition


def _make_article(title: str = "", summary: str = "") -> Article:
    return Article(
        title=title,
        link="https://example.com/1",
        summary=summary,
        published=None,
        source="TestSource",
        category="test",
    )


def _make_entity(name: str, keywords: list[str]) -> EntityDefinition:
    return EntityDefinition(name=name, display_name=name, keywords=keywords)


def test_keyword_match():
    """Korean keyword in title triggers match."""
    article = _make_article(title="삼성전자 채용 소식")
    entities = [_make_entity("삼성", ["삼성전자"])]
    results = apply_entity_rules([article], entities)
    assert len(results) == 1
    assert "삼성" in results[0].matched_entities
    assert "삼성전자" in results[0].matched_entities["삼성"]


def test_no_match():
    """Article without matching keywords has empty matched_entities."""
    article = _make_article(title="날씨 뉴스", summary="오늘 맑음")
    entities = [_make_entity("삼성", ["삼성전자"])]
    results = apply_entity_rules([article], entities)
    assert len(results) == 1
    assert results[0].matched_entities == {}


def test_case_insensitive():
    """ASCII keyword matching is case-insensitive."""
    article = _make_article(title="Google Hiring Update")
    entities = [_make_entity("Google", ["google"])]
    results = apply_entity_rules([article], entities)
    assert "Google" in results[0].matched_entities


def test_multiple_entities():
    """Multiple entities can match the same article."""
    article = _make_article(
        title="삼성전자와 LG전자 채용 공고",
        summary="대기업 채용 시즌",
    )
    entities = [
        _make_entity("삼성", ["삼성전자"]),
        _make_entity("LG", ["lg전자"]),
    ]
    results = apply_entity_rules([article], entities)
    assert "삼성" in results[0].matched_entities
    assert "LG" in results[0].matched_entities


def test_empty_articles():
    """Empty article list returns empty result."""
    entities = [_make_entity("테스트", ["키워드"])]
    results = apply_entity_rules([], entities)
    assert results == []


def test_keyword_in_summary():
    """Keyword in summary (not title) also triggers match."""
    article = _make_article(title="채용 뉴스", summary="네이버에서 신규 채용")
    entities = [_make_entity("네이버", ["네이버"])]
    results = apply_entity_rules([article], entities)
    assert "네이버" in results[0].matched_entities


def test_ascii_word_boundary():
    """ASCII keywords respect word boundaries."""
    article = _make_article(title="Googled something today")
    entities = [_make_entity("Google", ["google"])]
    results = apply_entity_rules([article], entities)
    # "google" should NOT match "Googled" due to word boundary
    assert results[0].matched_entities == {}
