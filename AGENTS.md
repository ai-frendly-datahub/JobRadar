# JOBRADAR

채용 뉴스, 커뮤니티, 공식 채용 페이지를 함께 읽어 hiring/salary/skill 신호를 분류하는 Standard Tier 레이더입니다.

## STRUCTURE

```
JobRadar/
├── jobradar/
│   ├── collector.py              # collect_sources() — 채용 데이터 수집
│   ├── browser_collector.py      # 동적 페이지/브라우저 수집 보조
│   ├── analyzer.py               # apply_entity_rules() — 회사/직무/기술 키워드 매칭
│   ├── reporter.py               # generate_report(), generate_index_html()
│   ├── storage.py                # RadarStorage — DuckDB upsert/query/retention
│   ├── models.py                 # radar-core 기반 모델 재사용
│   ├── config_loader.py          # YAML 로딩
│   ├── logger.py                 # 구조화 로깅
│   ├── resilience.py             # 재시도/장애 격리
│   └── exceptions.py             # 커스텀 예외
├── config/
│   ├── config.yaml
│   └── categories/job.yaml       # 소스 + 엔티티 정의
├── data/                         # DuckDB, raw data
├── reports/                      # 일자별 summary + index.html
├── tests/                        # analyzer / reporter / storage 테스트
├── docs/                         # 분석 산출물
└── main.py                       # CLI 엔트리포인트
```

## ENTITIES

| Entity | Examples |
|--------|----------|
| Company | 채용 기업명 |
| JobTitle | 백엔드, 프론트엔드, 데이터엔지니어, PM 등 |
| TechStack / EmploymentSignal | Python, React, AWS, hiring freeze, salary, layoff |

## DEVIATIONS FROM TEMPLATE

- `browser_collector.py`가 있어 일부 채용 플랫폼 동적 페이지 대응 가능
- `reports/`에 일자별 `job_YYYYMMDD_summary.json` 누적 산출물이 존재
- `reports/job_quality.json`와 `job_YYYYMMDD_quality.json`이 source/event freshness 상태를 기록
- analytics 산출물이 루트와 `docs/`에 함께 남아 있어 운영 산출물 구분이 필요할 수 있음
- taxonomy 기준으로 `공식 + 운영 + 시장 + 커뮤니티` 레이어를 유지한다.
- config loader가 source 메타데이터(`trust_tier`, `info_purpose`, `config`)를 보존한다.
- 광범위 경제/커뮤니티 소스는 직무·고용·스킬 신호 또는 source-level taxonomy 태그가 있을 때만 리포트에 반영한다.

## COMMANDS

```bash
python main.py --category job --recent-days 7
python scripts/check_quality.py
pip install 'radar-core[browser]'
pytest tests/ -v
```

## NOTES

- 브라우저 수집기 수정 시 네트워크/동적 DOM 의존성을 함께 확인
- 플랫폼 추가 시 `config/categories/job.yaml`과 collector 경로를 같이 점검
