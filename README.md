# JobRadar - 채용 레이더

**🌐 Live Report**: https://ai-frendly-datahub.github.io/JobRadar/

채용 뉴스, 커뮤니티, 공식 채용 페이지를 함께 수집해 회사·직무·기술스택·고용 신호를 분석하는 레이더입니다.

## 개요

- **수집 소스**: 채용 전문 미디어, Reddit 커뮤니티, LinkedIn/Indeed 연구, Kakao/Samsung 공식 채용 페이지
- **분석 대상**: 회사(Company), 직무(JobTitle), 기술스택(TechStack)
- **출력**: GitHub Pages HTML 리포트 (Flatpickr 캘린더 + Chart.js 트렌드)

## 빠른 시작

```bash
pip install -e ".[dev]"
python main.py --once
```

## 구조

```
JobRadar/
  jobradar/
    collector.py    # 채용 플랫폼 데이터 수집
    analyzer.py     # 엔티티 분석 (radar-core 위임)
    storage.py      # DuckDB 저장 (radar-core 위임)
    reporter.py     # HTML 리포트 생성 (radar-core 위임)
  config/
    config.yaml           # database_path, report_dir
    categories/job.yaml   # 수집 소스 + 엔티티 정의
  main.py           # CLI 진입점
  tests/            # 단위 테스트
```

## 설정

`config/config.yaml` 및 `config/categories/job.yaml` 참조.

## 소스 전략

- `공식`: Kakao Careers, Samsung Careers, LinkedIn Talent Blog
- `운영`: Indeed Hiring Lab, 채용/연봉/스킬 트렌드 신호
- `시장`: 채용 전문 미디어와 HR 업계 뉴스
- `커뮤니티`: Reddit 기반 구직자 체감 신호

광범위 경제/커뮤니티 소스는 회사명이나 산업명만으로는 리포트에 반영하지 않고, 직무·고용·스킬 신호가 있을 때만 유지합니다. `reports/job_quality.json`은 공식 채용공고, 노동시장 신호, 스킬/급여 신호의 freshness 상태를 기록합니다.

브라우저 소스를 제대로 수집하려면 `pip install 'radar-core[browser]'`가 필요합니다.

## 개발

```bash
pytest tests/ -v
```

## 스케줄

GitHub Actions로 매일 자동 수집 후 GitHub Pages 배포.

<!-- DATAHUB-OPS-AUDIT:START -->
## DataHub Operations

- CI/CD workflows: `deploy-pages.yml`, `radar-crawler.yml`.
- GitHub Pages visualization: `reports/index.html` (valid HTML); https://ai-frendly-datahub.github.io/JobRadar/.
- Latest remote Pages check: HTTP 200, HTML.
- Local workspace audit: 23 Python files parsed, 0 syntax errors.
- Re-run audit from the workspace root: `python scripts/audit_ci_pages_readme.py --syntax-check --write`.
- Latest audit report: `_workspace/2026-04-14_github_ci_pages_readme_audit.md`.
- Latest Pages URL report: `_workspace/2026-04-14_github_pages_url_check.md`.
<!-- DATAHUB-OPS-AUDIT:END -->
