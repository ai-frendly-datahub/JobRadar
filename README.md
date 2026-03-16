# JobRadar - 채용 레이더

**🌐 Live Report**: https://ai-frendly-datahub.github.io/JobRadar/

채용 정보를 수집·분석하는 레이더. 사람인, 잡코리아, 워크넷 등 주요 채용 플랫폼 데이터를 매일 수집하여 회사·직무·기술스택별로 분류하고 GitHub Pages에 배포합니다.

## 개요

- **수집 소스**: 사람인, 잡코리아, 워크넷
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

## 개발

```bash
pytest tests/ -v
```

## 스케줄

GitHub Actions로 매일 자동 수집 후 GitHub Pages 배포.
