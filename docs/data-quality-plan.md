# Data Quality Plan

- 생성 시각: `2026-04-11T16:05:37.910248+00:00`
- 우선순위: `P2`
- 데이터 품질 점수: `77`
- 가장 약한 축: `권위성`
- Governance: `high`
- Primary Motion: `conversion`

## 현재 이슈

- 고거버넌스 저장소 대비 공식 근거 source가 얕음
- 가장 약한 품질 축은 권위성(52)

## 필수 신호

- 공식 채용공고와 job board API
- 급여·스킬·지역·고용형태 구조화 필드
- 산업별 hiring trend와 기업별 채용 변화

## 품질 게이트

- 회사명·직무명·지역을 canonical key로 정규화
- 중복 공고와 repost를 구분
- 게시일·마감일·수집일을 별도 필드로 유지

## 다음 구현 순서

- 공식 채용공고와 job board source tier를 보강
- salary/skill extractor를 운영 레이어로 추가
- 회사·직무·지역 canonicalization rule을 추가

## 운영 규칙

- 원문 URL, 수집일, 이벤트 발생일은 별도 필드로 유지한다.
- 공식 source와 커뮤니티/시장 source를 같은 신뢰 등급으로 병합하지 않는다.
- collector가 인증키나 네트워크 제한으로 skip되면 실패를 숨기지 말고 skip 사유를 기록한다.
- 이 문서는 `scripts/build_data_quality_review.py --write-repo-plans`로 재생성한다.
