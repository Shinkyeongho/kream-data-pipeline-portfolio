# Spark·Delta Lake 개인 확장

기존 MySQL 팀 프로젝트의 정제 규칙을 Spark·Delta로 재구현하고, 저장 버전·배치 재시도·Gold 집계의 정합성을 검증했습니다. 개인 확장 기간은 2026.10.22이며 기존 팀 프로젝트 기간과 구분합니다.

## 구현한 범위

| 실제 데이터로 확인한 범위 | 결과·근거 |
| --- | --- |
| CSV 5종 Bronze 적재 | 674,232개 파일별 레코드. 원문·해시·출처 보존. 고유 검수 건수와 다름 |
| 초기 검수 Silver 분류 | 335,000행 → 정상 271,787·격리 33,854·제외 29,359 |
| 정제된 실제 데이터의 배치 재생 | 이전 개정 19,761행 갱신·신규 252,026행 추가. 초기 Spark Silver와 전 컬럼 양방향 차이 0행 |
| 일별 Gold | 최종 상태 271,787행과 유효 개정 작업량 291,548행 구분. 두 Gold 모든 집계 행의 Python Decimal 대조 일치 |

가상 데이터로 먼저 MERGE·내용 멱등성·원장·현재 개정 충돌 거부를 확인했습니다. 실제 데이터에는 이미 정제한 스냅샷의 배치 재생을 적용했습니다. **새 raw CSV 배치 전체를 정제하는 증분 파이프라인은 아직 구현하지 않았습니다.**

## 바로 확인할 자료

- [구조와 입력·출력](docs/architecture.md)
- [실행 근거·실제/가상 검증 범위](docs/validation.md)
- [설계 판단·개인 수행·개발 방식](docs/decisions.md)
- [환경과 순서·비공개 입력·재현 제약](docs/reproduce.md)
- [검증 인덱스](evidence/verification-index.json)

## 대표 구현

| 확인할 문제 | 코드 | 저장된 결과 |
| --- | --- | --- |
| 원문을 보존하며 적재했는가? | [02 Bronze](lessons/02_bronze_ingestion.py) | [36개 검사](results/lesson-02-latest.json) |
| 금액·시각·참조를 어떻게 판단했는가? | [03 숫자](lessons/03_numeric_candidates.py) · [04 시각](lessons/04_datetime_candidates.py) · [05 사번](lessons/05_ids_employee_reference.py) · [06 상품](lessons/06_product_names.py) · [07 카테고리](lessons/07_category_reference.py) | [검증 범위](docs/validation.md) |
| 중복·충돌·최신 유효 개정은? | [08 초기 Silver](lessons/08_revision_silver.py) | [34개 검사](results/lesson-08-latest.json) |
| 실제 데이터 재시도 결과가 같은가? | [11 실제 Silver 배치 재생](lessons/11_real_silver_replay.py) | [23개 검사·양방향 차이 0](results/lesson-11-latest.json) |
| 최종 상태와 작업량의 분모가 다른가? | [12 일별 Gold](lessons/12_gold_daily.py) | [20개 검사](results/lesson-12-latest.json) |

가상 예제는 [01 저장·과거 버전](lessons/01_delta_versions.py), [09 MERGE](lessons/09_merge_basics.py), [10 배치 원장·예외 주입](lessons/10_batch_ledger.py)입니다. 실제 원본 데이터 처리 성과와 구분합니다.

## 재현

Python 3.12.14·Java 17·PySpark 4.0.1·Delta Lake 4.0.0에서 로컬 실행했습니다. 원본 CSV를 보유한 환경의 순차 실행 절차는 [재현 문서](docs/reproduce.md)를 따릅니다. 패키지는 [requirements.txt](requirements.txt)에 고정했습니다.

원본·전체 Delta·가상환경·캐시는 공개하지 않습니다. 저장된 JSON은 검사와 집계 요약이며 원본 데이터나 완전한 실행 데이터 백업이 아닙니다. 각 스크립트는 새 실행 경로를 만들고, 같은 대상에 대한 재시도는 해당 예제 내부에서 검증합니다.

## 확인한 한계

- 초기 Spark Silver와 배치 재생 결과는 전 컬럼으로 대조했지만 MySQL 최종 테이블 전 컬럼과 대조한 것은 아닙니다.
- 실습 08의 입력 분류와 실습 11·12는 선행 정제 근거를 공유합니다. 같은 규칙의 독립적 타당성 증명과 구분합니다.
- 원장·대상은 별도 트랜잭션이며 단일 실행자를 가정합니다. 실제 프로세스 강제 종료가 아닌 특정 구간의 예외 주입 시험입니다.
- 새 원본 정제 증분·전체 개정 이력 충돌·마스터 변경·동시성·Gold 증분은 미구현입니다.
- 처리 성능 개선 비교는 하지 않았습니다. Windows 종료 시 임시 JAR 정리 경고가 남았고, 데이터 검사 통과·종료 코드 0과 구분해 기록했습니다.

## 개발 도구 활용

개인 Delta Lake 확장의 구현·검사 코드와 문서 작성에 생성형 AI 지원을 활용했습니다. 각 단계를 로컬에서 직접 실행하고, Silver 전 컬럼 대조와 Gold 집계 검사의 결과를 확인했습니다. [개발 방식과 수행 범위](docs/decisions.md#개발-방식과-수행-범위)

팀 MySQL·Pandas 산출물은 [기존 프로젝트 문서](../../docs/data-and-scope.md)와 출처를 유지합니다.
