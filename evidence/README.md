# 수치 근거 파일

아래 파일은 사용자가 최종안으로 지정한 팀 저장소 ZIP에서 바이트 변경 없이 선별했습니다. 기록된 실행 결과이며 이번 정리 과정에서 전체 검증을 다시 실행한 결과는 아닙니다.

| 파일 | 확인할 내용 |
| --- | --- |
| [source_manifest.csv](source_manifest.csv) | 원본 5종의 해시·행 수·컬럼 수 |
| [asis_business_diagnosis.csv](asis_business_diagnosis.csv) | 문제별 원본 진단, 분모와 해석 |
| [sql_pandas_comparison.csv](sql_pandas_comparison.csv) | 기존 SQL 진단과 Pandas의 8개 지표 비교 |
| [pandas_verification_summary.json](pandas_verification_summary.json) | 482개 검사, 대상 범위, SQL 품질 점검 상태 |
| [general_department_risk.csv](general_department_risk.csv) | 일반 검수의 부서별 위험 집계 |
| [general_duration_under10.csv](general_duration_under10.csv) | 10분 미만 검수 집계 |
| [reinspection_department_risk.csv](reinspection_department_risk.csv) | 재검수 분석 대상의 부서별 집계 |
| [team_asset_manifest.json](team_asset_manifest.json) | 선별한 팀 파일의 원본 경로·크기·해시 |

원본 진단 건수와 최종 격리·제외 건수는 범위가 다릅니다. 동일 행의 오류가 중첩될 수 있어 문제별 건수를 합산하지 않습니다. 482개 검사 실패 0개를 미검증 업무 규칙까지 포함한 전체 품질 100%로 확대하지 않습니다.

이후 개인 확장의 새 실행 요약은 이 팀 근거 파일과 구분해 [Delta 검증 인덱스](../extensions/delta-lake/evidence/verification-index.json)에 연결했습니다. 초기 Spark Silver·배치 재생·Gold 대조 범위는 [개인 확장 검증 문서](../extensions/delta-lake/docs/validation.md)를 참고합니다.
