# 환경·입력·실행 순서

## 확인한 환경

| 구성 | 로컬 검증 버전 |
| --- | --- |
| OS | Windows |
| Python | 3.12.14 |
| Java | 17 |
| PySpark | 4.0.1 |
| delta-spark | 4.0.0 |

[requirements.txt](../requirements.txt)에 Python 패키지를 고정했습니다. Maven Delta 패키지를 처음 받는 환경에는 네트워크가 필요하고, 실행에는 Java가 필요합니다.

검증한 Windows 환경에는 Hadoop 런타임이 구성되어 있었습니다. 저장소에서 임의 외부 실행 파일을 자동 설치하지 않습니다. 다른 Windows 환경이나 Linux·클라우드에서 자동 실행된다는 검증은 하지 않았습니다. 한글 경로의 Spark 실행 문제를 피하기 위해 Python 환경과 데이터·캐시는 영문 경로로 분리했습니다.

## 필요한 비공개 입력

실제 데이터 단계에는 교육 프로젝트에서 제공받은 data.zip이 필요합니다. 원본 CSV는 재배포하지 않으며 이 저장소에서 다운로드를 제공하지 않습니다. 제공 ZIP의 data/ 아래에는 다음 파일이 있어야 합니다.

- kream_product_std_dict.csv
- kream_category_parent_lookup.csv
- kream_inspector_master.csv
- kream_inspection_300000.csv
- kream_inspection_join_ready.csv

파일별 SHA-256과 행 수·헤더가 [공개 원본 매니페스트](../../../evidence/source_manifest.csv)에 맞지 않으면 Bronze 단계가 중단됩니다. 임의 CSV를 넣어 같은 결과가 나온다는 범용 예제가 아닙니다.

원본이 없으면 코드·저장된 JSON·문서를 검토할 수 있습니다. 전체 CSV 처리는 재실행할 수 없습니다. 01은 별도 가상 2행으로 실행 가능하지만 09·10은 현재 스크립트의 선행 성공 결과·런타임 계보 확인을 요구하므로 단독 실행 절차라고 안내하지 않습니다.

## 설치 예시

저장소를 영문 경로에 준비한 후, PowerShell에서 실행합니다. 아래 경로는 예시이며 자신의 경로로 바꿉니다. 기존 교육용 가상환경을 수정하지 않고 별도 환경을 만듭니다.

```powershell
Set-Location -LiteralPath 'C:\work\kream-data-pipeline-portfolio'
python -m venv 'C:\kream-delta-env'
& 'C:\kream-delta-env\Scripts\python.exe' -m pip install -r '.\extensions\delta-lake\requirements.txt'
$DeltaPython = 'C:\kream-delta-env\Scripts\python.exe'
$env:PYTHONIOENCODING = 'utf-8'
$env:KREAM_DELTA_RUNTIME_DIR = 'C:\kream-delta-data'
$SourceZip = 'D:\private\data.zip'
```

Java 17과 필요한 Windows 런타임 구성을 먼저 확인합니다. 위 설치 명령만으로 Java·Hadoop까지 구성되는 것은 아닙니다.

## 실행 순서

모든 스크립트는 lessons/의 번호 순서로 실행합니다. 실습 02에만 원본 ZIP을 명시합니다.

```powershell
$lessonFiles = Get-ChildItem -LiteralPath '.\extensions\delta-lake\lessons' -Filter '*.py' | Sort-Object Name
foreach ($lessonFile in $lessonFiles) {
    $lessonArgs = @($lessonFile.FullName)
    if ($lessonFile.Name -eq '02_bronze_ingestion.py') {
        $lessonArgs += @('--source-zip', $SourceZip)
    }
    & $DeltaPython @lessonArgs
    if ($LASTEXITCODE -ne 0) { throw "중단된 단계: $($lessonFile.Name)" }
}
```

이 절차는 검증한 환경을 재구성하기 위한 안내입니다. 저장소 이동·새 환경 설치부터 전체를 다시 실행한 별도 검증 기록은 아닙니다. 저장된 요약은 기존 로컬 환경의 실제 실행 결과입니다.

| 번호 | 단계 | 선행 입력 |
| --- | --- | --- |
| 01 | 가상 저장·갱신·과거 버전 | 없음 |
| 02 | CSV 5종 Bronze | 원본 ZIP·저장소의 원본 매니페스트 |
| 03~07 | 숫자·시각·사번·상품·카테고리 후보 | 최신 성공 요약에 연결된 실제 Delta 입력 |
| 08 | 초기 Silver·격리·제외 | 07·같은 Bronze의 사전 |
| 09 | 가상 MERGE | 08 성공·해당 실제 Silver의 버전 확인 |
| 10 | 가상 배치 원장 | 09 성공·09 코드·실행 테이블 |
| 11 | 실제 정제 스냅샷 재생 | 08·10 성공, 08 코드와 초기 Silver·스테이지 |
| 12 | 고정 스냅샷 두 Gold | 11 성공과 연결된 08 분류 스테이지 |

## 결과와 재실행 의미

각 실습은 환경변수 경로의 lesson-NN/<run_id>/에 새 데이터를 쓰고, 저장소 results/lesson-NN-latest.json을 갱신합니다. 결과 JSON만 옮겨서는 실제 Delta 데이터가 복구되지 않습니다.

01은 검사 수 필드 없이 checks와 all_passed를 기록하고, 02~12는 check_count와 all_passed도 제공합니다. 실패 시 다음 단계로 진행하지 않고 해당 결과와 오류를 확인합니다. 기존 로컬 환경에서는 종료 시 임시 JAR 정리 경고가 남았지만 프로세스 종료 코드 0과 데이터 검사 통과를 확인했습니다.

스크립트 전체 재실행은 새 경로·새 예제 원장을 만듭니다. 09~11의 같은 대상 재입력·완료 반복·예외 후 재시도는 각각 한 실행 안에서 검증합니다. 매번 새 경로를 만드는 것을 멱등성 근거로 사용하지 않습니다.

중간 단계만 다시 실행해 최신 요약의 계보를 바꾸면 후속 단계가 이전 실행을 가리킬 수 있습니다. 실제 데이터를 다시 만들 때는 해당 단계 이후를 순서대로 실행하고, 이전 실행 폴더와 그 결과를 임의로 섞지 않습니다. 여러 실행을 동시에 돌리지 않습니다.

## Git 공개 제외

원본 ZIP·CSV, 런타임의 Delta·JSONL 입력·캐시·가상환경, 인증정보, 개인 학습 기록은 공개하지 않습니다. 코드·요약 JSON에는 원본 행의 개인 이름·사번 목록을 출력하지 않으며 소량 행 예시는 가상 자료로 구분합니다.

기존 팀 MySQL 실행 코드는 이 개인 포트폴리오의 공개 대상에서 제외했습니다. 구현 출처와 기존 팀 라이선스·공개 범위를 임의로 바꾸지 않습니다. [프로젝트 범위](../../../docs/data-and-scope.md)
