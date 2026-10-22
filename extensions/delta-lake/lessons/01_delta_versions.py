"""학습용 2행으로 Delta 저장, 갱신, 과거 버전 조회를 검증한다.

실제 KREAM 원본을 사용하지 않는다. 실행마다 새 폴더를 생성하며 기존 데이터를
삭제하거나 덮어쓰지 않는다. 결과는 lessons의 상위 results 폴더에 기록한다.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

# Python 작업자가 필요할 때 현재 가상환경의 Python을 사용한다.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    # .local은 gitignore 대상이다. 생성되는 Parquet·로그를 공개 코드와 분리한다.
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    run_root = runtime / 'lesson-01' / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    table_path = run_root / 'inspection_delta'

    # Maven 패키지와 Spark의 실행 부산물은 프로젝트 안의 무시된 경로에 둔다.
    builder = (
        SparkSession.builder.appName('KREAM-Delta-Lesson-01')
        .master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy'))
        .config('spark.sql.warehouse.dir', (run_root / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'Asia/Seoul')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2')
        .config('spark.ui.enabled', 'false')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        # 1. 작은 표(DataFrame)를 만든다. 프로젝트 데이터와 구분한 학습용 값이다.
        initial = spark.sql("""
            SELECT INSP_ID, CAST(REV_NO AS INT) AS REV_NO,
                   CAST(minutes AS DECIMAL(12,8)) AS INSP_DURATION_MIN
            FROM VALUES
                ('INSP_000001', 1, 5.0),
                ('INSP_000002', 1, 10.0)
            AS sample(INSP_ID, REV_NO, minutes)
        """)

        # 2. Delta 형식으로 저장한다. 첫 성공 커밋은 테이블 버전 0이다.
        initial.write.format('delta').mode('errorifexists').save(str(table_path))
        table = DeltaTable.forPath(spark, str(table_path))

        # 3. 한 값을 갱신한다. 업무 개정 REV_NO와 Delta 버전은 별개임을 보인다.
        table.update(
            condition="INSP_ID = 'INSP_000001'",
            set={'INSP_DURATION_MIN': '7.0'},
        )

        # 4. 최신 상태와 버전 0의 상태를 각각 읽는다.
        current = spark.read.format('delta').load(str(table_path))
        previous = (
            spark.read.format('delta').option('versionAsOf', 0).load(str(table_path))
        )
        before = {r['INSP_ID']: r.asDict() for r in previous.collect()}
        after = {r['INSP_ID']: r.asDict() for r in current.collect()}
        history = [
            {'version': r['version'], 'operation': r['operation']}
            for r in table.history().select('version', 'operation').collect()
        ]

        # 최신 값과 과거 값이 구분되고, 갱신 후에도 다른 행이 보존되는지 확인한다.
        checks = {
            'previous_row_count_is_2': len(before) == 2,
            'current_row_count_is_2': len(after) == 2,
            'version_0_preserves_5_minutes': before['INSP_000001']['INSP_DURATION_MIN'] == Decimal('5'),
            'current_has_7_minutes': after['INSP_000001']['INSP_DURATION_MIN'] == Decimal('7'),
            'other_row_unchanged': before['INSP_000002'] == after['INSP_000002'],
            'business_revision_unchanged': after['INSP_000001']['REV_NO'] == 1,
            'table_versions_0_and_1': sorted(r['version'] for r in history) == [0, 1],
            'delta_log_exists': (table_path / '_delta_log').is_dir(),
        }
        result = {
            'lesson': '01_delta_versions',
            'dataset': 'synthetic_learning_sample_2_rows_not_project_data',
            'spark_version': spark.version,
            'delta_spark_version': '4.0.0',
            'python_version': sys.version.split()[0],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'before_version_0': list(before.values()),
            'current_version_1': list(after.values()),
            'history': history,
            'checks': checks,
            'all_passed': all(checks.values()),
        }
        results = root / 'results'
        results.mkdir(exist_ok=True)
        payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        (results / 'lesson-01-latest.json').write_text(payload + '\n', encoding='utf-8')
        print(payload)
        if not result['all_passed']:
            raise RuntimeError('학습용 Delta 저장·갱신·버전 조회 검사에 실패했습니다.')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
