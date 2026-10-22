"""숫자 후보 테이블에 KST 시각 후보와 시간 순서 검사를 추가한다.

원본·상위 Delta 테이블은 수정하지 않는다. 최종 Silver·격리 분류는 아니다.
KST 벽시계 값을 TimestampNTZ로 저장해 MySQL DATETIME의 비교 기준에 맞춘다.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, types as T

FIELDS = [
    ('INBOUND_DT', 'inbound_dt_kst'),
    ('INSP_DONE_DT', 'insp_done_dt_kst'),
    ('EXTRACTED_AT', 'extracted_at_kst'),
]
# (허용 정규식, Spark 형식, Python 형식, UTC 여부)
FORMATS = [
    (r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z', "yyyy-MM-dd'T'HH:mm:ss'Z'", '%Y-%m-%dT%H:%M:%SZ', True),
    (r'[0-9]{14}', 'yyyyMMddHHmmss', '%Y%m%d%H%M%S', False),
    (r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 'yyyy-MM-dd HH:mm:ss', '%Y-%m-%d %H:%M:%S', False),
    (r'[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 'yyyy/MM/dd HH:mm:ss', '%Y/%m/%d %H:%M:%S', False),
    (r'[0-9]{4}\.[0-9]{2}\.[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 'yyyy.MM.dd HH:mm:ss', '%Y.%m.%d %H:%M:%S', False),
]
NULL_TOKENS = ['', 'NULL', 'NONE', 'NA', 'N/A', 'NAN', '-']


def datetime_candidates(df):
    result = df
    for raw_name, candidate_name in FIELDS:
        raw = F.col(raw_name)
        candidate = F.lit(None).cast('timestamp_ntz')
        known_format = F.lit(False)
        for pattern, spark_format, _, is_utc in reversed(FORMATS):
            matches = F.coalesce(raw.rlike(r'\A(?:' + pattern + r')\z'), F.lit(False))
            parsed = F.try_to_timestamp(raw, F.lit(spark_format))
            # 세션 timestampType을 NTZ로 지정했다. Z를 읽은 경우에만 KST로 9시간 이동.
            if is_utc:
                parsed = parsed + F.expr('INTERVAL 9 HOURS')
            candidate = F.when(matches, parsed).otherwise(candidate)
            known_format = known_format | matches
        result = result.withColumn(candidate_name, candidate).withColumn(
            candidate_name + '_status',
            F.when(F.upper(F.trim(F.coalesce(raw, F.lit('')))).isin(NULL_TOKENS), F.lit('MISSING_TIME'))
            .when(~known_format, F.lit('UNSUPPORTED_FORMAT'))
            .when(F.col(candidate_name).isNull(), F.lit('INVALID_DATE'))
            .otherwise(F.lit('CONVERTED')),
        )
    converted = F.lit(True)
    for _, name in FIELDS:
        converted = converted & (F.col(name + '_status') == 'CONVERTED')
    result = (
        result.withColumn('done_before_inbound', F.coalesce(F.col('insp_done_dt_kst') < F.col('inbound_dt_kst'), F.lit(False)))
        .withColumn('extracted_before_done', F.coalesce(F.col('extracted_at_kst') < F.col('insp_done_dt_kst'), F.lit(False)))
    )
    return (
        result.withColumn('datetime_rules_passed', converted & ~F.col('done_before_inbound') & ~F.col('extracted_before_done'))
        .withColumn('datetime_status',
            F.when(~converted, F.lit('UNPARSEABLE_TIME'))
            .when(F.col('done_before_inbound') & F.col('extracted_before_done'), F.lit('BOTH_TIME_ORDER_ERRORS'))
            .when(F.col('done_before_inbound'), F.lit('DONE_BEFORE_INBOUND'))
            .when(F.col('extracted_before_done'), F.lit('EXTRACTED_BEFORE_DONE'))
            .otherwise(F.lit('VALID_DATETIME')))
        .withColumn('_datetime_rule_version', F.lit('datetime_v1_z_plus9_unmarked_kst'))
    )


def reference_time(raw):
    if raw is None or raw.strip(' ').upper() in NULL_TOKENS:
        return None, 'MISSING_TIME'
    for pattern, _, python_format, is_utc in FORMATS:
        if re.fullmatch(pattern, raw):
            try:
                value = datetime.strptime(raw, python_format)
                if is_utc:
                    value += timedelta(hours=9)
                return value, 'CONVERTED'
            except (ValueError, OverflowError):
                return None, 'INVALID_DATE'
    return None, 'UNSUPPORTED_FORMAT'


def main():
    root = Path(__file__).resolve().parents[1]
    upstream = json.loads((root / 'results/lesson-03-latest.json').read_text(encoding='utf-8'))
    if not upstream.get('all_passed'):
        raise ValueError('먼저 실습 03의 숫자 후보 검사를 통과해야 합니다.')
    source_run_id = upstream['run_id']
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', source_run_id):
        raise ValueError('잘못된 숫자 후보 run_id')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-03' / source_run_id / 'inspection_numeric_candidates'
    if not (source / '_delta_log').is_dir():
        raise FileNotFoundError('숫자 후보 테이블이 없습니다. 기존 실습과 같은 런타임 경로를 사용하세요.')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-04' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (
        SparkSession.builder.appName('KREAM-Delta-Lesson-04-Datetime').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy'))
        .config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        previous = spark.read.format('delta').option('versionAsOf', 0).load(str(source))
        target = run / 'inspection_numeric_datetime_candidates'
        datetime_candidates(previous).write.format('delta').mode('errorifexists').save(str(target))
        saved = spark.read.format('delta').load(str(target))
        status_counts = {r['datetime_status']: r['count'] for r in saved.groupBy('datetime_status').count().collect()}
        metrics = saved.agg(
            F.count('*').alias('rows'),
            F.sum(F.when(F.col('datetime_status') == 'UNPARSEABLE_TIME', 1).otherwise(0)).alias('unparseable_rows'),
            F.sum(F.col('done_before_inbound').cast('int')).alias('done_before_inbound'),
            F.sum(F.col('extracted_before_done').cast('int')).alias('extracted_before_done'),
            F.sum((~F.col('datetime_rules_passed')).cast('int')).alias('datetime_error_unique'),
            F.sum((F.col('numeric_rules_passed') & F.col('datetime_rules_passed')).cast('int')).alias('numeric_and_datetime_passed'),
        ).first().asDict()
        selected = [raw for raw, _ in FIELDS] + [name for _, name in FIELDS] + [name + '_status' for _, name in FIELDS]
        oracle_rows, mismatches = 0, 0
        for row in saved.select(*selected).toLocalIterator():
            oracle_rows += 1
            mismatches += any((row[name], row[name + '_status']) != reference_time(row[raw]) for raw, name in FIELDS)

        with (root.parents[1] / 'evidence/asis_business_diagnosis.csv').open(encoding='utf-8-sig', newline='') as stream:
            baseline = {r['issue']: int(r['rows']) for r in csv.DictReader(stream) if r['column_name'] == 'DATETIME' and r['source_file'] == 'kream_inspection_300000.csv'}

        # 학습용 입력과 고정 기대값: 실제 원본 행으로 표현하지 않는다.
        # (이름, 입고, 완료, 추출, 기대 KST 시각 3개, 기대 행 상태)
        cases = [
            ('utc_day_rollover', '2024-01-01T18:00:00Z', '2024-01-02 03:00:00', '20240102040000', ('2024-01-02 03:00:00', '2024-01-02 03:00:00', '2024-01-02 04:00:00'), 'VALID_DATETIME'),
            ('mixed_formats', '20240229100000', '2024/02/29 10:05:00', '2024.02.29 10:06:00', ('2024-02-29 10:00:00', '2024-02-29 10:05:00', '2024-02-29 10:06:00'), 'VALID_DATETIME'),
            ('invalid_calendar', '2023-02-29 10:00:00', '2023-03-01 10:05:00', '2023-03-01 10:06:00', (None, '2023-03-01 10:05:00', '2023-03-01 10:06:00'), 'UNPARSEABLE_TIME'),
            ('missing', '', '2024-01-01 10:00:00', '2024-01-01 11:00:00', (None, '2024-01-01 10:00:00', '2024-01-01 11:00:00'), 'UNPARSEABLE_TIME'),
            ('done_before', '2024-01-01 11:00:00', '2024-01-01 10:00:00', '2024-01-01 12:00:00', ('2024-01-01 11:00:00', '2024-01-01 10:00:00', '2024-01-01 12:00:00'), 'DONE_BEFORE_INBOUND'),
            ('extracted_before', '2024-01-01 10:00:00', '2024-01-01 11:00:00', '2024-01-01 10:30:00', ('2024-01-01 10:00:00', '2024-01-01 11:00:00', '2024-01-01 10:30:00'), 'EXTRACTED_BEFORE_DONE'),
            ('both_order_errors', '2024-01-01 11:00:00', '2024-01-01 10:00:00', '2024-01-01 09:00:00', ('2024-01-01 11:00:00', '2024-01-01 10:00:00', '2024-01-01 09:00:00'), 'BOTH_TIME_ORDER_ERRORS'),
            ('equal_allowed', '2024-01-01 10:00:00', '2024-01-01 10:00:00', '2024-01-01 10:00:00', ('2024-01-01 10:00:00',) * 3, 'VALID_DATETIME'),
            ('offset_not_supported', '2024-01-01T00:00:00+00:00', '2024-01-01 10:00:00', '2024-01-01 11:00:00', (None, '2024-01-01 10:00:00', '2024-01-01 11:00:00'), 'UNPARSEABLE_TIME'),
            ('no_double_shift', '2024-01-01 09:00:00', '2024-01-01T00:00:00Z', '2024-01-01 10:00:00', ('2024-01-01 09:00:00', '2024-01-01 09:00:00', '2024-01-01 10:00:00'), 'VALID_DATETIME'),
        ]
        case_path = run / 'boundary_cases.jsonl'
        case_path.write_text(''.join(json.dumps({'case_id': c[0], **{raw: c[i + 1] for i, (raw, _) in enumerate(FIELDS)}}, ensure_ascii=True) + '\n' for c in cases), encoding='utf-8')
        case_input = spark.read.schema('case_id string, INBOUND_DT string, INSP_DONE_DT string, EXTRACTED_AT string').json(str(case_path))
        case_output = datetime_candidates(case_input)
        expected = {c[0]: (c[4], c[5]) for c in cases}
        case_checks = {}
        for row in case_output.collect():
            values = tuple(row[name].strftime('%Y-%m-%d %H:%M:%S') if row[name] is not None else None for _, name in FIELDS)
            case_checks['boundary_' + row['case_id']] = (values, row['datetime_status']) == expected[row['case_id']]
        checks = {
            'all_335000_rows_retained': metrics['rows'] == 335000,
            'upstream_columns_and_metadata_retained': previous.exceptAll(saved.select(previous.columns)).count() == 0 and saved.select(previous.columns).exceptAll(previous).count() == 0,
            'kst_candidates_use_timestamp_ntz': all(isinstance(saved.schema[name].dataType, T.TimestampNTZType) for _, name in FIELDS),
            'unparseable_count_matches_baseline': metrics['unparseable_rows'] == baseline['UNPARSEABLE_TIME'],
            'done_before_count_matches_baseline': metrics['done_before_inbound'] == baseline['DONE_BEFORE_INBOUND'],
            'extracted_before_count_matches_baseline': metrics['extracted_before_done'] == baseline['EXTRACTED_BEFORE_DONE'],
            'unique_datetime_errors_match_baseline': metrics['datetime_error_unique'] == baseline['TIME_ORDER_ERROR_UNIQUE'],
            'all_rows_match_python_datetime_reference': oracle_rows == 335000 and mismatches == 0,
            'upstream_not_modified': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'all_boundary_cases_present': len(case_checks) == len(cases),
            'candidate_delta_log_exists': (target / '_delta_log').is_dir(),
            **case_checks,
        }
        result = {
            'lesson': '04_datetime_candidates', 'run_id': run_id, 'numeric_run_id': source_run_id,
            'bronze_run_id': upstream['bronze_run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'numeric_and_datetime_candidates_not_final_silver_or_quarantine',
            'timestamp_semantics': 'KST_wall_clock_as_timestamp_ntz',
            'datetime_status_counts': status_counts, 'metrics': metrics,
            'python_reference_mismatch_rows': mismatches, 'boundary_cases': len(cases),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values()),
        }
        (root / 'results/lesson-04-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('\n학습용 날짜 변환 사례 (실제 원본의 행이 아님):', flush=True)
        case_output.select('case_id', 'inbound_dt_kst', 'insp_done_dt_kst', 'extracted_at_kst', 'datetime_status').orderBy('case_id').show(20, truncate=False)
        if not result['all_passed']:
            raise RuntimeError('날짜 후보 검사 실패: results/lesson-04-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
