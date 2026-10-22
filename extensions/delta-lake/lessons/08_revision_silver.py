"""검수 결과·필수값·원문 충돌을 검사하고 최신 유효 업무 개정으로 Silver를 만든다.

팀 SQL 37·50·53·54C·55·56 기준. Delta 버전과 REV_NO는 서로 다르다.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window, functions as F, types as T

BUSINESS = ['INSP_ID', 'REV_NO', 'ITEM_STD_CODE', 'ITEM_NM', 'PARENT_CAT_ID', 'TRADE_PRICE_KRW', 'INSP_DURATION_MIN', 'INSP_RESULT', 'INSPECTOR_EMP_NO', 'INBOUND_DT', 'INSP_DONE_DT', 'EXTRACTED_AT', 'INSP_MEMO']
PAYLOAD = [c for c in BUSINESS if c not in {'INSP_ID', 'REV_NO'}]
REQUIRED = [c for c in BUSINESS if c not in {'PARENT_CAT_ID', 'INSP_MEMO'}]
PREVIOUS_CHECKS = ['numeric_rules_passed', 'datetime_rules_passed', 'employee_reference_status', 'category_rules_passed', 'product_name_rules_passed']
RULE = 'silver_v1_sql_37_50_53_54C_55_56'


def revision_number(df):
    # 현재 원본은 1·2다. 새 입력의 잘못된 문자열을 정수로 잘라 맞추지 않는다.
    return df.withColumn('normalized_rev_no', F.when(F.col('REV_NO').rlike(r'\A[0-9]+\z'), F.expr('try_cast(REV_NO as int)')))


def validate_rows(rows, dictionary):
    results = dictionary.filter(F.col('CODE_TYPE') == 'INSP_RESULT')
    if results.count() != results.select('STD_CODE').distinct().count():
        raise ValueError('검수 결과 사전 코드 중복')
    lookup = results.select(F.col('STD_CODE').alias('insp_result_candidate'),
        F.col('BANNED_WORD_YN').alias('_result_banned'),
        F.expr('try_cast(FAKE_PENALTY_MULTIPLIER as decimal(3,1))').alias('penalty_multiplier_candidate'))
    rows = revision_number(rows).join(lookup, F.col('INSP_RESULT') == F.col('insp_result_candidate'), 'left')
    missing = reduce(lambda a, b: a | b, [F.col(c).isNull() | (F.trim(c) == '') for c in REQUIRED])
    return (rows.withColumn('result_check_status', F.when(F.col('insp_result_candidate').isNull(), 'UNMAPPED_RESULT')
        .when(F.col('_result_banned') == 'N', 'REGISTERED_RESULT').otherwise('BANNED_OR_INVALID_RESULT'))
        .withColumn('required_value_missing', missing).drop('_result_banned'))


def group_revisions(rows):
    # JSON struct는 컬럼 이름과 NULL을 포함해 원문 11개 컬럼의 차이를 보존한다.
    return rows.groupBy('normalized_insp_id', 'normalized_rev_no').agg(
        F.count('*').alias('revision_group_rows'),
        F.countDistinct(F.to_json(F.struct(*PAYLOAD), options={'ignoreNullFields': 'false'})).alias('raw_payload_versions'))


def classify(rows, groups):
    group_keys = groups.select(F.col('normalized_insp_id').alias('_group_id'), F.col('normalized_rev_no').alias('_group_rev'), 'revision_group_rows', 'raw_payload_versions')
    rows = rows.join(group_keys, F.col('normalized_insp_id').eqNullSafe(F.col('_group_id')) & F.col('normalized_rev_no').eqNullSafe(F.col('_group_rev')), 'left')
    key_bad = F.col('normalized_insp_id').isNull() | F.col('normalized_rev_no').isNull() | (F.col('normalized_rev_no') < 1)
    check_missing = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in PREVIOUS_CHECKS]) | F.col('raw_payload_versions').isNull()
    bad = (key_bad | ~F.coalesce(F.col('numeric_rules_passed'), F.lit(False))
        | ~F.coalesce(F.col('datetime_rules_passed'), F.lit(False))
        | ~F.coalesce(F.col('employee_reference_status') == 'VALID_REFERENCE', F.lit(False))
        | ~F.coalesce(F.col('category_rules_passed'), F.lit(False))
        | ~F.coalesce(F.col('product_name_rules_passed'), F.lit(False))
        | (F.col('raw_payload_versions') > 1)
        | (F.col('result_check_status') != 'REGISTERED_RESULT') | F.col('required_value_missing'))
    rows = (rows.withColumn('inspection_key_error', key_bad).withColumn('missing_check', check_missing)
        .withColumn('revision_conflict', F.col('raw_payload_versions') > 1)
        .withColumn('record_error', bad).drop('_group_id', '_group_rev'))
    ranked = (rows.filter(~F.col('missing_check') & ~F.col('record_error'))
        .withColumn('valid_occurrence_no', F.row_number().over(Window.partitionBy('normalized_insp_id', 'normalized_rev_no').orderBy('_source_record_number')))
        .withColumn('latest_valid_rev_no', F.max('normalized_rev_no').over(Window.partitionBy('normalized_insp_id')))
        .select('_source_record_id', 'valid_occurrence_no', 'latest_valid_rev_no'))
    rows = rows.join(ranked, '_source_record_id', 'left')
    reason = (F.when(F.col('missing_check'), 'CHECK_RESULT_MISSING').when(F.col('record_error'), 'QUALITY_RULE_VIOLATION')
        .when(F.col('valid_occurrence_no') > 1, 'IDENTICAL_DUPLICATE')
        .when(F.col('normalized_rev_no') < F.col('latest_valid_rev_no'), 'SUPERSEDED_VALID_REVISION')
        .otherwise('LATEST_VALID_REVISION'))
    return (rows.withColumn('primary_reason', reason)
        .withColumn('disposition', F.when(F.col('missing_check'), 'CHECK_MISSING').when(F.col('record_error'), 'QUARANTINE')
            .when(F.col('primary_reason').isin('IDENTICAL_DUPLICATE', 'SUPERSEDED_VALID_REVISION'), 'EXCLUDED').otherwise('SILVER'))
        .withColumn('_silver_rule_version', F.lit(RULE)))


def project_silver(rows):
    return rows.select(
        F.col('normalized_insp_id').alias('INSP_ID'), F.col('normalized_rev_no').alias('REV_NO'), 'ITEM_STD_CODE',
        F.col('standard_item_name_candidate').alias('ITEM_NM'), F.col('effective_parent_cat_id').alias('PARENT_CAT_ID'),
        F.col('price_krw_candidate').cast('decimal(18,4)').alias('TRADE_PRICE_KRW'),
        F.col('duration_min_candidate').cast('decimal(12,8)').alias('INSP_DURATION_MIN'),
        F.col('insp_result_candidate').alias('INSP_RESULT'), F.col('normalized_inspector_emp_no').alias('INSPECTOR_EMP_NO'),
        F.col('inbound_dt_kst').alias('INBOUND_DT'), F.col('insp_done_dt_kst').alias('INSP_DONE_DT'),
        F.col('extracted_at_kst').alias('EXTRACTED_AT'), 'INSP_MEMO',
        *[c for c in rows.columns if c.startswith('_source_') or c in {'_ingest_batch_id', '_ingested_at_utc'}],
        F.col('_source_record_number').alias('source_bronze_row_id'), F.lit(RULE).alias('rule_version'))


def independent_python(previous, dictionary):
    result_codes = {r['STD_CODE']: r for r in dictionary.filter(F.col('CODE_TYPE') == 'INSP_RESULT').collect()}
    groups = defaultdict(set)
    group_counts = Counter()
    records = {}
    fields = list(dict.fromkeys(BUSINESS + ['normalized_insp_id', '_source_record_number'] + PREVIOUS_CHECKS))
    for row in previous.select(*fields).toLocalIterator():
        raw_rev = row['REV_NO']
        rev = int(raw_rev) if raw_rev is not None and re.fullmatch('[0-9]+', raw_rev) and int(raw_rev) <= 2147483647 else None
        key = (row['normalized_insp_id'], rev)
        # Spark는 원문을 직접 비교하고 Python 대조에서는 같은 원문 튜플의 SHA-256을 사용한다.
        digest = hashlib.sha256(json.dumps(tuple(row[c] for c in PAYLOAD), ensure_ascii=True).encode()).digest()
        groups[key].add(digest)
        group_counts[key] += 1
        d = result_codes.get(row['INSP_RESULT'])
        result_status = 'UNMAPPED_RESULT' if d is None else 'REGISTERED_RESULT' if d['BANNED_WORD_YN'] == 'N' else 'BANNED_OR_INVALID_RESULT'
        required = any(row[c] is None or row[c].strip(' ') == '' for c in REQUIRED)
        missing = any(row[c] is None for c in PREVIOUS_CHECKS)
        key_bad = key[0] is None or rev is None or rev < 1
        bad_without_conflict = key_bad or not row['numeric_rules_passed'] or not row['datetime_rules_passed'] or row['employee_reference_status'] != 'VALID_REFERENCE' or not row['category_rules_passed'] or not row['product_name_rules_passed'] or result_status != 'REGISTERED_RESULT' or required
        records[row['_source_record_number']] = (key, result_status, d['STD_CODE'] if d else None, Decimal(d['FAKE_PENALTY_MULTIPLIER']) if d else None, required, missing, key_bad, bool(bad_without_conflict))
    eligible = defaultdict(list)
    latest = {}
    for number, (key, _, _, _, _, missing, _, bad) in records.items():
        if not missing and not bad and len(groups[key]) == 1:
            eligible[key].append(number)
            latest[key[0]] = max(latest.get(key[0], 0), key[1])
    ranks = {number: i + 1 for numbers in eligible.values() for i, number in enumerate(sorted(numbers))}
    expected = {}
    for number, (key, result_status, candidate, multiplier, required, missing, key_bad, bad) in records.items():
        conflict = len(groups[key]) > 1
        error = bad or conflict
        occurrence = ranks.get(number)
        latest_rev = latest.get(key[0]) if occurrence is not None else None
        if missing:
            disposition, reason = 'CHECK_MISSING', 'CHECK_RESULT_MISSING'
        elif error:
            disposition, reason = 'QUARANTINE', 'QUALITY_RULE_VIOLATION'
        elif occurrence > 1:
            disposition, reason = 'EXCLUDED', 'IDENTICAL_DUPLICATE'
        elif key[1] < latest_rev:
            disposition, reason = 'EXCLUDED', 'SUPERSEDED_VALID_REVISION'
        else:
            disposition, reason = 'SILVER', 'LATEST_VALID_REVISION'
        expected[number] = (key[1], result_status, candidate, multiplier, required, key_bad, missing, conflict, error, group_counts[key], len(groups[key]), occurrence, latest_rev, disposition, reason)
    return expected


def main():
    root = Path(__file__).resolve().parents[1]
    latest = json.loads((root / 'results/lesson-07-latest.json').read_text(encoding='utf-8'))
    if not latest.get('all_passed'):
        raise ValueError('먼저 실습 07을 통과해야 합니다.')
    for key in ['run_id', 'bronze_run_id']:
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', latest[key]):
            raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-07' / latest['run_id'] / 'inspection_category_candidates'
    bronze = runtime / 'lesson-02' / latest['bronze_run_id'] / 'bronze'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-08' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-08-Silver').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        def read(path):
            return spark.read.format('delta').option('versionAsOf', 0).load(str(path))
        previous = read(source)
        dictionary = read(bronze / 'bronze_product_std_dict')
        checked = validate_rows(previous, dictionary)
        groups = group_revisions(checked)
        groups.write.format('delta').mode('errorifexists').save(str(run / 'revision_groups'))
        classify(checked, read(run / 'revision_groups')).write.format('delta').mode('errorifexists').save(str(run / 'inspection_disposition_stage'))
        saved = read(run / 'inspection_disposition_stage')
        selected = saved.filter(F.col('disposition') == 'SILVER')
        # 저장 범위·정밀도 검증을 통과하기 전에 작은 DECIMAL 타입으로 쓰지 않는다.
        storage_bad = ((F.col('price_krw_candidate') < 0) | (F.col('price_krw_candidate') > F.lit(Decimal('99999999999999.9999')))
            | (F.col('duration_min_candidate') < 0) | (F.col('duration_min_candidate') > F.lit(Decimal('9999.99999999'))))
        limits = {'normalized_insp_id': 20, 'ITEM_STD_CODE': 50, 'standard_item_name_candidate': 150, 'effective_parent_cat_id': 20, 'insp_result_candidate': 50, 'normalized_inspector_emp_no': 20}
        for name, length in limits.items():
            storage_bad = storage_bad | F.col(name).isNull() | (F.length(name) > length)
        for name in ['price_krw_candidate', 'duration_min_candidate', 'inbound_dt_kst', 'insp_done_dt_kst', 'extracted_at_kst']:
            storage_bad = storage_bad | F.col(name).isNull()
        storage_errors = selected.filter(storage_bad).count()
        if storage_errors:
            raise ValueError(f'Silver 저장 범위 검사 실패: {storage_errors}행')
        project_silver(selected).write.format('delta').mode('errorifexists').save(str(run / 'silver_inspection'))
        for disposition, name in [('QUARANTINE', 'quarantine_inspection'), ('EXCLUDED', 'excluded_inspection'), ('CHECK_MISSING', 'missing_check_inspection')]:
            saved.filter(F.col('disposition') == disposition).write.format('delta').mode('errorifexists').save(str(run / name))
        silver = read(run / 'silver_inspection')
        expected = independent_python(previous, dictionary)
        fields = ['normalized_rev_no', 'result_check_status', 'insp_result_candidate', 'penalty_multiplier_candidate', 'required_value_missing', 'inspection_key_error', 'missing_check', 'revision_conflict', 'record_error', 'revision_group_rows', 'raw_payload_versions', 'valid_occurrence_no', 'latest_valid_rev_no', 'disposition', 'primary_reason']
        mismatches, row_count = 0, 0
        for row in saved.select('_source_record_number', *fields).toLocalIterator():
            row_count += 1
            mismatches += tuple(row[k] for k in fields) != expected[row['_source_record_number']]
        counts = {r['disposition']: r['count'] for r in saved.groupBy('disposition').count().collect()}
        reasons = {r['primary_reason']: r['count'] for r in saved.groupBy('primary_reason').count().collect()}
        group_counts = {r['status']: {'key_groups': r['groups'], 'source_rows': r['rows']} for r in groups.withColumn('status', F.when(F.col('raw_payload_versions') > 1, 'REVISION_CONFLICT').when(F.col('revision_group_rows') > 1, 'IDENTICAL_DUPLICATE').otherwise('SINGLE_RECORD')).groupBy('status').agg(F.count('*').alias('groups'), F.sum('revision_group_rows').alias('rows')).collect()}
        # 가상 학습 사례만 출력하며 실제 원문 행이나 개인정보는 출력하지 않는다.
        fixture_rows = []
        def add(number, ident, rev='1', price='100', numeric=True, result='PASS', extracted='2024-01-01 10:00:00', required_name='Model A', prior=True):
            row = dict.fromkeys(BUSINESS, 'value')
            row.update(INSP_ID=ident, REV_NO=rev, ITEM_STD_CODE='ITEM_A', ITEM_NM=required_name, PARENT_CAT_ID='CAT_00001', TRADE_PRICE_KRW=price, INSP_DURATION_MIN='5', INSP_RESULT=result, INSPECTOR_EMP_NO='KRM000001', INBOUND_DT='2024-01-01 09:00:00', INSP_DONE_DT='2024-01-01 09:05:00', EXTRACTED_AT=extracted, INSP_MEMO=None)
            row.update(normalized_insp_id=ident, _source_record_id='synthetic:' + str(number), _source_record_number=number,
                numeric_rules_passed=numeric, datetime_rules_passed=True, employee_reference_status='VALID_REFERENCE', category_rules_passed=True, product_name_rules_passed=prior)
            fixture_rows.append(row)
        add(1, 'INSP_000001'); add(2, 'INSP_000001')
        add(3, 'INSP_000002', price='100'); add(4, 'INSP_000002', price='200')
        add(5, 'INSP_000003', extracted='2024-02-01 10:00:00'); add(6, 'INSP_000003', rev='2')
        add(7, 'INSP_000004'); add(8, 'INSP_000004', rev='2', numeric=False)
        add(9, 'INSP_000005', rev='0'); add(10, 'INSP_000006', rev='1.5')
        add(11, 'INSP_000007', result='UNKNOWN'); add(12, 'INSP_000008', required_name=' ')
        add(13, 'INSP_000009', prior=None)
        add(14, 'INSP_000010'); add(15, 'INSP_000010', extracted='2024-01-01 10:00:01')
        fixture_path = run / 'synthetic_rows.jsonl'
        fixture_path.write_text(''.join(json.dumps(r, ensure_ascii=True) + '\n' for r in fixture_rows), encoding='utf-8')
        schema = T.StructType([T.StructField(c, T.StringType()) for c in BUSINESS + ['normalized_insp_id', '_source_record_id', 'employee_reference_status']] + [T.StructField('_source_record_number', T.LongType())] + [T.StructField(c, T.BooleanType()) for c in PREVIOUS_CHECKS if c != 'employee_reference_status'])
        fixture = spark.read.schema(schema).json(str(fixture_path))
        fd = spark.sql("SELECT 'INSP_RESULT' AS CODE_TYPE, 'PASS' AS STD_CODE, 'N' AS BANNED_WORD_YN, '1.0' AS FAKE_PENALTY_MULTIPLIER")
        fc = validate_rows(fixture, fd)
        sample_rows = classify(fc, group_revisions(fc)).select('_source_record_number', 'disposition', 'primary_reason').collect()
        fixed = {1: ('SILVER', 'LATEST_VALID_REVISION'), 2: ('EXCLUDED', 'IDENTICAL_DUPLICATE'),
            3: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'), 4: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'),
            5: ('EXCLUDED', 'SUPERSEDED_VALID_REVISION'), 6: ('SILVER', 'LATEST_VALID_REVISION'),
            7: ('SILVER', 'LATEST_VALID_REVISION'), 8: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'),
            9: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'), 10: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'),
            11: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'), 12: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'),
            13: ('CHECK_MISSING', 'CHECK_RESULT_MISSING'), 14: ('QUARANTINE', 'QUALITY_RULE_VIOLATION'), 15: ('QUARANTINE', 'QUALITY_RULE_VIOLATION')}
        boundary = {'boundary_row_' + str(r['_source_record_number']): (r['disposition'], r['primary_reason']) == fixed[r['_source_record_number']] for r in sample_rows}
        partition_union = silver.select('_source_record_id')
        for name in ['quarantine_inspection', 'excluded_inspection', 'missing_check_inspection']:
            partition_union = partition_union.unionByName(read(run / name).select('_source_record_id'))
        checks = {
            '335000_rows_preserved': row_count == saved.count() == 335000,
            'upstream_columns_preserved': previous.exceptAll(saved.select(previous.columns)).count() == 0 and saved.select(previous.columns).exceptAll(previous).count() == 0,
            'all_classifications_match_python': mismatches == 0 and len(expected) == row_count,
            'counts_match_saved_team_final': counts == {'SILVER': 271787, 'QUARANTINE': 33854, 'EXCLUDED': 29359},
            'exclusion_reasons_match_saved_final': reasons.get('IDENTICAL_DUPLICATE') == 9598 and reasons.get('SUPERSEDED_VALID_REVISION') == 19761,
            'silver_one_row_per_inspection': silver.count() == silver.select('INSP_ID').distinct().count() == 271787,
            'silver_source_ids_unique': silver.select('_source_record_id').distinct().count() == 271787,
            'silver_no_rule_errors_or_missing_checks': selected.filter(F.col('record_error') | F.col('missing_check')).count() == 0,
            'silver_is_first_latest_valid_revision': selected.filter((F.col('valid_occurrence_no') != 1) | (F.col('normalized_rev_no') != F.col('latest_valid_rev_no'))).count() == 0,
            'silver_storage_errors_zero': storage_errors == 0,
            'silver_values_match_selected_stage': silver.exceptAll(project_silver(selected)).count() == 0 and project_silver(selected).exceptAll(silver).count() == 0,
            'silver_money_precision': silver.schema['TRADE_PRICE_KRW'].dataType == T.DecimalType(18, 4),
            'silver_duration_precision': silver.schema['INSP_DURATION_MIN'].dataType == T.DecimalType(12, 8),
            'all_source_rows_assigned_once': partition_union.count() == partition_union.distinct().count() == 335000 and previous.select('_source_record_id').exceptAll(partition_union).count() == 0,
            'silver_time_order': silver.filter((F.col('INBOUND_DT') > F.col('INSP_DONE_DT')) | (F.col('INSP_DONE_DT') > F.col('EXTRACTED_AT'))).count() == 0,
            'silver_category_reference': silver.select('PARENT_CAT_ID').distinct().join(read(runtime / 'lesson-07' / latest['run_id'] / 'valid_category_master').select(F.col('normalized_cat_id').alias('PARENT_CAT_ID')), 'PARENT_CAT_ID', 'left_anti').count() == 0,
            'silver_employee_matches_validated_master_key': selected.filter(~F.col('normalized_inspector_emp_no').eqNullSafe(F.col('_master_emp_no'))).count() == 0,
            'upstream_not_modified': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'all_boundary_rows_present': len(sample_rows) == len(fixed) == len(boundary),
            **boundary,
        }
        result = {'lesson': '08_revision_silver', 'run_id': run_id, 'category_run_id': latest['run_id'], 'bronze_run_id': latest['bronze_run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'scope': 'initial_snapshot_silver_not_incremental_or_gold',
            'rows': row_count, 'disposition_counts': counts, 'primary_reason_counts': reasons, 'revision_group_counts': group_counts,
            'python_reference_mismatch_rows': mismatches, 'boundary_rows': len(fixed),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-08-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('학습용 가상 개정 사례:', flush=True)
        for r in sorted(sample_rows, key=lambda r: r['_source_record_number']):
            print(r['_source_record_number'], r['disposition'], r['primary_reason'], flush=True)
        if not result['all_passed']:
            raise RuntimeError('최종 분류 검사 실패: results/lesson-08-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
