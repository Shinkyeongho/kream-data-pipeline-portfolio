"""팀 SQL 57·66·67 기준으로 최종 결과와 유효 개정 기록의 일별 Gold를 분리한다.

완료일 KST 기준, 고정 스냅샷 전체 재집계. Gold 증분 갱신·지급 사실 증명은 아니다.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
getcontext().prec = 50

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

RULE = 'gold_daily_v1_sql_57_66_67'
RESULTS = {'PASS': 'pass_rows', 'FAKE_REJECT': 'fake_reject_rows', 'BOX_DMG_HOLD': 'box_damage_hold_rows', 'CLAIM_FAKE_PASS': 'fake_pass_claim_rows'}


def period(column):
    return (F.when(column < F.lit('2022-01-01').cast('date'), 'ISOLATED_EARLY_PERIOD')
        .when(column >= F.lit('2026-10-01').cast('date'), 'REINSPECTION_BATCH_PERIOD')
        .when(column >= F.lit('2026-09-01').cast('date'), 'PARTIAL_BOUNDARY_MONTH')
        .otherwise('CONTINUOUS_PERIOD'))


def python_period(day):
    return 'ISOLATED_EARLY_PERIOD' if day < date(2022, 1, 1) else 'REINSPECTION_BATCH_PERIOD' if day >= date(2026, 10, 1) else 'PARTIAL_BOUNDARY_MONTH' if day >= date(2026, 9, 1) else 'CONTINUOUS_PERIOD'


def daily_results(current):
    base = (current.withColumn('done_date_kst', F.to_date('INSP_DONE_DT'))
        .withColumn('analysis_period', period(F.col('done_date_kst')))
        .withColumn('_compensation', F.when(F.col('INSP_RESULT') == 'CLAIM_FAKE_PASS', F.col('TRADE_PRICE_KRW') * F.lit(3)).otherwise(F.lit(0)).cast('decimal(19,4)')))
    return (base.groupBy('done_date_kst', 'analysis_period').agg(F.count('*').alias('inspection_rows'),
        *[F.sum(F.when(F.col('INSP_RESULT') == code, 1).otherwise(0)).alias(name) for code, name in RESULTS.items()],
        F.sum('TRADE_PRICE_KRW').cast('decimal(24,4)').alias('total_trade_price_krw'),
        F.sum('_compensation').cast('decimal(24,4)').alias('compensation_loss_krw'))
        .withColumn('rule_version', F.lit(RULE)))


def daily_workload(events):
    return (events.withColumn('done_date_kst', F.to_date('INSP_DONE_DT'))
        .withColumn('analysis_period', period(F.col('done_date_kst')))
        .withColumn('event_type', F.when(F.col('REV_NO') == 1, 'FIRST_INSPECTION').otherwise('REINSPECTION'))
        .groupBy('done_date_kst', 'event_type', 'analysis_period').agg(F.count('*').alias('inspection_event_rows'),
            F.countDistinct('INSPECTOR_EMP_NO').alias('observed_inspector_ids'),
            F.sum('INSP_DURATION_MIN').cast('decimal(20,8)').alias('total_work_minutes'),
            F.round(F.avg('INSP_DURATION_MIN'), 4).cast('decimal(12,4)').alias('avg_work_minutes'))
        .withColumn('rule_version', F.lit(RULE)))


def independent_results(current):
    grouped = defaultdict(lambda: {'count': 0, 'results': defaultdict(int), 'price': Decimal(0), 'compensation': Decimal(0)})
    for row in current.select('INSP_DONE_DT', 'INSP_RESULT', 'TRADE_PRICE_KRW').toLocalIterator():
        day = row['INSP_DONE_DT'].date()
        value = grouped[(day, python_period(day))]
        value['count'] += 1
        value['results'][row['INSP_RESULT']] += 1
        value['price'] += row['TRADE_PRICE_KRW']
        if row['INSP_RESULT'] == 'CLAIM_FAKE_PASS':
            value['compensation'] += row['TRADE_PRICE_KRW'] * 3
    return {key: (v['count'], *(v['results'][code] for code in RESULTS), v['price'], v['compensation']) for key, v in grouped.items()}


def independent_workload(events):
    grouped = defaultdict(lambda: {'count': 0, 'ids': set(), 'minutes': Decimal(0)})
    for row in events.select('INSP_DONE_DT', 'REV_NO', 'INSPECTOR_EMP_NO', 'INSP_DURATION_MIN').toLocalIterator():
        day = row['INSP_DONE_DT'].date()
        event = 'FIRST_INSPECTION' if row['REV_NO'] == 1 else 'REINSPECTION'
        value = grouped[(day, event, python_period(day))]
        value['count'] += 1
        value['ids'].add(row['INSPECTOR_EMP_NO'])
        value['minutes'] += row['INSP_DURATION_MIN']
    return {key: (v['count'], len(v['ids']), v['minutes'], (v['minutes'] / v['count']).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)) for key, v in grouped.items()}


def main():
    root = Path(__file__).resolve().parents[1]
    previous = json.loads((root / 'results/lesson-11-latest.json').read_text(encoding='utf-8'))
    if not previous.get('all_passed'):
        raise ValueError('먼저 실습 11을 통과해야 합니다.')
    for key in ['run_id', 'source_silver_run_id']:
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', previous[key]):
            raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    current_path = runtime / 'lesson-11' / previous['run_id'] / 'replayed_silver_inspection'
    stage_path = runtime / 'lesson-08' / previous['source_silver_run_id'] / 'inspection_disposition_stage'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-12' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-12-Gold-Daily').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        current_version = DeltaTable.forPath(spark, str(current_path)).history(1).first()['version']
        stage_version_before = DeltaTable.forPath(spark, str(stage_path)).history(1).first()['version']
        def read(path, ver=None):
            reader = spark.read.format('delta')
            return (reader.option('versionAsOf', ver) if ver is not None else reader).load(str(path))
        current = read(current_path, current_version)
        stage = read(stage_path, 0)
        events = stage.filter(~F.col('missing_check') & ~F.col('record_error') & (F.col('valid_occurrence_no') == 1)).select(
            F.col('normalized_insp_id').alias('INSP_ID'), F.col('normalized_rev_no').alias('REV_NO'),
            F.col('normalized_inspector_emp_no').alias('INSPECTOR_EMP_NO'),
            F.col('duration_min_candidate').alias('INSP_DURATION_MIN'), F.col('insp_done_dt_kst').alias('INSP_DONE_DT'))
        daily_results(current).write.format('delta').mode('errorifexists').save(str(run / 'gold_inspection_result_daily'))
        daily_workload(events).write.format('delta').mode('errorifexists').save(str(run / 'gold_inspection_workload_daily'))
        result_daily, workload_daily = read(run / 'gold_inspection_result_daily'), read(run / 'gold_inspection_workload_daily')
        print('두 Gold 테이블 저장 완료. 일별 집계를 Python Decimal 계산과 비교합니다.', flush=True)
        result_fields = ['inspection_rows', *RESULTS.values(), 'total_trade_price_krw', 'compensation_loss_krw']
        work_fields = ['inspection_event_rows', 'observed_inspector_ids', 'total_work_minutes', 'avg_work_minutes']
        actual_result = {(r['done_date_kst'], r['analysis_period']): tuple(r[k] for k in result_fields) for r in result_daily.collect()}
        actual_work = {(r['done_date_kst'], r['event_type'], r['analysis_period']): tuple(r[k] for k in work_fields) for r in workload_daily.collect()}
        py_result, py_work = independent_results(current), independent_workload(events)
        current_stats = current.agg(F.count('*').alias('rows'), F.sum('TRADE_PRICE_KRW').alias('price'),
            F.sum(F.when(F.col('INSP_RESULT') == 'CLAIM_FAKE_PASS', F.col('TRADE_PRICE_KRW') * 3).otherwise(0)).alias('compensation'),
            F.min(F.to_date('INSP_DONE_DT')).alias('first_date'), F.max(F.to_date('INSP_DONE_DT')).alias('last_date')).first().asDict()
        event_stats = events.agg(F.count('*').alias('rows'), F.sum('INSP_DURATION_MIN').alias('minutes'),
            F.min(F.to_date('INSP_DONE_DT')).alias('first_date'), F.max(F.to_date('INSP_DONE_DT')).alias('last_date')).first().asDict()
        result_totals = result_daily.agg(*[F.sum(c).alias(c) for c in result_fields]).first().asDict()
        work_totals = workload_daily.agg(F.sum('inspection_event_rows').alias('rows'), F.sum('total_work_minutes').alias('minutes')).first().asDict()
        known_results = current.select('INSP_RESULT').distinct().count() == current.filter(F.col('INSP_RESULT').isin(list(RESULTS))).select('INSP_RESULT').distinct().count()
        # 가상 날짜 경계와 누락 날짜를 확인한다. 실제 업무 값은 출력하지 않는다.
        fixture = spark.sql("SELECT * FROM VALUES (DATE '2021-12-31'), (DATE '2022-01-01'), (DATE '2026-08-31'), (DATE '2026-09-01'), (DATE '2026-10-01') AS x(day)")
        boundary_actual = {r['day']: r['period'] for r in fixture.select('day', period(F.col('day')).alias('period')).collect()}
        sparse = spark.sql("SELECT * FROM VALUES (TIMESTAMP_NTZ '2026-01-01 12:00:00', 1, 'KRM000001', CAST(1 AS DECIMAL(12,8))), (TIMESTAMP_NTZ '2026-01-03 12:00:00', 2, 'KRM000001', CAST(2 AS DECIMAL(12,8))) AS x(INSP_DONE_DT, REV_NO, INSPECTOR_EMP_NO, INSP_DURATION_MIN)")
        sparse_saved = daily_workload(sparse).collect()
        zero_date_invented = any(str(r['done_date_kst']) == '2026-01-02' for r in sparse_saved)
        checks = {
            'current_scope_271787': current_stats['rows'] == 271787,
            'event_scope_291548': event_stats['rows'] == 291548,
            'scope_difference_older_valid_19761': event_stats['rows'] - current_stats['rows'] == 19761,
            'event_revision_keys_unique': events.select('INSP_ID', 'REV_NO').distinct().count() == 291548,
            'daily_result_keys_unique': result_daily.count() == result_daily.select('done_date_kst').distinct().count(),
            'daily_workload_keys_unique': workload_daily.count() == workload_daily.select('done_date_kst', 'event_type').distinct().count(),
            'result_daily_all_values_match_python': actual_result == py_result,
            'workload_daily_all_values_match_python': actual_work == py_work,
            'daily_results_represent_all_current': result_totals['inspection_rows'] == current_stats['rows'],
            'daily_result_categories_sum_to_rows': sum(result_totals[c] for c in RESULTS.values()) == result_totals['inspection_rows'],
            'known_result_codes_only': known_results,
            'trade_price_total_matches_source': result_totals['total_trade_price_krw'] == current_stats['price'],
            'policy_compensation_total_matches_source': result_totals['compensation_loss_krw'] == current_stats['compensation'],
            'daily_workload_represent_all_events': work_totals['rows'] == event_stats['rows'],
            'work_minutes_total_matches_source': work_totals['minutes'] == event_stats['minutes'],
            'gold_required_values_not_null': result_daily.filter(F.col('done_date_kst').isNull() | F.col('total_trade_price_krw').isNull() | F.col('compensation_loss_krw').isNull()).count() == 0 and workload_daily.filter(F.col('done_date_kst').isNull() | F.col('total_work_minutes').isNull() | F.col('avg_work_minutes').isNull()).count() == 0,
            'period_boundaries_match_python': boundary_actual == {day: python_period(day) for day in boundary_actual},
            'sparse_missing_day_not_assumed_zero': len(sparse_saved) == 2 and not zero_date_invented,
            'current_source_version_unchanged': DeltaTable.forPath(spark, str(current_path)).history(1).first()['version'] == current_version,
            'stage_source_version_unchanged': DeltaTable.forPath(spark, str(stage_path)).history(1).first()['version'] == stage_version_before,
        }
        period_totals = [r.asDict() for r in result_daily.groupBy('analysis_period').agg(F.sum('inspection_rows').alias('inspection_rows'), F.count('*').alias('observed_done_dates')).orderBy('analysis_period').collect()]
        event_totals = [r.asDict() for r in workload_daily.groupBy('event_type').agg(F.sum('inspection_event_rows').alias('event_rows'), F.sum('total_work_minutes').alias('total_work_minutes')).orderBy('event_type').collect()]
        result = {'lesson': '12_gold_daily', 'run_id': run_id, 'replay_run_id': previous['run_id'], 'source_silver_run_id': previous['source_silver_run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'scope': 'frozen_snapshot_daily_results_and_valid_revision_events_not_incremental_gold',
            'date_basis': 'KST_inspection_completion_wall_clock', 'source_versions': {'replayed_silver': current_version, 'classification_stage': 0},
            'current_source_stats': current_stats, 'event_source_stats': event_stats,
            'daily_table_rows': {'final_results': len(actual_result), 'valid_event_workload': len(actual_work)},
            'daily_result_totals': result_totals, 'daily_workload_totals': work_totals,
            'period_totals': period_totals, 'event_type_totals': event_totals,
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-12-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str), flush=True)
        if not result['all_passed']:
            raise RuntimeError('Gold 검사 실패: results/lesson-12-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
