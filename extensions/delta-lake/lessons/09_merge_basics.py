"""가상 데이터로 MERGE의 신규·높은 개정·늦은 도착·내용 멱등성을 배운다.

실습 08의 실제 Silver는 수정하지 않는다. 운영 증분 파이프라인이 아닌 축소 예제다.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

COLUMNS = ['INSP_ID', 'REV_NO', 'PRICE_KRW']
SCHEMA = 'INSP_ID string, REV_NO int, PRICE_KRW decimal(18,4)'


def guarded_merge(spark, target_path, source):
    """미리 정제한 축소 업무 컬럼 3개만 처리한다. 단일 실행자를 가정한다."""
    if source.columns != COLUMNS:
        raise ValueError('예제 입력 스키마가 다릅니다.')
    invalid = (F.col('INSP_ID').isNull() | ~F.col('INSP_ID').rlike(r'\AINSP_[0-9]{6}\z')
        | F.col('REV_NO').isNull() | (F.col('REV_NO') < 1)
        | F.col('PRICE_KRW').isNull() | (F.col('PRICE_KRW') < 0))
    if source.filter(invalid).count():
        raise ValueError('INVALID_INPUT')
    if source.count() != source.select('INSP_ID').distinct().count():
        raise ValueError('DUPLICATE_SOURCE_KEY')
    target = spark.read.format('delta').load(str(target_path))
    if target.count() != target.select('INSP_ID').distinct().count():
        raise ValueError('DUPLICATE_TARGET_KEY')
    # 같은 개정인데 금액이 다르면 덮어쓰지 않고 이번 예제 배치 전체를 거부한다.
    conflict = (source.alias('s').join(target.alias('t'), F.col('s.INSP_ID') == F.col('t.INSP_ID'), 'inner')
        .filter((F.col('s.REV_NO') == F.col('t.REV_NO')) & ~F.col('s.PRICE_KRW').eqNullSafe(F.col('t.PRICE_KRW'))))
    if conflict.count():
        raise ValueError('CURRENT_REVISION_CONTENT_CONFLICT')
    (DeltaTable.forPath(spark, str(target_path)).alias('t')
        .merge(source.alias('s'), 't.INSP_ID = s.INSP_ID')
        .whenMatchedUpdate(condition='s.REV_NO > t.REV_NO', set={'REV_NO': 's.REV_NO', 'PRICE_KRW': 's.PRICE_KRW'})
        .whenNotMatchedInsert(values={c: 's.' + c for c in COLUMNS})
        .execute())


def main():
    root = Path(__file__).resolve().parents[1]
    prerequisite = json.loads((root / 'results/lesson-08-latest.json').read_text(encoding='utf-8'))
    if not prerequisite.get('all_passed'):
        raise ValueError('먼저 실습 08을 통과해야 합니다.')
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', prerequisite['run_id']):
        raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    actual_silver = runtime / 'lesson-08' / prerequisite['run_id'] / 'silver_inspection'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-09' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-09-Merge').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        target = run / 'synthetic_current_inspections'
        def frame(label, records):
            path = run / (label + '.jsonl')
            path.write_text(''.join(json.dumps({'INSP_ID': ident, 'REV_NO': rev, 'PRICE_KRW': price}) + '\n' for ident, rev, price in records), encoding='utf-8')
            return spark.read.schema(SCHEMA).json(str(path))
        def read(version=None):
            reader = spark.read.format('delta')
            if version is not None:
                reader = reader.option('versionAsOf', version)
            return reader.load(str(target))
        def snapshot(version=None):
            return {r['INSP_ID']: (r['REV_NO'], r['PRICE_KRW']) for r in read(version).collect()}
        def version():
            return DeltaTable.forPath(spark, str(target)).history(1).first()['version']
        initial = [('INSP_000001', 1, 100), ('INSP_000002', 1, 200)]
        frame('initial', initial).write.format('delta').mode('errorifexists').save(str(target))
        original = snapshot()
        v0 = version()
        actual_v0 = DeltaTable.forPath(spark, str(actual_silver)).history(1).first()['version']
        batch = frame('new_and_updated', [('INSP_000001', 2, 120), ('INSP_000003', 1, 300)])
        guarded_merge(spark, target, batch)
        first, first_version = snapshot(), version()
        guarded_merge(spark, target, batch)
        replay, replay_version = snapshot(), version()
        guarded_merge(spark, target, frame('late_old_revision', [('INSP_000001', 1, 100)]))
        delayed, delayed_version = snapshot(), version()
        rejections = {}
        for label, records, expected_error in [
            ('negative_price', [('INSP_000002', 2, -10)], 'INVALID_INPUT'),
            ('same_revision_conflict', [('INSP_000001', 2, 999)], 'CURRENT_REVISION_CONTENT_CONFLICT'),
            ('duplicate_source', [('INSP_000003', 2, 310), ('INSP_000003', 3, 320)], 'DUPLICATE_SOURCE_KEY'),
        ]:
            before, before_version = snapshot(), version()
            try:
                guarded_merge(spark, target, frame(label, records))
                error = None
            except ValueError as exc:
                error = str(exc)
            rejections[label] = {'error': error, 'expected_error': expected_error,
                'data_unchanged': snapshot() == before, 'version_unchanged': version() == before_version}
        guarded_merge(spark, target, frame('next_valid_update', [('INSP_000002', 2, 220)]))
        final, final_version = snapshot(), version()
        history = [{'version': r['version'], 'operation': r['operation'], 'operation_metrics': r['operationMetrics']} for r in DeltaTable.forPath(spark, str(target)).history().orderBy('version').collect()]
        expected_first = {'INSP_000001': (2, Decimal('120.0000')), 'INSP_000002': (1, Decimal('200.0000')), 'INSP_000003': (1, Decimal('300.0000'))}
        expected_final = dict(expected_first)
        expected_final['INSP_000002'] = (2, Decimal('220.0000'))
        checks = {
            'initial_two_rows': len(original) == 2,
            'initial_delta_version_zero': v0 == 0,
            'higher_revision_updated': first.get('INSP_000001') == (2, Decimal('120.0000')),
            'new_key_inserted': first.get('INSP_000003') == (1, Decimal('300.0000')),
            'unmentioned_existing_row_retained': first.get('INSP_000002') == original['INSP_000002'],
            'first_merge_matches_fixed_expectation': first == expected_first,
            'same_input_replay_same_business_values': replay == first,
            'same_input_replay_no_extra_rows': len(replay) == 3,
            'late_old_revision_does_not_overwrite': delayed == first,
            'later_valid_revision_updated': final.get('INSP_000002') == (2, Decimal('220.0000')),
            'final_matches_fixed_expectation': final == expected_final,
            'final_keys_unique': read().count() == read().select('INSP_ID').distinct().count() == 3,
            'final_total_price': sum(value[1] for value in final.values()) == Decimal('640.0000'),
            'version_zero_still_readable': snapshot(0) == original,
            'first_merge_version_still_readable': snapshot(first_version) == first,
            'merge_recorded_in_history': sum(r['operation'] == 'MERGE' for r in history) >= 2,
            'actual_335000_row_lesson_silver_untouched': DeltaTable.forPath(spark, str(actual_silver)).history(1).first()['version'] == actual_v0,
        }
        for label, result in rejections.items():
            checks[label + '_rejected'] = result['error'] == result['expected_error']
            checks[label + '_no_data_or_version_change'] = result['data_unchanged'] and result['version_unchanged']
        def serialize(values):
            return [{'INSP_ID': ident, 'REV_NO': rev, 'PRICE_KRW': str(price)} for ident, (rev, price) in sorted(values.items())]
        result = {'lesson': '09_merge_basics', 'run_id': run_id, 'prerequisite_silver_run_id': prerequisite['run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'scope': 'synthetic_three_column_merge_not_real_project_incremental_pipeline',
            'initial_rows': serialize(original), 'after_first_merge': serialize(first), 'after_replay': serialize(replay),
            'after_late_old_revision': serialize(delayed), 'final_rows': serialize(final),
            'observed_versions': {'initial': v0, 'first_merge': first_version, 'replay': replay_version, 'late_old_revision': delayed_version, 'final': final_version},
            'rejections': rejections, 'history': history,
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-09-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not result['all_passed']:
            raise RuntimeError('MERGE 예제 검사 실패: results/lesson-09-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
