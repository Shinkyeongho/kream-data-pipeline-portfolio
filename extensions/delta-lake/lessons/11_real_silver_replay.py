"""실제 데이터의 이전 유효 개정과 최종 Silver를 배치 재생해 결과를 대조한다.

정제 완료 스냅샷 재생이다. 미지의 raw CSV 배치 정제·전 개정 이력 처리는 아니다.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

spec = importlib.util.spec_from_file_location('lesson08_revision_silver', Path(__file__).with_name('08_revision_silver.py'))
lesson08 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lesson08)

RULE = 'real_silver_replay_v1'
LEDGER_SCHEMA = 'batch_id string, payload_sha256 string, status string, attempts int, row_count long, updated_at_utc string, target_version_after long, merge_rule_version string'


class ReplayInterrupted(RuntimeError):
    pass


def version(spark, path):
    return DeltaTable.forPath(spark, str(path)).history(1).first()['version']


def content_hash(source):
    """행 전체를 모으지 않고 정렬된 JSON을 순회해 하나의 내용 해시를 계산한다."""
    digest = hashlib.sha256()
    digest.update((RULE + '\n' + source.schema.json() + '\n').encode('utf-8'))
    serialized = source.orderBy('INSP_ID').select(F.to_json(F.struct(*source.columns), options={'ignoreNullFields': 'false'}).alias('record'))
    for row in serialized.toLocalIterator():
        digest.update(row['record'].encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


class ReplayRunner:
    def __init__(self, spark, target, ledger, schema):
        self.spark, self.target, self.ledger, self.schema = spark, target, ledger, schema

    def entry(self, batch_id):
        rows = self.spark.read.format('delta').load(str(self.ledger)).filter(F.col('batch_id') == batch_id).collect()
        if len(rows) > 1:
            raise ValueError('중복 배치 원장 키')
        return rows[0].asDict() if rows else None

    def write_entry(self, batch_id, fingerprint, state, attempt, count, target_version=None):
        row = self.spark.range(1).select(F.lit(batch_id).alias('batch_id'), F.lit(fingerprint).alias('payload_sha256'),
            F.lit(state).alias('status'), F.lit(attempt).cast('int').alias('attempts'), F.lit(count).cast('long').alias('row_count'),
            F.lit(datetime.now(timezone.utc).isoformat()).alias('updated_at_utc'), F.lit(target_version).cast('long').alias('target_version_after'), F.lit(RULE).alias('merge_rule_version'))
        (DeltaTable.forPath(self.spark, str(self.ledger)).alias('t').merge(row.alias('s'), 't.batch_id = s.batch_id')
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    def process(self, batch_id, source, interrupt_after_merge=False):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', batch_id):
            raise ValueError('잘못된 배치 ID')
        # 계산식의 nullable 추론과 저장 후 nullable 표기는 다를 수 있다.
        # 필수값은 아래에서 실제 행으로 검사하고 이름·순서·자료형을 대조한다.
        if [(f.name, f.dataType) for f in source.schema] != [(f.name, f.dataType) for f in self.schema]:
            raise ValueError('입력 스키마가 기준 Silver와 다름')
        count = source.count()
        if count != source.select('INSP_ID').distinct().count() or count != source.select('_source_record_id').distinct().count():
            raise ValueError('입력 업무 키 또는 출처 중복')
        invalid = (F.col('REV_NO').isNull() | (F.col('REV_NO') < 1)
            | ~F.col('INSP_ID').rlike(r'\AINSP_[0-9]{6}\z')
            | (F.col('TRADE_PRICE_KRW') < 0) | (F.col('INSP_DURATION_MIN') < 0)
            | (F.col('INBOUND_DT') > F.col('INSP_DONE_DT')) | (F.col('INSP_DONE_DT') > F.col('EXTRACTED_AT')))
        for name in lesson08.BUSINESS:
            if name not in {'PARENT_CAT_ID', 'INSP_MEMO'}:
                invalid = invalid | F.col(name).isNull()
        if source.filter(invalid).count():
            raise ValueError('기본 자료형·필수값·범위·시간 검사 실패')
        fingerprint = content_hash(source)
        prior = self.entry(batch_id)
        if prior and (prior['payload_sha256'] != fingerprint or prior['merge_rule_version'] != RULE):
            raise ValueError('같은 배치 ID의 내용 또는 규칙 변경')
        if prior and prior['status'] == 'COMPLETED':
            return 'SKIPPED_COMPLETED'
        if prior and prior['status'] != 'STARTED':
            raise ValueError('알 수 없는 원장 상태')
        target = self.spark.read.format('delta').load(str(self.target))
        if target.count() != target.select('INSP_ID').distinct().count():
            raise ValueError('대상 업무 키 중복')
        differences = reduce(lambda a, b: a | b, [~F.col('s.' + name).eqNullSafe(F.col('t.' + name)) for name in lesson08.BUSINESS])
        conflict = source.alias('s').join(target.alias('t'), F.col('s.INSP_ID') == F.col('t.INSP_ID'), 'inner').filter((F.col('s.REV_NO') == F.col('t.REV_NO')) & differences)
        if conflict.count():
            raise ValueError('현재 개정의 업무 내용 충돌')
        attempt = prior['attempts'] + 1 if prior else 1
        self.write_entry(batch_id, fingerprint, 'STARTED', attempt, count)
        (DeltaTable.forPath(self.spark, str(self.target)).alias('t').merge(source.alias('s'), 't.INSP_ID = s.INSP_ID')
            .whenMatchedUpdateAll(condition='s.REV_NO > t.REV_NO').whenNotMatchedInsertAll().execute())
        if interrupt_after_merge:
            raise ReplayInterrupted('데이터 반영 후 완료 기록 전 가상 중단')
        self.write_entry(batch_id, fingerprint, 'COMPLETED', attempt, count, version(self.spark, self.target))
        return 'APPLIED'


def main():
    root = Path(__file__).resolve().parents[1]
    latest = json.loads((root / 'results/lesson-08-latest.json').read_text(encoding='utf-8'))
    prerequisite = json.loads((root / 'results/lesson-10-latest.json').read_text(encoding='utf-8'))
    if not latest.get('all_passed') or not prerequisite.get('all_passed'):
        raise ValueError('먼저 실습 08·10을 통과해야 합니다.')
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', latest['run_id']):
        raise ValueError('잘못된 초기 Silver 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source_run = runtime / 'lesson-08' / latest['run_id']
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-11' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-11-Real-Silver-Replay').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        def read(path, ver=None):
            reader = spark.read.format('delta')
            return (reader.option('versionAsOf', ver) if ver is not None else reader).load(str(path))
        current = read(source_run / 'silver_inspection', 0).cache()
        stage = read(source_run / 'inspection_disposition_stage', 0)
        old_stage = stage.filter(F.col('primary_reason') == 'SUPERSEDED_VALID_REVISION')
        old = lesson08.project_silver(old_stage).select(current.columns).cache()
        print('실제 입력 확인: 이전 유효 개정과 초기 Silver를 읽었습니다.', flush=True)
        old_count, current_count = old.count(), current.count()
        target, ledger = run / 'replayed_silver_inspection', run / 'batch_ledger'
        current.limit(0).write.format('delta').mode('errorifexists').save(str(target))
        empty_file = run / 'empty.jsonl'
        empty_file.write_text('', encoding='utf-8')
        spark.read.schema(LEDGER_SCHEMA).json(str(empty_file)).write.format('delta').mode('errorifexists').save(str(ledger))
        original_versions = {name: version(spark, source_run / name) for name in ['silver_inspection', 'inspection_disposition_stage']}
        runner = ReplayRunner(spark, target, ledger, current.schema)
        old_action = runner.process('real-older-valid', old)
        first_target_version = version(spark, target)
        old_values_match = read(target).exceptAll(old).count() == 0 and old.exceptAll(read(target)).count() == 0
        print(f'이전 유효 개정 {old_count:,}행 반영 완료. 최신 스냅샷을 반영합니다.', flush=True)
        try:
            runner.process('real-current-snapshot', current, interrupt_after_merge=True)
            interrupted = False
        except ReplayInterrupted:
            interrupted = True
        interrupted_entry = runner.entry('real-current-snapshot')
        first_current_merge = DeltaTable.forPath(spark, str(target)).history(1).first()['operationMetrics']
        after_failure_match = read(target).exceptAll(current).count() == 0 and current.exceptAll(read(target)).count() == 0
        print('최신 데이터 반영 후 완료 기록 전 중단을 재현했습니다. 같은 입력으로 재시도합니다.', flush=True)
        retry_runner = ReplayRunner(spark, target, ledger, current.schema)
        retry_action = retry_runner.process('real-current-snapshot', current)
        retry_entry = retry_runner.entry('real-current-snapshot')
        before_skip = (version(spark, target), version(spark, ledger))
        skip_action = retry_runner.process('real-current-snapshot', current)
        skip_versions = before_skip == (version(spark, target), version(spark, ledger))
        print('최신 스냅샷 재시도와 완료 반복 검사를 마쳤습니다. 이전 개정의 지연 도착을 확인합니다.', flush=True)
        late_action = retry_runner.process('real-late-older', old)
        final = read(target)
        final_count = final.count()
        forward_difference = current.exceptAll(final.select(current.columns)).count()
        reverse_difference = final.select(current.columns).exceptAll(current).count()
        entries = [row.asDict() for row in read(ledger).orderBy('batch_id').collect()]
        checks = {
            'older_valid_rows_19761': old_count == 19761,
            'current_snapshot_rows_271787': current_count == 271787,
            'older_stage_quality_passed': old_stage.filter(F.col('record_error') | F.col('missing_check')).count() == 0,
            'old_batch_applied': old_action == 'APPLIED',
            'old_batch_all_values_match': old_values_match,
            'old_batch_snapshot_preserved': read(target, first_target_version).exceptAll(old).count() == 0 and old.exceptAll(read(target, first_target_version)).count() == 0,
            'interruption_after_current_merge_observed': interrupted,
            'interrupted_ledger_started': interrupted_entry['status'] == 'STARTED' and interrupted_entry['attempts'] == 1,
            'data_already_matches_current_before_retry': after_failure_match,
            'current_merge_updated_19761': int(first_current_merge.get('numTargetRowsUpdated', '-1')) == 19761,
            'current_merge_inserted_252026': int(first_current_merge.get('numTargetRowsInserted', '-1')) == 252026,
            'retry_applied_and_completed': retry_action == 'APPLIED' and retry_entry['status'] == 'COMPLETED' and retry_entry['attempts'] == 2,
            'completed_current_repeat_skipped': skip_action == 'SKIPPED_COMPLETED',
            'completed_repeat_both_versions_unchanged': skip_versions,
            'late_older_batch_processed': late_action == 'APPLIED',
            'final_rows_271787': final_count == 271787,
            'final_inspection_keys_unique': final.select('INSP_ID').distinct().count() == final_count,
            'final_source_ids_unique': final.select('_source_record_id').distinct().count() == final_count,
            'all_columns_forward_difference_zero': forward_difference == 0,
            'all_columns_reverse_difference_zero': reverse_difference == 0,
            'business_schema_preserved': final.schema == current.schema,
            'ledger_three_unique_completed_batches': len(entries) == len({r['batch_id'] for r in entries}) == 3 and all(r['status'] == 'COMPLETED' for r in entries),
            'source_tables_not_modified': all(version(spark, source_run / name) == ver for name, ver in original_versions.items()),
        }
        result = {'lesson': '11_real_silver_replay', 'run_id': run_id, 'source_silver_run_id': latest['run_id'],
            'prerequisite_ledger_run_id': prerequisite['run_id'], 'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'real_prevalidated_silver_snapshot_replay_not_raw_csv_incremental',
            'older_valid_rows': old_count, 'current_snapshot_rows': current_count, 'final_rows': final_count,
            'first_current_merge_metrics': first_current_merge,
            'all_column_difference_rows': {'source_minus_replay': forward_difference, 'replay_minus_source': reverse_difference},
            'ledger_entries': entries, 'interrupted_ledger_entry': interrupted_entry,
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-11-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not result['all_passed']:
            raise RuntimeError('실제 Silver 배치 재생 검사 실패: results/lesson-11-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
