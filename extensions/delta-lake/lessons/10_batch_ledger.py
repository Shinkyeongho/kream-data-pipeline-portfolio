"""가상 데이터의 배치 ID·내용 해시·처리 상태·재시도를 Delta 원장에 저장한다.

단일 실행자 학습 예제. 대상과 원장은 별도 트랜잭션이며 실제 CSV 증분은 아니다.
"""
from __future__ import annotations

import hashlib
import importlib.util
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

spec = importlib.util.spec_from_file_location('lesson09_merge_basics', Path(__file__).with_name('09_merge_basics.py'))
lesson09 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lesson09)

RULE_VERSION = 'synthetic_merge_ledger_v1'
LEDGER_SCHEMA = 'batch_id string, payload_sha256 string, rule_version string, status string, attempts int, updated_at_utc string, silver_version_after long'


class SimulatedFailure(RuntimeError):
    pass


def fingerprint(source):
    if source.columns != lesson09.COLUMNS or source.schema.simpleString() != 'struct<INSP_ID:string,REV_NO:int,PRICE_KRW:decimal(18,4)>':
        raise ValueError('INVALID_SCHEMA')
    records = []
    for row in source.collect():
        if row['INSP_ID'] is None or not re.fullmatch(r'INSP_[0-9]{6}', row['INSP_ID']) or row['REV_NO'] is None or row['REV_NO'] < 1 or row['PRICE_KRW'] is None or row['PRICE_KRW'] < 0:
            raise ValueError('INVALID_INPUT')
        records.append([row['INSP_ID'], row['REV_NO'], format(row['PRICE_KRW'], '.4f')])
    if len({r[0] for r in records}) != len(records):
        raise ValueError('DUPLICATE_SOURCE_KEY')
    # 정제된 3개 업무 컬럼·규칙 버전 기준. 원본 파일 바이트 해시는 아니다.
    canonical = json.dumps({'rule_version': RULE_VERSION, 'rows': sorted(records)}, ensure_ascii=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


class BatchRunner:
    def __init__(self, spark, target, ledger):
        self.spark, self.target, self.ledger = spark, target, ledger

    def get(self, batch_id):
        found = self.spark.read.format('delta').load(str(self.ledger)).filter(F.col('batch_id') == batch_id).collect()
        if len(found) > 1:
            raise ValueError('DUPLICATE_LEDGER_KEY')
        return found[0].asDict() if found else None

    def save(self, batch_id, payload, status, attempts, silver_version=None):
        row = self.spark.range(1).select(
            F.lit(batch_id).alias('batch_id'), F.lit(payload).alias('payload_sha256'),
            F.lit(RULE_VERSION).alias('rule_version'), F.lit(status).alias('status'), F.lit(attempts).cast('int').alias('attempts'),
            F.lit(datetime.now(timezone.utc).isoformat()).alias('updated_at_utc'), F.lit(silver_version).cast('long').alias('silver_version_after'))
        (DeltaTable.forPath(self.spark, str(self.ledger)).alias('t').merge(row.alias('s'), 't.batch_id = s.batch_id')
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    def process(self, batch_id, source, fail_after_merge=False):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', batch_id):
            raise ValueError('INVALID_BATCH_ID')
        payload = fingerprint(source)
        prior = self.get(batch_id)
        if prior and prior['payload_sha256'] != payload:
            raise ValueError('BATCH_ID_PAYLOAD_CONFLICT')
        if prior and prior['status'] == 'COMPLETED':
            return 'SKIPPED_COMPLETED'
        if prior and prior['status'] != 'STARTED':
            raise ValueError('UNKNOWN_LEDGER_STATE')
        attempts = prior['attempts'] + 1 if prior else 1
        self.save(batch_id, payload, 'STARTED', attempts)
        lesson09.guarded_merge(self.spark, self.target, source)
        if fail_after_merge:
            raise SimulatedFailure('SIMULATED_AFTER_DATA_BEFORE_COMPLETION')
        version = DeltaTable.forPath(self.spark, str(self.target)).history(1).first()['version']
        self.save(batch_id, payload, 'COMPLETED', attempts, version)
        return 'APPLIED'


def main():
    root = Path(__file__).resolve().parents[1]
    previous = json.loads((root / 'results/lesson-09-latest.json').read_text(encoding='utf-8'))
    if not previous.get('all_passed'):
        raise ValueError('먼저 실습 09를 통과해야 합니다.')
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', previous['run_id']):
        raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-10' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-10-Batch-Ledger').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        target, ledger = run / 'synthetic_current_inspections', run / 'batch_ledger'
        def frame(label, records):
            path = run / (label + '.jsonl')
            path.write_text(''.join(json.dumps({'INSP_ID': ident, 'REV_NO': rev, 'PRICE_KRW': price}) + '\n' for ident, rev, price in records), encoding='utf-8')
            return spark.read.schema(lesson09.SCHEMA).json(str(path))
        def version(path):
            return DeltaTable.forPath(spark, str(path)).history(1).first()['version']
        def snapshot(ver=None):
            reader = spark.read.format('delta')
            if ver is not None:
                reader = reader.option('versionAsOf', ver)
            return {r['INSP_ID']: (r['REV_NO'], r['PRICE_KRW']) for r in reader.load(str(target)).collect()}
        initial = frame('initial', [('INSP_000001', 1, 100), ('INSP_000002', 1, 200)])
        initial.write.format('delta').mode('errorifexists').save(str(target))
        spark.read.schema(LEDGER_SCHEMA).json(spark.sparkContext.emptyRDD()).write.format('delta').mode('errorifexists').save(str(ledger))
        runner = BatchRunner(spark, target, ledger)
        original = snapshot()
        empty_ledger = spark.read.format('delta').load(str(ledger)).count() == 0
        old_target = runtime / 'lesson-09' / previous['run_id'] / 'synthetic_current_inspections'
        previous_version = version(old_target)
        a_records = [('INSP_000001', 2, 120), ('INSP_000003', 1, 300)]
        a = frame('batch_A', a_records)
        first_action = runner.process('batch-A', a)
        first = snapshot()
        after_first_versions = (version(target), version(ledger))
        first_entry = runner.get('batch-A')
        skip_action = runner.process('batch-A', a)
        skip_data = snapshot() == first
        skip_versions = (version(target), version(ledger)) == after_first_versions
        reordered = frame('batch_A_reordered', list(reversed(a_records)))
        reordered_action = runner.process('batch-A', reordered)
        reordered_versions = (version(target), version(ledger)) == after_first_versions
        before_conflict = (version(target), version(ledger))
        try:
            runner.process('batch-A', frame('batch_A_changed', [('INSP_000001', 2, 125), ('INSP_000003', 1, 300)]))
            conflict = None
        except ValueError as exc:
            conflict = str(exc)
        conflict_unchanged = snapshot() == first and (version(target), version(ledger)) == before_conflict
        b = frame('batch_B', [('INSP_000002', 2, 220), ('INSP_000004', 1, 400)])
        try:
            runner.process('batch-B', b, fail_after_merge=True)
            simulated = None
        except SimulatedFailure as exc:
            simulated = str(exc)
        after_failure = snapshot()
        failed_entry = runner.get('batch-B')
        # 새 실행 객체가 원장을 다시 읽는다. 실제 프로세스 강제 종료를 재현한 것은 아니다.
        retry_runner = BatchRunner(spark, target, ledger)
        retry_action = retry_runner.process('batch-B', b)
        after_retry = snapshot()
        retried_entry = retry_runner.get('batch-B')
        before_b_skip = (version(target), version(ledger))
        b_skip = retry_runner.process('batch-B', b)
        b_skip_versions = (version(target), version(ledger)) == before_b_skip
        c_action = retry_runner.process('batch-C', frame('batch_C_late', [('INSP_000001', 1, 100)]))
        final = snapshot()
        c_entry = retry_runner.get('batch-C')
        before_c_skip = (version(target), version(ledger))
        c_skip = retry_runner.process('batch-C', frame('batch_C_repeat', [('INSP_000001', 1, 100)]))
        c_skip_versions = (version(target), version(ledger)) == before_c_skip
        entries = [r.asDict() for r in spark.read.format('delta').load(str(ledger)).orderBy('batch_id').collect()]
        expected_first = {'INSP_000001': (2, Decimal('120.0000')), 'INSP_000002': (1, Decimal('200.0000')), 'INSP_000003': (1, Decimal('300.0000'))}
        expected_final = dict(expected_first)
        expected_final.update({'INSP_000002': (2, Decimal('220.0000')), 'INSP_000004': (1, Decimal('400.0000'))})
        checks = {
            'initial_ledger_empty': empty_ledger,
            'first_batch_applied': first_action == 'APPLIED',
            'first_batch_values_correct': first == expected_first,
            'first_batch_completed_once': first_entry['status'] == 'COMPLETED' and first_entry['attempts'] == 1,
            'completed_batch_skipped': skip_action == 'SKIPPED_COMPLETED',
            'completed_repeat_data_unchanged': skip_data,
            'completed_repeat_both_versions_unchanged': skip_versions,
            'row_order_independent_fingerprint': fingerprint(a) == fingerprint(reordered),
            'reordered_repeat_skipped_without_commits': reordered_action == 'SKIPPED_COMPLETED' and reordered_versions,
            'same_batch_id_changed_payload_rejected': conflict == 'BATCH_ID_PAYLOAD_CONFLICT',
            'changed_payload_no_data_or_commit_change': conflict_unchanged,
            'failure_after_merge_observed': simulated == 'SIMULATED_AFTER_DATA_BEFORE_COMPLETION',
            'failure_data_already_applied': after_failure == expected_final,
            'failure_not_marked_completed': failed_entry['status'] == 'STARTED' and failed_entry['attempts'] == 1 and failed_entry['silver_version_after'] is None,
            'retry_resumes_from_persisted_ledger': retry_action == 'APPLIED',
            'retry_completed_with_attempt_two': retried_entry['status'] == 'COMPLETED' and retried_entry['attempts'] == 2,
            'retry_same_values_no_duplicates': after_retry == after_failure,
            'completed_retry_skips_without_commits': b_skip == 'SKIPPED_COMPLETED' and b_skip_versions,
            'late_old_revision_keeps_current_values': final == after_retry,
            'late_batch_recorded_completed': c_action == 'APPLIED' and c_entry['status'] == 'COMPLETED',
            'late_batch_repeat_skips_without_commits': c_skip == 'SKIPPED_COMPLETED' and c_skip_versions,
            'ledger_three_unique_completed_batches': len(entries) == len({r['batch_id'] for r in entries}) == 3 and all(r['status'] == 'COMPLETED' for r in entries),
            'final_matches_fixed_expectation': final == expected_final,
            'final_four_unique_keys': spark.read.format('delta').load(str(target)).count() == len(final) == 4,
            'final_total_price_1040': sum(v[1] for v in final.values()) == Decimal('1040.0000'),
            'initial_version_still_readable': snapshot(0) == original,
            'lesson09_table_untouched': version(old_target) == previous_version,
        }
        result = {'lesson': '10_batch_ledger', 'run_id': run_id, 'prerequisite_merge_run_id': previous['run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'scope': 'synthetic_single_writer_ledger_not_multitable_atomic_or_real_csv_incremental',
            'ledger_entries': entries, 'simulated_failure': {'error': simulated, 'ledger_entry_after_failure': failed_entry},
            'final_rows': [{'INSP_ID': key, 'REV_NO': value[0], 'PRICE_KRW': str(value[1])} for key, value in sorted(final.items())],
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-10-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not result['all_passed']:
            raise RuntimeError('배치 원장 검사 실패: results/lesson-10-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
