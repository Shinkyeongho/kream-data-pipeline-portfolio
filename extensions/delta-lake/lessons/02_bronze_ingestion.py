"""원본 CSV 5종을 정제하지 않고 Bronze Delta로 적재하고 원문·출처를 검사한다.

CSV의 논리 레코드 순서를 고정하기 위해 표준 csv 파서로 JSON Lines 중간 파일을
만든다. 원본 CSV 바이트는 별도로 보존한다. 원본 중복·빈값·NULL 문자열을 유지한다.
매 실행은 새 폴더의 독립 초기 적재이며, 증분·중복 재실행 방지는 다음 실습 범위다.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4
from zipfile import ZipFile

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, types as T

HEADERS = {
    'kream_product_std_dict.csv': 'DICT_ID CODE_TYPE STD_CODE STD_KOR_NM SYNONYM_LIST BANNED_WORD_YN BASE_RETAIL_KRW FAKE_PENALTY_MULTIPLIER'.split(),
    'kream_category_parent_lookup.csv': 'CAT_ID CAT_NM CAT_LEVEL REG_DT'.split(),
    'kream_inspector_master.csv': 'EMP_NO EMP_NM DEPT_NM POSITION_NM HIRE_DT ACTIVE_YN'.split(),
    'kream_inspection_300000.csv': 'INSP_ID REV_NO ITEM_STD_CODE ITEM_NM PARENT_CAT_ID TRADE_PRICE_KRW INSP_DURATION_MIN INSP_RESULT INSPECTOR_EMP_NO INBOUND_DT INSP_DONE_DT EXTRACTED_AT INSP_MEMO'.split(),
    'kream_inspection_join_ready.csv': 'INSP_ID REV_NO ITEM_STD_CODE ITEM_NM PARENT_CAT_ID PARENT_CAT_NM TRADE_PRICE_KRW INSP_DURATION_MIN INSP_RESULT COMPENSATION_LOSS_KRW INSPECTOR_EMP_NO INSPECTOR_EMP_NM INSPECTOR_DEPT_NM INSPECTOR_POSITION_NM INBOUND_DT INSP_DONE_DT EXTRACTED_AT INSP_MEMO'.split(),
}
TABLES = {
    'kream_product_std_dict.csv': 'bronze_product_std_dict',
    'kream_category_parent_lookup.csv': 'bronze_category_parent_lookup',
    'kream_inspector_master.csv': 'bronze_inspector_master',
    'kream_inspection_300000.csv': 'bronze_inspection',
    'kream_inspection_join_ready.csv': 'bronze_inspection_join_ready',
}


def prepare_source(archive, name, root, expected):
    """알려진 CSV만 열어 원본 바이트와 논리 레코드 순서를 보존한다."""
    columns = HEADERS[name]
    original = root / 'raw' / name
    original.parent.mkdir(exist_ok=True)
    with archive.open('data/' + name) as source, original.open('xb') as target:
        shutil.copyfileobj(source, target)
    with original.open('rb') as stream:
        sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    if sha != expected['sha256']:
        raise ValueError(f'최종안의 원본 해시와 불일치: {name}')
    parsed = root / 'parsed' / (Path(name).stem + '.jsonl')
    parsed.parent.mkdir(exist_ok=True)
    count = 0
    # CSV 셀 안의 줄바꿈도 파서로 처리한다. strip 같은 정규화는 하지 않는다.
    with original.open(encoding='utf-8-sig', newline='') as stream, parsed.open('x', encoding='utf-8', newline='\n') as out:
        reader = csv.reader(stream, strict=True)
        if next(reader) != columns:
            raise ValueError(f'헤더 또는 컬럼 순서 불일치: {name}')
        for count, values in enumerate(reader, start=1):
            if len(values) != len(columns):
                raise ValueError(f'컬럼 수 불일치: {name}, logical record {count}')
            row = dict(zip(columns, values))
            row['_source_record_number'] = count
            out.write(json.dumps(row, ensure_ascii=True, separators=(',', ':')) + '\n')
    if count != int(expected['rows']):
        raise ValueError(f'원본 건수가 최종안과 다름: {name}: {count}')
    return parsed, count, sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-zip', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    repo = root.parents[1]
    manifest_path = repo / 'evidence' / 'source_manifest.csv'
    with manifest_path.open(encoding='utf-8-sig', newline='') as stream:
        expected = {r['file']: r for r in csv.DictReader(stream)}
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-02' / run_id
    run.mkdir(parents=True, exist_ok=False)
    ingested_at = datetime.now(timezone.utc).isoformat()
    prepared = {}
    with ZipFile(args.source_zip) as archive:
        for name in HEADERS:
            prepared[name] = prepare_source(archive, name, run, expected[name])
            print(f'CSV prepared: {name}, {prepared[name][1]:,} rows', flush=True)

    builder = (
        SparkSession.builder.appName('KREAM-Delta-Lesson-02-Bronze').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy'))
        .config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2')
        .config('spark.ui.enabled', 'false')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    tables = []
    try:
        for name, columns in HEADERS.items():
            parsed, count, sha = prepared[name]
            schema = T.StructType([T.StructField(c, T.StringType()) for c in columns] + [
                T.StructField('_source_record_number', T.LongType())
            ])
            raw = spark.read.schema(schema).option('mode', 'FAILFAST').json(str(parsed))
            bronze = (
                raw.withColumn('_source_file', F.lit(name))
                .withColumn('_source_sha256', F.lit(sha))
                .withColumn('_source_record_id', F.concat(F.lit(sha + ':'), F.col('_source_record_number')))
                .withColumn('_ingest_batch_id', F.lit(run_id))
                .withColumn('_ingested_at_utc', F.lit(ingested_at))
            )
            table = run / 'bronze' / TABLES[name]
            bronze.write.format('delta').mode('errorifexists').save(str(table))
            stored = spark.read.format('delta').load(str(table))

            # 행 순서는 보장되지 않으므로 원본 레코드 번호를 포함해 다중집합 대조한다.
            missing = bronze.exceptAll(stored.select(bronze.columns)).count()
            extra = stored.select(bronze.columns).exceptAll(bronze).count()
            stats = stored.agg(
                F.count('*').alias('rows'),
                F.countDistinct('_source_record_number').alias('record_numbers'),
                F.min('_source_record_number').alias('min_record'),
                F.max('_source_record_number').alias('max_record'),
                F.countDistinct('_source_record_id').alias('record_ids'),
            ).first().asDict()
            checks = {
                'source_sha256_matches_team_manifest': sha == expected[name]['sha256'],
                'row_count_matches_original': stats['rows'] == count,
                'source_record_numbers_complete': stats['record_numbers'] == count and stats['min_record'] == 1 and stats['max_record'] == count,
                'source_record_ids_unique': stats['record_ids'] == count,
                'all_business_columns_are_strings': all(isinstance(stored.schema[c].dataType, T.StringType) for c in columns),
                'all_values_and_metadata_match_input': missing == 0 and extra == 0,
                'delta_log_exists': (table / '_delta_log').is_dir(),
            }
            token_counts = None
            if name == 'kream_inspection_300000.csv':
                token_counts = stored.agg(
                    F.sum(F.when(F.col('INSPECTOR_EMP_NO') == '', 1).otherwise(0)).alias('employee_empty_string'),
                    F.sum(F.when(F.col('INSPECTOR_EMP_NO') == 'NULL', 1).otherwise(0)).alias('employee_literal_NULL'),
                    F.sum(F.when(F.col('INSPECTOR_EMP_NO') == 'NA', 1).otherwise(0)).alias('employee_literal_NA'),
                    F.sum(F.when(F.col('INSP_DURATION_MIN') == '-999', 1).otherwise(0)).alias('duration_minus999'),
                ).first().asDict()
                checks['raw_missing_and_sentinel_tokens_preserved'] = token_counts == {
                    'employee_empty_string': 400, 'employee_literal_NULL': 400,
                    'employee_literal_NA': 400, 'duration_minus999': 1200,
                }
            tables.append({
                'source_file': name, 'table': TABLES[name], 'source_rows': count,
                'bronze_rows': stats['rows'], 'business_columns': len(columns),
                'metadata_columns': 6, 'source_sha256': sha,
                'missing_rows': missing, 'extra_rows': extra,
                'raw_token_counts': token_counts, 'checks': checks,
            })
            print(f'Bronze verified: {TABLES[name]}, {stats["rows"]:,} rows', flush=True)
        result = {
            'lesson': '02_bronze_ingestion', 'run_id': run_id,
            'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'spark_version': spark.version, 'delta_spark_version': version('delta-spark'),
            'semantics': 'independent_initial_load_per_run_not_incremental_ingestion',
            'tables': tables, 'total_physical_rows': sum(t['bronze_rows'] for t in tables),
            'check_count': sum(len(t['checks']) for t in tables),
            'all_passed': all(all(t['checks'].values()) for t in tables),
        }
        output = root / 'results'
        output.mkdir(exist_ok=True)
        (output / 'lesson-02-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not result['all_passed']:
            raise RuntimeError('Bronze 검사 실패: results/lesson-02-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
