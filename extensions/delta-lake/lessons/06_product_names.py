"""팀 SQL 29·31·54C를 기준으로 상품명 검사와 코드 기반 보정 후보를 저장한다.

원문을 보존한다. 최종 Silver·카테고리 복구·증분 처리는 이번 범위가 아니다.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def product_lookup(dictionary):
    relevant = dictionary.filter(F.col('CODE_TYPE').isin('ITEM_MODEL', 'BANNED_WORD'))
    # 상품 코드 비교는 원문 전체 일치다. 대소문자·공백을 임의로 바꾸지 않는다.
    if relevant.count() != relevant.select('STD_CODE').distinct().count():
        raise ValueError('상품 사전 코드 중복: 대표를 임의 선택하지 않고 중단합니다.')
    code = relevant.select(
        F.col('STD_CODE').alias('_dict_code'), F.col('STD_KOR_NM').alias('code_standard_name'),
        F.col('CODE_TYPE').alias('_dict_type'), F.col('BANNED_WORD_YN').alias('_dict_banned'))
    terms = (relevant.withColumn('_term', F.explode(F.concat(F.array('STD_KOR_NM'), F.split(F.coalesce(F.col('SYNONYM_LIST'), F.lit('')), ','))))
        .withColumn('_term', F.lower(F.trim('_term'))).filter(F.col('_term') != '')
        .groupBy('_term').agg(
            F.collect_set(F.when(F.col('CODE_TYPE') == 'ITEM_MODEL', F.col('STD_CODE'))).alias('_name_codes'),
            F.max(F.when((F.col('CODE_TYPE') == 'BANNED_WORD') & (F.col('BANNED_WORD_YN') == 'Y'), 1).otherwise(0)).alias('_banned_term')))
    return code, terms


def classify_names(rows, code, terms):
    rows = (rows.withColumn('_term', F.lower(F.trim('ITEM_NM')))
        .join(code, F.col('ITEM_STD_CODE') == F.col('_dict_code'), 'left').join(terms, '_term', 'left'))
    # 집계한 이름별 코드 집합을 사용해 여러 별칭이 조인 행을 늘리지 않게 한다.
    other_code = F.coalesce(F.size(F.array_except(F.col('_name_codes'), F.array('ITEM_STD_CODE'))) > 0, F.lit(False))
    same_code = F.coalesce(F.array_contains(F.col('_name_codes'), F.col('ITEM_STD_CODE')), F.lit(False))
    status = (F.when((F.col('_dict_type') == 'BANNED_WORD') & (F.col('_dict_banned') == 'Y'), 'BANNED_PRODUCT_CODE')
        .when(F.col('_banned_term') == 1, 'BANNED_NAME_EXACT_MATCH')
        .when(F.col('_dict_code').isNull() | (F.col('_dict_type') != 'ITEM_MODEL'), 'PRODUCT_CODE_REVIEW')
        .when(other_code, 'OTHER_PRODUCT_NAME_REVIEW')
        .when(F.col('ITEM_NM') == F.col('code_standard_name'), 'STANDARD_NAME')
        .when(F.col('_term') == F.lower(F.trim('code_standard_name')), 'STANDARD_NAME_SPELLING_VARIANT')
        .when(same_code, 'REGISTERED_ALIAS').otherwise('UNMAPPED_NAME_REVIEW'))
    rows = rows.withColumn('original_name_check_status', status)
    recover = ((F.col('original_name_check_status') == 'UNMAPPED_NAME_REVIEW')
               & (F.col('_dict_type') == 'ITEM_MODEL') & (F.col('_dict_banned') == 'N'))
    rows = rows.withColumn('product_name_status', F.when(recover, 'RECOVERED_BY_ITEM_CODE').otherwise(F.col('original_name_check_status')))
    # SQL 54C는 미매핑 이름만 코드로 복구한다. 다른 상품 이름·금칙어는 복구하지 않는다.
    passed = F.col('product_name_status').isin('STANDARD_NAME', 'REGISTERED_ALIAS', 'RECOVERED_BY_ITEM_CODE')
    return (rows.withColumn('product_name_rules_passed', passed)
        .withColumn('standard_item_name_candidate', F.when(passed, F.col('code_standard_name')))
        .withColumn('product_name_rule_source', F.when(recover, 'STANDARD_ITEM_CODE').otherwise('ORIGINAL_NAME_CHECK'))
        .withColumn('_product_rule_version', F.lit('product_v1_sql_29_31_54C'))
        .drop('_term', '_dict_code', '_dict_type', '_dict_banned', '_name_codes', '_banned_term'))


def python_reference(dictionary_rows):
    codes, names = {}, defaultdict(list)
    for row in dictionary_rows:
        if row['CODE_TYPE'] not in {'ITEM_MODEL', 'BANNED_WORD'}:
            continue
        if row['STD_CODE'] in codes:
            raise ValueError('Python 검사: 상품 코드 중복')
        codes[row['STD_CODE']] = row
        for name in [row['STD_KOR_NM'], *(row['SYNONYM_LIST'] or '').split(',')]:
            if name is not None and name.strip(' '):
                names[name.strip(' ').lower()].append(row)

    def evaluate(item_code, item_name):
        row = codes.get(item_code)
        term = item_name.strip(' ').lower() if item_name is not None else None
        matched = names.get(term, [])
        if row and row['CODE_TYPE'] == 'BANNED_WORD' and row['BANNED_WORD_YN'] == 'Y':
            original = 'BANNED_PRODUCT_CODE'
        elif any(t['CODE_TYPE'] == 'BANNED_WORD' and t['BANNED_WORD_YN'] == 'Y' for t in matched):
            original = 'BANNED_NAME_EXACT_MATCH'
        elif not row or row['CODE_TYPE'] != 'ITEM_MODEL':
            original = 'PRODUCT_CODE_REVIEW'
        elif any(t['CODE_TYPE'] == 'ITEM_MODEL' and t['STD_CODE'] != item_code for t in matched):
            original = 'OTHER_PRODUCT_NAME_REVIEW'
        elif item_name == row['STD_KOR_NM']:
            original = 'STANDARD_NAME'
        elif term == row['STD_KOR_NM'].strip(' ').lower():
            original = 'STANDARD_NAME_SPELLING_VARIANT'
        elif any(t['CODE_TYPE'] == 'ITEM_MODEL' and t['STD_CODE'] == item_code for t in matched):
            original = 'REGISTERED_ALIAS'
        else:
            original = 'UNMAPPED_NAME_REVIEW'
        recover = original == 'UNMAPPED_NAME_REVIEW' and row['CODE_TYPE'] == 'ITEM_MODEL' and row['BANNED_WORD_YN'] == 'N'
        status = 'RECOVERED_BY_ITEM_CODE' if recover else original
        passed = status in {'STANDARD_NAME', 'REGISTERED_ALIAS', 'RECOVERED_BY_ITEM_CODE'}
        return (row['STD_KOR_NM'] if row else None, original, status,
                row['STD_KOR_NM'] if passed else None, passed,
                'STANDARD_ITEM_CODE' if recover else 'ORIGINAL_NAME_CHECK')
    return evaluate


def main():
    root = Path(__file__).resolve().parents[1]
    latest = json.loads((root / 'results/lesson-05-latest.json').read_text(encoding='utf-8'))
    if not latest.get('all_passed'):
        raise ValueError('먼저 실습 05를 통과해야 합니다.')
    for key in ['run_id', 'bronze_run_id']:
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', latest[key]):
            raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-05' / latest['run_id'] / 'inspection_id_employee_candidates'
    dict_path = runtime / 'lesson-02' / latest['bronze_run_id'] / 'bronze/bronze_product_std_dict'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-06' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-06-Products').master('local[2]')
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
        previous, dictionary = read(source), read(dict_path)
        code, terms = product_lookup(dictionary)
        target = run / 'inspection_product_candidates'
        classify_names(previous, code, terms).write.format('delta').mode('errorifexists').save(str(target))
        saved = read(target)
        fields = ['code_standard_name', 'original_name_check_status', 'product_name_status',
                  'standard_item_name_candidate', 'product_name_rules_passed', 'product_name_rule_source']
        oracle = python_reference(dictionary.collect())
        mismatches, rows = 0, 0
        for row in saved.select('ITEM_STD_CODE', 'ITEM_NM', *fields).toLocalIterator():
            rows += 1
            mismatches += tuple(row[f] for f in fields) != oracle(row['ITEM_STD_CODE'], row['ITEM_NM'])
        original_counts = {r['original_name_check_status']: r['count'] for r in saved.groupBy('original_name_check_status').count().collect()}
        final_counts = {r['product_name_status']: r['count'] for r in saved.groupBy('product_name_status').count().collect()}
        # 독립된 가상 사전과 고정 기대값으로 우선순위 및 제한 조건을 확인한다.
        fixture = [
            {'STD_CODE': 'ITEM_A', 'STD_KOR_NM': 'Model Alpha', 'SYNONYM_LIST': 'Alpha, Alpha(오타), Shared', 'CODE_TYPE': 'ITEM_MODEL', 'BANNED_WORD_YN': 'N'},
            {'STD_CODE': 'ITEM_B', 'STD_KOR_NM': 'Model Beta', 'SYNONYM_LIST': 'Beta,Shared', 'CODE_TYPE': 'ITEM_MODEL', 'BANNED_WORD_YN': 'N'},
            {'STD_CODE': 'BAN_X', 'STD_KOR_NM': '금지품', 'SYNONYM_LIST': 'Replica', 'CODE_TYPE': 'BANNED_WORD', 'BANNED_WORD_YN': 'Y'},
        ]
        cases = [
            ('standard', 'ITEM_A', 'Model Alpha', 'STANDARD_NAME', 'STANDARD_NAME', 'Model Alpha', True),
            ('alias', 'ITEM_A', ' alpha ', 'REGISTERED_ALIAS', 'REGISTERED_ALIAS', 'Model Alpha', True),
            ('annotated_alias', 'ITEM_A', 'Alpha(오타)', 'REGISTERED_ALIAS', 'REGISTERED_ALIAS', 'Model Alpha', True),
            ('spelling_variant', 'ITEM_A', ' model alpha ', 'STANDARD_NAME_SPELLING_VARIANT', 'STANDARD_NAME_SPELLING_VARIANT', None, False),
            ('unmapped_code_recovery', 'ITEM_A', 'Unknown text', 'UNMAPPED_NAME_REVIEW', 'RECOVERED_BY_ITEM_CODE', 'Model Alpha', True),
            ('other_product', 'ITEM_A', 'Beta', 'OTHER_PRODUCT_NAME_REVIEW', 'OTHER_PRODUCT_NAME_REVIEW', None, False),
            ('ambiguous_alias', 'ITEM_A', 'Shared', 'OTHER_PRODUCT_NAME_REVIEW', 'OTHER_PRODUCT_NAME_REVIEW', None, False),
            ('banned_name', 'ITEM_A', 'Replica', 'BANNED_NAME_EXACT_MATCH', 'BANNED_NAME_EXACT_MATCH', None, False),
            ('banned_code', 'BAN_X', 'Model Alpha', 'BANNED_PRODUCT_CODE', 'BANNED_PRODUCT_CODE', None, False),
            ('unknown_code', 'ITEM_UNKNOWN', 'Alpha', 'PRODUCT_CODE_REVIEW', 'PRODUCT_CODE_REVIEW', None, False),
            ('code_not_trimmed', ' ITEM_A', 'Alpha', 'PRODUCT_CODE_REVIEW', 'PRODUCT_CODE_REVIEW', None, False),
            ('no_fuzzy_match', 'ITEM_A', 'Replicaaa', 'UNMAPPED_NAME_REVIEW', 'RECOVERED_BY_ITEM_CODE', 'Model Alpha', True),
        ]
        def json_input(name, records, schema):
            path = run / name
            path.write_text(''.join(json.dumps(r, ensure_ascii=True) + '\n' for r in records), encoding='utf-8')
            return spark.read.schema(schema).json(str(path))
        fd = json_input('synthetic_dictionary.jsonl', fixture, 'STD_CODE string, STD_KOR_NM string, SYNONYM_LIST string, CODE_TYPE string, BANNED_WORD_YN string')
        samples = json_input('synthetic_cases.jsonl', [{'case_id': c[0], 'ITEM_STD_CODE': c[1], 'ITEM_NM': c[2]} for c in cases], 'case_id string, ITEM_STD_CODE string, ITEM_NM string')
        fc, ft = product_lookup(fd)
        actual_samples = classify_names(samples, fc, ft).collect()
        expected = {c[0]: c[3:] for c in cases}
        boundary = {'boundary_' + r['case_id']: (r['original_name_check_status'], r['product_name_status'], r['standard_item_name_candidate'], r['product_name_rules_passed']) == expected[r['case_id']] for r in actual_samples}
        duplicate = fd.unionByName(fd.limit(1))
        try:
            product_lookup(duplicate)
            duplicate_rejected = False
        except ValueError:
            duplicate_rejected = True
        checks = {
            '335000_rows_retained': rows == 335000,
            'source_ids_unique_no_join_fanout': saved.select('_source_record_id').distinct().count() == rows,
            'upstream_columns_preserved': previous.exceptAll(saved.select(previous.columns)).count() == 0 and saved.select(previous.columns).exceptAll(previous).count() == 0,
            'python_all_rows_match': mismatches == 0 and rows == 335000,
            'all_rows_classified': sum(original_counts.values()) == rows and sum(final_counts.values()) == rows,
            'banned_never_recovered': saved.filter(F.col('original_name_check_status').isin('BANNED_PRODUCT_CODE', 'BANNED_NAME_EXACT_MATCH') & F.col('product_name_rules_passed')).count() == 0,
            'other_product_never_recovered': saved.filter((F.col('original_name_check_status') == 'OTHER_PRODUCT_NAME_REVIEW') & F.col('product_name_rules_passed')).count() == 0,
            'recovery_only_unmapped': saved.filter((F.col('product_name_status') == 'RECOVERED_BY_ITEM_CODE') & (F.col('original_name_check_status') != 'UNMAPPED_NAME_REVIEW')).count() == 0,
            'upstream_version_unchanged': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'dictionary_version_unchanged': DeltaTable.forPath(spark, str(dict_path)).history(1).first()['version'] == 0,
            'duplicate_dictionary_code_rejected': duplicate_rejected,
            'all_boundary_cases_present': len(actual_samples) == len(cases) == len(boundary),
            **boundary,
        }
        result = {'lesson': '06_product_names', 'run_id': run_id, 'id_employee_run_id': latest['run_id'],
            'bronze_run_id': latest['bronze_run_id'], 'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'product_names_only_not_final_silver_category_recovery_pending', 'rows': rows,
            'original_name_status_counts': original_counts, 'product_name_status_counts': final_counts,
            'python_reference_mismatch_rows': mismatches, 'boundary_cases': len(cases),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-06-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('학습용 사례 (실제 원본 행이 아님):', flush=True)
        for row in sorted(actual_samples, key=lambda r: r['case_id']):
            print(row['case_id'], row['original_name_check_status'], '→', row['product_name_status'], flush=True)
        if not result['all_passed']:
            raise RuntimeError('상품 검사 실패: results/lesson-06-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
