"""ID 정규화, 정상 검수원 마스터 구성, 사번 참조를 검사한다.

기존 팀 SQL 23·33·40·41번의 규칙을 Spark로 구현한다. 실제 검수의 최종 Silver는
아직 만들지 않는다. 원본·상위 테이블을 보존하고 별도 Delta 후보를 저장한다.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, Window, functions as F

WIDE = '０１２３４５６７８９ＡＣＴＫＲＭＩＮＳＰ＿'
ASCII = '0123456789ACTKRMINSP_'
SPACE_PATTERN = '[ \\t\\r\\n\u00a0\u200b\u3000]'
NULLS = ['', 'NULL', 'NONE', 'NA', 'N/A', 'NAN', '-']
PATTERNS = {'INSP': r'INSP_[0-9]{6}', 'KRM': r'KRM[0-9]{6}'}
# 기존 SQL 40번에서 교차 확인 근거로 유지한 다섯 부서. 새로운 이름을 추측하지 않는다.
CONFIRMED_DEPARTMENTS = ['럭셔리워치감정팀', '스니커즈검수2팀', '스트릿의류검수팀', '클레임재검수팀', '테크레고검수팀']


def normalize_id(df, raw_name, name, family):
    raw = F.col(raw_name)
    cleaned = F.translate(F.upper(F.regexp_replace(raw, SPACE_PATTERN, '')), WIDE, ASCII)
    if family == 'INSP':
        canonical = (F.when(cleaned.rlike(r'\AINSP-[0-9]{6}\z'), F.regexp_replace(cleaned, '-', '_'))
                     .when(cleaned.rlike(r'\A[0-9]{6}\z'), F.concat(F.lit('INSP_'), cleaned)).otherwise(cleaned))
    else:
        canonical = (F.when(cleaned.rlike(r'\AKRM-[0-9]{6}\z'), F.regexp_replace(cleaned, '-', ''))
                     .when(cleaned.rlike(r'\A[0-9]{6}\z'), F.concat(F.lit('KRM'), cleaned)).otherwise(cleaned))
    good = F.coalesce(canonical.rlike(r'\A' + PATTERNS[family] + r'\z'), F.lit(False))
    return (df.withColumn(name + '_canonical', canonical)
            .withColumn(name, F.when(good, canonical))
            .withColumn(name + '_status',
                F.when(F.coalesce(canonical, F.lit('')).isin(NULLS), F.lit('MISSING_CANDIDATE'))
                .when(~good, F.lit('UNRESOLVED'))
                .when(raw.eqNullSafe(canonical), F.lit('UNCHANGED'))
                .otherwise(F.lit('NORMALIZED_CANDIDATE'))))


def reference_id(raw, family):
    if raw is None:
        return None, None, 'MISSING_CANDIDATE'
    text = re.sub(SPACE_PATTERN, '', raw).upper().translate(str.maketrans(WIDE, ASCII))
    if family == 'INSP' and re.fullmatch(r'INSP-[0-9]{6}', text):
        text = text.replace('-', '_')
    elif family == 'KRM' and re.fullmatch(r'KRM-[0-9]{6}', text):
        text = text.replace('-', '')
    elif re.fullmatch(r'[0-9]{6}', text):
        text = ('INSP_' if family == 'INSP' else 'KRM') + text
    valid = re.fullmatch(PATTERNS[family], text) is not None
    status = 'MISSING_CANDIDATE' if text in NULLS else 'UNRESOLVED' if not valid else 'UNCHANGED' if raw == text else 'NORMALIZED_CANDIDATE'
    return text, text if valid else None, status


def department_terms(dictionary):
    terms = (dictionary.filter(F.col('CODE_TYPE').isin('DEPT_STD', 'BANNED_DEPT'))
        .withColumn('_term', F.explode(F.concat(F.array('STD_KOR_NM'), F.split(F.coalesce(F.col('SYNONYM_LIST'), F.lit('')), ','))))
        .withColumn('_term', F.lower(F.trim('_term'))).filter(F.col('_term') != ''))
    normal = (F.col('CODE_TYPE') == 'DEPT_STD') & (F.col('BANNED_WORD_YN') == 'N')
    return terms.groupBy('_term').agg(
        F.countDistinct(F.when(normal, F.col('STD_CODE'))).alias('_normal_codes'),
        F.countDistinct(F.when(F.col('BANNED_WORD_YN') == 'Y', F.col('STD_CODE'))).alias('_banned_codes'),
        F.min(F.when(normal, F.col('STD_KOR_NM'))).alias('_department_name'))


def master_candidates(master, dictionary):
    prepared = normalize_id(master, 'EMP_NO', 'normalized_emp_no', 'KRM')
    prepared = prepared.withColumn('_term', F.lower(F.trim('DEPT_NM'))).join(department_terms(dictionary), '_term', 'left')
    banned = F.coalesce(F.col('_banned_codes'), F.lit(0)) > 0
    direct = (~banned) & (F.col('_normal_codes') == 1)
    confirmed = (~banned) & (F.coalesce(F.col('_normal_codes'), F.lit(0)) == 0) & F.col('DEPT_NM').isin(CONFIRMED_DEPARTMENTS)
    prepared = (prepared.withColumn('normalized_department', F.when(direct, F.col('_department_name')).when(confirmed, F.col('DEPT_NM')))
        .withColumn('department_rule_source', F.when(banned, 'BANNED').when(direct, 'DICTIONARY').when(confirmed, 'MASTER_CROSSCHECK').otherwise('UNRESOLVED'))
        .withColumn('normalized_active_yn', F.when(F.col('ACTIVE_YN') == 'Y', F.lit('Y')).otherwise(F.lit('N')))
        .withColumn('normalized_hire_date', F.expr('try_cast(substring(HIRE_DT, 1, 10) as date)'))
        .withColumn('_occurrence', F.row_number().over(Window.partitionBy('normalized_emp_no').orderBy('_source_record_number'))))
    # 같은 정규화 키의 내용이 충돌하면 임의로 대표 행을 선택하지 않는다.
    payload = F.to_json(F.struct('EMP_NM', 'normalized_department', 'POSITION_NM', 'normalized_hire_date', 'normalized_active_yn'))
    conflicts = prepared.filter(F.col('normalized_emp_no').isNotNull()).groupBy('normalized_emp_no').agg(F.countDistinct(payload).alias('_payload_versions'))
    prepared = prepared.join(conflicts, 'normalized_emp_no', 'left')
    reason = (F.when(F.col('normalized_emp_no').isNull(), F.lit('UNRESOLVED_EMPLOYEE_KEY'))
        .when(F.col('department_rule_source') == 'BANNED', F.lit('BANNED_DEPARTMENT'))
        .when(F.col('normalized_department').isNull(), F.lit('UNRESOLVED_DEPARTMENT'))
        .when(F.col('normalized_hire_date').isNull(), F.lit('INVALID_HIRE_DATE'))
        .when(F.col('_payload_versions') > 1, F.lit('MASTER_KEY_CONTENT_CONFLICT'))
        .when(F.col('_occurrence') > 1, F.lit('IDENTICAL_REPEAT')).otherwise(F.lit('MASTER_CHECKS_PASSED')))
    return (prepared.withColumn('master_reason', reason)
        .withColumn('master_disposition',
            F.when(F.col('master_reason') == 'MASTER_CHECKS_PASSED', F.lit('SILVER_CANDIDATE'))
            .when(F.col('master_reason') == 'IDENTICAL_REPEAT', F.lit('EXCLUDED_CANDIDATE'))
            .when(F.col('master_reason').isin('BANNED_DEPARTMENT', 'MASTER_KEY_CONTENT_CONFLICT'), F.lit('QUARANTINE_CANDIDATE'))
            .otherwise(F.lit('REVIEW_REQUIRED'))))


def attach_employee_reference(df, valid_master):
    rows = normalize_id(normalize_id(df, 'INSP_ID', 'normalized_insp_id', 'INSP'), 'INSPECTOR_EMP_NO', 'normalized_inspector_emp_no', 'KRM')
    lookup = valid_master.select(F.col('normalized_emp_no').alias('_master_emp_no'), F.col('_source_record_number').alias('_master_source_record_number'), F.col('normalized_active_yn').alias('_master_active_yn'))
    rows = rows.join(lookup, rows['normalized_inspector_emp_no'] == lookup['_master_emp_no'], 'left')
    return (rows.withColumn('employee_reference_status',
        F.when(F.col('normalized_inspector_emp_no_status') == 'MISSING_CANDIDATE', F.lit('MISSING_EMPLOYEE_KEY'))
        .when(F.col('normalized_inspector_emp_no').isNull(), F.lit('INVALID_EMPLOYEE_FORMAT'))
        .when(F.col('_master_emp_no').isNull(), F.lit('NO_VALID_MASTER')).otherwise(F.lit('VALID_REFERENCE')))
        .withColumn('id_and_employee_rules_passed', F.col('normalized_insp_id').isNotNull() & (F.col('employee_reference_status') == 'VALID_REFERENCE'))
        .withColumn('_id_employee_rule_version', F.lit('id_employee_v1_source_sql_23_33_40_41')))


def python_master_reference(master_rows, dictionary_rows):
    terms = defaultdict(list)
    for row in dictionary_rows:
        if row['CODE_TYPE'] not in {'DEPT_STD', 'BANNED_DEPT'}:
            continue
        for text in [row['STD_KOR_NM'], *(row['SYNONYM_LIST'] or '').split(',')]:
            if text.strip(' '):
                terms[text.strip(' ').lower()].append(row)
    prepared, occurrence, payloads = [], Counter(), defaultdict(set)
    for row in sorted(master_rows, key=lambda r: r['_source_record_number']):
        key = reference_id(row['EMP_NO'], 'KRM')[1]
        occurrence[key] += 1
        matches = terms[row['DEPT_NM'].strip(' ').lower()]
        banned = any(r['BANNED_WORD_YN'] == 'Y' for r in matches)
        normal = [r for r in matches if r['CODE_TYPE'] == 'DEPT_STD' and r['BANNED_WORD_YN'] == 'N']
        codes = {r['STD_CODE'] for r in normal}
        department = min(r['STD_KOR_NM'] for r in normal) if not banned and len(codes) == 1 else row['DEPT_NM'] if not banned and not codes and row['DEPT_NM'] in CONFIRMED_DEPARTMENTS else None
        try:
            hire_date = date.fromisoformat(row['HIRE_DT'][:10])
        except ValueError:
            hire_date = None
        payload = (row['EMP_NM'], department, row['POSITION_NM'], hire_date, 'Y' if row['ACTIVE_YN'] == 'Y' else 'N')
        payloads[key].add(payload)
        prepared.append((key, row['_source_record_number'], banned, occurrence[key], payload))
    selected, counts = {}, Counter()
    for key, number, banned, occurrence_no, payload in prepared:
        if key is None:
            disposition = 'REVIEW_REQUIRED'
        elif banned:
            disposition = 'QUARANTINE_CANDIDATE'
        elif payload[1] is None or payload[3] is None:
            disposition = 'REVIEW_REQUIRED'
        elif len(payloads[key]) > 1:
            disposition = 'QUARANTINE_CANDIDATE'
        elif occurrence_no > 1:
            disposition = 'EXCLUDED_CANDIDATE'
        else:
            disposition = 'SILVER_CANDIDATE'
            selected[key] = (number, payload)
        counts[disposition] += 1
    return selected, dict(counts)


def main():
    root = Path(__file__).resolve().parents[1]
    previous_result = json.loads((root / 'results/lesson-04-latest.json').read_text(encoding='utf-8'))
    if not previous_result.get('all_passed'):
        raise ValueError('먼저 실습 04를 통과해야 합니다.')
    for key in ['run_id', 'bronze_run_id']:
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', previous_result[key]):
            raise ValueError('잘못된 이전 실습 run_id')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-04' / previous_result['run_id'] / 'inspection_numeric_datetime_candidates'
    bronze = runtime / 'lesson-02' / previous_result['bronze_run_id'] / 'bronze'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-05' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-05-IDs').master('local[2]')
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
        master, dictionary, previous = read(bronze / 'bronze_inspector_master'), read(bronze / 'bronze_product_std_dict'), read(source)
        staged = master_candidates(master, dictionary)
        staged.write.format('delta').mode('errorifexists').save(str(run / 'inspector_master_stage'))
        staged = read(run / 'inspector_master_stage')
        valid = staged.filter(F.col('master_disposition') == 'SILVER_CANDIDATE')
        valid.write.format('delta').mode('errorifexists').save(str(run / 'valid_inspector_master'))
        valid = read(run / 'valid_inspector_master')
        # 참조 테이블의 유일성을 조인 전에 강제 확인한다.
        if valid.count() != valid.select('normalized_emp_no').distinct().count():
            raise ValueError('정상 마스터 키 중복: 조인을 중단합니다.')
        attached = attach_employee_reference(previous, valid)
        attached.write.format('delta').mode('errorifexists').save(str(run / 'inspection_id_employee_candidates'))
        saved = read(run / 'inspection_id_employee_candidates')
        master_counts = {r['master_disposition']: r['count'] for r in staged.groupBy('master_disposition').count().collect()}
        ref_counts = {r['employee_reference_status']: r['count'] for r in saved.groupBy('employee_reference_status').count().collect()}
        id_changes = saved.agg(
            F.sum((~F.col('INSP_ID').eqNullSafe(F.col('normalized_insp_id_canonical'))).cast('int')).alias('inspection_id_changed'),
            F.sum((~F.col('INSPECTOR_EMP_NO').eqNullSafe(F.col('normalized_inspector_emp_no_canonical'))).cast('int')).alias('employee_id_changed'),
        ).first().asDict()
        py_master, py_counts = python_master_reference(master.collect(), dictionary.collect())
        actual_master = {r['normalized_emp_no']: (r['_source_record_number'], (r['EMP_NM'], r['normalized_department'], r['POSITION_NM'], r['normalized_hire_date'], r['normalized_active_yn'])) for r in valid.collect()}
        mismatches, row_count = 0, 0
        selected = ['INSP_ID', 'INSPECTOR_EMP_NO', 'normalized_insp_id_canonical', 'normalized_insp_id', 'normalized_insp_id_status', 'normalized_inspector_emp_no_canonical', 'normalized_inspector_emp_no', 'normalized_inspector_emp_no_status', 'employee_reference_status']
        for row in saved.select(*selected).toLocalIterator():
            row_count += 1
            a, b = reference_id(row['INSP_ID'], 'INSP'), reference_id(row['INSPECTOR_EMP_NO'], 'KRM')
            ref = 'MISSING_EMPLOYEE_KEY' if b[2] == 'MISSING_CANDIDATE' else 'INVALID_EMPLOYEE_FORMAT' if b[1] is None else 'VALID_REFERENCE' if b[1] in py_master else 'NO_VALID_MASTER'
            actual = (row['normalized_insp_id_canonical'], row['normalized_insp_id'], row['normalized_insp_id_status'], row['normalized_inspector_emp_no_canonical'], row['normalized_inspector_emp_no'], row['normalized_inspector_emp_no_status'], row['employee_reference_status'])
            mismatches += actual != (*a, *b, ref)

        cases = [
            ('standard', 'INSP_000001', 'KRM000001', 'INSP_000001', 'KRM000001', 'VALID_REFERENCE'),
            ('spaces', ' INSP_000001\u200b', '\tKRM000001\u3000', 'INSP_000001', 'KRM000001', 'VALID_REFERENCE'),
            ('wide', 'ＩＮＳＰ＿０００００１', 'ＫＲＭ０００００１', 'INSP_000001', 'KRM000001', 'VALID_REFERENCE'),
            ('hyphen', 'insp-000001', 'krm-000001', 'INSP_000001', 'KRM000001', 'VALID_REFERENCE'),
            ('digits', '000001', '000001', 'INSP_000001', 'KRM000001', 'VALID_REFERENCE'),
            ('ghost', 'INSP_000001', 'KRM999999', 'INSP_000001', 'KRM999999', 'NO_VALID_MASTER'),
            ('missing', 'INSP_000001', 'NULL', 'INSP_000001', None, 'MISSING_EMPLOYEE_KEY'),
            ('wrong_length', 'INSP_000001', 'KRM00001', 'INSP_000001', None, 'INVALID_EMPLOYEE_FORMAT'),
            ('inactive_allowed', 'INSP_000001', 'KRM000002', 'INSP_000001', 'KRM000002', 'VALID_REFERENCE'),
            ('bad_inspection_id', 'INSP_1', 'KRM000001', None, 'KRM000001', 'VALID_REFERENCE'),
        ]
        path = run / 'boundary_cases.jsonl'
        path.write_text(''.join(json.dumps({'case_id': c[0], 'INSP_ID': c[1], 'INSPECTOR_EMP_NO': c[2]}, ensure_ascii=True) + '\n' for c in cases), encoding='utf-8')
        fixture = spark.sql("SELECT * FROM VALUES ('KRM000001', 1L, 'Y'), ('KRM000002', 2L, 'N') AS m(normalized_emp_no, _source_record_number, normalized_active_yn)")
        samples = attach_employee_reference(spark.read.schema('case_id string, INSP_ID string, INSPECTOR_EMP_NO string').json(str(path)), fixture)
        expected = {c[0]: (c[3], c[4], c[5]) for c in cases}
        sample_checks = {'boundary_' + r['case_id']: (r['normalized_insp_id'], r['normalized_inspector_emp_no'], r['employee_reference_status']) == expected[r['case_id']] for r in samples.collect()}
        checks = {
            'master_3150_rows_retained': staged.count() == 3150,
            'master_original_columns_preserved': master.exceptAll(staged.select(master.columns)).count() == 0 and staged.select(master.columns).exceptAll(master).count() == 0,
            'master_counts_match_saved_final': master_counts == {'SILVER_CANDIDATE': 2960, 'QUARANTINE_CANDIDATE': 43, 'EXCLUDED_CANDIDATE': 147},
            'master_matches_python_reference': actual_master == py_master and master_counts == py_counts,
            'valid_master_keys_unique': valid.count() == 2960 and valid.select('normalized_emp_no').distinct().count() == 2960,
            'no_unresolved_master_key_conflicts': staged.filter(F.col('_payload_versions') > 1).count() == 0,
            'inspection_335000_rows_retained_no_join_fanout': row_count == 335000,
            'upstream_columns_preserved': previous.exceptAll(saved.select(previous.columns)).count() == 0 and saved.select(previous.columns).exceptAll(previous).count() == 0,
            'normalization_changes_match_existing_diagnosis': id_changes == {'inspection_id_changed': 6000, 'employee_id_changed': 6000},
            'missing_valid_master_matches_existing_diagnosis': sum(v for k, v in ref_counts.items() if k != 'VALID_REFERENCE') == 5942,
            'all_rows_match_python_reference': mismatches == 0 and row_count == 335000,
            'upstream_not_modified': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'all_boundary_cases_present': len(sample_checks) == len(cases),
            **sample_checks,
        }
        result = {
            'lesson': '05_ids_employee_reference', 'run_id': run_id,
            'datetime_run_id': previous_result['run_id'], 'bronze_run_id': previous_result['bronze_run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'id_and_employee_reference_only_not_final_inspection_silver',
            'master_disposition_counts': master_counts, 'employee_reference_status_counts': ref_counts,
            'normalization_changes': id_changes, 'rows': row_count,
            'python_reference_mismatch_rows': mismatches, 'boundary_cases': len(cases),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values()),
        }
        (root / 'results/lesson-05-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('\n학습용 ID·참조 사례 (실제 원본 행이 아님):', flush=True)
        samples.select('case_id', 'INSP_ID', 'normalized_insp_id', 'INSPECTOR_EMP_NO', 'normalized_inspector_emp_no', 'employee_reference_status').orderBy('case_id').show(20, truncate=False)
        if not result['all_passed']:
            raise RuntimeError('ID·사번 참조 검사 실패: results/lesson-05-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
