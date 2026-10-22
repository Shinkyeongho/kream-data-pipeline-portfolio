"""카테고리 마스터·단일 매핑 근거·결측 복구·최종 마스터 참조 후보.

팀 SQL 23·24·31·32·44·45·46의 기준을 사용한다. 최종 검수 Silver는 아니다.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
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
NAME_EVIDENCE = ['STANDARD_NAME', 'STANDARD_NAME_SPELLING_VARIANT', 'REGISTERED_ALIAS']


def normalize_category(df, raw, name):
    text = F.translate(F.upper(F.regexp_replace(F.col(raw), SPACE_PATTERN, '')), WIDE, ASCII)
    canonical = F.when(text.rlike(r'\ACAT-[0-9]{5}\z'), F.regexp_replace(text, '-', '_')).otherwise(text)
    good = F.coalesce(canonical.rlike(r'\ACAT_[0-9]{5}\z'), F.lit(False))
    return (df.withColumn(name + '_canonical', canonical).withColumn(name, F.when(good, canonical))
        .withColumn(name + '_status', F.when(F.coalesce(canonical, F.lit('')).isin(NULLS), 'MISSING_CANDIDATE')
            .when(~good, 'UNRESOLVED').when(F.col(raw).eqNullSafe(canonical), 'UNCHANGED').otherwise('NORMALIZED_CANDIDATE')))


def python_category(raw):
    if raw is None:
        return None, None, 'MISSING_CANDIDATE'
    canonical = re.sub(SPACE_PATTERN, '', raw).upper().translate(str.maketrans(WIDE, ASCII))
    if re.fullmatch(r'CAT-[0-9]{5}', canonical):
        canonical = canonical.replace('-', '_')
    good = re.fullmatch(r'CAT_[0-9]{5}', canonical) is not None
    status = 'MISSING_CANDIDATE' if canonical in NULLS else 'UNRESOLVED' if not good else 'UNCHANGED' if raw == canonical else 'NORMALIZED_CANDIDATE'
    return canonical, canonical if good else None, status


def prepare_master(master):
    staged = (normalize_category(master, 'CAT_ID', 'normalized_cat_id')
        .withColumn('normalized_cat_name', F.col('CAT_NM'))
        .withColumn('normalized_cat_level', F.upper(F.regexp_replace(F.trim('CAT_LEVEL'), ' ', '_')))
        .withColumn('normalized_reg_dt', F.try_to_timestamp(F.regexp_replace('REG_DT', '/', '-'), F.lit('yyyy-MM-dd HH:mm:ss')))
        .withColumn('_occurrence', F.row_number().over(Window.partitionBy('normalized_cat_id').orderBy('_source_record_number'))))
    # 내용이 다른 동일 키의 대표를 임의로 선택하지 않는 추가 방어 조건.
    conflicts = staged.filter(F.col('normalized_cat_id').isNotNull()).groupBy('normalized_cat_id').agg(
        F.countDistinct(F.to_json(F.struct('normalized_cat_name', 'normalized_cat_level', 'normalized_reg_dt'))).alias('_normalized_payload_versions'),
        F.countDistinct(F.to_json(F.struct('CAT_NM', 'CAT_LEVEL', 'REG_DT'))).alias('_raw_payload_versions'))
    staged = staged.join(conflicts, 'normalized_cat_id', 'left')
    reason = (F.when(F.col('normalized_cat_id').isNull(), 'UNRESOLVED_CATEGORY_ID')
        .when(F.col('CAT_NM').isNull() | (F.trim('CAT_NM') == ''), 'MISSING_CATEGORY_NAME')
        .when((F.length('normalized_cat_id') > 20) | (F.length('CAT_NM') > 100), 'DECLARED_LENGTH_EXCEEDED')
        .when(F.col('normalized_cat_level').isNull() | ~F.col('normalized_cat_level').isin('TOP_LEVEL', 'BRAND_LINE'), 'UNSUPPORTED_CATEGORY_LEVEL')
        .when(F.col('normalized_reg_dt').isNull(), 'UNRESOLVED_REGISTRATION_DATETIME')
        .when(F.col('_normalized_payload_versions') > 1, 'MASTER_KEY_CONTENT_CONFLICT')
        .when(F.col('_occurrence') > 1, 'IDENTICAL_REPEAT').otherwise('MASTER_CHECKS_PASSED'))
    return staged.withColumn('category_master_reason', reason).withColumn('category_master_disposition',
        F.when(F.col('category_master_reason') == 'MASTER_CHECKS_PASSED', 'SILVER_CANDIDATE')
        .when(F.col('category_master_reason') == 'IDENTICAL_REPEAT', 'EXCLUDED_CANDIDATE').otherwise('QUARANTINE_CANDIDATE'))


def mapping_evidence(rows, dictionary, eligible):
    valid_codes = dictionary.filter((F.col('CODE_TYPE') == 'ITEM_MODEL') & (F.col('BANNED_WORD_YN') == 'N')).select(F.col('STD_CODE').alias('_normal_item_code'))
    evidence = (rows.filter(F.col('original_name_check_status').isin(NAME_EVIDENCE))
        .join(valid_codes, F.col('ITEM_STD_CODE') == F.col('_normal_item_code'), 'inner')
        .join(eligible, F.col('normalized_parent_cat_id') == F.col('_eligible_cat_id'), 'inner'))
    return (evidence.groupBy('ITEM_STD_CODE').agg(F.countDistinct('normalized_parent_cat_id').alias('category_count'),
        F.min('normalized_parent_cat_id').alias('mapped_category_id'), F.count('*').alias('mapping_evidence_rows'))
        .filter(F.col('category_count') == 1))


def attach_reference(rows, dictionary, mapping, valid_ids, quarantine_ids):
    codes = dictionary.filter(F.col('CODE_TYPE').isin('ITEM_MODEL', 'BANNED_WORD')).select(
        F.col('STD_CODE').alias('_item_code'), F.col('CODE_TYPE').alias('_item_type'), F.col('BANNED_WORD_YN').alias('_item_banned'))
    rows = rows.join(codes, F.col('ITEM_STD_CODE') == F.col('_item_code'), 'left').join(mapping, 'ITEM_STD_CODE', 'left')
    missing = F.col('normalized_parent_cat_id_status') == 'MISSING_CANDIDATE'
    recovery_status = (F.when(~missing, 'NOT_MISSING')
        .when(F.col('_item_banned') == 'Y', 'BANNED_PRODUCT')
        .when(F.col('_item_code').isNull() | (F.col('_item_type') != 'ITEM_MODEL'), 'PRODUCT_CODE_REVIEW')
        .when(~F.coalesce(F.col('original_name_check_status').isin(NAME_EVIDENCE), F.lit(False)), 'PRODUCT_NAME_REVIEW')
        .when(F.col('mapped_category_id').isNull(), 'NO_SINGLE_CATEGORY_MAPPING')
        .when(F.col('_item_banned').isNull() | (F.col('_item_banned') != 'N'), 'DICTIONARY_FLAG_REVIEW')
        .otherwise('RECOVERABLE_CANDIDATE'))
    rows = rows.withColumn('category_recovery_status', recovery_status)
    recovered = F.col('category_recovery_status') == 'RECOVERABLE_CANDIDATE'
    rows = (rows.withColumn('effective_parent_cat_id', F.when(recovered, F.col('mapped_category_id')).otherwise(F.col('normalized_parent_cat_id')))
        .withColumn('category_reference_origin', F.when(recovered, 'RECOVERY_CANDIDATE').otherwise('ORIGINAL_OR_NORMALIZED'))
        .join(valid_ids, F.col('effective_parent_cat_id') == F.col('_valid_cat_id'), 'left')
        .join(quarantine_ids, F.col('effective_parent_cat_id') == F.col('_quarantine_cat_id'), 'left'))
    status = (F.when(F.col('effective_parent_cat_id').isNull() & missing, 'MISSING_REFERENCE')
        .when(F.col('effective_parent_cat_id').isNull(), 'UNRESOLVED_REFERENCE')
        .when(F.col('_valid_cat_id').isNotNull(), 'SILVER_MASTER_FOUND')
        .when(F.col('_quarantine_cat_id').isNotNull(), 'MASTER_QUARANTINED').otherwise('MASTER_KEY_NOT_FOUND'))
    return (rows.withColumn('category_reference_status', status)
        .withColumn('category_rules_passed', F.col('category_reference_status') == 'SILVER_MASTER_FOUND')
        .withColumn('_category_rule_version', F.lit('category_v1_sql_23_24_31_32_44_45_46'))
        .drop('_item_code', '_item_type', '_item_banned', '_valid_cat_id', '_quarantine_cat_id'))


def python_master(master_rows):
    prepared, raw_payloads, normalized_payloads, occurrences = [], defaultdict(set), defaultdict(set), Counter()
    for row in sorted(master_rows, key=lambda r: r['_source_record_number']):
        key = python_category(row['CAT_ID'])[1]
        level = row['CAT_LEVEL'].strip(' ').replace(' ', '_').upper() if row['CAT_LEVEL'] is not None else None
        try:
            dt = datetime.strptime(row['REG_DT'].replace('/', '-'), '%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            dt = None
        payload = (row['CAT_NM'], level, dt)
        normalized_payloads[key].add(payload)
        raw_payloads[key].add((row['CAT_NM'], row['CAT_LEVEL'], row['REG_DT']))
        occurrences[key] += 1
        prepared.append((row, key, payload, occurrences[key]))
    by_row, valid, quarantined = {}, {}, set()
    for row, key, payload, occurrence in prepared:
        name, level, dt = payload
        if key is None:
            reason = 'UNRESOLVED_CATEGORY_ID'
        elif name is None or not name.strip(' '):
            reason = 'MISSING_CATEGORY_NAME'
        elif len(key) > 20 or len(name) > 100:
            reason = 'DECLARED_LENGTH_EXCEEDED'
        elif level not in {'TOP_LEVEL', 'BRAND_LINE'}:
            reason = 'UNSUPPORTED_CATEGORY_LEVEL'
        elif dt is None:
            reason = 'UNRESOLVED_REGISTRATION_DATETIME'
        elif len(normalized_payloads[key]) > 1:
            reason = 'MASTER_KEY_CONTENT_CONFLICT'
        elif occurrence > 1:
            reason = 'IDENTICAL_REPEAT'
        else:
            reason = 'MASTER_CHECKS_PASSED'
        disposition = 'SILVER_CANDIDATE' if reason == 'MASTER_CHECKS_PASSED' else 'EXCLUDED_CANDIDATE' if reason == 'IDENTICAL_REPEAT' else 'QUARANTINE_CANDIDATE'
        by_row[row['_source_record_number']] = (key, *payload, reason, disposition)
        if disposition == 'SILVER_CANDIDATE':
            valid[key] = payload
        elif disposition == 'QUARANTINE_CANDIDATE' and key is not None:
            quarantined.add(key)
    eligible = {key for key, values in raw_payloads.items() if key is not None and len(values) == 1}
    return by_row, valid, quarantined, eligible


def main():
    root = Path(__file__).resolve().parents[1]
    latest = json.loads((root / 'results/lesson-06-latest.json').read_text(encoding='utf-8'))
    if not latest.get('all_passed'):
        raise ValueError('먼저 실습 06을 통과해야 합니다.')
    for key in ['run_id', 'bronze_run_id']:
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', latest[key]):
            raise ValueError('잘못된 선행 실행 ID')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-06' / latest['run_id'] / 'inspection_product_candidates'
    bronze = runtime / 'lesson-02' / latest['bronze_run_id'] / 'bronze'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-07' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (SparkSession.builder.appName('KREAM-Delta-Lesson-07-Categories').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy')).config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
        .config('spark.sql.shuffle.partitions', '2').config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        def read(path):
            return spark.read.format('delta').option('versionAsOf', 0).load(str(path))
        previous = read(source)
        master = read(bronze / 'bronze_category_parent_lookup')
        dictionary = read(bronze / 'bronze_product_std_dict')
        relevant = dictionary.filter(F.col('CODE_TYPE').isin('ITEM_MODEL', 'BANNED_WORD'))
        if relevant.count() != relevant.select('STD_CODE').distinct().count():
            raise ValueError('상품 코드 사전 중복')
        prepare_master(master).write.format('delta').mode('errorifexists').save(str(run / 'category_master_stage'))
        staged = read(run / 'category_master_stage')
        valid = staged.filter(F.col('category_master_disposition') == 'SILVER_CANDIDATE')
        if valid.count() != valid.select('normalized_cat_id').distinct().count():
            raise ValueError('정상 카테고리 키 중복')
        valid.write.format('delta').mode('errorifexists').save(str(run / 'valid_category_master'))
        valid_ids = valid.select(F.col('normalized_cat_id').alias('_valid_cat_id'))
        quarantine_ids = staged.filter(F.col('category_master_disposition') == 'QUARANTINE_CANDIDATE').select(F.col('normalized_cat_id').alias('_quarantine_cat_id')).filter(F.col('_quarantine_cat_id').isNotNull()).distinct()
        eligible = staged.filter((F.col('_raw_payload_versions') == 1) & F.col('normalized_cat_id').isNotNull()).select(F.col('normalized_cat_id').alias('_eligible_cat_id')).distinct()
        normalized = normalize_category(previous, 'PARENT_CAT_ID', 'normalized_parent_cat_id')
        mapping_evidence(normalized, dictionary, eligible).write.format('delta').mode('errorifexists').save(str(run / 'product_category_mapping'))
        mapping = read(run / 'product_category_mapping')
        attach_reference(normalized, dictionary, mapping, valid_ids, quarantine_ids).write.format('delta').mode('errorifexists').save(str(run / 'inspection_category_candidates'))
        saved = read(run / 'inspection_category_candidates')
        py_stage, py_valid, py_quarantined, py_eligible = python_master(master.collect())
        master_fields = ['normalized_cat_id', 'normalized_cat_name', 'normalized_cat_level', 'normalized_reg_dt', 'category_master_reason', 'category_master_disposition']
        actual_master = {r['_source_record_number']: tuple(r[k] for k in master_fields) for r in staged.collect()}
        py_codes = {r['STD_CODE']: r for r in relevant.collect()}
        py_mapping_groups, py_mapping_counts = defaultdict(set), Counter()
        for row in previous.select('ITEM_STD_CODE', 'PARENT_CAT_ID', 'original_name_check_status').toLocalIterator():
            code = py_codes.get(row['ITEM_STD_CODE'])
            cat = python_category(row['PARENT_CAT_ID'])[1]
            if code and code['CODE_TYPE'] == 'ITEM_MODEL' and code['BANNED_WORD_YN'] == 'N' and row['original_name_check_status'] in NAME_EVIDENCE and cat in py_eligible:
                py_mapping_groups[row['ITEM_STD_CODE']].add(cat)
                py_mapping_counts[row['ITEM_STD_CODE']] += 1
        py_mapping = {key: (next(iter(cats)), py_mapping_counts[key]) for key, cats in py_mapping_groups.items() if len(cats) == 1}
        actual_mapping = {r['ITEM_STD_CODE']: (r['mapped_category_id'], r['mapping_evidence_rows']) for r in mapping.collect()}
        mismatches, rows = 0, 0
        fields = ['normalized_parent_cat_id_canonical', 'normalized_parent_cat_id', 'normalized_parent_cat_id_status', 'category_recovery_status', 'effective_parent_cat_id', 'category_reference_origin', 'category_reference_status', 'category_rules_passed']
        for row in saved.select('ITEM_STD_CODE', 'PARENT_CAT_ID', 'original_name_check_status', *fields).toLocalIterator():
            rows += 1
            canonical, cat, norm_status = python_category(row['PARENT_CAT_ID'])
            code = py_codes.get(row['ITEM_STD_CODE'])
            mapped = py_mapping.get(row['ITEM_STD_CODE'])
            if norm_status != 'MISSING_CANDIDATE':
                recovery = 'NOT_MISSING'
            elif code and code['BANNED_WORD_YN'] == 'Y':
                recovery = 'BANNED_PRODUCT'
            elif not code or code['CODE_TYPE'] != 'ITEM_MODEL':
                recovery = 'PRODUCT_CODE_REVIEW'
            elif row['original_name_check_status'] not in NAME_EVIDENCE:
                recovery = 'PRODUCT_NAME_REVIEW'
            elif mapped is None:
                recovery = 'NO_SINGLE_CATEGORY_MAPPING'
            elif code['BANNED_WORD_YN'] != 'N':
                recovery = 'DICTIONARY_FLAG_REVIEW'
            else:
                recovery = 'RECOVERABLE_CANDIDATE'
            effective = mapped[0] if recovery == 'RECOVERABLE_CANDIDATE' else cat
            origin = 'RECOVERY_CANDIDATE' if recovery == 'RECOVERABLE_CANDIDATE' else 'ORIGINAL_OR_NORMALIZED'
            status = ('MISSING_REFERENCE' if norm_status == 'MISSING_CANDIDATE' else 'UNRESOLVED_REFERENCE') if effective is None else 'SILVER_MASTER_FOUND' if effective in py_valid else 'MASTER_QUARANTINED' if effective in py_quarantined else 'MASTER_KEY_NOT_FOUND'
            expected = (canonical, cat, norm_status, recovery, effective, origin, status, status == 'SILVER_MASTER_FOUND')
            mismatches += tuple(row[k] for k in fields) != expected
        master_counts = {r['category_master_disposition']: r['count'] for r in staged.groupBy('category_master_disposition').count().collect()}
        reference_counts = {r['category_reference_status']: r['count'] for r in saved.groupBy('category_reference_status').count().collect()}
        recovery_counts = {r['category_recovery_status']: r['count'] for r in saved.groupBy('category_recovery_status').count().collect()}
        recovered_reference_counts = {r['category_reference_status']: r['count'] for r in saved.filter(F.col('category_reference_origin') == 'RECOVERY_CANDIDATE').groupBy('category_reference_status').count().collect()}
        # 가상 사례: 복구 가능성과 정상 마스터 참조는 별도 조건이다.
        def fixture(records, name, schema):
            path = run / name
            path.write_text(''.join(json.dumps(r, ensure_ascii=True) + '\n' for r in records), encoding='utf-8')
            return spark.read.schema(schema).json(str(path))
        fd = spark.sql("SELECT * FROM VALUES ('A', 'ITEM_MODEL', 'N'), ('B', 'ITEM_MODEL', 'N'), ('C', 'BANNED_WORD', 'Y') AS d(STD_CODE, CODE_TYPE, BANNED_WORD_YN)")
        fm = spark.sql("SELECT * FROM VALUES ('A', 1L, 'CAT_00001', 2L), ('B', 1L, 'CAT_00002', 1L) AS m(ITEM_STD_CODE, category_count, mapped_category_id, mapping_evidence_rows)")
        fv = spark.sql("SELECT 'CAT_00001' AS _valid_cat_id")
        fq = spark.sql("SELECT 'CAT_00002' AS _quarantine_cat_id")
        cases = [
            ('existing', 'CAT_00001', 'A', 'STANDARD_NAME', 'CAT_00001', 'NOT_MISSING', 'SILVER_MASTER_FOUND'),
            ('wide', ' ＣＡＴ＿００００１\u200b', 'A', 'STANDARD_NAME', 'CAT_00001', 'NOT_MISSING', 'SILVER_MASTER_FOUND'),
            ('hyphen', 'cat-00001', 'A', 'STANDARD_NAME', 'CAT_00001', 'NOT_MISSING', 'SILVER_MASTER_FOUND'),
            ('recover', '', 'A', 'REGISTERED_ALIAS', 'CAT_00001', 'RECOVERABLE_CANDIDATE', 'SILVER_MASTER_FOUND'),
            ('recovered_but_quarantined', 'NULL', 'B', 'STANDARD_NAME', 'CAT_00002', 'RECOVERABLE_CANDIDATE', 'MASTER_QUARANTINED'),
            ('banned', '', 'C', 'BANNED_PRODUCT_CODE', None, 'BANNED_PRODUCT', 'MISSING_REFERENCE'),
            ('unknown_item', '', 'X', 'PRODUCT_CODE_REVIEW', None, 'PRODUCT_CODE_REVIEW', 'MISSING_REFERENCE'),
            ('name_not_evidence', '', 'A', 'UNMAPPED_NAME_REVIEW', None, 'PRODUCT_NAME_REVIEW', 'MISSING_REFERENCE'),
            ('invalid_not_filled', '00001', 'A', 'STANDARD_NAME', None, 'NOT_MISSING', 'UNRESOLVED_REFERENCE'),
            ('ghost_not_replaced', 'CAT_99999', 'A', 'STANDARD_NAME', 'CAT_99999', 'NOT_MISSING', 'MASTER_KEY_NOT_FOUND'),
        ]
        fr = fixture([{'case_id': c[0], 'PARENT_CAT_ID': c[1], 'ITEM_STD_CODE': c[2], 'original_name_check_status': c[3]} for c in cases], 'synthetic_references.jsonl', 'case_id string, PARENT_CAT_ID string, ITEM_STD_CODE string, original_name_check_status string')
        actual_cases = attach_reference(normalize_category(fr, 'PARENT_CAT_ID', 'normalized_parent_cat_id'), fd, fm, fv, fq).collect()
        expected_cases = {c[0]: c[4:] for c in cases}
        boundary = {'boundary_' + r['case_id']: (r['effective_parent_cat_id'], r['category_recovery_status'], r['category_reference_status']) == expected_cases[r['case_id']] for r in actual_cases}
        fe = spark.sql("SELECT 'CAT_00001' AS _eligible_cat_id UNION ALL SELECT 'CAT_00002'")
        evidence_rows = fixture([
            {'ITEM_STD_CODE': 'A', 'normalized_parent_cat_id': 'CAT_00001', 'original_name_check_status': 'STANDARD_NAME'},
            {'ITEM_STD_CODE': 'A', 'normalized_parent_cat_id': 'CAT_00002', 'original_name_check_status': 'REGISTERED_ALIAS'},
            {'ITEM_STD_CODE': 'B', 'normalized_parent_cat_id': 'CAT_00001', 'original_name_check_status': 'STANDARD_NAME'},
        ], 'synthetic_evidence.jsonl', 'ITEM_STD_CODE string, normalized_parent_cat_id string, original_name_check_status string')
        boundary_mapping = mapping_evidence(evidence_rows, fd, fe).collect()
        checks = {
            'master_1050_rows_preserved': staged.count() == 1050,
            'master_raw_columns_preserved': master.exceptAll(staged.select(master.columns)).count() == 0 and staged.select(master.columns).exceptAll(master).count() == 0,
            'master_matches_python': actual_master == py_stage,
            'master_counts_match_saved_final': master_counts == {'SILVER_CANDIDATE': 970, 'QUARANTINE_CANDIDATE': 30, 'EXCLUDED_CANDIDATE': 50},
            'valid_master_keys_unique': valid.count() == valid.select('normalized_cat_id').distinct().count() == 970,
            'no_master_content_conflict': staged.filter(F.col('_normalized_payload_versions') > 1).count() == 0,
            'mapping_matches_python': actual_mapping == py_mapping,
            '20_single_category_mappings': len(actual_mapping) == mapping.count() == 20,
            '335000_rows_retained': rows == 335000,
            'source_keys_unique_no_fanout': saved.select('_source_record_id').distinct().count() == rows,
            'upstream_columns_preserved': previous.exceptAll(saved.select(previous.columns)).count() == 0 and saved.select(previous.columns).exceptAll(previous).count() == 0,
            'all_rows_match_python': rows == 335000 and mismatches == 0,
            '1000_missing_recovery_candidates': recovery_counts.get('RECOVERABLE_CANDIDATE', 0) == 1000,
            'invalid_values_never_filled': saved.filter((F.col('normalized_parent_cat_id_status') == 'UNRESOLVED') & (F.col('category_reference_origin') == 'RECOVERY_CANDIDATE')).count() == 0,
            'raw_ghost_values_never_replaced': saved.filter(F.col('normalized_parent_cat_id').isNotNull() & ~F.col('effective_parent_cat_id').eqNullSafe(F.col('normalized_parent_cat_id'))).count() == 0,
            'ambiguous_mapping_not_selected': len(boundary_mapping) == 1 and boundary_mapping[0]['ITEM_STD_CODE'] == 'B',
            'all_boundary_cases_present': len(actual_cases) == len(cases) == len(boundary),
            'upstream_unchanged': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'category_bronze_unchanged': DeltaTable.forPath(spark, str(bronze / 'bronze_category_parent_lookup')).history(1).first()['version'] == 0,
            **boundary,
        }
        result = {'lesson': '07_category_reference', 'run_id': run_id, 'product_run_id': latest['run_id'], 'bronze_run_id': latest['bronze_run_id'],
            'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'scope': 'category_candidates_not_final_inspection_silver',
            'rows': rows, 'category_master_counts': master_counts, 'mapping_rows': len(actual_mapping),
            'category_recovery_counts': recovery_counts, 'category_reference_counts': reference_counts,
            'recovered_candidate_reference_counts': recovered_reference_counts,
            'python_reference_mismatch_rows': mismatches, 'boundary_cases': len(cases),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values())}
        (root / 'results/lesson-07-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('학습용 카테고리 사례 (실제 원본 행이 아님):', flush=True)
        for r in sorted(actual_cases, key=lambda r: r['case_id']):
            print(r['case_id'], r['category_recovery_status'], r['category_reference_status'], flush=True)
        if not result['all_passed']:
            raise RuntimeError('카테고리 검사 실패: results/lesson-07-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
