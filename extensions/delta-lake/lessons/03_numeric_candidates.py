"""Bronze 원문을 보존하며 금액·시간 변환 후보와 상태를 만든다.

숫자 관련 두 컬럼만 검사한다. 결과는 최종 Silver나 최종 격리가 아니다.
Spark 결과를 Python Decimal 기준 계산과 전 행 대조하고 경계 사례도 검사한다.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from uuid import uuid4

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, types as T

NULL_TOKENS = ['', 'NULL', 'NONE', 'NA', 'N/A', 'NAN', '-']
PRICE_TYPE = 'decimal(24,4)'
DURATION_TYPE = 'decimal(20,8)'


def numeric_candidates(df):
    """정규식으로 허용 형식을 확인한 뒤 숫자로 변환한다. Python UDF는 사용하지 않는다."""
    p, d = F.col('TRADE_PRICE_KRW'), F.col('INSP_DURATION_MIN')

    def matches(column, pattern):
        # Java 정규식의 절대 끝 경계: 마지막 줄바꿈을 무시하고 통과시키지 않는다.
        return column.rlike(r'\A(?:' + pattern + r')\z')

    digits = r'[0-9]{1,12}(?:\.[0-9]{1,4})?'
    won_number = F.regexp_replace(p, '[,원]', '')
    price = (
        F.when(matches(p, digits), p.cast(PRICE_TYPE))
        .when(matches(p, r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)원') & (F.length(won_number) <= 12), won_number.cast(PRICE_TYPE))
        .when(matches(p, digits + '만원'), F.regexp_replace(p, '만원$', '').cast(PRICE_TYPE) * F.lit(10000))
        .when(matches(p, r'\$' + digits), F.substring(p, 2, 100).cast(PRICE_TYPE) * F.lit(1350))
        .otherwise(F.lit(None).cast(PRICE_TYPE)).cast(PRICE_TYPE)
    )
    duration = (
        F.when(matches(d, r'[0-9]{1,9}(?:\.[0-9]{1,4})?'), d.cast(DURATION_TYPE))
        .when(matches(d, r'[0-9]{1,9}분\(min\)'), F.regexp_replace(d, r'분\(min\)$', '').cast(DURATION_TYPE))
        .when(matches(d, r'[0-9]{1,9}초\(sec\)'), F.regexp_replace(d, r'초\(sec\)$', '').cast(DURATION_TYPE) / F.lit(60))
        .when(matches(d, r'[0-9]{1,9}s'), F.regexp_replace(d, 's$', '').cast(DURATION_TYPE) / F.lit(60))
        .otherwise(F.lit(None).cast(DURATION_TYPE)).cast(DURATION_TYPE)
    )
    result = df.withColumn('price_krw_candidate', price).withColumn('duration_min_candidate', duration)
    return (
        result.withColumn('price_status',
            F.when(F.upper(F.trim(F.coalesce(p, F.lit('')))).isin(NULL_TOKENS), F.lit('MISSING_PRICE'))
            .when(matches(p, r'-[0-9]+(?:\.[0-9]+)?'), F.lit('NEGATIVE_PRICE'))
            .when(F.col('price_krw_candidate').isNull(), F.lit('PRICE_FORMAT_OR_RANGE_REVIEW'))
            .otherwise(F.lit('CONVERTED')))
        .withColumn('duration_status',
            F.when(F.trim(d) == '-999', F.lit('SENTINEL_MINUS_999'))
            .when(F.upper(F.trim(F.coalesce(d, F.lit('')))).isin(NULL_TOKENS), F.lit('MISSING_DURATION'))
            .when(F.col('duration_min_candidate').isNull(), F.lit('DURATION_FORMAT_OR_RANGE_REVIEW'))
            .otherwise(F.lit('CONVERTED')))
        .withColumn('numeric_rules_passed',
            (F.col('price_status') == 'CONVERTED') & (F.col('duration_status') == 'CONVERTED'))
        .withColumn('_numeric_rule_version', F.lit('numeric_v1_usd1350'))
    )


def reference_price(value):
    """기존 audit_bronze.py의 규칙을 Python Decimal로 별도 계산한다."""
    if value is None or value.strip(' ').upper() in NULL_TOKENS:
        return None, 'MISSING_PRICE'
    if re.fullmatch(r'-[0-9]+(?:\.[0-9]+)?', value):
        return None, 'NEGATIVE_PRICE'
    digits = r'[0-9]{1,12}(?:\.[0-9]{1,4})?'
    if re.fullmatch(digits, value):
        amount = Decimal(value)
    elif re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)원', value) and len(value[:-1].replace(',', '')) <= 12:
        amount = Decimal(value[:-1].replace(',', ''))
    elif re.fullmatch(digits + '만원', value):
        amount = Decimal(value[:-2]) * 10000
    elif re.fullmatch(r'\$' + digits, value):
        amount = Decimal(value[1:]) * 1350
    else:
        return None, 'PRICE_FORMAT_OR_RANGE_REVIEW'
    return amount.quantize(Decimal('.0001'), rounding=ROUND_HALF_UP), 'CONVERTED'


def reference_duration(value):
    if value is not None and value.strip(' ') == '-999':
        return None, 'SENTINEL_MINUS_999'
    if value is None or value.strip(' ').upper() in NULL_TOKENS:
        return None, 'MISSING_DURATION'
    if re.fullmatch(r'[0-9]{1,9}(?:\.[0-9]{1,4})?', value):
        minutes = Decimal(value)
    elif re.fullmatch(r'[0-9]{1,9}분\(min\)', value):
        minutes = Decimal(value[:-6])
    elif re.fullmatch(r'[0-9]{1,9}초\(sec\)', value):
        minutes = Decimal(value[:-6]) / 60
    elif re.fullmatch(r'[0-9]{1,9}s', value):
        minutes = Decimal(value[:-1]) / 60
    else:
        return None, 'DURATION_FORMAT_OR_RANGE_REVIEW'
    return minutes.quantize(Decimal('.00000001'), rounding=ROUND_HALF_UP), 'CONVERTED'


def main():
    root = Path(__file__).resolve().parents[1]
    bronze_result = json.loads((root / 'results/lesson-02-latest.json').read_text(encoding='utf-8'))
    if not bronze_result.get('all_passed'):
        raise ValueError('먼저 실습 02의 Bronze 검사를 통과해야 합니다.')
    bronze_run_id = bronze_result['run_id']
    if not re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}', bronze_run_id):
        raise ValueError('잘못된 Bronze run_id')
    runtime = Path(os.environ.get('KREAM_DELTA_RUNTIME_DIR', str(root / '.local'))).resolve()
    source = runtime / 'lesson-02' / bronze_run_id / 'bronze/bronze_inspection'
    if not (source / '_delta_log').is_dir():
        raise FileNotFoundError('실습 02의 Bronze 테이블이 없습니다. 동일 런타임 경로를 사용하세요.')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    run = runtime / 'lesson-03' / run_id
    run.mkdir(parents=True, exist_ok=False)
    builder = (
        SparkSession.builder.appName('KREAM-Delta-Lesson-03-Numeric').master('local[2]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(runtime / 'ivy'))
        .config('spark.sql.warehouse.dir', (run / 'warehouse').as_uri())
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
        .config('spark.databricks.delta.snapshotPartitions', '2').config('spark.ui.enabled', 'false')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        bronze = spark.read.format('delta').option('versionAsOf', 0).load(str(source))
        staged = numeric_candidates(bronze)
        target = run / 'inspection_numeric_candidates'
        staged.write.format('delta').mode('errorifexists').save(str(target))
        saved = spark.read.format('delta').load(str(target))
        price_counts = {r['price_status']: r['count'] for r in saved.groupBy('price_status').count().collect()}
        duration_counts = {r['duration_status']: r['count'] for r in saved.groupBy('duration_status').count().collect()}
        rule_counts = {str(r['numeric_rules_passed']).lower(): r['count'] for r in saved.groupBy('numeric_rules_passed').count().collect()}
        # 전체 행을 한 번에 collect하지 않고 순차 조회하며 별도 계산과 대조한다.
        mismatches = 0
        oracle_rows = 0
        for row in saved.select('TRADE_PRICE_KRW', 'INSP_DURATION_MIN', 'price_krw_candidate', 'duration_min_candidate', 'price_status', 'duration_status').toLocalIterator():
            oracle_rows += 1
            expected = (*reference_price(row['TRADE_PRICE_KRW']), *reference_duration(row['INSP_DURATION_MIN']))
            actual = (row['price_krw_candidate'], row['price_status'], row['duration_min_candidate'], row['duration_status'])
            mismatches += actual != expected
        # 기존 원본 진단값과 비교한다. 현재 코드로 기대 건수를 다시 만들지 않는다.
        baseline = root.parents[1] / 'evidence/asis_business_diagnosis.csv'
        expected_price, expected_duration = {}, {}
        with baseline.open(encoding='utf-8-sig', newline='') as stream:
            for row in csv.DictReader(stream):
                if row['source_file'] != 'kream_inspection_300000.csv':
                    continue
                if row['column_name'] == 'TRADE_PRICE_KRW':
                    expected_price[row['issue']] = int(row['rows'])
                if row['column_name'] == 'INSP_DURATION_MIN':
                    expected_duration[row['issue']] = int(row['rows'])

        # 전 행 검산과 별도로 작은 경계 사례에 고정 기대값을 둔다.
        cases = [
            ('plain', '1000', '15', '1000', 'CONVERTED', '15', 'CONVERTED'),
            ('usd', '$100', '300초(sec)', '135000', 'CONVERTED', '5', 'CONVERTED'),
            ('manwon', '74만원', '90s', '740000', 'CONVERTED', '1.5', 'CONVERTED'),
            ('won', '1,234원', '7분(min)', '1234', 'CONVERTED', '7', 'CONVERTED'),
            ('missing', '', '-999', None, 'MISSING_PRICE', None, 'SENTINEL_MINUS_999'),
            ('negative', '-10', 'NULL', None, 'NEGATIVE_PRICE', None, 'MISSING_DURATION'),
            ('format', '12,34원', '60S', None, 'PRICE_FORMAT_OR_RANGE_REVIEW', None, 'DURATION_FORMAT_OR_RANGE_REVIEW'),
            ('rounding', '$0.0001', '61s', '0.1350', 'CONVERTED', '1.01666667', 'CONVERTED'),
            ('padding', ' 100 ', '1.5초(sec)', None, 'PRICE_FORMAT_OR_RANGE_REVIEW', None, 'DURATION_FORMAT_OR_RANGE_REVIEW'),
            ('nulls', None, None, None, 'MISSING_PRICE', None, 'MISSING_DURATION'),
            ('newline', '100\n', '60s\n', None, 'PRICE_FORMAT_OR_RANGE_REVIEW', None, 'DURATION_FORMAT_OR_RANGE_REVIEW'),
        ]
        case_path = run / 'boundary_cases.jsonl'
        case_path.write_text(''.join(json.dumps({'case_id': c[0], 'TRADE_PRICE_KRW': c[1], 'INSP_DURATION_MIN': c[2]}, ensure_ascii=True) + '\n' for c in cases), encoding='utf-8')
        case_schema = 'case_id string, TRADE_PRICE_KRW string, INSP_DURATION_MIN string'
        case_rows = numeric_candidates(spark.read.schema(case_schema).json(str(case_path))).collect()
        expected_cases = {c[0]: (Decimal(c[3]) if c[3] is not None else None, c[4], Decimal(c[5]) if c[5] is not None else None, c[6]) for c in cases}
        case_checks = {r['case_id']: (r['price_krw_candidate'], r['price_status'], r['duration_min_candidate'], r['duration_status']) == expected_cases[r['case_id']] for r in case_rows}
        checks = {
            'all_335000_rows_retained': saved.count() == 335000,
            'raw_values_and_metadata_retained': bronze.exceptAll(saved.select(bronze.columns)).count() == 0 and saved.select(bronze.columns).exceptAll(bronze).count() == 0,
            'price_status_matches_existing_diagnosis': price_counts == expected_price,
            'duration_status_matches_existing_diagnosis': duration_counts == expected_duration,
            'all_rows_match_python_decimal_reference': oracle_rows == 335000 and mismatches == 0,
            'price_decimal_type': saved.schema['price_krw_candidate'].dataType == T.DecimalType(24, 4),
            'duration_decimal_type': saved.schema['duration_min_candidate'].dataType == T.DecimalType(20, 8),
            'invalid_price_not_replaced_with_zero': saved.filter((F.col('price_status') != 'CONVERTED') & F.col('price_krw_candidate').isNotNull()).count() == 0,
            'invalid_duration_not_replaced_with_zero': saved.filter((F.col('duration_status') != 'CONVERTED') & F.col('duration_min_candidate').isNotNull()).count() == 0,
            'bronze_not_modified': DeltaTable.forPath(spark, str(source)).history(1).first()['version'] == 0,
            'candidate_delta_log_exists': (target / '_delta_log').is_dir(),
            **{'boundary_' + name: ok for name, ok in case_checks.items()},
        }
        result = {
            'lesson': '03_numeric_candidates', 'run_id': run_id, 'bronze_run_id': bronze_run_id,
            'executed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'two_numeric_columns_only_not_final_silver_or_quarantine',
            'fixed_usd_krw_rate': 1350, 'rows': oracle_rows,
            'price_status_counts': price_counts, 'duration_status_counts': duration_counts,
            'numeric_rules_passed_counts': rule_counts,
            'python_reference_mismatches': mismatches, 'boundary_cases': len(cases),
            'checks': checks, 'check_count': len(checks), 'all_passed': all(checks.values()),
        }
        (root / 'results/lesson-03-latest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        print('\n학습용 변환 사례 (실제 원본의 행이 아님):', flush=True)
        numeric_candidates(spark.read.schema(case_schema).json(str(case_path))).select('case_id', 'TRADE_PRICE_KRW', 'price_krw_candidate', 'INSP_DURATION_MIN', 'duration_min_candidate', 'price_status', 'duration_status').orderBy('case_id').show(20, truncate=False)
        if not result['all_passed']:
            raise RuntimeError('숫자 후보 변환 검사 실패: results/lesson-03-latest.json 확인')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
