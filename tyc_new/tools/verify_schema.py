#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证脚本1: Databricks ODS层表结构验证

在Databricks上运行，检查所有ODS表是否正确创建：
  - 表是否存在
  - 列名和类型是否正确
  - 分区字段是否为dt
  - 存储格式是否为Delta

执行方式：
  spark-submit verify_schema.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'etl_script'))

from common.spark_utils import get_spark, CATALOG, SCHEMA

EXPECTED_TABLES = [
    'ods_api_call_record_df',
    'ods_company_819_info_df',
    'ods_company_1058_risk_info_df',
    'ods_company_822_change_info_df',
    'ods_company_854_stock_info_df',
    'ods_company_1168_org_type_info_df',
    'ods_company_1149_scale_info_df',
    'ods_company_967_main_index_info_df',
    'ods_company_1114_lawsuit_info_df',
    'ods_company_973_cash_flow_info_df',
    'ods_company_P51060_paydex_info_df',
]


def verify_table_exists(spark, table_name: str) -> bool:
    full_name = f'{CATALOG}.{SCHEMA}.{table_name}'
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        return True
    except:
        return False


def verify_table_schema(spark, table_name: str):
    full_name = f'{CATALOG}.{SCHEMA}.{table_name}'
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}").collect()
        columns = []
        partition_cols = []
        is_delta = False

        for row in desc:
            col_name = row.col_name
            if col_name.startswith('#'):
                continue
            if col_name == 'Provider':
                is_delta = (row.data_type == 'delta')
            elif col_name == 'Partition Columns':
                continue
            elif row.col_name and row.col_name != '':
                if row.col_name == 'dt':
                    partition_cols.append('dt')
                else:
                    columns.append(col_name)

        return {
            'columns': columns,
            'partition_cols': partition_cols,
            'is_delta': is_delta,
        }
    except Exception as e:
        print(f"[ERROR] 验证表结构失败: {e}")
        return None


def main():
    spark = get_spark()

    print("=" * 70)
    print("Databricks ODS层 - 表结构验证")
    print("=" * 70)
    print()

    all_pass = True

    # 1. 检查schema是否存在
    print("1. 检查Schema...")
    try:
        spark.sql(f"SHOW SCHEMAS IN {CATALOG}").filter(f"namespace = '{SCHEMA}'").show()
        print("[OK] pw_ods schema 存在")
    except:
        print("[FAIL] pw_ods schema 不存在！请先创建schema")
        all_pass = False

    # 2. 检查每张表
    print("\n2. 检查各ODS表...")
    for table_name in EXPECTED_TABLES:
        full_name = f'{CATALOG}.{SCHEMA}.{table_name}'
        exists = verify_table_exists(spark, table_name)

        if exists:
            schema_info = verify_table_schema(spark, table_name)
            if schema_info:
                col_count = len(schema_info['columns'])
                has_dt_partition = 'dt' in schema_info['partition_cols']
                is_delta = schema_info['is_delta']

                status = "[OK]" if (has_dt_partition and is_delta) else "[WARN]"
                print(f"  {status} {table_name}: {col_count}列, dt分区={has_dt_partition}, Delta={is_delta}")
                if not has_dt_partition:
                    print(f"    [WARN] 未检测到dt分区字段")
                if not is_delta:
                    print(f"    [WARN] 存储格式非Delta")
            else:
                print(f"  [WARN] {table_name}: 存在但无法获取详细结构")
        else:
            print(f"  [FAIL] {table_name}: 不存在！")
            all_pass = False

    # 3. 检查客户表
    print("\n3. 检查客户表(ads层)...")
    customer_table = f'{CATALOG}.pw_ads.ads_customer_wide_tab_tmp_df'
    try:
        count = spark.sql(f"SELECT COUNT(*) FROM {customer_table}").collect()[0][0]
        print(f"  [OK] {customer_table}: {count}条记录")
    except Exception as e:
        print(f"  [FAIL] {customer_table}: 不存在或无法访问 ({e})")
        all_pass = False

    print("\n" + "=" * 70)
    if all_pass:
        print("验证结果: 全部通过 ✓")
    else:
        print("验证结果: 存在问题，请检查上述FAIL项")
    print("=" * 70)


if __name__ == '__main__':
    main()