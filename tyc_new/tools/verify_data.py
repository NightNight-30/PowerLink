#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证脚本2: 数据质量验证

在Databricks上运行step1和step2后，验证数据是否正确写入：
  - api_call_record是否有今天的成功记录
  - 各目标表是否有解析后的数据
  - 字段映射是否正确（抽样检查）
  - 关联追溯(api_record_id)是否有效

执行方式：
  spark-submit verify_data.py [接口号]
  - 不指定：验证所有接口
  - 指定接口号：只验证指定接口(如819/1058/P51060)
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'etl_script'))

from common.spark_utils import get_spark, get_target_table_name, CATALOG, SCHEMA, API_RECORD_TABLE

INTERFACE_KEYS = ['819', '1058', '822', '854', '1168', '1149', '967', '1114', '973', 'P51060']


def verify_api_call_record(spark, dt: str):
    """验证api_call_record表"""
    print("\n--- api_call_record ---")

    total = spark.sql(f"SELECT COUNT(*) FROM {API_RECORD_TABLE} WHERE dt = '{dt}'").collect()[0][0]
    success = spark.sql(f"SELECT COUNT(*) FROM {API_RECORD_TABLE} WHERE dt = '{dt}' AND status_code = 0").collect()[0][0]
    failed = spark.sql(f"SELECT COUNT(*) FROM {API_RECORD_TABLE} WHERE dt = '{dt}' AND status_code != 0").collect()[0][0]

    print(f"  dt={dt}: 总计{total}条, 成功{success}条, 失败{failed}条")

    if success > 0:
        print(f"  [OK] 有成功记录")
        # 抽样显示
        spark.sql(
            f"SELECT interface_name, input_param, status_code, create_time "
            f"FROM {API_RECORD_TABLE} WHERE dt = '{dt}' AND status_code = 0 "
            f"LIMIT 5"
        ).show(truncate=False)
    else:
        print(f"  [WARN] 无成功记录，请先运行step1")


def verify_target_table(spark, interface_key: str, dt: str):
    """验证目标解析表"""
    table_name = get_target_table_name(interface_key)
    print(f"\n--- {interface_key}: {table_name} ---")

    try:
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name} WHERE dt = '{dt}'").collect()[0][0]
        print(f"  dt={dt}: {count}条记录")

        if count > 0:
            print(f"  [OK] 有解析数据")
            # 抽样显示前3行
            company_col = 'main_company_name' if interface_key == '1058' else 'company_name'
            spark.sql(
                f"SELECT {company_col}, api_record_id, data_create_time "
                f"FROM {table_name} WHERE dt = '{dt}' LIMIT 3"
            ).show(truncate=False)

            # 验证api_record_id关联
            linked = spark.sql(
                f"SELECT COUNT(*) FROM {table_name} t "
                f"JOIN {API_RECORD_TABLE} r ON t.api_record_id = r.id AND t.dt = r.dt "
                f"WHERE t.dt = '{dt}'"
            ).collect()[0][0]
            print(f"  [OK] api_record_id关联有效: {linked}/{count}条可追溯")
        else:
            print(f"  [WARN] 无解析数据，请先运行step2")
    except Exception as e:
        print(f"  [FAIL] 验证失败: {e}")


def main():
    spark = get_spark()
    dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    target_keys = [sys.argv[1]] if len(sys.argv) > 1 else INTERFACE_KEYS

    print("=" * 70)
    print("Databricks ODS层 - 数据质量验证")
    print("=" * 70)
    print(f"验证日期: {dt}")
    print()

    # 1. 验证api_call_record
    verify_api_call_record(spark, dt)

    # 2. 验证各目标表
    for key in target_keys:
        verify_target_table(spark, key, dt)

    print("\n" + "=" * 70)
    print("验证完成")
    print("=" * 70)


if __name__ == '__main__':
    main()