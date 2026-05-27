#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证脚本3: 幂等性验证

在Databricks上运行step1两次，验证：
  - 第1次运行: 应调用API并写入记录
  - 第2次运行: 应SKIP_SUCCESS(不重复调用)

执行方式：
  1. 先运行: spark-submit 819-step1_api_fetch.py
  2. 再运行: spark-submit verify_idempotency.py 819
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'etl_script'))

from common.config_loader import load_config, get_interface_name
from common.spark_utils import get_spark, API_RECORD_TABLE, CATALOG, SCHEMA

INTERFACE_KEYS = ['819', '1058', '822', '854', '1168', '1149', '967', '1114', '973', 'P51060']


def verify_idempotency(spark, interface_key: str, dt: str):
    """验证幂等性: 已有成功记录的公司不会被重复调用"""
    config = load_config()
    interface_name = get_interface_name(config, interface_key)

    print(f"\n--- 验证 {interface_key} ({interface_name}) ---")

    # 查询今天的成功记录数
    success_count = spark.sql(
        f"SELECT COUNT(*) FROM {API_RECORD_TABLE} "
        f"WHERE dt = '{dt}' AND interface_name = '{interface_name}' AND status_code = 0"
    ).collect()[0][0]

    # 查询今天的总记录数(包括失败)
    total_count = spark.sql(
        f"SELECT COUNT(*) FROM {API_RECORD_TABLE} "
        f"WHERE dt = '{dt}' AND interface_name = '{interface_name}'"
    ).collect()[0][0]

    print(f"  总记录数: {total_count}, 成功记录数: {success_count}")

    # 检查每个公司是否只有1条成功记录(幂等性保证)
    if success_count > 0:
        duplicate_check = spark.sql(
            f"SELECT input_param, COUNT(*) as cnt FROM {API_RECORD_TABLE} "
            f"WHERE dt = '{dt}' AND interface_name = '{interface_name}' AND status_code = 0 "
            f"GROUP BY input_param HAVING cnt > 1"
        ).collect()

        if len(duplicate_check) == 0:
            print(f"  [OK] 幂等性验证通过: 每个公司最多1条成功记录，不会重复调用")
        else:
            print(f"  [FAIL] 幂等性验证失败: 以下公司有多条成功记录:")
            for row in duplicate_check:
                print(f"    {row.input_param}: {row.cnt}条成功记录")
    else:
        print(f"  [WARN] 无成功记录，请先运行step1")


def main():
    spark = get_spark()
    dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    target_keys = [sys.argv[1]] if len(sys.argv) > 1 else INTERFACE_KEYS

    print("=" * 70)
    print("Databricks ODS层 - 幂等性验证")
    print("=" * 70)
    print(f"验证日期: {dt}")
    print()
    print("说明: 运行step1两次后，应确认每个公司只有1条成功记录")
    print()

    for key in target_keys:
        verify_idempotency(spark, key, dt)

    print("\n" + "=" * 70)
    print("验证完成")
    print("=" * 70)


if __name__ == '__main__':
    main()