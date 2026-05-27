#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查1149接口 - 数据解析(Databricks版)

解析规则（与原版相同）：
  result → company_scale（企业规模，如"大型"）
  空字符串 → NULL

入库方式：动态分区覆盖（1:1）

执行方式：
  spark-submit 1149-step2_data_parse.py [公司名]
"""

import sys
import os
import json
import traceback
from datetime import datetime
from typing import Dict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, null_if_empty
)

INTERFACE_KEY = '1149'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)


def parse_scale_data(api_result: Dict, keyword: str, record_id: int) -> Dict:
    """解析1149接口数据，返回一行记录"""
    result = api_result.get('result')
    company_scale = result if result else None

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
        'company_scale': company_scale,
    }
    return row


def main():
    spark = get_spark()
    dt = datetime.now().strftime('%Y-%m-%d')
    table_name = get_target_table_name(INTERFACE_KEY)

    print("=" * 60)
    print(f"【Step2】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - 数据解析(Databricks版)")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"分区dt: {dt}")
    print(f"目标表: {table_name}")
    print()

    try:
        target_company = sys.argv[1] if len(sys.argv) > 1 else None
        if target_company:
            print(f"[INFO] 解析指定公司: {target_company}")

        records = get_today_success_records(spark, INTERFACE_NAME, dt, target_company)

        if not records:
            print("[WARNING] 没有获取到成功记录，任务结束")
            return

        stats = {'PARSED': 0, 'ERROR': 0}
        all_parsed_rows = []

        for i, rec in enumerate(records, 1):
            keyword = rec['input_param']
            print(f"\n[{i}/{len(records)}] {keyword}")
            print("-" * 60)

            try:
                row = parse_scale_data(rec['output_result'], keyword, rec['id'])
                all_parsed_rows.append(row)
                print(f"[SUCCESS] 解析入库: {keyword}")
                print(f"  company_scale: {row['company_scale']}")
                stats['PARSED'] += 1
            except Exception as e:
                print(f"[ERROR] 解析失败: {e}")
                traceback.print_exc()
                stats['ERROR'] += 1

        if all_parsed_rows:
            write_target_data(spark, all_parsed_rows, table_name, dt,
                              is_one_to_one=True, company_name=target_company)

        print("\n" + "=" * 60)
        print("解析完成！")
        print("-" * 60)
        print(f"总计: {len(records)} 条成功记录")
        print(f"  PARSED: {stats['PARSED']}")
        print(f"  ERROR:  {stats['ERROR']}")
        print("=" * 60)
    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()