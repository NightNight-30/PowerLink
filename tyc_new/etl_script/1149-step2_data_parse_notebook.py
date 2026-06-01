# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1149接口 - 数据解析(企业规模)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  result → company_scale（企业规模，如"大型"）
  空字符串 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, null_if_empty
)

INTERFACE_KEY = '1149'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def parse_scale_data(api_result, keyword, record_id):
    """解析1149接口数据，返回一行记录"""
    result = api_result.get('result')
    company_scale = result if result else None

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
        'company_scale': company_scale,
    }
    return row

# ========== 执行 ==========

print("=" * 60)
print(f"【Step2】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - 数据解析(Notebook版)")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"目标表: {table_name}")
print()

records = get_today_success_records(spark, dt, INTERFACE_KEY, company_name=target_company)

if not records:
    print("[WARNING] 没有获取到成功记录，任务结束")
else:
    stats = {'PARSED': 0, 'ERROR': 0}
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword}")
        print("-" * 60)

        try:
            row = parse_scale_data(json.loads(rec['output_result_str']), keyword, rec['id'])
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