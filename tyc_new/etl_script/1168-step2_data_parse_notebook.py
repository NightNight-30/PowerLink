# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1168接口 - 数据解析(机构类型)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  orgTypes/economyTypes数组 → level1/level2逗号分隔
  空字符串 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name
)

INTERFACE_KEY = '1168'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def parse_org_type_data(api_result, keyword, record_id):
    """解析1168接口数据，返回一行记录"""
    result = api_result.get('result')

    org_types = result.get('orgTypes', []) if result else []
    org_level1_list = [item.get('level1', '') for item in org_types]
    org_level2_list = [item.get('level2', '') for item in org_types]

    economy_types = result.get('economyTypes', []) if result else []
    economy_level1_list = [item.get('level1', '') for item in economy_types]
    economy_level2_list = [item.get('level2', '') for item in economy_types]

    def to_csv_or_none(lst):
        filtered = [v for v in lst if v]
        if not filtered:
            return None
        return ','.join(filtered)

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
        'org_type_level1': to_csv_or_none(org_level1_list),
        'org_type_level2': to_csv_or_none(org_level2_list),
        'economy_type_level1': to_csv_or_none(economy_level1_list),
        'economy_type_level2': to_csv_or_none(economy_level2_list),
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
            row = parse_org_type_data(json.loads(rec['output_result_str']), keyword, rec['id'])
            all_parsed_rows.append(row)
            print(f"[SUCCESS] 解析入库: {keyword}")
            print(f"  org_type_level1: {row['org_type_level1']}")
            print(f"  org_type_level2: {row['org_type_level2']}")
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