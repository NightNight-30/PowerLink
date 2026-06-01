# -*- coding: utf-8 -*-
"""【Notebook版】天眼查967接口 - 数据解析(主要指标-年度)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  result数组 → 每年度一行(1:N)
  ~28个DECIMAL字段 + show_year
  null值保持null，DECIMAL字段0为有效值不转NULL
  showYear → show_year（驼峰转下划线）
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name
)

INTERFACE_KEY = '967'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 常量定义 ==========

FIELD_MAPPING = {
    'showYear': 'show_year',
}

# ========== 解析函数 ==========

def map_field_name(api_key):
    if api_key in FIELD_MAPPING:
        return FIELD_MAPPING[api_key]
    return api_key


def parse_main_index_data(api_result, keyword, record_id):
    """result数组展平：每个年度对象 → 一行记录"""
    result = api_result.get('result')
    if not result or not isinstance(result, list):
        print(f"[WARNING] result字段为空或非数组，跳过: {keyword}")
        return []

    if not result:
        print(f"[INFO] 该公司无主要指标数据: {keyword}")
        return []

    rows = []
    for year_obj in result:
        row = {
            'api_record_id': record_id,
            'company_name': keyword,
        }
        for api_key, value in year_obj.items():
            db_col = map_field_name(api_key)
            row[db_col] = value  # DECIMAL字段：null保持null，0是有效值

        rows.append(row)

    return rows

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
    print("[WARNING] 没有找到可解析的数据")
else:
    success_count = 0
    failed_count = 0
    total_rows = 0
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword} (record_id={rec['id']})")
        print("-" * 60)

        try:
            api_result = json.loads(rec['output_result_str'])
            rows = parse_main_index_data(api_result, keyword, rec['id'])
            print(f"[INFO] 展平后得到 {len(rows)} 条年度记录")
            all_parsed_rows.extend(rows)
            total_rows += len(rows)
            success_count += 1
        except Exception as e:
            print(f"[ERROR] 解析失败: {e}")
            traceback.print_exc()
            failed_count += 1

    if all_parsed_rows or target_company:
        write_target_data(spark, all_parsed_rows, table_name, dt,
                          is_one_to_one=False, company_name=target_company)

    print("\n" + "=" * 60)
    print("解析完成！")
    print("-" * 60)
    print(f"总计公司数: {len(records)}")
    print(f"  SUCCESS: {success_count}")
    print(f"  FAILED:  {failed_count}")
    print(f"  总年度记录数: {total_rows}")
    print("=" * 60)