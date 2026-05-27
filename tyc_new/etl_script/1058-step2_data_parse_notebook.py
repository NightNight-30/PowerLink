# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1058接口 - 数据解析(3层嵌套展平)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  riskList → list → list  3层展平为1:N行
  main_company_name 来自搜索入参(非API返回)
  id → risk_id (避免与表主键冲突)
  companyName为空字符串时转为NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, null_if_empty
)

INTERFACE_KEY = '1058'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

# target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def parse_risk_data(api_result, keyword, record_id):
    """3层嵌套展平：riskList → list → list"""
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    risk_level = null_if_empty(result.get('riskLevel'))
    risk_list = result.get('riskList', [])

    if not risk_list:
        print(f"[INFO] 该公司无风险数据: {keyword}")
        return []

    rows = []
    for risk_category in risk_list:
        count = risk_category.get('count')
        name = risk_category.get('name')

        for risk_type_group in risk_category.get('list', []):
            total = risk_type_group.get('total')
            tag = risk_type_group.get('tag')

            for risk_item in risk_type_group.get('list', []):
                company_name_val = risk_item.get('companyName')
                if company_name_val == '' or company_name_val is None:
                    company_name_val = None
                company_id_val = risk_item.get('companyId')
                if company_id_val == 0 or company_id_val is None:
                    company_id_val = None

                row = {
                    'api_record_id': record_id,
                    'main_company_name': keyword,
                    'risk_level': risk_level,
                    'risk_category_count': count,
                    'risk_category_name': name,
                    'risk_type_total': total,
                    'risk_type_tag': tag,
                    'company_id': company_id_val,
                    'company_name': company_name_val,
                    'risk_id': risk_item.get('id'),
                    'risk_count': risk_item.get('riskCount'),
                    'risk_title': risk_item.get('title'),
                    'risk_type': risk_item.get('type'),
                    'risk_desc': risk_item.get('desc'),
                }
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

records = get_today_success_records(spark, INTERFACE_NAME, dt, company_name=target_company)

if not records:
    print("[WARNING] 没有找到可解析的数据")
else:
    success_count = 0
    failed_count = 0
    total_risk_rows = 0
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword} (record_id={rec['id']})")
        print("-" * 60)

        try:
            api_result = json.loads(rec['output_result_str'])
            rows = parse_risk_data(api_result, keyword, rec['id'])
            print(f"[INFO] 展平后得到 {len(rows)} 条风险记录")
            all_parsed_rows.extend(rows)
            total_risk_rows += len(rows)
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
    print(f"  总风险记录数: {total_risk_rows}")
    print("=" * 60)