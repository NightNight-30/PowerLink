# -*- coding: utf-8 -*-
"""【Notebook版】天眼查851接口 - 数据解析(欠税公告展平)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  result.items数组 → 展平为1:N行
  company_name 来自搜索入参(非API返回)
  items[].name → taxpayer_name (避免与company_name混淆)
  空字符串 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name
)

INTERFACE_KEY = '851'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def parse_tax_arrear_data(api_result, keyword, record_id):
    """展平欠税公告数据：每条欠税 → 一行"""
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    total = result.get('total')
    items = result.get('items') or []

    if not items:
        print(f"[INFO] 该公司无欠税记录: {keyword}")
        return []

    rows = []
    for item in items:
        row = {
            'api_record_id': record_id,
            'company_name': keyword,
            'total': total,
            'tax_id_number': item.get('taxIdNumber') or None,
            'new_own_tax_balance': item.get('newOwnTaxBalance') or None,
            'own_tax_amount': item.get('ownTaxAmount') or None,
            'publish_date': item.get('publishDate') or None,
            'own_tax_balance': item.get('ownTaxBalance') or None,
            'type': item.get('type') or None,
            'person_id_number': item.get('personIdNumber') or None,
            'tax_category': item.get('taxCategory') or None,
            'taxpayer_type': item.get('taxpayerType') or None,
            'person_id_name': item.get('personIdName') or None,
            'taxpayer_name': item.get('name') or None,
            'location': item.get('location') or None,
            'department': item.get('department') or None,
            'reg_type': item.get('regType') or None,
            'legal_person_name': item.get('legalpersonName') or None,
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

records = get_today_success_records(spark, dt, INTERFACE_KEY, company_name=target_company)

if not records:
    print("[WARNING] 没有找到可解析的数据")
else:
    success_count = 0
    failed_count = 0
    total_tax_rows = 0
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword} (record_id={rec['id']})")
        print("-" * 60)

        try:
            api_result = json.loads(rec['output_result_str'])
            rows = parse_tax_arrear_data(api_result, keyword, rec['id'])
            print(f"[INFO] 展平后得到 {len(rows)} 条欠税记录")
            all_parsed_rows.extend(rows)
            total_tax_rows += len(rows)
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
    print(f"  总欠税记录数: {total_tax_rows}")
    print("=" * 60)