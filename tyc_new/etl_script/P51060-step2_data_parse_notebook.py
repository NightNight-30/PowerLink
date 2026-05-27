# -*- coding: utf-8 -*-
"""【Notebook版】邓白氏P51060接口 - 数据解析
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  res为JSON字符串，需json.loads()二次解析
  companyHistoryPayDexes(List) → JSON字符串存储
  其余字段 camelCase → snake_case (FIELD_MAPPING显式映射)
  空字符串 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, null_if_empty
)

INTERFACE_KEY = 'P51060'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 常量定义 ==========

FIELD_MAPPING = {
    'companyPayDex': 'company_paydex',
    'companyPayDexDate': 'company_paydex_date',
    'companyHistoryPayDexes': 'company_history_paydexes',
    'industryPayDexDate': 'industry_paydex_date',
    'industryLowerQuartilePayDex': 'industry_lower_quartile_paydex',
    'industryMedianPayDex': 'industry_median_paydex',
    'industryUpperQuartilePayDex': 'industry_upper_quartile_paydex',
    'industryCountNum': 'industry_count_num',
    'industryCompanyPosition': 'industry_company_position',
    'companyAverage': 'company_average',
    'encompanyAverage': 'en_company_average',
    'industryAverage': 'industry_average',
    'enindustryAverage': 'en_industry_average',
}

# ========== 解析函数 ==========

def parse_paydex_data(api_result, keyword, record_id):
    """解析P51060 PAYDEX接口数据，返回一行记录"""
    res_str = api_result.get('res')
    if not res_str:
        print(f"[SKIP] res为空，跳过: {keyword}")
        return None

    try:
        res_data = json.loads(res_str)
    except json.JSONDecodeError as e:
        print(f"[ERROR] res JSON解析失败: {e}")
        return None

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
    }

    for api_key, db_col in FIELD_MAPPING.items():
        val = res_data.get(api_key)
        if api_key == 'companyHistoryPayDexes':
            row[db_col] = json.dumps(val, ensure_ascii=False) if val else None
        else:
            row[db_col] = null_if_empty(val)

    row['uscc'] = null_if_empty(res_data.get('uscc'))
    row['sic2'] = null_if_empty(res_data.get('sic2'))
    row['sic3'] = null_if_empty(res_data.get('sic3'))
    row['sic4'] = null_if_empty(res_data.get('sic4'))

    return row

# ========== 执行 ==========

print("=" * 60)
print(f"【Step2】邓白氏{INTERFACE_KEY}接口({INTERFACE_NAME}) - 数据解析(Notebook版)")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"目标表: {table_name}")
print()

records = get_today_success_records(spark, INTERFACE_NAME, dt, company_name=target_company)

if not records:
    print("[WARNING] 没有获取到成功记录，任务结束")
else:
    stats = {'PARSED': 0, 'SKIP_EMPTY': 0, 'ERROR': 0}
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword}")
        print("-" * 60)

        try:
            row = parse_paydex_data(json.loads(rec['output_result_str']), keyword, rec['id'])
            if row is None:
                stats['SKIP_EMPTY'] += 1
                continue
            all_parsed_rows.append(row)
            print(f"[SUCCESS] 解析入库: {keyword}")
            print(f"  company_paydex: {row.get('company_paydex')}")
            print(f"  company_average: {row.get('company_average')}")
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
    print(f"  PARSED:      {stats['PARSED']}")
    print(f"  SKIP_EMPTY:  {stats['SKIP_EMPTY']}")
    print(f"  ERROR:       {stats['ERROR']}")
    print("=" * 60)