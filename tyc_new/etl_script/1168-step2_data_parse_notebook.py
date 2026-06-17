# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1168接口 - 数据解析(机构类型)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  orgTypes/economyTypes数组 → level1/level2逗号分隔
  空字符串 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name, get_last_monthly_batch_date, get_monthly_day
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, get_supplementary_prepaid_companies
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

    org_types = (result.get('orgTypes') or []) if result else []
    org_level1_list = [item.get('level1', '') for item in org_types]
    org_level2_list = [item.get('level2', '') for item in org_types]

    economy_types = (result.get('economyTypes') or []) if result else []
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

# ========== Phase 2: 补充跑批解析(新增预付款客户) ==========

monthly_day = get_monthly_day(CONFIG)
last_batch_date = get_last_monthly_batch_date(CONFIG)
supp_companies = get_supplementary_prepaid_companies(spark, INTERFACE_KEY, monthly_day)

if supp_companies:
    print(f"\n{'=' * 60}")
    print(f"【补充跑批】解析新增预付款客户 - 写入月度分区dt={last_batch_date}")
    print(f"{'=' * 60}")

    original_dt = dt
    dt = last_batch_date  # 写入月度跑批日分区

    supp_success = 0
    supp_failed = 0

    for company in supp_companies:
        print(f"\n[补充] {company}")
        print("-" * 60)
        try:
            supp_records = get_today_success_records(spark, dt, INTERFACE_KEY, company_name=company)
            if not supp_records:
                print(f"[WARNING] 补充跑批: {company} 无成功调用记录，跳过解析")
                continue
            company_parsed_rows = []
            for rec in supp_records:
                rows = parse_org_type_data(json.loads(rec['output_result_str']), rec['input_param'], rec['id'])
                if rows is None:
                    continue
                if isinstance(rows, list):
                    company_parsed_rows.extend(rows)
                else:
                    company_parsed_rows.append(rows)
            if company_parsed_rows:
                write_target_data(spark, company_parsed_rows, table_name, dt,
                                  is_one_to_one=True, company_name=company)
                print(f"[SUCCESS] 补充解析入库: {company}, {len(company_parsed_rows)}条")
            supp_success += 1
        except Exception as e:
            print(f"[ERROR] 补充解析失败: {company} - {e}")
            traceback.print_exc()
            supp_failed += 1

    dt = original_dt  # 恢复原始dt

    print(f"\n补充跑批解析统计: SUCCESS={supp_success}, FAILED={supp_failed}")
else:
    print("\n[补充跑批] 无新增预付款客户需要补充解析")