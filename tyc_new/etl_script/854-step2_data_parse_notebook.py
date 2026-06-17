# -*- coding: utf-8 -*-
"""【Notebook版】天眼查854接口 - 数据解析(上市信息)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  4个Object字段展开：generalManager/chairman/secretaries/legal 各→type+name+id
  非上市公司API返回error_code=300000，step1记录失败，step2天然跳过
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name, get_last_monthly_batch_date, get_monthly_day
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, null_if_empty, get_supplementary_prepaid_companies
)

INTERFACE_KEY = '854'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def extract_person_fields(person_obj):
    """从Object字段提取type/name/id，空值或id=0转为NULL"""
    if not person_obj:
        return {'type': None, 'name': None, 'id': None}

    c_type = person_obj.get('cType')
    name = person_obj.get('name') or None
    pid = person_obj.get('id')

    if pid in ('0', '', 0, None):
        pid = None
    else:
        try:
            pid = int(pid)
        except (ValueError, TypeError):
            pid = None

    if c_type in (0, None):
        c_type = None
    else:
        try:
            c_type = int(c_type)
        except (ValueError, TypeError):
            c_type = None

    return {'type': c_type, 'name': name, 'id': pid}


def parse_stock_data(api_result, keyword, record_id):
    """解析854接口数据，返回一行记录"""
    result = api_result.get('result')

    gm = extract_person_fields(result.get('generalManager'))
    chairman = extract_person_fields(result.get('chairman'))
    secretary = extract_person_fields(result.get('secretaries'))
    legal = extract_person_fields(result.get('legal'))

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
        'area': null_if_empty(result.get('area')),
        'website': null_if_empty(result.get('website')),
        'stock_code': null_if_empty(result.get('code')),
        'address': null_if_empty(result.get('address')),
        'gm_type': gm['type'],
        'gm_name': gm['name'],
        'gm_id': gm['id'],
        'stock_company_name': null_if_empty(result.get('companyName')),
        'employees_num': null_if_empty(result.get('employeesNum')),
        'main_business': null_if_empty(result.get('mainBusiness')),
        'mobile': null_if_empty(result.get('mobile')),
        'chairman_type': chairman['type'],
        'chairman_name': chairman['name'],
        'chairman_id': chairman['id'],
        'industry': null_if_empty(result.get('industry')),
        'product_name': null_if_empty(result.get('productName')),
        'secretary_type': secretary['type'],
        'secretary_name': secretary['name'],
        'secretary_id': secretary['id'],
        'actual_controller': null_if_empty(result.get('actualController')),
        'controlling_shareholder': null_if_empty(result.get('controllingShareholder')),
        'eng_name': null_if_empty(result.get('engName')),
        'registered_capital': null_if_empty(result.get('registeredCapital')),
        'postalcode': null_if_empty(result.get('postalcode')),
        'legal_person_type': legal['type'],
        'legal_person_name': legal['name'],
        'legal_person_id': legal['id'],
        'listed_name': null_if_empty(result.get('name')),
        'fax': null_if_empty(result.get('fax')),
        'used_name': null_if_empty(result.get('usedName')),
        'final_controller': null_if_empty(result.get('finalController')),
        'introduction': null_if_empty(result.get('introduction')),
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
            row = parse_stock_data(json.loads(rec['output_result_str']), keyword, rec['id'])
            all_parsed_rows.append(row)
            print(f"[SUCCESS] 解析入库: {keyword}")
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
                rows = parse_stock_data(json.loads(rec['output_result_str']), rec['input_param'], rec['id'])
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