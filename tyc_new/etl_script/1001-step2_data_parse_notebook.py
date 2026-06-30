# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1001接口 - 数据解析(工商信息-分公司查总公司)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  result对象 → 一行记录(1:1)
  1001用分公司名查询,返回: result.companyName=分公司名, result.headquarters=总公司信息对象
  headquarters字段(PDF文档): name/id/reg_status/reg_capital/estiblish_time/alias/logo/personLogo/legalPersonName
  命名驼峰/下划线混合,两种都映射; 空字符串→None
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name, get_last_monthly_batch_date, get_monthly_day
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, get_supplementary_prepaid_companies
)

INTERFACE_KEY = '1001'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名(分公司名)

# ========== 常量定义 ==========

# result对象字段驼峰→下划线
# result.companyName = 分公司名(顶层)
# result.headquarters.* = 总公司信息(parent_company_前缀)
# PDF文档里headquarters字段命名驼峰/下划线混合,两种都支持
FIELD_MAPPING = {
    # result顶层
    'companyName':      'company_name',                    # 分公司名
    # headquarters对象(总公司信息)
    'name':             'parent_company_name',             # 总公司名
    'id':               'parent_company_id',               # 总公司id
    'regStatus':        'parent_company_reg_status',       # 经营状态(驼峰)
    'reg_status':       'parent_company_reg_status',       # 经营状态(下划线)
    'estiblishTime':    'parent_company_estiblish_time',   # 成立日期(驼峰)
    'estiblish_time':   'parent_company_estiblish_time',   # 成立日期(下划线)
    'regCapital':       'parent_company_reg_capital',      # 注册资本(驼峰)
    'reg_capital':      'parent_company_reg_capital',      # 注册资本(下划线)
    'alias':            'parent_company_alias',            # 公司简称
    'logo':             'parent_company_logo',             # logo
    'personLogo':       'parent_company_person_logo',      # 法人图片
    'legalPersonName':  'parent_company_legal_person_name',# 法人
}

# ========== 解析函数 ==========

def map_field_name(api_key):
    return FIELD_MAPPING.get(api_key)


def parse_company_info_data(api_result, keyword, record_id):
    """result对象 → 一行记录(1:1)
    1001返回: result.companyName=分公司名, result.headquarters=总公司信息对象
    """
    result = api_result.get('result')
    if not result or not isinstance(result, dict):
        print(f"[WARNING] result字段为空或非对象，跳过: {keyword}")
        return None

    row = {'api_record_id': record_id}

    # companyName在result顶层(分公司名),为空时用入参keyword兜底
    company_name = result.get('companyName')
    row['company_name'] = company_name if company_name else keyword

    # headquarters对象(总公司信息)
    hq = result.get('headquarters')
    if hq and isinstance(hq, dict):
        for api_key, value in hq.items():
            db_col = map_field_name(api_key)
            if db_col:  # 只保留FIELD_MAPPING定义的列
                row[db_col] = value if value != '' else None
    else:
        print(f"[WARNING] headquarters为空,无总公司信息: {keyword}")

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
    print("[WARNING] 没有找到可解析的数据")
else:
    success_count = 0
    failed_count = 0
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword} (record_id={rec['id']})")
        print("-" * 60)

        try:
            api_result = json.loads(rec['output_result_str'])
            parsed = parse_company_info_data(api_result, keyword, rec['id'])
            if parsed:
                all_parsed_rows.append(parsed)
                print(f"[INFO] 解析成功: {keyword} ({len(parsed)}个字段)")
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"[ERROR] 解析失败: {e}")
            traceback.print_exc()
            failed_count += 1

    if all_parsed_rows or target_company:
        write_target_data(spark, all_parsed_rows, table_name, dt,
                          is_one_to_one=True, company_name=target_company)

    print("\n" + "=" * 60)
    print("解析完成！")
    print("-" * 60)
    print(f"总计公司数: {len(records)}")
    print(f"  SUCCESS: {success_count}")
    print(f"  FAILED:  {failed_count}")
    print("=" * 60)

# ========== Phase 2: 补充跑批解析(新增预付款客户) ==========

monthly_day = get_monthly_day(CONFIG)
last_batch_date = get_last_monthly_batch_date(CONFIG)
supp_companies = get_supplementary_prepaid_companies(spark, INTERFACE_KEY, monthly_day)

if supp_companies:
    print(f"\n{'=' * 60}")
    print(f"【补充跑批】解析新增预付款分公司 - 写入月度分区dt={last_batch_date}")
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
                parsed = parse_company_info_data(json.loads(rec['output_result_str']), rec['input_param'], rec['id'])
                if parsed:
                    company_parsed_rows.append(parsed)
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
    print("\n[补充跑批] 无新增预付款分公司需要补充解析")
