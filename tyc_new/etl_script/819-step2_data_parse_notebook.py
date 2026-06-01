# -*- coding: utf-8 -*-
"""【Notebook版】天眼查819接口 - 数据解析
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  Array + child String → 逗号分隔字符串（email_list, history_names）
  Object + 多KV       → 展开每个KV为独立列（industryAll）
  Object + 可能多条   → JSON字符串（staff_list_json）+ 提取total
  Number时间戳        → datetime对象（≥1e10为毫秒）
  简单字段            → 驼峰→下划线命名 + 显式映射
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, camel_to_snake, timestamp_to_datetime,
    array_to_string, null_if_empty
)

INTERFACE_KEY = '819'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 常量定义 ==========

TIMESTAMP_FIELDS = {
    'estiblishTime': 'est_date',
    'fromTime': 'from_date',
    'toTime': 'to_date',
    'approvedTime': 'approval_date',
    'updateTimes': 'update_time',
    'cancelDate': 'cancel_date',
    'revokeDate': 'revoke_date',
}

FIELD_MAPPING = {
    'id': 'company_id',
    'type': 'legal_person_type',
    'orgNumber': 'org_code',
    'creditCode': 'social_credit_code',
    'actualCapital': 'paid_capital',
    'actualCapitalCurrency': 'paid_capital_currency',
    'companyOrgType': 'company_org_type',
    'base': 'province_short',
    'alias': 'company_alias',
    'name': 'company_name_api',
    'property3': 'property3',
    'BRNNumber': 'brn_number',
}

# ========== 解析函数 ==========

def parse_819_data(api_result, keyword, record_id):
    """解析819接口数据，返回一行记录"""
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return None

    parsed = {}

    # 关联字段
    parsed['api_record_id'] = record_id
    parsed['company_name'] = keyword

    # Object + 多KV → 展开每个KV为独立列
    industry_all = result.pop('industryAll', {})
    if isinstance(industry_all, dict):
        for k, v in industry_all.items():
            col_name = f'industry_all_{camel_to_snake(k)}'
            parsed[col_name] = v

    # Object + 可能多条 → JSON字符串 + 提取total
    staff_list = result.pop('staffList', {})
    if isinstance(staff_list, dict):
        parsed['staff_list_total'] = staff_list.get('total')
        staff_result = staff_list.get('result')
        if staff_result:
            parsed['staff_list_json'] = json.dumps(staff_result, ensure_ascii=False)

    # Array + child String → 逗号分隔字符串
    result.pop('historyNames', None)  # 丢弃分号字符串版本
    email_list = result.pop('emailList', None)
    if isinstance(email_list, list):
        parsed['email_list'] = array_to_string(email_list)
    history_name_list = result.pop('historyNameList', None)
    if isinstance(history_name_list, list):
        parsed['history_names'] = array_to_string(history_name_list)

    # 时间戳字段 → datetime对象
    for api_key, db_col in TIMESTAMP_FIELDS.items():
        if api_key in result:
            parsed[db_col] = timestamp_to_datetime(result.pop(api_key))

    # 字段重命名
    for api_key, db_col in FIELD_MAPPING.items():
        if api_key in result:
            parsed[db_col] = result.pop(api_key)

    # 剩余简单字段 → 驼峰转下划线
    for k, v in result.items():
        col_name = camel_to_snake(k)
        if isinstance(v, dict):
            parsed[col_name] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, list):
            parsed[col_name] = array_to_string(v) if all(isinstance(x, str) for x in v) else json.dumps(v, ensure_ascii=False)
        else:
            parsed[col_name] = v

    # 空字符串 → None
    for k, v in parsed.items():
        if v == '':
            parsed[k] = None

    return parsed

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
            parsed = parse_819_data(api_result, keyword, rec['id'])
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

    if all_parsed_rows:
        write_target_data(spark, all_parsed_rows, table_name, dt,
                          is_one_to_one=True, company_name=target_company)

    print("\n" + "=" * 60)
    print("解析完成！")
    print("-" * 60)
    print(f"总计公司数: {len(records)}")
    print(f"  SUCCESS: {success_count}")
    print(f"  FAILED:  {failed_count}")
    print("=" * 60)