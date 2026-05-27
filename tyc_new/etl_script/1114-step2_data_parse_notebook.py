# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1114接口 - 数据解析(法律诉讼)
前置条件: Cell1已执行notebook_init, Step1已执行

解析规则：
  result.items数组 → 每条诉讼一行(1:N)
  casePersons取前2人 → role1/gid1/emotion1/sptname1/name1/type1 + role2...
  id → lawsuit_id (避免与表主键冲突)
  submitTime → submit_time (毫秒时间戳→datetime对象)
  company_name 来自搜索入参(非API返回)
  空字符串/0 → NULL
"""

import json, traceback
from datetime import datetime, timedelta
from common.config_loader import load_config, get_interface_name
from common.spark_utils import (
    get_spark, get_today_success_records, write_target_data,
    get_target_table_name, timestamp_to_datetime, null_if_empty
)

INTERFACE_KEY = '1114'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
table_name = get_target_table_name(INTERFACE_KEY)

# target_company = None  # 设为None=全量解析, 或指定公司名如"广东领益智造股份有限公司"

# ========== 解析函数 ==========

def parse_person(person, prefix):
    """从casePersons对象提取字段"""
    if not person:
        return {
            f'{prefix}_role': None, f'{prefix}_gid': None, f'{prefix}_emotion': None,
            f'{prefix}_sptname': None, f'{prefix}_name': None, f'{prefix}_type': None,
        }

    emotion = person.get('emotion')
    if emotion is not None:
        try:
            emotion = int(emotion)
        except (ValueError, TypeError):
            emotion = None

    return {
        f'{prefix}_role': null_if_empty(person.get('role')),
        f'{prefix}_gid': null_if_empty(person.get('gid')),
        f'{prefix}_emotion': emotion,
        f'{prefix}_sptname': null_if_empty(person.get('sptname')),
        f'{prefix}_name': null_if_empty(person.get('name')),
        f'{prefix}_type': null_if_empty(person.get('type')),
    }


def parse_lawsuit_data(api_result, keyword, record_id):
    """展平法律诉讼数据：每条诉讼 → 一行"""
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    total = result.get('total')
    items = result.get('items', [])

    if not items:
        print(f"[INFO] 该公司无诉讼记录: {keyword}")
        return []

    rows = []
    for item in items:
        case_persons = item.get('casePersons', [])
        person1 = case_persons[0] if len(case_persons) > 0 else None
        person2 = case_persons[1] if len(case_persons) > 1 else None

        p1_fields = parse_person(person1, '1')
        p2_fields = parse_person(person2, '2')

        case_result = null_if_empty(person1.get('result')) if person1 else None

        row = {
            'api_record_id': record_id,
            'company_name': keyword,
            'total': total,
            'lawsuit_id': item.get('id'),
            'doc_type': null_if_empty(item.get('docType')),
            'lawsuit_url': null_if_empty(item.get('lawsuitUrl')),
            'lawsuit_h5_url': null_if_empty(item.get('lawsuitH5Url')),
            'title': null_if_empty(item.get('title')),
            'court': null_if_empty(item.get('court')),
            'judge_time': null_if_empty(item.get('judgeTime')),
            'uuid': null_if_empty(item.get('uuid')),
            'case_no': null_if_empty(item.get('caseNo')),
            'case_type': null_if_empty(item.get('caseType')),
            'case_reason': null_if_empty(item.get('caseReason')),
            'case_money': null_if_empty(item.get('caseMoney')),
            'submit_time': timestamp_to_datetime(item.get('submitTime')),
            'case_result': case_result,
            'role1': p1_fields['1_role'],
            'gid1': p1_fields['1_gid'],
            'emotion1': p1_fields['1_emotion'],
            'sptname1': p1_fields['1_sptname'],
            'name1': p1_fields['1_name'],
            'type1': p1_fields['1_type'],
            'role2': p2_fields['2_role'],
            'gid2': p2_fields['2_gid'],
            'emotion2': p2_fields['2_emotion'],
            'sptname2': p2_fields['2_sptname'],
            'name2': p2_fields['2_name'],
            'type2': p2_fields['2_type'],
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
    total_rows = 0
    all_parsed_rows = []

    for i, rec in enumerate(records, 1):
        keyword = rec['input_param']
        print(f"\n[{i}/{len(records)}] {keyword} (record_id={rec['id']})")
        print("-" * 60)

        try:
            api_result = json.loads(rec['output_result_str'])
            rows = parse_lawsuit_data(api_result, keyword, rec['id'])
            print(f"[INFO] 展平后得到 {len(rows)} 条诉讼记录")
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
    print(f"  总诉讼记录数: {total_rows}")
    print("=" * 60)