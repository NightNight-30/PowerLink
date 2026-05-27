# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1114接口 - API数据拉取(含翻页)
核心改动: 两阶段分离 - API调用(事不过三+翻页) + Delta写入(失败直接终止)
前置条件: Cell1已执行notebook_init

1114接口支持翻页(pageNum/pageSize)，天眼查最多返回500条记录
Phase1采用循环翻页策略，合并所有页数据存入一条记录
"""

from common.config_loader import load_config, get_interface_name, get_api_config
from common.spark_utils import (get_spark, get_company_list, has_success_today, write_api_records, MAX_RETRY)
import json, requests, traceback
from datetime import datetime, timedelta

INTERFACE_KEY = '1114'
PAGE_SIZE = 20
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

print("=" * 60)
print(f"【Notebook版】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取(含翻页)")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"翻页策略: pageSize={PAGE_SIZE}, 循环拉取所有页合并存储")
print(f"重试策略: 事不过三(最多{MAX_RETRY}次) + 两阶段分离")
print()


# ========== Phase 1: API调用(含翻页) ==========

def call_api_page(keyword, page_num):
    """调用单页API"""
    api_config = get_api_config(CONFIG, INTERFACE_KEY)
    headers = {'Authorization': CONFIG['providers'][api_config['provider']]['token']}
    params = {'keyword': keyword, 'pageNum': page_num, 'pageSize': PAGE_SIZE}

    print(f"[INFO] 调用API: {keyword} (第{page_num}页)")
    response = requests.get(
        api_config['url'],
        headers=headers,
        params=params,
        timeout=api_config.get('timeout', 30)
    )
    response.raise_for_status()
    return response.json()


def call_api_all_pages(keyword):
    """
    循环翻页拉取所有数据，合并为完整响应
    返回合并后的完整API响应（error_code=0时包含所有items）
    """
    first_page = call_api_page(keyword, 1)
    error_code = first_page.get('error_code', -1)

    if error_code != 0:
        return first_page

    result = first_page.get('result')
    if not result:
        return first_page

    total = result.get('total', 0)
    all_items = result.get('items', [])
    print(f"[INFO] 总记录数: {total}, 第1页获取: {len(all_items)}条")

    if total <= PAGE_SIZE:
        return first_page

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"[INFO] 需翻页: 共{total_pages}页")

    for page_num in range(2, total_pages + 1):
        try:
            page_result = call_api_page(keyword, page_num)
            page_error_code = page_result.get('error_code', -1)
            if page_error_code != 0:
                print(f"[WARNING] 第{page_num}页返回错误({page_error_code}), 停止翻页")
                break
            page_items = page_result.get('result', {}).get('items', [])
            all_items.extend(page_items)
            print(f"[INFO] 第{page_num}页获取: {len(page_items)}条, 累计: {len(all_items)}条")
        except Exception as e:
            print(f"[WARNING] 第{page_num}页翻页失败: {e}, 停止翻页")
            break

    merged_result = first_page.copy()
    merged_result['result'] = {'total': total, 'items': all_items}
    return merged_result


def call_api_with_retry(keyword):
    """Phase 1: 事不过三重试(含翻页)，只管API调用，不管写入"""
    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试(含翻页): {keyword}")
        try:
            api_result = call_api_all_pages(keyword)
            error_code = api_result.get('error_code', -1)
            if error_code == 0:
                total = api_result.get('result', {}).get('total', 0)
                items_count = len(api_result.get('result', {}).get('items', []))
                print(f"[SUCCESS] API调用成功: {keyword} (total={total}, 实际获取={items_count}条)")
                return ('SUCCESS', api_result)
            else:
                error_msg = api_result.get('reason', '')
                print(f"[FAILED] API返回错误({error_code}): {error_msg}")
                last_error = (error_code, api_result)
        except requests.RequestException as e:
            error_detail = {
                'error_type': 'HTTP_EXCEPTION', 'error_code': -1,
                'error_msg': str(e), 'traceback': traceback.format_exc()
            }
            print(f"[EXCEPTION] HTTP请求失败: {e}")
            last_error = (-1, error_detail)
        except Exception as e:
            error_detail = {
                'error_type': 'OTHER_EXCEPTION', 'error_code': -2,
                'error_msg': str(e), 'traceback': traceback.format_exc()
            }
            print(f"[EXCEPTION] 处理失败: {e}")
            last_error = (-2, error_detail)

    print(f"[FAILED] 已达最大重试次数({MAX_RETRY})，放弃: {keyword}")
    return ('FAILED', last_error)


# ========== Phase 2: Delta写入 ==========

def write_success_record(keyword, api_result):
    """Phase 2(成功): 写入Delta，失败直接终止不重试(节省API配额)"""
    record = {
        'interface_name': INTERFACE_NAME,
        'call_datetime': datetime.now(),
        'input_param': keyword,
        'status_code': 0,
        'output_result': json.dumps(api_result, ensure_ascii=False),
    }
    write_api_records(spark, [record], dt)


def write_failure_record(keyword, error_info):
    """Phase 2(失败): 写入失败记录，失败直接终止不重试(节省API配额)"""
    error_output = error_info[1] if isinstance(error_info[1], dict) else {'raw_error': str(error_info[1])}
    record = {
        'interface_name': INTERFACE_NAME,
        'call_datetime': datetime.now(),
        'input_param': keyword,
        'status_code': error_info[0],
        'output_result': json.dumps(error_output, ensure_ascii=False),
    }
    write_api_records(spark, [record], dt)


# ========== 两阶段编排 ==========

def process_company(keyword):
    """两阶段编排: Phase1(API翻页重试) → Phase2(Delta写入,失败即终止)"""
    if has_success_today(spark, INTERFACE_NAME, keyword, dt):
        print(f"[SKIP] 当天已有成功记录，跳过: {keyword}")
        return 'SKIP_SUCCESS'

    status, result = call_api_with_retry(keyword)

    if status == 'SUCCESS':
        try:
            write_success_record(keyword, result)
        except Exception as e:
            print(f"[FATAL] Delta写入失败(成功记录): {keyword} - {e}")
            raise  # 不重试，直接终止，节省API配额
        return 'SUCCESS'
    else:
        try:
            write_failure_record(keyword, result)
        except Exception as e:
            print(f"[FATAL] Delta写入失败(失败记录): {keyword} - {e}")
            raise  # 不重试，直接终止
        return 'FAILED'


# ========== 执行 ==========

companies = get_company_list(spark)
if not companies:
    print("[WARNING] 没有获取到公司列表，任务结束")
else:
    stats = {'SUCCESS': 0, 'FAILED': 0, 'SKIP_SUCCESS': 0}

    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}] {company}")
        print("-" * 60)
        result = process_company(company)
        stats[result] += 1

    print("\n" + "=" * 60)
    print("拉取完成！")
    print("-" * 60)
    print(f"总计: {len(companies)} 家公司")
    print(f"  SUCCESS:      {stats['SUCCESS']}")
    print(f"  FAILED:       {stats['FAILED']}")
    print(f"  SKIP_SUCCESS: {stats['SKIP_SUCCESS']}")
    print("=" * 60)
    print(f"\n下一步: 执行 {INTERFACE_KEY}-step2_data_parse.py 解析数据")

# 如需指定单个公司，取消注释下行:
# companies = ['公司名']