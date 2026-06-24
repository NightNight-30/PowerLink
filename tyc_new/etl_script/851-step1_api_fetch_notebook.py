# -*- coding: utf-8 -*-
"""【Notebook版】天眼查851接口 - API数据拉取(欠税公告,含翻页)
核心改动: 查询即计费接口，失败不重试，直接返回失败记录(保留翻页逻辑)
前置条件: Cell1已执行notebook_init

851接口支持翻页(pageNum/pageSize)，天眼查最多返回250页/5000条记录
翻页逻辑保留，但失败不重试
"""

from common.config_loader import load_config, get_interface_name, get_api_config, should_run_today, is_prepaid_filter_enabled, get_monthly_day, get_last_monthly_batch_date, is_hk_tw_filter_enabled
from common.spark_utils import (get_spark, get_company_list, has_success_today, write_api_records, get_supplementary_prepaid_companies)
import json, requests, traceback
from datetime import datetime, timedelta

INTERFACE_KEY = '851'
PAGE_SIZE = 20
MAX_PAGES = 250
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
CUSTOMER_DT = None  # 指定客户表分区日期，None=自动取MAX(dt)
INIT_MODE = False  # True=初始化模式:强制全量跑所有客户(含预付款),跳过Phase2
# 初始化模式: monthly接口写入月度分区(下游读月度分区), daily接口保持t-1
if INIT_MODE:
    dt = get_last_monthly_batch_date(CONFIG)

print("=" * 60)
print(f"【Notebook版】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取(含翻页)")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"客户表分区: {CUSTOMER_DT or '自动(MAX(dt))'}")
print(f"初始化模式: {INIT_MODE}")
print(f"翻页策略: pageSize={PAGE_SIZE}, 最多翻页{MAX_PAGES}页, 循环拉取合并存储")
print(f"重试策略: 无(查询即计费，失败直接返回)")
print()


# ========== Phase 1: API调用(含翻页,无重试) ==========

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
    查询即计费：失败不重试，但翻页正常拉取(每一页是单独计费调用)
    """
    try:
        first_page = call_api_page(keyword, 1)
    except requests.RequestException as e:
        error_detail = {
            'error_type': 'HTTP_EXCEPTION', 'error_code': -1,
            'error_msg': str(e), 'traceback': traceback.format_exc()
        }
        print(f"[EXCEPTION] 第1页HTTP请求失败: {e}")
        return ('FAILED', (-1, error_detail))
    except Exception as e:
        error_detail = {
            'error_type': 'OTHER_EXCEPTION', 'error_code': -2,
            'error_msg': str(e), 'traceback': traceback.format_exc()
        }
        print(f"[EXCEPTION] 第1页处理失败: {e}")
        return ('FAILED', (-2, error_detail))

    error_code = first_page.get('error_code', -1)
    if error_code != 0:
        error_msg = first_page.get('reason', '')
        print(f"[FAILED] API返回错误({error_code}): {error_msg}")
        return ('FAILED', (error_code, first_page))

    result = first_page.get('result')
    if not result:
        return ('SUCCESS', first_page)

    total = result.get('total', 0)
    all_items = result.get('items', [])
    print(f"[INFO] 总记录数: {total}, 第1页获取: {len(all_items)}条")

    if total <= PAGE_SIZE:
        print(f"[SUCCESS] API调用成功: {keyword} (total={total})")
        return ('SUCCESS', first_page)

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    if total_pages > MAX_PAGES:
        print(f"[WARNING] 总页数{total_pages}超过上限{MAX_PAGES}, 限制为{MAX_PAGES}页")
        total_pages = MAX_PAGES
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
    items_count = len(all_items)
    print(f"[SUCCESS] API调用成功: {keyword} (total={total}, 实际获取={items_count}条)")
    return ('SUCCESS', merged_result)


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
    write_api_records(spark, [record], dt, INTERFACE_KEY)


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
    write_api_records(spark, [record], dt, INTERFACE_KEY)


# ========== 两阶段编排 ==========

def process_company(keyword):
    """两阶段编排: Phase1(API调用,无重试) → Phase2(Delta写入,失败即终止)"""
    if has_success_today(spark, keyword, dt, INTERFACE_KEY):
        print(f"[SKIP] 当天已有成功记录，跳过: {keyword}")
        return 'SKIP_SUCCESS'

    status, result = call_api_all_pages(keyword)

    if status == 'SUCCESS':
        try:
            write_success_record(keyword, result)
        except Exception as e:
            print(f"[FATAL] Delta写入失败(成功记录): {keyword} - {e}")
            raise
        return 'SUCCESS'
    else:
        try:
            write_failure_record(keyword, result)
        except Exception as e:
            print(f"[FATAL] Delta写入失败(失败记录): {keyword} - {e}")
            raise
        return 'FAILED'


# ========== 执行 ==========

# 频次检查: 根据配置判断今天是否需要调用
if not should_run_today(CONFIG, INTERFACE_KEY, force_run=INIT_MODE):
    freq = get_api_config(CONFIG, INTERFACE_KEY).get('frequency', 'daily')
    monthly_day = get_monthly_day(CONFIG)
    print(f"[SKIP] {INTERFACE_KEY}接口频次配置为'{freq}', 月度跑批日为每月{monthly_day}号, 今天不是调用日期, 跳过执行")
else:
    # 预付款过滤 + 获取客户列表
    prepaid_filter = is_prepaid_filter_enabled(CONFIG, INTERFACE_KEY)
    exclude_hk_tw = is_hk_tw_filter_enabled(CONFIG, INTERFACE_KEY)
    monthly_day = get_monthly_day(CONFIG)
    companies = get_company_list(spark, prepaid_filter=prepaid_filter, monthly_day=monthly_day, customer_dt=CUSTOMER_DT, force_all=INIT_MODE, exclude_hk_tw=exclude_hk_tw)
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


# ========== Phase 2: 补充跑批(新增预付款客户) ==========

monthly_day = get_monthly_day(CONFIG)
last_batch_date = get_last_monthly_batch_date(CONFIG)
supp_companies = get_supplementary_prepaid_companies(spark, INTERFACE_KEY, monthly_day, customer_dt=CUSTOMER_DT, exclude_hk_tw=exclude_hk_tw)

if supp_companies and not INIT_MODE:
    print(f"\n{'=' * 60}")
    print(f"【补充跑批】新增预付款客户 - 写入月度分区dt={last_batch_date}")
    print(f"{'=' * 60}")

    original_dt = dt
    dt = last_batch_date  # 写入月度跑批日分区

    supp_stats = {'SUCCESS': 0, 'FAILED': 0, 'SKIP_SUCCESS': 0}
    for i, company in enumerate(supp_companies, 1):
        print(f"\n[{i}/{len(supp_companies)}] {company} (补充)")
        print("-" * 60)
        result = process_company(company)
        supp_stats[result] += 1

    dt = original_dt  # 恢复原始dt

    print(f"\n补充跑批统计: SUCCESS={supp_stats['SUCCESS']}, FAILED={supp_stats['FAILED']}, SKIP={supp_stats['SKIP_SUCCESS']}")
elif INIT_MODE:
    print("\n[补充跑批] 初始化模式，跳过Phase 2")
else:
    print("\n[补充跑批] 无新增预付款客户需要补充处理")

# 如需指定单个公司，取消注释下行:
# companies = ['公司名']