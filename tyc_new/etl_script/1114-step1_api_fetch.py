#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step1】天眼查1114接口 - API数据拉取(含翻页+重试机制, Databricks版)

功能：
  1. 从ads_customer_wide_tab_tmp_df获取公司列表
  2. 调用1114API（支持翻页），合并所有页数据存入ods_api_call_record_df
  3. 幂等检查：当天已有成功记录则跳过
  4. 重试机制：事不过三，失败只保留一条记录

⚠️ 1114接口支持翻页(pageNum/pageSize)，天眼查最多返回500条记录
  step1采用循环翻页策略，合并所有页数据存入一条记录(STRING类型可存储大对象)

执行方式：
  spark-submit 1114-step1_api_fetch.py [公司名]
"""

import sys
import os
import json
import requests
import traceback
from datetime import datetime
from typing import Dict, List, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config_loader import load_config, get_api_config, get_interface_name
from common.spark_utils import (
    get_spark, get_company_list, has_success_today,
    write_api_records, MAX_RETRY, API_RECORD_TABLE
)

INTERFACE_KEY = '1114'
PAGE_SIZE = 20
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)


def call_api_page(keyword: str, page_num: int) -> Dict[str, Any]:
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


def call_api_all_pages(keyword: str) -> Dict[str, Any]:
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


def process_company(spark, keyword: str, dt: str) -> str:
    """处理单个公司: 幂等检查 → 翻页拉取 → 重试 → 写入Delta"""
    if has_success_today(spark, INTERFACE_NAME, keyword, dt):
        print(f"[SKIP] 当天已有成功记录，跳过: {keyword}")
        return 'SKIP_SUCCESS'

    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试: {keyword}")
        try:
            api_result = call_api_all_pages(keyword)
            error_code = api_result.get('error_code', -1)
            if error_code == 0:
                total = api_result.get('result', {}).get('total', 0)
                items_count = len(api_result.get('result', {}).get('items', []))
                record = {
                    'interface_name': INTERFACE_NAME,
                    'call_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'input_param': keyword,
                    'status_code': 0,
                    'output_result': json.dumps(api_result, ensure_ascii=False),
                }
                write_api_records(spark, [record], dt)
                print(f"[SUCCESS] API调用成功: {keyword} (total={total}, 实际获取={items_count}条)")
                return 'SUCCESS'
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

    if last_error:
        error_output = last_error[1] if isinstance(last_error[1], dict) else {'raw_error': str(last_error[1])}
        record = {
            'interface_name': INTERFACE_NAME,
            'call_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'input_param': keyword,
            'status_code': last_error[0],
            'output_result': json.dumps(error_output, ensure_ascii=False),
        }
        write_api_records(spark, [record], dt)
    print(f"[FAILED] 已达最大重试次数({MAX_RETRY})，放弃: {keyword}")
    return 'FAILED'


def main():
    spark = get_spark()
    dt = datetime.now().strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"【Step1】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取(含翻页, Databricks版)")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"分区dt: {dt}")
    print(f"翻页策略: pageSize={PAGE_SIZE}, 循环拉取所有页合并存储")
    print(f"重试策略: 事不过三(最多{MAX_RETRY}次)")
    print()

    try:
        if len(sys.argv) > 1:
            companies = [sys.argv[1]]
            print(f"[INFO] 拉取指定公司: {companies[0]}")
        else:
            companies = get_company_list(spark)

        if not companies:
            print("[WARNING] 没有获取到公司列表，任务结束")
            return

        stats = {'SUCCESS': 0, 'FAILED': 0, 'SKIP_SUCCESS': 0}

        for i, company in enumerate(companies, 1):
            print(f"\n[{i}/{len(companies)}] {company}")
            print("-" * 60)
            result = process_company(spark, company, dt)
            stats[result] += 1

        print("\n" + "=" * 60)
        print("拉取完成！")
        print("-" * 60)
        print(f"总计: {len(companies)} 家公司")
        print(f"  SUCCESS:      {stats['SUCCESS']}")
        print(f"  FAILED:       {stats['FAILED']}")
        print(f"  SKIP_SUCCESS: {stats['SKIP_SUCCESS']}")
        print("=" * 60)
        print(f"\n下一步: 执行 1114-step2_data_parse.py 解析数据")
    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()