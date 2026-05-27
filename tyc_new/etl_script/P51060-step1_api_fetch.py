#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step1】邓白氏P51060接口 - API数据拉取(含重试机制, Databricks版)

与天眼查接口差异：
  - POST请求（天眼查为GET）
  - SHA256签名认证（client_key + client_secret + sku_no + body + timestamp）
  - 搜索参数：entityName + uscc（从819信息表查取）
  - 响应格式：{code, res(JSON字符串), msg, trace}
  - 成功判断：code=0（天眼查为error_code=0）
  - 无结果：code=1（天眼查为error_code=300000）

执行方式：
  spark-submit P51060-step1_api_fetch.py [公司名]
"""

import sys
import os
import json
import hashlib
import uuid
import time
import requests
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config_loader import load_config, get_api_config, get_interface_name, get_provider_config
from common.spark_utils import (
    get_spark, get_company_list, has_success_today,
    write_api_records, get_uscc, MAX_RETRY
)

INTERFACE_KEY = 'P51060'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)


def call_api(spark, keyword: str) -> Dict[str, Any]:
    """调用邓白氏P51060 PAYDEX接口"""
    api_config = get_api_config(CONFIG, INTERFACE_KEY)
    provider_config = get_provider_config(CONFIG, api_config['provider'])

    client_key = provider_config['client_key']
    client_secret = provider_config['client_secret']
    sku_no = api_config['sku_no']
    timestamp = str(int(time.time() * 1000))
    tid = str(uuid.uuid4())

    # 构建请求body
    uscc = get_uscc(spark, keyword)
    body_dict = {"extraParam": {"entityName": keyword}}
    if uscc:
        body_dict["uscc"] = uscc
        print(f"[INFO] 查到uscc: {uscc}, 连同entityName一起传入")
    else:
        print(f"[INFO] 未查到uscc, 仅传入entityName: {keyword}")

    body = json.dumps(body_dict, ensure_ascii=False)

    # 计算签名: SHA256(client_key + client_secret + sku_no + body + timestamp)
    sign_str = client_key + client_secret + sku_no + body + timestamp
    sign = hashlib.sha256(sign_str.encode('utf-8')).hexdigest()

    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'client_key': client_key,
        'sku_no': sku_no,
        'timestamp': timestamp,
        'sign': sign,
        'tid': tid
    }

    print(f"[INFO] 调用API: {keyword}")
    response = requests.post(
        api_config['url'],
        headers=headers,
        data=body.encode('utf-8'),
        timeout=api_config.get('timeout', 30)
    )
    response.raise_for_status()
    return response.json()


def process_company(spark, keyword: str, dt: str) -> str:
    """处理单个公司: 幂等检查 → 重试 → 写入Delta"""
    if has_success_today(spark, INTERFACE_NAME, keyword, dt):
        print(f"[SKIP] 当天已有成功记录，跳过: {keyword}")
        return 'SKIP_SUCCESS'

    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试: {keyword}")
        try:
            api_result = call_api(spark, keyword)
            code = api_result.get('code', -1)
            if isinstance(code, str):
                code = int(code)

            if code == 0:
                record = {
                    'interface_name': INTERFACE_NAME,
                    'call_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'input_param': keyword,
                    'status_code': 0,
                    'output_result': json.dumps(api_result, ensure_ascii=False),
                }
                write_api_records(spark, [record], dt)
                print(f"[SUCCESS] API调用成功: {keyword}")
                return 'SUCCESS'
            elif code == 1:
                msg = api_result.get('msg', '')
                print(f"[NO_RESULT] API返回无结果(code=1): {msg}")
                last_error = (1, api_result)
            else:
                msg = api_result.get('msg', '')
                print(f"[FAILED] API返回错误(code={code}): {msg}")
                last_error = (code, api_result)
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
    print(f"【Step1】邓白氏{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取(Databricks版)")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"分区dt: {dt}")
    print(f"数据源: 邓白氏(POST + SHA256签名)")
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
        print(f"\n下一步: 执行 P51060-step2_data_parse.py 解析数据")
    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()