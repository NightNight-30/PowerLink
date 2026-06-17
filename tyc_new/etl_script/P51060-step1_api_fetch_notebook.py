# -*- coding: utf-8 -*-
"""【Notebook版】邓白氏P51060接口 - API数据拉取
核心改动: 两阶段分离 - API调用(事不过三) + Delta写入(失败直接终止)

与天眼查接口差异：
  - POST请求（天眼查为GET）
  - SHA256签名认证（client_key + client_secret + sku_no + body + timestamp）
  - 搜索参数：entityName + uscc（从819信息表查取）
  - 响应格式：{code, res(JSON字符串), msg, trace}
  - 成功判断：code=0（天眼查为error_code=0）
  - 无结果：code=1（天眼查为error_code=300000）

前置条件: Cell1已执行notebook_init
"""

from common.config_loader import load_config, get_interface_name, get_api_config, get_provider_config, should_run_today, is_prepaid_filter_enabled, get_monthly_day
from common.spark_utils import (get_spark, get_company_list, has_success_today, write_api_records, get_uscc, MAX_RETRY)
import json, requests, hashlib, uuid, time, traceback
from datetime import datetime, timedelta

INTERFACE_KEY = 'P51060'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
CUSTOMER_DT = None  # 指定客户表分区日期，None=自动取MAX(dt)

print("=" * 60)
print(f"【Notebook版】邓白氏{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"客户表分区: {CUSTOMER_DT or '自动(MAX(dt))'}")
print(f"数据源: 邓白氏(POST + SHA256签名)")
print(f"重试策略: 事不过三(最多{MAX_RETRY}次) + 两阶段分离")
print()


# ========== Phase 1: API调用 ==========

def call_api(keyword):
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


def call_api_with_retry(keyword):
    """Phase 1: 事不过三重试，只管API调用，不管写入"""
    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试: {keyword}")
        try:
            api_result = call_api(keyword)
            code = api_result.get('code', -1)
            if isinstance(code, str):
                code = int(code)

            if code == 0:
                print(f"[SUCCESS] API调用成功: {keyword}")
                return ('SUCCESS', api_result)
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
    """两阶段编排: Phase1(API重试) → Phase2(Delta写入,失败即终止)"""
    if has_success_today(spark, keyword, dt, INTERFACE_KEY):
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

# 频次检查: 根据配置判断今天是否需要调用
if not should_run_today(CONFIG, INTERFACE_KEY):
    freq = get_api_config(CONFIG, INTERFACE_KEY).get('frequency', 'daily')
    monthly_day = get_monthly_day(CONFIG)
    print(f"[SKIP] {INTERFACE_KEY}接口频次配置为'{freq}', 月度跑批日为每月{monthly_day}号, 今天不是调用日期, 跳过执行")
else:
    # 预付款过滤 + 获取客户列表
    prepaid_filter = is_prepaid_filter_enabled(CONFIG, INTERFACE_KEY)
    monthly_day = get_monthly_day(CONFIG)
    companies = get_company_list(spark, prepaid_filter=prepaid_filter, monthly_day=monthly_day, customer_dt=CUSTOMER_DT)
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
        print(f"\n下一步: 执行 P51060-step2_data_parse.py 解析数据")

# 如需指定单个公司，取消注释下行:
# companies = ['公司名']