# -*- coding: utf-8 -*-
"""【Notebook版】天眼查1001接口 - API数据拉取
1001 = 工商信息(分公司查总公司): 入参分公司名,返回总公司信息(1:1)
频次: daily (每天跑,依赖当天819解析结果过滤分公司)
前置条件: Cell1已执行notebook_init, 819-step2已完成,分公司入参表已重建
入参来源: ods_credit_api_input_branch_company_df (819-step2后由SQL全量重建,已过滤分公司)
"""

from common.config_loader import load_config, get_interface_name, get_api_config, should_run_today, is_prepaid_filter_enabled, get_monthly_day, is_hk_tw_filter_enabled, get_run_dt, is_init_mode
from common.spark_utils import (get_spark, get_branch_company_list, get_new_customers_list, has_success_today, write_api_records, MAX_RETRY)
import json, sys, requests, traceback
from datetime import datetime, timedelta

INTERFACE_KEY = '1001'
CONFIG = load_config()
INTERFACE_NAME = get_interface_name(CONFIG, INTERFACE_KEY)
spark = get_spark()
INIT_MODE = is_init_mode(CONFIG)
dt = get_run_dt(CONFIG, INTERFACE_KEY, init_mode=INIT_MODE)
CUSTOMER_DT = None  # 指定入参表分区日期，None=自动取MAX(dt)
# TARGETED_MODE: Jobs 通过 --targeted_mode=true 传;定向模式从新增客户表读入参(跑当天所有新增客户,含非分公司)
# 交互跑时手动改 True 测定向模式
# 定向模式下1001会对非分公司返回error/null(免费),parent_check只取parent_company_name非空的,自动过滤
TARGETED_MODE = '--targeted_mode=true' in sys.argv

# 初始化模式 + 定向模式同时开启时直接退出:初始化时全量跑批会覆盖所有客户,定向跑批无意义
if TARGETED_MODE and INIT_MODE:
    print("[SKIP] 初始化模式 + 定向模式同时开启,跳过定向跑批(初始化时全量跑批会覆盖所有客户,定向跑批无意义)")
    raise SystemExit(0)

print("=" * 60)
print(f"【Notebook版】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取")
print("=" * 60)
print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"分区dt: {dt}")
print(f"入参表分区: {CUSTOMER_DT or '自动(MAX(dt))'}")
print(f"初始化模式: {INIT_MODE}")
print(f"定向模式(TARGETED_MODE): {TARGETED_MODE}")
print(f"重试策略: 事不过三(最多{MAX_RETRY}次) + 两阶段分离 + normal_error_codes不重试")
print()


# ========== Phase 1: API调用 ==========

def call_api(keyword):
    """调用天眼查1001API (分公司查总公司)"""
    api_config = get_api_config(CONFIG, INTERFACE_KEY)
    headers = {'Authorization': CONFIG['providers'][api_config['provider']]['token']}
    params = {'keyword': keyword}

    print(f"[INFO] 调用API: {keyword}")
    response = requests.get(
        api_config['url'],
        headers=headers,
        params=params,
        timeout=api_config.get('timeout', 30)
    )
    response.raise_for_status()
    return response.json()


def call_api_with_retry(keyword):
    """Phase 1: 事不过三重试，只管API调用，不管写入
    normal_error_codes(如300000)为正常业务错误(非分公司/无总公司),不重试直接退出省时间"""
    api_config = get_api_config(CONFIG, INTERFACE_KEY)
    normal_error_codes = api_config.get('normal_error_codes', [])
    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试: {keyword}")
        try:
            api_result = call_api(keyword)
            error_code = api_result.get('error_code', -1)
            if error_code == 0:
                print(f"[SUCCESS] API调用成功: {keyword}")
                return ('SUCCESS', api_result)
            else:
                error_msg = api_result.get('reason', '')
                print(f"[FAILED] API返回错误({error_code}): {error_msg}")
                last_error = (error_code, api_result)
                # 正常业务错误(如300000=非分公司/无总公司)不重试,直接退出省时间
                # 定向模式下非分公司会触发300000,不重试可省3倍时间
                if error_code in normal_error_codes:
                    print(f"[SKIP] error_code {error_code} 在 normal_error_codes 中,不重试")
                    break
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

    print(f"[FAILED] 已达最大重试次数或遇到正常错误，放弃: {keyword}")
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

# HK/TW过滤(调用前读取白名单,排除港台公司)
exclude_hk_tw = is_hk_tw_filter_enabled(CONFIG, INTERFACE_KEY)

# 频次检查: 根据配置判断今天是否需要调用
if not should_run_today(CONFIG, INTERFACE_KEY, force_run=INIT_MODE):
    freq = get_api_config(CONFIG, INTERFACE_KEY).get('frequency', 'daily')
    monthly_day = get_monthly_day(CONFIG)
    print(f"[SKIP] {INTERFACE_KEY}接口频次配置为'{freq}', 月度跑批日为每月{monthly_day}号, 今天不是调用日期, 跳过执行")
else:
    if TARGETED_MODE:
        # 定向模式: 从新增客户表读入参,跑当天所有新增客户(含非分公司)
        # 非分公司会返回error/null(免费),parent_check只取parent_company_name非空的,自动过滤
        print("[定向模式] 从 ods_credit_api_input_new_customers_df 读入参(含非分公司)")
        companies = get_new_customers_list(spark, customer_dt=CUSTOMER_DT, exclude_hk_tw=exclude_hk_tw)
    else:
        # 正常模式: 从分公司入参表读入参(已过滤分公司)
        # 1001 prepaid_filter=false: 入参表已过滤好,无需再判断预付款
        companies = get_branch_company_list(spark, customer_dt=CUSTOMER_DT, force_all=INIT_MODE, exclude_hk_tw=exclude_hk_tw)
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
        label = '新增客户' if TARGETED_MODE else '分公司'
        print(f"总计: {len(companies)} 家{label}")
        print(f"  SUCCESS:      {stats['SUCCESS']}")
        print(f"  FAILED:       {stats['FAILED']}")
        print(f"  SKIP_SUCCESS: {stats['SKIP_SUCCESS']}")
        print("=" * 60)
        print(f"\n下一步: 执行 {INTERFACE_KEY}-step2_data_parse.py 解析数据")


# 如需指定单个公司，取消注释下行:
# companies = ['分公司名']
