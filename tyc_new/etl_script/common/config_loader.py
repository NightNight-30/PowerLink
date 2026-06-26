#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置加载模块 - Databricks版本

从Workspace读取config.json:
  - 默认路径: /Workspace/Shared/powerlink_warehouse/tyc_new/config/config.json
  - 环境变量 TYC_CONFIG_PATH 可覆盖

新增:
  - should_run_today(): 根据接口频次配置判断今天是否需要调用
  - is_prepaid_filter_enabled(): 判断接口是否启用预付款客户过滤
  - get_monthly_day(): 获取月度跑批日期(默认每月5号)
  - is_charge_per_query(): 判断接口是否查询即计费(1168/1114/851=true)
  - get_normal_error_codes(): 获取接口的正常错误码列表(用于预警分析)
  - get_error_code_desc(): 获取错误码描述映射
  - get_alert_config(): 获取预警邮件配置
  - get_prepaid_run_months(): 获取预付款半年跑批月份配置(P51060邓白用,[1,7])
  - get_last_prepaid_batch_date(): 计算最近预付款跑批日分区(Phase2 processed_since截止日)
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional


CONFIG_PATH = os.environ.get(
    'TYC_CONFIG_PATH',
    '/Workspace/Shared/powerlink_warehouse/tyc_new/config/config.json'
)


def load_config(config_path: str = None) -> Dict:
    path = config_path or CONFIG_PATH
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[FATAL] 加载配置文件失败({path}): {e}")
        raise


def get_api_config(config: Dict, interface_key: str) -> Dict:
    return config['apis'][interface_key]


def get_interface_name(config: Dict, interface_key: str) -> str:
    return config['apis'][interface_key]['name']


def get_provider_config(config: Dict, provider: str) -> Dict:
    return config['providers'][provider]


def get_monthly_day(config: Dict) -> int:
    """获取月度跑批日期，默认每月5号"""
    return config.get('schedule', {}).get('monthly_day', 5)


def get_last_monthly_batch_date(config) -> str:
    """
    计算最近月度跑批日跑批时写入的分区日期(yyyyMMdd格式)
    月度跑批日(monthly_day)跑批时dt=t-1，所以分区=monthly_day-1
    今天>=monthly_day: 本月(monthly_day-1) (如今天6月17号,月度5号→6月4号=20260604)
    今天<monthly_day: 上月(monthly_day-1) (如今天6月3号,月度5号→5月4号=20260504)

    这样月度跑批日正常跑批写入的分区 和 非月度跑批日Phase2补充/初始化写入的分区一致,
    下游统一读一个分区即可拿到全部数据(月度跑批日跑的+补充的预付款)
    """
    monthly_day = get_monthly_day(config)
    today = datetime.now()
    if today.day >= monthly_day:
        batch_date = today.replace(day=monthly_day)
    else:
        if today.month == 1:
            batch_date = today.replace(year=today.year - 1, month=12, day=monthly_day)
        else:
            batch_date = today.replace(month=today.month - 1, day=monthly_day)
    # 月度跑批日跑批写入的是t-1分区,所以减1天
    batch_date = batch_date - timedelta(days=1)
    return batch_date.strftime('%Y%m%d')


def should_run_today(config: Dict, interface_key: str, force_run: bool = False) -> bool:
    """
    根据接口频次配置判断今天是否需要调用
    frequency="daily" → 每天都调用
    frequency="monthly" → 仅在月度跑批日期调用(schedule.monthly_day)
    force_run=True → 强制运行,跳过频次检查(初始化模式用)
    """
    if force_run:
        return True
    api_config = get_api_config(config, interface_key)
    frequency = api_config.get('frequency', 'daily')
    if frequency == 'daily':
        return True
    elif frequency == 'monthly':
        monthly_day = get_monthly_day(config)
        return datetime.now().day == monthly_day
    else:
        print(f"[WARNING] 未知频次配置'{frequency}', 默认按daily处理")
        return True


def is_prepaid_filter_enabled(config: Dict, interface_key: str) -> bool:
    """
    判断接口是否启用预付款客户过滤
    prepaid_filter=true → 启用过滤:
      月度跑批日期: 处理全部客户(含预付款)
      非月度跑批日期: 仅处理非预付款客户(is_prepaid=False)
    prepaid_filter=false → 不过滤，处理全部客户
    """
    api_config = get_api_config(config, interface_key)
    return api_config.get('prepaid_filter', False)


def is_hk_tw_filter_enabled(config: Dict, interface_key: str) -> bool:
    """
    判断接口是否启用HK/TW客户过滤(免跑白名单)
    exclude_hk_tw=true → 调用前读取白名单排除HK/TW公司(province_short为'hk'/'tw')
    exclude_hk_tw=false → 不过滤,调用全部客户

    所有接口(含819)默认true: HK/TW属性基本不变,识别后无需重复调用
    新客户不在白名单中,首次仍会被819调用以识别其HK/TW属性,次日加入白名单后所有接口跳过
    默认false: 未配置时不过滤,保证向后兼容
    """
    api_config = get_api_config(config, interface_key)
    return api_config.get('exclude_hk_tw', False)


def get_prepaid_run_months(config: Dict, interface_key: str) -> Optional[List[int]]:
    """
    获取接口的预付款客户跑批月份配置(半年跑批接口用)
    返回月份列表如[1, 7]表示预付款客户仅在1月/7月的月度跑批日调用
    返回None表示未配置,预付款客户每个跑批日都调用(默认行为,账期每月跑/预付款每月跑)

    适用于: P51060(邓白)配[1,7]→预付款半年跑一次(省配额),账期每月跑
    其他接口不配→预付款每月跑批日跑(原行为)
    """
    api_config = get_api_config(config, interface_key)
    return api_config.get('prepaid_run_months', None)


def get_last_prepaid_batch_date(monthly_day: int, prepaid_run_months: List[int]) -> str:
    """
    计算最近的预付款跑批日分区(半年跑批接口的processed_since截止日)
    返回最近一个prepaid_run_months月份的(monthly_day-1)分区日期(yyyyMMdd)

    用于get_supplementary_prepaid_companies判断"上次预付款跑批至今的新增预付款客户":
      - 半年跑批接口的预付款上次调用在半年边界(如1月/7月跑批日),
        所以processed_since截止日=最近半年跑批日分区,而非当月跑批日分区
      - 否则会把所有预付款都当成"未处理"(当月跑批日Phase1没跑预付款)

    例: monthly_day=5, prepaid_run_months=[1,7]
      今天=2026-08-10 → 返回20260704 (最近半年跑批日7月4号分区)
      今天=2026-03-15 → 返回20260104 (今年1月4号分区,1月<=3月)
      今天=2026-07-05 → 返回20260704 (当月即半年月,Phase1已跑全部预付款,Phase2无补充)
    """
    today = datetime.now()
    sorted_months = sorted(prepaid_run_months)
    candidate = None
    for m in sorted_months:
        if m <= today.month:
            candidate = m
    if candidate is None:
        # 当前月比所有配置月份都小(如3月,配置[1,7])→取去年最后一个配置月
        candidate = sorted_months[-1]
        batch_date = datetime(today.year - 1, candidate, monthly_day)
    else:
        batch_date = datetime(today.year, candidate, monthly_day)
    batch_date = batch_date - timedelta(days=1)
    return batch_date.strftime('%Y%m%d')


def is_charge_per_query(config: Dict, interface_key: str) -> bool:
    """
    判断接口是否为查询即计费模式
    charge_per_query=true → 查询即计费: 每次调用都扣费(含失败), 调用次数=总调用
    charge_per_query=false → 查询不计费: 只有成功才扣费, 调用次数=成功数
    适用于: 1168/1114/851/1041为查询即计费, 其余为查询不计费, P51060暂按不计费
    """
    api_config = get_api_config(config, interface_key)
    return api_config.get('charge_per_query', False)


def get_normal_error_codes(config: Dict, interface_key: str) -> List[int]:
    """
    获取接口的正常错误码列表
    正常错误码: API返回这些码表示"正常范围内的失败"(如经查无结果)，不需要预警
    异常错误码: 不在正常列表中的失败码，需要预警
    """
    api_config = get_api_config(config, interface_key)
    return api_config.get('normal_error_codes', [])


def get_error_code_desc(config: Dict, provider: str) -> Dict[str, str]:
    """获取指定provider的错误码描述映射"""
    return config.get('error_code_desc', {}).get(provider, {})


def get_alert_config(config: Dict) -> Dict:
    """获取预警邮件配置(Graph API client_credentials 凭据、收件人等)"""
    return config.get('alert', {})


def get_data_export_config(config: Dict) -> Dict:
    """获取每日数据导出配置(输出目录、保留天数)"""
    return config.get('data_export', {})