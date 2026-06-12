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
"""

import json
import os
from datetime import datetime
from typing import Dict, List


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


def should_run_today(config: Dict, interface_key: str) -> bool:
    """
    根据接口频次配置判断今天是否需要调用
    frequency="daily" → 每天都调用
    frequency="monthly" → 仅在月度跑批日期调用(schedule.monthly_day)
    """
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
    """获取预警邮件配置(SMTP设置、收件人等)"""
    return config.get('alert', {})