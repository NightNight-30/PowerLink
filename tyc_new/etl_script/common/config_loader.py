#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置加载模块 - Databricks版本

与原版差异：
  - 去除mysql配置段（Databricks用SparkSession替代）
  - 支持从本地文件或DBFS读取config.json
  - CONFIG_PATH可通过环境变量或CLI参数指定
"""

import json
import os
import sys
from typing import Dict


CONFIG_PATH = os.environ.get(
    'TYC_CONFIG_PATH',
    '/dbfs/opt/tyc_new/config.json' if os.path.exists('/dbfs') else 'config.json'
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