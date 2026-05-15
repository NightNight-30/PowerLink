#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step1】天眼查822接口 - API数据拉取（含重试机制）

功能：
  1. 从customer_info表获取公司列表
  2. 调用822API，原始数据存入api_call_record表
  3. 幂等检查：当天已有成功记录则跳过
  4. 重试机制：事不过三，失败只保留最新一条记录

执行方式：
  python3 822-step1_api_fetch.py [公司名]
"""

import pymysql
import requests
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_KEY = '822'
MAX_RETRY = 3


def load_config() -> Dict:
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[FATAL] 加载配置文件失败: {e}")
        raise


CONFIG = load_config()
INTERFACE_NAME = CONFIG['apis'][INTERFACE_KEY]['name']


def get_api_config() -> Dict:
    return CONFIG['apis'][INTERFACE_KEY]


def get_db_connection() -> pymysql.Connection:
    mysql_config = CONFIG.get('mysql', {})
    return pymysql.connect(
        host=mysql_config.get('host', 'localhost'),
        port=mysql_config.get('port', 3306),
        user=mysql_config.get('user', 'root'),
        password=mysql_config.get('password', ''),
        database=mysql_config.get('database', 'powerlink'),
        charset=mysql_config.get('charset', 'utf8mb4')
    )


def get_company_list() -> List[str]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT DISTINCT customer_name
            FROM customer_info
            WHERE customer_name IS NOT NULL
              AND customer_name != ''
            ORDER BY customer_name
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            companies = [row[0] for row in results]
            print(f"[INFO] 从数据库获取到 {len(companies)} 家公司")
            return companies
    except Exception as e:
        print(f"[ERROR] 获取公司列表失败: {e}")
        raise
    finally:
        conn.close()


def has_success_today(keyword: str) -> bool:
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT COUNT(*)
            FROM api_call_record
            WHERE interface_name = %s
              AND input_param = %s
              AND DATE(call_datetime) = %s
              AND status_code = 0
            """
            cursor.execute(sql, (INTERFACE_NAME, keyword, today))
            count = cursor.fetchone()[0]
            return count > 0
    except Exception as e:
        print(f"[WARNING] 检查成功记录失败: {e}")
        return False
    finally:
        conn.close()


def delete_today_failure_records(keyword: str):
    """删除当天该公司在当前接口的所有失败记录（用于只保留最新一条）"""
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            DELETE FROM api_call_record
            WHERE interface_name = %s
              AND input_param = %s
              AND DATE(call_datetime) = %s
              AND status_code != 0
            """
            cursor.execute(sql, (INTERFACE_NAME, keyword, today))
            conn.commit()
    except Exception as e:
        print(f"[WARNING] 删除旧失败记录失败: {e}")
        conn.rollback()
    finally:
        conn.close()


def call_api(keyword: str) -> Dict[str, Any]:
    api_config = get_api_config()
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
    result = response.json()
    return result


def insert_call_record(keyword: str, status_code: int, output_result: Any):
    call_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO api_call_record
              (interface_name, call_datetime, input_param, status_code, output_result)
            VALUES (%s, %s, %s, %s, %s)
            """
            output_json = json.dumps(output_result, ensure_ascii=False) if output_result else None
            cursor.execute(sql, (
                INTERFACE_NAME,
                call_datetime,
                keyword,
                status_code,
                output_json
            ))
            conn.commit()
    except Exception as e:
        print(f"[ERROR] 插入调用记录失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def process_company(keyword: str) -> str:
    """
    处理单个公司的API拉取（含重试逻辑）
    成功：立即插入SUCCESS记录
    失败：重试3次，只保留最新一条失败记录（删旧+插新）
    返回: 'SUCCESS' / 'FAILED' / 'SKIP_SUCCESS'
    """
    # 1. 检查当天是否已有成功记录 → 幂等跳过
    if has_success_today(keyword):
        print(f"[SKIP] 当天已有成功调用记录，跳过: {keyword}")
        return 'SKIP_SUCCESS'

    # 2. 没有成功记录则尝试（即使有失败记录也继续尝试）
    last_error = None
    for attempt in range(1, MAX_RETRY + 1):
        print(f"[INFO] 第{attempt}次尝试: {keyword}")

        try:
            api_result = call_api(keyword)
            error_code = api_result.get('error_code', -1)

            if error_code == 0:
                insert_call_record(keyword, status_code=0, output_result=api_result)
                print(f"[SUCCESS] API调用成功: {keyword}")
                return 'SUCCESS'
            else:
                error_msg = api_result.get('reason', '')
                print(f"[FAILED] API返回错误({error_code}): {error_msg}")
                last_error = (error_code, api_result)

        except requests.RequestException as e:
            error_detail = {
                'error_type': 'HTTP_EXCEPTION',
                'error_code': -1,
                'error_msg': str(e),
                'traceback': traceback.format_exc()
            }
            print(f"[EXCEPTION] HTTP请求失败: {e}")
            last_error = (-1, error_detail)

        except Exception as e:
            error_detail = {
                'error_type': 'OTHER_EXCEPTION',
                'error_code': -2,
                'error_msg': str(e),
                'traceback': traceback.format_exc()
            }
            print(f"[EXCEPTION] 处理失败: {e}")
            last_error = (-2, error_detail)

    # 所有尝试都失败了 → 删除旧的失败记录，只保留最新1条
    delete_today_failure_records(keyword)
    if last_error:
        insert_call_record(keyword, status_code=last_error[0], output_result=last_error[1])
    print(f"[FAILED] 已达最大重试次数({MAX_RETRY})，放弃: {keyword}")
    return 'FAILED'


def main():
    print("=" * 60)
    print(f"【Step1】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"接口: {INTERFACE_NAME}")
    print(f"重试策略: 事不过三(最多{MAX_RETRY}次)")
    print()

    try:
        if len(sys.argv) > 1:
            companies = [sys.argv[1]]
            print(f"[INFO] 拉取指定公司: {companies[0]}")
        else:
            companies = get_company_list()

        if not companies:
            print("[WARNING] 没有获取到公司列表，任务结束")
            return

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
        print(f"\n下一步: 执行 822-step2_data_parse.py 解析数据")

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()