#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step1】邓白氏P51060接口 - API数据拉取（含重试机制）

功能：
  1. 从customer_info表获取公司列表
  2. 调用邓白氏PAYDEX接口，原始数据存入api_call_record表
  3. 幂等检查：当天已有成功记录则跳过
  4. 重试机制：事不过三，失败只保留最新一条记录

与天眼查接口差异：
  - POST请求（天眼查为GET）
  - SHA256签名认证（client_key + client_secret + sku_no + body + timestamp）
  - 搜索参数：entityName（公司名）+ uscc（统一社会信用代码，从819表查取）
  - 响应格式：{code, res(JSON字符串), msg, trace}
  - 成功判断：code=0（天眼查为error_code=0）
  - 无结果：code=1（天眼查为error_code=300000）

执行方式：
  python3 P51060-step1_api_fetch.py [公司名]
  - 不指定：从customer_info读取所有公司
  - 指定公司名：只拉取指定公司
"""

import base64
import pymysql
import requests
import json
import hashlib
import uuid
import time
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_KEY = 'P51060'
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


def get_uscc(company_name: str) -> Optional[str]:
    """从company_819_info表查询公司的统一社会信用代码"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT social_credit_code
            FROM company_819_info
            WHERE company_name = %s
              AND social_credit_code IS NOT NULL
              AND social_credit_code != ''
            LIMIT 1
            """
            cursor.execute(sql, (company_name,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"[WARNING] 查询uscc失败: {e}")
        return None
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


def has_success_today(keyword: str) -> bool:
    """检查当天该公司是否已有成功调用记录"""
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


def call_api(keyword: str) -> Dict[str, Any]:
    """调用邓白氏P51060 PAYDEX接口"""
    api_config = get_api_config()
    provider_config = CONFIG['providers'][api_config['provider']]

    client_key = base64.b64decode(provider_config['client_key']).decode('utf-8')
    client_secret = base64.b64decode(provider_config['client_secret']).decode('utf-8')
    sku_no = api_config['sku_no']
    timestamp = str(int(time.time() * 1000))
    tid = str(uuid.uuid4())

    # 构建请求body
    uscc = get_uscc(keyword)
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
    result = response.json()
    return result


def insert_call_record(keyword: str, status_code: int, output_result: Any):
    """插入一条调用记录到api_call_record"""
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
            code = api_result.get('code', -1)

            # code可能是int或str类型，统一处理
            if isinstance(code, str):
                code = int(code)

            if code == 0:
                insert_call_record(keyword, status_code=0, output_result=api_result)
                print(f"[SUCCESS] API调用成功: {keyword}")
                return 'SUCCESS'
            elif code == 1:
                # 无结果（类似天眼查error_code=300000）
                msg = api_result.get('msg', '')
                print(f"[NO_RESULT] API返回无结果(code=1): {msg}")
                last_error = (1, api_result)
            else:
                msg = api_result.get('msg', '')
                print(f"[FAILED] API返回错误(code={code}): {msg}")
                last_error = (code, api_result)

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
    print(f"【Step1】邓白氏{INTERFACE_KEY}接口({INTERFACE_NAME}) - API数据拉取")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"接口: {get_api_config()['name']}")
    print(f"数据源: 邓白氏(POST + SHA256签名)")
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
        print(f"\n下一步: 执行 P51060-step2_data_parse.py 解析数据")

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()