#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查1168接口 - 数据解析

功能：
  1. 从api_call_record读取当天成功记录
  2. 解析result中的orgTypes/economyTypes数组
  3. 拆为level1/level2列（逗号分隔），写入company_1168_org_type_info表
  4. 1:1关系，ON DUPLICATE KEY UPDATE

解析规则：
  orgTypes数组 → org_type_level1/org_type_level2（逗号分隔）
  economyTypes数组 → economy_type_level1/economy_type_level2（逗号分隔）
  空字符串 → NULL

执行方式：
  python3 1168-step2_data_parse.py [公司名]
"""

import pymysql
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_KEY = '1168'


def load_config() -> Dict:
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[FATAL] 加载配置文件失败: {e}")
        raise


CONFIG = load_config()
INTERFACE_NAME = CONFIG['apis'][INTERFACE_KEY]['name']


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


def get_success_records(keyword: Optional[str] = None) -> List[Dict]:
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if keyword:
                sql = """
                SELECT id, input_param, output_result
                FROM api_call_record
                WHERE interface_name = %s
                  AND input_param = %s
                  AND DATE(call_datetime) = %s
                  AND status_code = 0
                  AND create_time = (
                    SELECT MAX(create_time)
                    FROM api_call_record r2
                    WHERE r2.interface_name = %s
                      AND r2.input_param = %s
                      AND DATE(r2.call_datetime) = %s
                      AND r2.status_code = 0
                  )
                """
                cursor.execute(sql, (INTERFACE_NAME, keyword, today,
                                      INTERFACE_NAME, keyword, today))
            else:
                sql = """
                SELECT id, input_param, output_result
                FROM api_call_record
                WHERE interface_name = %s
                  AND DATE(call_datetime) = %s
                  AND status_code = 0
                  AND create_time = (
                    SELECT MAX(create_time)
                    FROM api_call_record r2
                    WHERE r2.interface_name = %s
                      AND r2.input_param = api_call_record.input_param
                      AND DATE(r2.call_datetime) = %s
                      AND r2.status_code = 0
                  )
                """
                cursor.execute(sql, (INTERFACE_NAME, today,
                                      INTERFACE_NAME, today))

            rows = cursor.fetchall()
            records = []
            for row in rows:
                output = json.loads(row[2]) if row[2] else None
                records.append({
                    'id': row[0],
                    'input_param': row[1],
                    'output_result': output
                })
            return records
    except Exception as e:
        print(f"[ERROR] 获取成功记录失败: {e}")
        raise
    finally:
        conn.close()


def parse_org_type_data(api_result: Dict, keyword: str, record_id: int) -> Dict:
    """解析1168接口数据，返回一行记录"""
    result = api_result.get('result')

    # orgTypes → level1/level2逗号分隔
    org_types = result.get('orgTypes', []) if result else []
    org_level1_list = [item.get('level1', '') for item in org_types]
    org_level2_list = [item.get('level2', '') for item in org_types]

    # economyTypes → level1/level2逗号分隔
    economy_types = result.get('economyTypes', []) if result else []
    economy_level1_list = [item.get('level1', '') for item in economy_types]
    economy_level2_list = [item.get('level2', '') for item in economy_types]

    def to_csv_or_none(lst):
        """逗号拼接，过滤空值"""
        filtered = [v for v in lst if v]
        if not filtered:
            return None
        return ','.join(filtered)

    row = {
        'api_record_id': record_id,
        'company_name': keyword,
        'org_type_level1': to_csv_or_none(org_level1_list),
        'org_type_level2': to_csv_or_none(org_level2_list),
        'economy_type_level1': to_csv_or_none(economy_level1_list),
        'economy_type_level2': to_csv_or_none(economy_level2_list),
    }
    return row


def insert_org_type_info(row: Dict):
    """1:1关系，ON DUPLICATE KEY UPDATE"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            columns = list(row.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            col_str = ', '.join(columns)

            update_cols = [c for c in columns if c not in ('id', 'company_name')]
            update_str = ', '.join([f'{c}=%s' for c in update_cols])

            sql = f"""
            INSERT INTO company_1168_org_type_info ({col_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_str}
            """
            values = [row[c] for c in columns]
            update_values = [row[c] for c in update_cols]
            cursor.execute(sql, values + update_values)
            conn.commit()
    except Exception as e:
        print(f"[ERROR] 插入数据失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("=" * 60)
    print(f"【Step2】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - 数据解析")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"接口: {INTERFACE_NAME}")
    print()

    try:
        if len(sys.argv) > 1:
            keyword = sys.argv[1]
            print(f"[INFO] 解析指定公司: {keyword}")
            records = get_success_records(keyword)
        else:
            records = get_success_records()

        if not records:
            print("[WARNING] 没有获取到成功记录，任务结束")
            return

        stats = {'PARSED': 0, 'ERROR': 0}

        for i, record in enumerate(records, 1):
            keyword = record['input_param']
            print(f"\n[{i}/{len(records)}] {keyword}")
            print("-" * 60)

            try:
                row = parse_org_type_data(record['output_result'], keyword, record['id'])
                insert_org_type_info(row)
                print(f"[SUCCESS] 解析入库: {keyword}")
                print(f"  org_type_level1: {row['org_type_level1']}")
                print(f"  org_type_level2: {row['org_type_level2']}")
                print(f"  economy_type_level1: {row['economy_type_level1']}")
                print(f"  economy_type_level2: {row['economy_type_level2']}")
                stats['PARSED'] += 1

            except Exception as e:
                print(f"[ERROR] 解析失败: {e}")
                print(traceback.format_exc())
                stats['ERROR'] += 1

        print("\n" + "=" * 60)
        print("解析完成！")
        print("-" * 60)
        print(f"总计: {len(records)} 条成功记录")
        print(f"  PARSED: {stats['PARSED']}")
        print(f"  ERROR:  {stats['ERROR']}")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()