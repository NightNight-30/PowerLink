#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查973接口 - 数据解析（现金流量表展平）

功能：
  1. 从api_call_record表读取当天成功的调用记录
  2. 按公司名去重：每组取create_time最近的一条
  3. 将result.corpCashFlow数组展平为每报告期一行
  4. 不提取corpFinancialYears（其信息已在corpCashFlow.showYear体现）
  5. DELETE旧数据 + INSERT新数据

解析规则：
  result.corpCashFlow[] → 展平为N行，每个报告期一行
  每行字段：show_year + 37个现金流字段（全部VARCHAR）
  company_name 来自搜索入参（input_param）
  空字符串 → NULL

执行方式：
  python3 973-step2_data_parse.py [公司名]
"""

import pymysql
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_KEY = '973'

# API返回的字段名已是snake_case，与DB列名一致
# 仅showYear需映射为show_year
FIELD_MAPPING = {
    'showYear': 'show_year',
}


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


def get_table_columns(table_name: str) -> List[str]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC {table_name}")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[WARNING] 获取表字段失败: {e}")
        return []
    finally:
        conn.close()


def get_today_success_records(company_name: str = None) -> List[tuple]:
    """
    从api_call_record读取当天成功记录并去重
    每个公司取create_time最近的一条
    """
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT r.id, r.input_param, r.output_result, r.create_time
            FROM api_call_record r
            INNER JOIN (
                SELECT input_param, MAX(create_time) as max_ct
                FROM api_call_record
                WHERE interface_name = %s
                  AND status_code = 0
                  AND DATE(call_datetime) = %s
                GROUP BY input_param
            ) t ON r.input_param = t.input_param AND r.create_time = t.max_ct
            WHERE r.interface_name = %s
              AND r.status_code = 0
            """
            params = [INTERFACE_NAME, today, INTERFACE_NAME]

            if company_name:
                sql += " AND r.input_param = %s"
                params.append(company_name)

            sql += " ORDER BY r.input_param"
            cursor.execute(sql, params)
            results = cursor.fetchall()
            print(f"[INFO] 从api_call_record读取到 {len(results)} 条去重后的成功记录")
            return results
    except Exception as e:
        print(f"[ERROR] 读取调用记录失败: {e}")
        raise
    finally:
        conn.close()


# ========== 解析逻辑 ==========

def parse_cash_flow_data(api_result: Dict, keyword: str, record_id: int) -> List[Dict]:
    """
    展平：result.corpCashFlow数组，每个报告期 → 一行
    不提取corpFinancialYears
    API返回的字段名已是snake_case，与DB列名一致，仅showYear需映射
    """
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    cash_flow_list = result.get('corpCashFlow', [])
    if not cash_flow_list:
        print(f"[INFO] 该公司无现金流量数据: {keyword}")
        return []

    rows = []
    for item in cash_flow_list:
        row = {'api_record_id': record_id, 'company_name': keyword}
        for api_key, value in item.items():
            db_col = FIELD_MAPPING.get(api_key, api_key)
            row[db_col] = value or None  # 空字符串→None
        rows.append(row)

    return rows


def delete_old_cash_flow_data(keyword: str):
    """删除该公司的旧现金流量数据"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM company_973_cash_flow_info WHERE company_name = %s"
            cursor.execute(sql, (keyword,))
            deleted = cursor.rowcount
            conn.commit()
            print(f"[INFO] 删除旧数据: {keyword} ({deleted}条)")
    except Exception as e:
        print(f"[ERROR] 删除旧数据失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_cash_flow_rows(keyword: str, rows: List[Dict], valid_columns: List[str]):
    """批量INSERT现金流量数据"""
    if not rows:
        print(f"[INFO] 无现金流量数据可插入: {keyword}")
        return

    columns = [c for c in valid_columns if c != 'id' and c != 'data_create_time']
    placeholders = ', '.join(['%s'] * len(columns))
    col_str = ', '.join(columns)

    values_list = []
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                values.append(None)
            else:
                values.append(str(val))
        values_list.append(values)

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = f"""
            INSERT INTO company_973_cash_flow_info ({col_str})
            VALUES ({placeholders})
            """
            cursor.executemany(sql, values_list)
            conn.commit()
            print(f"[INFO] 插入现金流量数据: {keyword} ({len(rows)}条)")
    except Exception as e:
        print(f"[ERROR] 插入现金流量数据失败 {keyword}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("=" * 60)
    print(f"【Step2】天眼查{INTERFACE_KEY}接口({INTERFACE_NAME}) - 数据解析")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("解析规则:")
    print("  result.corpCashFlow数组 → 展平为1:N行(每报告期一行)")
    print("  不提取corpFinancialYears(信息已在showYear体现)")
    print("  company_name 来自搜索入参(非API返回)")
    print("  空字符串 → NULL")
    print("  入库方式: DELETE旧数据 + INSERT新数据")
    print()

    try:
        valid_columns = get_table_columns('company_973_cash_flow_info')
        print(f"[INFO] 数据库表字段数: {len(valid_columns)} 个")

        target_company = sys.argv[1] if len(sys.argv) > 1 else None
        if target_company:
            print(f"[INFO] 解析指定公司: {target_company}")

        records = get_today_success_records(target_company)

        if not records:
            print("[WARNING] 没有找到可解析的数据")
            print("提示：先执行 973-step1_api_fetch.py 拉取数据")
            return

        success_count = 0
        failed_count = 0
        total_cash_flow_rows = 0

        for i, (record_id, company_name, result_json, create_time) in enumerate(records, 1):
            print(f"\n[{i}/{len(records)}] {company_name} (record_id={record_id})")
            print("-" * 60)

            try:
                api_result = json.loads(result_json)

                rows = parse_cash_flow_data(api_result, company_name, record_id)
                print(f"[INFO] 展平后得到 {len(rows)} 条现金流量记录")

                if not rows:
                    delete_old_cash_flow_data(company_name)
                    success_count += 1
                    continue

                delete_old_cash_flow_data(company_name)
                insert_cash_flow_rows(company_name, rows, valid_columns)

                total_cash_flow_rows += len(rows)
                success_count += 1
            except Exception as e:
                print(f"[ERROR] 解析失败: {e}")
                traceback.print_exc()
                failed_count += 1

        print("\n" + "=" * 60)
        print("解析完成！")
        print("-" * 60)
        print(f"总计公司数: {len(records)}")
        print(f"  SUCCESS: {success_count}")
        print(f"  FAILED:  {failed_count}")
        print(f"  总现金流量记录数: {total_cash_flow_rows}")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()