#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查1058接口 - 数据解析（3层嵌套展平）

功能：
  1. 从api_call_record表读取当天成功的调用记录
  2. 按公司名去重：每组取create_time最近的一条
  3. 将3层嵌套数据（riskList→list→list）展平为每条风险记录一行
  4. DELETE旧数据 + INSERT新数据，保证每家公司只保留最新快照

解析规则（参考tesa.ipynb cell 5的json_normalize逻辑，纯Python实现）：
  riskList[].count          → risk_category_count（风险类别下的条数）
  riskList[].name           → risk_category_name（自身风险/周边风险/历史风险/预警提醒）
  riskList[].list[].total   → risk_type_total（风险类型组下的条数）
  riskList[].list[].tag     → risk_type_tag（警示/高风险/提示信息）
  riskList[].list[].list[]  → 展平为独立行（companyId/companyName/id/riskCount/title/type/desc）

特殊处理：
  - main_company_name 来自搜索入参（input_param），非API返回的name字段
  - id 重命名为 risk_id，避免与表主键冲突
  - companyName 为空字符串时转为 NULL

执行方式：
  python3 1058-step2_data_parse.py [公司名]
  - 不指定：解析当天所有成功的调用记录
  - 指定公司名：只解析指定公司
"""

import pymysql
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_NAME = '1058'


def load_config() -> Dict:
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[FATAL] 加载配置文件失败: {e}")
        raise


CONFIG = load_config()


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


# ========== 数据库操作 ==========

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
    每个公司取create_time最近的一条，同时带出record id
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

def parse_risk_data(api_result: Dict, keyword: str, record_id: int) -> List[Dict]:
    """
    3层嵌套展平：riskList → list → list
    等效于 pd.json_normalize(record_path=['list','list'], meta=[...])

    每条最深层的风险条目 → 一行记录
    父层字段作为meta字段带入每行
    """
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    risk_level = result.get('riskLevel')
    if risk_level == '' or risk_level is None:
        risk_level = None
    risk_list = result.get('riskList', [])

    if not risk_list:
        print(f"[INFO] 该公司无风险数据: {keyword}")
        return []

    rows = []
    for risk_category in risk_list:
        count = risk_category.get('count')
        name = risk_category.get('name')
        category_type = risk_category.get('type')

        for risk_type_group in risk_category.get('list', []):
            total = risk_type_group.get('total')
            tag = risk_type_group.get('tag')

            for risk_item in risk_type_group.get('list', []):
                # companyName为空字符串时转为None（存入MySQL为NULL）
                company_name_val = risk_item.get('companyName')
                if company_name_val == '' or company_name_val is None:
                    company_name_val = None

                # companyId为0或None时转为None
                company_id_val = risk_item.get('companyId')
                if company_id_val == 0 or company_id_val is None:
                    company_id_val = None

                row = {
                    'api_record_id': record_id,
                    'main_company_name': keyword,
                    'risk_level': risk_level,
                    'risk_category_count': count,
                    'risk_category_name': name,
                    'risk_type_total': total,
                    'risk_type_tag': tag,
                    'company_id': company_id_val,
                    'company_name': company_name_val,
                    'risk_id': risk_item.get('id'),
                    'risk_count': risk_item.get('riskCount'),
                    'risk_title': risk_item.get('title'),
                    'risk_type': risk_item.get('type'),
                    'risk_desc': risk_item.get('desc'),
                }
                rows.append(row)

    return rows


def delete_old_risk_data(keyword: str):
    """删除该公司的旧风险数据，保证只保留最新快照"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM company_1058_risk_info WHERE main_company_name = %s"
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


def insert_risk_rows(keyword: str, rows: List[Dict], valid_columns: List[str]):
    """批量INSERT风险数据"""
    if not rows:
        print(f"[INFO] 无风险数据可插入: {keyword}")
        return

    # 只保留数据库中存在的列
    columns = [c for c in valid_columns if c != 'id' and c != 'data_create_time']
    placeholders = ', '.join(['%s'] * len(columns))
    col_str = ', '.join(columns)

    # 构建批量INSERT
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
            INSERT INTO company_1058_risk_info ({col_str})
            VALUES ({placeholders})
            """
            cursor.executemany(sql, values_list)
            conn.commit()
            print(f"[INFO] 插入风险数据: {keyword} ({len(rows)}条)")
    except Exception as e:
        print(f"[ERROR] 插入风险数据失败 {keyword}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("=" * 60)
    print(f"【Step2】天眼查{INTERFACE_NAME}接口 - 数据解析（3层嵌套展平）")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("解析规则:")
    print("  riskList → list → list  3层展平为1:N行")
    print("  main_company_name 来自搜索入参(非API返回)")
    print("  id → risk_id (避免与表主键冲突)")
    print("  入库方式: DELETE旧数据 + INSERT新数据")
    print()

    try:
        valid_columns = get_table_columns('company_1058_risk_info')
        print(f"[INFO] 数据库表字段数: {len(valid_columns)} 个")

        target_company = sys.argv[1] if len(sys.argv) > 1 else None
        if target_company:
            print(f"[INFO] 解析指定公司: {target_company}")

        records = get_today_success_records(target_company)

        if not records:
            print("[WARNING] 没有找到可解析的数据")
            print("提示：先执行 1058-step1_api_fetch.py 拉取数据")
            return

        success_count = 0
        failed_count = 0
        total_risk_rows = 0

        for i, (record_id, company_name, result_json, create_time) in enumerate(records, 1):
            print(f"\n[{i}/{len(records)}] {company_name} (record_id={record_id})")
            print("-" * 60)

            try:
                api_result = json.loads(result_json)

                # 1. 展平3层嵌套
                rows = parse_risk_data(api_result, company_name, record_id)
                print(f"[INFO] 展平后得到 {len(rows)} 条风险记录")

                if not rows:
                    # 无风险数据，仍需删除旧数据
                    delete_old_risk_data(company_name)
                    success_count += 1
                    continue

                # 2. 删除旧数据
                delete_old_risk_data(company_name)

                # 3. 插入新数据
                insert_risk_rows(company_name, rows, valid_columns)

                total_risk_rows += len(rows)
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
        print(f"  总风险记录数: {total_risk_rows}")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()