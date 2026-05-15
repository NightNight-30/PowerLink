#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查1114接口 - 数据解析（法律诉讼）

功能：
  1. 从api_call_record表读取当天成功的调用记录
  2. 按公司名去重：每组取create_time最近的一条
  3. 将result.items数组展平为每条诉讼记录一行
  4. casePersons取前2人，展开为role1/gid1/emotion1/sptname1/name1/type1 + role2...
  5. DELETE旧数据 + INSERT新数据

解析规则：
  result.total → total（诉讼总数）
  result.items[] → 展平为N行，每条诉讼一行
  id → lawsuit_id（避免与表主键冲突）
  submitTime → submit_time（毫秒时间戳→datetime）
  casePersons[0] → case_result/role1/gid1/emotion1/sptname1/name1/type1
  casePersons[1] → role2/gid2/emotion2/sptname2/name2/type2
  空字符串/0 → NULL
  emotion: 1=正面, 0=中性, -1=负面

执行方式：
  python3 1114-step2_data_parse.py [公司名]
"""

import pymysql
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_KEY = '1114'

FIELD_MAPPING = {
    'id': 'lawsuit_id',
    'docType': 'doc_type',
    'lawsuitUrl': 'lawsuit_url',
    'lawsuitH5Url': 'lawsuit_h5_url',
    'caseNo': 'case_no',
    'caseType': 'case_type',
    'caseReason': 'case_reason',
    'caseMoney': 'case_money',
    'submitTime': 'submit_time',
    'judgeTime': 'judge_time',
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

def convert_timestamp(ts) -> Optional[str]:
    """毫秒时间戳 → datetime字符串"""
    if ts is None:
        return None
    try:
        ts_int = int(ts)
        if ts_int >= 1e10:
            ts_int = ts_int // 1000
        return datetime.fromtimestamp(ts_int).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OSError):
        return None


def null_if_empty(val):
    """空字符串 → None"""
    if val is None or val == '':
        return None
    return val


def parse_person(person: Optional[Dict], prefix: str) -> Dict:
    """从casePersons对象提取字段，prefix为role1/role2等"""
    if not person:
        return {
            f'{prefix}_role': None,
            f'{prefix}_gid': None,
            f'{prefix}_emotion': None,
            f'{prefix}_sptname': None,
            f'{prefix}_name': None,
            f'{prefix}_type': None,
        }

    emotion = person.get('emotion')
    if emotion is not None:
        try:
            emotion = int(emotion)
        except (ValueError, TypeError):
            emotion = None

    return {
        f'{prefix}_role': null_if_empty(person.get('role')),
        f'{prefix}_gid': null_if_empty(person.get('gid')),
        f'{prefix}_emotion': emotion,
        f'{prefix}_sptname': null_if_empty(person.get('sptname')),
        f'{prefix}_name': null_if_empty(person.get('name')),
        f'{prefix}_type': null_if_empty(person.get('type')),
    }


def parse_lawsuit_data(api_result: Dict, keyword: str, record_id: int) -> List[Dict]:
    """
    展平法律诉讼数据：每条诉讼 → 一行
    casePersons取前2人展开
    """
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return []

    total = result.get('total')
    items = result.get('items', [])

    if not items:
        print(f"[INFO] 该公司无诉讼记录: {keyword}")
        return []

    rows = []
    for item in items:
        case_persons = item.get('casePersons', [])
        person1 = case_persons[0] if len(case_persons) > 0 else None
        person2 = case_persons[1] if len(case_persons) > 1 else None

        p1_fields = parse_person(person1, '1')
        p2_fields = parse_person(person2, '2')

        # casePersons[0].result → case_result
        case_result = null_if_empty(person1.get('result')) if person1 else None

        row = {
            'api_record_id': record_id,
            'company_name': keyword,
            'total': total,
            'lawsuit_id': item.get('id'),
            'doc_type': null_if_empty(item.get('docType')),
            'lawsuit_url': null_if_empty(item.get('lawsuitUrl')),
            'lawsuit_h5_url': null_if_empty(item.get('lawsuitH5Url')),
            'title': null_if_empty(item.get('title')),
            'court': null_if_empty(item.get('court')),
            'judge_time': null_if_empty(item.get('judgeTime')),
            'uuid': null_if_empty(item.get('uuid')),
            'case_no': null_if_empty(item.get('caseNo')),
            'case_type': null_if_empty(item.get('caseType')),
            'case_reason': null_if_empty(item.get('caseReason')),
            'case_money': null_if_empty(item.get('caseMoney')),
            'submit_time': convert_timestamp(item.get('submitTime')),
            'case_result': case_result,
            'role1': p1_fields['1_role'],
            'gid1': p1_fields['1_gid'],
            'emotion1': p1_fields['1_emotion'],
            'sptname1': p1_fields['1_sptname'],
            'name1': p1_fields['1_name'],
            'type1': p1_fields['1_type'],
            'role2': p2_fields['2_role'],
            'gid2': p2_fields['2_gid'],
            'emotion2': p2_fields['2_emotion'],
            'sptname2': p2_fields['2_sptname'],
            'name2': p2_fields['2_name'],
            'type2': p2_fields['2_type'],
        }
        rows.append(row)

    return rows


def delete_old_lawsuit_data(keyword: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM company_1114_lawsuit_info WHERE company_name = %s"
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


def insert_lawsuit_rows(keyword: str, rows: List[Dict], valid_columns: List[str]):
    if not rows:
        print(f"[INFO] 无诉讼数据可插入: {keyword}")
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
            INSERT INTO company_1114_lawsuit_info ({col_str})
            VALUES ({placeholders})
            """
            cursor.executemany(sql, values_list)
            conn.commit()
            print(f"[INFO] 插入诉讼数据: {keyword} ({len(rows)}条)")
    except Exception as e:
        print(f"[ERROR] 插入诉讼数据失败 {keyword}: {e}")
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
    print("  result.items数组 → 每条诉讼一行(1:N)")
    print("  casePersons取前2人 → role1/gid1/emotion1/sptname1/name1/type1 + role2...")
    print("  id → lawsuit_id (避免与表主键冲突)")
    print("  submitTime → submit_time (毫秒时间戳→datetime)")
    print("  company_name 来自搜索入参(非API返回)")
    print("  入库方式: DELETE旧数据 + INSERT新数据")
    print()

    try:
        valid_columns = get_table_columns('company_1114_lawsuit_info')
        print(f"[INFO] 数据库表字段数: {len(valid_columns)} 个")

        target_company = sys.argv[1] if len(sys.argv) > 1 else None
        if target_company:
            print(f"[INFO] 解析指定公司: {target_company}")

        records = get_today_success_records(target_company)

        if not records:
            print("[WARNING] 没有找到可解析的数据")
            print("提示：先执行 1114-step1_api_fetch.py 拉取数据")
            return

        success_count = 0
        failed_count = 0
        total_rows = 0

        for i, (record_id, company_name, result_json, create_time) in enumerate(records, 1):
            print(f"\n[{i}/{len(records)}] {company_name} (record_id={record_id})")
            print("-" * 60)

            try:
                api_result = json.loads(result_json)

                rows = parse_lawsuit_data(api_result, company_name, record_id)
                print(f"[INFO] 展平后得到 {len(rows)} 条诉讼记录")

                if not rows:
                    delete_old_lawsuit_data(company_name)
                    success_count += 1
                    continue

                delete_old_lawsuit_data(company_name)
                insert_lawsuit_rows(company_name, rows, valid_columns)

                total_rows += len(rows)
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
        print(f"  总诉讼记录数: {total_rows}")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()