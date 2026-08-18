# -*- coding: utf-8 -*-
"""【Notebook版】Access/Excel 手工数据加载情况预警邮件

晚 10 点跑(避开其他任务跑批窗口), 汇总当天 access_to_delta_notebook.py 的处理情况:
- 读 manual_load.meta_table 当天(success/failed)记录
- 对 success 文件涉及的 Delta 表实时 COUNT(*)
- 通过 Graph API 发 HTML 邮件

不发邮件的条件:
- 当天 meta 表无任何记录(无文件需要解析, 业务方当天没上传)→ 不发

前置条件: Cell1 已执行 notebook_init
"""

import os
import json
import base64
import requests
from urllib.parse import quote
from datetime import datetime, timedelta

from common.config_loader import load_config, get_manual_load_config, get_alert_config
from common.spark_utils import get_spark


CONFIG = load_config()
spark = get_spark()
_ml = get_manual_load_config(CONFIG)
if not _ml:
    raise RuntimeError("config.json 缺 manual_load 段, 请参考 config.json.example 补全")

CATALOG = _ml["catalog"]
SCHEMA = _ml["schema"]
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
TABLE_PREFIX = _ml["table_prefix"]
META_TABLE = f"{FULL_SCHEMA}.{_ml['meta_table']}"

run_date = datetime.now().strftime('%Y-%m-%d')
today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow_start = today_start + timedelta(days=1)
today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S')
tomorrow_start_str = tomorrow_start.strftime('%Y-%m-%d %H:%M:%S')

print("=" * 60)
print("【Access/Excel 手工数据加载情况预警邮件】")
print("=" * 60)
print(f"分析日期: {run_date}")
print(f"meta 表: {META_TABLE}")
print(f"时间窗口: {today_start_str} ~ {tomorrow_start_str}")
print()

# 兼容旧 meta 表: 若缺 table_counts 列(老版本创建)则补列, 否则下面 SELECT table_counts 会报错
try:
    cols = [r.col_name for r in spark.sql(f"DESCRIBE TABLE {META_TABLE}").collect()]
    if "table_counts" not in cols:
        print("meta 表缺 table_counts 列, 执行 ALTER TABLE ADD COLUMNS...")
        spark.sql(f"ALTER TABLE {META_TABLE} ADD COLUMNS (table_counts STRING)")
except Exception as e:
    print(f"[WARN] 检查/补列 table_counts 失败(忽略): {e}")
print()


# ========== 1. 读当天 meta 表记录 ==========

# 读当天 meta 表记录, 按 filename 取最新一条(以最终解析情况为准, 同名文件当天多次重跑只看最后一次)
today_rows = spark.sql(f"""
    SELECT filename, md5, processed_at, table_names, table_counts, status
    FROM (
        SELECT filename, md5, processed_at, table_names, table_counts, status,
               ROW_NUMBER() OVER (PARTITION BY filename ORDER BY processed_at DESC) AS rn
        FROM {META_TABLE}
        WHERE processed_at >= '{today_start_str}'
          AND processed_at < '{tomorrow_start_str}'
    ) WHERE rn = 1
    ORDER BY processed_at DESC
""").collect()

if not today_rows:
    print("当天 meta 表无任何记录(业务方未上传文件, 或脚本未跑)→ 不发邮件")
    print("=" * 60)
    print("结束")
    raise SystemExit(0)

print(f"当天 meta 表共 {len(today_rows)} 个文件(已按 filename 取最新一条):")
for r in today_rows:
    tag = "✅" if r.status == "success" else "❌"
    print(f"  {tag} {r.filename} ({r.processed_at}) status={r.status[:80]}")
print()


# ========== 2. 对 success 文件涉及的 Delta 表实时 COUNT ==========

# 收集所有需要 count 的表名(success 文件解析 table_names 字段)
def parse_table_names(table_names_str):
    if not table_names_str:
        return []
    return [t.strip() for t in str(table_names_str).split(",") if t.strip()]


def parse_table_counts(table_counts_str):
    """解析 meta 表 table_counts 列(JSON: {orig_table_name: row_cnt}). 解析失败/空值返回 {}"""
    if not table_counts_str:
        return {}
    try:
        d = json.loads(table_counts_str)
        # 值统一 int, 异常值保留为字符串便于显示
        return {k: (v if isinstance(v, int) else str(v)) for k, v in d.items()}
    except Exception as e:
        print(f"[WARN] 解析 table_counts JSON 失败({table_counts_str[:80]}...): {e}")
        return {}

tables_to_count = []  # 保留顺序, 用于后续按文件展示
seen = set()
for r in today_rows:
    if r.status != "success":
        continue
    for t in parse_table_names(r.table_names):
        delta_tbl_name = TABLE_PREFIX + t.lower()
        if delta_tbl_name not in seen:
            seen.add(delta_tbl_name)
            tables_to_count.append(delta_tbl_name)

# 实时 count
print(f"对 {len(tables_to_count)} 张 Delta 表实时 COUNT(*)...")
table_counts = {}  # {delta_table_name: row_count or error_str}
for tbl in tables_to_count:
    full_name = f"{FULL_SCHEMA}.{tbl}"
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {full_name}").collect()[0][0]
        table_counts[tbl] = cnt
        print(f"  OK {tbl}: {cnt:,} 行")
    except Exception as e:
        table_counts[tbl] = f"error: {str(e)[:100]}"
        print(f"  FAIL {tbl}: {e}")
print()


# ========== 3. 组装每个文件 + 表行数 ==========

# 按文件组织: {filename: {info, tables: [(table_name, row_count)]}}
files_info = []
grand_total_rows = 0
for r in today_rows:
    file_entry = {
        'filename': r.filename,
        'md5': r.md5,
        'processed_at': r.processed_at,
        'status': r.status,
        'tables': [],
        'table_counts': parse_table_counts(r.table_counts) if r.status == "success" else {},
    }
    if r.status == "success":
        for t in parse_table_names(r.table_names):
            delta_tbl_name = TABLE_PREFIX + t.lower()
            cnt = table_counts.get(delta_tbl_name, "未读取")
            if isinstance(cnt, int):
                grand_total_rows += cnt
            file_entry['tables'].append((t, delta_tbl_name, cnt))
    files_info.append(file_entry)


# ========== 3.5 拉取最近两条成功解析记录 (供差异对比, 不分 filename) ==========

# 全表 status='success' 且 table_counts 非空, 按 processed_at DESC 取 rn=1(本次)/rn=2(上次)
# 不按 filename 分区: 业务方可能改文件名(如 _20260817 → _20260818), 但内容延续, 仍需对比
# 过滤 table_counts IS NOT NULL/'' : 老脚本写的 success 记录该列为 NULL, 无法对比, 直接跳过找更早的
prev_success_info = None  # {'filename': ..., 'md5': ..., 'processed_at': ..., 'table_counts': {...}}
prev_rows = spark.sql(f"""
    SELECT filename, md5, processed_at, table_counts
    FROM (
        SELECT filename, md5, processed_at, table_counts,
               ROW_NUMBER() OVER (ORDER BY processed_at DESC) AS rn
        FROM {META_TABLE}
        WHERE status = 'success'
          AND table_counts IS NOT NULL
          AND table_counts != ''
    ) WHERE rn = 2
""").collect()
if prev_rows:
    r = prev_rows[0]
    prev_success_info = {
        'filename': r.filename,
        'md5': r.md5,
        'processed_at': r.processed_at,
        'table_counts': parse_table_counts(r.table_counts),
    }
    print(f"拉取到上次成功记录用于差异对比: {prev_success_info['filename']} ({prev_success_info['processed_at']}), md5={prev_success_info['md5'][:8]}, 表数={len(prev_success_info['table_counts'])}")
else:
    print("未拉取到带行数信息的上次成功记录 (rn=2 不存在), 无法对比")
print()


# ========== 4. 生成 HTML 邮件 ==========

def build_html_email(files_info, run_date, grand_total_rows, prev_success_info):
    """生成 HTML 邮件: 顶部彩带 + PowerLink 徽章 + tesa logo + 表格"""
    success_files = [f for f in files_info if f['status'] == 'success']
    failed_files = [f for f in files_info if f['status'] != 'success']
    need_alert = len(failed_files) > 0

    title = "⚠️ Access 加载异常" if need_alert else "✅ Access 加载正常"
    title += f" - 业务日期 {run_date}"

    brand_bar = """
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; border:none; height:2px; margin:4px 0;">
<tr>
<td width="66%" height="2" style="background-color:#E3000F; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
<td width="2%" height="2" style="background-color:#FFFFFF; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
<td width="32%" height="2" style="background-color:#009fdf; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
</tr>
</table>"""

    header = f"""<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; border:none; margin:4px 0;">
<tr>
<td valign="middle" style="padding:4px 0; border:none;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; border:none;">
<tr>
<td height="24" style="background-color:#E3000F; color:#FFFFFF; font-size:15px; font-weight:bold; padding:0 10px; height:24px; line-height:20px; font-family:'Microsoft YaHei',Arial,sans-serif; border:none;">Power</td>
<td width="4" height="24" style="background-color:#FFFFFF; height:24px; line-height:20px; font-size:1px; border:none;">&nbsp;</td>
<td height="24" style="background-color:#009fdf; color:#FFFFFF; font-size:15px; font-weight:bold; padding:0 10px; height:24px; line-height:20px; font-family:'Microsoft YaHei',Arial,sans-serif; border:none;">Link</td>
</tr>
</table>
</td>
<td width="10" style="border:none; font-size:0; line-height:0;">&nbsp;</td>
<td valign="middle" style="padding:4px 0; border:none;">
<img src="cid:tesa_logo" height="40" alt="tesa" style="display:block; height:40px; border:0;" />
</td>
<td width="100%" align="right" valign="middle" style="border:none; text-align:right; padding:4px 0; color:#009fdf; font-size:18px; font-weight:bold; font-family:'Microsoft YaHei',Arial,sans-serif;">{title}</td>
</tr>
</table>"""

    # 汇总条 (假设单 Access 文件场景, 不列文件数)
    status_summary = '<span class="warn">❌ 加载失败</span>' if failed_files else '<span class="normal">✅ 加载成功</span>'
    summary = f"""
<div class="summary">
<strong>汇总统计 (业务日期 {run_date})</strong><br>
文件名: {files_info[0]['filename'] if files_info else '-'} |
状态: {status_summary} |
涉及 Delta 表: {len(table_counts)} 张, 合计 {grand_total_rows:,} 行
</div>"""

    # 详情表 (单文件场景: 文件名在标题里, 表格列 原表名/Delta表名/上次行数/本次行数/变更)
    # 若有上次成功记录 (prev_success_info), 则对比行数填入 上次/变更 列; 否则 上次='-', 变更='首次'
    the_file = files_info[0] if files_info else None
    prev_counts = prev_success_info.get('table_counts', {}) if prev_success_info else {}
    prev_ts_str = prev_success_info['processed_at'] if prev_success_info else None

    if the_file and the_file['status'] == "success" and the_file['tables']:
        def fmt_cnt(c):
            return f"{c:,}" if isinstance(c, int) else f'<span class="warn">{c}</span>'

        def fmt_delta(p_cnt, c_cnt):
            # 本次新增 (上次无此表)
            if p_cnt is None:
                return f'<span class="normal">新增</span>'
            # 行数异常 (非 int)
            if not (isinstance(p_cnt, int) and isinstance(c_cnt, int)):
                return '<span class="warn">类型异常</span>'
            # 行数未变
            if p_cnt == c_cnt:
                return '<span style="color:#999;">-</span>'
            # 行数变更
            delta = c_cnt - p_cnt
            sign = "+" if delta >= 0 else ""
            cls = "normal" if delta >= 0 else "warn"
            return f'<span class="{cls}">{sign}{delta:,}</span>'

        # 当前文件的表行集合 (用于后面找"上次有但本次无"的移除表)
        curr_orig_set = set(orig for (orig, _, _) in the_file['tables'])
        table_rows = ""
        for (orig, delta_name, cnt) in the_file['tables']:
            p_cnt = prev_counts.get(orig)
            table_rows += f"""
<tr>
<td style="text-align:left;">{orig}</td>
<td style="text-align:left;">{delta_name}</td>
<td style="text-align:right;">{fmt_cnt(p_cnt) if p_cnt is not None else '-'}</td>
<td style="text-align:right;">{fmt_cnt(cnt)}</td>
<td style="text-align:right;">{fmt_delta(p_cnt, cnt)}</td>
</tr>"""
        # 移除表: 上次有但本次无 (仅当有 prev_counts 时才存在)
        for orig in sorted(set(prev_counts.keys()) - curr_orig_set):
            p_cnt = prev_counts[orig]
            table_rows += f"""
<tr>
<td style="text-align:left;">{orig}</td>
<td style="text-align:left;">-</td>
<td style="text-align:right;">{fmt_cnt(p_cnt)}</td>
<td style="text-align:right;">-</td>
<td style="text-align:right;"><span class="warn">移除</span></td>
</tr>"""

        title_extra = f' <span style="color:#999; font-size:12px;">(对比上次: {prev_ts_str})</span>' if prev_ts_str else ''
        detail_table = f"""
<h3>各表加载详情 - {the_file['filename']}{title_extra}</h3>
<table class="data">
<tr>
<th>原表名</th><th>Delta 表名</th><th>上次行数</th><th>本次行数</th><th>变更</th>
</tr>
{table_rows}
</table>"""
    elif the_file and the_file['status'] == "success" and not the_file['tables']:
        detail_table = f"""
<h3>各表加载详情 - {the_file['filename']}</h3>
<table class="data">
<tr>
<th>原表名</th><th>Delta 表名</th><th>上次行数</th><th>本次行数</th><th>变更</th>
</tr>
<tr><td colspan="5" style="color:#999; text-align:left;">成功但未发现任何 Access 表</td></tr>
</table>"""
    elif the_file:
        # 失败: status 字段截断 200 字, 红字
        fail_reason = the_file['status'][:200]
        detail_table = f"""
<h3>加载详情 - {the_file['filename']}</h3>
<table class="data">
<tr>
<th>原表名</th><th>Delta 表名</th><th>上次行数</th><th>本次行数</th><th>变更</th>
</tr>
<tr><td colspan="5" style="color:#E3000F; text-align:left;">{fail_reason}</td></tr>
</table>"""
    else:
        detail_table = ""

    # 解析规则说明
    rules_section = """
<h3>解析规则说明</h3>
<table class="rules">
<tr><th width="20%">环节</th><th>规则</th></tr>
<tr>
<td>表名清洗</td>
<td>Access 表名转小写, 非 <code>[a-z0-9_]</code> 字符替换为 <code>_</code>, 连续 <code>_</code> 合并, 前后 <code>_</code> 去除, 加 <code>manual_</code> 前缀<br>
<em>例: "KNA1 Customer" → manual_kna1_customer</em></td>
</tr>
<tr>
<td>列名清洗</td>
<td>列名非 <code>[a-zA-Z0-9_]</code> 字符替换为 <code>_</code>, 连续 <code>_</code> 合并, 前后 <code>_</code> 去除; 重复列名自动加 <code>_N</code> 后缀</td>
</tr>
<tr>
<td>类型映射</td>
<td>非数值列强制转 string (处理 Java 对象/混合类型), 全 NULL 列填空字符串 (避免 Parquet 丢列), 数值列保持原类型 (int/float/bool 原样写入)</td>
</tr>
<tr>
<td>写入模式</td>
<td>全表 <code>overwrite</code> (TRUNCATE+INSERT), schema 允许变更 (<code>overwriteSchema=true</code>)</td>
</tr>
<tr>
<td>去重机制</td>
<td>上传前按文件 MD5 比对 meta 表最新 success 记录, 内容未变则跳过; 同名多文件只处理最新日期的, 其余归档至 processed/ (默认保留最近 3 个)</td>
</tr>
</table>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; background-color: #FFFFFF; padding: 20px; color: #5E5E5E; }}
h3 {{ color: #009fdf; border-bottom: 1px solid #373737; padding-bottom: 4px; }}
table.data {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
table.data th {{ background-color: #373737; color: #FFFFFF; padding: 10px; text-align: center; border: 1px solid #373737; }}
table.data td {{ border: 1px solid #CCCCCC; padding: 8px; text-align: center; color: #5E5E5E; }}
table.data tr:nth-child(even) {{ background-color: #F8F8F8; }}
.warn {{ color: #E3000F; font-weight: bold; }}
.normal {{ color: #009fdf; }}
.summary {{ background-color: #FFFFFF; border: 1px solid #373737; border-top: 4px solid #373737; padding: 15px; margin: 10px 0; }}
table.rules {{ border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 13px; }}
table.rules th {{ background-color: #2E75B6; color: #FFFFFF; padding: 8px 12px; text-align: left; border: 1px solid #2E75B6; }}
table.rules td {{ border: 1px solid #CCCCCC; padding: 8px 12px; text-align: left; color: #5E5E5E; vertical-align: top; }}
table.rules tr:nth-child(even) {{ background-color: #F8F8F8; }}
table.rules code {{ background-color: #E8F4FD; padding: 1px 4px; border-radius: 2px; color: #009fdf; font-family: 'Consolas', 'Monaco', monospace; }}
table.rules em {{ color: #999; }}
</style>
</head>
<body>
{brand_bar}
{header}
{summary}
{detail_table}
{rules_section}
{brand_bar}
<p style="color:#5E5E5E; font-size:12px;">
数据来源: {META_TABLE} (status=success/failed) + 各 Delta 表实时 COUNT(*)<br>
本邮件由 access_load_alert_notebook.py 每天 22:00 自动发送
</p>
</body></html>"""
    return html, need_alert


html_content, need_alert = build_html_email(files_info, run_date, grand_total_rows, prev_success_info)
print(f"邮件生成完成: {len(files_info)} 文件 / {len(table_counts)} 张 Delta 表 / 合计 {grand_total_rows:,} 行 / need_alert={need_alert}")


# ========== 5. 通过 Graph API 发送邮件 ==========

def send_alert_email(config, html_content, run_date, need_alert):
    """通过 Microsoft Graph API (client_credentials) 发送预警邮件
    复用 alert 段的 Graph 凭据, 与 daily_call_analysis_alert_notebook_v2.py 一致
    """
    alert_config = get_alert_config(config)
    if not alert_config:
        print("[WARNING] 未配置预警邮件(alert section), 跳过发送")
        return False

    tenant_id = alert_config.get('tenant_id')
    client_id = alert_config.get('client_id')
    client_secret = alert_config.get('client_secret')
    from_addr = alert_config.get('from_addr')
    to_addr = alert_config.get('to_addr', [])
    subject_prefix = alert_config.get('subject_prefix', '【Access 加载预警】')
    logo_path = alert_config.get('logo_path', '')
    cloud = alert_config.get('cloud', 'global')

    if cloud == 'china':
        token_endpoint = f"https://login.partner.microsoftonline.cn/{tenant_id}/oauth2/v2.0/token"
        graph_scope = "https://microsoftgraph.chinacloudapi.cn/.default"
        graph_base = "https://microsoftgraph.chinacloudapi.cn"
    else:
        token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        graph_scope = "https://graph.microsoft.com/.default"
        graph_base = "https://graph.microsoft.com"

    if not all([tenant_id, client_id, client_secret, from_addr, to_addr]):
        print("[WARNING] Graph API 配置不完整(需 tenant_id/client_id/client_secret/from_addr/to_addr), 跳过发送")
        return False

    subject = f"{subject_prefix} {run_date} "
    subject += "⚠️ 存在失败" if need_alert else "✅ 全部成功"

    # 1. 换 access token
    token_resp = None
    try:
        token_resp = requests.post(token_endpoint, data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': graph_scope,
        }, timeout=30)
        token_resp.raise_for_status()
        access_token = token_resp.json().get('access_token')
        if not access_token:
            print(f"[ERROR] Graph token 响应中无 access_token: {token_resp.text[:500]}")
            return False
    except Exception as e:
        print(f"[ERROR] 换取 Graph token 失败: {e}")
        if token_resp is not None:
            print(f"       HTTP {token_resp.status_code} 响应: {token_resp.text[:500]}")
        return False

    # 2. 构造邮件 + 内联 logo
    attachments = []
    if logo_path and os.path.exists(logo_path):
        with open(logo_path, 'rb') as f:
            logo_b64 = base64.b64encode(f.read()).decode('utf-8')
        attachments.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "tesa_logo.png",
            "contentType": "image/png",
            "contentBytes": logo_b64,
            "isInline": True,
            "contentId": "tesa_logo",
        })
    elif logo_path:
        print(f"[WARNING] logo 文件不存在: {logo_path}(邮件将不含 tesa logo)")

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_content},
            "toRecipients": [{"emailAddress": {"address": a}} for a in to_addr],
            "attachments": attachments,
        },
        "saveToSentItems": False,
    }

    send_url = f"{graph_base}/v1.0/users/{quote(from_addr, safe='')}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    try:
        send_resp = requests.post(send_url, json=payload, headers=headers, timeout=60)
        if send_resp.status_code == 202:
            print(f"[SUCCESS] 预警邮件已发送: {subject} → {', '.join(to_addr)}")
            return True
        print(f"[ERROR] Graph sendMail 失败: HTTP {send_resp.status_code}")
        print(f"       响应: {send_resp.text[:800]}")
        return False
    except Exception as e:
        print(f"[ERROR] Graph sendMail 异常: {e}")
        return False


# ========== 6. 执行 ==========

send_result = send_alert_email(CONFIG, html_content, run_date, need_alert)

print("\n" + "=" * 60)
print("分析完成!")
print("-" * 60)
if need_alert:
    print(f"⚠️ 本次有 {sum(1 for f in files_info if f['status'] != 'success')} 个文件加载失败, 已发送预警邮件")
else:
    print("✅ 所有文件加载成功")
if send_result:
    print("邮件发送成功")
else:
    print("邮件未发送(配置缺失或发送失败)")
print("=" * 60)

displayHTML(html_content)
