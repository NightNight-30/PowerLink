# -*- coding: utf-8 -*-
"""【Notebook版】每日解析数据导出 (Phase 1: CSV + ZIP + Phase 2: 邮件发送)

Phase 1: 读取所有接口 step2 解析后的 Delta 表, 每接口一个 CSV, 打包 ZIP 到固定目录
Phase 2: 通过 Graph API 邮件发送 ZIP + 解析统计表(账期 vs 预付款客户拆分)

- 输出目录: {data_export.base_dir}/{dt}/  (config.json 配置)
- 每接口一个 CSV: {interface_key}_{interface_name}.csv (UTF-8 BOM, Excel 兼容中文)
- 当日 ZIP: daily_data_{dt}.zip
- 保留策略: {data_export.retention_days} 天滚动窗口
- 邮件发送: 复用 alert 段的 Graph API 凭据; 邮件体含统计表 + ZIP 作为普通附件

【测试全量月度数据量时】将 EXPORT_DT 改为对应月度跑批日期(如 20260605)即可。
"""

import os
import base64
import shutil
import zipfile
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from pyspark.sql import functions as F

from common.config_loader import (
    load_config, get_interface_name, get_api_config,
    get_data_export_config, get_alert_config, get_last_monthly_batch_date,
)
from common.spark_utils import get_spark, get_target_table_name, get_api_record_table, CUSTOMER_TABLE


CONFIG = load_config()
spark = get_spark()

# ========== 可调参数 ==========

# 是否发送邮件(Phase 2); False 时仅生成 ZIP 不发邮件
SEND_EMAIL = False

ALL_INTERFACE_KEYS = ['819', '851', '1058', '822', '854', '1168', '1149', '967', '1041', '1114', '973', '1001', 'P51060']

# 1:1 / 1:N 数据关系映射 (来源: 各接口 step2 脚本的 is_one_to_one 参数)
# 1001为1:1(分公司查总公司,返回单个总公司对象), 在1:1集合中
ONE_TO_ONE_INTERFACES = {'819', '854', '1149', '1168', '1001', 'P51060'}

# 最终输出目录 + 保留天数, 从 config.json 的 data_export 段读取
_export_cfg = get_data_export_config(CONFIG)
BASE_DIR = _export_cfg.get('base_dir', '/Workspace/Shared/powerlink_warehouse/tyc_new/data_exports')
RETENTION_DAYS = _export_cfg.get('retention_days', 30)

# Graph API 普通附件上限(超过需要 upload session)
ATTACHMENT_LIMIT = 4 * 1024 * 1024

# ==============================

# 数据取值范围:
#   - 分析日期 run_date = T (今天)
#   - 数据分区 dt = T-1 (账期客户昨天跑批) + 月度跑批日 (预付款客户/月度接口)
#   - 创建时间窗口: data_create_time 在 [今天0点, 明天0点) 之间
#     只看"今天解析写入"的数据, 排除月度跑批日跑的历史数据
run_date = datetime.now().strftime('%Y-%m-%d')
dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
monthly_dt = get_last_monthly_batch_date(CONFIG)

today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow_start = today_start + timedelta(days=1)
today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S')
tomorrow_start_str = tomorrow_start.strftime('%Y-%m-%d %H:%M:%S')

print("=" * 60)
print("【每日解析数据导出 (Phase 1: CSV + ZIP + Phase 2: 邮件)】")
print("=" * 60)
print(f"分析日期: {run_date} (数据分区: dt={dt}, 月度分区={monthly_dt})")
print(f"创建时间窗口: {today_start_str} ~ {tomorrow_start_str}")
print()


# ========== 1. 保留策略: 清理 30 天前的子目录 ==========

def cleanup_old_dirs(base_dir, retention_days):
    """删除超过 retention_days 天的日期子目录(目录名为 YYYYMMDD)"""
    if not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)
        return 0

    cutoff_str = (datetime.now() - timedelta(days=retention_days)).strftime('%Y%m%d')

    deleted = 0
    for name in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, name)
        if not os.path.isdir(dir_path):
            continue
        # 仅匹配 8 位数字日期目录
        if not (name.isdigit() and len(name) == 8):
            continue
        if name < cutoff_str:
            try:
                shutil.rmtree(dir_path)
                deleted += 1
                print(f"  [CLEAN] 删除过期目录: {name}/")
            except Exception as e:
                print(f"  [WARN] 删除失败 {name}/: {e}")
    return deleted


print(f"[Step 1] 保留策略: 清理 {RETENTION_DAYS} 天前的子目录")
deleted_count = cleanup_old_dirs(BASE_DIR, RETENTION_DAYS)
print(f"  完成: 删除 {deleted_count} 个过期目录")
print()


# ========== 2. 创建当日目录 ==========

print(f"[Step 2] 数据分区:")
print(f"  dt = {dt} (T-1, 账期客户昨天跑批)")
print(f"  monthly_dt = {monthly_dt} (月度跑批日, 预付款客户/月度接口)")
print()

day_dir = os.path.join(BASE_DIR, dt)
os.makedirs(day_dir, exist_ok=True)
print(f"  当日目录: {day_dir}")
print()


# ========== 3. 导出每接口 CSV ==========

def export_interface_csv(spark, interface_key, interface_name, dt, workspace_dir):
    """导出单接口解析数据为 CSV (UTF-8 BOM, Excel 兼容)。

    实现说明：此 workspace 禁用了公共 DBFS 根目录, 且 /Workspace/Shared 不允许
    Spark executor 写入。因此用 df.toPandas() 把数据 collect 到 driver,
    再用 Python 文件 API 直接写到 Workspace (driver 可写)。
    """
    table = get_target_table_name(interface_key)

    # 查 T-1 和月度跑批日两个分区 + 当天解析写入的数据
    cnt = spark.sql(
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE dt IN ('{dt}', '{monthly_dt}') "
        f"AND data_create_time >= '{today_start_str}' AND data_create_time < '{tomorrow_start_str}'"
    ).collect()[0][0]
    if cnt == 0:
        return None

    df = spark.sql(
        f"SELECT * FROM {table} "
        f"WHERE dt IN ('{dt}', '{monthly_dt}') "
        f"AND data_create_time >= '{today_start_str}' AND data_create_time < '{tomorrow_start_str}'"
    )

    # TIMESTAMP 列先 cast 成 string, 避免 toPandas 时 nanosecond 超范围 casting 报错
    # (pandas datetime64[ns] 只覆盖 1677-2262 年, 819 表存在超范围值)
    timestamp_cols = [f.name for f in df.schema.fields if str(f.dataType).startswith('Timestamp')]
    if timestamp_cols:
        select_exprs = [
            F.col(c).cast('string').alias(c) if c in timestamp_cols else F.col(c)
            for c in df.columns
        ]
        df = df.select(*select_exprs)

    pdf = df.toPandas()

    safe_name = interface_name.replace('/', '_').replace('\\', '_')
    csv_name = f"{interface_key}_{safe_name}.csv"
    csv_path = os.path.join(workspace_dir, csv_name)

    # utf-8-sig 自动写 BOM, Excel 打开中文不乱码
    pdf.to_csv(csv_path, index=False, encoding='utf-8-sig')

    return csv_path, cnt


print(f"[Step 3] 导出 CSV (dt={dt}):")
csv_files = []  # (interface_key, interface_name, csv_path, size_bytes, row_count)
for ik in ALL_INTERFACE_KEYS:
    interface_name = get_interface_name(CONFIG, ik)
    try:
        result = export_interface_csv(spark, ik, interface_name, dt, day_dir)
        if result is None:
            print(f"  {ik}({interface_name}): 0 行 (跳过)")
            continue
        csv_path, row_count = result
        size = os.path.getsize(csv_path)
        csv_files.append((ik, interface_name, csv_path, size, row_count))
        size_kb = size / 1024
        size_str = f"{size_kb/1024:.2f} MB" if size_kb >= 1024 else f"{size_kb:.2f} KB"
        safe_name = interface_name.replace('/', '_').replace('\\', '_')
        print(f"  {ik}({interface_name}): {row_count:,} 行, {size_str} → {ik}_{safe_name}.csv")
    except Exception as e:
        print(f"  {ik}({interface_name}): 导出失败 - {e}")

print()


# ========== 4. 打包 ZIP ==========

print(f"[Step 4] 打包 ZIP:")
zip_path = os.path.join(day_dir, f"daily_data_{dt}.zip")
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for ik, name, path, _, _ in csv_files:
        arcname = os.path.basename(path)
        zf.write(path, arcname)
        print(f"  + {arcname}")

zip_size = os.path.getsize(zip_path)
zip_mb = zip_size / (1024 * 1024)
print(f"  ZIP: {zip_path}")
print(f"  ZIP 大小: {zip_mb:.2f} MB")
print()


# ========== 5. 收集解析统计 (账期 vs 预付款) ==========

def collect_interface_stats(spark, interface_key, interface_name, dt, customer_dt):
    """统计某接口 dt 分区: 调用成功数 + 解析行数 + 账期/预付款/未分类 拆分

    - success_count: 当天成功调用记录数 (status_code=0), 来自 ods_api_call_record_{key}_df
    - 通过 LEFT JOIN 客户表 ods_credit_api_input_company_df 的 is_prepaid 分类
    - 1058 接口必须用 main_company_name (搜索入参=客户公司), 不能用 company_name
      (后者是 API 返回的风险相关公司, 可能完全不同或为 NULL)
    """
    table = get_target_table_name(interface_key)
    relationship = '1:1' if interface_key in ONE_TO_ONE_INTERFACES else '1:N'

    # 调用成功数 (来自调用记录表, 同样查两分区 + 当天创建时间)
    try:
        call_record_table = get_api_record_table(interface_key)
        success_count = spark.sql(
            f"SELECT COUNT(*) FROM {call_record_table} "
            f"WHERE dt IN ('{dt}', '{monthly_dt}') "
            f"AND create_time >= '{today_start_str}' AND create_time < '{tomorrow_start_str}' "
            f"AND status_code = 0"
        ).collect()[0][0]
    except Exception as e:
        print(f"  [WARN] {interface_key} 读取调用记录失败: {e}")
        success_count = 0

    total_count = spark.sql(
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE dt IN ('{dt}', '{monthly_dt}') "
        f"AND data_create_time >= '{today_start_str}' AND data_create_time < '{tomorrow_start_str}'"
    ).collect()[0][0]
    if total_count == 0:
        return {
            'interface_key': interface_key,
            'interface_name': interface_name,
            'frequency': get_api_config(CONFIG, interface_key).get('frequency', 'daily'),
            'relationship': relationship,
            'success_count': success_count,
            'total': 0, 'prepaid': 0, 'non_prepaid': 0, 'uncategorized': 0,
            'has_data': False, 'has_company_col': False,
        }

    # 检测公司名列: 1058 优先 main_company_name (company_name 是风险相关公司不是客户公司)
    field_names = {f.name for f in spark.table(table).schema.fields}
    if 'main_company_name' in field_names:
        company_col = 'main_company_name'
    elif 'company_name' in field_names:
        company_col = 'company_name'
    else:
        company_col = None

    if not company_col:
        return {
            'interface_key': interface_key,
            'interface_name': interface_name,
            'frequency': get_api_config(CONFIG, interface_key).get('frequency', 'daily'),
            'relationship': relationship,
            'success_count': success_count,
            'total': total_count, 'prepaid': 0, 'non_prepaid': 0,
            'uncategorized': total_count,
            'has_data': True, 'has_company_col': False,
        }

    sql = f"""
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN c.is_prepaid = '是' THEN 1 ELSE 0 END) as prepaid,
      SUM(CASE WHEN c.is_prepaid = '否' THEN 1 ELSE 0 END) as non_prepaid,
      SUM(CASE WHEN c.is_prepaid IS NULL OR c.is_prepaid NOT IN ('是','否') THEN 1 ELSE 0 END) as uncategorized
    FROM {table} t
    LEFT JOIN (
      SELECT DISTINCT name, is_prepaid FROM {CUSTOMER_TABLE}
      WHERE dt = '{customer_dt}'
    ) c ON t.{company_col} = c.name
    WHERE t.dt IN ('{dt}', '{monthly_dt}')
      AND t.data_create_time >= '{today_start_str}' AND t.data_create_time < '{tomorrow_start_str}'
    """
    r = spark.sql(sql).collect()[0]
    return {
        'interface_key': interface_key,
        'interface_name': interface_name,
        'frequency': get_api_config(CONFIG, interface_key).get('frequency', 'daily'),
        'relationship': relationship,
        'success_count': success_count,
        'total': r.total,
        'prepaid': r.prepaid or 0,
        'non_prepaid': r.non_prepaid or 0,
        'uncategorized': r.uncategorized or 0,
        'has_data': True,
        'has_company_col': True,
    }


print(f"[Step 5] 收集解析统计 (账期 vs 预付款):")
customer_dt = dt  # 客户表统一用 T-1 (最新分区), 不管业务数据是 T-1 还是月度跑批日分区
customer_count = spark.sql(f"SELECT COUNT(*) FROM {CUSTOMER_TABLE} WHERE dt = '{customer_dt}'").collect()[0][0]
if customer_count == 0:
    print(f"  [WARNING] 客户表 dt={customer_dt} 分区无数据! 统计的账期/预付款数都会是 0")
else:
    print(f"  客户表分区: {customer_dt} ({customer_count:,} 条客户记录)")
stats_list = []
for ik in ALL_INTERFACE_KEYS:
    interface_name = get_interface_name(CONFIG, ik)
    try:
        stat = collect_interface_stats(spark, ik, interface_name, dt, customer_dt)
        stats_list.append(stat)
        if stat['total'] > 0:
            unc_tag = f", 未分类 {stat['uncategorized']:,}" if stat['uncategorized'] > 0 else ""
            print(f"  {ik}({interface_name}) [{stat['relationship']}]: "
                  f"成功 {stat['success_count']:,}, 解析 {stat['total']:,} 行 "
                  f"(账期 {stat['non_prepaid']:,} / 预付 {stat['prepaid']:,}{unc_tag})")
        else:
            print(f"  {ik}({interface_name}): 无数据")
    except Exception as e:
        print(f"  {ik}({interface_name}): 统计失败 - {e}")
        stats_list.append({
            'interface_key': ik, 'interface_name': interface_name,
            'frequency': 'daily', 'relationship': '1:N', 'success_count': 0,
            'total': 0, 'prepaid': 0, 'non_prepaid': 0, 'uncategorized': 0,
            'has_data': False, 'has_company_col': False, 'error': str(e),
        })
print()


# ========== 6. 构建邮件 HTML ==========

def build_data_export_email_html(stats_list, dt):
    """构建数据导出邮件 HTML: 顶部彩带 + PowerLink 徽章 + tesa logo + 问候 + 统计表"""
    grand_success = sum(s['success_count'] for s in stats_list)
    grand_total = sum(s['total'] for s in stats_list)
    grand_prepaid = sum(s['prepaid'] for s in stats_list)
    grand_non_prepaid = sum(s['non_prepaid'] for s in stats_list)
    grand_uncategorized = sum(s['uncategorized'] for s in stats_list)

    brand_bar = """
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; border:none; height:2px; margin:4px 0;">
<tr>
<td width="66%" height="2" style="background-color:#E3000F; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
<td width="2%" height="2" style="background-color:#FFFFFF; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
<td width="32%" height="2" style="background-color:#009fdf; height:2px; line-height:2px; font-size:1px; border:none;">&nbsp;</td>
</tr>
</table>"""

    header = f"""
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse; border:none; margin:4px 0;">
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
<td width="100%" align="right" valign="middle" style="border:none; text-align:right; padding:4px 0; color:#009fdf; font-size:18px; font-weight:bold; font-family:'Microsoft YaHei',Arial,sans-serif;">【接口解析数据】业务日期 {dt}</td>
</tr>
</table>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; background-color: #FFFFFF; padding: 20px; color: #5E5E5E; }}
table.data {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
table.data th {{ background-color: #373737; color: #FFFFFF; padding: 10px; text-align: center; border: 1px solid #373737; }}
table.data td {{ border: 1px solid #CCCCCC; padding: 8px; text-align: center; color: #5E5E5E; }}
table.data tr:nth-child(even) {{ background-color: #F8F8F8; }}
.normal {{ color: #009fdf; }}
.unc {{ color: #999999; }}
.total-row {{ background-color: #E8F4FD; font-weight: bold; }}
.greeting {{ font-size: 14px; line-height: 1.8; color: #5E5E5E; margin: 10px 0; }}
</style>
</head>
<body>
{brand_bar}
{header}
<p class="greeting">Dear all：<br>
&nbsp;&nbsp;&nbsp;&nbsp;请查收 PowerLink 项目业务日期 <strong>{dt}</strong> 接口解析数据，数据文件见附件！今日解析情况一览见下表：</p>
<table class="data">
<tr>
<th>接口ID</th><th>接口名称</th><th>频次</th>
<th>调用成功</th><th>解析行数</th><th>数据关系</th>
<th>账期客户</th><th>预付款客户</th><th>未分类</th>
</tr>
"""

    for s in stats_list:
        if not s['has_data']:
            html += f"""
<tr>
<td>{s['interface_key']}</td>
<td>{s['interface_name']}</td>
<td>{s['frequency']}</td>
<td>{s['success_count']:,}</td>
<td colspan="5" style="color:#999;">无数据</td>
</tr>"""
        elif not s['has_company_col']:
            html += f"""
<tr>
<td>{s['interface_key']}</td>
<td>{s['interface_name']}</td>
<td>{s['frequency']}</td>
<td>{s['success_count']:,}</td>
<td>{s['total']:,}</td>
<td>{s['relationship']}</td>
<td colspan="3" style="color:#999;">无客户字段</td>
</tr>"""
        else:
            unc_class = ' class="unc"' if s['uncategorized'] > 0 else ''
            html += f"""
<tr>
<td>{s['interface_key']}</td>
<td>{s['interface_name']}</td>
<td>{s['frequency']}</td>
<td>{s['success_count']:,}</td>
<td>{s['total']:,}</td>
<td>{s['relationship']}</td>
<td class="normal">{s['non_prepaid']:,}</td>
<td>{s['prepaid']:,}</td>
<td{unc_class}>{s['uncategorized']:,}</td>
</tr>"""

    html += f"""
<tr class="total-row">
<td colspan="3">合计</td>
<td>{grand_success:,}</td>
<td>{grand_total:,}</td>
<td>—</td>
<td>{grand_non_prepaid:,}</td>
<td>{grand_prepaid:,}</td>
<td>{grand_uncategorized:,}</td>
</tr>
</table>
{brand_bar}
<p style="color:#5E5E5E; font-size:12px;">
此邮件由 tesa® 数据团队自动生成，如有疑问请联系 Powerlink.GC@tesa.com<br>
附件: daily_data_{dt}.zip (各接口解析数据 CSV)
</p>
</body></html>"""

    return html


html_content = build_data_export_email_html(stats_list, dt)


# ========== 7. 发送邮件 ==========

def send_data_export_email(config, html_content, zip_path, dt):
    """通过 Graph API 发送数据导出邮件 (复用 alert 段的 Graph 凭据)

    附件: 内联 tesa logo (cid:tesa_logo) + ZIP 普通附件
    """
    alert_config = get_alert_config(config)
    if not alert_config:
        print("[WARNING] 未配置 alert section, 跳过邮件发送")
        return False

    tenant_id = alert_config.get('tenant_id')
    client_id = alert_config.get('client_id')
    client_secret = alert_config.get('client_secret')
    from_addr = alert_config.get('from_addr')
    to_addr = alert_config.get('to_addr', [])
    logo_path = alert_config.get('logo_path', '')
    cloud = alert_config.get('cloud', 'global')

    if not all([tenant_id, client_id, client_secret, from_addr, to_addr]):
        print("[WARNING] Graph API 配置不完整 (需 tenant_id/client_id/client_secret/from_addr/to_addr), 跳过发送")
        return False

    if not os.path.exists(zip_path):
        print(f"[WARNING] ZIP 文件不存在: {zip_path}, 跳过发送")
        return False

    zip_size = os.path.getsize(zip_path)
    if zip_size > ATTACHMENT_LIMIT:
        print(f"[ERROR] ZIP {zip_size/1024/1024:.2f} MB > 4MB, 超过 Graph API 普通附件上限 (需 upload session, 暂不支持)")
        return False

    subject = f"【PowerLink数据】{dt} 接口解析数据"

    # 1. Token
    if cloud == 'china':
        token_endpoint = f"https://login.partner.microsoftonline.cn/{tenant_id}/oauth2/v2.0/token"
        graph_scope = "https://microsoftgraph.chinacloudapi.cn/.default"
        graph_base = "https://microsoftgraph.chinacloudapi.cn"
    else:
        token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        graph_scope = "https://graph.microsoft.com/.default"
        graph_base = "https://graph.microsoft.com"

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
            print(f"[ERROR] Graph token 响应无 access_token: {token_resp.text[:500]}")
            return False
    except Exception as e:
        print(f"[ERROR] 换取 Graph token 失败: {e}")
        return False

    # 2. 附件: 内联 logo + ZIP
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
        print(f"[WARNING] logo 文件不存在: {logo_path} (邮件将不含 tesa logo)")

    with open(zip_path, 'rb') as f:
        zip_b64 = base64.b64encode(f.read()).decode('utf-8')
    attachments.append({
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": os.path.basename(zip_path),
        "contentType": "application/zip",
        "contentBytes": zip_b64,
        "isInline": False,
    })

    # 3. Send
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
        send_resp = requests.post(send_url, json=payload, headers=headers, timeout=120)
        if send_resp.status_code == 202:
            print(f"[SUCCESS] 数据导出邮件已发送: {subject}")
            print(f"         收件人: {', '.join(to_addr)}")
            print(f"         附件: {os.path.basename(zip_path)} ({zip_size/1024/1024:.2f} MB)")
            return True
        print(f"[ERROR] Graph sendMail 失败: HTTP {send_resp.status_code}")
        print(f"       响应: {send_resp.text[:800]}")
        return False
    except Exception as e:
        print(f"[ERROR] Graph sendMail 异常: {e}")
        return False


print(f"[Step 6] 发送邮件:")
if not SEND_EMAIL:
    print("  跳过 (SEND_EMAIL = False)")
    send_result = False
elif not csv_files:
    print("  跳过 (没有任何 CSV 导出成功, 不发送邮件)")
    send_result = False
else:
    send_result = send_data_export_email(CONFIG, html_content, zip_path, dt)
print()


# ========== 8. 汇总 ==========

total_rows = sum(c[4] for c in csv_files)
uncompressed = sum(c[3] for c in csv_files)
uncompressed_mb = uncompressed / (1024 * 1024)
ratio = (zip_size / uncompressed * 100) if uncompressed else 0

print("=" * 60)
print("【导出完成】")
print("=" * 60)
print(f"日期: {run_date} (dt={dt})")
print(f"导出接口: {len(csv_files)}/{len(ALL_INTERFACE_KEYS)} 个有数据")
print(f"总行数: {total_rows:,} 行")
print(f"未压缩合计: {uncompressed_mb:.2f} MB")
print(f"ZIP 大小: {zip_mb:.2f} MB (压缩比 {ratio:.1f}%)")
print(f"输出位置: {day_dir}/")
if SEND_EMAIL:
    print(f"邮件发送: {'✅ 成功' if send_result else '❌ 失败或跳过'}")
print("=" * 60)

displayHTML(html_content)
