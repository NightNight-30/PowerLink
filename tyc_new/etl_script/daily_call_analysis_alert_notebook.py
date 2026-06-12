# -*- coding: utf-8 -*-
"""【Notebook版】外部数据接口调用分析 & 预警邮件
每天跑批完成后运行，汇总所有接口的调用情况，区分正常失败与异常失败，发送预警邮件。

正常失败: status_code在接口的normal_error_codes列表中(如天眼查300000=经查无结果, 邓白氏1021/2001=未收录)
异常失败: status_code不在正常列表中，需要预警

前置条件: Cell1已执行notebook_init
"""

from common.config_loader import (
    load_config, get_interface_name, get_api_config,
    get_normal_error_codes, get_error_code_desc, get_alert_config,
    should_run_today, get_monthly_day, is_charge_per_query
)
from common.spark_utils import (
    get_spark, get_api_record_table, CATALOG, SCHEMA
)
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import datetime, timedelta

CONFIG = load_config()
spark = get_spark()
dt = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
# run_date = datetime.now().strftime('%Y-%m-%d')
run_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

ALL_INTERFACE_KEYS = ['819', '851', '1058', '822', '854', '1168', '1149', '967', '1114', '973', 'P51060']

print("=" * 60)
print("【外部数据接口调用分析 & 预警邮件】")
print("=" * 60)
print(f"分析日期: {run_date} (分区dt: {dt})")
print()


# ========== 1. 读取所有接口的调用记录 ==========

def get_interface_stats(interface_key):
    """读取某接口的调用记录表，按status_code分组统计"""
    table = get_api_record_table(interface_key)
    api_config = get_api_config(CONFIG, interface_key)
    normal_codes = get_normal_error_codes(CONFIG, interface_key)
    provider = api_config.get('provider', 'tyc')
    desc_map = get_error_code_desc(CONFIG, provider)
    interface_name = get_interface_name(CONFIG, interface_key)
    frequency = api_config.get('frequency', 'daily')
    charge_per_query = is_charge_per_query(CONFIG, interface_key)

    # 该接口今天是否应该跑
    should_run = should_run_today(CONFIG, interface_key)

    try:
        # 检查表是否有数据
        count_result = spark.sql(f"SELECT COUNT(*) FROM {table} WHERE dt = '{dt}'").collect()[0][0]

        if count_result == 0:
            return {
                'interface_key': interface_key,
                'interface_name': interface_name,
                'provider': provider,
                'frequency': frequency,
                'should_run': should_run,
                'has_data': False,
                'total': 0,
                'success': 0,
                'normal_failure': 0,
                'abnormal_failure': 0,
                'charge_per_query': charge_per_query,
                'call_count': 0,
                'abnormal_details': []
            }

        # 按status_code分组统计
        rows = spark.sql(
            f"SELECT status_code, COUNT(*) as cnt FROM {table} "
            f"WHERE dt = '{dt}' GROUP BY status_code ORDER BY status_code"
        ).collect()

        total = sum(r.cnt for r in rows)
        success = 0
        normal_failure = 0
        abnormal_failure = 0
        abnormal_details = []

        for r in rows:
            code = r.status_code
            cnt = r.cnt
            if code == 0:
                success = cnt
            elif code in normal_codes:
                normal_failure += cnt
            else:
                abnormal_failure += cnt
                # 获取异常记录的公司名和详情
                detail_rows = spark.sql(
                    f"SELECT input_param, status_code, output_result FROM {table} "
                    f"WHERE dt = '{dt}' AND status_code = {code}"
                ).collect()
                companies = [dr.input_param for dr in detail_rows[:10]]  # 最多取10个
                desc = desc_map.get(str(code), f'未知错误码({code})')
                # 尝试从output_result中提取API返回的reason/msg
                reason_list = []
                for dr in detail_rows[:5]:
                    try:
                        result_dict = json.loads(dr.output_result)
                        reason = result_dict.get('reason', result_dict.get('msg', ''))
                        if reason:
                            reason_list.append(reason)
                    except:
                        pass
                abnormal_details.append({
                    'code': code,
                    'count': cnt,
                    'desc': desc,
                    'companies': companies,
                    'reasons': reason_list[:3]
                })

        call_count = total if charge_per_query else success
        return {
            'interface_key': interface_key,
            'interface_name': interface_name,
            'provider': provider,
            'frequency': frequency,
            'should_run': should_run,
            'has_data': True,
            'total': total,
            'success': success,
            'normal_failure': normal_failure,
            'abnormal_failure': abnormal_failure,
            'charge_per_query': charge_per_query,
            'call_count': call_count,
            'abnormal_details': abnormal_details
        }

    except Exception as e:
        print(f"[ERROR] 读取{interface_key}接口数据失败: {e}")
        return {
            'interface_key': interface_key,
            'interface_name': interface_name,
            'provider': provider,
            'frequency': frequency,
            'should_run': should_run,
            'has_data': False,
            'error': str(e),
            'total': 0, 'success': 0, 'normal_failure': 0, 'abnormal_failure': 0,
            'charge_per_query': charge_per_query, 'call_count': 0,
            'abnormal_details': []
        }


# ========== 2. 汇总所有接口 ==========

all_stats = []
for ik in ALL_INTERFACE_KEYS:
    stat = get_interface_stats(ik)
    all_stats.append(stat)
    if stat['total'] > 0:
        status_tag = "✅" if stat['abnormal_failure'] == 0 else "⚠️"
        run_tag = ""
    elif not stat['should_run']:
        status_tag = "—"
        run_tag = " [未跑批]"
    else:
        status_tag = "⚠️"
        run_tag = " [无数据]"
    print(f"  {status_tag} {ik}({stat['interface_name']}): "
          f"总计={stat['total']}, 成功={stat['success']}, "
          f"正常失败={stat['normal_failure']}, 异常失败={stat['abnormal_failure']}, "
          f"调用次数={stat['call_count']}{run_tag}")

print()


# ========== 3. 判断是否需要预警 ==========

# 有异常失败 或 跑批数据异常时需要预警
has_abnormal = any(s['abnormal_failure'] > 0 for s in all_stats)
has_no_data_for_run = any(s['should_run'] and not s['has_data'] for s in all_stats)
need_alert = has_abnormal or has_no_data_for_run

print(f"预警判断: 异常失败={has_abnormal}, 数据缺失={has_no_data_for_run} → 需要预警={need_alert}")
print()


# ========== 4. 生成邮件内容 ==========

def generate_html_email(stats_list, run_date, dt, need_alert):
    """生成HTML格式的预警邮件"""

    # 总计
    grand_total = sum(s['total'] for s in stats_list)
    grand_success = sum(s['success'] for s in stats_list)
    grand_normal = sum(s['normal_failure'] for s in stats_list)
    grand_abnormal = sum(s['abnormal_failure'] for s in stats_list)
    grand_call_count = sum(s['call_count'] for s in stats_list)

    # 预警标题
    if need_alert:
        title = f"⚠️ 外部数据接口异常预警 - {run_date}"
    else:
        title = f"✅ 外部数据接口运行正常 - {run_date}"

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; background-color: #FFFFFF; padding: 20px; color: #5E5E5E; }}
.header-row {{ display: flex; align-items: center; justify-content: flex-start; gap: 10px; margin: 10px 0; }}
.tesa-logo {{ height: 40px; }}
.project-badge {{ display: inline-flex; align-items: center; height: 26px; border-radius: 4px; overflow: hidden; }}
.project-badge-red {{ background-color: #E3000F; color: #FFFFFF; font-size: 18px; font-weight: bold; padding: 0 12px; height: 26px; line-height: 26px; }}
.project-badge-gap {{ background-color: #FFFFFF; height: 26px; width: 4px; }}
.project-badge-blue {{ background-color: #009fdf; color: #FFFFFF; font-size: 18px; font-weight: bold; padding: 0 12px; height: 26px; line-height: 26px; }}
h2 {{ color: #009fdf; border-bottom: 2px solid #373737; padding-bottom: 8px; margin-top: 15px; }}
h3 {{ color: #009fdf; border-bottom: 1px solid #373737; padding-bottom: 4px; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
th {{ background-color: #373737; color: #FFFFFF; padding: 10px; text-align: center; border: 1px solid #373737; }}
td {{ border: 1px solid #CCCCCC; padding: 8px; text-align: center; color: #5E5E5E; }}
tr:nth-child(even) {{ background-color: #F8F8F8; }}
.warn {{ color: #E3000F; font-weight: bold; }}
.normal {{ color: #009fdf; }}
.detail {{ background-color: #E8F4FD; border-left: 4px solid #009fdf; padding: 10px; margin: 8px 0; }}
.no-run {{ color: #999999; }}
.summary {{ background-color: #FFFFFF; border: 1px solid #373737; border-top: 4px solid #373737; padding: 15px; margin: 10px 0; }}
.total-row {{ background-color: #E8F4FD; font-weight: bold; }}
.divider {{ border: none; border-top: 2px solid #373737; margin: 15px 0; }}
.brand-bar {{ height: 6px; overflow: hidden; margin: 10px 0; }}
.brand-bar-red {{ background-color: #E3000F; width: 66%; height: 6px; float: left; }}
.brand-bar-gap {{ background-color: #FFFFFF; width: 2%; height: 6px; float: left; }}
.brand-bar-blue {{ background-color: #009fdf; width: 32%; height: 6px; float: left; }}
</style>
</head>
<body>
<div class="brand-bar"><div class="brand-bar-red"></div><div class="brand-bar-gap"></div><div class="brand-bar-blue"></div></div>
<div class="header-row">
<div class="project-badge">
<span class="project-badge-red">Power</span><span class="project-badge-gap"></span><span class="project-badge-blue">Link</span>
</div>
<img src="cid:tesa_logo" class="tesa-logo" alt="tesa">
</div>
<h2>{title}</h2>
<div class="summary">
<strong>汇总统计(dt={dt})</strong><br>
总调用: {grand_total} | 成功: <span class="normal">{grand_success}</span> |
正常失败: {grand_normal} | 异常失败: <span class="warn">{grand_abnormal}</span> |
调用次数: {grand_call_count}
</div>

<h3>各接口调用详情</h3>
<table>
<tr>
<th>接口ID</th><th>接口名称</th><th>数据源</th><th>频次</th>
<th>总调用</th><th>成功</th><th>正常失败</th><th>异常失败</th><th>调用次数</th><th>状态</th>
</tr>
"""

    for s in stats_list:
        if s['total'] > 0:
            run_tag = ""
            status_str = "✅ 正常" if s['abnormal_failure'] == 0 else '<span class="warn">⚠️ 异常</span>'
        elif not s['should_run']:
            run_tag = '<span class="no-run">[未跑批]</span>'
            status_str = '<span class="no-run">— 未跑批</span>'
        else:
            run_tag = ""
            status_str = '<span class="warn">⚠️ 无数据</span>'

        abnormal_class = ' class="warn"' if s['abnormal_failure'] > 0 else ''
        html += f"""
<tr>
<td>{s['interface_key']}</td>
<td>{s['interface_name']}</td>
<td>{s['provider']}</td>
<td>{s['frequency']}{run_tag}</td>
<td>{s['total']}</td>
<td class="normal">{s['success']}</td>
<td>{s['normal_failure']}</td>
<td{abnormal_class}>{s['abnormal_failure']}</td>
<td>{s['call_count']}</td>
<td>{status_str}</td>
</tr>"""

    html += f"""
<tr class="total-row">
<td>合计</td><td>—</td><td>—</td><td>—</td>
<td>{grand_total}</td><td class="normal">{grand_success}</td>
<td>{grand_normal}</td><td{(' class="warn"' if grand_abnormal > 0 else '')}>{grand_abnormal}</td>
<td>{grand_call_count}</td><td>—</td>
</tr>"""

    html += "</table>\n"

    # 异常详情
    abnormal_items = [s for s in stats_list if s['abnormal_failure'] > 0]
    if abnormal_items:
        html += "<h3>⚠️ 异常失败详情</h3>\n"
        for s in abnormal_items:
            html += f"""<div class="detail">
<strong>{s['interface_key']} - {s['interface_name']}</strong><br>
"""
            for d in s['abnormal_details']:
                companies_str = ", ".join(d['companies'][:10])
                reasons_str = "; ".join(d['reasons'][:3]) if d['reasons'] else ""
                html += f"""
错误码 <span class="warn">{d['code']}</span> ({d['desc']}): 共{d['count']}次<br>
涉及公司: {companies_str}<br>
"""
                if reasons_str:
                    html += f"API返回信息: {reasons_str}<br>"
                html += "<br>"
            html += "</div>\n"

    # 未跑批的接口说明
    no_run_interfaces = [s for s in stats_list if s['total'] == 0 and not s['should_run']]
    if no_run_interfaces:
        html += "<h3>未跑批接口</h3>\n<p>"
        monthly_day = get_monthly_day(CONFIG)
        for s in no_run_interfaces:
            html += f"{s['interface_key']}({s['interface_name']}) - 频次为{ s['frequency']}, 月度跑批日为每月{monthly_day}号<br>"
        html += "</p>\n"

    # 正常失败说明
    normal_fail_items = [s for s in stats_list if s['normal_failure'] > 0 and s['total'] > 0]
    if normal_fail_items:
        html += "<h3>正常失败说明</h3>\n<p>以下失败属于正常范围内，公司未被收录或无数据:</p>\n<ul>\n"
        for s in normal_fail_items:
            normal_codes = get_normal_error_codes(CONFIG, s['interface_key'])
            desc_map = get_error_code_desc(CONFIG, s['provider'])
            code_descs = [f"{c}({desc_map.get(str(c), '未知')})" for c in normal_codes]
            html += f"<li>{s['interface_key']}({s['interface_name']}): 正常失败{s['normal_failure']}次, 正常错误码={', '.join(code_descs)}</li>\n"
        html += "</ul>\n"

    html += """
<div class="brand-bar"><div class="brand-bar-red"></div><div class="brand-bar-gap"></div><div class="brand-bar-blue"></div></div>
<p style="color:#5E5E5E; font-size:12px;">
此邮件由tesa®数据团队自动生成，如有疑问请联系 Powerlink.GC@tesa.com<br>
天眼查正常错误码: 300000=经查无结果 | 邓白氏正常错误码: 1=请求无结果, 1021=未收录(暂归正常), 2001=处理失败/参数问题<br>
天眼查异常错误码: 300001=请求失败, 300002=账号失效, 300003=账号过期, 300004=频率过快,
300005=无权限, 300006=余额不足, 300007=次数不足, 300008=缺少参数, 300009=账号有误,
300010=URL不存在, 300011=IP无权限, 300012=报告生成中<br>
邓白氏异常错误码: 1000=参数错误, 1001=认证参数错误, 1002=签名验证错误, 1003=IP错误,
1004=账号不可用, 1005=账号过期, 1006=余额不足, 1007=限流, 1008=日用量上限,
1009=产品不可用, 1010=产品过期, 1011=产品用完, 1012=产品未生效, 1013=余额异常,
1014=业务逻辑问题, 2002=系统错误, 2003=请求超时, 2004=配置错误
</p>
</body></html>"""

    return html


html_content = generate_html_email(all_stats, run_date, dt, need_alert)


# ========== 5. 发送邮件 ==========

def send_alert_email(config, html_content, run_date, need_alert):
    """通过SMTP发送预警邮件"""
    alert_config = get_alert_config(config)

    if not alert_config:
        print("[WARNING] 未配置预警邮件(alert section), 跳过发送")
        return False

    smtp_host = alert_config.get('smtp_host')
    smtp_port = alert_config.get('smtp_port', 465)
    smtp_ssl = alert_config.get('smtp_ssl', True)
    smtp_user = alert_config.get('smtp_user')
    smtp_password = alert_config.get('smtp_password')
    from_addr = alert_config.get('from_addr')
    to_addr = alert_config.get('to_addr', [])
    subject_prefix = alert_config.get('subject_prefix', '【外部数据预警】')

    if not all([smtp_host, smtp_user, smtp_password, from_addr, to_addr]):
        print("[WARNING] 预警邮件配置不完整, 跳过发送")
        return False

    subject = f"{subject_prefix} {run_date} "
    if need_alert:
        subject += "⚠️ 存在异常失败"
    else:
        subject += "✅ 运行正常"

    msg = MIMEMultipart('related')
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addr)

    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    # 嵌入tesa logo(CID方式)
    logo_path = alert_config.get('logo_path', '')
    if logo_path and os.path.exists(logo_path):
        with open(logo_path, 'rb') as f:
            logo_data = f.read()
        logo_part = MIMEImage(logo_data)
        logo_part.add_header('Content-ID', '<tesa_logo>')
        msg.attach(logo_part)

    try:
        if smtp_ssl:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()

        server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
        print(f"[SUCCESS] 预警邮件已发送: {subject} → {', '.join(to_addr)}")
        return True
    except Exception as e:
        print(f"[ERROR] 预警邮件发送失败: {e}")
        return False


# ========== 6. 执行 ==========

# send_result = send_alert_email(CONFIG, html_content, run_date, need_alert)

# print("\n" + "=" * 60)
# print("分析完成！")
# print("-" * 60)
# if need_alert:
#     print("⚠️ 本次存在异常失败，已发送预警邮件")
# else:
#     print("✅ 所有接口运行正常")
# if send_result:
#     print("邮件发送成功")
# else:
#     print("邮件未发送(配置缺失或发送失败)")
# print("=" * 60)

displayHTML(html_content)
