#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 powerlink 数据库的数据字典 Excel"""

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

wb = Workbook()

# ========== 样式 ==========
header_font = Font(name='微软雅黑', bold=True, size=11, color='FFFFFF')
header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
title_font = Font(name='微软雅黑', bold=True, size=14, color='1F4E79')
section_font = Font(name='微软雅黑', bold=True, size=12, color='2E75B6')
normal_font = Font(name='微软雅黑', size=10)
thin_border = Border(left=Side(style='thin', color='D9D9D9'), right=Side(style='thin', color='D9D9D9'), top=Side(style='thin', color='D9D9D9'), bottom=Side(style='thin', color='D9D9D9'))
center_align = Alignment(horizontal='center', vertical='center', wrap_text=True)
left_align = Alignment(horizontal='left', vertical='center', wrap_text=True)
alt_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')

headers = ['序号', '字段名', '中文名称', '数据类型', '长度', '是否主键', '是否允许空', '默认值', '来源系统', '原始字段路径', '转换规则', '备注']
col_widths = [6, 25, 18, 12, 8, 10, 10, 20, 10, 30, 24, 36]


def write_sheet(ws, overview_data, fields_data, start_overview_row=1, start_fields_row=10):
    # 概览区
    ws.merge_cells(f'A{start_overview_row}:L{start_overview_row}')
    ws[f'A{start_overview_row}'] = '数据表概览'
    ws[f'A{start_overview_row}'].font = title_font
    for i, (k, v) in enumerate(overview_data):
        r = start_overview_row + 1 + i
        ws[f'A{r}'] = k
        ws[f'A{r}'].font = Font(name='微软雅黑', bold=True, size=10)
        ws[f'B{r}'] = v
        ws[f'B{r}'].font = normal_font

    # 字段区标题
    title_row = start_fields_row
    ws.merge_cells(f'A{title_row}:L{title_row}')
    ws[f'A{title_row}'] = '字段明细'
    ws[f'A{title_row}'].font = section_font

    # 表头
    header_row = title_row + 1
    for col_idx, h in enumerate(headers, 1):
        cell = ws.cell(row=header_row, column=col_idx)
        cell.value = h
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_align
        cell.border = thin_border

    # 数据行
    for row_idx, field in enumerate(fields_data):
        r = header_row + 1 + row_idx
        for col_idx, val in enumerate(field, 1):
            cell = ws.cell(row=r, column=col_idx)
            cell.value = val
            cell.font = normal_font
            cell.alignment = left_align if col_idx > 3 else center_align
            cell.border = thin_border
            if row_idx % 2 == 1:
                cell.fill = alt_fill

    # 列宽
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


# ========== Sheet1: api_call_record ==========
ws1 = wb.active
ws1.title = 'api_call_record'

overview1 = [
    ('数据库', 'powerlink'), ('表名', 'api_call_record'), ('表描述', '三方接口调用记录表'),
    ('引擎', 'InnoDB'), ('字符集', 'utf8mb4'), ('所属系统', '天眼查数据接入'),
    ('负责人', ''), ('创建日期', '2026-05-14'),
]

fields1 = [
    (1, 'id', '主键ID', 'BIGINT', '', 'Y', 'N', '自增', '内部', '-', '-', '自增主键'),
    (2, 'interface_name', '接口名', 'VARCHAR', '32', 'N', 'N', '', '内部', '-', '-', '标识819/967/971/973等接口'),
    (3, 'call_datetime', '调用日期时间', 'DATETIME', '', 'N', 'N', '', '内部', '-', '-', 'API调用发生的时间'),
    (4, 'input_param', '入参(公司名)', 'VARCHAR', '200', 'N', 'N', '', '内部', '-', '-', '搜索关键字/公司名'),
    (5, 'status_code', '状态码', 'INT', '', 'N', 'N', '', '天眼查', 'result.error_code', '-', '0=成功，负数=异常，正数=业务错误码'),
    (6, 'output_result', '出参结果', 'JSON', '', 'N', 'Y', '', '天眼查', 'response body', '原始JSON完整存储', '成功时为API完整响应JSON，失败时为错误详情JSON'),
    (7, 'create_time', '创建时间', 'DATETIME', '', 'N', 'Y', 'CURRENT_TIMESTAMP', '内部', '-', '-', '记录入库时间'),
]

write_sheet(ws1, overview1, fields1)

# ========== Sheet2: company_819_info ==========
ws2 = wb.create_sheet('company_819_info')

overview2 = [
    ('数据库', 'powerlink'), ('表名', 'company_819_info'), ('表描述', '企业基本信息(819接口)'),
    ('引擎', 'InnoDB'), ('字符集', 'utf8mb4'), ('所属系统', '天眼查数据接入'),
    ('唯一约束', 'company_name'), ('创建日期', '2026-05-14'),
    ('解析规则说明', 'Array+child String→逗号分隔字符串; Object+多KV→展开为独立列; Object+可能多条→JSON字符串+提取total; Number时间戳→DATETIME'),
]

fields2 = [
    # 基础标识
    (1, 'id', '主键ID', 'BIGINT', '', 'Y', 'N', '自增', '内部', '-', '-', '自增主键'),
    (2, 'api_record_id', 'API调用记录ID', 'BIGINT', '', 'N', 'Y', '', '内部', 'api_call_record.id', '-', '关联api_call_record表，可追溯原始API调用'),
    (3, 'data_create_time', '数据创建时间', 'DATETIME', '', 'N', 'Y', 'CURRENT_TIMESTAMP', '内部', '-', '-', '解析入库时间'),
    (4, 'company_name', '公司名(搜索关键字)', 'VARCHAR', '200', 'UK', 'N', '', '内部', '-', '-', '搜索关键字/公司名，唯一约束'),
    (5, 'company_id', '企业ID', 'BIGINT', '', 'N', 'Y', '', '天眼查', 'result.id', '-', '天眼查企业唯一ID'),

    # 法人/类型
    (6, 'legal_person_type', '法人类型', 'INT', '', 'N', 'Y', '', '天眼查', 'result.type', '-', '1=自然人，2=公司'),
    (7, 'reg_status', '经营状态', 'VARCHAR', '31', 'N', 'Y', '', '天眼查', 'result.regStatus', '-', '如:存续、注销、吊销等'),
    (8, 'legal_person_name', '法定代表人', 'VARCHAR', '120', 'N', 'Y', '', '天眼查', 'result.legalPersonName', '-', ''),
    (9, 'company_org_type', '企业类型', 'VARCHAR', '127', 'N', 'Y', '', '天眼查', 'result.companyOrgType', '-', '如:其他股份有限公司(上市)'),
    (10, 'is_micro_ent', '是否小微企业', 'INT', '', 'N', 'Y', '', '天眼查', 'result.isMicroEnt', '-', '0=否，1=是'),

    # 资本
    (11, 'reg_capital', '注册资本(含单位)', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.regCapital', '-', '如:730710.9252万人民币'),
    (12, 'reg_capital_currency', '注册资本币种', 'VARCHAR', '10', 'N', 'Y', '', '天眼查', 'result.regCapitalCurrency', '-', '人民币/美元/欧元等'),
    (13, 'paid_capital', '实缴资本(含单位)', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.actualCapital', '字段映射: actualCapital→paid_capital', '如:183130.690104万人民币'),
    (14, 'paid_capital_currency', '实缴资本币种', 'VARCHAR', '10', 'N', 'Y', '', '天眼查', 'result.actualCapitalCurrency', '字段映射: actualCapitalCurrency→paid_capital_currency', '人民币/美元/欧元等'),

    # 时间(时间戳→datetime)
    (15, 'est_date', '成立日期', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.estiblishTime', '时间戳→datetime: ≥1e10为毫秒÷1000', '字段映射: estiblishTime→est_date'),
    (16, 'from_date', '经营开始时间', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.fromTime', '时间戳→datetime: ≥1e10为毫秒÷1000', '字段映射: fromTime→from_date'),
    (17, 'to_date', '经营结束时间', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.toTime', '时间戳→datetime: ≥1e10为毫秒÷1000', 'null表示无固定期限'),
    (18, 'approval_date', '核准时间', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.approvedTime', '时间戳→datetime: ≥1e10为毫秒÷1000', '字段映射: approvedTime→approval_date'),
    (19, 'cancel_date', '注销日期', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.cancelDate', '时间戳→datetime: ≥1e10为毫秒÷1000', ''),
    (20, 'revoke_date', '吊销日期', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.revokeDate', '时间戳→datetime: ≥1e10为毫秒÷1000', ''),
    (21, 'cancel_reason', '注销原因', 'VARCHAR', '500', 'N', 'Y', '', '天眼查', 'result.cancelReason', '-', ''),
    (22, 'revoke_reason', '吊销原因', 'VARCHAR', '500', 'N', 'Y', '', '天眼查', 'result.revokeReason', '-', ''),
    (23, 'update_time', '更新时间', 'DATETIME', '', 'N', 'Y', '', '天眼查', 'result.updateTimes', '时间戳→datetime: ≥1e10为毫秒÷1000', '字段映射: updateTimes→update_time'),

    # 证件编码
    (24, 'social_credit_code', '统一社会信用代码', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.creditCode', '字段映射: creditCode→social_credit_code', ''),
    (25, 'org_code', '组织机构代码', 'VARCHAR', '31', 'N', 'Y', '', '天眼查', 'result.orgNumber', '字段映射: orgNumber→org_code', ''),
    (26, 'tax_number', '纳税人识别号', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.taxNumber', '-', ''),
    (27, 'reg_number', '注册号', 'VARCHAR', '31', 'N', 'Y', '', '天眼查', 'result.regNumber', '-', ''),
    (28, 'brn_number', '商业登记号', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.BRNNumber', '-', 'BRN缩写保留'),

    # 地理
    (29, 'province_short', '省份简称', 'VARCHAR', '31', 'N', 'Y', '', '天眼查', 'result.base', '字段映射: base→province_short', '如:gd=广东'),
    (30, 'city', '市', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.city', '-', ''),
    (31, 'district', '区', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.district', '-', ''),
    (32, 'district_code', '行政区划代码', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.districtCode', '-', ''),
    (33, 'reg_location', '注册地址', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.regLocation', '-', ''),
    (34, 'reg_location_half_width', '注册地址(半角)', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.regLocationHalfWidth', '-', '半角字符版本'),
    (35, 'reg_institute', '登记机关', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.regInstitute', '-', ''),
    (36, 'economic_function_zone1', '经济功能区1', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.economicFunctionZone1', '-', ''),
    (37, 'economic_function_zone2', '经济功能区2', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.economicFunctionZone2', '-', ''),
    (38, 'above_scale', '是否规模以上', 'VARCHAR', '10', 'N', 'Y', '', '天眼查', 'result.aboveScale', '-', '如:规模以上工业'),

    # 经营范围/行业
    (39, 'business_scope', '经营范围', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.businessScope', '-', ''),
    (40, 'industry', '行业', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industry', '-', ''),

    # 名称/别名
    (41, 'company_alias', '简称', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.alias', '字段映射: alias→company_alias', '企业简称/别名'),
    (42, 'used_bond_name', '股票曾用名', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.usedBondName', '-', ''),
    (43, 'property3', '英文名', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.property3', '-', 'property3缩写保留'),

    # 联系方式
    (44, 'email', '邮箱(单个)', 'VARCHAR', '1024', 'N', 'Y', '', '天眼查', 'result.email', '-', 'API返回的第一个邮箱'),
    (45, 'email_list', '全部邮箱', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.emailList', 'Array+child String → 逗号分隔字符串', '如: a@b.com,c@d.com,d@e.com'),
    (46, 'phone_number', '企业联系方式', 'VARCHAR', '1024', 'N', 'Y', '', '天眼查', 'result.phoneNumber', '-', ''),
    (47, 'website_list', '网址', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.websiteList', '-', ''),

    # 曾用名
    (48, 'history_names', '曾用名', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.historyNameList', 'Array+child String → 逗号分隔字符串', '忽略historyNames(分号分隔)，取historyNameList(JSON数组)'),

    # 股票
    (49, 'bond_name', '股票名', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.bondName', '-', ''),
    (50, 'bond_num', '股票号', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.bondNum', '-', ''),
    (51, 'bond_type', '股票类型', 'VARCHAR', '31', 'N', 'Y', '', '天眼查', 'result.bondType', '-', '如:A股'),

    # 评分/规模
    (52, 'tags', '企业标签', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.tags', '-', ''),
    (53, 'staff_num_range', '人员规模', 'VARCHAR', '200', 'N', 'Y', '', '天眼查', 'result.staffNumRange', '-', '如:5000-9999人'),
    (54, 'social_staff_num', '参保人数', 'INT', '', 'N', 'Y', '', '天眼查', 'result.socialStaffNum', '-', ''),
    (55, 'percentile_score', '企业评分(万分制)', 'INT', '', 'N', 'Y', '', '天眼查', 'result.percentileScore', '-', ''),

    # staffList(Object+可能多条)
    (56, 'staff_list_total', '主要人员总数', 'INT', '', 'N', 'Y', '', '天眼查', 'result.staffList.total', 'Object提取total字段', ''),
    (57, 'staff_list_json', '主要人员列表', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.staffList.result', 'Object+可能多条 → JSON字符串', 'JSON数组，每项含name/id/type/typeJoin'),

    # industryAll(Object+多KV展开)
    (58, 'industry_all_category', '国民经济行业分类-门类', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.category', 'Object+多KV展开', '如:制造业'),
    (59, 'industry_all_category_big', '国民经济行业分类-大类', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryBig', 'Object+多KV展开', '如:计算机、通信和其他电子设备制造业'),
    (60, 'industry_all_category_middle', '国民经济行业分类-中类', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryMiddle', 'Object+多KV展开', '如:其他电子设备制造'),
    (61, 'industry_all_category_small', '国民经济行业分类-小类', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categorySmall', 'Object+多KV展开', ''),
    (62, 'industry_all_category_code_first', '国民经济行业分类-门类代码', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryCodeFirst', 'Object+多KV展开', '如:C'),
    (63, 'industry_all_category_code_second', '国民经济行业分类-大类代码', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryCodeSecond', 'Object+多KV展开', '如:39'),
    (64, 'industry_all_category_code_third', '国民经济行业分类-中类代码', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryCodeThird', 'Object+多KV展开', '如:399'),
    (65, 'industry_all_category_code_fourth', '国民经济行业分类-小类代码', 'VARCHAR', '255', 'N', 'Y', '', '天眼查', 'result.industryAll.categoryCodeFourth', 'Object+多KV展开', '如:3990'),
]

write_sheet(ws2, overview2, fields2)

# ========== Sheet3: customer_info ==========
ws3 = wb.create_sheet('customer_info')

overview3 = [
    ('数据库', 'powerlink'), ('表名', 'customer_info'), ('表描述', '客户公司列表'),
    ('引擎', 'InnoDB'), ('字符集', 'utf8mb4'), ('所属系统', '天眼查数据接入'),
    ('负责人', ''), ('创建日期', '2026-05-14'),
]

fields3 = [
    (1, 'id', '主键ID', 'BIGINT', '', 'Y', 'N', '自增', '内部', '-', '-', '自增主键'),
    (2, 'customer_name', '公司名', 'VARCHAR', '200', 'N', 'N', '', '内部', '-', '-', '客户公司名称，用于API搜索关键字'),
    (3, 'create_time', '创建时间', 'DATETIME', '', 'N', 'Y', 'CURRENT_TIMESTAMP', '内部', '-', '-', '记录入库时间'),
]

write_sheet(ws3, overview3, fields3)

# ========== Sheet4: company_1058_risk_info ==========
ws4 = wb.create_sheet('company_1058_risk_info')

overview4 = [
    ('数据库', 'powerlink'), ('表名', 'company_1058_risk_info'), ('表描述', '企业天眼风险(1058接口)'),
    ('引擎', 'InnoDB'), ('字符集', 'utf8mb4'), ('所属系统', '天眼查数据接入'),
    ('数据关系', '1:N(1公司→N条风险记录)'), ('创建日期', '2026-05-14'),
    ('解析规则说明', '3层嵌套展平(riskList→list→list); DELETE旧数据+INSERT新数据; main_company_name来自搜索入参'),
]

fields4 = [
    (1, 'id', '主键ID', 'BIGINT', '', 'Y', 'N', '自增', '内部', '-', '-', '自增主键'),
    (2, 'api_record_id', 'API调用记录ID', 'BIGINT', '', 'N', 'Y', '', '内部', 'api_call_record.id', '-', '关联api_call_record表，可追溯原始API调用'),
    (3, 'data_create_time', '数据创建时间', 'DATETIME', '', 'N', 'Y', 'CURRENT_TIMESTAMP', '内部', '-', '-', '解析入库时间'),
    (4, 'main_company_name', '主公司名(搜索关键字)', 'VARCHAR', '200', 'N', 'N', '', '内部', '-', '-', '来自搜索入参(input_param)，非API返回'),
    (5, 'risk_level', '风险等级', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.riskLevel', '-', '顶层风险等级'),
    (6, 'risk_category_count', '风险类别下的条数', 'INT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].count', '3层嵌套展平meta字段', '该风险类别下的风险条数'),
    (7, 'risk_category_name', '风险类别名', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.riskList[].name', '3层嵌套展平meta字段', '自身风险/周边风险/历史风险/预警提醒'),
    (8, 'risk_type_total', '风险类型下的条数', 'INT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].total', '3层嵌套展平meta字段', '该风险类型组下的条数'),
    (9, 'risk_type_tag', '风险标签', 'VARCHAR', '50', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].tag', '3层嵌套展平meta字段', '警示/高风险/提示信息'),
    (10, 'company_id', '涉及公司ID', 'BIGINT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].companyId', '-', '可空，风险涉及的公司ID'),
    (11, 'company_name', '涉及公司名', 'VARCHAR', '200', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].companyName', '空字符串→NULL', '可空，风险涉及的公司名'),
    (12, 'risk_id', '风险条目ID', 'BIGINT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].id', '字段映射: id→risk_id', '避免与表主键冲突'),
    (13, 'risk_count', '风险数量', 'INT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].riskCount', '-', ''),
    (14, 'risk_title', '风险描述', 'VARCHAR', '500', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].title', '-', '风险详情描述'),
    (15, 'risk_type', '风险类型码', 'INT', '', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].type', '-', '风险类型编码'),
    (16, 'risk_desc', '风险简述', 'VARCHAR', '200', 'N', 'Y', '', '天眼查', 'result.riskList[].list[].list[].desc', '-', '风险简要分类描述'),
]

write_sheet(ws4, overview4, fields4)

# ========== Sheet5: company_822_change_info ==========
ws5 = wb.create_sheet('company_822_change_info')

overview5 = [
    ('数据库', 'powerlink'), ('表名', 'company_822_change_info'), ('表描述', '变更记录(822接口)'),
    ('引擎', 'InnoDB'), ('字符集', 'utf8mb4'), ('所属系统', '天眼查数据接入'),
    ('数据关系', '1:N(1公司→N条变更记录)'), ('创建日期', '2026-05-15'),
    ('解析规则说明', '2层展平(result.items数组); DELETE旧数据+INSERT新数据; company_name来自搜索入参'),
]

fields5 = [
    (1, 'id', '主键ID', 'BIGINT', '', 'Y', 'N', '自增', '内部', '-', '-', '自增主键'),
    (2, 'api_record_id', 'API调用记录ID', 'BIGINT', '', 'N', 'Y', '', '内部', 'api_call_record.id', '-', '关联api_call_record表'),
    (3, 'data_create_time', '数据创建时间', 'DATETIME', '', 'N', 'Y', 'CURRENT_TIMESTAMP', '内部', '-', '-', '解析入库时间'),
    (4, 'company_name', '主公司名(搜索关键字)', 'VARCHAR', '200', 'N', 'N', '', '内部', '-', '-', '来自搜索入参(input_param)，非API返回'),
    (5, 'total', '变更记录总数', 'INT', '', 'N', 'Y', '', '天眼查', 'result.total', '2层展平meta字段', '变更记录总条数'),
    (6, 'change_item', '变更项名称', 'VARCHAR', '200', 'N', 'Y', '', '天眼查', 'result.items[].changeItem', '驼峰→下划线', '如: 经营范围变更'),
    (7, 'content_before', '变更前内容', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.items[].contentBefore', '空字符串→NULL', ''),
    (8, 'content_after', '变更后内容', 'TEXT', '', 'N', 'Y', '', '天眼查', 'result.items[].contentAfter', '空字符串→NULL', ''),
    (9, 'change_time', '变更时间', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.items[].changeTime', '-', '日期字符串，如2025-03-26'),
    (10, 'create_time', '记录创建时间', 'VARCHAR', '20', 'N', 'Y', '', '天眼查', 'result.items[].createTime', '-', '日期字符串，如2025-03-27'),
]

write_sheet(ws5, overview5, fields5)

# ========== 保存 ==========
output_path = '/Users/wangshuaijia/workspace/tyc/数据字典_powerlink.xlsx'
wb.save(output_path)
print(f"数据字典已生成: {output_path}")