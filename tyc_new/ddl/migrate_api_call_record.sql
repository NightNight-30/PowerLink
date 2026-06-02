-- ============================================
-- 历史数据迁移: ods_api_call_record_df → 各接口独立表
-- 将共享表的测试数据按interface_name分配到对应独立表的dt='20260601'分区
-- ============================================

-- 819 - 企业基本信息（含主要人员）
INSERT INTO powerlink.pw_ods.ods_api_call_record_819_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '企业基本信息（含主要人员）';

-- 851 - 欠税公告
INSERT INTO powerlink.pw_ods.ods_api_call_record_851_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '欠税公告';

-- 1058 - 企业天眼风险
INSERT INTO powerlink.pw_ods.ods_api_call_record_1058_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '企业天眼风险';

-- 822 - 变更记录
INSERT INTO powerlink.pw_ods.ods_api_call_record_822_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '变更记录';

-- 854 - 上市公司企业简介
INSERT INTO powerlink.pw_ods.ods_api_call_record_854_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '上市公司企业简介';

-- 1168 - 组织机构
INSERT INTO powerlink.pw_ods.ods_api_call_record_1168_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '组织机构';

-- 1149 - 企业规模
INSERT INTO powerlink.pw_ods.ods_api_call_record_1149_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '企业规模';

-- 967 - 主要指标-年度
INSERT INTO powerlink.pw_ods.ods_api_call_record_967_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '主要指标-年度';

-- 1114 - 法律诉讼
INSERT INTO powerlink.pw_ods.ods_api_call_record_1114_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '法律诉讼';

-- 973 - 现金流量表
INSERT INTO powerlink.pw_ods.ods_api_call_record_973_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '现金流量表';

-- P51060 - 付款指数（PAYDEX®）
INSERT INTO powerlink.pw_ods.ods_api_call_record_P51060_df
SELECT id, interface_name, call_datetime, input_param, status_code, output_result, create_time, '20260601' AS dt
FROM powerlink.pw_ods.ods_api_call_record_df
WHERE interface_name = '付款指数（PAYDEX®）';