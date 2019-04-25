--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_WBL_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_WBL_TEST" 
(
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
  ---
  --- RUN USING DWH_WH_PRF_ALASTAIR
  ---
AS
  g_sql VARCHAR2(8000);
  g_start DATE   := '01 Jul 2009';
  g_end DATE     := '30 Sep 2009';
  g_count               NUMBER := 0;
  g_recs_inserted       NUMBER := 0;
  g_cnt                 NUMBER;
  Gp_table_name         VARCHAR2(31) := 'stg_rms_rtl_allocation';
  Gp_log_script_name    VARCHAR2(31) :='';
  Gp_log_procedure_name VARCHAR2(31);
  Gp_description        VARCHAR2(31) := 'stg_rms_rtl_allocation';
  g_stmt                VARCHAR2(1500);
  g_table_name          VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION';
  --g_arc_table_name    varchar2(31);
  --g_hsp_table_name    varchar2(31);
  g_cpy_table_name VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION_CPY';
  g_index_name     VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION';
  g_cpy_index_name VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION_CPY';
  g_pk_name        VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN';
  g_cpy_pk_name    VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN_CPY';
  g_pk_stmt        VARCHAR2(1500);
  g_tablespace     VARCHAR2(31) := 'STG_STAGING';
  g_deal           NUMBER(14);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_HR_WBL_TEST';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'TEST';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  --
  --**************************************************************************************************
BEGIN
  p_success := FALSE;
   
  l_text   := 'UPDATE OF SK2 FOR DWH_HR_PERFORMANCE.hr_bee_ed_payment_mn started';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

FOR V_CUR IN
(WITH SELEXT AS (SELECT app.sk1_benefit_factor_id1 sk1_benefit_factor_id,
  MAX(deb.ed_category) ed_category,
  cal.this_mn_end_date this_mn_end_date,
  deb.sk1_ed_beneficiary_id sk1_ed_beneficiary_id,
  MAX(debh.sk2_ed_beneficiary_id) sk2_ed_beneficiary_id,
  MAX(bf.ed_percentage) ed_percentage,
  SUM(app.ed_payment_terms) ed_payment_terms
  --          sum(app.ed_settlement_discount) ed_settlement_discount
FROM DWH_HR_PERFORMANCE.hr_bee_ap_invoice_payment app,
  DWH_PERFORMANCE.dim_calendar cal,
  DWH_HR_PERFORMANCE.dim_hr_bee_supplier ds,
  DWH_HR_PERFORMANCE.dim_hr_bee_ed_beneficiary deb,
  DWH_HR_PERFORMANCE.dim_hr_bee_ed_beneficiary_hist debh,
  DWH_HR_PERFORMANCE.dim_hr_bee_benefit_factor bf
WHERE app.sk1_supplier_id = ds.sk1_supplier_id
AND ds.supplier_no        = TO_CHAR(deb.vendor_no)
AND ds.supplier_no        = TO_CHAR(debh.vendor_no)
AND cal.this_mn_end_date BETWEEN debh.sk2_active_from_date AND debh.sk2_active_to_date
AND app.payment_date            = cal.calendar_date
AND app.sk1_benefit_factor_id1  = bf.sk1_benefit_factor_id
AND app.sk1_benefit_factor_id1 <> 0
GROUP BY app.sk1_benefit_factor_id1,
  cal.this_mn_end_date,
  deb.sk1_ed_beneficiary_id)
  SELECT SE.sk1_benefit_factor_id sk1_benefit_factor_id,
  SE.ed_category ed_category,
  SE.this_mn_end_date this_mn_end_date,
  SE.sk1_ed_beneficiary_id sk1_ed_beneficiary_id,
  SE.sk2_ed_beneficiary_id sk2_ed_beneficiary_id,
  SE.ed_percentage ed_percentage,
   SE.ed_payment_terms d_payment_terms
  FROM SELEXT SE
)
LOOP
  UPDATE DWH_HR_PERFORMANCE.hr_bee_ed_payment_mn A
  SET A.sk2_ed_beneficiary_id   = V_CUR.sk2_ed_beneficiary_id
  WHERE A.sk1_ed_beneficiary_id = V_CUR.sk1_ed_beneficiary_id
  AND A.sk1_benefit_factor_id   = V_CUR.sk1_benefit_factor_id
  AND A.payment_mn_end_date     = V_CUR.THIS_mn_end_date;
  
     g_recs_inserted:= g_recs_inserted + 1;
  COMMIT;
  
END LOOP;
l_text   := 'UPDATE OF SK2 FOR DWH_HR_PERFORMANCE.hr_bee_ed_payment_mn = '||g_recs_inserted  ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


FOR V_CUR IN
(WITH SELEXT AS (
select app.sk1_benefit_factor_id2 sk1_benefit_factor_id2,
          cal.this_mn_end_date this_mn_end_date,
          max(deb.ed_category) ed_category,
          deb.sk1_ed_beneficiary_id sk1_ed_beneficiary_id,
          max(debh.sk2_ed_beneficiary_id) sk2_ed_beneficiary_id,
          max(bf.ed_percentage) ed_percentage,
--          sum(app.ed_payment_terms) ed_payment_terms
          sum(app.ed_settlement_discount) ed_settlement_discount
   from   hr_bee_ap_invoice_payment  app,
          dim_calendar cal,
          dim_hr_bee_supplier ds,
          dim_hr_bee_ed_beneficiary deb,
          dim_hr_bee_ed_beneficiary_hist debh,
          dim_hr_bee_benefit_factor bf
   where  app.sk1_supplier_id       = ds.sk1_supplier_id     and
          ds.supplier_no            = to_char(deb.vendor_no) and
          ds.supplier_no            = to_char(debh.vendor_no) and
          cal.this_mn_end_date    between debh.sk2_active_from_date and debh.sk2_active_to_date and
          app.payment_date          = cal.calendar_date      and
          app.sk1_benefit_factor_id2 = bf.sk1_benefit_factor_id and
          app.sk1_benefit_factor_id2 <> 0
   group by
          app.sk1_benefit_factor_id2,
          cal.this_mn_end_date,
          deb.sk1_ed_beneficiary_id  
)
  SELECT SE.sk1_benefit_factor_id2 sk1_benefit_factor_id2,
  SE.ed_category ed_category,
  SE.this_mn_end_date this_mn_end_date,
  SE.sk1_ed_beneficiary_id sk1_ed_beneficiary_id,
  SE.sk2_ed_beneficiary_id sk2_ed_beneficiary_id,
  SE.ed_percentage ed_percentage
  --,
 --  SE.ed_payment_terms Ed_payment_terms
  FROM SELEXT SE
)
LOOP
  UPDATE DWH_HR_PERFORMANCE.hr_bee_ed_payment_mn A
  SET A.sk2_ed_beneficiary_id   = V_CUR.sk2_ed_beneficiary_id
  WHERE A.sk1_ed_beneficiary_id = V_CUR.sk1_ed_beneficiary_id
  AND A.sk1_benefit_factor_id   = V_CUR.sk1_benefit_factor_id2
  AND A.payment_mn_end_date     = V_CUR.THIS_mn_end_date;
  COMMIT;
       g_recs_inserted:= g_recs_inserted + 1;
END LOOP;
l_text   := 'UPDATE OF SK2 FOR DWH_HR_PERFORMANCE.hr_bee_ed_payment_mn = '||g_recs_inserted  ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);





  p_success := true;

END WH_PRF_HR_WBL_TEST;
