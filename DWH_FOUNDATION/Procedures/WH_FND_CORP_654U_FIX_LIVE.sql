--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_654U_FIX_LIVE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_654U_FIX_LIVE" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        August 2008
  --  Author:      Alastair de Wet
  --  Purpose:     Load deal_actual detail information in the foundation layer
  --               with input ex staging table from RMS.
  --  Tables:      Input  - stg_rms_deal_actual_detail_cpy
  --               Output - fnd_deal_actual_detail
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --
  --  Maintenance
  --  28 jan 2011 qc4141 Key for FND_DEAL_ACTUAL_DETAIL
  --                       and FND_DEAL_ACTUAL_DETAIL_RTV is not unique enough
  --  13 Apr 2011 qc414 - Add CREATE_DATE_TIME
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := 10000;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_start_deal_no   NUMBER  := 0;
  g_end_deal_no   NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_hospital_text stg_rms_deal_actual_detail_hsp.sys_process_msg%type;
  g_rec_out fnd_deal_actual_detail%rowtype;
  g_rec_in stg_rms_deal_actual_detail_cpy%rowtype;
  g_found BOOLEAN;
  g_valid BOOLEAN;
  --g_date              date          := to_char(sysdate,('dd mon yyyy'));
  g_date DATE := TRUNC(sysdate);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_654U_FIX_LIVE';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_tran;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_tran;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE FND_DEAL_ACTUAL_DETAIL DATA EX RMS';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF stg_rms_deal_actual_detail_cpy%rowtype;
  a_stg_input stg_array;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF fnd_deal_actual_detail%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF fnd_deal_actual_detail%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  a_count_u INTEGER := 0;
  -- For arrays used to update the staging table process_code --
type staging_array1
IS
  TABLE OF stg_rms_deal_actual_detail_cpy.sys_source_batch_id%type INDEX BY binary_integer;
type staging_array2
IS
  TABLE OF stg_rms_deal_actual_detail_cpy.sys_source_sequence_no%type INDEX BY binary_integer;
  a_staging1 staging_array1;
  a_staging2 staging_array2;
  a_empty_set_s1 staging_array1;
  a_empty_set_s2 staging_array2;
  a_count_stg INTEGER := 0;
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF FND_deal_actual_detail EX RMS STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_foundation.temp_deal_actual_detail');
  COMMIT;
  l_text := 'TRUNCATE COMPLETED OF dwh_foundation.temp_deal_actual_detail';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
     execute immediate 'alter session enable parallel dml';

     ww_dbms_stats.gather_table_stats('dwh_foundation','temp_deal_actual_detail');
    l_text := 'TABLE temp_deal_actual_detail STATISTICS UPDATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
   g_start_deal_no := 0;
  g_end_deal_no := 4999;
  
--     g_start_deal_no := 30000;
--  g_end_deal_no := 32999;
  
    FOR G_Sub IN 0..10
  LOOP
  
--      execute immediate 'alter session set workarea_size_policy=manual';
--    execute immediate 'alter session set sort_area_size=100000000';
--    execute immediate 'alter session enable parallel dml';
  INSERT
  INTO dwh_foundation.temp_deal_actual_detail
WITH selstg AS
  (SELECT
   -- /*+ parallel(B,2) parallel(A,2) */
    a.SYS_SOURCE_BATCH_ID ,
    a.SYS_SOURCE_SEQUENCE_NO ,
    a.SYS_LOAD_DATE ,
    a.SYS_PROCESS_CODE ,
    a.SYS_LOAD_SYSTEM_NAME ,
    a.SYS_MIDDLEWARE_BATCH_ID ,
    a.SYS_PROCESS_MSG ,
    a.SOURCE_DATA_STATUS_CODE ,
    a.DEAL_NO ,
    a.DEAL_DETAIL_NO ,
    a.PO_NO ,
    a.SHIPMENT_NO ,
    a.TRAN_DATE ,
    a.ITEM_NO ,
    a.LOCATION_NO ,
    a.ACCRUAL_PERIOD_MONTH_END_DATE ,
    a.TOTAL_QTY ,
    a.TOTAL_COST ,
    a.PURCH_SALES_ADJ_CODE ,
    --    to_char(a.create_datetime, 'dd-mm-yyy hh24:mi:ss') create_datetime,
    a.create_datetime,
    A.Last_Update_Datetime
  FROM Dwh_Foundation.Stg_Rms_Deal_Actual_Detail_Arc A,
    dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC B,
    Fnd_Deal Fd
  WHERE 
-- a.deal_no                                        in(35309,33810)
--  AND a.deal_detail_no                                   = 1
--  AND a.po_no                                            = 7149957
--  AND a.shipment_no                                      = 9512787
--  AND a.tran_date                                        = '25/JUL/12'
--  AND a.item_no                                          = 6008000679664
--  AND a.location_no                                      = 6060
--  AND 
  b.deal_no between g_start_deal_no and g_end_deal_no
  and 
  b.deal_no                                          = fd.deal_no
  AND b.maxsource                                        = a.sys_source_batch_id
  AND a.deal_no                                          = b.deal_no
  AND A.Deal_Detail_No                                   = B.Deal_Detail_No
  AND a.po_no                                            = b.po_no
  AND A.Shipment_No                                      = B.Shipment_No
  AND a.tran_date                                        = b.tran_date
  AND A.Item_No                                          = B.Item_No
  AND A.Location_No                                      = B.Location_No
  AND TO_CHAR(a.create_datetime, 'dd-mm-yyyy hh24:mi:ss') = TO_CHAR(b.create_datetime, 'dd-mm-yyyy hh24:mi:ss')
  GROUP BY a.SYS_SOURCE_BATCH_ID ,
    a.SYS_SOURCE_SEQUENCE_NO ,
    a.SYS_LOAD_DATE ,
    a.SYS_PROCESS_CODE ,
    a.SYS_LOAD_SYSTEM_NAME ,
    a.SYS_MIDDLEWARE_BATCH_ID ,
    a.SYS_PROCESS_MSG ,
    a.SOURCE_DATA_STATUS_CODE ,
    a.DEAL_NO ,
    a.DEAL_DETAIL_NO ,
    a.PO_NO ,
    a.SHIPMENT_NO ,
    a.TRAN_DATE ,
    a.ITEM_NO ,
    a.LOCATION_NO ,
    a.ACCRUAL_PERIOD_MONTH_END_DATE ,
    a.TOTAL_QTY ,
    a.TOTAL_COST ,
    a.PURCH_SALES_ADJ_CODE ,
    a.create_datetime,
    a.LAST_UPDATE_DATETIME
  )
SELECT 
--/*+ parallel (fnd,2) */ 
fnd.DEAL_NO ,
  fnd.DEAL_DETAIL_NO ,
  fnd.PO_NO ,
  fnd.SHIPMENT_NO ,
  fnd.TRAN_DATE ,
  fnd.ITEM_NO ,
  fnd.LOCATION_NO ,
  fnd.ACCRUAL_PERIOD_MONTH_END_DATE ,
  fnd.TOTAL_QTY ,
  fnd.TOTAL_COST ,
  fnd.PURCH_SALES_ADJ_CODE
  --,to_timestamp(fnd.create_datetime, 'dd-mm-yyy hh24:mi:ss')
  ,
  fnd.create_datetime ,
  fnd.LAST_UPDATE_DATETIME ,
  fnd.SOURCE_DATA_STATUS_CODE ,
  fnd.LAST_UPDATED_DATE
FROM fnd_deal_actual_detail fnd,
  selstg ss
WHERE fnd.deal_no                                        = ss.deal_no
AND fnd.deal_detail_no                                   = ss.deal_detail_no
AND fnd.po_no                                            = ss.po_no
AND fnd.shipment_no                                      = ss.shipment_no
AND fnd.tran_date                                        = ss.tran_date
AND fnd.item_no                                          = ss.item_no
AND fnd.location_no                                      = ss.location_no
AND TO_CHAR(fnd.create_datetime, 'dd-mm-yyyy hh24:mi:ss') = TO_CHAR(ss.create_datetime, 'dd-mm-yyyy hh24:mi:ss')
GROUP BY fnd.DEAL_NO ,
  fnd.DEAL_DETAIL_NO ,
  fnd.PO_NO ,
  fnd.SHIPMENT_NO ,
  fnd.TRAN_DATE ,
  fnd.ITEM_NO ,
  fnd.LOCATION_NO ,
  fnd.ACCRUAL_PERIOD_MONTH_END_DATE ,
  fnd.TOTAL_QTY ,
  fnd.TOTAL_COST ,
  fnd.PURCH_SALES_ADJ_CODE ,
  fnd.create_datetime ,
  fnd.LAST_UPDATE_DATETIME ,
  fnd.SOURCE_DATA_STATUS_CODE ,
  fnd.LAST_UPDATED_DATE;
g_recs_inserted := sql%rowcount;
COMMIT;
    l_text := 'deal range ='||g_start_deal_no||'-'||g_end_deal_no||'   recs='||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  g_start_deal_no := g_start_deal_no + 5000;
  g_end_deal_no := g_end_deal_no + 5000;

end loop;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_run_completed ||sysdate;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := ' ';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;
p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END WH_FND_CORP_654U_FIX_LIVE;
