--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_654U_FIXA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_654U_FIXA" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- DATA FIX - reprocess all stg_arc recss
  --**************************************************************************************************
  --  Date:        April 2011
  --  Author:      Wendy Lyttle
  --  Purpose:     Truncate and reload temp table with key and max(sys_source_batch_id)
  --               with input ex staging table from RMS.
  --  Tables:      Input  - stg_rms_deal_actual_detail_arc
  --               Output - temp_stg_rms_dl_actl_dtl_ARC
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --
  --  Maintenance
  --  28 jan 2011 qc4141 Key for temp_deal_actual_detail
  --                       and temp_deal_actual_detail_RTV is not unique enough
  --  13 Apr 2011 qc414 - Add CREATE_DATE_TIME
  --
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor

  --**************************************************************************************************
  g_start_deal NUMBER := 0;
  g_end_deal   NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_654U_FIXA';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_tran;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_tran;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE temp_stg_rms_dl_actl_dtl_ARC DATA EX STG';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
BEGIN
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD THE temp_stg_rms_dl_actl_dtl_ARC DATA EX STG STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --
  -- Preparation for load
  --
  EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC');
  COMMIT;
  l_text := 'TRUNCATE COMPLETED OF TEMP_STG_RMS_DL_ACTL_DTL_ARC';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','TEMP_STG_RMS_DL_ACTL_DTL_ARC', DEGREE => 8);
  COMMIT;
  l_text := 'GATHER STATS COMPLETED';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  G_START_DEAL := 0;
  G_END_DEAL   := 12500;
  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,    deal_detail_no ,    po_no ,    shipment_no ,    tran_date ,
    item_no ,    location_no ,    MAX(sys_source_batch_id) maxsource,    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
  WHERE 
--  sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,    deal_detail_no,    po_no,    shipment_no,    tran_date,
    item_no,    location_no,    CREATE_DATETIME
;
  
l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;

G_START_DEAL := 12501;
G_END_DEAL := 18500;

  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,    deal_detail_no ,    po_no ,    shipment_no ,    tran_date ,
    item_no ,    location_no ,    MAX(sys_source_batch_id) maxsource,    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
 where  
 --sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,    deal_detail_no,    po_no,    shipment_no,    tran_date,
    item_no,    location_no,    CREATE_DATETIME
;
  
l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT; 

G_START_DEAL := 18501;
G_END_DEAL := 24000;

  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,
    deal_detail_no ,
    po_no ,
    shipment_no ,
    tran_date ,
    item_no ,
    location_no ,
    MAX(sys_source_batch_id) maxsource,
    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
 where  
 --sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,
    deal_detail_no,
    po_no,
    shipment_no,
    tran_date,
    item_no,
    location_no,
    CREATE_DATETIME
;
  
l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;

G_START_DEAL := 24001;
G_END_DEAL := 32000;
  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,
    deal_detail_no ,
    po_no ,
    shipment_no ,
    tran_date ,
    item_no ,
    location_no ,
    MAX(sys_source_batch_id) maxsource,
    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
 where  
 --sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,
    deal_detail_no,
    po_no,
    shipment_no,
    tran_date,
    item_no,
    location_no,
    CREATE_DATETIME
;
  
l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;

G_START_DEAL := 32001;
G_END_DEAL := 40000;

  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,
    deal_detail_no ,
    po_no ,
    shipment_no ,
    tran_date ,
    item_no ,
    location_no ,
    MAX(sys_source_batch_id) maxsource,
    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
 where  
 --sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,
    deal_detail_no,
    po_no,
    shipment_no,
    tran_date,
    item_no,
    location_no,
    CREATE_DATETIME
;
  

l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;

G_START_DEAL := 40001;
G_END_DEAL := 60000;

  INSERT
    /*+ append */
  INTO dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  
SELECT
    /*+ PARALLEL(lid 2) */
    deal_no ,
    deal_detail_no ,
    po_no ,
    shipment_no ,
    tran_date ,
    item_no ,
    location_no ,
    MAX(sys_source_batch_id) maxsource,
    CREATE_DATETIME
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_ARC lid
 where 
 --sys_load_date >= '17 september 2012' and 
  deal_no BETWEEN G_START_DEAL AND G_END_DEAL
  GROUP BY deal_no,
    deal_detail_no,
    po_no,
    shipment_no,
    tran_date,
    item_no,
    location_no,
    CREATE_DATETIME
;
  
l_text := 'INSERT COMPLETED for deals from '||G_START_DEAL||' to '||G_END_DEAL||' for recs='||sql%rowcount;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;


DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','TEMP_STG_RMS_DL_ACTL_DTL_ARC', DEGREE => 8);
COMMIT;
l_text := 'GATHER STATS COMPLETED';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
END WH_FND_CORP_654U_FIXA;
