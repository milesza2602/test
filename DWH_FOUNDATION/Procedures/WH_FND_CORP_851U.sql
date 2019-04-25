--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_851U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_851U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
       )
AS
  --**************************************************************************************************
  --  Date:        August 2014
  --  Author:      W Lyttle
  --  Purpose:     Supply Chain Reports - part 1 of mart creation
  --  Tables:      Input  - fnd_rtl_allocation
  --               Output - fnd_alloc_tracker_alloc
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
 --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_date DATE;
  g_start_date DATE;
  g_rec_out fnd_alloc_tracker_alloc%rowtype;
  G_CNT_DATE       INTEGER := 0;
  G_CNT_ITEM       INTEGER := 0;
  G_CNT_ALLOC      INTEGER := 0;
  G_FILLRATE_QTY   NUMBER;
  G_ORIG_ALLOC_QTY NUMBER;
  G_CNT_RECS       INTEGER := 0;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_851U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS supply chain report - STEP 2';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF fnd_alloc_tracker_alloc%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF fnd_alloc_tracker_alloc%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_empty_set_i tbl_array_i;
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
          IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
            g_forall_limit  := p_forall_limit;
          END IF;
          p_success := false;
          l_text    := dwh_constants.vc_log_draw_line;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'LOAD supply chain report - STEP 1 STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
          dwh_lookup.dim_control(g_date);
          l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

           execute immediate('truncate table DWH_FOUNDATION.MART_SHIPMENTS_SUPPCHN');
           commit;

          l_text := 'truncate table DWH_FOUNDATION.MART_SHIPMENTS_SUPPCHN' ;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        execute immediate 'alter session set workarea_size_policy=manual';
        execute immediate 'alter session set sort_area_size=100000000';
        EXECUTE immediate 'alter session enable parallel dml';
        INSERT /*+ APPEND */
        INTO DWH_FOUNDATION.MART_SHIPMENTS_SUPPCHN
 /* Formatted on 08/08/2014 11:16:02 AM (QP5 v5.185.11230.41888) */
  --      INSERT /*+ APPEND */
--       INTO DWH_FOUNDATION.MART_SHIPMENTS_SUPPCHN

WITH seldat
     AS (SELECT 
''''||DU_ID||'''' du_id,
 ITEM_NO,   
--SUPPLIER_NO ,
-- PO_NO ,
-- SDN_NO ,
-- ASN_ID ,
 SHIP_DATE ,
 RECEIVE_DATE ,
-- SHIPMENT_STATUS_CODE ,
-- INV_MATCH_STATUS_CODE ,
-- INV_MATCH_DATE ,
 TO_LOC_NO ,
 FROM_LOC_NO ,
-- COURIER_ID ,
-- EXT_REF_IN_ID ,
 DIST_NO ,
-- DIST_TYPE ,
-- REF_ITEM_NO ,
 TSF_ALLOC_NO ,
-- TSF_PO_LINK_NO ,
-- IBT_TYPE ,
-- SIMS_REF_ID ,
-- SHIPMENT_LINE_STATUS_CODE ,
-- RECEIVED_QTY ,
-- COST_PRICE ,
-- REG_RSP ,
-- ASN_QTY ,
-- ACTUAL_MASS ,
-- CARTON_STATUS_CODE ,
-- CARTON_STATUS_DESC ,
-- SDN_QTY ,
-- PO_NOT_BEFORE_DATE ,
-- CONTRACT_NO ,
 ACTL_RCPT_DATE ,
-- SUPP_PACK_SIZE ,
-- CANCELLED_QTY ,
 --AUTO_RECEIPT_CODE ,
-- SOURCE_DATA_STATUS_CODE ,
--LAST_UPDATED_DATE ,
 --TO_LOC_DEBT_COMM_PERC ,
 FINAL_LOC_NO ,
-- FROM_LOC_DEBT_COMM_PERC ,
 RELEASE_DATE ,
 TRUNK_IND ,
 ALLOC_NO ,
 SUPPLY_CHAIN_CODE ,
 FIRST_DC_NO ,
 SHIP_RANK DURANK ,
 LOC_TYPE ,
 DATE_RECEIVED_BY_1ST_XDOCK ,
 DATE_DESPATCHED_BY_1ST_XDOCK ,
 DATE_RECEIVED_BY_2ND_XDOCK ,
 DATE_DESPATCHED_BY_2ND_XDOCK ,
 INTO_STORE_DATE 
from DWH_FOUNDATION.TEMP_MART_SHIPMENTS_SUPPCHN
           )
  SELECT    *
            FROM   SELDAT
            PIVOT
            (
   --                         MAX( SUPPLIER_NO)  SUPPLIER_NO ,
   --                         MAX( PO_NO)  PO_NO ,
   --                         MAX( SDN_NO)  SDN_NO ,
   --                         MAX( ASN_ID)  ASN_ID ,
                            MAX( SHIP_DATE)  SHIP_DATE ,
                            MAX( RECEIVE_DATE)  RECEIVE_DATE ,
   --                         MAX( SHIPMENT_STATUS_CODE)  SHIPMENT_STATUS_CODE ,
   --                         MAX( INV_MATCH_STATUS_CODE)  INV_MATCH_STATUS_CODE ,
   --                         MAX( INV_MATCH_DATE)  INV_MATCH_DATE ,
                            MAX( TO_LOC_NO)  TO_LOC_NO ,
                            MAX( FROM_LOC_NO)  FROM_LOC_NO ,
   --                         MAX( COURIER_ID)  COURIER_ID ,
   --                         MAX( EXT_REF_IN_ID)  EXT_REF_IN_ID ,
                            MAX( DIST_NO)  DIST_NO ,
   --                         MAX( DIST_TYPE)  DIST_TYPE ,
   --                         MAX( REF_ITEM_NO)  REF_ITEM_NO ,
                            MAX( TSF_ALLOC_NO)  TSF_ALLOC_NO ,
   --                         MAX( TSF_PO_LINK_NO)  TSF_PO_LINK_NO ,
   --                         MAX( IBT_TYPE)  IBT_TYPE ,
   --                         MAX( SIMS_REF_ID)  SIMS_REF_ID ,
   --                         MAX( SHIPMENT_LINE_STATUS_CODE)  SHIPMENT_LINE_STATUS_CODE ,
   --                         sum(nvl( RECEIVED_QTY,0)) RECEIVED_QTY ,
   --                         MAX( COST_PRICE)  COST_PRICE ,
   --                         MAX( REG_RSP)  REG_RSP ,
   --                         sum(nvl( ASN_QTY,0)) ASN_QTY ,
   --                         MAX( ACTUAL_MASS)  ACTUAL_MASS ,
   --                         MAX( CARTON_STATUS_CODE)  CARTON_STATUS_CODE ,
   --                         MAX( CARTON_STATUS_DESC)  CARTON_STATUS_DESC ,
   --                         sum(nvl( SDN_QTY,0)) SDN_QTY ,
   --                         MAX( PO_NOT_BEFORE_DATE)  PO_NOT_BEFORE_DATE ,
   --                         MAX( CONTRACT_NO)  CONTRACT_NO ,
                            MAX( ACTL_RCPT_DATE)  ACTL_RCPT_DATE ,
   --                         MAX( SUPP_PACK_SIZE)  SUPP_PACK_SIZE ,
   --                         sum(nvl( CANCELLED_QTY,0)) CANCELLED_QTY ,
   --                         MAX( AUTO_RECEIPT_CODE)  AUTO_RECEIPT_CODE ,
   --                         MAX( SOURCE_DATA_STATUS_CODE)  SOURCE_DATA_STATUS_CODE ,
   --                         MAX( LAST_UPDATED_DATE)  LAST_UPDATED_DATE ,
   --                         MAX( TO_LOC_DEBT_COMM_PERC)  TO_LOC_DEBT_COMM_PERC ,
                            MAX( FINAL_LOC_NO)  FINAL_LOC_NO ,
-- MAX( FROM_LOC_DEBT_COMM_PERC)  FROM_LOC_DEBT_COMM_PERC ,
MAX( RELEASE_DATE)  RELEASE_DATE ,
MAX( TRUNK_IND)  TRUNK_IND ,
MAX( ALLOC_NO)  ALLOC_NO ,
MAX( SUPPLY_CHAIN_CODE)  SUPPLY_CHAIN_CODE ,
MAX( FIRST_DC_NO)  FIRST_DC_NO ,
--MAX( SHIP_RANK)  SHIP_RANK ,
MAX( LOC_TYPE)  LOC_TYPE ,
MAX( DATE_RECEIVED_BY_1ST_XDOCK)  DATE_RECEIVED_BY_1ST_XDOCK ,
MAX( DATE_DESPATCHED_BY_1ST_XDOCK)  DATE_DESPATCHED_BY_1ST_XDOCK ,
MAX( DATE_RECEIVED_BY_2ND_XDOCK)  DATE_RECEIVED_BY_2ND_XDOCK ,
MAX( DATE_DESPATCHED_BY_2ND_XDOCK)  DATE_DESPATCHED_BY_2ND_XDOCK ,
MAX( INTO_STORE_DATE)  INTO_STORE_DATE 
            FOR
               (DURANK)
            IN
               (
                 1 ,2,3,4,5
               )
)
ORDER BY  DU_ID, ITEM_NO;

--SELECT LOC_TYPE FROM DIM_LOCATION WHERE LOCATION_NO = 207
           
        g_recs_read    :=SQL%ROWCOUNT;
        g_recs_inserted:=SQL%ROWCOUNT;
        COMMIT;

         l_text := 'records inserted='||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
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

END wh_fnd_corp_851u;
