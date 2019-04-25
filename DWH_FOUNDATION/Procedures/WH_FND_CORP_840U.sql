--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_840U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_840U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
    ,
   p_from_loc_no IN INTEGER,
   p_to_loc_no   IN INTEGER
    )
AS
  --**************************************************************************************************
  --  Date:        February 2010
  --  Author:      M Munnik
  --  Purpose:     Load Allocation data to allocation tracker table FOR THE LAST 90 DAYS.
  --               NB> the last 90-days sub-partitions are truncated via a procedure before this one runs.
  --               For CHBD only.
  --  Tables:      Input  - fnd_rtl_allocation
  --               Output - fnd_alloc_tracker_alloc
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  7 may 2010 - wendy - add last insert
  --  14 MAY 2010 - WENDY - add first_whse_supplier_no to output
  --  28 may 2010 - wendy - ensure that extract query is same in live and full_live
  --  28 may 2010 - wendy - add cancelled_qty and remove cancel_ind
  --  23 July 2010 - wendy - add p_period_ind and hint to cursor
  --  13 August 2012 - wendy - optimization
  --                         - change to INSERT APPEND
  --                         - change current cursor to use all FULL TABLE SCANS
  --  Maintenance:
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
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
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_840U_'||p_from_loc_no;
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS Alloc Data TO ALLOC TRACKER TABLE';
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
  --**********just for testing  START *****************
  --P_From_Loc_No := 0;
--  G_Start_Date := G_Date - 90;
  --execute immediate ('truncate table dwh_foundation.test_alloc_tracker_alloc');
  --COMMIT;
  --    l_text := 'truncate dwh_foundation.test_alloc_tracker_alloc';
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**********just for testing  END ***************
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF fnd_alloc_tracker_alloc EX FOUNDATION STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  Dwh_Lookup.Dim_Control(G_Date);
  -- test
 -- G_Date := '30-apr-2015';
  -- end test
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date||' '||p_from_loc_no||' '||p_to_loc_no;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  -- TRIED THE FOLLOWING 
  -- Changing this to see if can improve performance.
  -- 3 spinner jobs will initiate but 2 of them will fun with no records.
  -- This is so that we can test to run one job only without
  -- having to change the schedule.
  --- BUT WAS NOT SUCCESSFULL - REVERTING TO NORMAL SPINNER
 -- IF p_from_loc_no = 0 THEN
 --   g_start_date  := g_date - 90;
/*          IF p_from_loc_no = 0 THEN
           g_start_date := g_date - 30;
         ELSE
           IF p_to_loc_no = 99999 THEN
             g_start_date := g_date - 90;
             g_date       := g_date - 61;
           ELSE
             IF p_from_loc_no > 0 THEN
               g_start_date := g_date - 60;
               g_date       := g_date - 31;
             ELSE
               g_start_date := g_date - 90;
             END IF;
           END IF;
         END IF;
 */   
        IF p_from_loc_no = 0 THEN
           g_start_date := g_date - 90;
         ELSE
           IF p_to_loc_no = 99999 THEN
             g_start_date := null;
             g_date       := null;
           ELSE
             IF p_from_loc_no > 0 THEN
               g_start_date := null;
               g_date       := null;
             ELSE
               g_start_date := null;
             END IF;
           END IF;
         END IF;     
    if     g_start_date is not null
    and   g_date     is not null
    then 
    l_text := 'DATA LOADED FOR PERIOD '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     --**************************************************************************************************
    -- Bulk fetch loop controlling main program execution
    --**************************************************************************************************
        execute immediate 'alter session set workarea_size_policy=manual';
        execute immediate 'alter session set sort_area_size=100000000';
    EXECUTE immediate 'alter session enable parallel dml';
    INSERT /*+ APPEND */
    INTO dwh_foundation.fnd_alloc_tracker_alloc
   -- SELECT 
    --COUNT(DISTINCT RELEASE_DATE), COUNT(DISTINCT ITEM_NO), COUNT(DISTINCT ALLOC_NO), SUM(NVL(fillrate_qty,0)) ,
    --SUM(NVL(orig_alloc_qty,0)),
 --   COUNT(*)
  --  INTO 
    --G_CNT_DATE, G_CNT_ITEM, G_CNT_ALLOC, G_FILLRATE_QTY, G_ORIG_ALLOC_QTY, 
  --  G_CNT_RECS
  --  FROM (
    SELECT
    -- trying no hint
  /*+ full(a) full(i)  parallel(a,4) parallel(i,4)  */
  --
      a.release_date,
      a.alloc_no,
      a.to_loc_no,
      a.item_no,
      i.primary_supplier_no supplier_no,
      (
      CASE
        WHEN a.po_no IS NULL
        THEN 'WH'
        ELSE 'XD'
      END) supply_chain_code,
      to_number(SUBSTR(a.wh_no,1,3)) first_dc_no,
      a.trunk_ind,
      NVL(a.alloc_qty,0) alloc_qty,
      NVL(a.orig_alloc_qty,0) orig_alloc_qty,
      NVL(a.alloc_cancel_qty,0) alloc_cancel_qty,
      (
      CASE
        WHEN NVL(a.orig_alloc_qty,0) <> NVL(a.alloc_cancel_qty,0)
        THEN a.orig_alloc_qty
      END) fillrate_qty,
      g_date last_updated_date,
      to_number(SUBSTR(a.wh_no,1,3)) wh_no,
      to_number(SUBSTR(a.wh_no,1,3)) first_whse_supplier_no
    FROM dwh_foundation.fnd_rtl_allocation a
    JOIN dwh_performance.dim_item i
    ON a.item_no = i.item_no
    WHERE a.release_date BETWEEN g_start_date AND g_date
    AND i.business_unit_no NOT IN(50,70)
    AND (CHAIN_CODE  <> 'DJ'
    or chain_code is null)
   --   )
      ;
    g_recs_read    :=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT;
    COMMIT;
  END IF;
 --     l_text := 'records selected='||g_cnt_recs;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   g_recs_read    :=g_cnt_recs;
 -- g_recs_inserted:=g_cnt_recs;
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
END WH_FND_CORP_840U;
