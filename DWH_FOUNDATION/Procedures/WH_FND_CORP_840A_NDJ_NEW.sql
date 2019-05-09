--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_840A_NDJ_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_840A_NDJ_NEW" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
    )
AS
  --**************************************************************************************************
  --  Date:        May 2015
  --  Author:      W Lyttle
  --  Purpose:     Load Non-DJ(David Jones) Allocation data to allocation tracker table FOR THE LAST 120 DAYS.
  --               NB> the last 120-days sub-partitions are truncated via a procedure before this one runs.
  --               For CHBD only.
  --               THIS PERIOD IS FROM G_DATE-120(this_week_start_date) 
  --                              TO G_DATE-91(this_week_end_date) 
  --  Tables:      Input  - fnd_rtl_allocation
  --               Output - temp_alloc_tracker_alloc_NDJ1
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
  g_date_min_120    DATE;
  g_date_min_90    DATE;
  g_date_min_91    DATE;
  g_START_WK_DATE    DATE;
  g_END_WK_DATE    DATE;
  g_TEST_DATE    DATE;
  G_COUNT_WEEKS    NUMBER := 0;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_840A_NDJ_NEW';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS Alloc Data TO ALLOC TRACKER TABLE';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
      
    
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      EXECUTE immediate 'alter session enable parallel dml';

      
      --**************************************************************************************************
      -- Look up batch date from dim_control
      --**************************************************************************************************
      Dwh_Lookup.Dim_Control(G_Date);
      
      -- TESTING
     -- G_DATE := '19 MAY 2015';
      -- TESTING
      
     
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--
--       **** determine period
--       going to do from monday or current_day-120 to sunday current_week
--
      g_date_min_120  := g_date - 120;
      g_date_min_91   := g_date - 91;

      select min(this_week_start_date), count(distinct this_week_start_date) into   g_start_wk_date, g_count_weeks
      from dim_calendar 
      where calendar_date = g_date_min_120;

      select distinct this_week_end_date into  g_end_wk_date
      from dim_calendar 
      where calendar_date = g_date_min_91;

         
--**************************************************************************************************
-- truncate temp table
--**************************************************************************************************

      l_text := 'DATA LOADED FOR PERIOD '||g_start_wk_date||' TO '||g_end_wk_date||' **no_of_weeks='||g_count_weeks;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

      l_text := 'truncate table dwh_foundation.temp_alloc_tracker_alloc_NDJ1' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'truncate table dwh_foundation.temp_alloc_tracker_alloc_NDJ1';

--**************************************************************************************************
-- build partition_truncate_sql
--**************************************************************************************************
 --     g_partition_name := 'fnd_alloc_tracker_alloc'||v_cur.fin_year_no||'_'||v_cur.fin_week_no;
 --     g_sql_trunc_partition := 'alter table dwh_foundation.fnd_alloc_tracker_alloc truncate SUBPARTITION '||g_partition_name;
     l_text := 'Running GATHER_TABLE_STATS ON TEMP_ALLOC_TRACKER_ALLOC_NDJ1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
--                                   'TEMP_ALLOC_TRACKER_ALLOC_NDJ1', DEGREE => 8);     

-- below is the new code for stats collection in 12c - Ref: 23Jan2019
    DWH_FOUNDATION.GENERIC_GATHER_TABLE_STATS(l_procedure_name,'TEMP_ALLOC_TRACKER_ALLOC_DJ1');
   
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

    INSERT /*+ APPEND */
    INTO dwh_foundation.temp_alloc_tracker_alloc_NDJ1
      SELECT
        /*+   parallel(Fa,4) parallel(Di,4)   */
              fa.release_date,
              fa.alloc_no,
              fa.to_loc_no,
              fa.item_no,
              di.primary_supplier_no supplier_no,
              ( CASE  WHEN fa.po_no IS NULL THEN 'WH' ELSE 'XD' END) supply_chain_code,
              fa.first_dc_no,
              fa.trunk_ind,
              NVL(fa.alloc_qty,0) alloc_qty,
              NVL(fa.orig_alloc_qty,0) orig_alloc_qty,
              NVL(fa.alloc_cancel_qty,0) alloc_cancel_qty,
              (CASE WHEN NVL(fa.orig_alloc_qty,0) <> NVL(fa.alloc_cancel_qty,0) THEN fa.orig_alloc_qty END) fillrate_qty,
              g_date last_updated_date,
              WH_PHYSICAL_WH_NO wh_no,
              WH_PHYSICAL_WH_NO first_whse_supplier_no,
              CHAIN_CODE
            FROM dwh_foundation.fnd_rtl_allocation fa
            JOIN dwh_performance.dim_item di
            ON fa.item_no = di.item_no
            JOIN dwh_performance.dim_location dl
            ON dl.location_no = fa.wh_no
            WHERE fa.release_date BETWEEN g_start_wk_date AND g_end_wk_date
            AND di.business_unit_no NOT IN(50,70)
            AND (fa.CHAIN_CODE  <> 'DJ' or fa.CHAIN_CODE  is null)
              ;
            g_recs_read    :=SQL%ROWCOUNT;
           g_recs_inserted:=SQL%ROWCOUNT;
            COMMIT;
       l_text := 'Running GATHER_TABLE_STATS ON TEMP_ALLOC_TRACKER_ALLOC_DJ1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
--                                   'TEMP_ALLOC_TRACKER_ALLOC_DJ1', DEGREE => 8);     

-- below is the new code for stats collection in 12c - Ref: 23Jan2019
    DWH_FOUNDATION.GENERIC_GATHER_TABLE_STATS(l_procedure_name,'TEMP_ALLOC_TRACKER_ALLOC_DJ1');
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
END WH_FND_CORP_840A_NDJ_NEW;