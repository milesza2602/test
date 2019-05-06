--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_844D_DJ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_844D_DJ" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
    )
AS
  --**************************************************************************************************
  --  Date:        May 2015
  --  Author:      W Lyttle
  --  Purpose:     Load DJ(David Jones) Allocation data to 'allocation tracker receipts in store table'
  --               for all supply_chain_code.
--                 NB. the last 120-days sub-partitions are truncated via a procedure before this one runs.
--                 For CHBD only.
  --               THIS PERIOD IS FROM G_DATE-30(this_week_start_date) 
  --                              TO G_DATE(this_week_end_date) 
  --  Tables:      Input  - fnd_rtl_allocation
  --               Output - TEMP_A_TRACK_ACTL_RCPT_DJ4
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
  g_date_min_60    DATE;
    g_date_min_61    DATE;
  g_date_min_30    DATE;
    g_date_min_31    DATE;
  g_START_WK_DATE    DATE;
  g_END_WK_DATE    DATE;
  g_TEST_DATE    DATE;
  G_COUNT_WEEKS    NUMBER := 0;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_844D_DJ';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS Alloc Data TO ALLOC TRACKER receipts in store TABLE';
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
      --G_DATE := '19 MAY 2015';
      -- TESTING
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--
--       **** determine period
--       going to do from monday or current_day-120 to sunday current_week
--
      g_date_min_30  := g_date - 30;
      g_date_min_31  := g_date - 31;

      select min(this_week_start_date), count(distinct this_week_start_date) into   g_start_wk_date,  g_count_weeks
      from dim_calendar 
      where calendar_date = g_date_min_30;

      select min(this_week_start_date) into  g_test_date
      from dim_calendar 
      where calendar_date = g_date_min_31;

      select distinct this_week_end_date into  g_end_wk_date
      from dim_calendar 
      where calendar_date = g_date;

     -- need to check that week does not form part of period for any other periods loading 
      if g_start_wk_date = g_test_date
         then g_start_wk_date := g_start_wk_date + 7;
              g_count_weeks := g_count_weeks - 1;
      end if;
--**************************************************************************************************
-- truncate temp table
--**************************************************************************************************

      l_text := 'DATA LOADED FOR PERIOD '||g_start_wk_date||' TO '||g_end_wk_date||' **no_of_weeks='||g_count_weeks;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

      l_text := 'truncate table dwh_foundation.TEMP_A_TRACK_ACTL_RCPT_DJ4' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'truncate table dwh_foundation.TEMP_A_TRACK_ACTL_RCPT_DJ4';

--**************************************************************************************************
-- build partition_truncate_sql
--**************************************************************************************************
 --     g_partition_name := 'fnd_alloc_tracker_alloc'||v_cur.fin_year_no||'_'||v_cur.fin_week_no;
 --     g_sql_trunc_partition := 'alter table dwh_foundation.fnd_alloc_tracker_alloc truncate SUBPARTITION '||g_partition_name;
          L_TEXT := 'Running GATHER_TABLE_STATS ON TEMP_A_TRACK_ACTL_RCPT_DJ4';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION',
                                   'TEMP_A_TRACK_ACTL_RCPT_DJ4', DEGREE => 8);     
   
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

    INSERT /*+ APPEND */
    INTO dwh_foundation.TEMP_A_TRACK_ACTL_RCPT_DJ4
    with  selcal AS (   
                         select CALENDAR_DATE, 1 CNTDAY
                         from   dim_calendar
                         where  calendar_date betweeN g_start_wk_date AND g_end_wk_date
                         and    fin_day_no not in(6,7)
                         and    rsa_public_holiday_ind = 0),
           SELSHIP AS (
                       SELECT /*+ MATERIALIZE     */
                               *
                       FROM DWH_FOUNDATION.TEMP_SHIPMENTS_MIN_DJ S
                      )
            ,
          selall as (
                          select /*+   full(a) full(s) parallel(a,2)  parallel(s,2)  */
                                              a.release_date
                                              ,
                                              a.alloc_no,
                                              s.to_loc_no,
                                              s.actl_rcpt_date ACTL_GRN_DATE,
                                              S.DU_ID,
                                              a.first_dc_no,
                                              a.item_no,
                                              SUM(nvl(s.received_qty,0)) ACTL_grn_qty,
                                              SUM(CNTDAY) num_days_to_ACTL_GRN,
                                              0 num_weighted_days_to_ACTL_GRN ,
                                              g_date last_updated_date,
                                              SUM(nvl(s.cancelled_qty,0)) cancelled_qty,
                                                'DJ'
                                 FROM FND_ALLOC_TRACKER_ALLOC_DJ A
                                      JOIN SELSHIP s
                              ON a.item_no      = s.item_no
                              AND a.to_loc_no   = s.TO_loc_no
                              AND a.alloc_no    = s.DIST_no
                                  JOIN SELCAL
                                  ON CALENDAR_DATE BETWEEN A.RELEASE_DATE AND s.actl_rcpt_date
                                      where a.trunk_ind                 = 0
                                       AND s.received_qty                > 0
                                       AND a.release_date BETWEEN g_start_wk_date AND g_end_wk_date
                                  -- dwh_lookup.no_of_workdays(a.release_date, s2.actl_rcpt_date) num_days_to_ACTL_GRN,
                           group by           a.release_date,
                                              a.alloc_no,
                                              s.to_loc_no,
                                              s.actl_rcpt_date ,
                                              S.DU_ID,
                                              a.first_dc_no,
                                              a.item_no
                  union all
                              select /*+  full(a) full(s2) parallel(a,2)  parallel(s2,2)  */
                                              a.release_date,
                                              a.alloc_no,
                                              s2.to_loc_no,
                                              s2.actl_rcpt_date ACTL_GRN_DATE,
                                              S2.DU_ID,
                                              a.first_dc_no,
                                              a.item_no,
                                              SUM(nvl(s2.received_qty,0)) ACTL_grn_qty,
                                              SUM(CNTDAY) num_days_to_ACTL_GRN,
                                              0 num_weighted_days_to_ACTL_GRN ,
                                              g_date last_updated_date,
                                              SUM(nvl(s2.cancelled_qty,0)) cancelled_qty,
                                                'DJ'
                                 FROM FND_ALLOC_TRACKER_ALLOC_DJ A
                                      JOIN SELSHIP s2
                              ON a.item_no      = s2.item_no
                              AND a.to_loc_no   = s2.TO_loc_no
                              AND a.alloc_no    = s2.TSF_ALLOC_no
                                  JOIN SELCAL
                                  ON CALENDAR_DATE BETWEEN A.RELEASE_DATE AND s2.actl_rcpt_date
                             where a.trunk_ind                 = 1
                                       AND s2.received_qty                > 0
                                       AND a.release_date BETWEEN g_start_wk_date AND g_end_wk_date
                           group by           a.release_date,
                                              a.alloc_no,
                                              s2.to_loc_no,
                                              s2.actl_rcpt_date ,
                                              S2.DU_ID,
                                              a.first_dc_no,
                                              a.item_no
)
          select RELEASE_DATE
                ,ALLOC_NO
                ,TO_LOC_NO
                ,ACTL_GRN_DATE
                ,DU_ID
                ,FIRST_DC_NO
                ,ITEM_NO
                ,ACTL_grn_qty/NVL(num_days_to_ACTL_GRN,1) ACTL_grn_qty
                ,case when num_days_to_ACTL_GRN > 0 then num_days_to_ACTL_GRN -1
                  end num_days_to_ACTL_GRN
                ,CASE WHEN num_days_to_ACTL_GRN > 0  then 
                           (num_days_to_ACTL_GRN -1) * (ACTL_grn_qty/NVL(num_days_to_ACTL_GRN,1))
                      WHEN num_days_to_ACTL_GRN IS NOT NULL then 
                           num_days_to_ACTL_GRN  *  (ACTL_grn_qty/NVL(num_days_to_ACTL_GRN,1))
                      ELSE NUM_WEIGHTED_DAYS_to_ACTL_GRN
                end NUM_WEIGHTED_DAYS_to_ACTL_GRN
                 ,LAST_UPDATED_DATE
                ,CANCELLED_QTY
                , 'DJ' CHAIN_CODE
            from selall sa;
 
            g_recs_read    :=SQL%ROWCOUNT;
           g_recs_inserted:=SQL%ROWCOUNT;
            COMMIT;

          L_TEXT := 'Running GATHER_TABLE_STATS ON TEMP_A_TRACK_ACTL_RCPT_DJ4';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION',
                                   'TEMP_A_TRACK_ACTL_RCPT_DJ4', DEGREE => 8);     
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

END WH_FND_CORP_844D_DJ;
