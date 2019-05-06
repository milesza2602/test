--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_008U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_008U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --*********************************************************************************************
  --  Date:        08 March 2019
  --  Author:      Shuaib Salie and Lisa Kriel
  --  Purpose:     Load EMPLOYEE_LOCATION_WEEK information for Scheduling for Staff(S4S)
  --
  --  Truncate Partition  process : Due to changes which can be made, we have to truncate the current
  --                                patition's data and load the new data
  --                                based upon employee_id and tran_date
  --                                and last_updated_date
  --
  --  Tables:      Input    - RTL_EMP_AVAIL_LOC_JOB_DY
  --               Output   - DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK
  --  Packages:    dwh_constants, dwh_log, dwh_valid
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
  --*********************************************************************************************

  g_forall_limit        INTEGER                              := dwh_constants.vc_forall_limit;
  g_recs_read           INTEGER                              := 0;
  g_recs_inserted       INTEGER                              := 0;
  g_recs_updated        INTEGER                              := 0;
  g_recs_tbc            INTEGER                              := 0;
  g_error_count         NUMBER                               := 0;
  g_error_index         NUMBER                               := 0;
  g_count               NUMBER                               := 0;
  g_rec_out             RTL_EMP_AVAIL_LOC_JOB_WK%rowtype;
  g_found               BOOLEAN;
  g_date                DATE;
  g_run_date            DATE                                    := TRUNC(sysdate);
  g_run_seq_no          NUMBER                                  := 0;
  g_recs                NUMBER                                  := 0;
  g_recs_deleted        INTEGER                                 := 0;
  g_loop_fin_year_no    pls_integer                             := 0;
  g_loop_fin_month_no   pls_integer                             := 0;
  g_loop_start_date     date;
  g_loop_end_date       date;
  g_sub                 pls_integer                             :=  0;
  g_loop_cnt            pls_integer        :=  6; -- Number of partitions (months) to be truncated/replaced (revert to 6)

  l_message             sys_dwh_errlog.log_text%type;
  l_procedure_name      sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_008U';
  l_table_name          all_tables.table_name%type                := 'RTL_EMP_AVAIL_LOC_JOB_WK';
  l_table_owner         all_tables.owner%type                     := 'DWH_PERFORMANCE';
  l_degrees             pls_integer                               := 4;
  l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
  l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
  l_text                sys_dwh_log.log_text%type ;
  l_description         sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||' data  EX PERFORMANCE DY';
  l_process_type        sys_dwh_log_summary.log_process_type%type :=  dwh_constants.vc_log_process_type_n;

     -- For output arrays into bulk load forall statements --
  TYPE year_sub_rec is RECORD (
        fin_year_no     number,
        fin_sub_no      number,
        start_date      date,
        end_date        date 
  );
   TYPE DateCurTyp IS REF CURSOR;
   date_cv             DateCurTyp; 

   TYPE tbl_loop_list IS TABLE OF year_sub_rec INDEX BY BINARY_INTEGER;   
   date_list           tbl_loop_list;

  --**************************************************************************************************
  -- Insert records from Performance
  --**************************************************************************************************
procedure b_insert as
BEGIN

  l_text := 'Insert into '||l_table_owner||'.'||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

INSERT /*+ APPEND  PARALLEL(X,l_degrees)*/ INTO DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK X 
WITH
  SELEXT AS
  (
    SELECT   
       /*+ PARALLEL(RTL,l_degrees) */
      RTL.SK1_LOCATION_NO,
      RTL.SK1_EMPLOYEE_ID,
      RTL.SK1_JOB_ID,
      RTL.FIN_YEAR_NO,
      RTL.FIN_MONTH_NO,
      dc.FIN_WEEK_NO,
      RTL.cycle_start_date,
      RTL.cycle_end_date,
      RTL.week_number,
      RTL.FIXED_ROSTER_hrs
     FROM
      dwh_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY rtl
    inner join dim_calendar dc ON DC.CALENDAR_DATE = RTL.TRAN_DATE  
    WHERE  RTL.fin_year_no = g_loop_fin_year_no and rtl.fin_month_no = g_loop_fin_month_no
  ),

  SELEXT1 AS
  (Select  
      SK1_LOCATION_NO,
      SK1_EMPLOYEE_ID,
      SK1_JOB_ID,
      FIN_YEAR_NO,
      FIN_MONTH_NO,
      FIN_WEEK_NO,
      cycle_start_date,
      cycle_end_date,
      week_number,
      SUM(NVL(FIXED_ROSTER_hrs,0)) FIXED_ROSTER_hrs_WK,
      SUM(NVL(FIXED_ROSTER_hrs,0)) / 40 FIXED_ROSTER_FTE_WK
  from SELEXT 
  GROUP BY SK1_LOCATION_NO,
           SK1_EMPLOYEE_ID,
           SK1_JOB_ID,
           FIN_YEAR_NO,
           FIN_MONTH_NO,
           FIN_WEEK_NO,
           cycle_start_date,
           cycle_end_date,
           week_number)
  
  select 
      a.SK1_LOCATION_NO,
      a.SK1_JOB_ID,
      a.SK1_EMPLOYEE_ID,      
      a.FIN_YEAR_NO,
      a.FIN_MONTH_NO,
      a.FIN_WEEK_NO, 
      a.week_number,
      a.cycle_start_date,
      a.cycle_end_date,     
      a.FIXED_ROSTER_hrs_WK,
      a.FIXED_ROSTER_FTE_WK,
      a.FIXED_ROSTER_hrs_WK * JD.employee_rate FIXED_ROSTER_COST_wk,
      g_date last_updated_date
  from SELEXT1 a, DWH_PERFORMANCE.RTL_EMP_JOB_WK jd
    WHERE a.SK1_EMPLOYEE_ID = JD.SK1_EMPLOYEE_ID
      AND a.SK1_JOB_ID    = jd.SK1_JOB_ID
      AND a.FIN_YEAR_NO   = JD.FIN_YEAR_NO
      AND a.FIN_WEEK_NO   = JD.FIN_WEEK_NO  ;

g_recs        :=SQL%ROWCOUNT;
COMMIT;
        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    
        l_TEXT := l_table_name||' : recs = '||g_recs ||' for Fin '||g_loop_fin_year_no||'M'||g_loop_fin_month_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end b_insert;
  --***************************************************************************  
  -- Main process
  --***************************************************************************  
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit >
    dwh_constants.vc_forall_minimum THEN
    g_forall_limit := p_forall_limit;
  END IF;
  p_success := false;
  dwh_performance.dwh_s4s.write_initial_log_data(l_procedure_name,l_description) ;
  --***************************************************************************
    -- Look up batch date from dim_control
  --***************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  --***************************************************************************  
  -- prepare environment
  --***************************************************************************
  EXECUTE immediate 'alter session enable parallel dml';
  execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

  g_recs_inserted :=0;
  g_recs_deleted  := 0;

  --**************************************************************************************************
  -- Disabling of FK constraints
  --**************************************************************************************************
  DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);

  --*************************************************************************************************
  -- Truncate existing data and reload with new data one partition at a time
  --**************************************************************************************************       
     OPEN date_cv FOR 
         select distinct fin_year_no, fin_month_no, this_mn_start_date, fin_month_end_date 
           from dim_calendar_wk
          where this_mn_start_date < g_date + 42
            and THIS_MN_END_DATE > add_months(g_date, -g_loop_cnt)
       order by fin_month_end_date desc;
       FETCH date_cv BULK COLLECT INTO date_list;
    CLOSE date_cv;
    begin
       for g_sub in 1 .. date_list.count
         loop 
           g_loop_fin_year_no := date_list(g_sub).fin_year_no; 
           g_loop_fin_month_no := date_list(g_sub).fin_sub_no;

            -- truncate subpartition
            DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                        l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_MONTH_NO);
            -- Replace with new data
            b_insert;                 
        end loop;          
    end;  

l_text := 'Running GATHER_TABLE_STATS ON '|| L_table_owner||'.'||L_table_name;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats (L_table_owner, L_table_name,DEGREE => l_degrees);
 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);

--*****************************************************************************
-- Write final log data
--*****************************************************************************

    DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_procedure_name,l_description,g_recs_read,g_recs_inserted,g_recs_updated);
    COMMIT;
     p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_procedure_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,
                              l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,
                              '','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_procedure_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,
                              l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,
                              '','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END WH_PRF_S4S_008U;
