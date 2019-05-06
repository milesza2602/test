--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_032U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_032U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table
--**************************************************************************************************
--  Date:        22 Feb 2019
--  Author:      Shuaib Salie ; Lisa Kriel
--  Purpose:     Load EMPLOYEE_CONSTRAINTS DAY information for Scheduling for Staff(S4S)
--                NB> constraint_start_date = MONDAY and constraint_end_date = any day of week
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_CONSTR_WK
--               Output   - RTL_EMP_CONSTR_LOC_JOB_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--                Date Processing :
--                ------------
--                  We should not be sent any records for an employee where the dates overlap for the constraint periods.
--                  The constraint_start_date can be any day but the constraint_end_date will be sent as the constraint_start_dtae of the next period 
--                     but we will subtract 1 day from it to derive the constraint_end_date of the previous period.
--                  This all depends on the derivation criteria. 
--                  eg. RECORD 1 : constraint_start_date = '1 jan 2015'  constraint_end_date = '12 january 2015'
--                      RECORD 2 : constraint_start_date = '12 jan 2015'  constraint_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : constraint_start_date = '1 jan 2015'  constraint_end_date = '11 january 2015' **** note changed end_date
--                            RECORD 2 : constraint_start_date = '12 jan 2015'  constraint_end_date = NULL
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
g_forall_limit          integer       :=  dwh_constants.vc_forall_limit;
g_recs_read             integer       :=  0;
g_recs_inserted         integer       :=  0;
g_recs_updated          integer       :=  0;
g_rec_out               RTL_EMP_CONSTR_LOC_JOB_WK%rowtype;
g_date                  date;
g_constr_end_date       date;
g_batch_date            date          := trunc(sysdate);
g_run_date              date          := trunc(sysdate);
g_recs                  number        :=  0;
g_loop_fin_year_no      number        :=  0;
g_loop_fin_week_no      number        :=  0;
g_loop_start_date       date;
g_sub                   integer       :=  0;
g_loop_cnt              integer       :=  30; -- Number of partitions to be truncated/replaced (revert to 30)
g_degrees               integer       :=  4;
l_message               sys_dwh_errlog.log_text%type;
l_procedure_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_032U';
l_table_name            all_tables.table_name%type                := 'RTL_EMP_CONSTR_LOC_JOB_WK';
l_table_owner           all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name                  sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name           sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name           sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_text                  sys_dwh_log.log_text%type ;
l_description           sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||' data EX FOUNDATION';
l_process_type          sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

   l_text := 'Insert into '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

            INSERT /*+ append PARALLEL(X,g_degrees)*/
          INTO dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK X
                   WITH SELEXT1 AS
                          (SELECT
                            /*+ FULL(flr)   full(EJ) full(EL) PARALLEL(flr,g_degrees) PARALLEL(EJ,g_degrees) PARALLEL(EL,g_degrees)*/
                                  DISTINCT FLR.CONSTRAINT_start_DATE ,
                                  FLR.CONSTRAINT_END_DATE ,
                                  FLR.STRICT_MIN_HRS_PER_WK ,
                                  FLR.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                                  FLR.MIN_HRS_PER_WK ,
                                  FLR.MAX_HRS_PER_WK ,
                                  FLR.MAX_DY_PER_WK ,
                                  FLR.MAX_CONS_DAYS ,
                                  DE.SK1_EMPLOYEE_ID ,
                                  DC.FIN_YEAR_NO ,
                                  DC.FIN_WEEK_NO ,
                                  DC.this_week_start_date ,
                                  EJ.SK1_JOB_ID ,
                                  EL.SK1_LOCATION_NO ,
                                  EJ.EMPLOYEE_RATE ,
                                  EL.EMPLOYEE_STATUS ,
                                  EL.EMPLOYEE_WORKSTATUS ,
                                  el.effective_start_date ,
                                  el.effective_end_date
                          FROM FND_S4S_EMP_CONSTR_WK flr
                          JOIN DWH_HR_PERFORMANCE.dim_employee DE
                             ON FLR.EMPLOYEE_ID = DE.EMPLOYEE_ID
                          JOIN DIM_CALENDAR DC                              
                              ON DC.CALENDAR_DATE BETWEEN FLR.CONSTRAINT_START_DATE AND NVL(FLR.CONSTRAINT_END_DATE- 1, g_constr_end_date)        
                          JOIN RTL_EMP_JOB_WK EJ
                                ON EJ.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                                AND EJ.FIN_YEAR_NO    = DC.FIN_YEAR_NO
                                AND EJ.FIN_WEEK_NO    = DC.FIN_WEEK_NO
                          JOIN RTL_EMP_LOC_STATUS_WK EL
                              ON EL.SK1_EMPLOYEE_ID = EJ.SK1_EMPLOYEE_ID
                              AND EL.FIN_YEAR_NO    = EJ.FIN_YEAR_NO
                              AND EL.FIN_WEEK_NO    = EJ.FIN_WEEK_NO
                          JOIN DIM_LOCATION DL
                              ON DL.SK1_LOCATION_NO = EL.SK1_LOCATION_NO
                           WHERE DC.CALENDAR_DATE between g_loop_start_date and g_constr_end_date
                           ),
                  selext2 AS
                            (SELECT
                              /*+ FULL(RTL)   PARALLEL(SE1,g_degrees) */
                                            DISTINCT SE1.CONSTRAINT_START_DATE ,
                                            SE1.CONSTRAINT_END_DATE ,
                                            SE1.STRICT_MIN_HRS_PER_WK ,
                                            SE1.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                                            SE1.MIN_HRS_PER_WK ,
                                            SE1.MAX_HRS_PER_WK ,
                                            SE1.MAX_DY_PER_WK ,
                                            SE1.MAX_CONS_DAYS ,
                                            SE1.SK1_EMPLOYEE_ID ,
                                            SE1.FIN_YEAR_NO ,
                                            SE1.FIN_WEEK_NO ,
                                            SE1.SK1_LOCATION_NO ,
                                            SE1.SK1_JOB_ID ,
                                            SE1.EMPLOYEE_RATE ,
                                            SE1.EMPLOYEE_STATUS ,
                                            SE1.EMPLOYEE_WORKSTATUS ,
                                            SE1.THIS_WEEK_START_DATE ,
                                            (
                                            CASE
                                              WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NULL      THEN SE1.CONSTRAINT_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NOT NULL      THEN SE1.CONSTRAINT_START_DATE
                                              ELSE NULL
                                                --SE1.availability_start_DATE - 1
                                            END) derive_start_date ,
                                            (
                                            CASE
                                              WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NULL      THEN NVL(SE1.CONSTRAINT_END_DATE- 1, g_constr_end_date)
                                              WHEN SE1.EMPLOYEE_STATUS         IN ('H','I','R')      AND se1.effective_end_date IS NOT NULL      THEN se1.effective_end_date
                                              ELSE NULL
                                                --SE1.availability_END_DATE - 1
                                            END) derive_end_date
                            FROM selext1 SE1
                            WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
                            ),
                  selext3 AS
                          (SELECT fin_year_no,
                                  fin_week_no,
                                  COUNT(DISTINCT SK1_employee_id) base_head_count
                          FROM selext2
                          WHERE employee_workstatus = 'A'
                          GROUP BY FIN_YEAR_NO,
                                   FIN_WEEK_NO
                          )
                SELECT DISTINCT SE2.SK1_EMPLOYEE_ID ,SE2.SK1_JOB_ID ,SE2.SK1_LOCATION_NO ,
                              SE2.FIN_YEAR_NO ,
                              SE2.FIN_WEEK_NO ,
                              SE2.CONSTRAINT_START_DATE ,
                              SE2.CONSTRAINT_END_DATE ,
                              SE2.STRICT_MIN_HRS_PER_WK ,
                              SE2.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                              SE2.MIN_HRS_PER_WK ,
                              SE2.MAX_HRS_PER_WK ,
                              SE2.MAX_DY_PER_WK ,
                              SE2.MAX_CONS_DAYS ,
                              (SE2.STRICT_MIN_HRS_PER_WK /40) BASE_FTE_WK ,
                              (SE2.EMPLOYEE_RATE * SE2.STRICT_MIN_HRS_PER_WK) BASE_COST_WK ,             
                              SE3.BASE_HEAD_COUNT, 
                              g_date
                  FROM SELEXT2 SE2 ,
                       SELEXT3 SE3 
                 where SE2.THIS_WEEK_START_DATE between DERIVE_START_DATE and DERIVE_END_DATE
                   AND derive_start_date  IS NOT NULL
                   AND SE2.FIN_YEAR_NO     = SE3.FIN_YEAR_NO(+)
                   and SE2.FIN_WEEK_NO     = SE3.FIN_WEEK_NO(+)                   
                 ORDER BY SE2.SK1_EMPLOYEE_ID
                         ,SE2.SK1_JOB_ID
                         ,SE2.SK1_LOCATION_NO
                         ,SE2.FIN_YEAR_NO
                         ,SE2.FIN_WEEK_NO;

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    

        --L_TEXT := L_table_name||' : recs = '||g_recs ||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
        L_TEXT := L_table_name||' : recs = '||g_recs ; --||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

  --**************************************************************************************************
  --                    M  a  i  n    p  r  o  c  e  s  s  
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;

  p_success := false;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  -- derivation of end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
  
    -- 6 weeks into the future
    SELECT distinct THIS_WEEK_END_DATE into G_CONSTR_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = g_date + 41;

  l_text             := 'Derived G_CONSTR_end_date - '||G_CONSTR_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session enable parallel dml';
  execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

  --**************************************************************************************************
  -- Disabling of FK constraints
  --**************************************************************************************************
  DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);

  --*************************************************************************************************
  -- Truncate existing data one partition at a time
  --**************************************************************************************************       
    begin

       for g_sub in 0 .. g_loop_cnt+7
         loop 
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (g_constr_end_date) - (g_sub * 7);  

           -- truncate subpartition
           DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO);
       end loop;   
       
    end;   
   --*************************************************************************************************
  -- Reload data with new data (foundation table not partitioned)
  --**************************************************************************************************  
    
      b_insert;  
 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                           l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_S4S_032U;
