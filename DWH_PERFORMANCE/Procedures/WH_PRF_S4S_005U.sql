--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_005U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_005U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        21 Feb 2019
--  Author:      Shuaib Salie ; Lisa Kriel
-- Purpose:     Load EMPLOYEE JOB DY FACT information for Scheduling for Staff(S4S)
--                NB> job_start_date = MONDAY and job_end_date = SUNDAY
--
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_JOB
--               Output   - DWH_PERFORMANCE.RTL_EMP_JOB_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--                Date Processing :
--                ------------
--                  We should not be sent any records for an employee where the dates overlap for the job periods.
--                  The job_start_date can be any day but the job_end_date will be sent as the job_start_date of the next period 
--                   This all depends on the derivation criteria.
--                  eg. RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015'
--                      RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015' 
--                            RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Maintenance:
--  wendy lyttle 13 may 2016  - excluding sk1_employee_id = 1089294  -- due to duplicate/overlapping info from source
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_rec_out            RTL_EMP_JOB_DY%rowtype;
g_found              boolean;
g_name               varchar2(40);
g_date               date          := trunc(sysdate);
g_JOB_end_date       date;
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;


g_min_start_date      date;
--g_max_week_no        number        :=  0;
--g_min_year_no        number        :=  0;
--g_max_year_no        number        :=  0;

g_recs_deleted       integer       :=  0;

g_loop_fin_year_no   number        :=  0;
g_loop_fin_week_no   number        :=  0;
g_sub                integer       :=  0;
g_loop_cnt           integer       :=  36; -- Number of partitions to be truncated/replaced include future weeks
g_wkday              number        :=  0;  
g_loop_start_date    date;
g_subpart_type       dba_part_tables.SUBPARTITIONING_TYPE%type; 
g_subpart_column_name dba_subpart_key_columns.column_name%type;


l_message            sys_dwh_errlog.log_text%type;
l_procedure_name     sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_005U';
l_table_name         all_tables.table_name%type                := 'RTL_EMP_JOB_DY';
l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD the '||l_table_name||' data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

  l_text := 'Insert into '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

          insert /*+ append parallel (X,6) */ into dwh_performance.RTL_EMP_JOB_DY X
            WITH
            SELEXT1 as (
                       SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,8) , PARALLEL(EL,8) */
                            --DISTINCT  
                              DC.CALENDAR_DATE TRAN_DATE
                              ,DC.THIS_WEEK_START_DATE 
                              ,FLR.JOB_START_DATE
                              ,FLR.JOB_END_DATE
                              ,dc.fin_year_no
                              ,dc.fin_week_no
                              ,FLR.EMPLOYEE_RATE
                              ,DE.SK1_EMPLOYEE_ID
                              ,DP.SK1_PAYPOLICY_ID
                              ,DJ.SK1_JOB_ID
                              ,EL.EMPLOYEE_STATUS
                              ,el.effective_START_DATE
                              ,el.effective_end_DATE
                         FROM FND_S4S_EMP_JOB flr
                        JOIN DWH_HR_PERFORMANCE.dim_employee DE
                          ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
                        JOIN DIM_JOB DJ 
                          ON DJ.JOB_ID = FLR.JOB_ID
                         and flr.job_start_date between dj.sk1_effective_from_date and dj.sk1_effective_to_date
                        JOIN DIM_CALENDAR DC                 
                          -- ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
                          ON   DC.CALENDAR_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
                        JOIN RTL_EMP_LOC_STATUS_DY EL
                          ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                         AND EL.TRAN_DATE = DC.CALENDAR_DATE
                        JOIN DIM_PAY_POLICY DP
                          ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
--                      where DC.FIN_YEAR_NO = g_loop_fin_year_no
--                        and DC.FIN_WEEK_NO = g_loop_fin_week_no
                         where DC.calendar_date between  g_min_start_date and g_JOB_end_date
            ),
            selext2 as (
                       SELECT 
                            --DISTINCT 
                              TRAN_DATE
                              ,fin_year_no
                              ,fin_week_no
                              ,JOB_START_DATE
                              ,JOB_END_DATE
                              ,EMPLOYEE_RATE
                              ,SK1_EMPLOYEE_ID
                              ,SK1_PAYPOLICY_ID
                              ,SK1_JOB_ID
                              ,EMPLOYEE_STATUS
                              ,(
                                CASE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('S')           THEN SE1.effective_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NULL      THEN SE1.job_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NOT NULL  THEN SE1.job_START_DATE
                                  ELSE NULL
                                END) derive_start_date ,
                               (
                                CASE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('S')           THEN SE1.effective_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NULL      THEN NVL(SE1.job_END_DATE, g_job_end_date)
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NOT NULL  THEN se1.effective_end_date
                                  ELSE NULL
                                END) derive_end_date
                         FROM selext1 SE1
                        WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
               )  
               -- ("SK1_JOB_ID", "SK1_EMPLOYEE_ID", "FIN_YEAR_NO", "FIN_WEEK_NO", "TRAN_DATE") 
                        select 
                            -- distinct
                               SK1_EMPLOYEE_ID                           
                               ,SK1_JOB_ID  
                               ,SK1_PAYPOLICY_ID
                               ,fin_year_no
                               ,fin_week_no
                               ,TRAN_DATE
                               ,JOB_START_DATE
                               ,JOB_END_DATE
                               ,EMPLOYEE_RATE                               
                               ,g_date LAST_UPDATED_DATE
                          from SELEXT2 SE2
                         where SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date    
            ;

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    
        l_TEXT := l_table_name||' : recs = '||g_recs ;
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
  IF p_forall_limit IS NOT NULL and p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;

  p_success := false;
  dwh_performance.dwh_s4s.write_initial_log_data(l_procedure_name,l_description);

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  -- derivation of end_date for recs where null.
    select distinct THIS_WEEK_END_DATE into g_JOB_end_date
    from DIM_CALENDAR
    where CALENDAR_DATE = g_date + 42;

  l_text             := 'Derived g_job_end_date - '||g_job_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
        execute immediate 'alter session enable parallel dml';
       execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

  --**************************************************************************************************
  -- Disabling of FK constraints
  --**************************************************************************************************
            DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);

  --*************************************************************************************************
  -- Truncate existing data one partition at a time
  --**************************************************************************************************  

    begin
      -- for g_sub in 1 .. g_loop_cnt
       for g_sub in 0 .. g_loop_cnt-1
         loop 
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_min_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (g_JOB_end_date) - (g_sub * 7);  

             -- truncate subpartition
             DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_procedure_name, l_table_name, l_table_owner,
                                                                  g_loop_fin_year_no, g_loop_fin_week_no);

        end loop;     
    end;  
 --*************************************************************************************************
  -- Reload data with new data (foundation table not partitioned)
  --**************************************************************************************************  
--     dbms_output.put_line ('g_min_start_date :'||g_min_start_date);
--     dbms_output.put_line ('g_JOB_end_date :'||g_JOB_end_date);
--     dbms_output.put_line ('before insert :');
     b_insert;  
--     dbms_output.put_line ('AFTER insert :');
 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
  DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_procedure_name,l_description,g_recs_read,g_recs_inserted,g_recs_updated);
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

END WH_PRF_S4S_005U;
