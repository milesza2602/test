--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_009U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_009U_OLD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE_JOB_WEEK  information for Scheduling for Staff(S4S)
--               Delete process :
--                   Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and job_start_date
--                                and last_updated_date
--
--  Tables:      Input    - RTL_EMP_JOB_DY
--               Output   - DWH_PERFORMANCE.RTL_EMP_JOB_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_rec_out            RTL_EMP_JOB_wk%rowtype;
g_found              boolean;
g_date               date;

g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
  g_NAME             VARCHAR2(40);
g_loop_fin_year_no   number        :=  0;
g_loop_fin_week_no   number        :=  0;
g_sub                integer       :=  0;
g_loop_cnt           integer       :=  30; -- Number of partitions to be truncated/replaced (revert to 30)
g_wkday              number        :=  0;  
g_end_date           date; --handles future partitions
g_loop_start_date    date;
g_subpart_type       dba_part_tables.SUBPARTITIONING_TYPE%type; 
g_subpart_column_name dba_subpart_key_columns.column_name%type;

l_message            sys_dwh_errlog.log_text%type;
l_procedure_name     sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_009U';
l_table_name         all_tables.table_name%type                := 'RTL_EMP_JOB_WK';
l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||' data EX PERFORMANCE WK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

       -- g_recs_inserted := 0;    
        g_recs := 0; 

   l_text := 'Insert into '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  INSERT /*+ APPEND  parallel (X,4) */ INTO DWH_PERFORMANCE.RTL_EMP_JOB_WK X
  WITH seldat as (
      select /*+ FULL(RTL) parallel(RTL,4) */ 
              fin_year_no, fin_week_no, max(tran_date) maxtran_date, sk1_employee_id, sk1_JOB_ID
        from dwh_performance.rtl_emp_job_dy         
         where FIN_YEAR_NO = g_loop_fin_year_no
           and FIN_WEEK_NO = g_loop_fin_week_no
       group by fin_year_no, fin_week_no, sk1_employee_id, sk1_JOB_ID)
      select /*+ FULL(JD) parallel(JD,4) */ 
         distinct         
         JD.SK1_EMPLOYEE_ID
        ,JD.SK1_JOB_ID
        ,JD.SK1_PAYPOLICY_ID
        ,JD.FIN_YEAR_NO
        ,JD.FIN_WEEK_NO
        ,JD.JOB_START_DATE
        ,JD.JOB_END_DATE
        ,JD.EMPLOYEE_RATE
        ,g_date LAST_UPDATED_DATE
      FROM dwh_performance.rtl_emp_job_dy jd,
             seldat sd
       where jd.sk1_employee_id = sd.sk1_employee_id
         and jd.sk1_JOB_ID = sd.sk1_JOB_ID
         and jd.tran_date = sd.maxtran_date               ;

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    
        l_TEXT := l_table_name||' : recs = '||g_recs ||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
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
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    dwh_performance.dwh_s4s.write_initial_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
      select distinct THIS_WEEK_END_DATE into g_end_date
    from DIM_CALENDAR
    where CALENDAR_DATE = g_date + 20;


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
  -- Remove existing data and reload one partition at a time
  --**************************************************************************************************       
    begin
       for g_sub in 1 .. g_loop_cnt                                                                                                  
         loop 
           if g_sub = 1 then
              DWH_PERFORMANCE.DWH_S4S.get_partition( l_table_name, l_table_owner, g_end_date, g_subpart_type, 
                                                    g_subpart_column_name, G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO); 
            else
              DWH_PERFORMANCE.DWH_S4S.get_previous_partition(g_subpart_type, g_subpart_column_name, G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO);      
           end if;

             -- truncate subpartition
             DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                           l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO);
             commit;  
             -- Replace with new data
             b_insert;    
             commit;   
        end loop;     
    end;  

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

END WH_PRF_S4S_009U_OLD;
