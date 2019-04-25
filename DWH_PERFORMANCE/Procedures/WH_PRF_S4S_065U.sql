--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_065U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_065U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     Load schedule employee exception information FACT information for SCHEDuling for Staff(S4S)
  --
  --  Tables:      Input    - dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY
  --               Output   - DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY
  --  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
  --
  --  Maintenance:
  --  Date:        07 Feb 2019
  --  Author:      Shuaib Salie ; Lisa Kriel
  --  What:        S4S New way forward
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit      INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read         INTEGER := 0;
  g_recs_inserted     INTEGER := 0;
  g_recs_updated      INTEGER := 0;
  g_NAME              VARCHAR2(40);
  g_date              DATE;
  g_recs              NUMBER     := 0;

  g_loop_fin_year_no   number        :=  0;
  g_loop_cnt           integer       :=  2; --  Partitions in years  

  l_message            sys_dwh_errlog.log_text%type;
  l_procedure_name     sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_065U';
  l_table_name         all_tables.table_name%type                := 'RTL_SCHED_XCPTN_EMP_DY';
  l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
  l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
  l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
  l_text               sys_dwh_log.log_text%type ;
  l_description        sys_dwh_log_summary.log_description%type  := 'LOAD '||l_table_name||' data  EX FOUNDATION';
  l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN
        g_recs_inserted := 0;    
        g_recs := 0; 

   l_text := 'Insert into  '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    INSERT /*+ append */ INTO DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY
     SELECT  
                /*+ full(flr) parallel(flr,8) */              
                    he.SK1_EMPLOYEE_ID
                   ,dj.SK1_JOB_ID
                   , dl.SK1_LOCATION_NO
                   , ex.SK1_EXCEPTION_TYPE_ID
                   , dc.FIN_YEAR_NO
                   , flr.EXCEPTION_DATE
                   , flr.EXCEPTION_START_TIME
                   , flr.EXCEPTION_END_TIME
                   , G_DATE LAST_UPDATED_DATE
            FROM dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY flr
            join dwh_performance.DIM_LOCATION DL
               on    dl.LOCATION_NO = flr.LOCATION_NO
            join dwh_hr_performance.DIM_employee he
               on    he.employee_ID = flr.employee_ID
            join dwh_performance.DIM_exception_TYPE ex
               on    ex.EXCEPTION_TYPE_ID = flr.EXCEPTION_TYPE_ID     
            join dwh_performance.dim_job dj
               on    dj.JOB_ID = flr.JOB_ID   
                  and flr.exception_date between dj.sk1_effective_from_date and dj.sk1_effective_to_date         
            join dwh_performance.dim_calendar dc
              on dc.calendar_date = flr.EXCEPTION_DATE   
        where  dc.fin_year_no = g_loop_fin_year_no;   

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    

        L_TEXT := L_table_name||' : recs = '||g_recs ||' for Fin '||g_loop_fin_year_no;
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
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'LOAD OF  '||l_table_name||'   EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select today_fin_year_no into g_loop_fin_year_no
   from dwh_performance.dim_control;

  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
   execute immediate 'alter session enable parallel dml';
   execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

  --**************************************************************************************************
  -- Disabling of FK constraints
  --**************************************************************************************************
  dwh_performance.dwh_S4S.enable_foreign_keys (l_table_name, l_table_owner);

  --*************************************************************************************************
  -- Remove existing data and reload one partition at a time
  --**************************************************************************************************       
   begin
       for g_sub in 1 .. g_loop_cnt                                                                                                  
         loop                

            -- Truncate Table Partition 
            DWH_PERFORMANCE.DWH_S4S.remove_partition_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                   l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO);
             commit;  
             -- Replace with new data
             b_insert;    
             commit;
             g_loop_fin_year_no :=g_loop_fin_year_no-1;
         end loop;
    end;  

 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
  dwh_performance.dwh_S4S.enable_foreign_keys (l_table_name, L_table_owner, true);

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

  END WH_PRF_S4S_065U;
