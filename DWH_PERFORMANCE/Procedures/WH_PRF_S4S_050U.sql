--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_050U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_050U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        20 Feb 2019
--  Author:      Shuaib Salie ; Lisa Kriel
--  Purpose:     Load Employee Schedule information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY
--               Output   - DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY  
--  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
--
--  Maintenance: Changed list of partitions to be dynamic based on last_updated rows on the Foundation table.
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
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.RTL_SCH_LOC_EMP_JB_DY%rowtype;
g_found              boolean;
g_date               date;
g_this_week_start_date date;
g_fin_days           number;
g_last_shift_date    date;
g_constr_end_date    date;
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
g_name               varchar2(40);
g_degrees           integer        :=  4;
g_loop_start_date    date;
ld_first_shift_date  date;
ld_last_shift_date   date;
l_message            sys_dwh_errlog.log_text%type;
l_procedure_name     sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_050U';
l_table_name         all_tables.table_name%type                := 'RTL_SCH_LOC_EMP_JB_DY';
l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||' data EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.RTL_SCH_LOC_EMP_JB_DY%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.RTL_SCH_LOC_EMP_JB_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

  l_text := 'Insert into '||l_table_name ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     insert /*+ append parallel(X,g_degrees)*/ into dwh_performance.RTL_SCH_LOC_EMP_JB_DY X
     SELECT /*+ full(flr) parallel(flr,g_degrees) full(he) parallel(he,g_degrees)*/              
                SK1_LOCATION_NO
               ,sk1_employee_ID
               ,SK1_JOB_ID
               ,dc.FIN_YEAR_NO
               ,dc.FIN_WEEK_NO               
               ,FLR.shift_clock_in
               ,FLR.shift_clock_out
               ,FLR.meal_break_minutes
               ,FLR.tea_break_minutes
               ,(((FLR.shift_clock_out - FLR.shift_clock_in) * 24 * 60) - meal_break_minutes) / 60 nett_scheduled_hours
               ,g_date
           FROM dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY flr,
                dwh_performance.DIM_LOCATION DL,
                dwh_HR_performance.dim_employee he,
                dwh_performance.dim_job DE,
                dwh_performance.dim_calendar dc
          WHERE FLR.LOCATION_NO = DL.LOCATION_NO
            AND FLR.JOB_ID = DE.JOB_ID
            AND FLR.EMPLOYEE_ID = HE.EMPLOYEE_ID 
            and flr.shift_clock_in between de.sk1_effective_from_date and de.sk1_effective_to_date
            and trunc(flr.shift_clock_in) = trunc(dc.calendar_date)
           and DC.calendar_date between ld_first_shift_date and ld_last_shift_date ;
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_read := g_recs_read + g_recs;
        g_recs_inserted := g_recs_inserted + g_recs;    
        l_TEXT := l_table_name||' : recs = '||g_recs ;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

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
  dwh_performance.dwh_s4s.write_initial_log_data(l_procedure_name,l_description);

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

   select min(trunc(flr.shift_clock_in)), max(trunc(flr.shift_clock_out)) 
     into ld_first_shift_date,ld_last_shift_date
   from dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY flr
   where trunc(flr.last_updated_date) >= g_date -1; --should possibly base this on previous run of current program??

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
  -- Truncate existing data for all partitions for date range
  --**************************************************************************************************       
   dwh_performance.dwh_s4s.trunc_range_subpartitions(l_procedure_name,l_table_name,l_table_owner,ld_first_shift_date,ld_last_shift_date);
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

END WH_PRF_S4S_050U;
