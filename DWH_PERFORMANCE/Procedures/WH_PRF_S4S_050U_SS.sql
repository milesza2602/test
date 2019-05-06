--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_050U_SS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_050U_SS" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load Employee Schedule information for Scheduling for Staff(S4S)
--
--               Delete process :
--                 Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and trunc(shift_clock_in)
--
--                The delete lists are used in the rollups as well.
--                The delete lists were created in the STG to FND load  
--                ie. FND_S4S_SCHLOCEMPJBDY_del_list
--
--
--  Tables:      Input    - dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY
--               Output   - DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY_SS  
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
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS%rowtype;
g_found              boolean;
g_date               date;
g_this_week_start_date date;
g_fin_days           number;
g_constr_end_date    date;
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
g_name               varchar2(40);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_050U_SS';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_SCH_LOC_EMP_JB_DY_SS data EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

  --**************************************************************************************************
  -- Remove constraints and indexes
--  I30_RTL_SCH_LC_EMP_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
--  I40_RTL_SCH_LC_EMP_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN
--  PK_RTL_SCH_LC_EMP_JB_DY	UNIQUE	SK1_JOB_ID, SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 
    BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM all_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_RTL_SCH_LC_EMP_JB_DY_SS'
    AND TABLE_NAME        = 'RTL_SCH_LOC_EMP_JB_DY_SS';
    
    l_text               := 'alter table dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS drop constraint PK_RTL_SCH_LC_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS drop constraint PK_RTL_SCH_LC_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_RTL_SCH_LC_EMP_JB_DY_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
-- I40_RTL_SCH_LC_EMP_JB_DY
--I30_RTL_SCH_LC_EMP_JB_DY   
  BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I30_RTL_SCH_LC_EMP_JB_DY_SS'
    AND TABLE_NAME   = 'RTL_SCH_LOC_EMP_JB_DY_SS';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_SCH_LC_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_SCH_LC_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I30_RTL_SCH_LC_EMP_JB_DY_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

  g_name := null;
  
  BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    WHERE index_NAME = 'I40_RTL_SCH_LC_EMP_JB_DY_SS'
    AND TABLE_NAME   = 'RTL_SCH_LOC_EMP_JB_DY_SS';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_SCH_LC_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_SCH_LC_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I40_RTL_SCH_LC_EMP_JB_DY_SS does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

      g_name := null;
 

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a_remove_indexes;
  
  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

  l_text := 'Insert into RTL_SCH_LOC_EMP_JB_DY_SS ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
--insert /*+ append */ into dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS
--         SELECT /*+ full(flr) parallel(flr,6) full(he) parallel(he,6)*/              
--                SK1_LOCATION_NO
--               ,sk1_employee_ID
--               ,SK1_JOB_ID
--               ,FLR.shift_clock_in
--               ,FLR.shift_clock_out
--               ,FLR.meal_break_minutes
--               ,FLR.tea_break_minutes
--               ,(((FLR.shift_clock_out - FLR.shift_clock_in) * 24 * 60) - meal_break_minutes) / 60 nett_scheduled_hours
--               ,g_date
--           FROM dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY flr,
--                dwh_performance.DIM_LOCATION DL,
--                dwh_HR_performance.dim_employee he,
--                dwh_performance.rtl_job_ss DE
--          WHERE FLR.LOCATION_NO = DL.LOCATION_NO
--            AND FLR.JOB_ID = DE.JOB_ID
--            AND FLR.EMPLOYEE_ID = HE.EMPLOYEE_ID    
--        ;

     insert /*+ append */ into dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS
     SELECT /*+ full(flr) parallel(flr,6) full(he) parallel(he,6)*/              
                SK1_LOCATION_NO
               ,sk1_employee_ID
               ,SK1_JOB_ID
               ,FLR.shift_clock_in
               ,FLR.shift_clock_out
               ,FLR.meal_break_minutes
               ,FLR.tea_break_minutes
               ,(((FLR.shift_clock_out - FLR.shift_clock_in) * 24 * 60) - meal_break_minutes) / 60 nett_scheduled_hours
               ,g_date
           FROM dwh_foundation.FND_S4S_SCH_LOC_EMP_JB_DY flr,
                dwh_performance.DIM_LOCATION DL,
                dwh_HR_performance.dim_employee he,
                dwh_performance.rtl_job_ss DE
          WHERE FLR.LOCATION_NO = DL.LOCATION_NO
            AND FLR.JOB_ID = DE.JOB_ID
            AND FLR.EMPLOYEE_ID = HE.EMPLOYEE_ID 
            and flr.shift_clock_in between de.sk1_effective_from_date and de.sk1_effective_to_date;
            
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_inserted := g_recs;          
        L_TEXT := 'RTL_SCH_LOC_EMP_JB_DY_SS : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

--**************************************************************************************************
-- create primary key and index
--  I30_RTL_SCH_LC_EMP_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
--  I40_RTL_SCH_LC_EMP_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN
--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCH_LOC_EMP_JB_DY_SS', DEGREE => 8);

      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_SCH_LC_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_SCH_LC_EMP_JB_DY_SS ON DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY_SS (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_SCH_LC_EMP_JB_DY_SS LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_SCH_LC_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_SCH_LC_EMP_JB_DY_SS ON DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY_SS (SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_SCH_LC_EMP_JB_DY_SS LOGGING NOPARALLEL') ;

  

   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c_add_indexes;


--**************************************************************************************************
-- create primary key and index
--  PK_RTL_SCH_LC_EMP_JB_DY	UNIQUE	SK1_JOB_ID, SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
    l_text          := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCH_LOC_EMP_JB_DY_SS', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS add constraint PK_RTL_SCH_LC_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_SCH_LOC_EMP_JB_DY_SS add CONSTRAINT PK_RTL_SCH_LC_EMP_JB_DY_SS 
      PRIMARY KEY (SK1_JOB_ID, SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SHIFT_CLOCK_IN)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
  
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end e_add_primary_key;

  --**************************************************************************************************
  --
  --
  --                    M  a  i  n    p  r  o  c  e  s  s
  --
  --
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  
  
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  l_text := 'LOAD OF RTL_EMP_JOB_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  
  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
     
  
  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCH_LOC_EMP_JB_DY';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCH_LOC_EMP_JB_DY', DEGREE => 8);

  a_remove_indexes;

  l_text := 'Truncating RTL_SCH_LOC_EMP_JB_DY_SS';
  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  execute immediate ('truncate table DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY_SS');
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_DY_SS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCH_LOC_EMP_JB_DY_SS', DEGREE => 8);

  b_insert;

   c_add_indexes;

 --  d_delete_prf;

   e_add_primary_key;
 
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
     dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;




END WH_PRF_S4S_050U_SS;
