--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_065U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_065U_OLD" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     Load schedule employee exception information FACT information for SCHEDuling for Staff(S4S)
  --
  --  Tables:      Input    - dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY
  --               Output   - DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------  
--               NON-standard process
--                    due to the delete part taking too long to run,
--                    but this process will have to be re-evaluated at some stage as the runs time will get longer and longer
--               ie.
--               1. truncate performance day table (target table)
--               2. a_remove_indexes from performance day table (target table)
--               3. b_insert into performance day table (target table)
--               4. c_add_indexes back onto performance day table (target table)
--               5. e_add_primary_key back onto performance day table (target table)
--
--
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
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_tbc      INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_cnt           NUMBER  := 0;
  g_NAME          VARCHAR2(40);
  g_rec_out RTL_SCHED_XCPTN_EMP_DY%rowtype;
  g_found                BOOLEAN;
  g_date                 DATE;
  G_THIS_WEEK_START_DATE DATE;
  g_fin_days             NUMBER;
  g_eff_end_date         DATE;
  g_run_date             DATE    := TRUNC(sysdate);
  g_run_seq_no           NUMBER  := 0;
  g_recs                 NUMBER  := 0;
  g_recs_deleted         INTEGER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_065U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_SCHED_XCPTN_EMP_DY data  EX FOUNDATION';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
 

  --**************************************************************************************************
  -- Remove constraints and indexes
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_RTL_SCHED_XCPTN_EMP_DY'
    AND TABLE_NAME        = 'RTL_SCHED_XCPTN_EMP_DY';
    
    l_text               := 'alter table dwh_performance.RTL_SCHED_XCPTN_EMP_DY drop constraint PK_RTL_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_SCHED_XCPTN_EMP_DY drop constraint PK_RTL_SCHED_XCPTN_EMP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_RTL_SCHED_XCPTN_EMP_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I30_RTL_SCHED_XCPTN_EMP_DY'
    AND TABLE_NAME        = 'RTL_SCHED_XCPTN_EMP_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_SCHED_XCPTN_EMP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I30_RTL_SCHED_XCPTN_EMP_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I40_RTL_SCHED_XCPTN_EMP_DY'
    AND TABLE_NAME        = 'RTL_SCHED_XCPTN_EMP_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_SCHED_XCPTN_EMP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I40_RTL_SCHED_XCPTN_EMP_DY does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


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

   l_text := 'Insert into RTL_SCHED_XCPTN_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          INSERT
            /*+ append */
          INTO DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY
        SELECT  /*+ full(flr) parallel(flr,6) */              
                    he.sk1_employee_ID
                    , ex.sk1_EXCEPTION_TYPE_ID
                  , FLR.exception_date
                  , dl.SK1_LOCATION_NO
                  , de.SK1_JOB_ID
                  , FLR.exception_start_time
                  , FLR.exception_END_time
                  , G_date
                  FROM dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY flr
                 join dwh_performance.DIM_LOCATION DL
                        on    dl.LOCATION_NO = flr.LOCATION_NO
                 join dwh_hr_performance.DIM_employee he
                        on    he.employee_ID = flr.employee_ID
                 join dwh_performance.DIM_exception_TYPE ex
                        on    ex.EXCEPTION_TYPE_ID = flr.EXCEPTION_TYPE_ID 
                 join dwh_performance.DIM_JOB DE
                        on    de.JOB_ID = flr.JOB_ID
                 
         ;
            
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_inserted := g_recs;          
        L_TEXT := 'RTL_SCHED_XCPTN_EMP_DY : recs = '||g_recs;
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
--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_SCHED_XCPTN_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCHED_XCPTN_EMP_DY', DEGREE => 8);

     
      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_SCHED_XCPTN_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_SCHED_XCPTN_EMP_DY ON DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_SCHED_XCPTN_EMP_DY LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_SCHED_XCPTN_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_SCHED_XCPTN_EMP_DY ON DWH_PERFORMANCE.RTL_SCHED_XCPTN_EMP_DY (SK1_EMPLOYEE_ID,EXCEPTION_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_SCHED_XCPTN_EMP_DY LOGGING NOPARALLEL') ;
--** nb. check logging **
 
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
-- create primary key 
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_SCHED_XCPTN_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCHED_XCPTN_EMP_DY', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_SCHED_XCPTN_EMP_DY add constraint PK_RTL_SCHED_XCPTN_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_SCHED_XCPTN_EMP_DY 
      add CONSTRAINT PK_RTL_SCHED_XCPTN_EMP_DY PRIMARY KEY (SK1_EMPLOYEE_ID,SK1_JOB_ID,SK1_LOCATION_NO,SK1_EXCEPTION_TYPE_ID,EXCEPTION_DATE)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
      
 
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in e_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in e_add_primary_key';
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
  
  l_text := 'LOAD OF RTL_SCHED_XCPTN_EMP_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
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
  
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_SCHED_XCPTN_EMP_DY', DEGREE => 8);

  l_text := 'truncate table dwh_performance.RTL_SCHED_XCPTN_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate('truncate table dwh_performance.RTL_SCHED_XCPTN_EMP_DY');

  a_remove_indexes;
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_SCHED_XCPTN_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_SCHED_XCPTN_EMP_DY', DEGREE => 8);

  b_insert;

  c_add_indexes;

 
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


  END WH_PRF_S4S_065U_OLD;
