--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_007U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_007U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load ABSENCE FACT information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_ABSENCE_EMP_DY
--               Output   - DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
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

--                Date Processing :
--                ------------
--                 No date processing - load all records from FOUNDATION
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            RTL_ABSENCE_EMP_DY%rowtype;
g_found              boolean;
g_name varchar2(40);
g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_007U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ABSENCE data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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
    WHERE CONSTRAINT_NAME = 'PK_P_RTL_ABSNC_LC_MP_DY'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_DY';
    
    l_text               := 'alter table dwh_performance.RTL_ABSENCE_EMP_DY drop constraint PK_P_RTL_ABSNC_LC_MP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_ABSENCE_EMP_DY drop constraint PK_P_RTL_ABSNC_LC_MP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_RTL_ABSNC_LC_MP_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I30_RTL_ABSNC_LC_MP_DY'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_ABSNC_LC_MP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_ABSNC_LC_MP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I30_RTL_ABSNC_LC_MP_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I40_RTL_ABSNC_LC_MP_DY'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_ABSNC_LC_MP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_ABSNC_LC_MP_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index i40_RTL_ABSNC_LC_MP_DY does not exist';
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

  l_text := 'Insert into RTL_ABSENCE_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
 INSERT /*+ append */
INTO dwh_performance.RTL_ABSENCE_EMP_DY
   WITH SELEMP AS
            (SELECT DISTINCT EMPLOYEE_ID,
              ABSENCE_DATE
            FROM dwh_foundation.FND_S4S_ABSENCE_EMP_DY
          --  WHERE last_updated_date = g_date
            ),
  selext AS
            (SELECT FLR.EMPLOYEE_ID ,
              FLR.absence_date ,
              FLR.S4S_leave_update_date ,
              FLR.ABSENCE_HOURS ,
              FLR.LEAVE_TYPE_ID ,
              DE.SK1_EMPLOYEE_ID ,
              CASE
                WHEN FLR.absence_date > FLR.S4S_leave_update_date
                THEN 1 -- 'PLANNED'
                ELSE 2 -- 'UNPLANNED'
              END ABSENCE_TYPE_id ,
              sk1_leave_type_id
            FROM dwh_foundation.FND_S4S_ABSENCE_EMP_DY flr,
              DWH_HR_PERFORMANCE.DIM_EMPLOYEE DE,
              dim_leave_type dl,
              SELEMP SE
            WHERE FLR.EMPLOYEE_ID = DE.EMPLOYEE_ID
            AND flr.leave_type_id = dl.leave_type_id
            AND FLR.EMPLOYEE_ID   = SE.EMPLOYEE_ID
            AND FLR.ABSENCE_DATE  = SE.ABSENCE_DATE
            and de.SK1_EMPLOYEE_id not IN (1150977,
1150027,
1085648,
1109814
)
            )
            SELECT 
        SE.SK1_EMPLOYEE_ID ,
        SE.absence_date ,
        SE.SK1_LEAVE_TYPE_ID ,
        da.SK1_ABSENCE_TYPE_ID ,
        SE.S4S_leave_update_date ,
        SE.ABSENCE_HOURS ,
         g_date
FROM selext se,
      dim_ABSENCE_TYPE da
WHERE se.ABSENCE_TYPE_id = da.ABSENCE_TYPE_id
ORDER BY se.SK1_EMPLOYEE_ID ,
          se.ABSENCE_DATE ;
                 
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_inserted := g_recs;        
        L_TEXT := 'RTL_ABSENCE_EMP_DY : recs = '||g_recs;
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
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_DY', DEGREE => 8);

      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_ABSNC_LC_MP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_ABSNC_LC_MP_DY ON DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_ABSNC_LC_MP_DY LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_ABSNC_LC_MP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_ABSNC_LC_MP_DY ON DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY (SK1_EMPLOYEE_ID)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_ABSNC_LC_MP_DY LOGGING NOPARALLEL') ;
   

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
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
    l_text          := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_DY ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_DY', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_ABSENCE_EMP_DY  add constraint PK_P_RTL_ABSNC_LC_MP_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_ABSENCE_EMP_DY 
      add CONSTRAINT PK_P_RTL_ABSNC_LC_MP_DY PRIMARY KEY (SK1_EMPLOYEE_ID, absence_date)                    
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
  
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_ABSENCE_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_ABSENCE_EMP_DY', DEGREE => 8);

  l_text := 'truncate table dwh_performance.RTL_ABSENCE_EMP_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate('truncate table dwh_performance.RTL_ABSENCE_EMP_DY');

  a_remove_indexes;
  
    l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_ABSENCE_EMP_DY', DEGREE => 8);

  b_insert;

   c_add_indexes;

  -- d_delete_prf;

   e_add_primary_key;



    l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_ABSENCE_EMP_DY', DEGREE => 8);

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



END WH_PRF_S4S_007U;
