--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_043U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_043U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load ABSENCE_EMPLOYEE_WEEK information for Scheduling for Staff(S4S)
--
--
--  Tables:      Input    - RTL_ABSENCE_EMP_DY
--               Output   - RTL_ABSENCE_EMP_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------  
--               NON-standard process
--                    due to the delete part taking too long to run,
--                    but this process will have to be re-evaluated at some stage as the runs time will get longer and longer
--               ie.
--               1. truncate performance week table (target table)
--               2. a_remove_indexes from performance week table (target table)
--               3. b_insert into performance week table (target table)
--               4. c_add_indexes back onto performance week table (target table)
--               5. e_add_primary_key back onto performance week table (target table)
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
g_rec_out            RTL_ABSENCE_EMP_WK%rowtype;
g_found              boolean;
g_date               date;
  g_NAME          VARCHAR2(40);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_043U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_ABSENCE_EMP_WK data  EX FOUNDATION';
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
    WHERE CONSTRAINT_NAME = 'PK_P_RTL_ABSNC_LC_MP_WK'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_WK';
    
    l_text               := 'alter table dwh_performance.RTL_ABSENCE_EMP_WK drop constraint PK_P_RTL_ABSNC_LC_MP_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_ABSENCE_EMP_WK drop constraint PK_P_RTL_ABSNC_LC_MP_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_RTL_ABSNC_LC_MP_WK does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_RTL_ABSNC_MP_WK'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_WK';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_RTL_ABSNC_MP_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_RTL_ABSNC_MP_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_RTL_ABSNC_MP_WK does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I20_RTL_ABSNC_MP_WK'
    AND TABLE_NAME        = 'RTL_ABSENCE_EMP_WK';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I20_RTL_ABSNC_MP_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I20_RTL_ABSNC_MP_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I20_RTL_ABSNC_MP_WK does not exist';
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

   l_text := 'Insert into RTL_ABSENCE_EMP_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
 INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK
    SELECT
      /*+ FULL(JD) PARALEL(JD,4) */
             SK1_EMPLOYEE_ID
              ,FIN_YEAR_NO
              ,FIN_WEEK_NO
              ,sk1_ABSENCE_TYPE_id
               ,sk1_LEAVE_TYPE_ID
              ,sum(nvl(ABSENCE_HOURS,0))
              ,g_date LAST_UPDATED_DATE
     FROM DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY  JD,
        DIM_CALENDAR DC
    WHERE jd.absence_date         = DC.CALENDAR_DATE
    GROUP BY SK1_EMPLOYEE_ID
              ,FIN_YEAR_NO
              ,FIN_WEEK_NO
              ,sk1_ABSENCE_TYPE_id
               ,sk1_LEAVE_TYPE_ID;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_inserted := g_recs;          
        L_TEXT := 'RTL_ABSENCE_EMP_WK : recs = '||g_recs;
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
--I10_RTL_ABSNC_MP_WK	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
--I20_RTL_ABSNC_MP_WK	NONUNIQUE	VALID	NORMAL	N	NO		NO	FIN_YEAR_NO, FIN_WEEK_NO, SK1_EMPLOYEE_ID

--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_WK', DEGREE => 8);

     
      l_text := 'create INDEX DWH_PERFORMANCE.I10_RTL_ABSNC_MP_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_RTL_ABSNC_MP_WK ON DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_RTL_ABSNC_MP_WK LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I20_RTL_ABSNC_MP_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I20_RTL_ABSNC_MP_WK ON DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK (SK1_EMPLOYEE_ID)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I20_RTL_ABSNC_MP_WK LOGGING NOPARALLEL') ;
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
--PK_P_RTL_ABSNC_LC_MP_WK	UNIQUE	VALID	NORMAL	N	NO		NO	FIN_YEAR_NO, FIN_WEEK_NO, SK1_LOCATION_NO, SK1_EMPLOYEE_ID
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_WK', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_ABSENCE_EMP_WK add constraint PK_P_RTL_ABSNC_LC_MP_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_ABSENCE_EMP_WK add CONSTRAINT PK_P_RTL_ABSNC_LC_MP_WK 
      PRIMARY KEY (FIN_YEAR_NO,FIN_WEEK_NO,SK1_EMPLOYEE_ID,SK1_ABSENCE_TYPE_ID,SK1_LEAVE_TYPE_ID)                    
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
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_ABSENCE_EMP_WK  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
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
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_WK', DEGREE => 8);

  l_text := 'truncate table dwh_performance.RTL_ABSENCE_EMP_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate('truncate table dwh_performance.RTL_ABSENCE_EMP_WK');

  a_remove_indexes;
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ABSENCE_EMP_WK', DEGREE => 8);

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


END WH_PRF_S4S_043U;
