--------------------------------------------------------
--  DDL for Procedure AAAAA_WL_S4S_1DEC2014
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."AAAAA_WL_S4S_1DEC2014" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     check data sent after datafixes to end_date
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_LOC_STATUS
--               Output   - DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY
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
g_rec_out            RTL_EMP_LOC_STATUS_DY%rowtype;
g_found              boolean;
g_date               date;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_eff_end_date  date;

g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'AAAAA_WL_S4S_1DEC2014';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_EMP_LOC_STATUS_DY data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n; 
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_EMP_LOC_STATUS_DY  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    SELECT distinct THIS_WEEK_END_DATE into g_eff_end_date
    FROM DIM_CALENDAR 
    WHERE CALENDAR_DATE = trunc(sysdate) + 20;
    
   -- G_DATE := '1 DEC 2014';
      
    l_text := 'Derived g_eff_end_date - '||g_eff_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

execute immediate('drop table dwh_datafix.wl_1dec2015_emplocstatusdy');
commit;
    l_text := 'drop table dwh_datafix.wl_1dec2015_emplocstatusdy';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
execute immediate('create table dwh_datafix.wl_1dec2015_emplocstatusdy 
as select * from DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY 
where sk1_employee_id is null');
commit;
    l_text := 'create table dwh_datafix.wl_1dec2015_emplocstatusdy';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
insert /*+ append */ into dwh_datafix.wl_1dec2015_emplocstatusdy
 WITH
SELEXT1 AS (
            SELECT DISTINCT
                  /*+ full(flr) full(de) full(dl) */
                  de.SK1_EMPLOYEE_ID ,
                  dl.SK1_LOCATION_NO ,
                  flr.EMPLOYEE_STATUS ,
                  flr.EMPLOYEE_WORKSTATUS ,
                  flr.EFFECTIVE_START_DATE ,
                  flr.EFFECTIVE_END_DATE,
                    dc.calendar_date tran_date
            FROM FND_S4S_EMP_LOC_STATUS flr
            JOIN DWH_HR_PERFORMANCE.dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            JOIN DIM_LOCATION DL
                  ON DL.LOCATION_NO = FLR.LOCATION_NO
            JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE  AND  NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
     --      where flr.last_updated_date >= G_DATE
     --      OR FLR.EFFECTIVE_END_DATE IS NULL
           ),
selext2 as (SELECT DISTINCT SK1_EMPLOYEE_ID ,
                  SK1_LOCATION_NO ,
                  EMPLOYEE_STATUS ,
                  tran_date,
                  EMPLOYEE_WORKSTATUS ,
                  EFFECTIVE_START_DATE ,
                  EFFECTIVE_END_DATE,
                   ( CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN SE1.effective_START_DATE
                        ELSE NULL
                          --SE1.availability_start_DATE - 1
                      END) derive_start_date ,
                   (CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN g_eff_end_date
                         WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN se1.effective_end_date - 1
                        ELSE NULL
                          --SE1.availability_END_DATE - 1
                      END) derive_end_date
  FROM selext1 SE1
   WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
select distinct 
se2.SK1_LOCATION_NO
,se2.SK1_EMPLOYEE_ID
,se2.TRAN_DATE
,se2.EMPLOYEE_STATUS
,se2.EMPLOYEE_WORKSTATUS
,se2.EFFECTIVE_START_DATE
,se2.EFFECTIVE_END_DATE
,to_date('31/01/2015', 'dd/mm/yyyy')
from selext2 se2
where se2.tran_DATE BETWEEN derive_start_date AND derive_end_date;

  g_recs :=SQL%ROWCOUNT ;
          COMMIT;

commit;
       L_TEXT := 'emp_loc_status load to dwh_datafix.wl_1dec2015_emplocstatusdy ='||g_recs;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
   
      l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'WL_1DEC2015_EMPLOCSTATUSDY', DEGREE => 8);
   
 
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
END AAAAA_WL_S4S_1DEC2014;
