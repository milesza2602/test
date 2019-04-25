--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_005U_ENDDT_DATAFX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_005U_ENDDT_DATAFX" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE JOB DY FACT information for Scheduling for Staff(S4S)
--                NB> job_start_date = MONDAY and job_end_date = SUNDAY
--
--               Delete process :
--                   Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and job_start_date
--
--                The delete lists are used in the rollups as well.
--                The delete lists were created in the STG to FND load  
--                ie. FND_S4S_EMP_JOB_del_list
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_JOB
--               Output   - DWH_PERFORMANCE.RTL_EMP_JOB_DY
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
g_rec_out            RTL_EMP_JOB_DY%rowtype;
g_found              boolean;
g_JOB_end_date           date;

g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_005U_ENDDT_DATAFX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE EMPLOYEE JOB DY  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_EMP_JOB_DY   EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
        dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--
--g_date := '7 dec 2014';

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    -- derivation of cobnstr_end_date for recs where null.
    --  = 21days+ g_date+ days for rest of week
    SELECT distinct THIS_WEEK_END_DATE into g_JOB_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = g_date + 20;


    l_text := 'Derived g_JOB_end_date - '||g_JOB_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_JOB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_JOB', DEGREE => 8);

insert /*+ append */ into dwh_performance.wl_2dec2015_empjobdy
WITH
SELEXT1 AS (
           SELECT /*= FULL(FLR) FULL(EL) PARALLEL(FLR,4) , PARALLEL(EL,4) */
                DISTINCT  DC.CALENDAR_DATE TRAN_DATE
                  ,FLR.JOB_START_DATE
                  ,FLR.JOB_END_DATE
                  ,FLR.EMPLOYEE_RATE
                  ,DE.SK1_EMPLOYEE_ID
                  ,DP.SK1_PAYPOLICY_ID
                  ,DJ.SK1_JOB_ID
                  ,EL.EMPLOYEE_STATUS
                  ,effective_START_DATE
                  ,effective_end_DATE
            FROM
                  FND_S4S_EMP_JOB flr
            JOIN DWH_HR_PERFORMANCE.dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
       --    where flr.last_updated_date = g_date
        --    and not exists (select distinct a.sk1_employee_id
       --           from  dwh_datafix.wl_emp_loc_status_dups a
        --          where a.sk1_employee_id = de.sk1_employee_id)
           
           --------------------- added in to recover job running out of sequence ------------------
       --    or exists (select distinct employee_id, job_start_date from dwh_foundation.FND_S4S_EMP_JOB_DEL_LIST A
       --  where run_seq_no = 2
       --  AND A.EMPLOYEE_ID = flr.EMPLOYEE_ID
       --   AND flr.job_start_date = A.job_START_DATE)
          ------------------------
),
selext2 as (SELECT DISTINCT TRAN_DATE
                  ,JOB_START_DATE
                  ,JOB_END_DATE
                  ,EMPLOYEE_RATE
                  ,SK1_EMPLOYEE_ID
                  ,SK1_PAYPOLICY_ID
                  ,SK1_JOB_ID
                  ,EMPLOYEE_STATUS
                  ,  (
                      CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN SE1.job_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN SE1.job_START_DATE
                        ELSE NULL
                          --SE1.availability_start_DATE - 1
                      END) derive_start_date ,
                      (
                      CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN NVL(SE1.job_END_DATE, g_job_end_date)
                         WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN se1.effective_end_date
                        ELSE NULL
                          --SE1.availability_END_DATE - 1
                      END) derive_end_date
  FROM selext1 SE1
   WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT 
DISTINCT
                  SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,g_date LAST_UPDATED_DATE
            FROM SELEXT2 SE2
             WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
             
;
commit;
    l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_EMP_JOB_DY', DEGREE => 8);

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




END WH_PRF_S4S_005U_ENDDT_DATAFX;
