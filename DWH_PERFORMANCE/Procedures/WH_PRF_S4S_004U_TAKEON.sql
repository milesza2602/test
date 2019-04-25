--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004U_TAKEON" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
   --**************************************************************************************************
    -- copy of wh_prf_s4s_004u - taken on 6/oct/2014 before code to change aviability to cycle
    --**************************************************************************************************
   --  Date:        July 2014
   --  Author:      Wendy lyttle
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
--
--               Delete process :
--           a.)    Due to changes which can be made, we have to drop the current data and load the new data
--                    eg. If record exist for Mon/Tues/Wed but we now receive a record for Thurs/Fri
--                        we have to delete the Mon/Tues/Wed record and keep the Thurs/Fri record(s)
--                           based upon employee_id and availability_start_date
--           b.)    Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and cycle_start_date
--
--                The delete lists are used in the rollups as well.
--                The delete lists were created in the STG to FND load  
--                ie. FND_S4S_EMP_AVAIL_DY_del_list
--                    FND_S4S_EMP_AVAIL_DY_DEL
--
   --**************************************************************************************************
   -- setup dates
   -- Each cycle_period has a certain no_of_weeks in which certain days apply(availability)
   -- We have to 'cycle' these weeks from the availability_start_date through to the beginning of the next date
   -- To do this we have to
   -- 1. derive the end_date for the availability period
   -- 2. generate the missing weeks during these periods
   -- 3. generate the missing weeks between periods
   --**************************************************************************************************
   --
   --  Tables:      Input    - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3
   --               Output   - RTL_EMP_AVAIL_LOC_JOB_DY -- RTL_EMP_AVAIL_LOC_JOB_DY
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
   g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read        INTEGER := 0;
   g_recs_inserted    INTEGER := 0;
   g_recs_updated     INTEGER := 0;
   g_recs_tbc         INTEGER := 0;
   g_error_count      NUMBER := 0;
   g_error_index      NUMBER := 0;
   g_count            NUMBER := 0;
   g_rec_out          RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
   g_found            BOOLEAN;
   g_date             DATE;
   g_SUB      NUMBER := 0;
   g_end_date             DATE;

g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004U_TAKEON';
      l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
      l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
      l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
      l_text sys_dwh_log.log_text%TYPE;
      l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD THE RTL_EMP_AVAIL_LOC_JOB_DY data  EX FOUNDATION';
      l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
      -- For output arrays into bulk load forall statements --
      TYPE tbl_array_i
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
      TYPE tbl_array_u
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
        a_tbl_insert tbl_array_i;
        a_tbl_update tbl_array_u;
        a_empty_set_i tbl_array_i;
        a_empty_set_u tbl_array_u;
        a_count   INTEGER := 0;
        a_count_i INTEGER := 0;
        a_count_u INTEGER := 0;
---
-- Reason for extra step (ie. with caluse) is that when we ran the history in we had no response
-- This 'step' approach seemed to work better
--

begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_EMP_CONSTR_WK  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);
--
--g_date := '7 dec 2014';

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       select distinct this_week_start_date + 20
   into g_end_date
   from dim_calendar where calendar_date = g_date;
 --g_end_date := '7 dec 2014';
 --g_date := '16 nov 2014';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';
    
   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART3', DEGREE => 8);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3


    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';
    
  l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_JOB_DY', DEGREE => 8);
  
    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_emp_avail_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_AVAIL_DY', DEGREE => 8);
  
    l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_LOC_STATUS_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_LOC_STATUS_DY', DEGREE => 8);

    l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TEMP_S4S_LOC_EMP_DY_PART3', DEGREE => 8);
   --**************************************************************************************************
 

INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY
with selexta
 as
 (SELECT
    /*+ FULL(FC) FULL(FLR) parallel(flr,8) parallel(fc,8)  */
    distinct
            FLR.EMPLOYEE_ID ,
            FC.ORIG_CYCLE_START_DATE,
            fc.CYCLE_START_date,
            fc.CYCLE_end_date,
            fc.WEEK_NUMBER,
            FLR.DAY_OF_WEEK,
            FLR.AVAILABILITY_START_DATE,
            FLR.AVAILABILITY_END_DATE,
            FLR.FIXED_ROSTER_START_TIME,
            FLR.FIXED_ROSTER_END_TIME,
            FLR.NO_OF_WEEKS,
            FLR.MEAL_BREAK_MINUTES,
            de.sk1_employee_id,
            fc.this_week_start_date
  FROM dwh_foundation.FND_S4S_emp_avail_DY flr
  JOIN dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3 fc
        ON fc.employee_id             = flr.employee_id
        AND fc.availability_start_date = flr.availability_start_date
        AND fc.week_number            = FLR.WEEK_NUMBER
  join dwh_hr_performance.dim_employee de
  on de.employee_id = fc.employee_id
  ORDER BY de.sk1_employee_id,fc.CYCLE_START_date )
  ,
  selext1
  as 
 ( select  /*+ FULL(sea) FULL(dc) FULL(ej) FULL(el) parallel(sea,8) parallel(dc,8) parallel(ej,8) parallel(el,8) */
           sea.EMPLOYEE_ID ,
            sea.ORIG_CYCLE_START_DATE,
            sea.CYCLE_START_date,
            sea.CYCLE_end_date,
            sea.WEEK_NUMBER,
            sea.DAY_OF_WEEK,
            sea.AVAILABILITY_START_DATE,
            sea.AVAILABILITY_END_DATE,
            sea.FIXED_ROSTER_START_TIME,
            sea.FIXED_ROSTER_END_TIME,
            sea.NO_OF_WEEKS,
            sea.MEAL_BREAK_MINUTES,
            sea.sk1_employee_id,
            el.SK1_LOCATION_NO,
            ej.SK1_job_id,
            el.EMPLOYEE_STATUS,
            dc.calendar_date tran_date,
            el.EFFECTIVE_START_DATE,
            el.EFFECTIVE_END_DATE,
            ((( sea.fixed_roster_end_time - sea.fixed_roster_start_time) * 24 * 60) - sea.meal_break_minutes) / 60 FIXED_ROSTER_HRS
  FROM selexta sea
  JOIN DIM_CALENDAR DC
      ON DC.THIS_WEEK_START_DATE BETWEEN sea.CYCLE_START_DATE AND NVL(sea.CYCLE_END_DATE, NVL(sea.CYCLE_END_DATE, G_END_DATE))
       AND dc.this_week_start_date = sea.this_week_start_date
      AND dc.fin_day_no           = sea.DAY_OF_WEEK
  JOIN RTL_EMP_JOB_DY Ej
      ON Ej.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
      AND Ej.TRAN_DATE      = dc.CALENDAR_DATE
  JOIN RTL_EMP_LOC_STATUS_DY El
        ON El.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
        AND El.TRAN_DATE      = dc.CALENDAR_DATE
 where 
 --flr.last_updated_date = g_date   
 -- not sure if should be here as driven by 'temp table' above-- 
 -- or
              dc.calendar_date between sea.cycle_start_date and g_end_date  -- not sure if should be here-- 
              )
  
  ,
  selext2 AS
  (
  SELECT DISTINCT se1.SK1_LOCATION_NO ,
                    se1.SK1_EMPLOYEE_ID ,
                    se1.SK1_JOB_ID ,
                    se1.TRAN_DATE ,
                    se1.AVAILABILITY_START_DATE ,
                    se1.NO_OF_WEEKS ,
                    se1.DAY_OF_WEEK ,
                    se1.ORIG_CYCLE_START_DATE ,
                    se1.CYCLE_START_DATE ,
                    se1.CYCLE_END_DATE ,
                    se1.WEEK_NUMBER ,
                    se1.AVAILABILITY_END_DATE ,
                    se1.FIXED_ROSTER_START_TIME ,
                    se1.FIXED_ROSTER_END_TIME ,
                    se1.MEAL_BREAK_MINUTES ,
                    se1.FIXED_ROSTER_HRS ,
                    --    rtl.sk1_employee_id rtl_exists ,
                    SE1.EMPLOYEE_STATUS ,
                    SE1.EFFECTIVE_START_DATE ,
                    SE1.EFFECTIVE_END_DATE ,
                    se1.employee_id ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NULL      THEN se1.ORIG_CYCLE_START_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NOT NULL      THEN se1.ORIG_CYCLE_START_DATE
      ELSE NULL
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
      --   WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R') AND SE1.availability_start_date >= se1.effective_start_date AND se1.availability_end_date IS NULL THEN to_date('19/10/2014','dd/mm/yyyy')
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NULL      THEN G_END_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NOT NULL      THEN se1.availability_end_date
      ELSE NULL
      END) derive_end_date
  FROM selext1 SE1
  WHERE SE1.EMPLOYEE_STATUS        = 'S'
      OR ( SE1.EMPLOYEE_STATUS        IN ('H','I','R')
      AND SE1.availability_start_date >= se1.effective_start_date )
  ) 
  
SELECT /*+ full(se2) */ DISTINCT se2.SK1_LOCATION_NO ,
                se2.SK1_JOB_ID ,
                se2.SK1_EMPLOYEE_ID ,
                se2.TRAN_DATE ,
                se2.AVAILABILITY_START_DATE ,
                se2.NO_OF_WEEKS ,
                se2.DAY_OF_WEEK ,
                se2.ORIG_CYCLE_START_DATE ORIG_CYCLE_START_DATE ,
                SE2.CYCLE_START_DATE ,
                sE2.cycle_end_date ,
                se2.WEEK_NUMBER ,
                se2.AVAILABILITY_END_DATE ,
                se2.FIXED_ROSTER_START_TIME ,
                se2.FIXED_ROSTER_END_TIME ,
                se2.MEAL_BREAK_MINUTES ,
                se2.FIXED_ROSTER_HRS ,
                G_DATE LAST_UPDATED_DATE
FROM selext2 se2 
WHERE se2.TRAN_DATE BETWEEN derive_start_date AND derive_end_date
AND derive_start_date  IS NOT NULL
;

 G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--****************************************************************************

l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_AVAIL_LOC_JOB_DY';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_AVAIL_LOC_JOB_DY', DEGREE => 8);


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type,
      dwh_constants.vc_log_ended, g_recs_read, g_recs_inserted, g_recs_updated, '','');
      l_text := dwh_constants.vc_log_time_completed || TO_CHAR (SYSDATE, ('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_read || g_recs_read;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_inserted || g_recs_inserted;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_updated || g_recs_updated;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_run_completed || SYSDATE;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_draw_line;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := ' ';
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
   COMMIT;
   p_success := TRUE;
EXCEPTION
   WHEN dwh_errors.e_insert_error
   THEN
      l_message := dwh_constants.vc_err_mm_insert || SQLCODE || ' ' || SQLERRM;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
      ROLLBACK;
      p_success := FALSE;
      RAISE;
WHEN OTHERS THEN
      l_message := dwh_constants.vc_err_mm_other || SQLCODE || ' ' || SQLERRM;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
      ROLLBACK;
      p_success := FALSE;
      RAISE;


END WH_PRF_S4S_004U_takeon;
