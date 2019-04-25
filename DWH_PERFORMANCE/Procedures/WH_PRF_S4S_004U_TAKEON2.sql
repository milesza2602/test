--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004U_TAKEON2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004U_TAKEON2" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  FOR DATA TAKEON ONLY
  --**************************************************************************************************
   --  Date:        July 2014
   --  Author:      Wendy lyttle
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)



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
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_tbc      INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_rec_out RTL_EMP_JOB_DY%rowtype;
  g_found                BOOLEAN;
  g_date                 DATE;
  G_THIS_WEEK_START_DATE DATE;
  g_fin_days             NUMBER;
  G_END_DATE         DATE;
  g_run_date             DATE   := TRUNC(sysdate);
  g_run_seq_no           NUMBER := 0;
  g_recs                 NUMBER := 0;
  g_EMP_START            VARCHAR2(11);
  g_EMP_END              VARCHAR2(11);
  g_recs_deleted         INTEGER := 0;
  l_message sys_dwh_errlog.log_text%type;
--  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_005U';
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_004U_TAKEON2';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_EMP_JOB_DY data  EX FOUNDATION';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF RTL_EMP_AVAIL_LOC_JOB_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  -- g_date := '7 dec 2014';
  -- derivation of cobnstr_end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
  SELECT DISTINCT THIS_WEEK_END_DATE
  INTO G_END_DATE
  FROM DIM_CALENDAR
  WHERE CALENDAR_DATE = g_date + 20;
  l_text             := 'Derived G_END_DATE - '||G_END_DATE;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
   begin
    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
    end;

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
 

G_EMP_START := '7059390';
G_EMP_END   := '999999999';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY
WITH selext1 AS
  (
  SELECT
    /*+ FULL(FC) FULL(FLR) FULL(EJ) FULL(EL)   */
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
            el.SK1_LOCATION_NO,
            ej.SK1_job_id,
            el.EMPLOYEE_STATUS,
            DE.SK1_EMPLOYEE_ID,
            dc.calendar_date tran_date,
            el.EFFECTIVE_START_DATE,
            el.EFFECTIVE_END_DATE,
            ((( flr.fixed_roster_end_time - flr.fixed_roster_start_time) * 24 * 60) - flr.meal_break_minutes) / 60 FIXED_ROSTER_HRS
  FROM dwh_foundation.FND_S4S_emp_avail_DY flr
        JOIN dim_employee DE
        ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3 fc
        ON fc.employee_id             = flr.employee_id
     --  AND fc.ORIG_CYCLE_start_date = flr.CYCLE_start_date
        AND fc.availability_start_date = flr.availability_start_date
        AND fc.week_number            = FLR.WEEK_NUMBER
  JOIN DIM_CALENDAR DC
      ON DC.THIS_WEEK_START_DATE BETWEEN fc.CYCLE_START_DATE AND NVL(fc.CYCLE_END_DATE, G_END_DATE)
        --       ON DC.THIS_WEEK_START_DATE BETWEEN fc.CYCLE_START_DATE AND NVL(fc.CYCLE_END_DATE- 1, '19 oct 2014')
      AND dc.this_week_start_date = fc.this_week_start_date
      AND dc.fin_day_no           = FLR.DAY_OF_WEEK
  JOIN RTL_EMP_JOB_DY Ej
      ON Ej.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
      AND Ej.TRAN_DATE      = dc.CALENDAR_DATE
  JOIN RTL_EMP_LOC_STATUS_DY El
        ON El.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
        AND El.TRAN_DATE      = dc.CALENDAR_DATE
  --      where flr.employee_id in ('1002436','7089864')
 where 
 --flr.last_updated_date = g_date   -- not sure if should be here-- 
 -- or
              dc.calendar_date between flr.cycle_start_date and g_end_date -- not sure if should be here-- 
              AND FLR.EMPLOYEE_ID BETWEEN G_EMP_START AND G_EMP_END
  GROUP BY FLR.EMPLOYEE_ID ,
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
            el.SK1_LOCATION_NO,
            ej.SK1_job_id,
            el.EMPLOYEE_STATUS,
            DE.SK1_EMPLOYEE_ID,
            dc.calendar_date ,
            el.EFFECTIVE_START_DATE,
            el.EFFECTIVE_END_DATE,
            ((( flr.fixed_roster_end_time - flr.fixed_roster_start_time) * 24 * 60) - flr.meal_break_minutes) / 60
  ORDER BY FLR.EMPLOYEE_ID ,
    dc.calendar_date ,
    FLR.DAY_OF_WEEK ,
    fc.week_number
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
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
      --   WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R') AND SE1.availability_start_date >= se1.effective_start_date AND se1.availability_end_date IS NULL THEN to_date('19/10/2014','dd/mm/yyyy')
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NULL      THEN G_END_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NOT NULL      THEN se1.availability_end_date
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
  WHERE SE1.EMPLOYEE_STATUS        = 'S'
      OR ( SE1.EMPLOYEE_STATUS        IN ('H','I','R')
      AND SE1.availability_start_date >= se1.effective_start_date )
  ) 
  
SELECT /*+ full(rtl) */ DISTINCT se2.SK1_LOCATION_NO ,
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
  ----and se2.tran_date between sp.cycle_end_date - (7*mCYCLEweeknum)+1 and sp.cycle_end_date
AND derive_start_date  IS NOT NULL
ORDER BY se2.SK1_LOCATION_NO
,se2.SK1_JOB_ID
,se2.SK1_EMPLOYEE_ID
,se2.TRAN_DATE ;
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
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_run_completed ||sysdate;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := ' ';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;
p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;


END WH_PRF_S4S_004U_TAKEON2;
