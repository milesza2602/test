--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_006U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_006U_TAKEON" (
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
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_006U_TAKEON';
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
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION', 'FND_S4S_EMP_JOB', DEGREE => 8);
  

  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  INSERT /*+ APPEND */
  INTO DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_DY
 SELECT  /*+ full(flr) full(de) */
                        SK1_LOCATION_NO ,
                    SK1_EMPLOYEE_ID ,
                    SK1_JOB_ID,
                    sk1_payment_category_NO ,
                    FLR.PAY_WEEK_DATE ,
                    flr.BUSINESS_DATE ,
                    flr.LAST_MODIFIED_DATE ,
                    FLR.ACTUAL_HRS ,
                    g_date
FROM dwh_foundation.FND_S4S_LOC_EMP_job_PAYCAT_DY flr,
      DIM_LOCATION DL,
      dwh_hr_performance.dim_employee DE,
      DIM_JOB DJ,
      dim_payment_category dpc
WHERE FLR.LOCATION_NO      = DL.LOCATION_NO
  AND FLR.EMPLOYEE_ID         = DE.EMPLOYEE_ID
  AND FLR.JOB_ID              = DJ.JOB_ID
  AND FLR.payment_category_NO = Dpc.payment_category_NO
--  and flr.last_updated_date = g_date
;

G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--****************************************************************************

l_text := 'Running GATHER_TABLE_STATS ON RTL_LOC_EMP_JOB_PAYCAT_DY';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOC_EMP_JOB_PAYCAT_DY', DEGREE => 8);

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

END WH_PRF_S4S_006U_TAKEON;
