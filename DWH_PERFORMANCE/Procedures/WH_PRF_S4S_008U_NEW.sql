--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_008U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_008U_NEW" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --***************************************************************************
  -- ***********************
  --***************************************************************************
  -- ***********************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     Load EMPLOYEE_LOCATION_WEEK information for Scheduling for
  -- Staff(S4S)
  --
  --               Delete process :
  --           Due to changes which can be made, we have to drop the current
  -- data and load the new data
  --                        based upon employee_id and tran_date
  --                           and last_updated_date
  --
  --
  --  Tables:      Input    - TST_EMP_AVAIL_LOC_JOB_DY
  --               Output   - DWH_PERFORMANCE.TST_EMP_AVAIL_LOC_JOB_WK
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --
  -- DELETED
  /*  DELETE FROM dwh_performance.TST_EMP_AVAIL_LOC_JOB_DY
  WHERE SK1_EMPLOYEE_ID IN (1184460
  ,1126731
  ,1150463
  ,1116630
  ,1123783
  ,1134165
  ,1134301
  ,1184369
  ,1094154
  ,1180677
  ,1115804
  ,1104147
  ,1125356
  ,1095334
  ,1180618
  ,1092147
  ,1091392
  ,1184347
  ,1181475
  ,1082540
  ,1123653
  ,1093060
  ,1137523
  ,1140322
  ,1185061
  ,1137403
  ,1154494
  ,1150399
  ,1184442
  ,1126644
  ,1104412
  ,1100483
  ,1182140
  ,1088974
  ,1148924
  ,1180566
  ,1140348
  ,1082420
  ,1089476
  ,1085814
  ,1127645
  ,1150220
  ,1155309
  ,1181106
  ,1097064
  ,1096280
  ,1097570
  ,1184454
  ,1185038
  ,1087141
  ,1124182
  ,1086857
  ,1120975
  ,1180615
  ,1148786
  ,1110530
  ,1088104
  ,1155843
  ,1107005
  ,1086509
  ,1100221
  ,1158315
  ,1181715)
  */
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --***************************************************************************
  -- ***********************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_tbc      INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_rec_out TST_EMP_AVAIL_LOC_JOB_WK%rowtype;
  g_found        BOOLEAN;
  g_date         DATE;
  g_run_date     DATE    := TRUNC(sysdate);
  g_run_seq_no   NUMBER  := 0;
  g_recs         NUMBER  := 0;
  g_recs_deleted INTEGER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_008U_NEW';
  l_name sys_dwh_log.log_name%type                     :=
  dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type :=
  dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type :=
  dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type :=
  'LOAD THE TST_EMP_AVAIL_LOC_JOB_WK data  EX FOUNDATION';
  l_process_type sys_dwh_log_summary.log_process_type%type :=
  dwh_constants.vc_log_process_type_n;
  --***************************************************************************
  -- ***********************
  -- Main process
  --***************************************************************************
  -- ***********************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit >
    dwh_constants.vc_forall_minimum THEN
    g_forall_limit := p_forall_limit;
  END IF;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  l_text := 'LOAD OF TST_EMP_AVAIL_LOC_JOB_WK  EX FOUNDATION STARTED '||
  TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,
  l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,
  '','','','','');
  --***************************************************************************
  -- ***********************
  -- Look up batch date from dim_control
  --***************************************************************************
  -- ***********************
  dwh_lookup.dim_control(g_date);
  -- hardcoding batch_date for testing
  --g_date := trunc(sysdate);
  --g_date := '7 dec 2014';
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  --***************************************************************************
  -- ***********************
  -- prepare environment
  --***************************************************************************
  -- ***********************
  g_recs_inserted :=0;
  g_recs_deleted  := 0;
  l_text          := 'TRUNCATE TABLE DWH_PERFORMANCE.TST_EMP_AVAIL_LOC_JOB_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.TST_EMP_AVAIL_LOC_JOB_WK');
  l_text := 'Running GATHER_TABLE_STATS ON TST_EMP_AVAIL_LOC_JOB_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text)
  ;
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TST_EMP_AVAIL_LOC_JOB_WK',
  DEGREE => 8);
  --***************************************************************************
  -- ***********************
  -- Insert records from Performance
  --***************************************************************************
  -- ***********************
        g_recs_inserted := 0;    
        g_recs := 0; 
 
  INSERT
    /*+ APPEND */
  INTO
    DWH_PERFORMANCE.TST_EMP_AVAIL_LOC_JOB_WK
WITH
  seldat AS
  (
    SELECT
      /*+ FULL(RTL) PARALlEL(RTL,4) */
      DISTINCT fin_year_no,
      fin_week_no,
      -- MAX(tran_date) maxtran_date,
      tran_date maxtran_date,
      sk1_employee_id,
      sk1_JOB_ID,
      sk1_location_no
    FROM
      dwh_performance.TST_EMP_AVAIL_LOC_JOB_DY rtl,
      dim_calendar dc
    WHERE
      DC.CALENDAR_DATE = RTL.TRAN_DATE
      --                   and sk1_employee_id not in
      --(1128052,1081138,1105841,1140310
      --                    ,1115612,1106616,1110799,1121833
      --,1091054,1093962,1098280,1118245,1099362,1123783,1147314,1179699
      --,1086608,1083959,1081113,1181140,1185455,1149637,1143309,1139745
      --,1085485,1128957,1093332,1147727,1128074,1102435,1088974,1115283
      --,1098609,1110606,1140383,1094196,1129192,1107927,1126803,1107740
      --,1186080,1086637,1124145,1096589,1085983,1179730,1094109,1153060
      --,1127089,1147406,1088457)
  )
  ,
  SELEXT AS
  (
    SELECT
      RTL.SK1_LOCATION_NO,
      RTL.SK1_EMPLOYEE_ID,
      RTL.SK1_JOB_ID,
      FIN_YEAR_NO,
      FIN_WEEK_NO,
      cycle_start_date,
      cycle_end_date,
      week_number,
      SUM(NVL(FIXED_ROSTER_hrs,0)) FIXED_ROSTER_hrs_WK,
      SUM(NVL(FIXED_ROSTER_hrs,0)) / 40 FIXED_ROSTER_FTE_WK
      --                    SUM(((fixed_roster_end_time -
      -- fixed_roster_start_time) * 24 * 60) - meal_break_minutes) / 40
      -- FIXED_ROSTER_FTE_WK
    FROM
      dwh_PERFORMANCE.TST_EMP_AVAIL_LOC_JOB_DY rtl,
      seldat sd
    WHERE
      rtl.tran_DATE         = sd.maxtran_date -- WAS LEFT OUTER BUT MSADE STRAIGHT JOIN
    AND RTL.SK1_LOCATION_NO = sd.SK1_LOCATION_NO
    AND RTL.SK1_EMPLOYEE_ID = sd.SK1_EMPLOYEE_ID
    AND RTL.SK1_JOB_ID      = sd.SK1_JOB_ID
    GROUP BY
      RTL.SK1_LOCATION_NO,
      RTL.SK1_EMPLOYEE_ID,
      RTL.SK1_JOB_ID,
      FIN_YEAR_NO,
      FIN_WEEK_NO,
      cycle_start_date,
      CYCLE_END_DATE,
      WEEK_NUMBER
  )

    SELECT
      /*+ FULL(jd) PARALlEL(jd,4) */
      DISTINCT rtl.SK1_LOCATION_NO,
      rtl.SK1_JOB_ID,
      rtl.SK1_EMPLOYEE_ID,
      rtl.FIN_YEAR_NO,
      rtl.FIN_WEEK_NO,
      rtl.WEEK_NUMBER,
      rtl.cycle_start_date,
      rtl.cycle_end_date,
      RTL.FIXED_ROSTER_hrs_WK ,
      RTL.FIXED_ROSTER_FTE_WK,
      RTL.FIXED_ROSTER_hrs_WK * employee_rate FIXED_ROSTER_COST_wk,
      g_date LAST_UPDATED_DATE
    FROM
      SelEXT rtl,
      DWH_PERFORMANCE.RTL_EMP_JOB_WK jd
    WHERE
      RTL.SK1_EMPLOYEE_ID = JD.SK1_EMPLOYEE_ID(+)
    AND rtl.SK1_JOB_ID    = jd.SK1_JOB_ID(+)
    AND RTL.FIN_YEAR_NO   = JD.FIN_YEAR_NO(+)
    AND RTL.FIN_WEEK_NO   = JD.FIN_WEEK_NO(+)
;
                
g_recs_read         :=SQL%ROWCOUNT;
g_recs_inserted     :=SQL%ROWCOUNT;

COMMIT;

l_text := 'Running GATHER_TABLE_STATS ON TST_EMP_AVAIL_LOC_JOB_WK';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TST_EMP_AVAIL_LOC_JOB_WK',
DEGREE => 8);

--FIN_YEAR_NO", "FIN_WEEK_NO", "SK1_EMPLOYEE_ID", "SK1_JOB_ID
--*****************************************************************************
-- *********************
-- Write final log data
--*****************************************************************************
-- *********************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,
l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,
g_recs_inserted,g_recs_updated,'','');
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,(
'dd mon yyyy hh24:mi:ss'));
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
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,
  l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,
  '','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,
  l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,
  '','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END WH_PRF_S4S_008U_NEW;
