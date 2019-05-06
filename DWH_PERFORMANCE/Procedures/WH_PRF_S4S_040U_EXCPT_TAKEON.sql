--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_040U_EXCPT_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_040U_EXCPT_TAKEON" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  FOR DATA TAKEON ONLY -- but with left outer joins 
  --                           to be used in exception reporting to highlight missing data
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     Load EMPLOYEE STATUS DY FACT information for Scheduling for Staff(S4S)
  --                NB> effective_start_date = MONDAY and effective_end_date = any day in week
  --
  --  Tables:      Input    - dwh_foundation.FND_S4S_EMP_LOC_STATUS
  --               Output   - DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
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
  g_rec_out RTL_EMP_LOC_STATUS_DY_X%rowtype;
  g_found                BOOLEAN;
  g_date                 DATE;
  G_THIS_WEEK_START_DATE DATE;
  g_fin_days             NUMBER;
  g_eff_end_date         DATE;
  g_run_date             DATE   := TRUNC(sysdate);
  g_run_seq_no           NUMBER := 0;
  g_recs                 NUMBER := 0;
  g_EMP_START            VARCHAR2(11);
  g_EMP_END              VARCHAR2(11);
  g_recs_deleted         INTEGER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_040U_EXCPT_TAKEON';
--  l_module_name sys_dwh_errlog.log_procedure_name%type := ' WH_PRF_S4S_040U_TAKEON';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_EMP_LOC_STATUS_DY_X data  EX FOUNDATION';
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
  l_text := 'LOAD OF RTL_EMP_LOC_STATUS_DY_X  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
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
  INTO g_eff_end_date
  FROM DIM_CALENDAR
  WHERE CALENDAR_DATE = g_date + 20;
  l_text             := 'Derived g_eff_end_date - '||g_eff_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_loc_status', DEGREE => 8);
  --**************************************************************************************************
  G_EMP_START := '0';
  G_EMP_END   := '7017267';
  l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  INSERT /*+ APPEND */
  INTO DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
WITH SELEXT1 AS
  ( SELECT DISTINCT
    /*+ full(flr) full(de) full(dl) */
    de.SK1_EMPLOYEE_ID ,
    dl.SK1_LOCATION_NO ,
    flr.EMPLOYEE_STATUS ,
    flr.EMPLOYEE_WORKSTATUS ,
    flr.EFFECTIVE_START_DATE ,
    flr.EFFECTIVE_END_DATE,
    dc.calendar_date tran_date
  FROM FND_S4S_EMP_LOC_STATUS flr
  JOIN dim_employee DE
  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN DIM_LOCATION DL
  ON DL.LOCATION_NO = FLR.LOCATION_NO
  JOIN DIM_CALENDAR DC
  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
  WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
 -- AND FLR.EMPLOYEE_ID NOT IN ('1002901' ,'7008314' ,'7020901' ,'7063764' ,'7074043' ,'7089367' ,'7090798' )
    -- flr.last_updated_date = g_date
    --  and
    --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
  ),
  selext2 AS
  (SELECT DISTINCT SK1_EMPLOYEE_ID ,
    SK1_LOCATION_NO ,
    EMPLOYEE_STATUS ,
    tran_date,
    EMPLOYEE_WORKSTATUS ,
    EFFECTIVE_START_DATE ,
    EFFECTIVE_END_DATE,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN SE1.effective_START_DATE
      ELSE NULL
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN g_eff_end_date
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN se1.effective_end_date - 1
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
 -- WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
  )
SELECT DISTINCT SK1_LOCATION_NO ,
  SK1_EMPLOYEE_ID ,
  TRAN_DATE ,
  EMPLOYEE_STATUS ,
  EMPLOYEE_WORKSTATUS ,
  EFFECTIVE_START_DATE ,
  EFFECTIVE_END_DATE ,
  g_date LAST_UPDATED_DATE,
  derive_start_date, derive_end_date
FROM selext2 se2
--WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date ;
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --**************************************************************************************************
G_EMP_START := '7017268';
G_EMP_END   := '7035583';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
WITH SELEXT1 AS
  ( SELECT DISTINCT
    /*+ full(flr) full(de) full(dl) */
    de.SK1_EMPLOYEE_ID ,
    dl.SK1_LOCATION_NO ,
    flr.EMPLOYEE_STATUS ,
    flr.EMPLOYEE_WORKSTATUS ,
    flr.EFFECTIVE_START_DATE ,
    flr.EFFECTIVE_END_DATE,
    dc.calendar_date tran_date
  FROM FND_S4S_EMP_LOC_STATUS flr
  JOIN dim_employee DE
  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN DIM_LOCATION DL
  ON DL.LOCATION_NO = FLR.LOCATION_NO
  JOIN DIM_CALENDAR DC
  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
  WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
--  AND FLR.EMPLOYEE_ID NOT IN ('1002901' ,'7008314' ,'7020901' ,'7063764' ,'7074043' ,'7089367' ,'7090798' )
    -- flr.last_updated_date = g_date
    --  and
    --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
  ),
  selext2 AS
  (SELECT DISTINCT SK1_EMPLOYEE_ID ,
    SK1_LOCATION_NO ,
    EMPLOYEE_STATUS ,
    tran_date,
    EMPLOYEE_WORKSTATUS ,
    EFFECTIVE_START_DATE ,
    EFFECTIVE_END_DATE,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN SE1.effective_START_DATE
      ELSE NULL
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN g_eff_end_date
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN se1.effective_end_date - 1
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
 -- WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
  )
SELECT DISTINCT SK1_LOCATION_NO ,
  SK1_EMPLOYEE_ID ,
  TRAN_DATE ,
  EMPLOYEE_STATUS ,
  EMPLOYEE_WORKSTATUS ,
  EFFECTIVE_START_DATE ,
  EFFECTIVE_END_DATE ,
  g_date LAST_UPDATED_DATE,
  derive_start_date, derive_end_date
FROM selext2 se2
--WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date ;
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --**************************************************************************************************
G_EMP_START := '7035584';
G_EMP_END   := '7053466';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
WITH SELEXT1 AS
  ( SELECT DISTINCT
    /*+ full(flr) full(de) full(dl) */
    de.SK1_EMPLOYEE_ID ,
    dl.SK1_LOCATION_NO ,
    flr.EMPLOYEE_STATUS ,
    flr.EMPLOYEE_WORKSTATUS ,
    flr.EFFECTIVE_START_DATE ,
    flr.EFFECTIVE_END_DATE,
    dc.calendar_date tran_date
  FROM FND_S4S_EMP_LOC_STATUS flr
  JOIN dim_employee DE
  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN DIM_LOCATION DL
  ON DL.LOCATION_NO = FLR.LOCATION_NO
  JOIN DIM_CALENDAR DC
  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
  WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
 -- AND FLR.EMPLOYEE_ID NOT IN ('1002901' ,'7008314' ,'7020901' ,'7063764' ,'7074043' ,'7089367' ,'7090798' )
    -- flr.last_updated_date = g_date
    --  and
    --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
  ),
  selext2 AS
  (SELECT DISTINCT SK1_EMPLOYEE_ID ,
    SK1_LOCATION_NO ,
    EMPLOYEE_STATUS ,
    tran_date,
    EMPLOYEE_WORKSTATUS ,
    EFFECTIVE_START_DATE ,
    EFFECTIVE_END_DATE,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN SE1.effective_START_DATE
      ELSE NULL
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN g_eff_end_date
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN se1.effective_end_date - 1
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
 -- WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
  )
SELECT DISTINCT SK1_LOCATION_NO ,
  SK1_EMPLOYEE_ID ,
  TRAN_DATE ,
  EMPLOYEE_STATUS ,
  EMPLOYEE_WORKSTATUS ,
  EFFECTIVE_START_DATE ,
  EFFECTIVE_END_DATE ,
  g_date LAST_UPDATED_DATE,
  derive_start_date, derive_end_date
FROM selext2 se2
--WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date 
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --**************************************************************************************************
G_EMP_START := '7053467';
G_EMP_END   := '7059389';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
WITH SELEXT1 AS
  ( SELECT DISTINCT
    /*+ full(flr) full(de) full(dl) */
    de.SK1_EMPLOYEE_ID ,
    dl.SK1_LOCATION_NO ,
    flr.EMPLOYEE_STATUS ,
    flr.EMPLOYEE_WORKSTATUS ,
    flr.EFFECTIVE_START_DATE ,
    flr.EFFECTIVE_END_DATE,
    dc.calendar_date tran_date
  FROM FND_S4S_EMP_LOC_STATUS flr
  JOIN dim_employee DE
  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN DIM_LOCATION DL
  ON DL.LOCATION_NO = FLR.LOCATION_NO
  JOIN DIM_CALENDAR DC
  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
  WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
--  AND FLR.EMPLOYEE_ID NOT IN ('1002901' ,'7008314' ,'7020901' ,'7063764' ,'7074043' ,'7089367' ,'7090798' )
    -- flr.last_updated_date = g_date
    --  and
    --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
  ),
  selext2 AS
  (SELECT DISTINCT SK1_EMPLOYEE_ID ,
    SK1_LOCATION_NO ,
    EMPLOYEE_STATUS ,
    tran_date,
    EMPLOYEE_WORKSTATUS ,
    EFFECTIVE_START_DATE ,
    EFFECTIVE_END_DATE,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN SE1.effective_START_DATE
      ELSE NULL
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN g_eff_end_date
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN se1.effective_end_date - 1
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
 -- WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
  )
SELECT DISTINCT SK1_LOCATION_NO ,
  SK1_EMPLOYEE_ID ,
  TRAN_DATE ,
  EMPLOYEE_STATUS ,
  EMPLOYEE_WORKSTATUS ,
  EFFECTIVE_START_DATE ,
  EFFECTIVE_END_DATE ,
  g_date LAST_UPDATED_DATE,
  derive_start_date, derive_end_date
FROM selext2 se2
--WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date 
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --**************************************************************************************************
G_EMP_START := '7059390';
G_EMP_END   := '999999999';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY_X
WITH SELEXT1 AS
  ( SELECT DISTINCT
    /*+ full(flr) full(de) full(dl) */
    de.SK1_EMPLOYEE_ID ,
    dl.SK1_LOCATION_NO ,
    flr.EMPLOYEE_STATUS ,
    flr.EMPLOYEE_WORKSTATUS ,
    flr.EFFECTIVE_START_DATE ,
    flr.EFFECTIVE_END_DATE,
    dc.calendar_date tran_date
  FROM FND_S4S_EMP_LOC_STATUS flr
  JOIN dim_employee DE
  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
  JOIN DIM_LOCATION DL
  ON DL.LOCATION_NO = FLR.LOCATION_NO
  JOIN DIM_CALENDAR DC
  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
  WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
 -- AND FLR.EMPLOYEE_ID NOT IN ('1002901' ,'7008314' ,'7020901' ,'7063764' ,'7074043' ,'7089367' ,'7090798' )
    -- flr.last_updated_date = g_date
    --  and
    --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
  ),
  selext2 AS
  (SELECT DISTINCT SK1_EMPLOYEE_ID ,
    SK1_LOCATION_NO ,
    EMPLOYEE_STATUS ,
    tran_date,
    EMPLOYEE_WORKSTATUS ,
    EFFECTIVE_START_DATE ,
    EFFECTIVE_END_DATE,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN SE1.effective_START_DATE
      ELSE NULL
        --SE1.availability_start_DATE - 1
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')
      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NULL
      THEN g_eff_end_date
      WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')
      AND se1.effective_end_date IS NOT NULL
      THEN se1.effective_end_date - 1
      ELSE NULL
        --SE1.availability_END_DATE - 1
    END) derive_end_date
  FROM selext1 SE1
 -- WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
  )
SELECT DISTINCT SK1_LOCATION_NO ,
  SK1_EMPLOYEE_ID ,
  TRAN_DATE ,
  EMPLOYEE_STATUS ,
  EMPLOYEE_WORKSTATUS ,
  EFFECTIVE_START_DATE ,
  EFFECTIVE_END_DATE ,
  g_date LAST_UPDATED_DATE,
  derive_start_date, derive_end_date
FROM selext2 se2
--WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date ;
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--****************************************************************************

l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_LOC_STATUS_DY_X';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_LOC_STATUS_DY_X', DEGREE => 8);

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
END WH_PRF_S4S_040U_EXCPT_TAKEON;
