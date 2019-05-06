--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_005U_EXCPT_RVS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_005U_EXCPT_RVS" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  FOR DATA TAKEON ONLY
  --**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE JOB DY FACT information for Scheduling for Staff(S4S)
--                NB> job_start_date = MONDAY and job_end_date = SUNDAY
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_JOB
--               Output   - DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
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
  g_rec_out RTL_EMP_JOB_DY_X_rvs%rowtype;
  g_found                BOOLEAN;
  g_date                 DATE;
  G_THIS_WEEK_START_DATE DATE;
  g_fin_days             NUMBER;
  g_JOB_end_date         DATE;
  g_run_date             DATE   := TRUNC(sysdate);
  g_run_seq_no           NUMBER := 0;
  g_recs                 NUMBER := 0;
  g_EMP_START            VARCHAR2(11);
  g_EMP_END              VARCHAR2(11);
  g_recs_deleted         INTEGER := 0;
  l_message sys_dwh_errlog.log_text%type;
--  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_005U';
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_005U_EXCPT_RVS';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_EMP_JOB_DY_X_rvs data  EX FOUNDATION';
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
  l_text := 'LOAD OF RTL_EMP_JOB_DY_X_rvs  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
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
  INTO g_JOB_end_date
  FROM DIM_CALENDAR
  WHERE CALENDAR_DATE = g_date + 20;
  l_text             := 'Derived g_JOB_end_date - '||g_JOB_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_JOB', DEGREE => 8);
  --**************************************************************************************************
  G_EMP_START := '0';
  G_EMP_END   := '7017267';
  l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  INSERT RTL_EMP_JOB_DY_X_rvs APPEND */
  INTO DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
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
            LEFT OUTER JOIN dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            LEFT OUTER JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            LEFT OUTER JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            LEFT OUTER JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            LEFT OUTER JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
                    WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
       --    where flr.last_updated_date = g_date
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
   --WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT RTL_EMP_JOB_DY_X_rvs FULL(RTL)   PARALLEL(RTL,4) */
DISTINCT
         SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,G_DATE LAST_UPDATED_DATE
,derive_start_date 
, derive_end_date
, EMPLOYEE_STATUS
            FROM SELEXT2 SE2
 --            WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
ORDER BY TRAN_DATE
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
INSERT RTL_EMP_JOB_DY_X_rvs APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
WITH
SELEXT1 AS (
           SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,4) , PARALLEL(EL,4) */
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
            LEFT OUTER JOIN dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            LEFT OUTER JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            LEFT OUTER JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            LEFT OUTER JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            LEFT OUTER JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
                    WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
       --    where flr.last_updated_date = g_date
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
   --WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT RTL_EMP_JOB_DY_X_rvs FULL(RTL)   PARALLEL(RTL,4) */
DISTINCT
         SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,G_DATE LAST_UPDATED_DATE
,derive_start_date 
, derive_end_date
, EMPLOYEE_STATUS
            FROM SELEXT2 SE2
      --       WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
ORDER BY TRAN_DATE
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
*/
  --**************************************************************************************************
G_EMP_START := '7035584';
G_EMP_END   := '7053466';
l_text      := 'HISTORY TAKE ON - EMP SELECTION = '||G_EMP_START||'-'||G_EMP_END;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
INSERT RTL_EMP_JOB_DY_X_rvs APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
WITH
SELEXT1 AS (
           SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,4) , PARALLEL(EL,4) */
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
            LEFT OUTER JOIN dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            LEFT OUTER JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            LEFT OUTER JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            LEFT OUTER JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            LEFT OUTER JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
                    WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
       --    where flr.last_updated_date = g_date
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
   --WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT RTL_EMP_JOB_DY_X_rvs FULL(RTL)   PARALLEL(RTL,4) */
DISTINCT
         SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,G_DATE LAST_UPDATED_DATE
,derive_start_date 
, derive_end_date
, EMPLOYEE_STATUS
            FROM SELEXT2 SE2
     --        WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
ORDER BY TRAN_DATE
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
INSERT RTL_EMP_JOB_DY_X_rvs APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
WITH
SELEXT1 AS (
           SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,4) , PARALLEL(EL,4) */
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
            LEFT OUTER JOIN dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            LEFT OUTER JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            LEFT OUTER JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            LEFT OUTER JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            LEFT OUTER JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
                    WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
       --    where flr.last_updated_date = g_date
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
   --WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT RTL_EMP_JOB_DY_X_rvs FULL(RTL)   PARALLEL(RTL,4) */
DISTINCT
         SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,G_DATE LAST_UPDATED_DATE
,derive_start_date 
, derive_end_date
, EMPLOYEE_STATUS
            FROM SELEXT2 SE2
   --          WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
ORDER BY TRAN_DATE
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
INSERT RTL_EMP_JOB_DY_X_rvs APPEND */
INTO DWH_PERFORMANCE.RTL_EMP_JOB_DY_X_rvs
WITH
SELEXT1 AS (
           SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,4) , PARALLEL(EL,4) */
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
            LEFT OUTER JOIN dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            LEFT OUTER JOIN DIM_JOB DJ
                  ON DJ.JOB_ID = FLR.JOB_ID
            LEFT OUTER JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
            LEFT OUTER JOIN RTL_EMP_LOC_STATUS_DY EL
                  ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                  AND EL.TRAN_DATE = DC.CALENDAR_DATE
            LEFT OUTER JOIN DIM_PAY_POLICY DP
                  ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
                    WHERE FLR.EMPLOYEE_ID BETWEEN g_EMP_START AND g_EMP_END
       --    where flr.last_updated_date = g_date
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
   --WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
SELECT RTL_EMP_JOB_DY_X_rvs FULL(RTL)   PARALLEL(RTL,4) */
DISTINCT
         SK1_EMPLOYEE_ID
,SK1_JOB_ID
,TRAN_DATE
,JOB_START_DATE
,JOB_END_DATE
,EMPLOYEE_RATE
,SK1_PAYPOLICY_ID
,G_DATE LAST_UPDATED_DATE
,derive_start_date 
, derive_end_date
, EMPLOYEE_STATUS
            FROM SELEXT2 SE2
      --       WHERE SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date
ORDER BY TRAN_DATE
;
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'recs inserted='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
L_TEXT := '---------------------------**----------------------';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--****************************************************************************

l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY_X_rvs';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_JOB_DY_X_rvs', DEGREE => 8);

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

END WH_PRF_S4S_005U_EXCPT_RVS;
