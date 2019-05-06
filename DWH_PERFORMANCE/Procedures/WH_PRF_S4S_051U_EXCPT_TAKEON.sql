--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_051U_EXCPT_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_051U_EXCPT_TAKEON" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load Employee Schedule information for Scheduling for Staff(S4S)
--
--
--                nb. we are working with SHIFT_CLOCK_IN and SHIFT_CLOCK_OUT
--                      which consist of date+time.
--                    this must be remmebered when doing joins to other tables as they
--                        use date only.
--
--               nb. FULL OUTER JOIN done between sumed(dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY)
--                              and RTL_ABSENCE_EMP_WK
--                               is because someone can be anbent for a week 
--                                   but have no schedule
--
--
--  Tables:      Input    - dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY
--               Output   - DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK_X  
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
g_rec_out            dwh_performance.RTL_SCH_LOC_EMP_JB_WK_X%rowtype;
g_found              boolean;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_constr_end_date  date;


g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_051U_EXCPT_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_SCH_LOC_EMP_JB_WK_X data EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.RTL_SCH_LOC_EMP_JB_WK_X%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.RTL_SCH_LOC_EMP_JB_WK_X%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        TBL_ARRAY_U;
a_empty_set_i       tbl_array_i;
a_empty_set_u       TBL_ARRAY_U;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_SCH_LOC_EMP_JB_WK  EX FOUNDATION STARTED '||
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



    l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_LOC_STATUS_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_EMP_LOC_STATUS_DY', DEGREE => 8);
                                   
      
 

      INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK_X
WITH selext1a AS
  (SELECT
          /*+ full(flr) parallel(flr,6) */
          FLR.SK1_JOB_ID ,
          FLR.SK1_LOCATION_NO ,
          FLR.sk1_employee_ID ,
          DC.fin_year_no ,
          DC.fin_week_no ,
          EMPLOYEE_RATE,
          SUM(NVL(NETT_SCHEDULED_HOURS,0)) total_NETT_SCHEDULED_HOURS
  FROM dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY flr
  LEFT OUTER JOIN dwh_PERFORMANCE.DIM_CALENDAR DC
         ON TRUNC(flr.shift_clock_in) = dc.calendar_date
  LEFT OUTER JOIN DWH_PERFORMANCE.RTL_EMP_JOB_WK RE
        ON RE.SK1_JOB_ID       = flr.SK1_JOB_ID
        AND RE.SK1_EMPLOYEE_ID = flr.SK1_EMPLOYEE_ID
        AND RE.FIN_YEAR_NO     = dc.FIN_YEAR_NO
        AND RE.FIN_WEEK_NO     = dc.FIN_WEEK_NO
     --      WHERE  flr.last_updated_date = g_date
  GROUP BY FLR.SK1_JOB_ID ,
            FLR.SK1_LOCATION_NO ,
            FLR.sk1_employee_ID ,
            DC.fin_year_no ,
            DC.fin_week_no,
            EMPLOYEE_RATE
  ) ,
  selext1b AS
          (SELECT DISTINCT ae.SK1_EMPLOYEE_ID ,
                        ae.FIN_YEAR_NO ,
                        ae.FIN_WEEK_NO ,
                        re.sk1_job_id ,
                        el.sk1_location_no
                        --   ,dai.ABSENCE_TYPE_ID
                        --      ,dli.LEAVE_TYPE_ID
                        --      ,Ae.aBSENCE_HOURS
                        ,
                        CASE
                          WHEN dli.LEAVE_TYPE_ID  = 1000185   AND DAI.ABSENCE_TYPE_ID = 1 THEN ABSENCE_HOURS
                          ELSE 0
                        END TRAINING_HOURS ,
                        employee_rate,
                        CASE
                          WHEN DAI.ABSENCE_TYPE_ID = 1 AND dli.LEAVE_TYPE_ID   IN( 1000228,1000173,1000176,1000177 ,1000178,1000229,1000230,1000175 ,1000183,1000194,1000184)
                                                              THEN absence_hours       * LEAVE_TYPE_PERCENT_OF_HOURS / 100
                          ELSE 0
                        END absence_hours_value                        
          FROM DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK AE
          LEFT OUTER JOIN DIM_LEAVE_TYPE DLI
          ON AE.SK1_LEAVE_TYPE_ID = DLI.SK1_LEAVE_TYPE_ID
          LEFT OUTER JOIN DIM_ABSENCE_TYPE DAI
          ON AE.SK1_ABSENCE_TYPE_ID = DAI.SK1_ABSENCE_TYPE_ID
          LEFT OUTER JOIN DWH_PERFORMANCE.RTL_EMP_JOB_WK RE
          ON RE.SK1_EMPLOYEE_ID = ae.SK1_EMPLOYEE_ID
          AND RE.FIN_YEAR_NO    = ae.FIN_YEAR_NO
          AND RE.FIN_WEEK_NO    = ae.FIN_WEEK_NO
          LEFT OUTER JOIN DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_WK el
          ON el.SK1_EMPLOYEE_ID      = ae.SK1_EMPLOYEE_ID
          AND el.FIN_YEAR_NO         = ae.FIN_YEAR_NO
          AND el.FIN_WEEK_NO         = ae.FIN_WEEK_NO
   --       WHERE AE.last_updated_date = g_date
          ),
  selext1 AS
          (SELECT NVL(se1a.SK1_JOB_ID, se1b.SK1_JOB_ID) SK1_JOB_ID,
                    NVL(se1a.SK1_LOCATION_NO, se1b.SK1_LOCATION_NO) SK1_LOCATION_NO,
                    NVL(se1a.sk1_employee_ID , se1b.sk1_employee_ID) sk1_employee_ID,
                    NVL(se1a.fin_year_no , se1b.fin_year_no) fin_year_no ,
                    NVL(se1a.fin_week_no , se1b.fin_week_no) fin_week_no ,
                    NVL(se1a.total_NETT_SCHEDULED_HOURS, NULL) total_NETT_SCHEDULED_HOURS,
                    --      nvl(se1b.ABSENCE_TYPE_ID, null) ABSENCE_TYPE_ID,
                    --       nvl(se1b.LEAVE_TYPE_ID, null) LEAVE_TYPE_ID,
                    --         nvl(se1b.ABSENCE_HOURS, null) ABSENCE_HOURS,
                    NVL(se1b.absence_hours_value, NULL) absence_hours_value,
                    NVL(se1b.TRAINING_HOURS, NULL) TRAINING_HOURS,
                    --        nvl(se1b.LEAVE_TYPE_PERCENT_OF_HOURS, null)  LEAVE_TYPE_PERCENT_OF_HOURS,
                    NVL(se1a.employee_rate, se1b.employee_rate) employee_rate
          FROM selext1a se1a
          FULL OUTER  JOIN selext1b se1b
                ON se1b.SK1_EMPLOYEE_ID = SE1a.SK1_EMPLOYEE_ID
                AND se1b.FIN_YEAR_NO    = SE1a.FIN_YEAR_NO
                AND se1b.FIN_WEEK_NO    = SE1a.FIN_WEEK_NO
          ),
  selext3 AS
          (SELECT SE1.SK1_JOB_ID ,
                  SE1.SK1_LOCATION_NO ,
                  SE1.SK1_EMPLOYEE_ID ,
                  SE1.FIN_YEAR_NO ,
                  SE1.FIN_WEEK_NO ,
                  NVL(SE1.TOTAL_NETT_SCHEDULED_HOURS,0) TOTAL_NETT_SCHEDULED_HOURS,
                  SUM(NVL(TRAINING_HOURS,0)) TRAINING_HOURS ,
                  SUM(NVL(absence_hours_value,0)) absence_hours_value,
                  --      SUM(NVL(LEAVE_TYPE_PERCENT_OF_HOURS,0)) LEAVE_TYPE_PERCENT_OF_HOURS ,
                  MAX_HRS_PER_WK ,
                  EMPLOYEE_RATE
          FROM SELEXT1 SE1
          LEFT OUTER JOIN DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK EC
            -- must be changed back to straight LEFT OUTER JOIN once data in --LEFT OUTER JOIN DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK EC
                ON EC.SK1_JOB_ID       = se1.SK1_JOB_ID
               AND EC.SK1_EMPLOYEE_ID = se1.SK1_EMPLOYEE_ID
               AND EC.FIN_YEAR_NO     = se1.FIN_YEAR_NO
               AND EC.FIN_WEEK_NO     = se1.FIN_WEEK_NO
          GROUP BY sE1.SK1_JOB_ID ,
                    SE1.SK1_LOCATION_NO ,
                    SE1.SK1_EMPLOYEE_ID ,
                    SE1.FIN_YEAR_NO ,
                    SE1.FIN_WEEK_NO ,
                    NVL(SE1.TOTAL_NETT_SCHEDULED_HOURS,0),
                    MAX_HRS_PER_WK ,
                    EMPLOYEE_RATE
          ORDER BY SE1.SK1_EMPLOYEE_ID
          ),
  selext4 AS
                ( SELECT DISTINCT SK1_JOB_ID ,
                          SK1_LOCATION_NO ,
                          SK1_EMPLOYEE_ID ,
                          FIN_YEAR_NO ,
                          FIN_WEEK_NO ,
                          employee_rate ,
                          training_hours,
                          MAX_HRS_PER_WK,
                          absence_hours_value,
                          CASE
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS >= MAX_HRS_PER_WK
                            THEN TOTAL_NETT_SCHEDULED_HOURS+ TRAINING_HOURS
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS+absence_hours_value < MAX_HRS_PER_WK
                            THEN TOTAL_NETT_SCHEDULED_HOURS+ TRAINING_HOURS+absence_hours_value
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS+absence_hours_value >= MAX_HRS_PER_WK
                            THEN MAX_HRS_PER_WK
                            ELSE TOTAL_NETT_SCHEDULED_HOURS
                          END NETT_SCHEDULED_HOURS_WK,
                          TOTAL_NETT_SCHEDULED_HOURS
                FROM SELEXT3
                )
SELECT
          /*+ full(rtl) parallel(rtl,6) */
          DISTINCT se4.SK1_LOCATION_NO ,
                    se4.SK1_employee_ID ,
          se4.SK1_JOB_ID ,
          se4.FIN_YEAR_NO ,
          se4.FIN_WEEK_NO ,
          se4.TOTAL_NETT_SCHEDULED_HOURS,
          se4.nett_scheduled_hours_WK ,
          se4.nett_scheduled_hours_WK / 40 SCHEDULED_FTE_WK ,
          se4.nett_scheduled_hours_WK * se4.EMPLOYEE_RATE SCHEDULED_COST_WK ,
          G_DATE LAST_UPDATED_DATE,
          training_hours, absence_hours_value, MAX_HRS_PER_WK
FROM selext4 se4;
   g_recs_read:=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT;

   commit;



   l_text := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_WK_X';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_SCH_LOC_EMP_JB_WK_X', DEGREE => 8);

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
       
       
END WH_PRF_S4S_051U_EXCPT_TAKEON;
