--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_008U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_008U_TAKEON" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     Load EMPLOYEE_LOCATION_WEEK information for Scheduling for Staff(S4S)
  --
  --  Tables:      Input    - RTL_EMP_AVAIL_LOC_JOB_DY
  --               Output   - DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK
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
  g_rec_out RTL_EMP_AVAIL_LOC_JOB_WK%rowtype;
  g_found BOOLEAN;
  g_date  DATE;
  
  g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_008U_TAKEON';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_EMP_AVAIL_LOC_JOB_WK data  EX FOUNDATION';
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
  l_text := 'LOAD OF RTL_EMP_AVAIL_LOC_JOB_WK  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);
--g_date := '7 dec 2014';

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';
    
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK');


    
    
--**************************************************************************************************
-- Delete records from Performance
-- based on employee_id and AVAILABILITY_START_DATE
-- before loading from staging
--**************************************************************************************************

      g_recs_inserted := 0;

      select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_EMP_AVAIL_DY_DEL_LIST;
      
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      g_run_date := trunc(sysdate);

    BEGIN
           delete from DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK
           where (SK1_employee_id,fin_year_no, fin_week_no) in (select distinct SK1_employee_id,fin_year_no, fin_week_no from DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY RTL,
           dim_calendar dc
           WHERE 
           --rtl.last_updated_date = g_date
         --  and  
           dc.calendar_date        = rtl.tran_DATE);
       
            g_recs :=SQL%ROWCOUNT ;
            COMMIT;
            g_recs_deleted := g_recs;
                  
        l_text := 'Deleted from DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    exception
           when no_data_found then
                  l_text := 'No deletions done for DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK ';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
     end;          

   g_recs_inserted  :=0;
   g_recs_deleted := 0;
  INSERT
    /*+ APPEND */
  INTO DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_WK
        WITH seldat AS
                      (SELECT 
                        /*+ FULL(RTL) PARALlEL(RTL,4) */ distinct
                            fin_year_no,
                            fin_week_no,
                           -- MAX(tran_date) maxtran_date,
                            tran_date maxtran_date,
                            sk1_employee_id,
                            sk1_JOB_ID, sk1_location_no
                      FROM dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY rtl,
                         dim_calendar dc
                      WHERE 
             --         rtl.last_updated_date = g_date
              --        AND 
                     dc.calendar_date        = rtl.tran_DATE
                         ) ,
          SELEXT AS
                      (SELECT RTL.SK1_LOCATION_NO,
                              RTL.SK1_EMPLOYEE_ID,
                              RTL.SK1_JOB_ID,
                              FIN_YEAR_NO,
                              FIN_WEEK_NO,
                              cycle_start_date, cycle_end_date,
                              week_number,
                              SUM(NVL(FIXED_ROSTER_hrs,0)) FIXED_ROSTER_hrs_WK,
                              SUM(NVL(FIXED_ROSTER_hrs,0)) / 40 FIXED_ROSTER_FTE_WK
                              --                    SUM(((fixed_roster_end_time - fixed_roster_start_time) * 24 * 60) - meal_break_minutes) / 40 FIXED_ROSTER_FTE_WK
                      FROM dwh_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY rtl,
                        seldat sd
                      WHERE rtl.tran_DATE = sd.maxtran_date -- WAS LEFT OUTER BUT MSADE STRAIGHT JOIN
                      and RTL.SK1_LOCATION_NO = sd.SK1_LOCATION_NO
                        and      RTL.SK1_EMPLOYEE_ID = sd.SK1_EMPLOYEE_ID
                        and      RTL.SK1_JOB_ID = sd.SK1_JOB_ID
                            GROUP BY RTL.SK1_LOCATION_NO,
                                RTL.SK1_EMPLOYEE_ID,
                                RTL.SK1_JOB_ID,
                                FIN_YEAR_NO,
                                FIN_WEEK_NO, cycle_start_date, cycle_end_date, week_number
                      )
  
               SELECT  /*+ FULL(jd) PARALlEL(jd,4) */ DISTINCT rtl.SK1_LOCATION_NO,
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
              FROM SelEXT rtl,
                DWH_PERFORMANCE.RTL_EMP_JOB_WK jd
              WHERE rtl.SK1_EMPLOYEE_ID = jd.SK1_EMPLOYEE_ID(+)
              AND rtl.SK1_JOB_ID        = jd.SK1_JOB_ID(+)
              AND rtl.fin_year_no       = jd.fin_year_no(+)
              AND rtl.fin_week_no       = jd.fin_week_no(+) ;
g_recs_read              :=SQL%ROWCOUNT;
g_recs_inserted          :=SQL%ROWCOUNT;
COMMIT;

    l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_AVAIL_LOC_JOB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_EMP_AVAIL_LOC_JOB_WK', DEGREE => 8);


--FIN_YEAR_NO, FIN_WEEK_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID
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
END WH_PRF_S4S_008U_TAKEON;
