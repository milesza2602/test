--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004U" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
   --  Date:        March 2019
   --  Author:      Shuaib Salie and Lisa Kriel (original Wendy Lyttle)
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
--
   -- Comment  from FND:
-- 9 MARCH 2015 -- When a change occurs at source, the entire employee history is resent.
--              --   When it comes to running the 'explode' from FND to PRF_DAY, 
--                        we do it for all records 
--                 as the delete off the PRD_DAY table will take forever and be a huge number of records.
--              --   This is because we would need to process not only changed employess(coming from STG) 
--                but also any employees with availability_end_date = null on FND.
  --               Note that eventhough we will only use a subset of this data (next proc to run wh_prf_s4s_4u)
   --                we generate all days within each period
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
   --  Tables:      Input    - DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3
   --               Output   - RTL_EMP_AVAIL_LOC_JOB_DY 
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
   g_forall_limit        INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read           INTEGER := 0;
   g_recs_inserted       INTEGER := 0;
   g_recs_updated        INTEGER := 0;
   g_count               NUMBER  := 0;  
   g_run_seq_no          NUMBER  := 0;
   g_recs                NUMBER  := 0;  
   g_SUB                 NUMBER  := 0;
   g_run_date            date    := trunc(sysdate);
   g_rec_out             RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
   g_found               BOOLEAN;
   g_date                DATE;    
   g_end_date            DATE;
   g_name                varchar2(40);
   g_loop_fin_year_no    pls_integer        :=  0;
   g_loop_fin_month_no   pls_integer        :=  0;
   g_sub                 pls_integer        :=  0;
   g_loop_cnt            pls_integer        :=  6; -- Number of partitions (months) to be truncated/replaced (revert to 6)
   g_degrees             pls_integer        :=  4;
   g_loop_start_date     date;
   g_MIN_start_date      date;
   g_loop_end_date       date;
   g_max_end_date        date;
   g_subpart_type        dba_part_tables.SUBPARTITIONING_TYPE%type; 
   g_subpart_column_name dba_subpart_key_columns.column_name%type;

   l_message             sys_dwh_errlog.log_text%TYPE;
   l_procedure_name      sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_S4S_004U';
   l_table_name          all_tables.table_name%type                := 'RTL_EMP_AVAIL_LOC_JOB_DY';
   l_table_owner         all_tables.owner%type                     := 'DWH_PERFORMANCE';
   l_name                sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_md;
   l_system_name         sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_prf;
   l_script_name         sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_prf_md;
   l_text                sys_dwh_log.log_text%TYPE;
   l_description         sys_dwh_log_summary.log_description%TYPE  := 'LOAD THE '||l_table_name||' data  EX FOUNDATION';
   l_process_type        sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;

   -- For output arrays into bulk load forall statements --
     TYPE year_sub_rec is RECORD (
        fin_year_no    number,
        fin_sub_no     number,
        start_date     date,
        end_date       date 
  );
   TYPE DateCurTyp IS REF CURSOR;
   date_cv DateCurTyp; 

   TYPE tbl_loop_list IS TABLE OF year_sub_rec INDEX BY BINARY_INTEGER;   
   TYPE tbl_array_i IS TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
   TYPE tbl_array_u IS TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;

   date_list          tbl_loop_list;
   a_tbl_insert       tbl_array_i;
   a_tbl_update       tbl_array_u;
   a_empty_set_i      tbl_array_i;
   a_empty_set_u      tbl_array_u;
   a_count            INTEGER       := 0;
   a_count_i          INTEGER       := 0;
   a_count_u          INTEGER       := 0;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

  l_text := 'Insert into '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 -- insert /*+ append */  into DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U
   INSERT /*+ APPEND parallel (X,g_degrees)*/ into dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY X
    with selexta
     as
     (SELECT
        /*+ FULL(FC) FULL(FLR) parallel(flr,g_degrees) parallel(fc,g_degrees)  */
     --   distinct
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
      JOIN DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3 fc
            ON fc.employee_id             = flr.employee_id
            AND fc.availability_start_date = flr.availability_start_date
            AND fc.NO_OF_WEEKS             = FLR.NO_OF_WEEKS
            AND fc.week_number             = FLR.WEEK_NUMBER
      join dwh_hr_performance.dim_employee de
      on DE.EMPLOYEE_ID = FC.EMPLOYEE_ID
      --where  fc.this_week_start_date >= g_min_start_date --date filter     
      where  fc.this_week_start_date between g_MIN_start_date and g_max_end_date
      --ORDER BY de.sk1_employee_id,fc.CYCLE_START_date --Now
      )
      ,
      selext1
      as   ( select  
               /*+ FULL(sea) FULL(ej) FULL(el) parallel(sea,g_degrees) parallel(ej,g_degrees) parallel(el,g_degrees) */
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
                 dc.fin_year_no,
                 dc.fin_month_no,
                 ((( sea.fixed_roster_end_time - sea.fixed_roster_start_time) * 24 * 60) - sea.meal_break_minutes) / 60 FIXED_ROSTER_HRS
              FROM selexta sea
              inner JOIN DIM_CALENDAR DC
                   ON   dc.this_week_start_date = sea.this_week_start_date
                   AND dc.fin_day_no           = sea.DAY_OF_WEEK
              INNER JOIN RTL_EMP_JOB_DY EJ
                  ON Ej.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
                  AND Ej.TRAN_DATE      = dc.CALENDAR_DATE
              INNER JOIN RTL_EMP_LOC_STATUS_DY El
                    ON El.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
                    and EL.TRAN_DATE      = DC.CALENDAR_DATE
             where  (dc.calendar_date between SEA.cycle_start_date and g_end_date 

                                            )
                          )

      ,
      selext2 AS
      (
      SELECT DISTINCT se1.SK1_LOCATION_NO ,
                        se1.SK1_EMPLOYEE_ID ,
                        se1.SK1_JOB_ID ,
                        se1.FIN_YEAR_NO ,
                        se1.FIN_MONTH_NO ,
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

    SELECT /*+ full(se2) */ --DISTINCT 
                    se2.SK1_LOCATION_NO ,
                    se2.SK1_JOB_ID ,
                    se2.SK1_EMPLOYEE_ID ,
                    se2.FIN_YEAR_NO ,
                    se2.FIN_MONTH_NO ,
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
--    ORDER BY se2.SK1_LOCATION_NO
--            ,se2.SK1_JOB_ID
--            ,SE2.SK1_EMPLOYEE_ID
--            ,se2.TRAN_DATE
            ;                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        L_TEXT := L_TABLE_NAME||' : recs = '||g_recs;
        --L_TEXT := 'TEMP_S4S_AVAIL_004U : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
-- /* --------------------------------- LK START       
-- --!!! should this be a merge !!!
--        insert /*+ append */ into DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY
--        with seldup as (select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
--        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--        minus
--        select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
--        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--        having count(*) > 1)
--        select tmp.* from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U tmp, seldup sd
--        where tmp.sk1_employee_id = sd.sk1_employee_id
--        and tmp.sK1_LOCATION_NO = sd.sK1_LOCATION_NO
--        and tmp.SK1_JOB_ID = sd.SK1_JOB_ID
--        and tmp.TRAN_DATE = sd.tran_date;
--        g_recs :=SQL%ROWCOUNT ;
--        COMMIT;
--
--        L_TEXT := l_table_name||' : recs = '||g_recs;
--        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
------
----- these records delete dbecause employee has overlapping period
------
--
--        insert /*+ append */ into dwh_performance.s4s_avail_004u_error_table
--        select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE, g_date
--        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
--        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--        having count(*) > 1;
--        g_recs :=SQL%ROWCOUNT ;
--        COMMIT;
--
--        L_TEXT := 's4s_avail_004u_error_table : recs = '||g_recs;
--        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--/* --------------------------------- LK end 

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

 --**************************************************************************************************
 --                    M  a  i  n    p  r  o  c  e  s  s
 --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;


  p_success := false;
  dwh_performance.dwh_s4s.write_initial_log_data(l_procedure_name,l_description);

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  -- derivation of end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
    SELECT trunc(THIS_MN_END_DATE) into g_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = trunc(g_date) + 42;

  l_text             := 'Derived g_end_date - '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session enable parallel dml';
  execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

  l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats (L_table_owner,'TEMP_S4S_LOC_EMP_DY_PART3', DEGREE => g_degrees);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Disabling of FK constraints
  --**************************************************************************************************
  DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);

  --*************************************************************************************************
  -- Truncate existing data one partition at a time
  --**************************************************************************************************       
     OPEN date_cv FOR 
         select distinct fin_year_no, fin_month_no, this_mn_start_date, fin_month_end_date 
           from dim_calendar_wk
          where this_mn_start_date < g_end_date
           -- and THIS_MN_END_DATE > add_months(g_date, -g_loop_cnt)
            and THIS_MN_END_DATE > add_months(sysdate, -g_loop_cnt)           
       order by fin_month_end_date desc;
       FETCH date_cv BULK COLLECT INTO date_list;
    CLOSE date_cv;
    begin
       g_max_end_date := date_list(1).end_date;
       for g_sub in 1 .. date_list.count
         loop 
           g_loop_start_date  := date_list(g_sub).start_date;
           g_loop_fin_year_no := date_list(g_sub).fin_year_no; 
           g_loop_fin_month_no := date_list(g_sub).fin_sub_no;

            -- truncate subpartition
             DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                           l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_MONTH_NO);
             
             -- Replace with new data
--b_insert;  
             
        end loop;   
       g_min_start_date := g_loop_start_date;
--  l_text := 'Truncating TEMP_S4S_AVAIL_004U';
--  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--  execute immediate ('truncate table DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U');       
   --*************************************************************************************************
  -- Reload data with new data (foundation table not partitioned)
  --**************************************************************************************************  
      b_insert;  
    end;  

 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
    DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_procedure_name,l_description,g_recs_read,g_recs,g_recs);
    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_S4S_004U;
