--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004A_PRFTUNE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004A_PRFTUNE" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
   --**************************************************************************************************
    -- copy of wh_prf_s4s_004a - taken on 6/oct/2014 before code to change aviability to cycle
    --**************************************************************************************************
   --  Date:        July 2014
   --  Author:      Wendy lyttle
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
   --
   -- Comment  from FND:
   --**************************************************************************************************
   g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read        INTEGER := 0;
   g_recs_inserted    INTEGER := 0;
   g_recs_updated     INTEGER := 0;
   g_recs    INTEGER := 0;
   g_recs_tbc         INTEGER := 0;
   g_error_count      NUMBER := 0;
   g_error_index      NUMBER := 0;
   g_count            NUMBER := 0;
   g_rec_out          RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
   g_found            BOOLEAN;
   g_date             DATE;
   g_SUB      NUMBER := 0;
   g_end_date             DATE;
g_start_date date;
g_end_sub number;
 g_NAME          VARCHAR2(40);


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004A_PRFTUNE';
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
--
--g_date := '7 dec 2014';

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       select distinct this_week_start_date + 20
   into g_end_date
   from dim_calendar where calendar_date = g_date;

    l_text := 'END DATE BEING PROCESSED IS:- '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';
 
 execute immediate 'truncate table dwh_performance.temp_S4S_LOC_EMP_DY_part1x';
 commit;
---------------------------------------------------------------
--
-- STEP 2 : insert into dwh_performance.temp_S4S_LOC_EMP_DY_part1x
--        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
--            for this procedure we generate all data from cycle_start_date through to current_date+21days
--        : Filtering will be done later
--
---------------------------------------------------------------
     for v_cur in (
     
     WITH SELEXT AS (  select  /*+ full(fnd) parallel(fnd,4) full(DC) parallel(DC,4) */    
    DISTINCT     employee_id
                              , cycle_start_date
                              , no_of_weeks
                              , availability_start_date
                              , availability_end_date
                              , this_week_start_date,this_week_end_date
                          FROM dwh_foundation.FND_S4S_emp_avail_DY fnd, dim_calendar dc
                          where 
                                  
                            --    (
                                dc.calendar_date between fnd.cycle_start_date and g_end_date 
                         --       and availability_end_date is null
                             -- COMMENT OUT ONLY WHEN DOING A DATA TAKEON
                          --      ) or fnd.last_updated_date = g_date
                                
                              and employee_id = '7096173' --(  '7096809','7096588','7096173','7095840')                                
                         )
                          
                        select  /*+ full(SE) parallel(SE,4) */   
                          employee_id
                              , cycle_start_date
                              , no_of_weeks
                              , availability_start_date
                              , availability_end_date
                              , count(distinct this_week_start_date)  full_no_of_weeks
                              , min(this_week_start_date) min_cycle_this_wk_start_dt
                         --     , --max(this_week_start_date) max_cycle_this_wk_start_dt
                          --    , --max(this_week_end_date) max_cycle_this_wk_end_dt
                          FROM SELEXT SE
                           group by employee_id
                                , cycle_start_date
                                , no_of_weeks
                                , availability_start_date
                                , availability_end_date
                          order by cycle_start_date, availability_start_date
      ) loop
      
            g_start_date := null;
            g_end_date := v_cur.min_cycle_this_wk_start_dt-1;
            g_sub := 0;
            G_END_SUB := round(v_cur.full_no_of_weeks/v_cur.no_of_weeks);
    l_text               := '-------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      for g_sub in 0..G_END_SUB loop
     l_text               := '-------';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);           
      
    l_text               := 'g_sub='||g_sub
     ||'**g_end_sub='||g_end_sub
     ||'**g_start_date='||g_start_date
     ||'**g_end_date='||g_end_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);             
              
              g_start_date := g_end_date + 1;
              g_end_date := g_start_date + (v_cur.no_of_weeks * 7) - 1;

    l_text               := 
'employee_id='||v_cur.employee_id
 ||'**cycle_start_date='||v_cur.cycle_start_date
 ||'**no_of_weeks='||v_cur.no_of_weeks
 ||'**availability_start_date='||v_cur.availability_start_date
 ||'**availability_end_date='||v_cur.availability_end_date
 ||'**full_no_of_weeks='||v_cur.full_no_of_weeks
 ||'**min_cycle_this_wk_start_dt='||v_cur.min_cycle_this_wk_start_dt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

              
       INSERT /*+ APPEND */
            INTO dwh_performance.temp_S4S_LOC_EMP_DY_part1x
                         values (v_cur.employee_id
                          , v_cur.cycle_start_date
                          , v_cur.no_of_weeks
                          , v_cur.availability_start_date
                          , v_cur.availability_end_date
                          , v_cur.full_no_of_weeks
                          , v_cur.min_cycle_this_wk_start_dt
                          , v_cur.availability_start_date --v_cur.max_cycle_this_wk_start_dt
                          , v_cur.availability_start_date --v_cur.max_cycle_this_wk_end_dt
                          , g_start_date 
                          , g_end_date  );
          g_recs :=SQL%ROWCOUNT ;
             COMMIT;
             g_recs_updated := g_recs_updated + g_recs;
             
              if g_recs_updated mod 50000 = 0 
              then 
                   l_text := 'Recs inserted into   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part1x = '||g_recs_updated;
                   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
           end if;
             
    
          END LOOP;
       
       end loop;
    l_text := 'FINAL Recs inserted into   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part1x = '||g_recs_inserted;
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);



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



END WH_PRF_S4S_004A_PRFTUNE;
