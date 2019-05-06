--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004BA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004BA" 
                      (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
   --**************************************************************************************************
   --  Date:        Aug 2016 - revamp from July 2014 version = WH_PRF_S4S_004A
   --  Author:      Wendy lyttle
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
   --                     with load_group =  between emp_start_1 and emp_end_1
   --                     from table dwh_performance.TEMP_S4S_EMP_SPLIT to determine employee_id selection
   --
   --               When a change occurs at source, the entire employee history is resent
   --               When it comes to running the 'explode' from FND to PRF_DAY, 
   --                        we do it for all records
   --                        as the delete off the PRD_DAY table will take forever and be a huge number of records.
   --               This is because we would need to process not only changed employess(coming from STG) 
   --               but also any employees with availability_end_date = null on FND.
   --               NB. eventhough we will only use a subset of this data (next proc to run wh_prf_s4s_4u)
   --                   we generate all days within each period
   ----------------------------------------------------------------------
   --  Process :
   --------------
   -- STEP 1 : truncate table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
   -- STEP 2 : drop indexes table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
   -- STEP 3 : insert into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
   --        : Eventhough there are rules pertaining to selection/forecasting/processing periods
   --            for this procedure we generate all data from cycle_start_date through to current_date+21days
   --        : Filtering will be done later
   -- STEP 4 : add indexes table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
   ----------------------------------------------------------------------
   --  Tables: 
   --------------
   --               Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1BA
   --                          where primary-key =  PK_TMP_S4S_LC_EMP_DY_PRT1BA (employee_id,availability_start_date,cycle_start_date)
   --
  ----------------------------------------------------------------------
  --  Packages:    
  -- --------------
  --            dwh_constants, dwh_log, dwh_valid
   ----------------------------------------------------------------------
   --  Maintenance:
   -- --------------
   --  Wendy Lyttle              -- Change using example employee_id = to test
   --                            -- allows for generating all weeks but then removing weeks no required 
   --  Wendy Lyttle   6 oct 2014 -- change aviability to cycle
   --  Wendy Lyttle   9 MAR 2015 -- When a change occurs at source, the entire employee history is resent.
   --  Wendy Lyttle     Aug 2016 -- revamp proc - split into 3 employee ranges and hence 3 procs
   ----------------------------------------------------------------------
   --  Naming conventions
   -- --------------
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
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004BA';
      l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
      l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
      l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
      l_text sys_dwh_log.log_text%TYPE;
      l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD TEMP_S4S_LOC_EMP_DY_PART1BA';
      l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;

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
    l_text := 'LOAD TEMP_S4S_LOC_EMP_DY_PART1BA STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
-- hardcoding batch_date for testing
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
  g_name := null; 
  g_recs_inserted := 0;
  g_recs_updated := 0;
  G_RECS := 0;
 
---------------------------------------------------------------
--
-- STEP 1 : truncate table TEMP_S4S_LOC_EMP_DY_PART1BA
--
---------------------------------------------------------------
  l_text := 'TRUNCATE TABLE  dwh_performance.TEMP_S4S_004BA';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_004BA');

  l_text := 'TRUNCATE TABLE  dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA');
---------------------------------------------------------------
--
-- STEP 2 : drop indexes on table TEMP_S4S_LOC_EMP_DY_PART1BA
--
---------------------------------------------------------------  
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT1BA'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART1BA';

    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1BA';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1BA');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT1BA does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
  l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1BA';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART1BA', DEGREE => 4);
  l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1BA';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      
---------------------------------------------------------------
--
-- STEP 3 : insert into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BAtmp
--        : Eventhough there are rules pertaining to selection/forecasting/processing periods
--            for this procedure we generate all data from cycle_start_date through to current_date+21days
--        : Filtering will be done later
---------------------------------------------------------------
  l_text := '----------------------';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  
  g_recs_inserted := 0;
  
  INSERT /*+ APPEND */ INTO dwh_performance.TEMP_S4S_004BA
     WITH 
             SELEMP AS (  
                           select  /*+ full(fnd) parallel(fnd,4)  */    
                           DISTINCT     employee_id
                                  , cycle_start_date
                                  , no_of_weeks
                                  , availability_start_date
                                  , availability_end_date
                              FROM dwh_foundation.FND_S4S_emp_avail_DY fnd
      --                        where employee_id in ('1002158','1002289')
                              where employee_id in ('1002140'
    ,'1002158'
    ,'1002194'
    ,'1002236'
    ,'1002238'
    ,'1002247'
    ,'1002258'
    ,'1002289'
    ,'1002355'
    ,'1002421'
    ,'1002436'
    ,'1002440'
    ,'1002446'
    )

                              --, dwh_performance.TEMP_S4S_EMP_SPLIT SE
                              --where FND.EMPLOYEE_ID BETWEEN emp_start_1 AND emp_END_1
                        ),
             SELEXT AS (  
                       select  /*+  full(DC) */    
                           DISTINCT     employee_id
                                  , cycle_start_date
                                  , no_of_weeks
                                  , availability_start_date
                                  , availability_end_date
                                  , this_week_start_date,this_week_end_date
                              FROM SELEMP fnd,  dim_calendar dc
                              where DC.CALENDAR_DATE BETWEEN FND.CYCLE_START_DATE AND '29 aug 2016'
                        )
           select  /*+ full(SE) parallel(SE,4) */   
                      employee_id
                    , cycle_start_date
                    , no_of_weeks
                    , availability_start_date
                    , availability_end_date
                    , count(distinct this_week_start_date)  full_no_of_weeks
                    , min(this_week_start_date) min_cycle_this_wk_start_dt
                    , max(this_week_start_date) max_cycle_this_wk_start_dt
                    , max(this_week_end_date) max_cycle_this_wk_end_dt
                FROM SELEXT SE
                 group by employee_id
                      , cycle_start_date
                      , no_of_weeks
                      , availability_start_date
                      , availability_end_date
                order by cycle_start_date, availability_start_date
          ;
           g_recs :=SQL%ROWCOUNT ;
           l_text := 'Recs inserted into TEMP_S4S_004BA = '||g_recs;
           dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
           commit;

     l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_004BA';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_004BA', DEGREE => 4);

---------------------------------------------------------------
--
-- STEP 4 : insert into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
--           with correct dates
---------------------------------------------------------------      
  l_text := '----------------------';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  
  g_recs_inserted := 0;
  

     insert /*+ append */ into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA
     with 
     selemp as ( 
              select distinct fnd.*
                              , (trunc((full_no_of_weeks+no_of_weeks)/no_of_weeks)) * no_of_weeks xx
              from dwh_performance.TEMP_S4S_004BA fnd
              ),
      selext as ( 
               select distinct fnd.*
                               , this_week_start_date, this_week_end_date 
                               , case when full_no_of_weeks +no_of_weeks = xx then full_no_of_weeks else xx end xy
                               , dense_rank () over(order by employee_id, cycle_start_date, availability_start_date) rnk
               from selemp fnd, dim_calendar dc
               where this_week_start_date between min_cycle_this_wk_start_dt 
                                          and min_cycle_this_wk_start_dt 
                                              + ((case when full_no_of_weeks + no_of_weeks = xx then full_no_of_weeks else xx end) * 7)
               order by employee_id, cycle_start_date, availability_start_date
                ),
      selext2 as (
                 select se.*
                        ,row_number () over(partition by rnk order by this_week_start_date) rnk2
                 from selext se
                 )
     select employee_id
          , cycle_start_date
          , no_of_weeks
          , availability_start_date
          , availability_end_date
          , full_no_of_weeks
          , min_cycle_this_wk_start_dt
          , max_cycle_this_wk_start_dt
          , max_cycle_this_wk_end_dt
          , (this_week_end_date - ((no_of_weeks * 7)))+1 startdt
          , this_week_end_date
          , RNK rank
          , RNK2 rank2
     from selext2 se2
     where remainder(rnk2,no_of_weeks) =  0;

     g_recs :=SQL%ROWCOUNT ;
     COMMIT;
     g_recs_inserted := g_recs_inserted + g_recs;

     l_text := 'Recs inserted into TEMP_S4S_LOC_EMP_DY_PART1BA = '||g_recs_inserted;
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

---------------------------------------------------------------
--
-- STEP 5 : add indexes on table TEMP_S4S_LOC_EMP_DY_PART1BA
--
---------------------------------------------------------------  
  l_text := '----------------------';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

     l_text := 'Create Index I10_TMP_S4S_LC_EMP_DY_PRT1BA';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1BA 
                               ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1BA 
                                         (EMPLOYEE_ID, AVAILABILITY_START_DATE,CYCLE_START_DATE)     
                               TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
     Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1BA LOGGING NOPARALLEL') ;
  
     l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1BA';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART1BA', DEGREE => 4);
  l_text := '----------------------';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);


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




END WH_PRF_S4S_004BA  ;
