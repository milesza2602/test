--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004BB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004BB" (
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
   -- STEP 5 : truncate table TEMP_S4S_LOC_EMP_DY_PART2BB
   -- STEP 6 : drop index on table TEMP_S4S_LOC_EMP_DY_PART2BB
   -- STEP 7 : Insert into table TEMP_S4S_LOC_EMP_DY_PART2BB
   --          This step creates the list of employees with the following info :
   --             a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
   --             b.) RNK = numbered sequence starting at 1 for each week within period
   --           /* - don't need this c.) rank_week_number = the sequence of the weeks within period
   --                        eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
   -- STEP 8 : drop index on table TEMP_S4S_LOC_EMP_DY_PART2BB
   -- STEP 9 : update table TEMP_S4S_LOC_EMP_DY_PART2BB with week_number
   --          This step creates the list of employees with the following info :
   --             a.) week_number = week_number within the cycle period
   ----------------------------------------------------------------------
   --  Tables: 
   --------------
   --               Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB
   --                          where primary-key =  PK_TMP_S4S_LC_EMP_DY_PRT2BB (employee_id,availability_start_date,cycle_start_date)
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
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004BB';
      l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
      l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
      l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
      l_text sys_dwh_log.log_text%TYPE;
      l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD TEMP_S4S_LOC_EMP_DY_PART2BB';
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
    l_text := 'LOAD TEMP_S4S_LOC_EMP_DY_PART2BB STARTED '||
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
-- STEP 5 : truncate table TEMP_S4S_LOC_EMP_DY_PART2BB
--
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;

      l_text := 'TRUNCATE TABLE  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      EXECUTE IMMEDIATE
         ('TRUNCATE TABLE  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB');

 
     g_name := null; 
     
---------------------------------------------------------------
--
-- STEP 6 : drop indexes on table TEMP_S4S_LOC_EMP_DY_PART2BB
--
---------------------------------------------------------------    
BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT2BB'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART2BB';

    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2BB');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT2BB does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


 BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I20_TMP_S4S_LC_EMP_DY_PRT2BB'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART2BB';

    l_text               := 'drop INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2BB');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I20_TMP_S4S_LC_EMP_DY_PRT2BB does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;  


         l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART2BB', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


---------------------------------------------------------------
--
-- STEP 7 : Insert into table TEMP_S4S_LOC_EMP_DY_PART2BB
--
-- NOTES
--------
-- This step creates the list of employees with the following info :
-- a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
-- b.) RNK = numbered sequence starting at 1 for each week within period
-- /* - don't need this c.) rank_week_number = the sequence of the weeks within period
--           eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
--
---------------------------------------------------------------
      l_text := 'Starting insert into TEMP_S4S_LOC_EMP_DY_PART2BB';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    
 INSERT /*+ APPEND */
            INTO  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB
         WITH selext1
                          AS (
                          SELECT /*+ materialize FULL(SC) FULL(FS) PARALLEL(SC,4) PARALLEL(FS,6)  */
                               DISTINCT        sc.EMPLOYEE_ID,
                                       ORIG_CYCLE_START_DATE,
                                       sc.CYCLE_START_DATE,
                                       sc.CYCLE_end_date,
                                       fs.NO_OF_WEEKS  --, dc.calendar_date, dc.fin_day_no, fs.week_number
                                       ,
                                       dc.this_week_start_date,
                                       dc.this_week_end_date,
                                       dc.fin_year_no,
                                       dc.fin_week_no,
                                       SC.AVAILABILITY_START_DATE,
                                       SC.AVAILABILITY_end_DATE
                                  FROM dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1BA SC,
                                       dwh_foundation.FND_S4S_emp_avail_DY FS,
                                       DIM_CALENDAR DC
                                 WHERE     SC.EMPLOYEE_ID = FS.EMPLOYEE_ID
                                       AND SC.AVAILability_START_DATE = FS.AVAILability_START_DATE
                                        AND SC.ORIG_CYCLE_START_DATE = FS.CYCLE_START_DATE
                                       AND DC.CALENDAR_DATE BETWEEN SC.CYCLE_START_DATE   AND SC.CYCLE_END_DATE
                       ORDER BY
                                       sc.EMPLOYEE_ID,ORIG_CYCLE_START_DATE,
                                       sc.CYCLE_START_DATE, AVAILABILITY_START_DATE, FIN_YEAR_NO, FIN_WEEK_NO
                                       )
                                       ,
              selext2
                          AS (  
                          SELECT /*+ materialize */
                                       EMPLOYEE_ID,
                                       ORIG_CYCLE_START_DATE,
                                       CYCLE_START_DATE,
                                       CYCLE_end_date,
                                       AVAILABILITY_START_DATE,
                                       AVAILABILITY_end_DATE,
                                       NO_OF_WEEKS,
                                       this_week_start_date,
                                       this_week_end_date                  --, week_number
                                                         ,
                                       fin_year_no,
                                       fin_week_no,
                                       rnk ,
                                       (RNK / NO_OF_WEEKS) RANK_week_number
                           FROM (SELECT /*+ materialize */
                                               EMPLOYEE_ID,
                                               ORIG_CYCLE_START_DATE,
                                               CYCLE_START_DATE,
                                               CYCLE_end_date,
                                               AVAILABILITY_START_DATE,
                                               AVAILABILITY_end_DATE,
                                               NO_OF_WEEKS,
                                               this_week_start_date,
                                               this_week_end_date,
                                               fin_year_no,
                                               fin_week_no,
                                               --week_number,
                                               DENSE_RANK ()
                                               OVER (
                                                  PARTITION BY EMPLOYEE_ID,
                                                         CYCLE_START_DATE, AVAILABILITY_START_DATE
                                                  ORDER BY this_week_start_date)
                                                  rnk
                                          FROM SELEXT1) srk
                              ORDER BY EMPLOYEE_ID,
                                       CYCLE_START_DATE,
                                       (RNK / NO_OF_WEEKS)
                                       ),
              SELVAL
                              AS (  SELECT /*+ materialize */
                                           EMPLOYEE_ID,
                                           ORIG_CYCLE_START_DATE,
                                           AVAILABILITY_START_DATE,
                                           AVAILABILITY_end_DATE,
                                           CYCLE_START_DATE,
                                           CYCLE_end_date,
                                           NO_OF_WEEKS,
                                           this_week_start_date,
                                           this_week_end_date                  --, week_number
                                                             ,
                                           fin_year_no,
                                           fin_week_no,
                                           SRK.RNK
                                      FROM (SELECT
                                                   EMPLOYEE_ID,
                                                   CYCLE_START_DATE,
                                                   CYCLE_end_date,
                                                        ORIG_CYCLE_START_DATE,
                                           AVAILABILITY_START_DATE,
                                           AVAILABILITY_end_DATE,
                                                   NO_OF_WEEKS,
                                                   this_week_start_date,
                                                   this_week_end_date,
                                                   fin_year_no,
                                                   fin_week_no,
                                                   RANK_WEEK_NUMBER,
                                                   --week_number,
                                                   DENSE_RANK ()
                                                   OVER (
                                                      PARTITION BY EMPLOYEE_ID,
                                                                   CYCLE_START_DATE,
                                                                        ORIG_CYCLE_START_DATE,
                                           AVAILABILITY_START_DATE,
                                                                   NO_OF_WEEKS
                                                      ORDER BY RANK_WEEK_NUMBER)
                                                      rnk
                                              FROM SELEXT2) srk
                                  ORDER BY EMPLOYEE_ID,
                                           CYCLE_START_DATE,
                                           this_week_start_date)
         SELECT distinct
                            EMPLOYEE_ID,
                            CYCLE_START_DATE,
                            CYCLE_end_date,
                            NO_OF_WEEKS,
                            this_week_start_date,
                            this_week_end_date                             --, week_number
                                              ,
                            fin_year_no,
                            fin_week_no,
                            RNK emp_loc_rank,
                            CYCLE_START_DATE + 20 CYCLE_start_date_plus_20,
                            this_week_start_date + 20 this_week_start_date_plus_20,
                            0,
                                                             ORIG_CYCLE_START_DATE,
                                           AVAILABILITY_START_DATE, AVAILABILITY_end_DATE
                       FROM SELVAL
     ;
     g_recs_inserted :=0;
     g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'Recs inserted into TEMP_S4S_LOC_EMP_DY_PART2BB = '||g_recs_inserted;
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
 
---------------------------------------------------------------
--
-- STEP 8 : add indexes on table TEMP_S4S_LOC_EMP_DY_PART2BB
--
---------------------------------------------------------------  
 l_text := 'create INDEX I10_TMP_S4S_LC_EMP_DY_PRT2BB';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2BB ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB (EMPLOYEE_ID, THIS_WEEK_START_DATE, CYCLE_START_DATE, AVAILABILITY_START_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2BB LOGGING NOPARALLEL') ;

 l_text := 'create INDEX I20_TMP_S4S_LC_EMP_DY_PRT2BB';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2BB ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB (EMPLOYEE_ID, CYCLE_START_DATE, WEEK_NUMBER)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2BB LOGGING NOPARALLEL') ;

    
   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART2BB', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2BB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
---------------------------------------------------------------
--
-- STEP 9 : update table TEMP_S4S_LOC_EMP_DY_PART2BB with week_number
--
-- NOTES
--------
-- This step creates the list of employees with the following info :
-- a.) week_number = week_number within the cycle period
---------------------------------------------------------------
      l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      l_text := 'Starting update of TEMP_S4S_LOC_EMP_DY_PART2BB';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_sub := 100;

      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;

      FOR v_cur IN (  SELECT /*+ FULL(a)  PARALLEL(a,4)   */ DISTINCT employee_id,
                             CYCLE_start_date,
                             AVAILABILITY_START_DATE,
                             this_week_start_date,
                             this_week_start_date_plus_20,
                             no_of_weeks
                        FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB a
                    ORDER BY employee_id,
                             CYCLE_start_date,
                              AVAILABILITY_START_DATE,
                             this_week_start_date,
                             this_week_start_date_plus_20)
      LOOP
         g_sub := g_sub + 1;

         IF    g_sub > v_cur.no_of_weeks
            OR v_cur.CYCLE_start_date = v_cur.this_week_start_date
         THEN
            g_sub := 1;
         END IF;

         UPDATE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB p2
            SET p2.week_number = g_sub
          WHERE     p2.employee_id = v_cur.employee_id
                and p2.cycle_start_date = v_cur.cycle_start_date
                and p2.availability_start_date = v_cur.availability_start_date
                AND p2.this_week_start_date = v_cur.this_week_start_date;
          g_recs :=SQL%ROWCOUNT ;
         COMMIT;
         g_recs_updated := g_recs_updated + g_recs;
         
          if g_recs_updated mod 50000 = 0 
          then 
         l_text := 'Recs updated in   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2BB = '||g_recs_updated;
         dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       end if;
         

      END LOOP;


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




END WH_PRF_S4S_004BB  ;
