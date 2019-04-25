--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004BC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004BC" (
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
--PK_TMP_S4S_LC_EMP_DY_PRT1 EMPLOYEE_ID,AVAILABILITY_START_DATE,CYCLE_START_DATE
--PK_F_TEMP_S4S_LC_MP_DY_PRT2 EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE
--PK_F_TEMP_S4S_LC_MP_DY_PRT3 EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE

   -- Comment  from FND:
-- 9 MARCH 2015 -- When a change occurs at source, the entire employee history is resent.
--              --   When it comes to running the 'explode' from FND to PRF_DAY, 
--                        we do it for all records
--                 as the delete off the PRD_DAY table will take forever and be a huge number of records.
--              --   This is because we would need to process not only changed employess(coming from STG) 
--                but also any employees with availability_end_date = null on FND.
  --               Note that eventhough we will only use a subset of this data (next proc to run wh_prf_s4s_4u)
   --                we generate all days within each period
   ----------------------------------------------------------------------
   --  Process :
   --------------
    -- STEP 1 : truncate table dwh_performance.temp_S4S_FND_AVAIL_CYCLE_DATES
    -- STEP 2 : insert into dwh_performance.temp_S4S_LOC_EMP_DY_part1
    --        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
    --            for this procedure we generate all data from cycle_start_date through to current_date+21days
    --        : Filtering will be done later
    -- STEP 3 : truncate table TEMP_S4S_LOC_EMP_DY_part2
    -- STEP 4 : Insert into table TEMP_S4S_LOC_EMP_DY_part2
    --          This step creates the list of employees with the following info :
    --             a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
    --             b.) RNK = numbered sequence starting at 1 for each week within period
    --           /* - don't need this c.) rank_week_number = the sequence of the weeks within period
    --                        eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
    -- STEP 5 : update table TEMP_S4S_LOC_EMP_DY_part2 with week_number
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
    -- STEP 6 : insert into table TEMP_S4S_LOC_EMP_DY_part3 
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
   ----------------------------------------------------------------------
   --
   --  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part1, 
   --                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part2,
   --                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3
  --  Packages:    dwh_constants, dwh_log, dwh_valid
   --
   --  Maintenance:
   --  Change using example employee_id = to test
   --  allows for generating all weeks but then removing weeks no required 
   --
   --  Naming conventions
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
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004BC';
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
 

---------------------------------------------------------------
--
-- STEP 6 : truncate table TEMP_S4S_LOC_EMP_DY_part3
--
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;

      l_text := 'TRUNCATE TABLE  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      EXECUTE IMMEDIATE
         ('TRUNCATE TABLE  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3');
COMMIT;

     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_F_TEMP_S4S_LC_MP_DY_PRT3'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART3';
    
    l_text               := 'alter table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3 drop constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3 drop constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3 does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT3'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART3';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT3 does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I40_TMP_S4S_LC_EMP_DY_PRT3'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART3';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I40_TMP_S4S_LC_EMP_DY_PRT3 does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
  

         l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART2', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);




---------------------------------------------------------------
--
-- STEP 7 : insert into table TEMP_S4S_LOC_EMP_DY_part3 
--
-- NOTES
--------
-- This step creates the list of employees with the following info :
-- a.) week_number = week_number within the cycle period
---------------------------------------------------------------
      l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;
         
INSERT /*+ APPEND */
INTO dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3
WITH selext1 AS
            (SELECT /*+ materialize FULL(a)  PARALLEL(a,4)  */ DISTINCT EMPLOYEE_ID,
              ORIG_CYCLE_START_DATE,
              AVAILABILITY_START_DATE,
              AVAILABILITY_END_DATE ,
              CYCLE_START_DATE,
              CYCLE_END_DATE
            FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part2 a
            WHERE this_week_start_date BETWEEN orig_cycle_start_date AND NVL(availability_end_date, g_end_date)
            ),
  selext2 AS
          (SELECT /*+ materialize FULL(tmp)  PARALLEL(tmp,4)  FULL(se1)  PARALLEL(se1,4)*/ DISTINCT tmp.EMPLOYEE_ID ,
            tmp.ORIG_CYCLE_START_DATE ,
            tmp.AVAILABILITY_START_DATE ,
            tmp.AVAILABILITY_END_DATE ,
            tmp.CYCLE_START_DATE ,
            tmp.CYCLE_END_DATE ,
            tmp.NO_OF_WEEKS ,
            tmp.THIS_WEEK_START_DATE ,
            tmp.THIS_WEEK_END_DATE ,
            tmp.FIN_YEAR_NO ,
            tmp.FIN_WEEK_NO ,
            tmp.EMP_LOC_RANK ,
            tmp.AVAIL_CYCLE_START_DATE_PLUS_20 ,
            tmp.THIS_WEEK_START_DATE_PLUS_20 ,
            tmp.WEEK_NUMBER
          FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part2 tmp,
            selext1 se1
          WHERE (tmp.this_week_start_date BETWEEN tmp.orig_cycle_start_date AND NVL(tmp.availability_end_date, g_end_date)
          OR tmp.this_week_start_date BETWEEN se1.cycle_start_date AND se1.cycle_end_date)
          AND tmp.employee_id             = se1.employee_id
          AND tmp.orig_cycle_start_date   = se1.orig_cycle_start_date
          AND tmp.availability_start_date = se1.availability_start_date
          AND tmp.cycle_start_date        = se1.cycle_start_date
          ),
selext3 as (SELECT /*+ materialize */ DISTINCT EMPLOYEE_ID ,
                        ORIG_CYCLE_START_DATE ,
                        AVAILABILITY_START_DATE ,
                        AVAILABILITY_END_DATE ,
                        CYCLE_START_DATE ,
                        CYCLE_END_DATE ,
                        NO_OF_WEEKS ,
                        THIS_WEEK_START_DATE ,
                        THIS_WEEK_END_DATE ,
                        FIN_YEAR_NO ,
                        FIN_WEEK_NO ,
                        EMP_LOC_RANK ,
                        AVAIL_CYCLE_START_DATE_PLUS_20 ,
                        THIS_WEEK_START_DATE_PLUS_20 ,
                         WEEK_NUMBER
                      FROM selext2 
                      WHERE this_week_start_date BETWEEN availability_start_date AND NVL(availability_end_date, g_end_date)
                      ORDER BY EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE),
  seldup as (SELECT DISTINCT EMPLOYEE_ID FROM (select EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE, count(*) from selext3
            group by EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE
            having count(*) > 1))
  select se3.EMPLOYEE_ID ,
                        se3.ORIG_CYCLE_START_DATE ,
                        se3.AVAILABILITY_START_DATE ,
                        se3.AVAILABILITY_END_DATE ,
                        se3.CYCLE_START_DATE ,
                        se3.CYCLE_END_DATE ,
                        se3.NO_OF_WEEKS ,
                        se3.THIS_WEEK_START_DATE ,
                        se3.THIS_WEEK_END_DATE ,
                        se3.FIN_YEAR_NO ,
                        se3.FIN_WEEK_NO ,
                        se3.EMP_LOC_RANK ,
                        se3.AVAIL_CYCLE_START_DATE_PLUS_20 ,
                        se3.THIS_WEEK_START_DATE_PLUS_20 ,
                        se3.WEEK_NUMBER
from selext3 se3  
  where not exists (select sd.employee_id from seldup sd where se3.employee_id = sd.employee_id);
  
g_recs :=SQL%ROWCOUNT ;
COMMIT;

g_recs_inserted := g_recs;
l_text          := 'Recs inserted in   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3 = '||g_recs_inserted;

dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    l_text := 'ALTER TABLE dwh_performance.temp_S4S_LOC_EMP_DY_part3 ADD CONSTRAINT PK_F_TEMP_S4S_LC_MP_DY_PRT3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
          EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.temp_S4S_LOC_EMP_DY_part3
ADD CONSTRAINT PK_F_TEMP_S4S_LC_MP_DY_PRT3
PRIMARY KEY (EMPLOYEE_ID, THIS_WEEK_START_DATE, CYCLE_START_DATE, AVAILABILITY_START_DATE) USING INDEX TABLESPACE PRF_MASTER ENABLE') ;
 COMMIT;   

 l_text := 'create INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3 ON DWH_PERFORMANCE.temp_S4S_LOC_EMP_DY_part3 (EMPLOYEE_ID, CYCLE_START_DATE, WEEK_NUMBER)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3 LOGGING NOPARALLEL') ;

 l_text := 'create INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3 ON DWH_PERFORMANCE.temp_S4S_LOC_EMP_DY_part3 (EMPLOYEE_ID
,AVAILABILITY_START_DATE
,WEEK_NUMBER)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3 LOGGING NOPARALLEL') ;

    
   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART3', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;

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




END WH_PRF_S4S_004BC  ;
