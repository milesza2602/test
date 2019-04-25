--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004AC_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004AC_NEW" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
----------------------------------- wendy test new pertune -----------------------------------------------------
   --**************************************************************************************************
    -- copy of wh_prf_s4s_004a - taken on 6/oct/2014 before code to change aviability to cycle
    --**************************************************************************************************
   --  Date:        July 2014
   --  Author:      Wendy lyttle
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
   ----------------------------------------------------------------------
   --  Process :
   --------------
    -- STEP 1 : truncate table dwh_performance.temp_S4S_FND_AVAIL_CYCLE_DATES
    -- STEP 2 : insert into dwh_performance.TST_S4S_LOC_EMP_DY_part1
    --        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
    --            for this procedure we generate all data from cycle_start_date through to current_date+21days
    --        : Filtering will be done later
    -- STEP 3 : truncate table TST_S4S_LOC_EMP_DY_part2
    -- STEP 4 : Insert into table TST_S4S_LOC_EMP_DY_part2
    --          This step creates the list of employees with the following info :
    --             a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
    --             b.) RNK = numbered sequence starting at 1 for each week within period
    --           /* - don't need this c.) rank_week_number = the sequence of the weeks within period
    --                        eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
    -- STEP 5 : update table TST_S4S_LOC_EMP_DY_part2 with week_number
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
    -- STEP 6 : insert into table TST_S4S_LOC_EMP_DY_part3 
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
   ----------------------------------------------------------------------
   --
   --  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part1, 
   --                          dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part2,
   --                          dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part3
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
   g_run_period_end_date date;
   g_start_date date;
   g_end_sub number;
   g_NAME          VARCHAR2(40);


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004AC_NEW';
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

--
--**************************************************************************************************
-- C1_PREPARE_PART3 TABLE 
--------------------------
--
-- Step 1 : -- Remove constraints and indexes
--**************************************************************************************************
--
procedure C1_prepare_part3 as
BEGIN

     g_name := null; 
        BEGIN
              SELECT CONSTRAINT_NAME
                 INTO G_name
              FROM DBA_CONSTRAINTS
                  WHERE CONSTRAINT_NAME = 'PK_TST_S4S_LC_EMP_DY_PRT3'
                  AND TABLE_NAME        = 'TST_S4S_LOC_EMP_DY_PART3';
                  
              l_text               := 'alter table dwh_performance.TST_S4S_LOC_EMP_DY_PART3 drop constraint PK_TST_S4S_LC_EMP_DY_PRT3';
              dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
              EXECUTE immediate('alter table dwh_performance.TST_S4S_LOC_EMP_DY_PART3 drop constraint PK_TST_S4S_LC_EMP_DY_PRT3');
              COMMIT;
        
      EXCEPTION
      WHEN no_data_found THEN
            l_text := 'constraint PK_TST_S4S_LC_EMP_DY_PRT3 does not exist';
            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      END;


      BEGIN
          SELECT index_NAME
             INTO G_name
          FROM DBA_indexes
              WHERE index_NAME = 'I10_TST_S4S_LC_EMP_DY_PRT3'
              AND TABLE_NAME        = 'TST_S4S_LOC_EMP_DY_PART3';
              
          l_text               := 'drop INDEX DWH_PERFORMANCE.I10_TST_S4S_LC_EMP_DY_PRT3';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TST_S4S_LC_EMP_DY_PRT3');
          COMMIT;
          
        EXCEPTION
        WHEN no_data_found THEN
              l_text := 'index I10_TST_S4S_LC_EMP_DY_PRT3 does not exist';
              dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        END;

      BEGIN
          SELECT index_NAME
             INTO G_name
          FROM DBA_indexes
              WHERE index_NAME = 'I40_TST_S4S_LC_EMP_DY_PRT3'
              AND TABLE_NAME        = 'TST_S4S_LOC_EMP_DY_PART3';
              
          l_text               := 'drop INDEX DWH_PERFORMANCE.I40_TST_S4S_LC_EMP_DY_PRT3';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_TST_S4S_LC_EMP_DY_PRT3');
          COMMIT;
          
        EXCEPTION
        WHEN no_data_found THEN
              l_text := 'index I40_TST_S4S_LC_EMP_DY_PRT3 does not exist';
              dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        END;
        

         l_text := 'Running GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART2';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TST_S4S_LOC_EMP_DY_PART2', DEGREE => 4);
       l_text := 'Completed GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART2';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c1_prepare_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c1_prepare_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c1_prepare_part3;
      
--
--**************************************************************************************************
-- C2_INSERT_PART3 TABLE 
--------------------------
--
-- Step 2 : insert into dwh_performance.TST_S4S_LOC_EMP_DY_part3

-- This step creates the list of employees with the following info :
-- a.) week_number = week_number within the cycle period
--**************************************************************************************************
--

procedure c2_insert_part3 as
BEGIN

       
    INSERT /*+ APPEND */
    INTO dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part3
        WITH selext1 AS
                          (SELECT /*+ MATERIALIZE  FULL(A)  PARALLEL(A,6)  */ DISTINCT EMPLOYEE_ID,
                                                ORIG_CYCLE_START_DATE,
                                                AVAILABILITY_START_DATE,
                                                AVAILABILITY_END_DATE ,
                                                CYCLE_START_DATE,
                                                CYCLE_END_DATE
                          FROM dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part2 a
                          WHERE this_week_start_date BETWEEN orig_cycle_start_date AND NVL(availability_end_date, g_end_date)
                          ),
          selext2 AS
                       (SELECT /*+ MATERIALIZE FULL(tmp)  PARALLEL(tmp,4)  FULL(se1)  PARALLEL(se1,4)*/ DISTINCT tmp.EMPLOYEE_ID ,
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
                        FROM dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part2 tmp,
                              selext1 se1
                        WHERE (tmp.this_week_start_date BETWEEN tmp.orig_cycle_start_date AND NVL(tmp.availability_end_date, g_end_date)
                        OR tmp.this_week_start_date BETWEEN se1.cycle_start_date AND se1.cycle_end_date)
                        AND tmp.employee_id             = se1.employee_id
                        AND tmp.orig_cycle_start_date   = se1.orig_cycle_start_date
                        AND tmp.availability_start_date = se1.availability_start_date
                        AND tmp.cycle_start_date        = se1.cycle_start_date
                        )
        SELECT DISTINCT tmp.EMPLOYEE_ID ,
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
        FROM selext2 tmp
        WHERE this_week_start_date BETWEEN tmp.availability_start_date AND NVL(tmp.availability_end_date, g_end_date)
        ORDER BY orig_cycle_start_date,
                  availability_start_date,
                  this_week_start_date;
          
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        g_recs_inserted := g_recs;
        l_text          := 'Recs inserted in   dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part3 = '||g_recs_inserted;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

 exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c2_insert_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c2_insert_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c2_insert_part3;
      
----
--**************************************************************************************************
-- c3_COMPLETE_PART2 TABLE 
--------------------------
--
-- Step 3 : -- Add constraints and indexes back
--
--**************************************************************************************************
-- 
procedure c3_complete_part3 as
BEGIN

      l_text := 'ALTER TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part3 ADD CONSTRAINT PK_TST_S4S_LC_EMP_DY_PRT3';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
       EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part3
                          ADD CONSTRAINT PK_TST_S4S_LC_EMP_DY_PRT3
                          PRIMARY KEY (EMPLOYEE_ID, THIS_WEEK_START_DATE, CYCLE_START_DATE, AVAILABILITY_START_DATE) 
                          USING INDEX TABLESPACE PRF_MASTER ENABLE') ;
       COMMIT;   

      l_text := 'create INDEX DWH_PERFORMANCE.I10_TST_S4S_LC_EMP_DY_PRT3';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TST_S4S_LC_EMP_DY_PRT3 
                          ON DWH_PERFORMANCE.TST_S4S_LOC_EMP_DY_part3 (EMPLOYEE_ID, CYCLE_START_DATE, WEEK_NUMBER)     
                          TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TST_S4S_LC_EMP_DY_PRT3 LOGGING NOPARALLEL') ;

      l_text := 'create INDEX DWH_PERFORMANCE.I40_TST_S4S_LC_EMP_DY_PRT3';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_TST_S4S_LC_EMP_DY_PRT3 
                          ON DWH_PERFORMANCE.TST_S4S_LOC_EMP_DY_part3 (EMPLOYEE_ID,AVAILABILITY_START_DATE,WEEK_NUMBER)     
                          TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_TST_S4S_LC_EMP_DY_PRT3 LOGGING NOPARALLEL') ;

    
     l_text := 'Running GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART3';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TST_S4S_LOC_EMP_DY_PART3', DEGREE => 4);
     l_text := 'Completed GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART3';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c3_complete_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c3_complete_part3';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c3_complete_part3;
      
 
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
   g_run_period_end_date := G_END_DATE;

    l_text := 'END DATE BEING PROCESSED IS:- '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';
 


     l_text := '------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                  g_recs_inserted := 0;
                  g_recs_updated := 0;
                  G_RECS := 0;
        
              l_text := 'TRUNCATE TABLE  dwh_performance.TST_S4S_LOC_EMP_DY_part3';
              dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
              EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part3');
              
              c1_prepare_part3;
              c2_insert_part3;
              c3_complete_part3;

     l_text := '------------------------------------------------';
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




END WH_PRF_S4S_004AC_NEW;
