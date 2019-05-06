--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004A_SS_P1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004A_SS_P1" (
   p_forall_limit   IN     INTEGER,
   p_success        OUT    BOOLEAN)
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
--PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE

-- Comment  from FND:
-- 9 MARCH 2015 -- When a change occurs at source, the entire employee history is resent.
--              -- When it comes to running the 'explode' from FND to PRF_DAY, 
--                  we do it for all records
--                  as the delete off the PRD_DAY table will take forever and be a huge number of records.
--              -- This is because we would need to process not only changed employess(coming from STG) 
--                  but also any employees with availability_end_date = null on FND.
--                  Note that eventhough we will only use a subset of this data (next proc to run wh_prf_s4s_4u)
--                  we generate all days within each period
----------------------------------------------------------------------
--  Process :
--------------    
    -- STEP 2 : insert into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS
    --        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
    --           for this procedure we generate all data from cycle_start_date through to current_date+21days
    --        : Filtering will be done later
    -- STEP 3 : truncate table TEMP_S4S_LOC_EMP_DY_PART2_SS
    -- STEP 4 : Insert into table TEMP_S4S_LOC_EMP_DY_PART2_SS
    --          This step creates the list of employees with the following info :
    --             a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
    --             b.) RNK = numbered sequence starting at 1 for each week within period
    --           /* - don't need this c.) rank_week_number = the sequence of the weeks within period
    --                        eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
    -- STEP 5 : update table TEMP_S4S_LOC_EMP_DY_PART2_SS with week_number
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
    -- STEP 6 : insert into table TEMP_S4S_LOC_EMP_DY_PART3_SS 
    --          This step creates the list of employees with the following info :
    --             a.) week_number = week_number within the cycle period
   ----------------------------------------------------------------------
   --
   --  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS, 
   --                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS,
   --                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS
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
g_recs             INTEGER := 0;
g_recs_tbc         INTEGER := 0;
g_error_count      NUMBER := 0;
g_error_index      NUMBER := 0;
g_count            NUMBER := 0;
g_rec_out          RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
g_found            BOOLEAN;
g_date             DATE;
g_SUB              NUMBER := 0;
g_end_date         DATE;
g_start_date       date;
g_end_sub          number;
g_NAME             VARCHAR2(40);


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004A_SS_P1';
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

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     select distinct this_week_start_date ,this_week_start_date+ 20
      into g_start_date, g_end_date
     from dim_calendar where calendar_date = g_date;

    l_text := 'END DATE BEING PROCESSED IS:- '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

---------------------------------------------------------------
--
-- STEP 1 : truncate table dwh_performance.temp_S4S_FND_AVAIL_CYCLE_DATES
--
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      g_recs_updated := 0;
      G_RECS := 0;

      l_text := 'TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS');

     g_name := null; 
 BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT1_SS'
    AND TABLE_NAME   = 'TEMP_S4S_LOC_EMP_DY_PART1_SS';

    l_text          := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS');
    COMMIT;

  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT1_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART1_SS', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

---------------------------------------------------------------
--
-- STEP 2 : insert into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS
--        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
--            for this procedure we generate all data from cycle_start_date through to current_date+21days
--        : Filtering will be done later
--
---------------------------------------------------------------
     for v_cur in (

     WITH SELEXT AS (
     select  /*+ full(fnd) parallel(fnd,4) full(DC) parallel(DC,4) */    
             DISTINCT 
                     employee_id
                    ,cycle_start_date
                    ,no_of_weeks
                    ,availability_start_date
                    ,availability_end_date
                    ,dc.this_week_start_date
                    ,dc.this_week_end_date
                FROM dwh_foundation.FND_S4S_emp_avail_DY fnd, 
                     dim_calendar dc
--               where DC.CALENDAR_DATE BETWEEN FND.CYCLE_START_DATE AND G_END_DATE
               where dc.this_week_start_date = fnd.cycle_start_date
                 and FND.CYCLE_START_DATE between dc.this_week_start_date AND g_end_date                         
                  and dc.fin_year_no >= 2018
                 )

        select /*+ full(SE) parallel(SE,4) */   
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
      ) loop

            g_start_date := null;
            g_end_date := v_cur.min_cycle_this_wk_start_dt-1;
            g_sub := 0;
            G_END_SUB := round(v_cur.full_no_of_weeks/v_cur.no_of_weeks);

      for g_sub in 0..G_END_SUB loop
              g_start_date := g_end_date + 1;
              g_end_date := g_start_date + (v_cur.no_of_weeks * 7) - 1;

       INSERT /*+ APPEND */
            INTO dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS
                           values (v_cur.employee_id
                                  ,v_cur.cycle_start_date
                                  ,v_cur.no_of_weeks
                                  ,v_cur.availability_start_date
                                  ,v_cur.availability_end_date
                                  ,v_cur.full_no_of_weeks
                                  ,v_cur.min_cycle_this_wk_start_dt
                                  ,v_cur.max_cycle_this_wk_start_dt
                                  ,v_cur.max_cycle_this_wk_end_dt
                                  ,g_start_date 
                                  ,g_end_date  );
          g_recs :=SQL%ROWCOUNT ;
        COMMIT;
          g_recs_updated := g_recs_updated + g_recs;

              if g_recs_updated mod 50000 = 0 
              then 
                   l_text := 'Recs inserted into dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS = '||g_recs_updated;
                   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
           end if;


          END LOOP;

       end loop;
      l_text := 'FINAL Recs inserted into dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS = '||g_recs_inserted;
      DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);

      l_text := 'create INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS 
                               ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS 
                               (EMPLOYEE_ID, AVAILABILITY_START_DATE,CYCLE_START_DATE)     
                           TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS LOGGING NOPARALLEL') ;


     l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART1_SS', DEGREE => 4);
     l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);        

END WH_PRF_S4S_004A_SS_P1;
