--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004AA_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004AA_NEW" (
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
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004AA_NEW';
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
-- A1_PREPARE_PART1 TABLE 
--------------------------
--
-- Step 1 : -- Remove constraints and indexes
--**************************************************************************************************
--
procedure a1_prepare_part1 as
BEGIN

     g_name := null; 
      BEGIN
            SELECT CONSTRAINT_NAME
                  INTO G_name
            FROM DBA_CONSTRAINTS
                  WHERE CONSTRAINT_NAME = 'PK_TST_S4S_LC_EMP_DY_PRT1'
                  AND TABLE_NAME        = 'TST_S4S_LOC_EMP_DY_PART1';
            
            l_text               := 'alter table dwh_performance.TST_S4S_LOC_EMP_DY_PART1 drop constraint PK_TST_S4S_LC_EMP_DY_PRT1';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            EXECUTE immediate('alter table dwh_performance.TST_S4S_LOC_EMP_DY_PART1 drop constraint PK_TST_S4S_LC_EMP_DY_PRT1');
            COMMIT;
        
      EXCEPTION
      WHEN no_data_found THEN
                l_text := 'constraint PK_TST_S4S_LC_EMP_DY_PRT1 does not exist';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      END;


     l_text := 'Running GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TST_S4S_LOC_EMP_DY_PART1', DEGREE => 4);
     l_text := 'Completed GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 exception
      when dwh_errors.e_insert_error then
                 l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
                 dwh_log.record_error(l_module_name,sqlcode,l_message);
                 l_text := 'error in a1_prepare_part1';
                  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
                   l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
                   dwh_log.record_error(l_module_name,sqlcode,l_message);
                   l_text := 'error in a1_prepare_part1';
                    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a1_prepare_part1;

--
--**************************************************************************************************
-- A2_INSERT_PART1 TABLE 
--------------------------
--
-- Step 2 : insert into dwh_performance.TST_S4S_LOC_EMP_DY_part1
--        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
--            for this procedure we generate all data from cycle_start_date through to current_date+21days
--        : Filtering will be done later
--
--**************************************************************************************************
-- 
procedure a2_insert_part1na as
BEGIN

        insert /*+ append */ into dwh_performance.TST_S4S_LOC_EMP_DY_part1Na 
        --as
             WITH 

                  SELEXT1 AS 
                                       (  
                                          select  /*+ MATERIALIZE full(fnd) parallel(fnd,6) full(DC) parallel(DC,4) */    
                                             DISTINCT     employee_id
                                                  , cycle_start_date
                                                  , no_of_weeks
                                                  , availability_start_date
                                                  , availability_end_date
                                                --  , count(distinct this_week_start_date)  full_no_of_weeks
                                             , min(this_week_start_date) min_cycle_this_wk_start_dt
                                            , max(this_week_start_date) max_cycle_this_wk_start_dt
                                            , max(this_week_end_date) max_cycle_this_wk_end_dt
                                                  , g_run_period_end_date
                                              FROM dwh_foundation.FND_S4S_emp_avail_DY fnd, dim_calendar dc
                                              where   dc.calendar_date between fnd.cycle_start_date and g_run_period_end_date+((no_of_weeks*7*2)+7)
                                              and  cycle_start_date <= g_end_date
                                    /*            and  employee_id in (
                                                '1002590',
                                                '1002355'
                                                ,'1002247'
                                                )
                                    */            and EMPLOYEE_ID NOT IN ('7078713',
                                                                '7052745',
                                                                '7071470',
                                                                '7070206', '7005239')
                                                group by employee_id
                                            , cycle_start_date
                                            , no_of_weeks
                                            , availability_start_date
                                            , availability_end_date
                                            ,g_run_period_end_date
                                        )
                               select distinct employee_id
                                            , cycle_start_date
                                            , no_of_weeks
                                            , availability_start_date
                                            , availability_end_date
                                            , null full_no_of_weeks
                                            , min_cycle_this_wk_start_dt
                                            , max_cycle_this_wk_start_dt
                                            , max_cycle_this_wk_end_dt
                                            , min_cycle_this_wk_start_dt g_start_date
                                            , null end_sub --round(full_no_of_weeks/no_of_weeks)
                                            , min_cycle_this_wk_start_dt + (no_of_weeks * 7) - 1 g_end_date
                                            , g_run_period_end_date
                                            , null g_emp_max_end_date
                                            , this_week_start_date
                                            , this_week_end_date
                                            , 1 start_sub
                                        FROM SELEXT1 SE ,
                                             dim_calendar dc
                                        where calendar_date between min_cycle_this_wk_start_dt and max_cycle_this_wk_end_dt
                                        order by employee_id
                                            , cycle_start_date
                                            , no_of_weeks
                                            , availability_start_date
                                            , availability_end_date
                                         --   , full_no_of_weeks
                                            , min_cycle_this_wk_start_dt
                                            , max_cycle_this_wk_start_dt
                                            , max_cycle_this_wk_end_dt
                                        --    , g_start_date
                                          --  , g_end_date
                                            , g_run_period_end_date
                                         --   ,g_emp_max_end_date
                                            , this_week_start_date
                                            , this_week_end_date;
        
        
            g_recs :=SQL%ROWCOUNT ;
            COMMIT;
            g_recs_inserted := g_recs_inserted + g_recs;
         
            l_text := 'FINAL Recs inserted into   dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part1Na = '||g_recs_inserted;
            DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);

                l_text := 'Running GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1NA';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TST_S4S_LOC_EMP_DY_PART1NA', DEGREE => 4);
                l_text := 'Completed GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1NA';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   --     insert /*+ append */ into dwh_performance.TST_S4S_LOC_EMP_DY_part1Na 

 exception
      when dwh_errors.e_insert_error then
               l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               l_text := 'error in a2_insert_part1na';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
               l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               l_text := 'error in a2_insert_part1na';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a2_insert_part1na;

--
--**************************************************************************************************
-- A3_INSERT_PART1 TABLE 
--------------------------
--
-- Step 3 : insert into dwh_performance.TST_S4S_LOC_EMP_DY_part1
--        : Eventhough there are rules pertaining to selection/forecatsing/processing periods
--            for this procedure we generate all data from cycle_start_date through to current_date+21days
--        : Filtering will be done later
--
--**************************************************************************************************
-- 
procedure a3_insert_part1 as
BEGIN

        insert /*+ append */ into dwh_performance.TST_S4S_LOC_EMP_DY_part1

with selext1 as (                                                       
  select /*+ MATERIALIZE full(A) parallel(A,6)  */    distinct employee_id
                ,cycle_start_date
                ,no_of_weeks
                ,availability_start_date
                ,availability_end_date
                ,full_no_of_weeks
                ,min_cycle_this_wk_start_dt
                ,max_cycle_this_wk_start_dt
                ,max_cycle_this_wk_end_dt
                ,g_start_date  
                ,g_end_date 
                ,g_run_period_end_date
                ,g_emp_max_end_date
                ,g_start_date + ((97 * 7)+ 3*7) g_full_end_date
                , this_week_start_date
                , this_week_end_date
                , 1 start_sub
                , this_week_start_date + (7 * no_of_weeks) g_max_emp_period
                , dense_rank() over (partition by employee_id
                                              ,cycle_start_date
                                              ,no_of_weeks
                                              ,availability_start_date
                                              ,availability_end_date
                                              ,full_no_of_weeks
                                              ,min_cycle_this_wk_start_dt
                                              ,max_cycle_this_wk_start_dt
                                              ,max_cycle_this_wk_end_dt 
                                      order by this_week_start_date) wlrank
  from dwh_performance.TST_S4S_LOC_EMP_DY_part1Na A
/*where  employee_id in (
                                        '1002590',
                                        '1002355'
                                       ,'1002247'
                                        )
 */   order by employee_id
                  ,cycle_start_date
                  ,no_of_weeks
                  ,availability_start_date
                  ,availability_end_date
                  ,full_no_of_weeks
                  ,min_cycle_this_wk_start_dt
                  ,max_cycle_this_wk_start_dt
                  ,max_cycle_this_wk_end_dt
                  ,g_start_date  
                  ,g_end_date 
                  ,g_run_period_end_date
                  ,g_emp_max_end_date
                  ,g_start_date + ((97 * 7)+ 3*7) 
                  , this_week_start_date
                  , this_week_end_date
                  , start_sub
                  , this_week_start_date + (7 * no_of_weeks))
    select employee_id
                  ,cycle_start_date
                  ,no_of_weeks
                  ,availability_start_date
                  ,availability_end_date
                  ,full_no_of_weeks
                  ,min_cycle_this_wk_start_dt
                  ,max_cycle_this_wk_start_dt
                  ,max_cycle_this_wk_end_dt
         --         ,g_start_date  
        --          ,g_end_date 
         --         ,g_run_period_end_date
          --        ,g_emp_max_end_date
           --       , g_full_end_date
           --       , this_week_start_date
                  , this_week_end_date - (7 * no_of_weeks) + 1
                  , this_week_end_date
           --       , mod(wlrank,no_of_weeks) modrank
      from selext1
      where mod(wlrank,no_of_weeks) = 0
   order by EMPLOYEE_ID
            ,CYCLE_START_DATE
            ,NO_OF_WEEKS
            ,this_week_start_date; 
        
        
            g_recs :=SQL%ROWCOUNT ;
            COMMIT;
            g_recs_inserted := g_recs_inserted + g_recs;
         
            l_text := 'FINAL Recs inserted into   dwh_PERFORMANCE.TST_S4S_LOC_EMP_DY_part1 = '||g_recs_inserted;
            DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
/*EMPLOYEE_ID
ORIG_CYCLE_START_DATE
NO_OF_WEEKS
AVAILABILITY_START_DATE
AVAILABILITY_END_DATE
FULL_NO_OF_WEEKS
MIN_CYCLE_THIS_WK_START_DT
MAX_CYCLE_THIS_WK_START_DT
MAX_CYCLE_THIS_WK_END_DT
CYCLE_START_DATE
CYCLE_END_DATE
*/
    --    insert /*+ append */ into dwh_performance.TST_S4S_LOC_EMP_DY_part1Na 



 exception
      when dwh_errors.e_insert_error then
               l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               l_text := 'error in a3_insert_part1';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
               l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               l_text := 'error in a3_insert_part1';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a3_insert_part1;

--
--**************************************************************************************************
-- A4_COMPLETE_PART1 TABLE 
--------------------------
--
-- Step 4 : -- Add constraints and indexes back
--
--**************************************************************************************************
-- 
procedure a4_complete_part1 as
BEGIN

                l_text := 'ALTER TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part1 ADD CONSTRAINT PK_TST_S4S_LC_EMP_DY_PRT1';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                
                EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part1
                                            ADD CONSTRAINT PK_TST_S4S_LC_EMP_DY_PRT1
                                            PRIMARY KEY (EMPLOYEE_ID,AVAILABILITY_START_DATE,CYCLE_START_DATE) 
                                            USING INDEX TABLESPACE PRF_MASTER ENABLE') ;
                COMMIT;   
                
                l_text := 'Running GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TST_S4S_LOC_EMP_DY_PART1', DEGREE => 4);
                l_text := 'Completed GATHER_TABLE_STATS ON TST_S4S_LOC_EMP_DY_PART1';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
         EXCEPTION
                  when dwh_errors.e_insert_error then
                           l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
                           dwh_log.record_error(l_module_name,sqlcode,l_message);
                           l_text := 'error in a4_complete_part1';
                            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                   raise;
            
                  when others then
                           l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
                           dwh_log.record_error(l_module_name,sqlcode,l_message);
                           l_text := 'error in a4_complete_part1';
                            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                   raise;

end a4_complete_part1;


 
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
        
              l_text := 'TRUNCATE TABLE  dwh_performance.TST_S4S_LOC_EMP_DY_part1';
              dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
              EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part1');

              l_text := 'TRUNCATE TABLE  dwh_performance.TST_S4S_LOC_EMP_DY_part1NA';
              dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
              EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TST_S4S_LOC_EMP_DY_part1NA');
              
              a1_prepare_part1;
              a2_insert_part1na;
                  g_recs_inserted := 0;
                  g_recs_updated := 0;
                  G_RECS := 0;
              a3_insert_part1;
              a4_complete_part1;

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




END WH_PRF_S4S_004AA_NEW;
