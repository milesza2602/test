--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004A" (
   p_forall_limit   IN     INTEGER,
   p_success        OUT    BOOLEAN)
AS
--  Date:        05 March 2019
--  Author:      Shuaib Salie and Lisa Kriel
--  Purpose:     Load EMPLOYEE_LOCATION_DAY information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
--               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART0, 
--                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1, 
--                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2,
--                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3
--  Packages:    dwh_constants, dwh_log, dwh_valid,dwh_s4s
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
    g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
    g_recs_read        INTEGER := 0;
    g_recs_inserted    INTEGER := 0;
    g_recs_updated     INTEGER := 0;
    g_recs             INTEGER := 0;
    g_recs_tbc         INTEGER := 0;
    g_error_count      NUMBER := 0;
    g_error_index      NUMBER := 0;
    g_count            NUMBER := 0;   
    g_found            BOOLEAN;
    g_date             DATE;
    g_SUB              NUMBER := 0;
    g_end_date         DATE;
    g_min_start_date   date;
    g_start_date       date;
    g_end_sub          number;
    g_chk_day          VARCHAR2(20);
    l_degrees          pls_integer := 4;

    l_message sys_dwh_errlog.log_text%TYPE;    
    l_procedure_name  sys_dwh_errlog.log_procedure_name%TYPE     := 'WH_PRF_S4S_004A';
    l_table_owner          all_tables.owner%type                  := 'DWH_PERFORMANCE';       
    l_name sys_dwh_log.log_name%TYPE                         := dwh_constants.vc_log_name_rtl_md;
    l_system_name sys_dwh_log.log_system_name%TYPE           := dwh_constants.vc_log_system_name_rtl_prf;
    l_script_name sys_dwh_log.log_script_name%TYPE           := dwh_constants.vc_log_script_rtl_prf_md;
    l_text sys_dwh_log.log_text%TYPE;
    l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD THE TEMP S4S data EX FOUNDATION';
    l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;

    p_success := false;
    dwh_performance.dwh_s4s.write_initial_log_data(l_procedure_name,l_description);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_min_start_date := g_date - 200;

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     select distinct this_week_start_date ,this_week_start_date+ 42
      into g_start_date, g_end_date
     from dim_calendar where calendar_date = g_date;

    l_text := 'DATE RANGE BEING PROCESSED IS:- '||g_min_start_date||' to '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART1              
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      g_recs_updated := 0;
      G_RECS := 0;

--    l_text := 'TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0';
--    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
--    EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0');
    dwh_performance.dwh_s4s.truncate_table(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART0', l_table_owner);    
    dwh_performance.dwh_s4s.disable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART0', l_table_owner, l_degrees);         


  l_text := 'Start :Insert dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0';
  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);   

       INSERT /*+ APPEND parallel (X,l_degrees)*/ into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0 X
          select  
           /*+ full(fnd) parallel(fnd,l_degrees) full(DC) parallel(DC,l_degrees) */    
            distinct 
                 employee_id
               , cycle_start_date
               , no_of_weeks
               , availability_start_date
               , availability_end_date
               , this_week_start_date,this_week_end_date
               , sysdate LAST_UPDATED_DATE
        FROM dwh_foundation.FND_S4S_emp_avail_DY fnd
        left join  dim_calendar_wk dc on dc.this_week_end_date > fnd.availability_start_date 
              and dc.this_week_start_date < nvl(fnd.availability_end_date,G_END_DATE)        
        where DC.this_week_end_date > g_min_start_date ;                   
    g_recs :=SQL%ROWCOUNT ;
    commit;
 
    l_text := 'End :Insert dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0 : Records '||g_recs;
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);   
    dwh_performance.dwh_s4s.enable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART0', l_table_owner);   
    

    dwh_performance.dwh_s4s.truncate_table(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART1', l_table_owner);    
    dwh_performance.dwh_s4s.disable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART1', l_table_owner, l_degrees);  

      for v_cur in    ( select 
                            /*+ full(SE) parallel(SE,l_degrees) */ 
                               employee_id
                             , cycle_start_date
                             , no_of_weeks
                             , availability_start_date
                             , availability_end_date
                             , count(distinct this_week_start_date)  full_no_of_weeks
                             , min(this_week_start_date) min_cycle_this_wk_start_dt
                             , max(this_week_start_date) max_cycle_this_wk_start_dt
                             , max(this_week_end_date) max_cycle_this_wk_end_dt
                         FROM dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0  SE
                         group by employee_id
                                , cycle_start_date
                                , no_of_weeks
                                , availability_start_date
                                , availability_end_date
                         order by cycle_start_date, availability_start_date
                       ) 
      loop      
            g_start_date := null;
            g_end_date := v_cur.min_cycle_this_wk_start_dt-1;
            g_sub := 0;
            G_END_SUB := round(v_cur.full_no_of_weeks/v_cur.no_of_weeks);

          for g_sub in 0..G_END_SUB 
           loop
                  g_start_date := g_end_date + 1;
                  g_end_date   := g_start_date + (v_cur.no_of_weeks * 7) - 1;

           INSERT /*+ APPEND parallel (X,l_degrees) */ INTO dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1 X
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
               g_recs_inserted := g_recs_inserted + g_recs;

              if g_recs_inserted mod 50000 = 0 
                then 
                     l_text := 'Recs inserted into dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1 = '||g_recs_inserted;
                     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               end if;             

           END LOOP;      
      end loop;
      g_recs_read := g_recs_inserted; -- final log data
      l_text := 'FINAL Recs inserted into dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1 = '||g_recs_inserted;
      DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
          dwh_performance.dwh_s4s.enable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART1', l_table_owner);


      l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART1', DEGREE => l_degrees);
     l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART2
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;

     dwh_performance.dwh_s4s.truncate_table(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART2', l_table_owner);    
     dwh_performance.dwh_s4s.disable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART2', l_table_owner, l_degrees); 

---------------------------------------------------------------
-- Insert into table TEMP_S4S_LOC_EMP_DY_PART2
--
-- NOTES
--------
-- This step creates the list of employees with the following info :
-- a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
-- b.) RNK = numbered sequence starting at 1 for each week within period
-- /* - don't need this c.) rank_week_number = the sequence of the weeks within period
--           eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
---------------------------------------------------------------

      l_text := 'Starting insert into DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;

     INSERT /*+ APPEND parallel (X,l_degrees)*/ INTO  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2 X
         WITH SELEXT1 AS (SELECT /*+ FULL(SC) PARALLEL(SC,l_degrees)   */                      
                                 SC.EMPLOYEE_ID,
                                 SC.ORIG_CYCLE_START_DATE,
                                 SC.CYCLE_START_DATE,
                                 SC.CYCLE_END_DATE,
                                 SC.NO_OF_WEEKS,                                                          
                                 DC.THIS_WEEK_START_DATE,
                                 DC.THIS_WEEK_END_DATE,
                                 DC.FIN_YEAR_NO,
                                 DC.FIN_WEEK_NO,
                                 SC.AVAILABILITY_START_DATE,
                                 SC.AVAILABILITY_END_DATE
                          FROM DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1 SC,                        
                               DIM_CALENDAR_WK DC                   
                            where  DC.THIS_WEEK_START_DATE BETWEEN SC.CYCLE_START_DATE AND SC.CYCLE_END_DATE                     
                                 and DC.this_week_end_date > g_min_start_date  
                            and  DC.this_week_start_date <  g_end_date 
                                 )
                                       ,
              SELEXT2  AS (SELECT /*+ FULL(SRK) PARALLEL(SRK,l_degrees) */
                                       EMPLOYEE_ID,
                                       ORIG_CYCLE_START_DATE,
                                       CYCLE_START_DATE,
                                       CYCLE_END_DATE,
                                       AVAILABILITY_START_DATE,
                                       AVAILABILITY_END_DATE,
                                       NO_OF_WEEKS,
                                       THIS_WEEK_START_DATE,
                                       THIS_WEEK_END_DATE ,
                                       FIN_YEAR_NO,
                                       FIN_WEEK_NO,
                                       RNK ,
                                       (RNK / NO_OF_WEEKS) RANK_WEEK_NUMBER
                           FROM (SELECT /*+ FULL(SELEXT1) PARALLEL(SELEXT1,l_degrees) */
                                               EMPLOYEE_ID,
                                               ORIG_CYCLE_START_DATE,
                                               CYCLE_START_DATE,
                                               CYCLE_END_DATE,
                                               AVAILABILITY_START_DATE,
                                               AVAILABILITY_END_DATE,
                                               NO_OF_WEEKS,
                                               THIS_WEEK_START_DATE,
                                               THIS_WEEK_END_DATE,
                                               FIN_YEAR_NO,
                                               FIN_WEEK_NO,                                               
                                               DENSE_RANK ()
                                               OVER (
                                                  PARTITION BY EMPLOYEE_ID,
                                                         CYCLE_START_DATE, AVAILABILITY_START_DATE
                                                  ORDER BY THIS_WEEK_START_DATE)
                                                  RNK
                                          FROM SELEXT1) SRK
                                           ),
              SELVAL AS (  SELECT /*+ FULL(SRK) PARALLEL(SRK,l_degrees) */
                                     EMPLOYEE_ID,
                                     ORIG_CYCLE_START_DATE,
                                     AVAILABILITY_START_DATE,
                                     AVAILABILITY_END_DATE,
                                     CYCLE_START_DATE,
                                     CYCLE_END_DATE,
                                     NO_OF_WEEKS,
                                     THIS_WEEK_START_DATE,
                                     THIS_WEEK_END_DATE ,
                                     FIN_YEAR_NO,
                                     FIN_WEEK_NO,
                                     SRK.RNK
                           FROM (SELECT  /*+ FULL(SELEXT2) PARALLEL(SELEXT2,l_degrees) */
                                            EMPLOYEE_ID,
                                           CYCLE_START_DATE,
                                            CYCLE_END_DATE,
                                            ORIG_CYCLE_START_DATE,
                                            AVAILABILITY_START_DATE,
                                            AVAILABILITY_END_DATE,
                                            NO_OF_WEEKS,
                                            THIS_WEEK_START_DATE,
                                            THIS_WEEK_END_DATE,
                                            FIN_YEAR_NO,
                                            FIN_WEEK_NO,
                                            RANK_WEEK_NUMBER,                                     
                                            DENSE_RANK () OVER (PARTITION BY EMPLOYEE_ID,CYCLE_START_DATE,ORIG_CYCLE_START_DATE, 
                                                                             AVAILABILITY_START_DATE,NO_OF_WEEKS
                                                                ORDER BY RANK_WEEK_NUMBER
                                                                ) RNK
                                              FROM SELEXT2) SRK
                                           )
         SELECT 
            /*+ FULL(X) PARALLEL(X,l_degrees) */  
                EMPLOYEE_ID,
                CYCLE_START_DATE,
                CYCLE_END_DATE,
                NO_OF_WEEKS,
                THIS_WEEK_START_DATE,
                THIS_WEEK_END_DATE ,
                FIN_YEAR_NO,
                FIN_WEEK_NO,
                RNK EMP_LOC_RANK,
                CYCLE_START_DATE + 20 CYCLE_START_DATE_PLUS_20,
                THIS_WEEK_START_DATE + 20 THIS_WEEK_START_DATE_PLUS_20,
                ((this_week_start_date-CYCLE_start_date)/7)+1 week_number, --week number formulae
                ORIG_CYCLE_START_DATE,
                AVAILABILITY_START_DATE, 
                AVAILABILITY_END_DATE
         FROM SELVAL X;
        
       g_recs_inserted :=0;
     g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'Recs inserted into   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2 = '||g_recs_inserted;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 
     dwh_performance.dwh_s4s.enable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART2', l_table_owner);
     
    l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART2', DEGREE => l_degrees);
    l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART3
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      
      dwh_performance.dwh_s4s.truncate_table(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART3', l_table_owner);    
      dwh_performance.dwh_s4s.disable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART3', l_table_owner, l_degrees); 

    l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;

    INSERT /*+ APPEND parallel (X,l_degrees)*/ INTO dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3 X
    WITH selext1 AS (SELECT 
                       /*+ FULL(a)  PARALLEL(a,l_degrees)  */ 
                         DISTINCT
                         EMPLOYEE_ID,
                                  ORIG_CYCLE_START_DATE,
                                  AVAILABILITY_START_DATE,
                                  AVAILABILITY_END_DATE ,
                                  CYCLE_START_DATE,
                                  CYCLE_END_DATE
                     FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2 a
                     WHERE this_week_start_date BETWEEN orig_cycle_start_date AND NVL(availability_end_date, g_end_date)
                    ),
          selext2 AS
                  (SELECT 
                     /*+ FULL(tmp)  PARALLEL(tmp,l_degrees)  FULL(se1)  PARALLEL(se1,l_degrees)*/ 
                       DISTINCT 
                       tmp.EMPLOYEE_ID ,
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
                  FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2 tmp,
                       selext1 se1
                    WHERE tmp.THIS_WEEK_END_DATE      > se1.cycle_start_date
                      and tmp.THIS_WEEK_START_DATE    < se1.cycle_end_date                 
                      AND tmp.employee_id             = se1.employee_id
                      AND tmp.orig_cycle_start_date   = se1.orig_cycle_start_date
                      AND tmp.availability_start_date = se1.availability_start_date
                      AND tmp.cycle_start_date        = se1.cycle_start_date
                  ),
        selext3 as (SELECT
                       DISTINCT 
                          EMPLOYEE_ID ,
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
                    ORDER BY EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE
                    ),

          seldup as (SELECT DISTINCT EMPLOYEE_ID FROM 
                         (select EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE, count(*) from selext3
                           group by EMPLOYEE_ID,THIS_WEEK_START_DATE,CYCLE_START_DATE,AVAILABILITY_START_DATE
                          having count(*) > 1)
                     )

          select 
            /*+ FULL(se3)  PARALLEL(se3,l_degrees)*/  
              se3.EMPLOYEE_ID ,
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
    l_text          := 'Recs inserted in dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3 = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_performance.dwh_s4s.enable_primary_key(l_procedure_name, 'TEMP_S4S_LOC_EMP_DY_PART3', l_table_owner);      
    
   l_text := '----------------------------------------------------------------';
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   g_recs_inserted := 0;
   g_recs_updated := 0;
   g_recs := 0;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
  DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_procedure_name,l_description,g_recs_read,g_recs_inserted,g_recs_updated);
  COMMIT;
  p_success := TRUE;

EXCEPTION
   WHEN dwh_errors.e_insert_error THEN
      l_message := dwh_constants.vc_err_mm_insert || SQLCODE || ' ' || SQLERRM;
      dwh_log.record_error (l_procedure_name, SQLCODE, l_message);
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
      ROLLBACK;
      p_success := FALSE;
      RAISE;
    WHEN OTHERS THEN
          l_message := dwh_constants.vc_err_mm_other || SQLCODE || ' ' || SQLERRM;
          dwh_log.record_error (l_procedure_name, SQLCODE, l_message);
          dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
          ROLLBACK;
          p_success := FALSE;
          RAISE;

END WH_PRF_S4S_004A;
