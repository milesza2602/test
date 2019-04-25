--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004A_SS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004A_SS" (
   p_forall_limit   IN     INTEGER,
   p_success        OUT    BOOLEAN)
AS

--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
--
--
-- Redo (SS) 2018
--  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
--               Output   - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART0_SS, 
--                          dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS, 
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
    g_found            BOOLEAN;
    g_date             DATE;
    g_SUB              NUMBER := 0;
    g_end_date         DATE;
    g_dy_back_42days   date;
    g_start_date       date;
    g_end_sub          number;
    g_NAME             VARCHAR2(40);
    g_chk_day          VARCHAR2(20);
    

    l_message sys_dwh_errlog.log_text%TYPE;
    l_module_name sys_dwh_errlog.log_procedure_name%TYPE     := 'WH_PRF_S4S_004A_SS';
    l_name sys_dwh_log.log_name%TYPE                         := dwh_constants.vc_log_name_rtl_md;
    l_system_name sys_dwh_log.log_system_name%TYPE           := dwh_constants.vc_log_system_name_rtl_prf;
    l_script_name sys_dwh_log.log_script_name%TYPE           := dwh_constants.vc_log_script_rtl_prf_md;
    l_procedure_name sys_dwh_log.log_procedure_name%TYPE     := l_module_name;
    l_text sys_dwh_log.log_text%TYPE;
    l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD THE RTL_EMP_AVAIL_LOC_JOB_DY_SS data EX FOUNDATION';
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

     select distinct this_week_start_date ,this_week_start_date+ 20,this_week_start_date - 42
      into g_start_date, g_end_date,g_dy_back_42days
     from dim_calendar where calendar_date = g_date;

    l_text := 'END DATE BEING PROCESSED IS:- '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART1_SS              
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

    l_text := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT1_SS';
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
      
  l_text := 'TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS');
  
  SELECT TO_CHAR(sysdate, 'DAY') 
  into g_chk_day
  FROM dual; 
  
  l_text := 'Start :Insert dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS for a '||g_chk_day;
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);   
   
  IF g_chk_day = 'MONDAY' then   
      -- 2 year weekly rollup only on a Monday    
      INSERT /*+ APPEND */ into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS 
          select  
           /*+ full(fnd) parallel(fnd,8) full(DC) parallel(DC,8) */    
            distinct 
                 employee_id
               , cycle_start_date
               , no_of_weeks
               , availability_start_date
               , availability_end_date
               , this_week_start_date,this_week_end_date
               , sysdate LAST_UPDATED_DATE
        FROM dwh_foundation.FND_S4S_emp_avail_DY fnd
        left join  dim_calendar_wk dc on dc.this_week_start_date >= fnd.availability_start_date 
              and dc.this_week_start_date <= nvl(fnd.availability_end_date,G_END_DATE)
        where DC.this_week_start_date BETWEEN '26/jun/17' AND G_END_DATE; --weekly        
   ELSE
         -- 6 week daily rollup on all days except Monday
         INSERT /*+ APPEND */ into dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS 
          select  
           /*+ full(fnd) parallel(fnd,8) full(DC) parallel(DC,8) */    
            distinct 
                 employee_id
               , cycle_start_date
               , no_of_weeks
               , availability_start_date
               , availability_end_date
               , this_week_start_date,this_week_end_date
               , sysdate LAST_UPDATED_DATE
        FROM dwh_foundation.FND_S4S_emp_avail_DY fnd
        left join  dim_calendar_wk dc on dc.this_week_start_date >= fnd.availability_start_date 
              and dc.this_week_start_date <= nvl(fnd.availability_end_date,G_END_DATE)           
        where  DC.this_week_start_date BETWEEN g_dy_back_42days AND G_END_DATE;
    END IF;                  
       
    commit;
    
    l_text := 'End :Insert dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS for a '||g_chk_day;
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);   
    
      for v_cur in    ( select 
                            /*+ full(SE) parallel(SE,8) */ 
                               employee_id
                             , cycle_start_date
                             , no_of_weeks
                             , availability_start_date
                             , availability_end_date
                             , count(distinct this_week_start_date)  full_no_of_weeks
                             , min(this_week_start_date) min_cycle_this_wk_start_dt
                             , max(this_week_start_date) max_cycle_this_wk_start_dt
                             , max(this_week_end_date) max_cycle_this_wk_end_dt
                         FROM dwh_performance.TEMP_S4S_LOC_EMP_DY_PART0_SS  SE
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
                  
           INSERT /*+ APPEND */ INTO dwh_performance.TEMP_S4S_LOC_EMP_DY_PART1_SS
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

---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART2_SS
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;

      l_text := 'TRUNCATE TABLE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      EXECUTE IMMEDIATE ('TRUNCATE TABLE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS');

     g_name := null;         

  BEGIN
    SELECT index_NAME
       INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT2_SS'
    AND TABLE_NAME   = 'TEMP_S4S_LOC_EMP_DY_PART2_SS';

    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2_SS');
    COMMIT;
    
  EXCEPTION
      WHEN no_data_found THEN
        l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT2_SS does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

  BEGIN
    SELECT index_NAME
       INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I20_TMP_S4S_LC_EMP_DY_PRT2_SS'
    AND TABLE_NAME   = 'TEMP_S4S_LOC_EMP_DY_PART2_SS';

    l_text          := 'drop INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2_SS');
    COMMIT;
    
  EXCEPTION
      WHEN no_data_found THEN
        l_text := 'index I20_TMP_S4S_LC_EMP_DY_PRT2_SS does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;  

    l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART2_SS', DEGREE => 4);
    l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

---------------------------------------------------------------
-- STEP 4 : Insert into table TEMP_S4S_LOC_EMP_DY_PART2_SS
--
-- NOTES
--------
-- This step creates the list of employees with the following info :
-- a.) this_week_start_ and end_dates  = for all weeks in each AVAIL_CYCLE_START_DATE/AVAIL_CYCLE_END_DATE period
-- b.) RNK = numbered sequence starting at 1 for each week within period
-- /* - don't need this c.) rank_week_number = the sequence of the weeks within period
--           eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
---------------------------------------------------------------
      l_text := 'Starting insert into dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;
    
     INSERT /*+ APPEND */ INTO  dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS
        WITH SELEXT1 AS (SELECT /*+ FULL(FS) PARALLEL(SC,8) PARALLEL(FS,8)  */
                             DISTINCT  
                                 SC.EMPLOYEE_ID,
                                 SC.ORIG_CYCLE_START_DATE,
                                 SC.CYCLE_START_DATE,
                                 SC.CYCLE_END_DATE,
                                 FS.NO_OF_WEEKS,                                                          
                                 DC.THIS_WEEK_START_DATE,
                                 DC.THIS_WEEK_END_DATE,
                                 DC.FIN_YEAR_NO,
                                 DC.FIN_WEEK_NO,
                                 SC.AVAILABILITY_START_DATE,
                                 SC.AVAILABILITY_END_DATE
                          FROM DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART1_SS SC,
                               DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY FS,
                               DIM_CALENDAR DC
                          WHERE SC.EMPLOYEE_ID = FS.EMPLOYEE_ID
                            AND SC.AVAILABILITY_START_DATE = FS.AVAILABILITY_START_DATE
                            AND SC.ORIG_CYCLE_START_DATE = FS.CYCLE_START_DATE
                            AND DC.CALENDAR_DATE BETWEEN SC.CYCLE_START_DATE AND SC.CYCLE_END_DATE
                            --AND DC.FIN_YEAR_NO >= 2018
                          ORDER BY SC.EMPLOYEE_ID,
                                   ORIG_CYCLE_START_DATE,
                                   SC.CYCLE_START_DATE, 
                                   AVAILABILITY_START_DATE, 
                                   FIN_YEAR_NO, 
                                   FIN_WEEK_NO
                                 )
                                       ,
              SELEXT2  AS (SELECT /*+ FULL(SRK) PARALLEL(SRK,4) */
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
                           FROM (SELECT /*+ FULL(SELEXT1) PARALLEL(SELEXT1,4) */
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
                                  ORDER BY EMPLOYEE_ID,
                                           CYCLE_START_DATE,
                                           (RNK / NO_OF_WEEKS)
                                           ),
              SELVAL AS (  SELECT /*+ FULL(SRK) PARALLEL(SRK,4) */
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
                           FROM (SELECT  /*+ FULL(SELEXT2) PARALLEL(SELEXT2,4) */
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
                                  ORDER BY EMPLOYEE_ID,
                                           CYCLE_START_DATE,
                                           THIS_WEEK_START_DATE)
         SELECT 
            /*+ FULL(X) PARALLEL(X,4) */
             DISTINCT
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
                0,
                ORIG_CYCLE_START_DATE,
                AVAILABILITY_START_DATE, 
                AVAILABILITY_END_DATE
         FROM SELVAL X;
         
     g_recs_inserted :=0;
     g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'Recs inserted into   dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS = '||g_recs_inserted;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 

    l_text := 'create INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2_SS ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS (EMPLOYEE_ID, THIS_WEEK_START_DATE, CYCLE_START_DATE, AVAILABILITY_START_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT2_SS LOGGING NOPARALLEL') ;

    l_text := 'create INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2_SS ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS (EMPLOYEE_ID, CYCLE_START_DATE, WEEK_NUMBER)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I20_TMP_S4S_LC_EMP_DY_PRT2_SS LOGGING NOPARALLEL') ;
    
   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART2_SS', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      l_text := '----------------------------------------------------------------';
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_sub := 100;

      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;

      FOR v_cur IN (  SELECT /*+ FULL(a)  PARALLEL(a,6)   */ 
                         DISTINCT employee_id,
                             CYCLE_start_date,
                             AVAILABILITY_START_DATE,
                             this_week_start_date,
                             this_week_start_date_plus_20,
                             no_of_weeks
                        FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS a
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
         
        ---------------------------------------------------------------
        --Week number Calculation
        -- week_number = week_number within the cycle period
        -- eg,. if number_of_weeks = 4, then week1 = 0.25, week2 = 0.5, week3 = 0.75 week4 = 1, week5 = 1.25 etc..*/
        ---------------------------------------------------------------

         UPDATE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS p2
          SET p2.week_number = g_sub
         WHERE p2.employee_id = v_cur.employee_id
           and p2.cycle_start_date = v_cur.cycle_start_date
           and p2.availability_start_date = v_cur.availability_start_date
           AND p2.this_week_start_date = v_cur.this_week_start_date;
          g_recs :=SQL%ROWCOUNT ;
         COMMIT;
         g_recs_updated := g_recs_updated + g_recs;
         
         if g_recs_updated mod 50000 = 0 
          then l_text := 'Recs updated in dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS = '||g_recs_updated;
               dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
         end if;         

      END LOOP;
---------------------------------------------------------------
-- TEMP_S4S_LOC_EMP_DY_PART3_SS
---------------------------------------------------------------
     l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      g_recs_inserted := 0;

      l_text := 'TRUNCATE TABLE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      EXECUTE IMMEDIATE ('TRUNCATE TABLE dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS');
      COMMIT;

     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS'
    AND TABLE_NAME        = 'TEMP_S4S_LOC_EMP_DY_PART3_SS';
    
     l_text := 'alter table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3_SS drop constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3_SS drop constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'constraint PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

BEGIN
    SELECT index_NAME
     INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_TMP_S4S_LC_EMP_DY_PRT3_SS'
    AND TABLE_NAME   = 'TEMP_S4S_LOC_EMP_DY_PART3_SS';
    
    l_text          := 'drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I10_TMP_S4S_LC_EMP_DY_PRT3_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

  BEGIN
      SELECT index_NAME
       INTO G_name
      FROM DBA_indexes
      WHERE index_NAME = 'I40_TMP_S4S_LC_EMP_DY_PRT3_SS'
        AND TABLE_NAME   = 'TEMP_S4S_LOC_EMP_DY_PART3_SS';
        
      l_text := 'drop INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3_SS';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3_SS');
      COMMIT;
        
     EXCEPTION
        WHEN no_data_found THEN
         l_text := 'index I40_TMP_S4S_LC_EMP_DY_PRT3_SS does not exist';
         dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    END;  

    l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                    'TEMP_S4S_LOC_EMP_DY_PART2_SS', DEGREE => 4);
    l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART2_SS';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := '----------------------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      g_recs_inserted := 0;
      g_recs_updated := 0;
      g_recs := 0;
         
    INSERT /*+ APPEND */INTO dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS
    WITH selext1 AS (SELECT 
                       /*+ FULL(a)  PARALLEL(a,6)  */ 
                         DISTINCT EMPLOYEE_ID,
                                  ORIG_CYCLE_START_DATE,
                                  AVAILABILITY_START_DATE,
                                  AVAILABILITY_END_DATE ,
                                  CYCLE_START_DATE,
                                  CYCLE_END_DATE
                     FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS a
                     WHERE this_week_start_date BETWEEN orig_cycle_start_date AND NVL(availability_end_date, g_end_date)
                    ),
          selext2 AS
                  (SELECT 
                     /*+ FULL(tmp)  PARALLEL(tmp,6)  FULL(se1)  PARALLEL(se1,6)*/ 
                       DISTINCT tmp.EMPLOYEE_ID ,
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
                  FROM dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART2_SS tmp,
                       selext1 se1
                  WHERE (tmp.this_week_start_date BETWEEN tmp.orig_cycle_start_date AND NVL(tmp.availability_end_date, g_end_date)
                         OR tmp.this_week_start_date BETWEEN se1.cycle_start_date AND se1.cycle_end_date)
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
            /*+ FULL(se3)  PARALLEL(se3,6)*/  
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
    l_text          := 'Recs inserted in dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS = '||g_recs_inserted;
    
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        l_text := 'ALTER TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3_SS ADD CONSTRAINT PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
     EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.TEMP_S4S_LOC_EMP_DY_PART3_SS ADD CONSTRAINT PK_F_TEMP_S4S_LC_MP_DY_PRT3_SS
       PRIMARY KEY (EMPLOYEE_ID, THIS_WEEK_START_DATE, CYCLE_START_DATE, AVAILABILITY_START_DATE) USING INDEX TABLESPACE PRF_MASTER ENABLE') ;
        
    l_text := 'create INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3_SS';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3_SS ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS (EMPLOYEE_ID, CYCLE_START_DATE, WEEK_NUMBER)     
          TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
          Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_TMP_S4S_LC_EMP_DY_PRT3_SS LOGGING NOPARALLEL') ;
    
    l_text := 'create INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3_SS';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3_SS ON DWH_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_PART3_SS (EMPLOYEE_ID,AVAILABILITY_START_DATE,WEEK_NUMBER)     
          TABLESPACE PRF_MASTER NOLOGGING  PARALLEL(degree 8)');
          Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_TMP_S4S_LC_EMP_DY_PRT3_SS LOGGING NOPARALLEL') ;
    
   l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3_SS';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_S4S_LOC_EMP_DY_PART3_SS', DEGREE => 4);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3_SS';
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
   WHEN dwh_errors.e_insert_error THEN
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
          
END WH_PRF_S4S_004A_SS;
