--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_032U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_032U_TEST" 
 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE_CONSTRAINTS DAY information for Scheduling for Staff(S4S)
--                NB> constraint_start_date = MONDAY and constraint_end_date = any day of week
--
--               Delete process :
--                   Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and constraint_start_date
--
--                The delete lists are used in the rollups as well.
--                ie. FND_S4S_EMP_CONSTR_WK_del_list
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_CONSTR_WK
--               Output   - RTL_EMP_CONSTR_LOC_JOB_WK
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_cnt              number        :=  0;
g_rec_out            RTL_EMP_CONSTR_LOC_JOB_WK%rowtype;
g_found              boolean;
g_date               date;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_constr_end_date  date;
g_NAME VARCHAR2(40);
g_batch_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_032U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_EMP_CONSTR_LOC_JOB_WK data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Remove constraints and indexes
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_P_RTL_EMPCONSTLCJB_WK'
    AND TABLE_NAME        = 'RTL_EMP_CONSTR_LOC_JOB_WK';
    
    l_text               := 'alter table dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK drop constraint PK_P_RTL_EMPCONSTLCJB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK drop constraint PK_P_RTL_EMPCONSTLCJB_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_RTL_EMPCONSTLCJB_WK does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I30_RTL_EMPCONSTLCJB_WK'
    AND TABLE_NAME        = 'RTL_EMP_CONSTR_LOC_JOB_WK';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_EMPCONSTLCJB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_EMPCONSTLCJB_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index I30_RTL_EMPCONSTLCJB_WK does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I40_RTL_EMPCONSTLCJB_WK'
    AND TABLE_NAME        = 'RTL_EMP_CONSTR_LOC_JOB_WK';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_EMPCONSTLCJB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_EMPCONSTLCJB_WK');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I40_RTL_EMPCONSTLCJB_WK does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a_remove_indexes;
  
  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

   l_text := 'Insert into RTL_EMP_CONSTR_LOC_JOB_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
            INSERT
            /*+ append */
          INTO dwh_performance.rtl_emp_constr_loc_job_wk
                   WITH SELEXT1 AS
                          (SELECT
                            /*+ FULL(flr)   full(ej) full(el) PARALLEL(RTL,4) PARALLEL(ej,4) PARALLEL(elL,4)*/
                                  DISTINCT FLR.CONSTRAINT_start_DATE ,
                                  FLR.CONSTRAINT_END_DATE ,
                                  FLR.STRICT_MIN_HRS_PER_WK ,
                                  FLR.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                                  FLR.MIN_HRS_PER_WK ,
                                  FLR.MAX_HRS_PER_WK ,
                                  FLR.MAX_DY_PER_WK ,
                                  FLR.MAX_CONS_DAYS ,
                                  DE.SK1_EMPLOYEE_ID ,
                                  DC.FIN_YEAR_NO ,
                                  DC.FIN_WEEK_NO ,
                                  DC.this_week_start_date ,
                                  EJ.SK1_JOB_ID ,
                                  EL.SK1_LOCATION_NO ,
                                  EJ.EMPLOYEE_RATE ,
                                  EL.EMPLOYEE_STATUS ,
                                  EL.EMPLOYEE_WORKSTATUS ,
                                  el.effective_start_date ,
                                  el.effective_end_date
                          FROM FND_S4S_EMP_CONSTR_WK flr
                          JOIN DWH_HR_PERFORMANCE.dim_employee DE
                             ON FLR.EMPLOYEE_ID = DE.EMPLOYEE_ID
                          JOIN DIM_CALENDAR DC
                              ON DC.THIS_WEEK_START_DATE BETWEEN FLR.CONSTRAINT_START_DATE AND NVL(FLR.CONSTRAINT_END_DATE- 1, g_constr_end_date)
                                --                ON DC.THIS_WEEK_START_DATE BETWEEN FLR.CONSTRAINT_START_DATE  AND  NVL(FLR.CONSTRAINT_END_DATE- 1, '28/JUL/2014')
                          JOIN RTL_EMP_JOB_WK EJ
                                ON EJ.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                                AND EJ.FIN_YEAR_NO    = DC.FIN_YEAR_NO
                                AND EJ.FIN_WEEK_NO    = DC.FIN_WEEK_NO
                          JOIN RTL_EMP_LOC_STATUS_WK EL
                              ON EL.SK1_EMPLOYEE_ID = EJ.SK1_EMPLOYEE_ID
                              AND EL.FIN_YEAR_NO    = EJ.FIN_YEAR_NO
                              AND EL.FIN_WEEK_NO    = EJ.FIN_WEEK_NO
                          JOIN DIM_LOCATION DL
                              ON DL.SK1_LOCATION_NO = EL.SK1_LOCATION_NO
                                                         ),
                  selext2 AS
                            (SELECT
                              /*+ FULL(RTL)   PARALLEL(RTL,4) */
                                            DISTINCT SE1.CONSTRAINT_START_DATE ,
                                            SE1.CONSTRAINT_END_DATE ,
                                            SE1.STRICT_MIN_HRS_PER_WK ,
                                            SE1.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                                            SE1.MIN_HRS_PER_WK ,
                                            SE1.MAX_HRS_PER_WK ,
                                            SE1.MAX_DY_PER_WK ,
                                            SE1.MAX_CONS_DAYS ,
                                            SE1.SK1_EMPLOYEE_ID ,
                                            SE1.FIN_YEAR_NO ,
                                            SE1.FIN_WEEK_NO ,
                                            SE1.SK1_LOCATION_NO ,
                                            SE1.SK1_JOB_ID ,
                                            SE1.EMPLOYEE_RATE ,
                                            SE1.EMPLOYEE_STATUS ,
                                            SE1.EMPLOYEE_WORKSTATUS ,
                                            SE1.THIS_WEEK_START_DATE ,
                                            (
                                            CASE
                                              WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NULL      THEN SE1.CONSTRAINT_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NOT NULL      THEN SE1.CONSTRAINT_START_DATE
                                              ELSE NULL
                                                --SE1.availability_start_DATE - 1
                                            END) derive_start_date ,
                                            (
                                            CASE
                                              WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R')      AND se1.effective_end_date IS NULL      THEN NVL(SE1.CONSTRAINT_END_DATE- 1, g_constr_end_date)
                                              WHEN SE1.EMPLOYEE_STATUS         IN ('H','I','R')      AND se1.effective_end_date IS NOT NULL      THEN se1.effective_end_date
                                              ELSE NULL
                                                --SE1.availability_END_DATE - 1
                                            END) derive_end_date
                            FROM selext1 SE1
                            WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
                            ),
                  selext3 AS
                          (SELECT fin_year_no,
                                  fin_week_no,
                                  COUNT(DISTINCT SK1_employee_id) base_head_count
                          FROM selext2
                          WHERE employee_workstatus = 'A'
                          GROUP BY FIN_YEAR_NO,
                                    FIN_WEEK_NO
                          )
                SELECT DISTINCT  SE2.SK1_EMPLOYEE_ID ,SE2.SK1_JOB_ID ,SE2.SK1_LOCATION_NO ,
                              SE2.FIN_YEAR_NO ,
                              SE2.FIN_WEEK_NO ,
                              SE2.CONSTRAINT_START_DATE ,
                              SE2.CONSTRAINT_END_DATE ,
                              SE2.STRICT_MIN_HRS_PER_WK ,
                              SE2.MIN_HRS_BTWN_SHIFTS_PER_WK ,
                              SE2.MIN_HRS_PER_WK ,
                              SE2.MAX_HRS_PER_WK ,
                              SE2.MAX_DY_PER_WK ,
                              SE2.MAX_CONS_DAYS ,
                              (SE2.STRICT_MIN_HRS_PER_WK /40) BASE_FTE_WK ,
                              (SE2.EMPLOYEE_RATE * SE2.STRICT_MIN_HRS_PER_WK) BASE_COST_WK ,             
                              SE3.BASE_HEAD_COUNT, 
                              g_date
                FROM SELEXT2 SE2 ,
                      SELEXT3 SE3 
                WHERE se2.THIS_WEEK_START_DATE BETWEEN derive_start_date AND derive_end_date
                  ----and se2.tran_date between sp.cycle_end_date - (7*mCYCLEweeknum)+1 and sp.cycle_end_date
                AND derive_start_date  IS NOT NULL
                AND SE2.FIN_YEAR_NO     = SE3.FIN_YEAR_NO(+)
                AND SE2.FIN_WEEK_NO     = SE3.FIN_WEEK_NO(+)
                ORDER BY SE2.SK1_EMPLOYEE_ID
                ,SE2.SK1_JOB_ID
                ,SE2.SK1_LOCATION_NO
                ,SE2.FIN_YEAR_NO
                ,SE2.FIN_WEEK_NO;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
     
        L_TEXT := 'RTL_EMP_CONSTR_LOC_JOB_WK : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

--**************************************************************************************************
-- create primary key and index
--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_CONSTR_LOC_JOB_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_CONSTR_LOC_JOB_WK', DEGREE => 8);

     
      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_EMPCONSTLCJB_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_EMPCONSTLCJB_WK ON DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_EMPCONSTLCJB_WK LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_EMPCONSTLCJB_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_EMPCONSTLCJB_WK ON DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK (SK1_EMPLOYEE_ID, CONSTRAINT_START_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_EMPCONSTLCJB_WK LOGGING NOPARALLEL') ;
--** nb. check logging **
 
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c_add_indexes;

  --**************************************************************************************************
-- Check and write errors to delete table
--**************************************************************************************************
procedure d_delete_prf as
BEGIN
  g_recs_inserted := 0;
  SELECT MAX(run_seq_no) INTO g_run_seq_no
      FROM dwh_performance.prf_S4S_constrEMPLCjb_DEL_LIST
       WHERE batch_date = g_date;
       
  IF g_run_seq_no IS NULL THEN
      SELECT MAX(run_seq_no)
      INTO g_run_seq_no
      FROM dwh_performance.prf_S4S_constrEMPLCjb_DEL_LIST;
      IF g_run_seq_no IS NULL THEN
         g_run_seq_no  := 1;
      END IF;
  END IF;

  g_run_date := TRUNC(sysdate);

  BEGIN
    l_text := 'Insert into  DWH_PERFORMANCE.prf_S4S_constrEMPLCjb_DEL_LIST recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

    INSERT
    INTO dwh_performance.prf_S4S_constrEMPLCjb_DEL_LIST
    -- overlapping tran_date
        SELECT /*+ full(a) parallel(a,8) */
                      DISTINCT g_run_date, g_date,g_run_seq_no
                      ,a.SK1_EMPLOYEE_ID  ,a.SK1_JOB_ID ,a.SK1_LOCATION_NO,a.FIN_YEAR_NO,a.FIN_WEEK_NO
                      ,CONSTRAINT_START_DATE ,CONSTRAINT_END_DATE,STRICT_MIN_HRS_PER_WK
                      ,MIN_HRS_BTWN_SHIFTS_PER_WK,MIN_HRS_PER_WK,MAX_HRS_PER_WK,MAX_DY_PER_WK
                      ,MAX_CONS_DAYS,BASE_FTE_WK,BASE_COST_WK,BASE_HEAD_COUNT,LAST_UPDATED_DATE
        FROM dwh_performance.rtl_emp_constr_loc_job_wk a,
            (SELECT distinct sk1_employee_id
            from (
            SELECT /*+ full(a) parallel(a,8) */
                  sk1_employee_id,sk1_location_no, sk1_job_id, fin_year_no, fin_week_no,COUNT(*)
                  FROM dwh_performance.rtl_emp_constr_loc_job_wk
                  GROUP BY sk1_employee_id, sk1_location_no, sk1_job_id, fin_year_no, fin_week_no
                  HAVING COUNT(*) > 1)
              UNION
              -- more than one end_date = null
              SELECT DISTINCT sk1_employee_id
                  FROM dwh_hr_performance.dim_employee de,
                    (SELECT employee_id, COUNT(*)
                            FROM dwh_foundation.FND_S4S_EMP_constr_wk
                            WHERE constraint_end_date IS NULL
                            GROUP BY employee_id
                            HAVING COUNT(*) > 1
                    ) fnd
                WHERE fnd.employee_id = de.employee_id
              ) b
          
        WHERE a.sk1_employee_id = b.sk1_employee_id
        ;
        
        g_recs                 :=SQL%ROWCOUNT ;
        COMMIT;
        
        g_recs_inserted := g_recs;
        l_text          := 'Insert into prf_S4S_constrEMPLCjb_DEL_LIST recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      EXCEPTION
      WHEN no_data_found THEN
        l_text := 'No deletions done for DWH_PERFORMANCE.rtl_emp_constr_loc_job_wk ';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      END;
      
      g_recs_inserted :=0;
      g_recs_deleted  := 0;
      
   
    --**************************************************************************************************
    -- Check and write errors to delete table
    --**************************************************************************************************
    BEGIN
      l_text := 'Starting delete from rtl_emp_constr_loc_job_wk recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      DELETE
      FROM dwh_performance.rtl_emp_constr_loc_job_wk B
      WHERE EXISTS
              (SELECT *
              FROM dwh_performance.prf_S4S_constrEMPLCjb_DEL_LIST a
              WHERE run_seq_no      = g_run_seq_no
              AND B.SK1_employee_id = a.SK1_EMPLOYEE_ID
              AND B.SK1_location_no = a.SK1_location_no
              AND B.SK1_job_id = a.SK1_job_id
              );
              
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      
      g_recs_deleted := g_recs;
      l_text         := 'Deleted from DWH_PERFORMANCE.rtl_emp_constr_loc_job_wk recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
     
    g_recs_inserted := 0;
    g_recs_deleted  := 0;   
    EXCEPTION
    WHEN no_data_found THEN
                l_text := 'No deletions done for DWH_PERFORMANCE.rtl_emp_constr_loc_job_wk ';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    END;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end d_delete_prf;

--**************************************************************************************************
-- create primary key 
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_CONSTR_LOC_JOB_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_CONSTR_LOC_JOB_WK', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK add constraint PK_P_RTL_EMPCONSTLCJB_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK add CONSTRAINT PK_P_RTL_EMPCONSTLCJB_WK PRIMARY KEY (SK1_EMPLOYEE_ID,SK1_JOB_ID,SK1_LOCATION_NO,FIN_YEAR_NO,FIN_WEEK_NO)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
 
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end e_add_primary_key;
  --**************************************************************************************************
  --
  --
  --                    M  a  i  n    p  r  o  c  e  s  s
  --
  --
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  
  
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  l_text := 'LOAD OF RTL_EMP_LOC_STATUS_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  
  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  -- derivation of end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
    SELECT distinct THIS_WEEK_END_DATE into G_CONSTR_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = g_date + 20;
    
  l_text             := 'Derived G_CONSTR_end_date - '||G_CONSTR_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_CONSTR_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_CONSTR_WK', DEGREE => 8);

  l_text := 'truncate table DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate('truncate table dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK');

  a_remove_indexes;
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_CONSTR_LOC_JOB_WK';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_CONSTR_LOC_JOB_WK', DEGREE => 8);

  b_insert;

  c_add_indexes;

--  d_delete_prf;

  e_add_primary_key;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;



END WH_PRF_S4S_032U_TEST;
