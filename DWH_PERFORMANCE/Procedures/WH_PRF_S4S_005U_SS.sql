--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_005U_SS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_005U_SS" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE JOB DY FACT information for Scheduling for Staff(S4S)
--                NB> job_start_date = MONDAY and job_end_date = SUNDAY
--
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_JOB
--               Output   - DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------   
--               NON-standard process
--                    due to the delete part taking too long to run,
--                    but this process will have to be re-evaluated at some stage as the runs time will get longer and longer
--               ie.
--               1. truncate performance day table (target table)
--               2. a_remove_indexes from performance day table (target table)
--               3. b_insert into performance day table (target table)
--               4. c_add_indexes back onto performance day table (target table)
--               5. e_add_primary_key back onto performance day table (target table)
--
--
--                Date Processing :
--                ------------
--                  We should not be sent any records for an employee where the dates overlap for the job periods.
--                  The job_start_date can be any day but the job_end_date will be sent as the job_start_date of the next period 
--                   This all depends on the derivation criteria.
--                  eg. RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015'
--                      RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015' 
--                            RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--  Maintenance:
--  wendy lyttle 13 may 2016  - excluding sk1_employee_id = 1089294  -- due to duplicate/overlapping info from source
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
g_CNT                number        :=  0;
g_rec_out            RTL_EMP_JOB_DY%rowtype;
g_found              boolean;
g_JOB_end_date       date;
g_name               varchar2(40);
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_005U_SS';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE EMPLOYEE JOB DY  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Remove constraints and indexes
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 
  BEGIN
    select CONSTRAINT_NAME
    into G_name
    from DBA_CONSTRAINTS
    where CONSTRAINT_NAME = 'PK_P_RTL_EMP_JB_DY_SS'
    and TABLE_NAME        = 'RTL_EMP_JOB_DY_SS';
    
    l_text               := 'alter table dwh_performance.RTL_EMP_JOB_DY_SS drop constraint PK_P_RTL_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_EMP_JOB_DY_SS drop constraint PK_P_RTL_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_RTL_EMP_JB_DY_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    select index_NAME
    into G_name
    from DBA_indexes
    where index_NAME = 'I30_RTL_EMP_JB_DY_SS'
    and TABLE_NAME   = 'RTL_EMP_JOB_DY_SS';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index PI30_RTL_EMP_JB_DY_SS does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    select index_NAME
    into G_name
    from DBA_indexes
    where index_NAME = 'I40_RTL_EMP_JB_DY_SS'
    and TABLE_NAME   = 'RTL_EMP_JOB_DY_SS';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_EMP_JB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_EMP_JB_DY_SS');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I40_RTL_EMP_JB_DY_SS does not exist';
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

        g_recs_inserted := 0;    
        g_recs := 0; 
 
  l_text := 'Insert into RTL_EMP_JOB_DY_SS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
          insert
            /*+ append */
          into dwh_performance.RTL_EMP_JOB_DY_SS
            WITH
            SELEXT1 as (
                       SELECT /*+ FULL(FLR) FULL(EL) PARALLEL(FLR,8) , PARALLEL(EL,8) */
                            DISTINCT  
                              DC.CALENDAR_DATE TRAN_DATE
                              ,DC.THIS_WEEK_START_DATE 
                              ,FLR.JOB_START_DATE
                              ,FLR.JOB_END_DATE
                              ,FLR.EMPLOYEE_RATE
                              ,DE.SK1_EMPLOYEE_ID
                              ,DP.SK1_PAYPOLICY_ID
                              ,DJ.SK1_JOB_ID
                              ,EL.EMPLOYEE_STATUS
                              ,el.effective_START_DATE
                              ,el.effective_end_DATE
                        FROM dwh_foundation.FND_S4S_EMP_JOB_SS flr
                        --FROM FND_S4S_EMP_JOB flr
                        JOIN DWH_HR_PERFORMANCE.dim_employee DE
                          ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
                        JOIN rtl_job_ss DJ--DIM_JOB DJ 
                          ON DJ.JOB_ID = FLR.JOB_ID
                         and flr.job_start_date between dj.sk1_effective_from_date and dj.sk1_effective_to_date
                        JOIN DIM_CALENDAR DC
                          --ON DC.CALENDAR_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_job_end_date) --G_CONSTR_END_DATE)
                           ON DC.THIS_WEEK_START_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
                      OR   DC.CALENDAR_DATE BETWEEN FLR.JOB_START_DATE  AND NVL(FLR.JOB_END_DATE, g_JOB_end_date) --G_CONSTR_END_DATE)
                        JOIN RTL_EMP_LOC_STATUS_DY EL
                          ON EL.SK1_EMPLOYEE_ID = DE.SK1_EMPLOYEE_ID
                         AND EL.TRAN_DATE = DC.CALENDAR_DATE
                        JOIN DIM_PAY_POLICY DP
                          ON DP.PAYPOLICY_ID = FLR.PAYPOLICY_ID
            ),
            selext2 as (
                       SELECT 
                            DISTINCT 
                              TRAN_DATE
                              ,JOB_START_DATE
                              ,JOB_END_DATE
                              ,EMPLOYEE_RATE
                              ,SK1_EMPLOYEE_ID
                              ,SK1_PAYPOLICY_ID
                              ,SK1_JOB_ID
                              ,EMPLOYEE_STATUS
                              ,(
                                CASE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('S')           THEN SE1.effective_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NULL      THEN SE1.job_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NOT NULL  THEN SE1.job_START_DATE
                                  ELSE NULL
                                END) derive_start_date ,
                               (
                                CASE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('S')           THEN SE1.effective_START_DATE
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NULL      THEN NVL(SE1.job_END_DATE, g_job_end_date)
                                  WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R')   AND se1.effective_end_date IS NOT NULL  THEN se1.effective_end_date
                                  ELSE NULL
                                END) derive_end_date
                         FROM selext1 SE1
                        WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
               )  
                        select 
                             distinct
                               SK1_EMPLOYEE_ID
                               ,SK1_JOB_ID
                               ,TRAN_DATE
                               ,JOB_START_DATE
                               ,JOB_END_DATE
                               ,EMPLOYEE_RATE
                               ,SK1_PAYPOLICY_ID
                               ,g_date LAST_UPDATED_DATE
                          from SELEXT2 SE2
                         where SE2.TRAN_DATE BETWEEN derive_start_date and derive_end_date    
            ;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
 
        g_recs_inserted := g_recs;          
        L_TEXT := 'RTL_EMP_JOB_DY_SS : recs = '||g_recs;
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
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_JOB_DY_SS', DEGREE => 8);

      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_EMP_JB_DY_SS ON DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_EMP_JB_DY_SS LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_EMP_JB_DY_SS ON DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS (SK1_EMPLOYEE_ID, TRAN_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_EMP_JB_DY_SS LOGGING NOPARALLEL') ;
   

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

        l_text          := 'Start delete';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
  g_recs_inserted := 0;
  select MAX(run_seq_no) into g_run_seq_no
      from dwh_performance.PRF_S4S_EMPJOB_DEL_LIST_SS
       where batch_date = g_date;
  IF g_run_seq_no IS NULL THEN
      select MAX(run_seq_no)
      into g_run_seq_no
      from dwh_performance.PRF_S4S_EMPJOB_DEL_LIST_SS;
      IF g_run_seq_no IS NULL THEN
         g_run_seq_no  := 1;
      END IF;
  END IF;

  g_run_date := TRUNC(sysdate);

  BEGIN

    INSERT
    into dwh_performance.PRF_S4S_EMPJOB_DEL_LIST_SS
    -- overlapping tran_date
            select /*+ full(a) parallel(a,8) */
                    distinct g_run_date, g_date,g_run_seq_no,
                    a.sk1_employee_id,a.sk1_JOB_ID ,TRAN_DATE ,
                    JOB_START_DATE ,JOB_END_DATE ,EMPLOYEE_RATE ,SK1_PAYPOLICY_ID ,
                    LAST_UPDATED_DATE
                  from dwh_performance.RTL_EMP_JOB_DY_SS a,
                    (select distinct sk1_employee_id,sk1_JOB_ID
                    from
                                (select /*+ full(a) parallel(a,8) */
                                sk1_employee_id,sk1_JOB_ID,tran_date,COUNT(*) 
                                from dwh_performance.RTL_EMP_JOB_DY_SS a
                                GROUP BY sk1_employee_id, sk1_JOB_ID,tran_date
                                HAVING COUNT(*) > 1
                                )
                    UNION
                    -- more than one end_date = null
                        select distinct sk1_employee_id, sk1_JOB_ID
                            from dwh_hr_performance.dim_employee de,
                                    rtl_job_ss dl, --dim_JOB dl,
                                    (select /*+ full(a) parallel(a,8) */ employee_id,JOB_ID,COUNT(*)
                                    from dwh_foundation.FND_S4S_EMP_JOB_SS a
                                    --from dwh_foundation.FND_S4S_EMP_JOB a
                                    where JOB_end_date IS NULL
                                    GROUP BY employee_id,JOB_ID
                                    HAVING COUNT(*) > 1
                                    ) fnd
                        where fnd.employee_id = de.employee_id
                        and fnd.JOB_ID        = dl.JOB_ID
                        ) b
                      where a.sk1_employee_id = b.sk1_employee_id
                      and a.sk1_JOB_ID        = b.sk1_JOB_ID;
    
        g_recs                 :=SQL%ROWCOUNT ;
        COMMIT;
        
        g_recs_inserted := g_recs;
        l_text          := 'Insert into PRF_S4S_EMPJOB_DEL_LIST_SS recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      EXCEPTION
      WHEN no_data_found THEN
        l_text := 'No deletions done for DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS ';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      END;
      
      g_recs_inserted :=0;
      g_recs_deleted  := 0;
      
   
    --**************************************************************************************************
    -- Check and write errors to delete table
    --**************************************************************************************************
    BEGIN
      l_text := 'Starting delete from RTL_EMP_JOB_DY_SS recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      DELETE
      from dwh_performance.RTL_EMP_JOB_DY_SS B
      where EXISTS
              (select *
              from dwh_performance.PRF_S4S_EMPJOB_DEL_LIST a
              where run_seq_no      = g_run_seq_no
              and B.SK1_employee_id = a.SK1_EMPLOYEE_ID
              );
              
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      
      g_recs_deleted := g_recs;
      l_text         := 'Deleted from DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
     
    g_recs_inserted := 0;
    g_recs_deleted  := 0;   
    EXCEPTION
    WHEN no_data_found THEN
                l_text := 'No deletions done for DWH_PERFORMANCE.RTL_EMP_JOB_DY_SS ';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    END;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
              l_text := 'error in d_delete_prf';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
              l_text := 'error in d_delete_prf';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end d_delete_prf;


--**************************************************************************************************
-- create primary key and index
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
    l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY_SS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_JOB_DY_SS', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_EMP_JOB_DY_SS add constraint PK_P_RTL_EMP_JB_DY_SS';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_EMP_JOB_DY_SS add CONSTRAINT PK_P_RTL_EMP_JB_DY_SS PRIMARY KEY (SK1_JOB_ID, SK1_EMPLOYEE_ID, TRAN_DATE)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
  
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
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
  IF p_forall_limit IS NOT NULL and p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  
  
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  l_text := 'LOAD OF RTL_EMP_JOB_DY_SS  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    select distinct THIS_WEEK_END_DATE into g_JOB_end_date
    from DIM_CALENDAR
    where CALENDAR_DATE = g_date + 20;
    
  l_text             := 'Derived g_job_end_date - '||g_job_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  
  l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_JOB';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_JOB', DEGREE => 8);

  l_text := 'truncate table dwh_performance.RTL_EMP_JOB_DY_SS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate('truncate table dwh_performance.RTL_EMP_JOB_DY_SS');

  a_remove_indexes;
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_JOB_DY_SS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_JOB_DY_SS', DEGREE => 8);

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



END WH_PRF_S4S_005U_SS;
