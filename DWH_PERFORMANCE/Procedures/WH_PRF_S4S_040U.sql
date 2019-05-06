--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_040U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_040U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
--**************************************************************************************************
--  Date:        26 Feb 2019
--  Author:      Shuaib Salie ; Lisa Kriel
--  Purpose:     Load EMPLOYEE STATUS DY FACT information for Scheduling for Staff(S4S)
--                NB> effective_start_date = MONDAY and effective_end_date = any day in week - 1
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_LOC_STATUS
--               Output   - DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--                Date Processing :
--                ------------
--                  We should not be sent any records for an employee where the dates overlap for the effective periods.
--                  The effective_start_date can be any day but the effective_end_date will be sent as the effective_start_dtae of the next period 
--                     but we will subtract 1 day from it to derive the effective_end_date of the previous period.
--                  This all depends on the derivation criteria. 
--                  eg. RECORD 1 : effective_start_date = '1 jan 2015'  effective_end_date = '12 january 2015'
--                      RECORD 2 : effective_start_date = '12 jan 2015'  effective_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : effective_start_date = '1 jan 2015'  effective_end_date = '11 january 2015' **** note changed end_date
--                            RECORD 2 : effective_start_date = '12 jan 2015'  effective_end_date = NULL
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_tbc      INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_cnt           NUMBER  := 0;
  g_NAME          VARCHAR2(40);
  g_rec_out RTL_EMP_LOC_STATUS_DY%rowtype;
  g_found                BOOLEAN;
  g_date                 DATE;
  g_fin_days             NUMBER;
  g_eff_end_date         DATE;
    g_loop_start_date         DATE;
  
  
  g_run_date             DATE    := TRUNC(sysdate);
  g_run_seq_no           NUMBER  := 0;
  g_recs                 NUMBER  := 0;
  g_recs_deleted         INTEGER := 0;
  g_loop_fin_year_no   integer        :=  0;
  g_loop_fin_week_no   integer        :=  0;
  g_sub                integer        :=  0;
  g_loop_cnt           integer        :=  30; -- Number of partitions to be truncated/replaced (revert to 30)
  g_degrees            integer        :=  4;
  g_subpart_type       dba_part_tables.SUBPARTITIONING_TYPE%type; 
  g_subpart_column_name dba_subpart_key_columns.column_name%type;

  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_S4S_040U';
  l_table_name         all_tables.table_name%type                := 'RTL_EMP_LOC_STATUS_DY';
  l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE RTL_EMP_LOC_STATUS_DY data  EX FOUNDATION';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
 

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

   l_text := 'Insert into rtl_emp_loc_status_dy';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          INSERT /*+ append parallel (X,8)*/ INTO dwh_performance.rtl_emp_loc_status_dy X
        WITH SELEXT1 AS
                      ( SELECT DISTINCT
                            /*+ full(flr) parallel (flr,8) full(de) full(dl) */
                            de.SK1_EMPLOYEE_ID ,    dl.SK1_LOCATION_NO ,    flr.EMPLOYEE_STATUS ,    flr.EMPLOYEE_WORKSTATUS 
                            ,    flr.EFFECTIVE_START_DATE ,    
                            flr.EFFECTIVE_END_DATE,    
                           FIN_YEAR_NO,
                           FIN_WEEK_NO,
                           dc.calendar_date tran_date
                       FROM FND_S4S_EMP_LOC_STATUS flr
                      JOIN DWH_HR_PERFORMANCE.dim_employee DE
                            ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
                      JOIN DIM_LOCATION DL
                            ON DL.LOCATION_NO = FLR.LOCATION_NO
                      JOIN DIM_CALENDAR DC
                            ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                           -- or  DC.calendar_date BETWEEN FLR.EFFECTIVE_START_DATE AND NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                        where dc.calendar_date between g_loop_start_date and g_eff_end_date
                      ),
                      
          selext2 AS
                      (SELECT DISTINCT SK1_EMPLOYEE_ID ,                SK1_LOCATION_NO ,                EMPLOYEE_STATUS ,
                        FIN_YEAR_NO,
                        FIN_WEEK_NO,
                        tran_date,
                        EMPLOYEE_WORKSTATUS ,                EFFECTIVE_START_DATE ,                EFFECTIVE_END_DATE,
                        (  CASE WHEN SE1.EMPLOYEE_STATUS IN ('S')  THEN SE1.effective_START_DATE
                                WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R') AND se1.effective_end_date IS NULL THEN SE1.effective_START_DATE
                          WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R') AND se1.effective_end_date IS NOT NULL THEN SE1.effective_START_DATE
                          ELSE NULL
                            --SE1.availability_start_DATE - 1
                        END) derive_start_date ,
                        (CASE WHEN SE1.EMPLOYEE_STATUS IN ('S') THEN SE1.effective_START_DATE
                              WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R') AND se1.effective_end_date IS NULL THEN g_eff_end_date
                          WHEN SE1.EMPLOYEE_STATUS   IN ('H','I','R') AND se1.effective_end_date IS NOT NULL THEN se1.effective_end_date - 1
                          ELSE NULL
                            --SE1.availability_END_DATE - 1
                        END) derive_end_date
                      FROM selext1 SE1
                      WHERE SE1.EMPLOYEE_STATUS IN ('H','I','R', 'S')
                      )
        SELECT DISTINCT SK1_LOCATION_NO ,
                        SK1_EMPLOYEE_ID ,
                        FIN_YEAR_NO,
                        FIN_WEEK_NO,
                        TRAN_DATE ,
                        EMPLOYEE_STATUS ,
                        EMPLOYEE_WORKSTATUS ,
                        EFFECTIVE_START_DATE ,
                        EFFECTIVE_END_DATE ,
                        g_date LAST_UPDATED_DATE
        FROM selext2 se2
        WHERE se2.tran_DATE BETWEEN derive_start_date AND derive_end_date
        ORDER BY se2.SK1_LOCATION_NO ,
                  se2.SK1_EMPLOYEE_ID ,
                  se2.TRAN_DATE ;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
   
        g_recs_inserted := g_recs;          
        L_TEXT := 'rtl_emp_loc_status_dy : recs = '||g_recs;
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
  dwh_performance.dwh_s4s.write_initial_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type);
  
  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  -- derivation of end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
    SELECT distinct THIS_WEEK_END_DATE into G_EFF_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = g_date + 42;
    
  l_text             := 'Derived G_EFF_end_date - '||G_EFF_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
      --**************************************************************************************************
      -- Prepare environment
      --**************************************************************************************************
      EXECUTE immediate 'alter session enable parallel dml';
      execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';
      
--    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_LOC_STATUS';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_EMP_LOC_STATUS', DEGREE => 8);

      --**************************************************************************************************
      -- Disabling of FK constraints
      --**************************************************************************************************
      DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);
 
--*************************************************************************************************
-- Remove existing data by truncating partitions
--*************************************************************************************************        

    begin

       for g_sub in 0 .. g_loop_cnt+6
         loop 
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (G_EFF_end_date) - (g_sub * 7);  

           -- truncate subpartition
           DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO);
       end loop; 
       -- Insert range data
       b_insert;
    end;   
    
 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);

    
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                           l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
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


  END WH_PRF_S4S_040U;
