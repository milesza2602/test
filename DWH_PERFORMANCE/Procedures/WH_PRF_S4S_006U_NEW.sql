--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_006U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_006U_NEW" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        07 Feb 2018
--  Author:      Shuaib Salie ; Lisa Kriel
--  Purpose:     Load payment_category information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_LOC_EMP_JOB_PAYCAT_DY
--               Output   - DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Maintenance:
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
g_rec_out            RTL_LOC_EMP_JOB_PAYCAT_DY%rowtype;
g_rec_cnt            number        := 0;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
G_NAME               VARCHAR2(40);

g_loop_fin_year_no   number        :=  0;
g_loop_fin_week_no   number        :=  0;
g_sub                integer       :=  0;
g_loop_cnt           integer       :=  0;
g_wkday              number        :=  0;  
g_loop_start_date    date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_006U_NEW';
l_table_name         all_tables.table_name%type                := 'RTL_LOC_EMP_JOB_PAYCAT_DY';
l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||' data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************

procedure b_insert as
BEGIN

        g_recs_inserted := 0;    
        g_recs := 0; 

  l_text := 'Insert into '||l_table_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  INSERT /*+ APPEND parallel (X,6) */  INTO dwh_performance.RTL_LOC_EMP_JOB_PAYCAT_DY X
  SELECT /*+ FULL(FLR) Parallel(flr,8) */             
                SK1_LOCATION_NO,
                SK1_EMPLOYEE_ID,
                sk1_JOB_ID, 
                sk1_payment_category_no,
                dc.fin_year_no,
                dc.fin_week_no,
                flr.PAY_WEEK_DATE,
                flr.BUSINESS_DATE,
                flr.LAST_MODIFIED_DATE,
                flr.ACTUAL_HRS,
                g_date last_updated_date             
           FROM  dwh_foundation.FND_S4S_LOC_EMP_JOB_PAYCAT_DY flr
                ,dwh_performance.DIM_LOCATION DL
                ,dwh_hr_performance.dim_employee DE               
                ,dwh_performance.DIM_JOB DJ
                ,dwh_performance.dim_payment_category dpc     
                ,dwh_performance.dim_calendar dc 
          WHERE FLR.LOCATION_NO         = DL.LOCATION_NO
            AND FLR.EMPLOYEE_ID         = DE.EMPLOYEE_ID
            AND FLR.JOB_ID              = DJ.JOB_ID
            AND FLR.PAYMENT_CATEGORY_NO = DPC.PAYMENT_CATEGORY_NO            
            AND flr.BUSINESS_DATE between dj.sk1_effective_from_date and  dj.sk1_effective_to_date
            and flr.pay_week_date = dc.calendar_date
            and dc.fin_year_no = g_loop_fin_year_no
            and dc.fin_week_no = g_loop_fin_week_no        ;

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;

        g_recs_inserted := g_recs;         
        L_TEXT := L_table_name||' : recs = '||g_recs ||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
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
  --                    M  a  i  n    p  r  o  c  e  s  s
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;

  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'LOAD OF RTL_EMP_JOB_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --**************************************************************************************************
-- Only process if any data has come through in this batch.
--**************************************************************************************************
  g_rec_cnt := 0;
   --Select count(*) into g_rec_cnt from dwh_foundation.FND_S4S_LOC_EMP_JOB_PYCT_DY where last_updated_date > '25/Jun/2017';--= g_date;
  Select count(*) into g_rec_cnt from dwh_foundation.FND_S4S_LOC_EMP_JOB_PAYCAT_DY where last_updated_date = g_date;--!!!!!!
  If g_rec_cnt  > 0 then 

       l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
--            --**************************************************************************************************
--            -- Prepare environment
--            --**************************************************************************************************
      
    
        execute immediate 'alter session enable parallel dml';
        execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';
        
         --set extract period 
        select today_fin_day_no into g_wkday from dwh_performance.dim_control_report;   
--            EXECUTE immediate 'alter session set workarea_size_policy=manual';
--            EXECUTE immediate 'alter session set sort_area_size=100000000';
--            EXECUTE immediate 'alter session enable parallel dml';
--
--            l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_LOC_EMP_JOB_PAYCAT_DY';
--            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--            DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_S4S_LOC_EMP_JOB_PAYCAT_DY', DEGREE => 8);

    if g_wkday = 1 then
       g_loop_cnt := 25;
    else 
       g_loop_cnt := 26;
    end if;
    
   --A. Remove previous load periods for reload ...
    begin
       for g_sub in 1 .. g_loop_cnt                                                      -- BK30Sep2016                                                
         loop         
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (g_date) - (g_sub * 7);  
   --**************************************************************************************************
  -- Truncate Table Partition
  --**************************************************************************************************       
         execute immediate 'alter table '|| L_table_owner || '.'|| l_table_name ||' truncate subpartition for ('||G_LOOP_FIN_YEAR_NO||','||G_LOOP_FIN_WEEK_NO||')';
         l_text := 'Truncate Partition = '||g_loop_fin_year_no||' - '||g_loop_fin_week_no;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         commit;  
         
         b_insert;    
         commit;
         
         end loop;
    end;  


            l_text := 'Running GATHER_TABLE_STATS ON '||l_table_name;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            DBMS_STATS.gather_table_stats (L_table_owner, l_table_name, DEGREE => 8);

           -- b_insert;                       

      end if;

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

END WH_PRF_S4S_006U_NEW;
