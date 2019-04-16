--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_278US
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_278US" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Alastair de Wet
--  Purpose:     TAKE ON OR RESTRUCTURE DEPTH AND BREADTH AT GROUP LEVEL
--  Tables:      Input  - cust_basket_item  
--               Output - cust_db_company_month
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--   
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;
g_sub                integer;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_yr_loop             number;
g_mn_loop             number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;
g_run_date            date;
g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_278U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TAKEON/RESTRUCTURE cust_db_company_month EX cust_basket_item';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

TYPE df_array IS TABLE OF date
 INDEX BY BINARY_INTEGER;
 date_from df_array;
TYPE dt_array IS TABLE OF date
 INDEX BY BINARY_INTEGER;
 date_to   dt_array;

filter_lst  DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB(); 
 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 
    execute immediate 'alter session enable parallel dml';
    
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'TAKEON OF cust_db_company_month EX cust_basket_item STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

    select last_yr_fin_year_no,last_mn_fin_month_no 
    into   g_yr_00,g_mn_00 
    from dim_control;
    
    select unique this_mn_start_date,this_mn_end_date
    into   g_this_mn_start_date, g_this_mn_end_date
    from   dim_calendar
    where  fin_year_no  = g_yr_00 and 
           fin_month_no = g_mn_00 and
           fin_day_no   = 1;
   
   g_last_mn := g_mn_00 - 1;
   g_last_yr := g_yr_00;
   if g_last_mn = 0 then
      g_last_mn := 12;
      g_last_yr := g_last_yr - 1;
   end if;   

   l_text := 'Month being processed:= '|| g_this_mn_start_date || g_this_mn_end_date ||g_yr_00||g_mn_00; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
   g_run_date := g_this_mn_end_date + 5;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
--      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



   l_text := 'UPDATE STATS ON CUST_DB_COMPANY_MONTH'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_DB_COMPANY_MONTH';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto');

   COMMIT;

-- SET UP DATE RANGES THAT HAVE TO BE PROCESSED -- 
   g_mn_loop := g_mn_00;
   g_yr_loop := g_yr_00;


   

   
    l_text      := 'TRUNCATE ALL TEMP TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_COMPANY_MONTH_1';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_COMPANY_MONTH_2';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_COMPANY_MONTH_3';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_COMPANY_MONTH_4';
    execute immediate g_stmt;     
--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true; 
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;       
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
 
END WH_PRF_CUST_278US;
