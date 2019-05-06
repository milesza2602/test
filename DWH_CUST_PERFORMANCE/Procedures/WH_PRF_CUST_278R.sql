--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_278R
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_278R" (p_forall_limit in integer,p_success out boolean) AS 

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

--filter_lst  DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB(); 
 
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

-- CLEAR TABLES DOWN ---
--   g_stmt      := 'Alter table  DWH_CUST_PERFORMANCE.CUST_DB_company_MONTH truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
--   l_text      := g_stmt;
    l_text      := 'TRUNCATE ALL TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.CUST_DB_COMPANY_MONTH';
    execute immediate g_stmt;  


   l_text := 'UPDATE STATS ON ALL TABLES'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_1',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_2',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_3',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_4',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_COMPANY_MONTH',estimate_percent=>1, DEGREE => 32);
/*
    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_DB_COMPANY_MONTH';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto');
*/
   COMMIT;

-- SET UP DATE RANGES THAT HAVE TO BE PROCESSED -- 
   g_mn_loop := g_mn_00;
   g_yr_loop := g_yr_00;


   
   l_text := 'UPDATE STATS ON TEMP TABLES'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_1',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_2',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_3',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_COMPANY_MONTH_4',estimate_percent=>1, DEGREE => 32);

   COMMIT;
   l_text := 'MAIN INSERT START'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
   insert /*+ APPEND parallel (prf,12) */ into cust_db_company_month  prf
   with        cust_union_all as
   (
   select  /*+ FULL(t1)  parallel (t1,8)  */  *   from temp_db_company_month_1  t1
   union all
   select  /*+ FULL(t2)  parallel (t2,8)  */  *   from temp_db_company_month_2  t2
   union all
   select  /*+ FULL(t3)  parallel (t3,8)  */  *   from temp_db_company_month_3  t3
   union all
   select  /*+ FULL(t4)  parallel (t4,8)  */  *   from temp_db_company_month_4  t4
   )         
   select /*+ FULL(cua)  parallel (cua,8)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            company_no,
            max(customer_no) customer_no,
            sum(	num_item_101	),
            sum(	sales_101	),
            sum(	num_visit_101	),
            sum(	num_item_102	),
            sum(	sales_102	),
            sum(	num_visit_102	),
            sum(	num_item_103	),
            sum(	sales_103	),
            sum(	num_visit_103	),
            sum(	num_item_104	),
            sum(	sales_104	),
            sum(	num_visit_104	),
            sum(	num_item_105	),
            sum(	sales_105	),
            sum(	num_visit_105	),
            sum(	num_item_106	),
            sum(	sales_106	),
            sum(	num_visit_106	),
            sum(	num_item_107	),
            sum(	sales_107	),
            sum(	num_visit_107	),
            sum(	num_item_108	),
            sum(	sales_108	),
            sum(	num_visit_108	),
            sum(	num_item_109	),
            sum(	sales_109	),
            sum(	num_visit_109	),
            sum(	num_item_110	),
            sum(	sales_110	),
            sum(	num_visit_110	),
            sum(	num_item_111	),
            sum(	sales_111	),
            sum(	num_visit_111	),
            sum(	num_item_112	),
            sum(	sales_112	),
            sum(	num_visit_112	),
            sum(	num_item_201	),
            sum(	sales_201	),
            sum(	num_visit_201	),
            sum(	num_item_202	),
            sum(	sales_202	),
            sum(	num_visit_202	),
            sum(	num_item_203	),
            sum(	sales_203	),
            sum(	num_visit_203	),
            sum(	num_item_204	),
            sum(	sales_204	),
            sum(	num_visit_204	),
            sum(	num_item_205	),
            sum(	sales_205	),
            sum(	num_visit_205	),
            sum(	num_item_206	),
            sum(	sales_206	),
            sum(	num_visit_206	),
            sum(	num_item_207	),
            sum(	sales_207	),
            sum(	num_visit_207	),
            sum(	num_item_208	),
            sum(	sales_208	),
            sum(	num_visit_208	),
            sum(	num_item_209	),
            sum(	sales_209	),
            sum(	num_visit_209	),
            sum(	num_item_210	),
            sum(	sales_210	),
            sum(	num_visit_210	),
            sum(	num_item_211	),
            sum(	sales_211	),
            sum(	num_visit_211	),
            sum(	num_item_212	),
            sum(	sales_212	),
            sum(	num_visit_212	),
            sum(	num_item_301	),
            sum(	sales_301	),
            sum(	num_visit_301	),
            sum(	num_item_302	),
            sum(	sales_302	),
            sum(	num_visit_302	),
            sum(	num_item_303	),
            sum(	sales_303	),
            sum(	num_visit_303	),
            sum(	num_item_304	),
            sum(	sales_304	),
            sum(	num_visit_304	),
            sum(	num_item_305	),
            sum(	sales_305	),
            sum(	num_visit_305	),
            sum(	num_item_306	),
            sum(	sales_306	),
            sum(	num_visit_306	),
            sum(	num_item_307	),
            sum(	sales_307	),
            sum(	num_visit_307	),
            sum(	num_item_308	),
            sum(	sales_308	),
            sum(	num_visit_308	),
            sum(	num_item_309	),
            sum(	sales_309	),
            sum(	num_visit_309	),
            sum(	num_item_310	),
            sum(	sales_310	),
            sum(	num_visit_310	),
            sum(	num_item_311	),
            sum(	sales_311	),
            sum(	num_visit_311	),
            sum(	num_item_312	),
            sum(	sales_312	),
            sum(	num_visit_312	),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,company_no
   ;
   
   
   g_recs_inserted         := g_recs_inserted + sql%rowcount;
   commit;

   l_text := 'UPDATE STATS ON FINAL TABLE AFTER INSERTS'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_COMPANY_MONTH',estimate_percent=>1, DEGREE => 32);
   COMMIT;
   
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
 
END WH_PRF_CUST_278R;
