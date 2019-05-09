--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_271U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_271U" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Alastair de Wet
--  Purpose:     Roll up for curent month values ex basket item and Roll down of monthly values to create new month Depth and Breadth
--  Tables:      Input  - cust_basket_item , cust_db_dept_month
--               Output - cust_db_dept_month
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


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;
g_run_date            date;
g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_271U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE cust_db_dept_month EX cust_basket_item';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;




 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF cust_db_dept_month EX cust_basket_item STARTED AT '||
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

   l_text := 'Month being processed:= '||
             g_this_mn_start_date || g_this_mn_end_date ||g_yr_00||g_mn_00; 
             
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

   g_stmt      := 'Alter table  DWH_CUST_PERFORMANCE.CUST_DB_DEPT_MONTH truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
   l_text      := g_stmt;

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--    execute immediate g_stmt; 
   g_run_date := g_this_mn_end_date + 8;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (prf,4) */ into cust_db_dept_month prf
   with     bskt as 
   (
   select  /*+ parallel (cbi,4) parallel (di,4) */ primary_customer_identifier,
            di.department_no,
            max(customer_no)   customer_no,
            sum(item_tran_qty) num_item,
            sum(item_tran_selling - discount_selling) sales,  
            count(unique tran_no) num_visit
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between g_this_mn_start_date and g_this_mn_end_date  and
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier <> 998 and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,department_no
   )
   select /*+ FULL(cdm)  parallel (cdm,4) parallel (bskt,4) */
            g_yr_00,g_mn_00,
            nvl(cdm.primary_customer_identifier,bskt.primary_customer_identifier) primary_customer_identifier,
            nvl(cdm.department_no,bskt.department_no) department_no,
            nvl(bskt.customer_no,bskt.customer_no) customer_no,
            bskt.num_item,bskt.sales,bskt.num_visit,
            cdm.NUM_ITEM_101,cdm.SALES_101,cdm.NUM_VISIT_101,
            cdm.NUM_ITEM_102,cdm.SALES_102,cdm.NUM_VISIT_102, 
            cdm.NUM_ITEM_103,cdm.SALES_103,cdm.NUM_VISIT_103, 
            cdm.NUM_ITEM_104,cdm.SALES_104,cdm.NUM_VISIT_104, 
            cdm.NUM_ITEM_105,cdm.SALES_105,cdm.NUM_VISIT_105, 
            cdm.NUM_ITEM_106,cdm.SALES_106,cdm.NUM_VISIT_106, 
            cdm.NUM_ITEM_107,cdm.SALES_107,cdm.NUM_VISIT_107, 
            cdm.NUM_ITEM_108,cdm.SALES_108,cdm.NUM_VISIT_108, 
            cdm.NUM_ITEM_109,cdm.SALES_109,cdm.NUM_VISIT_109,
            cdm.NUM_ITEM_110,cdm.SALES_110,cdm.NUM_VISIT_110,
            cdm.NUM_ITEM_111,cdm.SALES_111,cdm.NUM_VISIT_111,
            cdm.NUM_ITEM_112,cdm.SALES_112,cdm.NUM_VISIT_112,
            cdm.NUM_ITEM_201,cdm.SALES_201,cdm.NUM_VISIT_201, 
            cdm.NUM_ITEM_202,cdm.SALES_202,cdm.NUM_VISIT_202,
            cdm.NUM_ITEM_203,cdm.SALES_203,cdm.NUM_VISIT_203,
            cdm.NUM_ITEM_204,cdm.SALES_204,cdm.NUM_VISIT_204,
            cdm.NUM_ITEM_205,cdm.SALES_205,cdm.NUM_VISIT_205,
            cdm.NUM_ITEM_206,cdm.SALES_206,cdm.NUM_VISIT_206,
            cdm.NUM_ITEM_207,cdm.SALES_207,cdm.NUM_VISIT_207,
            cdm.NUM_ITEM_208,cdm.SALES_208,cdm.NUM_VISIT_208,
            cdm.NUM_ITEM_209,cdm.SALES_209,cdm.NUM_VISIT_209,
            cdm.NUM_ITEM_210,cdm.SALES_210,cdm.NUM_VISIT_210,
            cdm.NUM_ITEM_211,cdm.SALES_211,cdm.NUM_VISIT_211,
            cdm.NUM_ITEM_212,cdm.SALES_212,cdm.NUM_VISIT_212,
            cdm.NUM_ITEM_301,cdm.SALES_301,cdm.NUM_VISIT_301,
            cdm.NUM_ITEM_302,cdm.SALES_302,cdm.NUM_VISIT_302,
            cdm.NUM_ITEM_303,cdm.SALES_303,cdm.NUM_VISIT_303,
            cdm.NUM_ITEM_304,cdm.SALES_304,cdm.NUM_VISIT_304,
            cdm.NUM_ITEM_305,cdm.SALES_305,cdm.NUM_VISIT_305,
            cdm.NUM_ITEM_306,cdm.SALES_306,cdm.NUM_VISIT_306,
            cdm.NUM_ITEM_307,cdm.SALES_307,cdm.NUM_VISIT_307,
            cdm.NUM_ITEM_308,cdm.SALES_308,cdm.NUM_VISIT_308,
            cdm.NUM_ITEM_309,cdm.SALES_309,cdm.NUM_VISIT_309,
            cdm.NUM_ITEM_310,cdm.SALES_310,cdm.NUM_VISIT_310,
            cdm.NUM_ITEM_311,cdm.SALES_311,cdm.NUM_VISIT_311,
            g_date
   from     cust_db_dept_month cdm
   full outer join
            bskt
   on       cdm.primary_customer_identifier = bskt.primary_customer_identifier  
   and      cdm.department_no               = bskt.department_no
   and      cdm.fin_year_no                 = g_last_yr         
   and      cdm.fin_month_no                = g_last_mn
   ;
 
   g_recs_inserted         := g_recs_inserted + sql%rowcount;

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
 
END WH_PRF_CUST_271U;
