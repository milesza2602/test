--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_318U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_318U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        MAY 2016
--  Author:      Alastair de Wet
--  Purpose:     Create ORPHAN MART table in the performance layer
--               with input ex basket transactions
--  Tables:      Input  - cust_basket
--               Output - cust_mart_orphan_month
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read            integer       :=  0;
g_recs_updated         integer       :=  0;
g_recs_deleted         integer       :=  0;
g_recs_inserted        integer       :=  0;
g_truncate_count       integer       :=  0;
g_this_mn_start_date   date ;
g_this_mn_end_date     date;
g_last_mn_fin_year_no  integer;
g_last_mn_fin_month_no integer;
g_stmt                 varchar(300);
g_run_date             date;
g_date                 date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_318U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE ORPHAN MART EX BASKET DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin


insert /*+ APPEND  */ into cust_mart_orphan_month prf
with cbskt as (

   select  /*+ FULL(cb) parallel (cb,8)  */ 
            primary_customer_identifier,
            max(case when cb.customer_no > 0 or 
                     (cb.primary_customer_identifier  >= 6007850000000000 and cb.primary_customer_identifier  <= 6007854099999999)  or
                     (cb.primary_customer_identifier  >= 6007856000000000 and cb.primary_customer_identifier  <= 6007859999999999)  or
                     (cb.primary_customer_identifier  >= 5900000000000000 and cb.primary_customer_identifier  <= 5999999999999999) 
                     then  1 else 0 end) active_wreward_customers , 
            max(case when cb.customer_no is null and primary_customer_identifier between 6007852800000000 and 6007852899999999 
                     then 1 else 0 end) active_orphan_customers,  
            sum(case when cb.customer_no > 0 or 
                     (cb.primary_customer_identifier  >= 6007850000000000 and cb.primary_customer_identifier  <= 6007854099999999)  or
                     (cb.primary_customer_identifier  >= 6007856000000000 and cb.primary_customer_identifier  <= 6007859999999999)  or
                     (cb.primary_customer_identifier  >= 5900000000000000 and cb.primary_customer_identifier  <= 5999999999999999) 
                     then tran_selling - discount_selling else 0 end) active_wreward_revenue ,               
            sum(case when cb.customer_no is null and primary_customer_identifier between 6007852800000000 and 6007852899999999
                     then tran_selling - discount_selling else 0 end) active_orphan_revenue,  
            sum(case when cb.customer_no > 0 or 
                     (cb.primary_customer_identifier  >= 6007850000000000 and cb.primary_customer_identifier  <= 6007854099999999)  or
                     (cb.primary_customer_identifier  >= 6007856000000000 and cb.primary_customer_identifier  <= 6007859999999999)  or
                     (cb.primary_customer_identifier  >= 5900000000000000 and cb.primary_customer_identifier  <= 5999999999999999)   
                     then 1 else 0 end) active_wreward_baskets ,               
            sum(case when cb.customer_no is null and primary_customer_identifier between 6007852800000000 and 6007852899999999
                     then 1 else 0 end) active_orphan_baskets,                                                                  

            sum(tran_selling - discount_selling) sales,  
            count(unique tran_no||tran_date||till_no||location_no) visits 
   from     cust_basket cb 
   where    tran_date between g_this_mn_start_date and g_this_mn_end_date and    
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0 
   group by primary_customer_identifier ),
 
   totbskt as (
   select /*+ FULL(cbskt) parallel (cbskt,8)  */
          sum(cbskt.active_wreward_customers) active_wreward_customers,
          sum(cbskt.active_orphan_customers)  active_orphan_customers,
          sum(cbskt.active_wreward_revenue)   active_wreward_revenue,
          sum(cbskt.active_orphan_revenue)    active_orphan_revenue,
          sum(cbskt.active_wreward_baskets)   active_wreward_baskets,
          sum(cbskt.active_orphan_baskets)    active_orphan_baskets ,
          sum(case when cbskt.active_orphan_baskets = 1               then 1 else 0 end) orphan_swipe_1,                                                                  
          sum(case when cbskt.active_orphan_baskets between 2 and 5   then 1 else 0 end) orphan_swipe_2_5,
          sum(case when cbskt.active_orphan_baskets between 6 and 10  then 1 else 0 end) orphan_swipe_6_10,
          sum(case when cbskt.active_orphan_baskets > 10              then 1 else 0 end) orphan_swipe_11_plus,
          sum(case when cbskt.active_orphan_baskets = 1               then cbskt.active_orphan_revenue else 0 end) orphan_revenue_1,                                                                  
          sum(case when cbskt.active_orphan_baskets between 2 and 5   then cbskt.active_orphan_revenue else 0 end) orphan_revenue_2_5,
          sum(case when cbskt.active_orphan_baskets between 6 and 10  then cbskt.active_orphan_revenue else 0 end) orphan_revenue_6_10,
          sum(case when cbskt.active_orphan_baskets > 10              then cbskt.active_orphan_revenue else 0 end) orphan_revenue_11_plus 
   from   cbskt ),
   conv as (
   select /*+ parallel(lcm,8) full(lcm) */ 
          count(*) orphan_customers_converted
          from dim_wod_lcm_cust_card lcm 
          where lcm.linked_date  between g_this_mn_start_date and g_this_mn_end_date and  
                lcm.backdate_option                = 0        and
                substr(lcm.PRIMARY_ACCOUNT_NO,1,8) = 60078528 )

   select g_last_mn_fin_year_no,
          g_last_mn_fin_month_no,
          active_wreward_customers,
          active_orphan_customers,
          orphan_customers_converted,
          active_wreward_revenue,
          active_orphan_revenue,
          active_wreward_baskets,
          active_orphan_baskets ,
          orphan_swipe_1,                                                                  
          orphan_swipe_2_5,
          orphan_swipe_6_10,
          orphan_swipe_11_plus,
          orphan_revenue_1,                                                                  
          orphan_revenue_2_5,
          orphan_revenue_6_10,
          orphan_revenue_11_plus,
          g_date
   from   conv,
          totbskt
          
;



      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************
     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_last_mn_fin_year_no, g_last_mn_fin_month_no
     from dim_control;

     select  distinct this_mn_start_date,this_mn_end_date
     into    g_this_mn_start_date,g_this_mn_end_date
     from    dim_calendar_wk
     where   fin_month_no = g_last_mn_fin_month_no
     and     fin_year_no  = g_last_mn_fin_year_no;

    l_text := 'Dates being rolled up '||g_this_mn_start_date||' thru '||g_this_mn_end_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Year/Month being procesed '||g_last_mn_fin_year_no||' '||g_last_mn_fin_month_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_run_date := g_this_mn_end_date + 4;
    if trunc(sysdate) <> g_run_date then
       l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
    end if;  
   
    l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    delete from cust_mart_orphan_month
    where  fin_month_no = g_last_mn_fin_month_no and
           fin_year_no  = g_last_mn_fin_year_no;

    g_recs_deleted :=  sql%rowcount;
    commit;


    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    g_recs_read := g_recs_updated + g_recs_inserted;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;


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
end wh_prf_cust_318u;
