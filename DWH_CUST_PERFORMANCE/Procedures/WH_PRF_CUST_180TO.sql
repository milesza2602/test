--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_180TO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_180TO" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2016
--  Author:      Alastair de Wet
--  Purpose:     Create  cust_wod_reward_swipe_cnt_wk fact table in the performance layer
--               with input ex basket transactions
--  Tables:      Input  - cust_basket
--               Output - cust_wod_reward_swipe_cnt_wk
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


g_last_wk_start_date   date ;
g_last_wk_end_date     date;
g_last_wk_fin_year_no  integer;
g_last_wk_fin_week_no  integer;

g_stmt                 varchar(300);
g_run_date             date;
g_date                 date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_180U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP SWIPE COUNT EX BASKET DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

insert /*+ parallel(prf,8) */ into cust_wod_reward_swipe_cnt_wk prf
with baskt as (
select   /*+ full(cb) full(bi) full(di) Parallel(cb,8) parallel(bi,8) */  
         cb.location_no,cb.till_no,cb.tran_no,cb.tran_date,
         max(cb.vmp_ind) vmp_ind,
         max(cb.loyalty_ww_swipe_no) loyalty_ww_swipe_no,
         max(cb.primary_customer_identifier) primary_customer_identifier,
         max(cb.customer_no) customer_no 
from     cust_basket  cb,
         cust_basket_item bi,
         dim_item di
where    cb.tran_date between g_last_wk_start_date and g_last_wk_end_date 
and      cb.tran_type = 'S'
and      cb.location_no = bi.location_no  
and      cb.till_no     = bi.till_no 
and      cb.tran_no     = bi.tran_no   
and      cb.tran_date   = bi.tran_date  
and      bi.item_no     = di.item_no
and      di.business_unit_no <> 70
group by cb.location_no,cb.till_no,cb.tran_no,cb.tran_date
               ),
      cnt   as (
select   /*+ Full(cbcnt)   Parallel(cbcnt,8) */  
         cbcnt.location_no,
         sum ( case
                when cbcnt.customer_no   is not null  or
                     (cbcnt.primary_customer_identifier  >= 6007850000000000 and primary_customer_identifier  <= 6007854099999999)  or
                     (cbcnt.primary_customer_identifier  >= 6007856000000000 and primary_customer_identifier  <= 6007859999999999)  or
                     (cbcnt.primary_customer_identifier  >= 5900000000000000 and primary_customer_identifier  <= 5999999999999999)  then
                     1
                else
                     0
                end ) loyalty_swipe_count,   
         sum ( case
                when cbcnt.vmp_ind IN (1,2) then
                     1 
                else
                     0
                end ) myschool_swipe_count 
from     baskt  cbcnt
group by cbcnt.location_no
               ),               
        rev as (
select   /*+ full(bk) full(bi) full(di) Parallel(bk,8) Parallel(bi,8) */
         bk.location_no,
         count(distinct bk.location_no||bk.till_no||bk.tran_no||bk.tran_date) total_tran_count,
         sum ( case
                when bk.customer_no   is not null  or
                     (bk.primary_customer_identifier  >= 6007850000000000 and bk.primary_customer_identifier  <= 6007854099999999)  or
                     (bk.primary_customer_identifier  >= 6007856000000000 and bk.primary_customer_identifier  <= 6007859999999999)  or
                     (bk.primary_customer_identifier  >= 5900000000000000 and bk.primary_customer_identifier  <= 5999999999999999)  then
                     bi.item_tran_selling-bi.discount_selling 
                else
                     0
                end ) loyalty_revenue,     
         sum (bi.item_tran_selling-bi.discount_selling)  total_revenue ,
         sum ( case
                when bk.VMP_IND IN (1,2) then
                     bi.item_tran_selling-bi.discount_selling
                else 
                     0
                end ) myschool_revenue 
from     cust_basket  bk,
         cust_basket_item bi,
         dim_item di
where    bk.tran_date between g_last_wk_start_date and g_last_wk_end_date 
and      bk.tran_type = 'S'
and      bk.location_no = bi.location_no  
and      bk.till_no     = bi.till_no 
and      bk.tran_no     = bi.tran_no   
and      bk.tran_date   = bi.tran_date  
and      bi.item_no     = di.item_no
and      di.business_unit_no <> 70
group by bk.location_no
               ) 
select   /*+ full(rv) full(cn) parallel(rv,8) parallel(cn,8) */
         g_last_wk_fin_year_no,
         g_last_wk_fin_week_no,
         rv.location_no, 
         cn.loyalty_swipe_count,
         cn.myschool_swipe_count,
         rv.total_tran_count,
         rv.loyalty_revenue,
         rv.total_revenue,
         rv.myschool_revenue,
         g_date
from     rev rv,
         cnt cn
where    rv.location_no = cn.location_no         
order by rv.location_no ;               

    
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
     select LAST_WK_FIN_YEAR_NO,
            LAST_WK_FIN_WEEK_NO,
            LAST_WK_START_DATE,
            LAST_WK_END_DATE
     into   g_last_wk_fin_year_no ,  
            g_last_wk_fin_week_no  ,
            g_last_wk_start_date  ,
            g_last_wk_end_date 
     from dim_control;
     
     g_last_wk_fin_year_no := 2015;
     g_last_wk_fin_week_no := 53;
     g_last_wk_start_date  := '29 Jun 2015';
     g_last_wk_end_date    := '05 Jul 2015';

for xyz in 1..52
loop
     g_last_wk_fin_week_no := g_last_wk_fin_week_no - 1;
     g_last_wk_start_date  := g_last_wk_start_date - 7;
     g_last_wk_end_date    := g_last_wk_end_date - 7;
    

    l_text := 'Dates being rolled up '||g_last_wk_start_date||' thru '||g_last_wk_end_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Year/Week being procesed '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_run_date := g_last_wk_end_date + 4;
--    if trunc(sysdate) <> g_run_date then
--       l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--       p_success := true;
--       return;
--    end if;  
   
--    l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--    g_stmt   := 'Alter table  DWH_CUST_PERFORMANCE.cust_wod_reward_swipe_cnt_wk truncate  subpartition for ('||g_last_mn_fin_year_no||','||g_last_mn_fin_month_no||') update global indexes';
--    l_text   := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    execute immediate g_stmt;  

    delete from cust_wod_reward_swipe_cnt_wk
    where  fin_week_no =  g_last_wk_fin_week_no and
           fin_year_no  = g_last_wk_fin_year_no;

    g_recs_deleted :=  sql%rowcount;
    commit;
--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_WOD_REWARD_SWIPE_CNT_WK',estimate_percent=>1, DEGREE => 32);
--    commit;


    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    g_recs_read := g_recs_updated + g_recs_inserted;

end loop;

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
end wh_prf_cust_180to;
