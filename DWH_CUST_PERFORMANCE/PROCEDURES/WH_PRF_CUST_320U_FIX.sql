--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_320U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_320U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2015
--  Author:      Alastair de Wet
--  Purpose:     Create Mart cust_loc_item_prom_dy fact table in the performance layer
--               with input ex basket aux transactions
--  Tables:      Input  - cust_basket_aux
--               Output - cust_loc_item_prom_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Mar 2018:   VAT Changes   
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
   
g_date                 date          := trunc(sysdate);
g_from_date            date          := trunc(sysdate);
g_to_date              date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_320U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'MART LOC_ITEM_PROM_DY EX BASKET AUX DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

      
insert /*+ APPEND parallel (prf,8) */ into cust_loc_item_prom_dy prf
with wfs as 
   ( 
   select   /*+ parallel (a1,8) full(a1) */
            location_no,
            till_no,
            tran_no,
            tran_date,
            item_no,
            max(promotion_no)              promotion_no,
            sum(wreward_sales_value)       diff_reward_sales, 
            count(*)                       diff_reward_units,
            sum(promotion_discount_amount) diff_reward_discount
   from     cust_basket_aux a1
   where    loyalty_group in ('WFS','pro') and
            promotion_discount_amount <> 0 and
            tran_date between g_from_date and g_to_date
   group by location_no,
            till_no,
            tran_no,
            tran_date,
            item_no 
    ),
      ww as 
    (
    select  /*+ parallel (a2,8) full(a2) */ 
            location_no,
            till_no,
            tran_no,
            tran_date,
            item_no,
            count(*)                       wreward_units,
            sum(promotion_discount_amount) wreward_discount
   from     cust_basket_aux a2
   where    loyalty_group = '666' and
            promotion_discount_amount <> 0 and
            tran_date between g_from_date and g_to_date
   group by location_no,
            till_no,
            tran_no,
            tran_date,
            item_no 
   )
   select   /*+ parallel (8) */
            wfs.location_no,
            wfs.item_no,
            wfs.promotion_no,
            wfs.tran_date,
            sum(wfs.diff_reward_sales),
            sum(wfs.diff_reward_sales / ((nvl(ivr.vat_rate_perc,di.vat_rate_perc)/100) + 1)),
            sum(wfs.diff_reward_discount),
            sum(wfs.diff_reward_discount / ((nvl(ivr.vat_rate_perc,di.vat_rate_perc)/100) + 1)),
            sum(wfs.diff_reward_units),
            sum(ww.wreward_discount),
            sum(ww.wreward_discount / ((nvl(ivr.vat_rate_perc,di.vat_rate_perc)/100) + 1)),
            sum(ww.wreward_units),
            max(g_date)
   from     wfs
   
            LEFT OUTER JOIN   ww   on (                           -- VAT rate change
            wfs.location_no  = ww.location_no and
            wfs.till_no      = ww.till_no and
            wfs.tran_no      = ww.tran_no and 
            wfs.tran_date    = ww.tran_date and
            wfs.item_no      = ww.item_no )

            join   dim_item di       on wfs.item_no      = di.item_no                -- VAT rate change
            join   dim_location dl   on wfs.location_no  = dl.location_no            -- VAT rate change

            LEFT OUTER JOIN   fnd_item_vat_rate  ivr  on (                           -- VAT rate change
                                   wfs.item_no       = ivr.item_no and               -- VAT rate change
                                   dl.vat_region_no  = ivr.vat_region_no   and                          -- VAT rate change
                                   wfs.tran_date between ivr.active_from_date and ivr.active_to_date)   -- VAT rate change    

   group by wfs.location_no,
            wfs.item_no,
            wfs.promotion_no,
            wfs.tran_date 
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

     g_from_date := '1 JAN 2019';
     g_to_date   := '22 JAN 2019';
    l_text := 'DATES BEING PROCESSED '||g_from_date||' thru '||g_to_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 


    l_text := 'BULK DELETE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
   
    delete from cust_loc_item_prom_dy
    where tran_date between g_from_date and g_to_date;
    
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
end wh_prf_cust_320u_fix;
