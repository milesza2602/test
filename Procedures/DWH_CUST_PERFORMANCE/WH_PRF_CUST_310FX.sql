--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_310FX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_310FX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Update basket fact table in the performance layer
--               with vmp_ind ex basket/basket_item transactions
--  Tables:      Input  - cust_basket_item
--               Output - cust_basket
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

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;



g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_310U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Update basket table with VMP indicator';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_cust_basket is
        with temp_port as
        (
         select /*+ FULL(cp)  parallel (cp,4) */ *
         from   dim_customer_portfolio cp
         where  product_code_no = 19
         and    nvl(portfolio_status_desc,' ') <> 'Closed'
         )
        select  /*+   parallel (a,8) parallel (b,8) */
                a.location_no,
                a.till_no,
                a.tran_no,
                a.tran_date,
                MAX(c.area_no) area_no
        from    cust_basket a,
                cust_basket_item b,
                dim_location c
        where   a.location_no       = c.location_no
--        and     a.last_updated_date = g_date 
        and     a.tran_date         between '1 JUL 2015' and '30 SEP 2015'
        and     substr(a.loyalty_ext_swipe_no,1,4)        =  5900
        and     a.tran_type       in ('S','R','V','Q')
        and     a.waste_ind       <> 1
        and     b.item_no         <> 999960
        and     c.sk1_country_code = 196
        and     c.area_no         in (9951,9952,8800)
        and     a.location_no      = b.location_no
        and     a.till_no          = b.till_no
        and     a.tran_no          = b.tran_no
        and     a.tran_date        = b.tran_date
        group by
                a.location_no,
                a.till_no,
                a.tran_no,
                a.tran_date
         union
        select  /*+   parallel (8) */
                a.location_no,
                a.till_no,
                a.tran_no,
                a.tran_date,
                MAX(c.area_no) area_no
        from    cust_basket a,
                cust_basket_item b,
                dim_location c,
                temp_port d
        where   a.location_no       = c.location_no
        and     a.customer_no       = d.customer_no
--        and     a.last_updated_date = g_date
        and     a.tran_date         between '1 JUL 2015' and '30 SEP 2015'
        and     a.tran_type       in ('S','R','V','Q')
        and     a.waste_ind       <> 1
        and     b.item_no         <> 999960
        and     c.sk1_country_code = 196
        and     c.area_no         in (9951,9952,8800)
        and     a.location_no      = b.location_no
        and     a.till_no          = b.till_no
        and     a.tran_no          = b.tran_no
        and     a.tran_date        = b.tran_date
        group by
                a.location_no,
                a.till_no,
                a.tran_no,
                a.tran_date ;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



FOR upd_rec IN c_cust_basket
   loop
     if upd_rec.area_no = 8800 then
        update cust_basket prf
        set    prf.vmp_ind          =	2
        where  prf.location_no      = upd_rec.location_no
        and    prf.till_no          = upd_rec.till_no
        and    prf.tran_no          = upd_rec.tran_no
        and    prf.tran_date        = upd_rec.tran_date
        and    nvl(prf.vmp_ind,0)          <>	2;
     else
        update cust_basket prf
        set    prf.vmp_ind          =	1
        where  prf.location_no      = upd_rec.location_no
        and    prf.till_no          = upd_rec.till_no
        and    prf.tran_no          = upd_rec.tran_no
        and    prf.tran_date        = upd_rec.tran_date 
        and    nvl(prf.vmp_ind,0)          <>	1;
     end if;
     g_recs_updated := g_recs_updated + SQL%ROWCOUNT;
   end loop;


      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;




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



    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;


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
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
end wh_prf_cust_310fx;
