-- ****** Object: Procedure W7131037.WH_PRF_CUST_302U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_302U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_engen_vmp_sales fact table in the performance layer
--               with input ex basket transactions
--  Tables:      Input  - cust_basket
--               Output - cust_engen_vmp_sales
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_302U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD VMP_SALES EX BASKET DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_cust_basket is
select   /*+ FULL(a)  parallel (4) */ a.location_no,
         a.till_no,
         a.tran_no,
         a.tran_date,
         max(case when  a.loyalty_ww_swipe_no is not null then
                        a.loyalty_ww_swipe_no
                  else  a.primary_customer_identifier
             end) primary_customer_identifier,
         sum(b.item_tran_selling-b.discount_selling) total_sales,
         sum(b.item_tran_qty) total_qty,
         sum(b.item_tran_selling-b.discount_selling) foods_sales,
         0,
         max(a.operator_id) operator_id,
         max(a.tran_time) tran_time,
         max(a.customer_no) customer_no
from     cust_basket a,
         cust_basket_item b,
         cust_engen_vmp_sales prf
where    a.location_no       = b.location_no
and      a.till_no           = b.till_no
and      a.tran_no           = b.tran_no
and      a.tran_date         = b.tran_date
and      a.location_no       = prf.location_no
and      a.till_no           = prf.till_no
and      a.tran_no           = prf.tran_no
and      a.tran_date         = prf.tran_date
and      a.tran_date         > g_date - 10
--and      a.last_updated_date = g_date - 1
and      a.vmp_ind           = 2
group by a.location_no,
         a.till_no,
         a.tran_no,
         a.tran_date
order by a.location_no,
         a.till_no,
         a.tran_no,
         a.tran_date ;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

insert /*+ APPEND parallel (prf,4) */ into cust_engen_vmp_sales prf
select /*+ FULL(a)   parallel (4) */
         a.tran_date,
         a.location_no,
         a.tran_no,
         a.till_no,
         max(a.tran_time) tran_time,
         max(case when  a.loyalty_ww_swipe_no is not null then
                        a.loyalty_ww_swipe_no
                  else  a.primary_customer_identifier
             end) primary_customer_identifier,
         max(a.operator_id) operator_id,
         max(a.customer_no) customer_no,
         trunc(sysdate),
         sum(b.item_tran_selling-b.discount_selling) total_sales,
         sum(b.item_tran_selling-b.discount_selling) foods_sales,
         0,
         sum(b.item_tran_qty) total_qty,
         g_date
from     cust_basket a,
         cust_basket_item b
where    a.location_no       = b.location_no
and      a.till_no           = b.till_no
and      a.tran_no           = b.tran_no
and      a.tran_date         = b.tran_date
and      a.tran_date         > g_date - 10
--and      a.last_updated_date = g_date - 1
and      a.VMP_IND           = 2
and      a.loyalty_ext_swipe_no is null
and      not exists
      (select /*+ nl_aj */ * from cust_engen_vmp_sales
       where    location_no        = a.location_no
       and      till_no            = a.till_no
       and      tran_no            = a.tran_no
       and      tran_date          = a.tran_date
       )
group by a.location_no,
         a.till_no,
         a.tran_no,
         a.tran_date
order by a.location_no,
         a.till_no,
         a.tran_no,
         a.tran_date ;


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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



FOR upd_rec IN c_cust_basket
   loop

     update cust_engen_vmp_sales prf
     set    prf.tran_time	          =	upd_rec.	tran_time,
            prf.primary_customer_identifier	=	upd_rec.	primary_customer_identifier,
            prf.operator_id	        =	upd_rec.	operator_id,
            prf.customer_no	        =	upd_rec.	customer_no,
            prf.load_date	          =	trunc(sysdate),
            prf.tran_selling	      =	upd_rec.	total_sales,
            prf.food_selling	      =	upd_rec.	foods_sales,
            prf.textile_selling	    =	0,
            prf.tran_qty	          =	upd_rec.	total_qty,
            prf. last_updated_date  = g_date
     where  prf.tran_date	          =	upd_rec.	tran_date    and
            prf.location_no	        =	upd_rec.	location_no  and
            prf.tran_no	            =	upd_rec.	tran_no      and
            prf.till_no	            =	upd_rec.	till_no             ;

      g_recs_updated := g_recs_updated + 1;
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
end wh_prf_cust_302u;
