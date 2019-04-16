--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_210U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_210U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_portfolio_creation fact table in the performance layer
--               with input ex dim customer portfolio transactions
--  Tables:      Input  - dim_customer_portfolio
--               Output - cust_portfolio_creation
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_210U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUSTOMER_PORTFOLIO DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_dim_customer_portfolio is
select   /*+ paralell(4) */ fnd.customer_no,
         max(case when  product_code_no = 28 then portfolio_create_date  end)  wod_create_date,
         max(case when  product_code_no = 28 then portfolio_close_date   end)  wod_close_date,
         max(case when  product_code_no = 19 then portfolio_create_date  end)  myschool_create_date,
         max(case when  product_code_no = 19 then portfolio_close_date   end)  myschool_close_date,
         max(case when  product_code_no in (1,2,3,4,5,6,7,9,21) then portfolio_create_date  end)  isc_create_date,
         max(case when  product_code_no in (1,2,3,4,5,6,7,9,21) then portfolio_close_date  end)  isc_close_date,
         max(case when  product_code_no = 20 then portfolio_create_date  end)  ww_visa_create_date,
         max(case when  product_code_no = 20 then portfolio_close_date   end)  ww_visa_close_date,
         max(case when  product_code_no = 99 then portfolio_create_date  end)  lw_create_date,
         max(case when  product_code_no = 99 then portfolio_close_date   end)  lw_close_date
from     dim_customer_portfolio fnd, cust_portfolio_creation prf
where    fnd.customer_no    = prf.customer_no
and      fnd.product_code_no in (1,2,3,4,5,6,7,9,19,20,21,28,99)
and      fnd.last_updated_date  = g_date
group by fnd.customer_no
order by fnd.customer_no  ;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

insert /*+ APPEND parallel (prf,4) */ into cust_portfolio_creation prf
select   /*+ FULL(fnd)  parallel (fnd,4) */
         customer_no,
         max(case when  product_code_no = 28 then portfolio_create_date  end)  wod_create_date,
         max(case when  product_code_no = 28 then portfolio_close_date   end)  wod_close_date,
         max(case when  product_code_no = 19 then portfolio_create_date  end)  myschool_create_date,
         max(case when  product_code_no = 19 then portfolio_close_date   end)  myschool_close_date,
         max(case when  product_code_no in (1,2,3,4,5,6,7,9,21) then portfolio_create_date  end)  isc_create_date,
         max(case when  product_code_no in (1,2,3,4,5,6,7,9,21) then portfolio_close_date  end)  isc_close_date,
         max(case when  product_code_no = 20 then portfolio_create_date  end)  ww_visa_create_date,
         max(case when  product_code_no = 20 then portfolio_close_date   end)  ww_visa_close_date,
         max(case when  product_code_no = 99 then portfolio_create_date  end)  lw_create_date,
         max(case when  product_code_no = 99 then portfolio_close_date   end)  lw_close_date,
         g_date as last_updated_date
from     dim_customer_portfolio fnd
where    product_code_no in (1,2,3,4,5,6,7,9,19,20,21,28,99)
and      last_updated_date = g_date
and      not exists
         (
         select /*+ nl_aj */ * from cust_portfolio_creation
         where    customer_no       = fnd.customer_no
         )
group by customer_no
order by customer_no  ;



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



FOR upd_rec IN c_dim_customer_portfolio
   loop

     update cust_portfolio_creation prf
     set    prf.wod_create_date     	=	upd_rec.	wod_create_date	,
            prf.wod_close_date	      =	upd_rec.	wod_close_date	,
            prf.myschool_create_date	=	upd_rec.	myschool_create_date	,
            prf.myschool_close_date	  =	upd_rec.	myschool_close_date	,
            prf.isc_create_date	      =	upd_rec.	isc_create_date	,
            prf.isc_close_date	      =	upd_rec.	isc_close_date	,
            prf.ww_visa_create_date	  =	upd_rec.	ww_visa_create_date	,
            prf.ww_visa_close_date	  =	upd_rec.	ww_visa_close_date	,
            prf.lw_create_date	      =	upd_rec.	lw_create_date	,
            prf.lw_close_date     	  =	upd_rec.	lw_close_date	,
            prf. last_updated_date    = g_date
     where  prf.customer_no  	        =	upd_rec.	customer_no;

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
end wh_prf_cust_210u;
