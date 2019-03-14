-- ****** Object: Procedure W7131037.WH_PRF_CUST_106U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_106U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust_basket_aux fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_cust_basket_aux
--               Output - cust_basket_aux
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

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_106U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET AUX EX POS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--FULL(fnd)
cursor c_fnd_cust_basket_aux is
select /*+  parallel (fnd,2) parallel (prf,2*/
              fnd.*
      from    fnd_cust_basket_aux fnd,
              cust_basket_aux prf
      where   fnd.location_no       = prf.location_no and
              fnd.tran_date         = prf.tran_date   and
              fnd.till_no           = prf.till_no     and
              fnd.tran_no           = prf.tran_no     and
              fnd.aux_seq_no        = prf.aux_seq_no  and
              fnd.last_updated_date = g_date          and
              fnd.tran_date         > g_date - 42
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.location_no,
              fnd.tran_date,
              fnd.till_no,fnd.tran_no,fnd.aux_seq_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
  --    /*+ APPEND parallel (prf,2) */

      insert /*+ APPEND  */ into cust_basket_aux prf
      select /*+ FULL(fnd) parallel (fnd,4) */
            	fnd.	location_no	,
            	fnd.	till_no	,
            	fnd.	tran_no	,
            	fnd.	tran_date	,
            	fnd.	aux_seq_no	,
            	fnd.	tran_type_code	,
            	fnd.	cust_name	,
            	fnd.	cust_tel_no	,
            	fnd.	price_overide_code	,
            	fnd.	ppc_code	,
            	fnd.	ppc_operator	,
            	fnd.	item_no	,
            	fnd.	promotion_no	,
            	fnd.	loyalty_group	,
            	fnd.	promotion_discount_amount	,
            	fnd.	loyalty_partner_id	,
            	fnd.	customer_no	,
            	fnd.	item_seq_no	,
            	fnd.	wreward_sales_value	,
            	fnd.	atg_customer_no	,
              0,
              g_date as last_updated_date,
              fnd.	EMPLOYEE_ID,
              fnd.	COMPANY_CODE,
              fnd.	PROMOTION_DISCOUNT_AMT_LOCAL,
              fnd.	WREWARD_SALES_VALUE_LOCAL
       from  fnd_cust_basket_aux fnd
       where fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from cust_basket_aux
       where  location_no       = fnd.location_no and
              tran_date         = fnd.tran_date   and
              till_no           = fnd.till_no     and
              tran_no           = fnd.tran_no     and
              aux_seq_no        = fnd.aux_seq_no
              )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



for upd_rec in c_fnd_cust_basket_aux
   loop
     update cust_basket_aux prf
     set    prf.	tran_type_code	          =	upd_rec.	tran_type_code	,
            prf.	cust_name	                =	upd_rec.	cust_name	,
            prf.	cust_tel_no	              =	upd_rec.	cust_tel_no	,
            prf.	price_overide_code	      =	upd_rec.	price_overide_code	,
            prf.	ppc_code	                =	upd_rec.	ppc_code	,
            prf.	ppc_operator	            =	upd_rec.	ppc_operator	,
            prf.	item_no	                  =	upd_rec.	item_no	,
            prf.	promotion_no	            =	upd_rec.	promotion_no	,
            prf.	loyalty_group           	=	upd_rec.	loyalty_group	,
            prf.	promotion_discount_amount	=	upd_rec.	promotion_discount_amount	,
            prf.	loyalty_partner_id	      =	upd_rec.	loyalty_partner_id	,
            prf.	customer_no	              =	upd_rec.	customer_no	,
            prf.	item_seq_no	              =	upd_rec.	item_seq_no	,
            prf.	wreward_sales_value	      =	upd_rec.	wreward_sales_value	,
            prf.	atg_customer_no	          =	upd_rec.	atg_customer_no	,
            prf.	EMPLOYEE_ID	              =	upd_rec.	EMPLOYEE_ID	,
            prf.	COMPANY_CODE              =	upd_rec.	COMPANY_CODE	,
            prf.	PROMOTION_DISCOUNT_AMT_LOCAL  =	upd_rec.	PROMOTION_DISCOUNT_AMT_LOCAL	,
            prf.	WREWARD_SALES_VALUE_LOCAL	=	upd_rec.	WREWARD_SALES_VALUE_LOCAL	,
            prf.  last_updated_date         = g_date
     where  prf.	location_no	      =	upd_rec.	location_no and
            prf.	tran_date	        =	upd_rec.	tran_date	  and
            prf.	till_no	          =	upd_rec.	till_no	    and
            prf.	tran_no	          =	upd_rec.	tran_no	    and
            prf.	aux_seq_no      	=	upd_rec.	aux_seq_no  and
            (
            nvl(prf.tran_type_code	          ,0) <>	upd_rec.	tran_type_code	or
            nvl(prf.cust_name	                ,0) <>	upd_rec.	cust_name	or
            nvl(prf.cust_tel_no	              ,0) <>	upd_rec.	cust_tel_no	or
            nvl(prf.price_overide_code	      ,0) <>	upd_rec.	price_overide_code	or
            nvl(prf.ppc_code	                ,0) <>	upd_rec.	ppc_code	or
            nvl(prf.ppc_operator	            ,0) <>	upd_rec.	ppc_operator	or
            nvl(prf.item_no	                  ,0) <>	upd_rec.	item_no	or
            nvl(prf.promotion_no	            ,0) <>	upd_rec.	promotion_no	or
            nvl(prf.loyalty_group	            ,0) <>	upd_rec.	loyalty_group	or
            nvl(prf.promotion_discount_amount	,0) <>	upd_rec.	promotion_discount_amount	or
            nvl(prf.loyalty_partner_id	      ,0) <>	upd_rec.	loyalty_partner_id	or
            nvl(prf.customer_no	              ,0) <>	upd_rec.	customer_no	or
            nvl(prf.item_seq_no             	,0) <>	upd_rec.	item_seq_no	or
            nvl(prf.wreward_sales_value	      ,0) <>	upd_rec.	wreward_sales_value	or
            nvl(prf.atg_customer_no	          ,0) <>	upd_rec.	atg_customer_no or
            nvl(prf.EMPLOYEE_ID             	,0) <>	upd_rec.	EMPLOYEE_ID or
            nvl(prf.COMPANY_CODE     	        ,0) <>	upd_rec.	COMPANY_CODE
            );

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


    select count(*)
    into   g_recs_read
    from   fnd_cust_basket_aux
    where  last_updated_date = g_date;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


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

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


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
end wh_prf_cust_106u;
