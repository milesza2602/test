-- ****** Object: Procedure W7131037.WH_PRF_CUST_018U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_018U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create CUSTOMER MASTER fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_wfs_customer
--               Output - dim_wfs_customer
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_018U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CUSTOMER MASTER EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_wfs_customer is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_wfs_customer fnd,
              dim_wfs_customer prf
      where   fnd.wfs_customer_no       = prf.wfs_customer_no
      and     fnd.last_updated_date     = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.wfs_customer_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,2) */ into dim_wfs_customer prf
       select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.	*
       from  fnd_wfs_customer fnd
       where fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_wfs_customer
       where  wfs_customer_no    = fnd.wfs_customer_no
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



FOR upd_rec IN c_fnd_wfs_customer
   loop
     update dim_wfs_customer prf
     set    prf.identity_no	=	upd_rec.	identity_no	,
            prf.phone_no_home	=	upd_rec.	phone_no_home	,
            prf.phone_no_work	=	upd_rec.	phone_no_work	,
            prf.phone_no_cell	=	upd_rec.	phone_no_cell	,
            prf.cust_name	=	upd_rec.	cust_name	,
            prf.post_address1	=	upd_rec.	post_address1	,
            prf.post_address2	=	upd_rec.	post_address2	,
            prf.post_address3	=	upd_rec.	post_address3	,
            prf.post_postcode	=	upd_rec.	post_postcode	,
            prf.email_address	=	upd_rec.	email_address	,
            prf.mosaic_cluster	=	upd_rec.	mosaic_cluster	,
            prf.mosaic_group	=	upd_rec.	mosaic_group	,
            prf.segment_code1	=	upd_rec.	segment_code1	,
            prf.segment_code2	=	upd_rec.	segment_code2	,
            prf.segment_code3	=	upd_rec.	segment_code3	,
            prf.segment_code4	=	upd_rec.	segment_code4	,
            prf.segment_date1	=	upd_rec.	segment_date1	,
            prf.segment_date2	=	upd_rec.	segment_date2	,
            prf.segment_date3	=	upd_rec.	segment_date3	,
            prf.segment_date4	=	upd_rec.	segment_date4	,
            prf.mail_me	=	upd_rec.	mail_me	,
            prf.res_addr_line1	=	upd_rec.	res_addr_line1	,
            prf.res_addr_line2	=	upd_rec.	res_addr_line2	,
            prf.res_addr_line3	=	upd_rec.	res_addr_line3	,
            prf.res_postcode	=	upd_rec.	res_postcode	,
            prf.language	=	upd_rec.	language	,
            prf.gender	=	upd_rec.	gender	,
            prf.marital_status	=	upd_rec.	marital_status	,
            prf.race	=	upd_rec.	race	,
            prf.date_of_birth	=	upd_rec.	date_of_birth	,
            prf.age_acc_holder	=	upd_rec.	age_acc_holder	,
            prf.no_of_dependants	=	upd_rec.	no_of_dependants	,
            prf.age_child1	=	upd_rec.	age_child1	,
            prf.age_child2	=	upd_rec.	age_child2	,
            prf.age_child3	=	upd_rec.	age_child3	,
            prf.age_child4	=	upd_rec.	age_child4	,
            prf.age_child5	=	upd_rec.	age_child5	,
            prf.ww_employee_no	=	upd_rec.	ww_employee_no	,
            prf.staff_co_id	=	upd_rec.	staff_co_id	,
            prf.charge_card_acc	=	upd_rec.	charge_card_acc	,
            prf.loyalty_acc	=	upd_rec.	loyalty_acc	,
            prf.loan_acc	=	upd_rec.	loan_acc	,
            prf.unit_trust_acc	=	upd_rec.	unit_trust_acc	,
            prf.notice_dep_acc	=	upd_rec.	notice_dep_acc	,
            prf.credit_card_acc	=	upd_rec.	credit_card_acc	,
            prf.laybye_acc	=	upd_rec.	laybye_acc	,
            prf.sds_acc	=	upd_rec.	sds_acc	,
            prf.customer_no_x	=	upd_rec.	customer_no_x	,
            prf.billing_cycle	=	upd_rec.	billing_cycle	,
            prf.credit_buro_score	=	upd_rec.	credit_buro_score	,
            prf.cust_title	=	upd_rec.	cust_title	,
            prf.cust_initial	=	upd_rec.	cust_initial	,
            prf.customer_status	=	upd_rec.	customer_status	,
            prf.customer_type	=	upd_rec.	customer_type	,
            prf.returned_mail	=	upd_rec.	returned_mail	,
            prf.credit_buro_date	=	upd_rec.	credit_buro_date	,
            prf.geo_code	=	upd_rec.	geo_code	,
            prf.myschool_ind	=	upd_rec.	myschool_ind	,
            prf.csm_basket_val_13wkbk	=	upd_rec.	csm_basket_val_13wkbk	,
            prf.csm_pref_store_13wkbk	=	upd_rec.	csm_pref_store_13wkbk	,
            prf.csm_shop_habit_seg_no_13wkbk	=	upd_rec.	csm_shop_habit_seg_no_13wkbk	,
            prf.	last_updated_date	=	g_date
     where  prf.	wfs_customer_no	      =	upd_rec.	wfs_customer_no  and
            (
            nvl(prf.identity_no	,0)   <>	upd_rec.	identity_no	or
            nvl(prf.phone_no_home	,0) <>	upd_rec.	phone_no_home	or
            nvl(prf.phone_no_work	,0) <>	upd_rec.	phone_no_work	or
            nvl(prf.phone_no_cell	,0) <>	upd_rec.	phone_no_cell	or
            nvl(prf.cust_name	,0) <>	upd_rec.	cust_name	or
            nvl(prf.post_address1	,0) <>	upd_rec.	post_address1	or
            nvl(prf.post_address2	,0) <>	upd_rec.	post_address2	or
            nvl(prf.post_address3	,0) <>	upd_rec.	post_address3	or
            nvl(prf.post_postcode	,0) <>	upd_rec.	post_postcode	or
            nvl(prf.email_address	,0) <>	upd_rec.	email_address	or
            nvl(prf.mail_me	,0) <>	upd_rec.	mail_me	or
            nvl(prf.language	,0) <>	upd_rec.	language	or
            nvl(prf.gender	,0) <>	upd_rec.	gender	or
            nvl(prf.marital_status	,0) <>	upd_rec.	marital_status	or
            nvl(prf.race	,0) <>	upd_rec.	race	or
            nvl(prf.date_of_birth	,'1 Jan 1900') <>	upd_rec.	date_of_birth	or
            nvl(prf.no_of_dependants	,0) <>	upd_rec.	no_of_dependants	or
            nvl(prf.age_child1	,'1 Jan 1900') <>	upd_rec.	age_child1	or
            nvl(prf.age_child2	,'1 Jan 1900') <>	upd_rec.	age_child2	or
            nvl(prf.age_child3	,'1 Jan 1900') <>	upd_rec.	age_child3	or
            nvl(prf.age_child4	,'1 Jan 1900') <>	upd_rec.	age_child4	or
            nvl(prf.age_child5	,'1 Jan 1900') <>	upd_rec.	age_child5	or
            nvl(prf.ww_employee_no	,0) <>	upd_rec.	ww_employee_no	or
            nvl(prf.staff_co_id	,0) <>	upd_rec.	staff_co_id	or
            nvl(prf.charge_card_acc	,0) <>	upd_rec.	charge_card_acc	or
            nvl(prf.loyalty_acc	,0) <>	upd_rec.	loyalty_acc	or
            nvl(prf.loan_acc	,0) <>	upd_rec.	loan_acc	or
            nvl(prf.unit_trust_acc	,0) <>	upd_rec.	unit_trust_acc	or
            nvl(prf.notice_dep_acc	,0) <>	upd_rec.	notice_dep_acc	or
            nvl(prf.credit_card_acc	,0) <>	upd_rec.	credit_card_acc	or
            nvl(prf.laybye_acc	,0) <>	upd_rec.	laybye_acc	or
            nvl(prf.sds_acc	,0) <>	upd_rec.	sds_acc	or
            nvl(prf.billing_cycle	,0) <>	upd_rec.	billing_cycle	or
            nvl(prf.credit_buro_score	,0) <>	upd_rec.	credit_buro_score	or
            nvl(prf.cust_title	,0) <>	upd_rec.	cust_title	or
            nvl(prf.cust_initial	,0) <>	upd_rec.	cust_initial	or
            nvl(prf.customer_status	,0) <>	upd_rec.	customer_status	or
            nvl(prf.customer_type	,0) <>	upd_rec.	customer_type	or
            nvl(prf.returned_mail	,0) <>	upd_rec.	returned_mail	or
            nvl(prf.credit_buro_date	,'1 Jan 1900') <>	upd_rec.	credit_buro_date	or
            nvl(prf.geo_code,0)	<>	upd_rec.	geo_code	or
            nvl(prf.myschool_ind,0)	<>	upd_rec.	myschool_ind	or
            nvl(prf.csm_basket_val_13wkbk,0)	<>	upd_rec.	csm_basket_val_13wkbk	or
            nvl(prf.csm_pref_store_13wkbk,0)	<>	upd_rec.	csm_pref_store_13wkbk	or
            nvl(prf.csm_shop_habit_seg_no_13wkbk,0)	<>	upd_rec.	csm_shop_habit_seg_no_13wkbk
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
    from   fnd_wfs_customer
   where  last_updated_date = g_date
;

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
end wh_prf_cust_018u;
