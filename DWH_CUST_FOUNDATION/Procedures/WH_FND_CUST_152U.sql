--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_152U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_152U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create customer interaction ex C2 fact table in the foundation layer
--               with input ex staging table from FV.
--  Tables:      Input  - stg_c2_cust_interaction_cpy
--               Output - fnd_cust_interaction
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
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
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;


g_inquiry_id        stg_c2_cust_interaction_cpy.inquiry_id%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_152U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUSTOMER INTERCATION EX C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select /*+ FULL(cpy)  parallel(cpy,4) */ * from stg_c2_cust_interaction_cpy cpy
where (inquiry_id)
in
(select /*+ FULL(cpy1)  parallel(cpy1,4) */ inquiry_id
from stg_c2_cust_interaction_cpy cpy1
group by inquiry_id
having count(*) > 1)
order by inquiry_id,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_c2_cust_interaction is
select /*+ FULL(cpy)  parallel(4) */
              cpy.*
      from    stg_c2_cust_interaction_cpy cpy,
              fnd_cust_interaction fnd
      where   cpy.inquiry_id       = fnd.inquiry_id and
              cpy.sys_process_code = 'N'  and
            (
            nvl(fnd.account_contact_id	,0) <>	cpy.	account_contact_id	or
            nvl(fnd.account_id	,0) <>	cpy.	account_id	or
            nvl(fnd.unresolved_cust_id	,0) <>	cpy.	unresolved_cust_id	or
            nvl(fnd.customer_int_id	,0) <>	cpy.	customer_int_id	or
            nvl(fnd.interaction_date	,'1 Jan 1900') <>	cpy.	interaction_date	or
            nvl(fnd.cust_int_closed	,'1 Jan 1900') <>	cpy.	cust_int_closed	or
            nvl(fnd.cust_int_chng_date	,'1 Jan 1900') <>	cpy.	cust_int_chng_date	or
            nvl(fnd.cust_int_chng_by	,0) <>	cpy.	cust_int_chng_by	or
            nvl(fnd.inquiry_type_desc	,0) <>	cpy.	inquiry_type_desc	or
            nvl(fnd.front_end_desc	,0) <>	cpy.	front_end_desc	or
            nvl(fnd.front_end_appl_id	,0) <>	cpy.	front_end_appl_id	or
            nvl(fnd.category1_desc	,0) <>	cpy.	category1_desc	or
            nvl(fnd.category2_desc	,0) <>	cpy.	category2_desc	or
            nvl(fnd.status_desc	,0) <>	cpy.	status_desc	or
            nvl(fnd.state_desc	,0) <>	cpy.	state_desc	or
            nvl(fnd.priority_desc	,0) <>	cpy.	priority_desc	or
            nvl(fnd.channel_desc	,0) <>	cpy.	channel_desc	or
            nvl(fnd.business_area_desc	,0) <>	cpy.	business_area_desc	or
            nvl(fnd.inq_chng_date	,'1 Jan 1900') <>	cpy.	inq_chng_date	or
            nvl(fnd.inq_chng_by	,0) <>	cpy.	inq_chng_by	or
            nvl(fnd.inquiry_txt	,0) <>	cpy.	inquiry_txt	or
            nvl(fnd.inq_desc_chng_date	,'1 Jan 1900') <>	cpy.	inq_desc_chng_date	or
            nvl(fnd.inq_desc_chng_by	,0) <>	cpy.	inq_desc_chng_by	or
            nvl(fnd.logged_date	,'1 Jan 1900') <>	cpy.	logged_date	or
            nvl(fnd.owner_user_name	,0) <>	cpy.	owner_user_name	or
            nvl(fnd.owner_grp_name	,0) <>	cpy.	owner_grp_name	or
            nvl(fnd.transfer_user_name	,0) <>	cpy.	transfer_user_name	or
            nvl(fnd.transfer_grp_name	,0) <>	cpy.	transfer_grp_name	or
            nvl(fnd.inq_closed_date	,'1 Jan 1900') <>	cpy.	inq_closed_date	or
            nvl(fnd.closed_by_name	,0) <>	cpy.	closed_by_name	or
            nvl(fnd.logged_by_name	,0) <>	cpy.	logged_by_name	or
            nvl(fnd.inq_det_chng_date	,'1 Jan 1900') <>	cpy.	inq_det_chng_date	or
            nvl(fnd.inq_det_chng_by	,0) <>	cpy.	inq_det_chng_by	or
            nvl(fnd.order_id	,0) <>	cpy.	order_id	or
            nvl(fnd.order_prod_offer	,0) <>	cpy.	order_prod_offer	or
            nvl(fnd.solution_txt	,0) <>	cpy.	solution_txt	or
            nvl(fnd.inq_sol_chng_date	,'1 Jan 1900') <>	cpy.	inq_sol_chng_date	or
            nvl(fnd.inq_sol_chng_by	,0) <>	cpy.	inq_sol_chng_by	or
            nvl(fnd.staff_name	,0) <>	cpy.	staff_name	or
            nvl(fnd.staff_no	,0) <>	cpy.	staff_no	or
            nvl(fnd.inq_stff_chng_date	,'1 Jan 1900') <>	cpy.	inq_stff_chng_date	or
            nvl(fnd.inq_stff_chng_by	,0) <>	cpy.	inq_stff_chng_by	or
            nvl(fnd.classified_date	,'1 Jan 1900') <>	cpy.	classified_date	or
            nvl(fnd.respond_date	,'1 Jan 1900') <>	cpy.	respond_date	or
            nvl(fnd.return_date	,'1 Jan 1900') <>	cpy.	return_date	or
            nvl(fnd.trans_accept_date	,'1 Jan 1900') <>	cpy.	trans_accept_date	or
            nvl(fnd.transfer_date	,'1 Jan 1900') <>	cpy.	transfer_date	or
            nvl(fnd.qa_ind	,0) <>	cpy.	qa_ind	or
            nvl(fnd.receipt_ack_ind	,0) <>	cpy.	receipt_ack_ind	or
            nvl(fnd.return_ind	,0) <>	cpy.	return_ind	or
            nvl(fnd.assign_date	,'1 Jan 1900') <>	cpy.	assign_date	or
            nvl(fnd.feedback_requ_ind	,0) <>	cpy.	feedback_requ_ind	or
            nvl(fnd.callback_date	,'1 Jan 1900') <>	cpy.	callback_date	or
            nvl(fnd.transfer_ind	,0) <>	cpy.	transfer_ind	or
            nvl(fnd.inq_trck_chng_date	,'1 Jan 1900') <>	cpy.	inq_trck_chng_date	or
            nvl(fnd.inq_trck_chng_by	,0) <>	cpy.	inq_trck_chng_by	or
            nvl(fnd.itb_external_no	,0) <>	cpy.	itb_external_no	or
            nvl(fnd.offer_no	,0) <>	cpy.	offer_no	or
            nvl(fnd.camp_id	,0) <>	cpy.	camp_id	or
            nvl(fnd.uncontactable_no	,0) <>	cpy.	uncontactable_no	or
            nvl(fnd.creative_id	,0) <>	cpy.	creative_id	or
            nvl(fnd.sales_amount_no	,0) <>	cpy.	sales_amount_no	or
            nvl(fnd.customer_experience_no	,0) <>	cpy.	customer_experience_no	or
            nvl(fnd.customer_experience_desc	,0) <>	cpy.	customer_experience_desc	or
            nvl(fnd.sla_resolve_by_date	,'1 Jan 1900') <>	cpy.	sla_resolve_by_date	or
            nvl(fnd.sla_resolve_no	,0) <>	cpy.	sla_resolve_no	or
            nvl(fnd.report_ind	,0) <>	cpy.	report_ind	or
            nvl(fnd.inquiry_type_id	,0) <>	cpy.	inquiry_type_id	or
            nvl(fnd.logged_datetime	,'1 Jan 1900') <>	cpy.	logged_datetime	or
            nvl(fnd.closed_datetime	,'1 Jan 1900') <>	cpy.	closed_datetime
            )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.inquiry_id,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_inquiry_id  := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.inquiry_id   = g_inquiry_id  then
        update stg_c2_cust_interaction_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_inquiry_id    := dupp_record.inquiry_id;


   end loop;

   commit;

   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end remove_duplicates;

--**************************************************************************************************
-- Insert dummy m aster records to ensure RI
--**************************************************************************************************
procedure create_dummy_masters as
begin

--******************************************************************************

--      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
--      select /*+ FULL(cpy)  parallel (cpy,2) */
--             distinct
--             cpy.primary_account_no	,
--             0,
--             1,
--             g_date,
--             1
--      from   stg_c2_cust_interaction_cpy cpy

--       where not exists
--      (select /*+ nl_aj */ * from fnd_customer_product
--       where  product_no          = cpy.primary_account_no )
--       and    sys_process_code    = 'N'
--       and    cpy.primary_account_no is not null ;

--       g_recs_dummy := g_recs_dummy + sql%rowcount;
      commit;


 --******************************************************************************


  exception
      when dwh_errors.e_insert_error then
       l_message := 'DUMMY INS - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'DUMMY INS  - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end create_dummy_masters;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into fnd_cust_interaction fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	inquiry_id	,
            	cpy.	account_contact_id	,
            	cpy.	account_id	,
            	cpy.	unresolved_cust_id	,
            	cpy.	customer_int_id	,
            	cpy.	interaction_date	,
            	cpy.	cust_int_closed	,
            	cpy.	cust_int_chng_date	,
            	cpy.	cust_int_chng_by	,
            	cpy.	inquiry_type_desc	,
            	cpy.	front_end_desc	,
            	cpy.	front_end_appl_id	,
            	cpy.	category1_desc	,
            	cpy.	category2_desc	,
            	cpy.	status_desc	,
            	cpy.	state_desc	,
            	cpy.	priority_desc	,
            	cpy.	channel_desc	,
            	cpy.	business_area_desc	,
            	cpy.	inq_chng_date	,
            	cpy.	inq_chng_by	,
            	cpy.	inquiry_txt	,
            	cpy.	inq_desc_chng_date	,
            	cpy.	inq_desc_chng_by	,
            	cpy.	logged_date	,
            	cpy.	owner_user_name	,
            	cpy.	owner_grp_name	,
            	cpy.	transfer_user_name	,
            	cpy.	transfer_grp_name	,
            	cpy.	inq_closed_date	,
            	cpy.	closed_by_name	,
            	cpy.	logged_by_name	,
            	cpy.	inq_det_chng_date	,
            	cpy.	inq_det_chng_by	,
            	cpy.	order_id	,
            	cpy.	order_prod_offer	,
            	cpy.	solution_txt	,
            	cpy.	inq_sol_chng_date	,
            	cpy.	inq_sol_chng_by	,
            	cpy.	staff_name	,
            	cpy.	staff_no	,
            	cpy.	inq_stff_chng_date	,
            	cpy.	inq_stff_chng_by	,
            	cpy.	classified_date	,
            	cpy.	respond_date	,
            	cpy.	return_date	,
            	cpy.	trans_accept_date	,
            	cpy.	transfer_date	,
            	cpy.	qa_ind	,
            	cpy.	receipt_ack_ind	,
            	cpy.	return_ind	,
            	cpy.	assign_date	,
            	cpy.	feedback_requ_ind	,
            	cpy.	callback_date	,
            	cpy.	transfer_ind	,
            	cpy.	inq_trck_chng_date	,
            	cpy.	inq_trck_chng_by	,
            	cpy.	itb_external_no	,
            	cpy.	offer_no	,
            	cpy.	camp_id	,
            	cpy.	uncontactable_no	,
            	cpy.	creative_id	,
            	cpy.	sales_amount_no	,
            	cpy.	customer_experience_no	,
            	cpy.	customer_experience_desc	,
            	cpy.	sla_resolve_by_date	,
            	cpy.	sla_resolve_no	,
            	cpy.	report_ind	,
            	cpy.	inquiry_type_id	,
            	cpy.	logged_datetime	,
            	cpy.	closed_datetime	,
              g_date as last_updated_date
       from  stg_c2_cust_interaction_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_interaction
       where  inquiry_id    = cpy.inquiry_id
              )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       and sys_process_code = 'N';


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



FOR upd_rec IN c_stg_c2_cust_interaction
   loop
     update fnd_cust_interaction fnd
     set    fnd.	account_contact_id	=	upd_rec.	account_contact_id	,
            fnd.	account_id	=	upd_rec.	account_id	,
            fnd.	unresolved_cust_id	=	upd_rec.	unresolved_cust_id	,
            fnd.	customer_int_id	=	upd_rec.	customer_int_id	,
            fnd.	interaction_date	=	upd_rec.	interaction_date	,
            fnd.	cust_int_closed	=	upd_rec.	cust_int_closed	,
            fnd.	cust_int_chng_date	=	upd_rec.	cust_int_chng_date	,
            fnd.	cust_int_chng_by	=	upd_rec.	cust_int_chng_by	,
            fnd.	inquiry_type_desc	=	upd_rec.	inquiry_type_desc	,
            fnd.	front_end_desc	=	upd_rec.	front_end_desc	,
            fnd.	front_end_appl_id	=	upd_rec.	front_end_appl_id	,
            fnd.	category1_desc	=	upd_rec.	category1_desc	,
            fnd.	category2_desc	=	upd_rec.	category2_desc	,
            fnd.	status_desc	=	upd_rec.	status_desc	,
            fnd.	state_desc	=	upd_rec.	state_desc	,
            fnd.	priority_desc	=	upd_rec.	priority_desc	,
            fnd.	channel_desc	=	upd_rec.	channel_desc	,
            fnd.	business_area_desc	=	upd_rec.	business_area_desc	,
            fnd.	inq_chng_date	=	upd_rec.	inq_chng_date	,
            fnd.	inq_chng_by	=	upd_rec.	inq_chng_by	,
            fnd.	inquiry_txt	=	upd_rec.	inquiry_txt	,
            fnd.	inq_desc_chng_date	=	upd_rec.	inq_desc_chng_date	,
            fnd.	inq_desc_chng_by	=	upd_rec.	inq_desc_chng_by	,
            fnd.	logged_date	=	upd_rec.	logged_date	,
            fnd.	owner_user_name	=	upd_rec.	owner_user_name	,
            fnd.	owner_grp_name	=	upd_rec.	owner_grp_name	,
            fnd.	transfer_user_name	=	upd_rec.	transfer_user_name	,
            fnd.	transfer_grp_name	=	upd_rec.	transfer_grp_name	,
            fnd.	inq_closed_date	=	upd_rec.	inq_closed_date	,
            fnd.	closed_by_name	=	upd_rec.	closed_by_name	,
            fnd.	logged_by_name	=	upd_rec.	logged_by_name	,
            fnd.	inq_det_chng_date	=	upd_rec.	inq_det_chng_date	,
            fnd.	inq_det_chng_by	=	upd_rec.	inq_det_chng_by	,
            fnd.	order_id	=	upd_rec.	order_id	,
            fnd.	order_prod_offer	=	upd_rec.	order_prod_offer	,
            fnd.	solution_txt	=	upd_rec.	solution_txt	,
            fnd.	inq_sol_chng_date	=	upd_rec.	inq_sol_chng_date	,
            fnd.	inq_sol_chng_by	=	upd_rec.	inq_sol_chng_by	,
            fnd.	staff_name	=	upd_rec.	staff_name	,
            fnd.	staff_no	=	upd_rec.	staff_no	,
            fnd.	inq_stff_chng_date	=	upd_rec.	inq_stff_chng_date	,
            fnd.	inq_stff_chng_by	=	upd_rec.	inq_stff_chng_by	,
            fnd.	classified_date	=	upd_rec.	classified_date	,
            fnd.	respond_date	=	upd_rec.	respond_date	,
            fnd.	return_date	=	upd_rec.	return_date	,
            fnd.	trans_accept_date	=	upd_rec.	trans_accept_date	,
            fnd.	transfer_date	=	upd_rec.	transfer_date	,
            fnd.	qa_ind	=	upd_rec.	qa_ind	,
            fnd.	receipt_ack_ind	=	upd_rec.	receipt_ack_ind	,
            fnd.	return_ind	=	upd_rec.	return_ind	,
            fnd.	assign_date	=	upd_rec.	assign_date	,
            fnd.	feedback_requ_ind	=	upd_rec.	feedback_requ_ind	,
            fnd.	callback_date	=	upd_rec.	callback_date	,
            fnd.	transfer_ind	=	upd_rec.	transfer_ind	,
            fnd.	inq_trck_chng_date	=	upd_rec.	inq_trck_chng_date	,
            fnd.	inq_trck_chng_by	=	upd_rec.	inq_trck_chng_by	,
            fnd.	itb_external_no	=	upd_rec.	itb_external_no	,
            fnd.	offer_no	=	upd_rec.	offer_no	,
            fnd.	camp_id	=	upd_rec.	camp_id	,
            fnd.	uncontactable_no	=	upd_rec.	uncontactable_no	,
            fnd.	creative_id	=	upd_rec.	creative_id	,
            fnd.	sales_amount_no	=	upd_rec.	sales_amount_no	,
            fnd.	customer_experience_no	=	upd_rec.	customer_experience_no	,
            fnd.	customer_experience_desc	=	upd_rec.	customer_experience_desc	,
            fnd.	sla_resolve_by_date	=	upd_rec.	sla_resolve_by_date	,
            fnd.	sla_resolve_no	=	upd_rec.	sla_resolve_no	,
            fnd.	report_ind	=	upd_rec.	report_ind	,
            fnd.	inquiry_type_id	=	upd_rec.	inquiry_type_id	,
            fnd.	logged_datetime	=	upd_rec.	logged_datetime	,
            fnd.	closed_datetime	=	upd_rec.	closed_datetime	,

            fnd.  last_updated_date         = g_date
     where  fnd.	inquiry_id	      =	upd_rec.	inquiry_id ;

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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin


--   g_recs_hospital := g_recs_hospital + sql%rowcount;

   commit;


   exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;



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


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

--    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    create_dummy_masters;

    select count(*)
    into   g_recs_read
    from   stg_c2_cust_interaction_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    flagged_records_hospital;


--    Taken out for better performance --------------------
--    update stg_c2_cust_interaction_cpy
--    set    sys_process_code = 'Y';





--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
       RAISE;
end wh_fnd_cust_152u;
