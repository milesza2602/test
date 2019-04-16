--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_018U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_018U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_customer ex vision fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_customer_cpy
--               Output - fnd_wfs_customer
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


g_wfs_customer_no        stg_vsn_customer_cpy.wfs_customer_no%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_018U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUSTOMER MASTER EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_customer_cpy
where (wfs_customer_no)
in
(select wfs_customer_no
from stg_vsn_customer_cpy
group by wfs_customer_no
having count(*) > 1)
order by wfs_customer_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_customer is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_vsn_customer_cpy cpy,
              fnd_wfs_customer fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.wfs_customer_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no  := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no  then
        update stg_vsn_customer_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_wfs_customer_no    := dupp_record.wfs_customer_no;


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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_customer fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	wfs_customer_no	,
            	cpy.	identity_no	,
            	cpy.	phone_no_home	,
            	cpy.	phone_no_work	,
            	cpy.	phone_no_cell	,
            	cpy.	cust_name	,
            	cpy.	post_address1	,
            	cpy.	post_address2	,
            	cpy.	post_address3	,
            	cpy.	post_postcode	,
            	cpy.	email_address	,
            	0	,
            	'',
            	0,0,0,0,
            	'1 Jan 1901','1 Jan 1901','1 Jan 1901','1 Jan 1901',
            	cpy.	mail_me	,
            	'','','','',
            	cpy.	language	,
            	cpy.	gender	,
            	cpy.	marital_status	,
            	cpy.	race	,
            	cpy.	date_of_birth	,
            	floor(months_between(sysdate,cpy.date_of_birth  ) /12) as age_acc_holder,
            	cpy.	no_of_dependants	,
            	cpy.	age_child1	,
            	cpy.	age_child2	,
            	cpy.	age_child3	,
            	cpy.	age_child4	,
            	cpy.	age_child5	,
            	cpy.	ww_employee_no	,
            	cpy.	staff_co_id	,
            	cpy.	charge_card_acc	,
            	cpy.	loyalty_acc	,
            	cpy.	loan_acc	,
            	cpy.	unit_trust_acc	,
            	cpy.	notice_dep_acc	,
            	cpy.	credit_card_acc	,
            	cpy.	laybye_acc	,
            	cpy.	sds_acc	,
            	cpy.	wfs_customer_no,
            	cpy.	billing_cycle	,
            	cpy.	credit_buro_score	,
            	cpy.	cust_title	,
            	cpy.	cust_initial	,
            	cpy.	customer_status	,
            	cpy.	customer_type	,
            	cpy.	returned_mail	,
            	cpy.	credit_buro_date	,
            	'',
            	'',
            	0,0,0,
              g_date as last_updated_date
       from  stg_vsn_customer_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_wfs_customer
       where  wfs_customer_no    = cpy.wfs_customer_no
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



FOR upd_rec IN c_stg_vsn_customer
   loop
     update fnd_wfs_customer fnd
     set    fnd.	identity_no	=	upd_rec.	identity_no	,
            fnd.	phone_no_home	=	upd_rec.	phone_no_home	,
            fnd.	phone_no_work	=	upd_rec.	phone_no_work	,
            fnd.	phone_no_cell	=	upd_rec.	phone_no_cell	,
            fnd.	cust_name	=	upd_rec.	cust_name	,
            fnd.	post_address1	=	upd_rec.	post_address1	,
            fnd.	post_address2	=	upd_rec.	post_address2	,
            fnd.	post_address3	=	upd_rec.	post_address3	,
            fnd.	post_postcode	=	upd_rec.	post_postcode	,
            fnd.	email_address	=	upd_rec.	email_address	,
            fnd.	mail_me	=	upd_rec.	mail_me	,
            fnd.	language	=	upd_rec.	language	,
            fnd.	gender	=	upd_rec.	gender	,
            fnd.	marital_status	=	upd_rec.	marital_status	,
            fnd.	race	=	upd_rec.	race	,
            fnd.	date_of_birth	=	upd_rec.	date_of_birth	,
            fnd.	age_acc_holder	=	floor(months_between(sysdate,upd_rec.date_of_birth  ) /12)  ,
            fnd.	no_of_dependants	=	upd_rec.	no_of_dependants	,
            fnd.	age_child1	=	upd_rec.	age_child1	,
            fnd.	age_child2	=	upd_rec.	age_child2	,
            fnd.	age_child3	=	upd_rec.	age_child3	,
            fnd.	age_child4	=	upd_rec.	age_child4	,
            fnd.	age_child5	=	upd_rec.	age_child5	,
            fnd.	ww_employee_no	=	upd_rec.	ww_employee_no	,
            fnd.	staff_co_id	=	upd_rec.	staff_co_id	,
            fnd.	charge_card_acc	=	upd_rec.	charge_card_acc	,
            fnd.	loyalty_acc	=	upd_rec.	loyalty_acc	,
            fnd.	loan_acc	=	upd_rec.	loan_acc	,
            fnd.	unit_trust_acc	=	upd_rec.	unit_trust_acc	,
            fnd.	notice_dep_acc	=	upd_rec.	notice_dep_acc	,
            fnd.	credit_card_acc	=	upd_rec.	credit_card_acc	,
            fnd.	laybye_acc	=	upd_rec.	laybye_acc	,
            fnd.	sds_acc	=	upd_rec.	sds_acc	,
            fnd.	billing_cycle	=	upd_rec.	billing_cycle	,
            fnd.	credit_buro_score	=	upd_rec.	credit_buro_score	,
            fnd.	cust_title	=	upd_rec.	cust_title	,
            fnd.	cust_initial	=	upd_rec.	cust_initial	,
            fnd.	customer_status	=	upd_rec.	customer_status	,
            fnd.	customer_type	=	upd_rec.	customer_type	,
            fnd.	returned_mail	=	upd_rec.	returned_mail	,
            fnd.	credit_buro_date	=	upd_rec.	credit_buro_date	,
            fnd.  last_updated_date         = g_date
     where  fnd.	wfs_customer_no	      =	upd_rec.	wfs_customer_no and
            (
            nvl(fnd.wfs_customer_no	,0) <>	upd_rec.	wfs_customer_no	or
            nvl(fnd.identity_no	,0) <>	upd_rec.	identity_no	or
            nvl(fnd.phone_no_home	,0) <>	upd_rec.	phone_no_home	or
            nvl(fnd.phone_no_work	,0) <>	upd_rec.	phone_no_work	or
            nvl(fnd.phone_no_cell	,0) <>	upd_rec.	phone_no_cell	or
            nvl(fnd.cust_name	,0) <>	upd_rec.	cust_name	or
            nvl(fnd.post_address1	,0) <>	upd_rec.	post_address1	or
            nvl(fnd.post_address2	,0) <>	upd_rec.	post_address2	or
            nvl(fnd.post_address3	,0) <>	upd_rec.	post_address3	or
            nvl(fnd.post_postcode	,0) <>	upd_rec.	post_postcode	or
            nvl(fnd.email_address	,0) <>	upd_rec.	email_address	or
            nvl(fnd.mail_me	,0) <>	upd_rec.	mail_me	or
            nvl(fnd.language	,0) <>	upd_rec.	language	or
            nvl(fnd.gender	,0) <>	upd_rec.	gender	or
            nvl(fnd.marital_status	,0) <>	upd_rec.	marital_status	or
            nvl(fnd.race	,0) <>	upd_rec.	race	or
            nvl(fnd.date_of_birth	,'1 Jan 1900') <>	upd_rec.	date_of_birth	or
            nvl(fnd.no_of_dependants	,0) <>	upd_rec.	no_of_dependants	or
            nvl(fnd.age_child1	,'1 Jan 1900') <>	upd_rec.	age_child1	or
            nvl(fnd.age_child2	,'1 Jan 1900') <>	upd_rec.	age_child2	or
            nvl(fnd.age_child3	,'1 Jan 1900') <>	upd_rec.	age_child3	or
            nvl(fnd.age_child4	,'1 Jan 1900') <>	upd_rec.	age_child4	or
            nvl(fnd.age_child5	,'1 Jan 1900') <>	upd_rec.	age_child5	or
            nvl(fnd.ww_employee_no	,0) <>	upd_rec.	ww_employee_no	or
            nvl(fnd.staff_co_id	,0) <>	upd_rec.	staff_co_id	or
            nvl(fnd.charge_card_acc	,0) <>	upd_rec.	charge_card_acc	or
            nvl(fnd.loyalty_acc	,0) <>	upd_rec.	loyalty_acc	or
            nvl(fnd.loan_acc	,0) <>	upd_rec.	loan_acc	or
            nvl(fnd.unit_trust_acc	,0) <>	upd_rec.	unit_trust_acc	or
            nvl(fnd.notice_dep_acc	,0) <>	upd_rec.	notice_dep_acc	or
            nvl(fnd.credit_card_acc	,0) <>	upd_rec.	credit_card_acc	or
            nvl(fnd.laybye_acc	,0) <>	upd_rec.	laybye_acc	or
            nvl(fnd.sds_acc	,0) <>	upd_rec.	sds_acc	or
            nvl(fnd.billing_cycle	,0) <>	upd_rec.	billing_cycle	or
            nvl(fnd.credit_buro_score	,0) <>	upd_rec.	credit_buro_score	or
            nvl(fnd.cust_title	,0) <>	upd_rec.	cust_title	or
            nvl(fnd.cust_initial	,0) <>	upd_rec.	cust_initial	or
            nvl(fnd.customer_status	,0) <>	upd_rec.	customer_status	or
            nvl(fnd.customer_type	,0) <>	upd_rec.	customer_type	or
            nvl(fnd.returned_mail	,0) <>	upd_rec.	returned_mail	or
            nvl(fnd.credit_buro_date	,'1 Jan 1900') <>	upd_rec.	credit_buro_date

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
    from   stg_vsn_customer_cpy
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
--    update stg_vsn_customer_cpy
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
       RAISE;
end wh_fnd_cust_018u;
