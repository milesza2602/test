--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_150U_20181117
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_150U_20181117" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Date:        June 2015
-- Author:      Jerome Appollis
-- Purpose:     Create fnd_wfs_om4_application table in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - stg_om4_application_cpy
--              Output - fnd_wfs_om4_application
-- Packages:    constants, dwh_log, dwh_valid
--
-- Maintenance:
--  2016-03-11 N Chauhan - added 23 more fields for NCA compliance.
--  2016-03-16 N Chauhan - field SUBJECTIVE_REFERRAL renamed to AGENT_INITIATED
--  2016-05-13 N Chauhan - field MATCHED_CONSUMER_NO added to enable link to FND_OM4_CR_SCORES
--  2017-03-30 N Chauhan - Additional fields for Data Revitalisation project.
--  2017-06-20 S Ismail - Added field PAPER_APP_CREATED_BY_USER.
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
g_truncate_count     integer       :=  0;


g_application_no       stg_om4_application_cpy.credit_applications_id%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_150U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS OMAPP DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_om4_application_cpy
where (credit_applications_id)
in
(select credit_applications_id
from stg_om4_application_cpy 
group by credit_applications_id
having count(*) > 1) 
order by credit_applications_id, sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_wfs_om4_omapp_dly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_om4_application_cpy stg,
              fnd_wfs_om4_application fnd
      where   stg.credit_applications_id        = fnd.credit_applications_id   and             
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.credit_applications_id,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

g_application_no   := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.credit_applications_id       = g_application_no then
        update stg_om4_application_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_application_no   := dupp_record.credit_applications_id;
    
    end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_om4_application fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.credit_applications_id	,
              cpy.created_by_user	,
              cpy.channel	,
              cpy.hmda_required	,
              cpy.entered_time_stamp	,
              cpy.app_number	,
              cpy.decision_party	,
              cpy.credit_application_status	,
              cpy.previous_status	,
              cpy.status_date_time_stamp	,
              cpy.combined_cheque_account	,
              cpy.combined_savings_account	,
              cpy.possible_duplicate	,
              cpy.confirmed_duplicate	,
              cpy.verify_app_address	,
              cpy.verify_app_income	,
              cpy.verify_app_employment	,
              cpy.verify_co_app_address	,
              cpy.verify_co_app_income	,
              cpy.verify_co_app_employment	,
              cpy.decision_party_notified_flag	,
              cpy.conditions_init_accepted_flag	,
              cpy.home_mortgage_disclosure_act	,
              cpy.dup_app_check_performed_flag	,
              cpy.tenant_code	,
              cpy.om4_tab	,
              cpy.poi_required	,
              cpy.sa_id	,
              cpy.master_application	,
              cpy.application_saved_timestamp	,
              cpy.store_number	,
              cpy.agent_type ,
              cpy.campaign_id,
              cpy.promotion_id,
              g_date as last_updated_date ,
              cpy. participants_count ,
              cpy. include_insurance ,
              cpy. world_of_difference ,
              cpy. my_school_village_planet ,
              cpy. personal_insurance_quote ,
              cpy. loc_correspondence ,
              cpy. loc_terms_conditions ,
              cpy. comm_channel_for_communication ,
              cpy. comm_channel_for_statement ,
              cpy. comm_channel_email_address ,
              cpy. comm_channel_other_email ,
              cpy. marketing_contact ,
              cpy. marketing_phone ,
              cpy. marketing_sms ,
              cpy. marketing_email ,
              cpy. marketing_post ,
              cpy. marketing_authorization ,
              cpy. agent_initiated ,
              cpy. app_doc_document_type ,
              cpy. app_doc_document_status ,
              cpy. app_doc_external_id ,
              cpy. app_doc_document_id ,
              cpy. proof_type ,
              cpy. matched_consumer_no ,
              cpy. app_create_date ,
              cpy. relative_last_name ,
              cpy. relative_relationship ,
              cpy. relative_phone_no ,
              cpy. relative_phone_type ,
              cpy. comm_channel_def_note_channel ,
              cpy. subj_referral_agent_codes ,
              cpy. relative_details_title ,
              cpy. relative_details_middle_name ,
              cpy. relative_details_first_name,
              cpy. paper_app_created_by_user
              
      from    stg_om4_application_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_om4_application 
       where  credit_applications_id           = cpy.credit_applications_id)
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


for upd_rec in c_stg_wfs_om4_omapp_dly
   loop
     update fnd_wfs_om4_application fnd 
     set    
            fnd.created_by_user	=	upd_rec.created_by_user	,
            fnd.channel	=	upd_rec.channel	,
            fnd.hmda_required	=	upd_rec.hmda_required	,
            fnd.entered_time_stamp	=	upd_rec.entered_time_stamp	,
            fnd.app_number	=	upd_rec.app_number	,
            fnd.decision_party	=	upd_rec.decision_party	,
            fnd.credit_application_status	=	upd_rec.credit_application_status	,
            fnd.previous_status	=	upd_rec.previous_status	,
            fnd.status_date_time_stamp	=	upd_rec.status_date_time_stamp	,
            fnd.combined_cheque_account	=	upd_rec.combined_cheque_account	,
            fnd.combined_savings_account	=	upd_rec.combined_savings_account	,
            fnd.possible_duplicate	=	upd_rec.possible_duplicate	,
            fnd.confirmed_duplicate	=	upd_rec.confirmed_duplicate	,
            fnd.verify_app_address	=	upd_rec.verify_app_address	,
            fnd.verify_app_income	=	upd_rec.verify_app_income	,
            fnd.verify_app_employment	=	upd_rec.verify_app_employment	,
            fnd.verify_co_app_address	=	upd_rec.verify_co_app_address	,
            fnd.verify_co_app_income	=	upd_rec.verify_co_app_income	,
            fnd.verify_co_app_employment	=	upd_rec.verify_co_app_employment	,
            fnd.decision_party_notified_flag	=	upd_rec.decision_party_notified_flag	,
            fnd.conditions_init_accepted_flag	=	upd_rec.conditions_init_accepted_flag	,
            fnd.home_mortgage_disclosure_act	=	upd_rec.home_mortgage_disclosure_act	,
            fnd.dup_app_check_performed_flag	=	upd_rec.dup_app_check_performed_flag	,
            fnd.tenant_code	=	upd_rec.tenant_code	,
            fnd.om4_tab	=	upd_rec.om4_tab	,
            fnd.poi_required	=	upd_rec.poi_required	,
            fnd.sa_id	=	upd_rec.sa_id	,
            fnd.master_application	=	upd_rec.master_application	,
            fnd.application_saved_timestamp	=	upd_rec.application_saved_timestamp	,
            fnd.store_number	=	upd_rec.store_number	,
            fnd.agent_type = upd_rec.agent_type	,
            fnd.campaign_id = upd_rec.campaign_id	,
            fnd.promotion_id = upd_rec.promotion_id	,
            fnd.last_updated_date          = g_date ,
            fnd. participants_count = upd_rec. participants_count ,
            fnd. include_insurance = upd_rec. include_insurance ,
            fnd. world_of_difference = upd_rec. world_of_difference ,
            fnd. my_school_village_planet = upd_rec. my_school_village_planet ,
            fnd. personal_insurance_quote = upd_rec. personal_insurance_quote ,
            fnd. loc_correspondence = upd_rec. loc_correspondence ,
            fnd. loc_terms_conditions = upd_rec. loc_terms_conditions ,
            fnd. comm_channel_for_communication = upd_rec. comm_channel_for_communication ,
            fnd. comm_channel_for_statement = upd_rec. comm_channel_for_statement ,
            fnd. comm_channel_email_address = upd_rec. comm_channel_email_address ,
            fnd. comm_channel_other_email = upd_rec. comm_channel_other_email ,
            fnd. marketing_contact = upd_rec. marketing_contact ,
            fnd. marketing_phone = upd_rec. marketing_phone ,
            fnd. marketing_sms = upd_rec. marketing_sms ,
            fnd. marketing_email = upd_rec. marketing_email ,
            fnd. marketing_post = upd_rec. marketing_post ,
            fnd. marketing_authorization = upd_rec. marketing_authorization ,
            fnd. agent_initiated = upd_rec. agent_initiated ,
            fnd. app_doc_document_type = upd_rec. app_doc_document_type ,
            fnd. app_doc_document_status = upd_rec. app_doc_document_status ,
            fnd. app_doc_external_id = upd_rec. app_doc_external_id ,
            fnd. app_doc_document_id = upd_rec. app_doc_document_id ,
            fnd. proof_type = upd_rec. proof_type ,
            fnd. matched_consumer_no = upd_rec. matched_consumer_no ,
            fnd. app_create_date = upd_rec. app_create_date ,
            fnd. relative_last_name = upd_rec. relative_last_name ,
            fnd. relative_relationship = upd_rec. relative_relationship ,
            fnd. relative_phone_no = upd_rec. relative_phone_no ,
            fnd. relative_phone_type = upd_rec. relative_phone_type ,
            fnd. comm_channel_def_note_channel = upd_rec. comm_channel_def_note_channel ,
            fnd. subj_referral_agent_codes = upd_rec. subj_referral_agent_codes ,
            fnd. relative_details_title = upd_rec. relative_details_title ,
            fnd. relative_details_middle_name = upd_rec. relative_details_middle_name ,
            fnd. relative_details_first_name = upd_rec. relative_details_first_name,
            fnd. paper_app_created_by_user = upd_rec. paper_app_created_by_user


            
     where  fnd.credit_applications_id     = upd_rec.credit_applications_id and
            ( 
            nvl(fnd.created_by_user	,0)	<>	upd_rec.created_by_user	or
            nvl(fnd.channel	,0)	<>	upd_rec.channel	or
            nvl(fnd.hmda_required	,0)	<>	upd_rec.hmda_required	or
            nvl(fnd.entered_time_stamp	,NULL)	<>	upd_rec.entered_time_stamp	or
            nvl(fnd.app_number	,0)	<>	upd_rec.app_number	or
            nvl(fnd.decision_party	,0)	<>	upd_rec.decision_party	or
            nvl(fnd.credit_application_status	,0)	<>	upd_rec.credit_application_status	or
            nvl(fnd.previous_status,0) <>	upd_rec.previous_status	or
            nvl(fnd.status_date_time_stamp	,NULL)	<>	upd_rec.status_date_time_stamp	or
            nvl(fnd.combined_cheque_account	,0)	<>	upd_rec.combined_cheque_account	or
            nvl(fnd.combined_savings_account	,0)	<>	upd_rec.combined_savings_account	or
            nvl(fnd.possible_duplicate	,0)	<>	upd_rec.possible_duplicate	or
            nvl(fnd.confirmed_duplicate	,0)	<>	upd_rec.confirmed_duplicate	or
            nvl(fnd.verify_app_address	,0)	<>	upd_rec.verify_app_address	or
            nvl(fnd.verify_app_income	,0)	<>	upd_rec.verify_app_income	or
            nvl(fnd.verify_app_employment	,0)	<>	upd_rec.verify_app_employment	or
            nvl(fnd.verify_co_app_address	,0)	<>	upd_rec.verify_co_app_address	or
            nvl(fnd.verify_co_app_income	,0)	<>	upd_rec.verify_co_app_income	or
            nvl(fnd.verify_co_app_employment	,0)	<>	upd_rec.verify_co_app_employment	or
            nvl(fnd.decision_party_notified_flag	,0)	<>	upd_rec.decision_party_notified_flag	or
            nvl(fnd.conditions_init_accepted_flag	,0)	<>	upd_rec.conditions_init_accepted_flag	or
            nvl(fnd.home_mortgage_disclosure_act	,0)	<>	upd_rec.home_mortgage_disclosure_act	or
            nvl(fnd.dup_app_check_performed_flag	,0)	<>	upd_rec.dup_app_check_performed_flag	or
            nvl(fnd.tenant_code	,0)	<>	upd_rec.tenant_code	or
            nvl(fnd.om4_tab	,0)	<>	upd_rec.om4_tab	or
            nvl(fnd.poi_required	,0)	<>	upd_rec.poi_required	or
            nvl(fnd.sa_id	,0)	<>	upd_rec.sa_id	or
            nvl(fnd.master_application	,0)	<>	upd_rec.master_application	or
            nvl(fnd.application_saved_timestamp	,NULL)	<>	upd_rec.application_saved_timestamp	or
            nvl(fnd.store_number	,0)	<>	upd_rec.store_number or
            nvl(fnd.agent_type	,0)	<>	upd_rec.agent_type or
            nvl(fnd.campaign_id	,0)	<>	upd_rec.campaign_id or
            nvl(fnd.promotion_id	,0)	<>	upd_rec.promotion_id or
            NVL(fnd. participants_count,0) <> upd_rec. participants_count OR
            NVL(fnd. include_insurance,0) <> upd_rec. include_insurance OR
            NVL(fnd. world_of_difference,0) <> upd_rec. world_of_difference OR
            NVL(fnd. my_school_village_planet,0) <> upd_rec. my_school_village_planet OR
            NVL(fnd. personal_insurance_quote,0) <> upd_rec. personal_insurance_quote OR
            NVL(fnd. loc_correspondence,0) <> upd_rec. loc_correspondence OR
            NVL(fnd. loc_terms_conditions,0) <> upd_rec. loc_terms_conditions OR
            NVL(fnd. comm_channel_for_communication,0) <> upd_rec. comm_channel_for_communication OR
            NVL(fnd. comm_channel_for_statement,0) <> upd_rec. comm_channel_for_statement OR
            NVL(fnd. comm_channel_email_address,0) <> upd_rec. comm_channel_email_address OR
            NVL(fnd. comm_channel_other_email,0) <> upd_rec. comm_channel_other_email OR
            NVL(fnd. marketing_contact,0) <> upd_rec. marketing_contact OR
            NVL(fnd. marketing_phone,0) <> upd_rec. marketing_phone OR
            NVL(fnd. marketing_sms,0) <> upd_rec. marketing_sms OR
            NVL(fnd. marketing_email,0) <> upd_rec. marketing_email OR
            NVL(fnd. marketing_post,0) <> upd_rec. marketing_post OR
            NVL(fnd. marketing_authorization,0) <> upd_rec. marketing_authorization OR
            NVL(fnd. agent_initiated,0) <> upd_rec. agent_initiated OR
            NVL(fnd. app_doc_document_type,0) <> upd_rec. app_doc_document_type OR
            NVL(fnd. app_doc_document_status,0) <> upd_rec. app_doc_document_status OR
            NVL(fnd. app_doc_external_id,0) <> upd_rec. app_doc_external_id OR
            NVL(fnd. app_doc_document_id,0) <> upd_rec. app_doc_document_id OR
            NVL(fnd. proof_type,0) <> upd_rec. proof_type OR
            nvl(fnd. matched_consumer_no ,0) <> upd_rec. matched_consumer_no or
            nvl(fnd. app_create_date, '01 JAN 1900') <> upd_rec. app_create_date OR
            nvl(fnd. relative_last_name, 0) <> upd_rec. relative_last_name OR
            nvl(fnd. relative_relationship, 0) <> upd_rec. relative_relationship OR
            nvl(fnd. relative_phone_no, 0) <> upd_rec. relative_phone_no OR
            nvl(fnd. relative_phone_type, 0) <> upd_rec. relative_phone_type OR
            nvl(fnd. comm_channel_def_note_channel, 0) <> upd_rec. comm_channel_def_note_channel OR
            nvl(fnd. subj_referral_agent_codes, 0) <> upd_rec. subj_referral_agent_codes OR
            nvl(fnd. relative_details_title, 0) <> upd_rec. relative_details_title OR
            nvl(fnd. relative_details_middle_name, 0) <> upd_rec. relative_details_middle_name OR
            nvl(fnd. relative_details_first_name, 0) <> upd_rec. relative_details_first_name OR
            nvl(fnd. paper_app_created_by_user, 0) <> upd_rec. paper_app_created_by_user


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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_om4_application_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id,
              cpy.sys_source_sequence_no,
              sysdate,'Y','DWH',
              cpy.sys_middleware_batch_id,
              'VALIDATION FAIL - REFERENCIAL ERROR',
              cpy.credit_applications_id	,
              cpy.created_by_user	,
              cpy.channel	,
              cpy.hmda_required	,
              cpy.entered_time_stamp	,
              cpy.app_number	,
              cpy.decision_party	,
              cpy.credit_application_status	,
              cpy.previous_status	,
              cpy.status_date_time_stamp	,
              cpy.combined_cheque_account	,
              cpy.combined_savings_account	,
              cpy.possible_duplicate	,
              cpy.confirmed_duplicate	,
              cpy.verify_app_address	,
              cpy.verify_app_income	,
              cpy.verify_app_employment	,
              cpy.verify_co_app_address	,
              cpy.verify_co_app_income	,
              cpy.verify_co_app_employment	,
              cpy.decision_party_notified_flag	,
              cpy.conditions_init_accepted_flag	,
              cpy.home_mortgage_disclosure_act	,
              cpy.dup_app_check_performed_flag	,
              cpy.tenant_code	,
              cpy.om4_tab	,
              cpy.poi_required	,
              cpy.sa_id	,
              cpy.master_application	,
              cpy.application_saved_timestamp	,
              cpy.store_number,
              cpy.agent_type,
              cpy.campaign_id,
              cpy.promotion_id,
              cpy. participants_count ,
              cpy. include_insurance ,
              cpy. world_of_difference ,
              cpy. my_school_village_planet ,
              cpy. personal_insurance_quote ,
              cpy. loc_correspondence ,
              cpy. loc_terms_conditions ,
              cpy. comm_channel_for_communication ,
              cpy. comm_channel_for_statement ,
              cpy. comm_channel_email_address ,
              cpy. comm_channel_other_email ,
              cpy. marketing_contact ,
              cpy. marketing_phone ,
              cpy. marketing_sms ,
              cpy. marketing_email ,
              cpy. marketing_post ,
              cpy. marketing_authorization ,
              cpy. agent_initiated ,
              cpy. app_doc_document_type ,
              cpy. app_doc_document_status ,
              cpy. app_doc_external_id ,
              cpy. app_doc_document_id ,
              cpy. proof_type ,
              cpy. matched_consumer_no ,
              cpy. app_create_date ,
              cpy. relative_last_name ,
              cpy. relative_relationship ,
              cpy. relative_phone_no ,
              cpy. relative_phone_type ,
              cpy. comm_channel_def_note_channel ,
              cpy. subj_referral_agent_codes ,
              cpy. relative_details_title ,
              cpy. relative_details_middle_name ,
              cpy. relative_details_first_name,
              cpy. paper_app_created_by_user

              
      from   stg_om4_application_cpy cpy
      where  
--      (    
--      NOT EXISTS 
--        (SELECT * FROM  dim_table dim
--         where  cpy.xxx       = dim.xxx ) or
--      not exists 
--        (select * from  dim_table dim1
--         where  cpy.xxx    = dim1.xxx ) 
--      ) and 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
        sys_process_code = 'N';
         

g_recs_hospital := g_recs_hospital + sql%rowcount;
      
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
    
    select count(*)
    into   g_recs_read
    from   stg_om4_application_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


--********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_crd_acc_dly_cpy
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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

end wh_fnd_wfs_150u_20181117;
