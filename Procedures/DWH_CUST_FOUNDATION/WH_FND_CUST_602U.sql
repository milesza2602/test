--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_602U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_602U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2017
--  Author:      Alastair de Wet
--  Purpose:     Load Campaign Feedback information into a fact table in the foundation layer
--               with input staging table.
--  Tables:      Input  - stg_svoc_campaign_feedback_cpy
--               Output - fnd_svoc_campaign_feedback
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:s
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
g_date               date          := trunc(sysdate);

g_subscriber_key         stg_svoc_campaign_feedback_cpy.subscriber_key%type;
g_job_id                 stg_svoc_campaign_feedback_cpy.job_id%type;
g_event_date             stg_svoc_campaign_feedback_cpy.event_date%type;
g_event_type             stg_svoc_campaign_feedback_cpy.event_type%type;
g_url                    stg_svoc_campaign_feedback_cpy.url%type;

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_602U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD MARKET CLOUD CAMPAIGN FEEDBACK INFORMATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * 
  from stg_svoc_campaign_feedback_cpy
 where (subscriber_key,job_id,event_date,event_type,url) in       (select subscriber_key,job_id,event_date,event_type,url
                              from stg_svoc_campaign_feedback_cpy
                              group by subscriber_key,job_id,event_date,event_type,url
                              having count(*) > 1)
 order by subscriber_key,job_id,event_date,event_type,url,
          sys_source_batch_id desc,
          sys_source_sequence_no desc;


cursor c_stg_svoc_campaign_feedback is
select /*+ FULL(cpy)  parallel (cpy,4) */
       cpy.*
  from stg_svoc_campaign_feedback_cpy cpy,
       fnd_svoc_campaign_feedback fnd
 where cpy.subscriber_key   = fnd.subscriber_key and
       cpy.job_id           = fnd.job_id and
       cpy.event_date       = fnd.event_date and
       cpy.event_type       = fnd.event_type and
       cpy.url              = fnd.url and
       cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
 order by cpy.subscriber_key,
          cpy.job_id,cpy.event_date,cpy.event_type,cpy.url,
          cpy.sys_source_batch_id,
          cpy.sys_source_sequence_no;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin
  g_subscriber_key  := 0;
  g_job_id          := 0;
  g_event_date      := '1 Jan 2000';
  g_event_type      := '0';
  g_url             := '0';

  for dupp_record in stg_dup
  loop

    if  dupp_record.subscriber_key = g_subscriber_key and
        dupp_record.job_id         = g_job_id and
        dupp_record.event_date     = g_event_date and
        dupp_record.event_type     = g_event_type and
        dupp_record.url            = g_url then
        update stg_svoc_campaign_feedback_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_subscriber_key := dupp_record.subscriber_key;
    g_job_id         := dupp_record.job_id;
    g_event_date     := dupp_record.event_date;
    g_event_type     := dupp_record.event_type;
    g_url            := dupp_record.url;
    
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

  insert /*+ APPEND parallel (fnd,8) */ into fnd_svoc_campaign_feedback fnd
  select /*+ FULL(cpy)  parallel (cpy,8) */
          cpy.account_id,
          cpy.oyb_account_id,
          cpy.job_id,
          cpy.list_id,
          cpy.batch_id,
          cpy.subscriber_id,
          cpy.subscriber_key,
          cpy.event_date,
          cpy.is_unique,
          cpy.domain,
          cpy.event_type,
          cpy.bounce_category,
          cpy.bounce_sub_category,
          cpy.bounce_type,
          cpy.url,
          cpy.link_name,
          cpy.link_content,
          cpy.load_date,
          cpy.email_name,
          cpy.email_subject,
          cpy.control_group_ind,
          g_date as last_updated_date,
          cpy.opt_out_reason
  from    stg_svoc_campaign_feedback_cpy cpy
  where   not exists (select /*+ nl_aj */ * 
                      from fnd_svoc_campaign_feedback
                      where subscriber_key = cpy.subscriber_key and
                            job_id         = cpy.job_id and
                            event_date     = cpy.event_date and
                            event_type     = cpy.event_type and
                            url            = cpy.url)
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
  for upd_rec in c_stg_svoc_campaign_feedback
  loop
    update 
            fnd_svoc_campaign_feedback fnd
       set  fnd.	account_id	        =	upd_rec.	account_id,
            fnd.	oyb_account_id	    =	upd_rec.	oyb_account_id,
            fnd.	list_id	            =	upd_rec.	list_id,
            fnd.	batch_id	          =	upd_rec.	batch_id,
            fnd.	subscriber_id	      =	upd_rec.	subscriber_id,
            fnd.	is_unique         	=	upd_rec.	is_unique,
            fnd.	domain	            =	upd_rec.	domain,
            fnd.	bounce_category	    =	upd_rec.	bounce_category,
            fnd.	bounce_sub_category	=	upd_rec.	bounce_sub_category,
            fnd.	bounce_type	        =	upd_rec.	bounce_type,
            fnd.	link_name           =	upd_rec.	link_name,
            fnd.	link_content	      =	upd_rec.	link_content,
            fnd.	load_date	          =	upd_rec.	load_date,
            fnd.	email_name	        =	upd_rec.	email_name,
            fnd.	email_subject	      =	upd_rec.	email_subject,
            fnd.	control_group_ind   =	upd_rec.	control_group_ind,
            fnd.	opt_out_reason	    =	upd_rec.	opt_out_reason,
            fnd.  last_updated_date   = g_date
     where  fnd.	subscriber_key	    =	upd_rec.	subscriber_key and
            fnd.	job_id     	        =	upd_rec.	job_id and
            fnd.	event_date     	    =	upd_rec.	event_date and
            fnd.	event_type     	    =	upd_rec.	event_type and
            fnd.	url     	          =	upd_rec.	url and
      (              
            nvl(fnd.	account_id	        ,0) <>	upd_rec.	account_id or
            nvl(fnd.	oyb_account_id	    ,0) <>	upd_rec.	oyb_account_id or
            nvl(fnd.	list_id	            ,0) <>	upd_rec.	list_id or
            nvl(fnd.	batch_id	          ,0) <>	upd_rec.	batch_id or
            nvl(fnd.	subscriber_id	      ,0) <>	upd_rec.	subscriber_id or
            nvl(fnd.	is_unique         	,0) <>	upd_rec.	is_unique or
            nvl(fnd.	domain	            ,0) <>	upd_rec.	domain or
            nvl(fnd.	bounce_category	    ,0) <>	upd_rec.	bounce_category or
            nvl(fnd.	bounce_sub_category	,0) <>	upd_rec.	bounce_sub_category or
            nvl(fnd.	bounce_type	        ,0) <>	upd_rec.	bounce_type or
            nvl(fnd.	link_name           ,0) <>	upd_rec.	link_name or
            nvl(fnd.	link_content	      ,0) <>	upd_rec.	link_content or
            nvl(fnd.	load_date,'1 Jan 1900') <>	upd_rec.	load_date or
            nvl(fnd.	email_name	        ,0) <>  upd_rec.	email_name or
            nvl(fnd.	email_subject	      ,0) <>	upd_rec.	email_subject or
            nvl(fnd.	control_group_ind   ,0) <>	upd_rec.	control_group_ind or
            nvl(fnd.	opt_out_reason	    ,0) <>	upd_rec.	opt_out_reason 
      );

    g_recs_updated := g_recs_updated + sql%rowcount;
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
  l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  remove_duplicates;

  select count(*)
    into g_recs_read
    from stg_svoc_campaign_feedback_cpy
   where sys_process_code = 'N';

  l_text := 'BULK UPDATE STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  flagged_records_update;

  l_text := 'BULK INSERT STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  flagged_records_insert;
  
--    Taken out for better performance --------------------
--    update stg_svoc_campaign_feedback_cpy
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
/*
  if g_recs_read <> g_recs_inserted + g_recs_updated  then
    l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    p_success := false;
    l_message := 'ERROR - Record counts do not balance see log file';
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise_application_error (-20246,'Record count error - see log files');
  end if;
*/
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
end WH_FND_CUST_602U;
