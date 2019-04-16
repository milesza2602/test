--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_156U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_156U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create customer interaction ex C2 fact table in the foundation layer
--               with input ex staging table from FV.
--  Tables:      Input  - stg_sas_talk2me_cpy
--               Output - fnd_talk2me
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


g_customer_no        stg_sas_talk2me_cpy.customer_no%type;
g_run_date           stg_sas_talk2me_cpy.run_date%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_156U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD talk2me EX SAS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
SELECT * FROM STG_SAS_TALK2ME_CPY
where (customer_no,run_date)
IN
(select customer_no,run_date
FROM STG_SAS_TALK2ME_CPY
group by customer_no,run_date
HAVING COUNT(*) > 1)
order by customer_no,run_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor fnd_del is
    select distinct run_date
    from   stg_sas_talk2me_cpy;


--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_customer_no  := 0;
   g_run_date     := '1 Jan 1900';

for dupp_record in stg_dup
   loop

    if  dupp_record.customer_no   = g_customer_no and
        dupp_record.run_date      = g_run_date then
        update stg_sas_talk2me_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_customer_no    := dupp_record.customer_no;
    g_run_date       := dupp_record.run_date;


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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_talk2me fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.customer_no	,
              cpy.run_date	,
            	cpy.wfs_customer_no	,
            	cpy.wfs_account_no	,
            	cpy.closed_ind	,
            	cpy.deceased_ind	,
            	cpy.fraud_ind	,
            	cpy.jailed_ind	,
            	cpy.delinquent_ind	,
            	cpy.title_code	,
            	cpy.first_middle_name_initial	,
            	cpy.first_name	,
            	cpy.last_name	,
            	cpy.preferred_language	,
            	cpy.email_address	,
            	cpy.cell_no	,
            	cpy.postal_address_line_1	,
            	cpy.postal_address_line_2	,
            	cpy.postal_address_line_3	,
            	cpy.postal_code	,
            	cpy.home_phone_no	,
            	cpy.work_phone_no	,
            	cpy.storcard_ind	,
            	cpy.creditcard_ind	,
            	cpy.differencecard_ind	,
            	cpy.myschool_ind	,
            	cpy.littleworld_ind	,
            	cpy.max_tran_date	,
            	cpy.storecard_otb	,
            	cpy.creditcard_otb	,
            	cpy.birthday_month	,
            	cpy.account_holder_age	,
            	cpy.gender_code	,
            	cpy.lsm	,
            	cpy.non_food_life_seg_code	,
            	cpy.food_life_seg_code	,
            	cpy.nfshv_current_seg	,
            	cpy.fshv_current_seg	,
            	cpy.csm_shopping_habit_segment_no	,
            	cpy.csm_preferred_store	,
            	cpy.start_tier	,
            	cpy.month_tier	,
            	cpy.month_spend	,
            	cpy.ytd_spend	,
              g_date as last_updated_date,
              cpy.DEBT_REVIEW_IND,
              cpy.CHARGED_OFF_IND,
              cpy.VITALITY_IND,
              cpy.MONTH_DISCOUNT,
              cpy.MONTH_GREEN_VALUE,
              cpy.MONTH_TIER_VALUE,
              cpy.YTD_DISCOUNT,
              cpy.YTD_GREEN_VALUE,
              cpy.YTD_TIER_VALUE
       from  stg_sas_talk2me_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_talk2me
       where  customer_no    = cpy.customer_no and
              run_date       = cpy.run_date
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

 
    l_text := 'TRUNCATE TABLE FND_TALK2ME STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute IMMEDIATE ('truncate table DWH_CUST_FOUNDATION.FND_TALK2ME');

--for del_rec in fnd_del
--   loop
--       delete from fnd_talk2me where run_date = del_rec.run_date;

--    end loop ;

    commit;

    select count(*)
    into   g_recs_read
    from   stg_sas_talk2me_cpy
    where  sys_process_code = 'N';

    commit;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;






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
      l_text :=  'THIS IS AN INSERTS ONLY LOAD!!!!!!!  '||TO_CHAR(SYSDATE,('dd mon yyyy hh24:mi:ss'));
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      l_text :=  'YOU ARE RERUNNING INSERTS THAT ARE ALREADY ON THE TABLE - WHY???  '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text :=  '(+-Line 225) DELETE THE DAYS RECS OFF THE FND_TALK2ME TABLE &  RERUN -OR- '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      l_text :=  'IGNORE AND CONTINUE BATCH IF THIS WAS A SIMPLE RERUN MISTAKE  '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance SEE LOG FILE';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - SEE LOG FILES');
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
end wh_fnd_cust_156u;
