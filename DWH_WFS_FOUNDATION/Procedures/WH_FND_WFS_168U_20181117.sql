--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_168U_20181117
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_168U_20181117" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: Load Credit Limit Increase (CLI) data
-- Tran_type: ODWH: OM4ITC   AIT: OM4ITCDCN
--
-- Date:       2016-06-14
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_OM4_CR_SCORES in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_OM4_CR_SCORES_CPY
--              Output - FND_WFS_OM4_CR_SCORES
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2016-04-25 N Chauhan - created - based on WH_FND_WFS_170U
--  2016-06-22 N Chauhan - referential integrity check added for matched_consumer_no.
--  2017-06-30 S Ismail - added MY01_CURR_MTHLY_INSTALL field

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_unique_key_field_val       dwh_wfs_foundation.stg_om4_cr_scores_cpy.consumer_no%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_168U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CREDIT LIMIT INCREASE (CLI) DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
   select * from  dwh_wfs_foundation.stg_om4_cr_scores_cpy
   where (consumer_no)
   in
   (select consumer_no 
    from dwh_wfs_foundation.stg_om4_cr_scores_cpy
    group by  consumer_no
    having count(*) > 1) 
   order by
    consumer_no,
    sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_om4_cr_scores_cpy stg,
              dwh_wfs_foundation.fnd_wfs_om4_cr_scores fnd
--              dwh_wfs_foundation.fnd_wfs_om4_application dep
      where   stg.consumer_no   = fnd.consumer_no     -- only ones existing in fnd
--              and stg.consumer_no  =  dep.matched_consumer_no       -- only those existing in depended table
              and stg.sys_process_code         = 'N'  

        and exists 
      (select /*+ nl_aj */ * from fnd_wfs_om4_application 
       where  matched_consumer_no = stg.consumer_no)
--    ... this 'exist' structure because fnd_wfs_om4_application may have multiple recs for a matched_consumer_no


-- Any further validation goes in here - like xxx.ind in (0,1) ---              




      order by
              stg.consumer_no,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key_field_val   := 0;

   for dupp_record in stg_dup
    loop

       if  dupp_record.consumer_no  = g_unique_key_field_val then
        update dwh_wfs_foundation.stg_om4_cr_scores_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key_field_val   := dupp_record.consumer_no;
    
    end loop;
   
   commit;
 
exception
      when others then
       l_message := substr('REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm, 1, 200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ append parallel (fnd,2) */ into fnd_wfs_om4_cr_scores fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. consumer_no ,
         cpy. application_id ,
         cpy. ait_seq_no ,
         cpy. identification_type ,
         cpy. identification_number ,
         cpy. result_status ,
         cpy. bccfilter1 ,
         cpy. bccfilter2 ,
         cpy. bccfilter3 ,
         cpy. bccfilter4 ,
         cpy. empirica_score ,
         cpy. empirica_exclusion_code ,
         cpy. empirica_indicator ,
         cpy. bcc_indicator ,
         cpy. bcc_score ,
         cpy. outcome ,
         cpy. reason ,
         cpy. response_status ,
         cpy. processing_start_date ,
         cpy. processing_time_secs ,
         cpy. unique_ref_guid ,
         cpy. mx01_consumer_number ,
         cpy. mx01_tot_active_accts ,
         cpy. mx01_tot_closed_accts_24mths ,
         cpy. mx01_tot_adverse_accts_24mths ,
         cpy. mx01_highest_act_mths_24mths ,
         cpy. mx01_no_revolving_accts ,
         cpy. mx01_no_curr_installment_accts ,
         cpy. mx01_no_curr_open_accts ,
         cpy. mx01_curr_balance ,
         cpy. mx01_curr_balance_ind ,
         cpy. mx01_curr_mthly_install ,
         cpy. mx01_curr_mthly_install_balind ,
         cpy. mx01_ca_amount ,
         cpy. mx01_ca_amnt_bal_ind ,
         cpy. enq_definite_match_count ,
         cpy. enq_possible_match_count ,
         cpy. enq_matched_consumer_no ,
         cpy. enq_possible_consumer_no ,
         cpy. enq_possible_adverse_ind ,
         cpy. nc04_consumer_number ,
         cpy. nc04_own_enq_1yrback ,
         cpy. nc04_own_enq_2yrsback ,
         cpy. nc04_own_enq_over_2yrsback ,
         cpy. nc04_other_enq_1yrback ,
         cpy. nc04_other_enq_2yrsback ,
         cpy. nc04_other_enq_over_2yrsback ,
         cpy. nc04_judgements_1yrback ,
         cpy. nc04_judgements_2yrsback ,
         cpy. nc04_judgements_over_2yrsback ,
         cpy. nc04_notices_1yrback ,
         cpy. nc04_notices_2yrsback ,
         cpy. nc04_notices_over_2yrsback ,
         cpy. nc04_defaults1yrback ,
         cpy. nc04_defaults_2yrsback ,
         cpy. nc04_defaults_over_2yrsback ,
         cpy. nc04_pay_profile_1yrback ,
         cpy. nc04_pay_profile_2yrsback ,
         cpy. nc04_pay_profile_over_2yrsback ,
         cpy. nc04_trce_alerts_1yrback ,
         cpy. nc04_trce_alerts_2yrsback ,
         cpy. nc04_trce_alerts_over_2yrsback ,
         cpy. subscriber_code ,
         cpy. client_reference ,
         cpy. branch_no ,
         cpy. batch_no ,
         cpy. em07_consumer_no ,
         cpy. em07_empirica_score ,
         cpy. em07_exclusion_code ,
         cpy. em07_exclusion_description ,
         cpy. em07_reason_code1 ,
         cpy. em07_reason_code2 ,
         cpy. em07_reason_code3 ,
         cpy. em07_reason_code4 ,
         cpy. em07_reason_desc_1 ,
         cpy. em07_reason_desc_2 ,
         cpy. em07_reason_desc_3 ,
         cpy. em07_reason_desc_4 ,
         cpy. em07_expansion_score ,
         cpy. em07_expansion_score_desc ,
         cpy. em07_empirica_version ,
         cpy. mc01_segment_code ,
         cpy. mc01_consumer_no ,
         cpy. mc01_curr_yr_enq_client ,
         cpy. mc01_curr_yr_enq_oth_sub ,
         cpy. mc01_curr_yr_pos_nlr_loans ,
         cpy. mc01_curr_yr_hmths_in_arrears ,
         cpy. mc01_prev_yr_enq_client ,
         cpy. mc01_prev_yr_enq_oth_sub ,
         cpy. mc01_prev_yr_pos_nlr_loans ,
         cpy. mc01_prev_yr_hmnths_inarrears ,
         cpy. mc01_cumulative_instal_value ,
         cpy. mc01_cumulative_out_balance ,
         cpy. mc01_wrst_mnth_in_arrears ,
         cpy. sbc_consumer_no ,
         cpy. gr01_suburb_code ,
         cpy. gr01_data_date ,
         cpy. gr01_load_date ,
         cpy. gr01_index_a ,
         cpy. gr01_index_b ,
         cpy. gr01_index_c ,
         cpy. gr01_index_d ,
         cpy. gr01_index_e ,
         cpy. gr01_index_f ,
         cpy. gr01_index_g ,
         cpy. gr01_index_h ,
         cpy. gr01_index_i ,
         cpy. gr01_index_j ,
         cpy. gr01_index_k ,
         cpy. gr01_index_l ,
         cpy. gr01_index_m ,
         cpy. gr01_index_n ,
         cpy. gr01_index_o ,
         cpy. gr01_index_p ,
         cpy. bcc04_dm001al ,
         cpy. bcc04_dm002al ,
         cpy. bcc04_dm003al ,
         cpy. bcc04_dm004al ,
         cpy. bcc04_dm005al ,
         cpy. bcc04_dm006al ,
         cpy. bcc04_dm007al ,
         cpy. bcc04_eq001al ,
         cpy. bcc04_eq001cl ,
         cpy. bcc04_eq001fn ,
         cpy. bcc04_eq002al ,
         cpy. bcc04_eq002cl ,
         cpy. bcc04_eq002fc ,
         cpy. bcc04_eq002nl ,
         cpy. bcc04_eq003cl ,
         cpy. bcc04_eq003fn ,
         cpy. bcc04_eq004al ,
         cpy. bcc04_eq004cl ,
         cpy. bcc04_eq004fc ,
         cpy. bcc04_eq004nl ,
         cpy. bcc04_eq007al ,
         cpy. bcc04_eq008al ,
         cpy. bcc04_eq008cl ,
         cpy. bcc04_eq008fc ,
         cpy. bcc04_ng001al ,
         cpy. bcc04_ng004al ,
         cpy. bcc04_ng008al ,
         cpy. bcc04_ng011al ,
         cpy. bcc04_ng022al ,
         cpy. bcc04_ng034al ,
         cpy. bcc04_pp001al ,
         cpy. bcc04_pp001cc ,
         cpy. bcc04_pp001cl ,
         cpy. bcc04_pp001fc ,
         cpy. bcc04_pp001fl ,
         cpy. bcc04_pp001nl ,
         cpy. bcc04_pp001pl ,
         cpy. bcc04_pp002al ,
         cpy. bcc04_pp002fc ,
         cpy. bcc04_pp003al ,
         cpy. bcc04_pp005al ,
         cpy. bcc04_pp005cc ,
         cpy. bcc04_pp005fl ,
         cpy. bcc04_pp005pl ,
         cpy. bcc04_pp006al ,
         cpy. bcc04_pp007al ,
         cpy. bcc04_pp007nl ,
         cpy. bcc04_pp008al ,
         cpy. bcc04_pp008nl ,
         cpy. bcc04_pp009nl ,
         cpy. bcc04_pp013al ,
         cpy. bcc04_pp014al ,
         cpy. bcc04_pp017cc ,
         cpy. bcc04_pp020al ,
         cpy. bcc04_pp027al ,
         cpy. bcc04_pp027cl ,
         cpy. bcc04_pp032cl ,
         cpy. bcc04_pp033al ,
         cpy. bcc04_pp033cc ,
         cpy. bcc04_pp034al ,
         cpy. bcc04_pp035cl ,
         cpy. bcc04_pp040cc ,
         cpy. bcc04_pp044al ,
         cpy. bcc04_pp044fc ,
         cpy. bcc04_pp044nl ,
         cpy. bcc04_pp045al ,
         cpy. bcc04_pp045fl ,
         cpy. bcc04_pp046cc ,
         cpy. bcc04_pp050al ,
         cpy. bcc04_pp050cc ,
         cpy. bcc04_pp051al ,
         cpy. bcc04_pp051cc ,
         cpy. bcc04_pp052al ,
         cpy. bcc04_pp053al ,
         cpy. bcc04_pp058al ,
         cpy. bcc04_pp058cl ,
         cpy. bcc04_pp058nl ,
         cpy. bcc04_pp059al ,
         cpy. bcc04_pp059fc ,
         cpy. bcc04_pp060al ,
         cpy. bcc04_pp060cc ,
         cpy. bcc04_pp060cl ,
         cpy. bcc04_pp061al ,
         cpy. bcc04_pp066al ,
         cpy. bcc04_pp067al ,
         cpy. bcc04_pp068al ,
         cpy. bcc04_pp068cl ,
         cpy. bcc04_pp069al ,
         cpy. bcc04_pp069cc ,
         cpy. bcc04_pp069nl ,
         cpy. bcc04_pp070al ,
         cpy. bcc04_pp070cc ,
         cpy. bcc04_pp070cl ,
         cpy. bcc04_pp070fc ,
         cpy. bcc04_pp070nl ,
         cpy. bcc04_pp071al ,
         cpy. bcc04_pp072cc ,
         cpy. bcc04_pp074al ,
         cpy. bcc04_pp078cl ,
         cpy. bcc04_pp079al ,
         cpy. bcc04_pp081cl ,
         cpy. bcc04_pp082nl ,
         cpy. bcc04_pp100cl ,
         cpy. bcc04_pp104cl ,
         g_date as last_updated_date,
         cpy. my01_curr_mthly_install
              
      from  dwh_wfs_foundation.stg_om4_cr_scores_cpy cpy
 --        inner join dwh_wfs_foundation.fnd_wfs_om4_application dep  on dep.matched_consumer_no = cpy.consumer_no
         left outer join dwh_wfs_foundation.fnd_wfs_om4_cr_scores fnd on fnd.consumer_no = cpy.consumer_no
      where fnd.consumer_no is null
          
        and exists 
      (select /*+ nl_aj */ * from fnd_wfs_om4_application 
       where  matched_consumer_no = cpy.consumer_no)
--    ... this 'exist' structure because fnd_wfs_om4_application may have multiple recs for a matched_consumer_no


-- Any further validation goes in here - like xxx.ind in (0,1) ---  


       and sys_process_code = 'N'; 


      g_recs_inserted := g_recs_inserted + sql%rowcount;
      
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := substr('FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := substr('FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;


--************************************************************************************************** 
-- Updates existing records in the staging table into foundation if there are changes
--**************************************************************************************************

procedure flagged_records_update as
begin


for upd_rec in c_stg
   loop
     update fnd_wfs_om4_cr_scores fnd 
     set    
         fnd. consumer_no = upd_rec. consumer_no ,
         fnd. application_id = upd_rec. application_id ,
         fnd. ait_seq_no = upd_rec. ait_seq_no ,
         fnd. identification_type = upd_rec. identification_type ,
         fnd. identification_number = upd_rec. identification_number ,
         fnd. result_status = upd_rec. result_status ,
         fnd. bccfilter1 = upd_rec. bccfilter1 ,
         fnd. bccfilter2 = upd_rec. bccfilter2 ,
         fnd. bccfilter3 = upd_rec. bccfilter3 ,
         fnd. bccfilter4 = upd_rec. bccfilter4 ,
         fnd. empirica_score = upd_rec. empirica_score ,
         fnd. empirica_exclusion_code = upd_rec. empirica_exclusion_code ,
         fnd. empirica_indicator = upd_rec. empirica_indicator ,
         fnd. bcc_indicator = upd_rec. bcc_indicator ,
         fnd. bcc_score = upd_rec. bcc_score ,
         fnd. outcome = upd_rec. outcome ,
         fnd. reason = upd_rec. reason ,
         fnd. response_status = upd_rec. response_status ,
         fnd. processing_start_date = upd_rec. processing_start_date ,
         fnd. processing_time_secs = upd_rec. processing_time_secs ,
         fnd. unique_ref_guid = upd_rec. unique_ref_guid ,
         fnd. mx01_consumer_number = upd_rec. mx01_consumer_number ,
         fnd. mx01_tot_active_accts = upd_rec. mx01_tot_active_accts ,
         fnd. mx01_tot_closed_accts_24mths = upd_rec. mx01_tot_closed_accts_24mths ,
         fnd. mx01_tot_adverse_accts_24mths = upd_rec. mx01_tot_adverse_accts_24mths ,
         fnd. mx01_highest_act_mths_24mths = upd_rec. mx01_highest_act_mths_24mths ,
         fnd. mx01_no_revolving_accts = upd_rec. mx01_no_revolving_accts ,
         fnd. mx01_no_curr_installment_accts = upd_rec. mx01_no_curr_installment_accts ,
         fnd. mx01_no_curr_open_accts = upd_rec. mx01_no_curr_open_accts ,
         fnd. mx01_curr_balance = upd_rec. mx01_curr_balance ,
         fnd. mx01_curr_balance_ind = upd_rec. mx01_curr_balance_ind ,
         fnd. mx01_curr_mthly_install = upd_rec. mx01_curr_mthly_install ,
         fnd. mx01_curr_mthly_install_balind = upd_rec. mx01_curr_mthly_install_balind ,
         fnd. mx01_ca_amount = upd_rec. mx01_ca_amount ,
         fnd. mx01_ca_amnt_bal_ind = upd_rec. mx01_ca_amnt_bal_ind ,
         fnd. enq_definite_match_count = upd_rec. enq_definite_match_count ,
         fnd. enq_possible_match_count = upd_rec. enq_possible_match_count ,
         fnd. enq_matched_consumer_no = upd_rec. enq_matched_consumer_no ,
         fnd. enq_possible_consumer_no = upd_rec. enq_possible_consumer_no ,
         fnd. enq_possible_adverse_ind = upd_rec. enq_possible_adverse_ind ,
         fnd. nc04_consumer_number = upd_rec. nc04_consumer_number ,
         fnd. nc04_own_enq_1yrback = upd_rec. nc04_own_enq_1yrback ,
         fnd. nc04_own_enq_2yrsback = upd_rec. nc04_own_enq_2yrsback ,
         fnd. nc04_own_enq_over_2yrsback = upd_rec. nc04_own_enq_over_2yrsback ,
         fnd. nc04_other_enq_1yrback = upd_rec. nc04_other_enq_1yrback ,
         fnd. nc04_other_enq_2yrsback = upd_rec. nc04_other_enq_2yrsback ,
         fnd. nc04_other_enq_over_2yrsback = upd_rec. nc04_other_enq_over_2yrsback ,
         fnd. nc04_judgements_1yrback = upd_rec. nc04_judgements_1yrback ,
         fnd. nc04_judgements_2yrsback = upd_rec. nc04_judgements_2yrsback ,
         fnd. nc04_judgements_over_2yrsback = upd_rec. nc04_judgements_over_2yrsback ,
         fnd. nc04_notices_1yrback = upd_rec. nc04_notices_1yrback ,
         fnd. nc04_notices_2yrsback = upd_rec. nc04_notices_2yrsback ,
         fnd. nc04_notices_over_2yrsback = upd_rec. nc04_notices_over_2yrsback ,
         fnd. nc04_defaults1yrback = upd_rec. nc04_defaults1yrback ,
         fnd. nc04_defaults_2yrsback = upd_rec. nc04_defaults_2yrsback ,
         fnd. nc04_defaults_over_2yrsback = upd_rec. nc04_defaults_over_2yrsback ,
         fnd. nc04_pay_profile_1yrback = upd_rec. nc04_pay_profile_1yrback ,
         fnd. nc04_pay_profile_2yrsback = upd_rec. nc04_pay_profile_2yrsback ,
         fnd. nc04_pay_profile_over_2yrsback = upd_rec. nc04_pay_profile_over_2yrsback ,
         fnd. nc04_trce_alerts_1yrback = upd_rec. nc04_trce_alerts_1yrback ,
         fnd. nc04_trce_alerts_2yrsback = upd_rec. nc04_trce_alerts_2yrsback ,
         fnd. nc04_trce_alerts_over_2yrsback = upd_rec. nc04_trce_alerts_over_2yrsback ,
         fnd. subscriber_code = upd_rec. subscriber_code ,
         fnd. client_reference = upd_rec. client_reference ,
         fnd. branch_no = upd_rec. branch_no ,
         fnd. batch_no = upd_rec. batch_no ,
         fnd. em07_consumer_no = upd_rec. em07_consumer_no ,
         fnd. em07_empirica_score = upd_rec. em07_empirica_score ,
         fnd. em07_exclusion_code = upd_rec. em07_exclusion_code ,
         fnd. em07_exclusion_description = upd_rec. em07_exclusion_description ,
         fnd. em07_reason_code1 = upd_rec. em07_reason_code1 ,
         fnd. em07_reason_code2 = upd_rec. em07_reason_code2 ,
         fnd. em07_reason_code3 = upd_rec. em07_reason_code3 ,
         fnd. em07_reason_code4 = upd_rec. em07_reason_code4 ,
         fnd. em07_reason_desc_1 = upd_rec. em07_reason_desc_1 ,
         fnd. em07_reason_desc_2 = upd_rec. em07_reason_desc_2 ,
         fnd. em07_reason_desc_3 = upd_rec. em07_reason_desc_3 ,
         fnd. em07_reason_desc_4 = upd_rec. em07_reason_desc_4 ,
         fnd. em07_expansion_score = upd_rec. em07_expansion_score ,
         fnd. em07_expansion_score_desc = upd_rec. em07_expansion_score_desc ,
         fnd. em07_empirica_version = upd_rec. em07_empirica_version ,
         fnd. mc01_segment_code = upd_rec. mc01_segment_code ,
         fnd. mc01_consumer_no = upd_rec. mc01_consumer_no ,
         fnd. mc01_curr_yr_enq_client = upd_rec. mc01_curr_yr_enq_client ,
         fnd. mc01_curr_yr_enq_oth_sub = upd_rec. mc01_curr_yr_enq_oth_sub ,
         fnd. mc01_curr_yr_pos_nlr_loans = upd_rec. mc01_curr_yr_pos_nlr_loans ,
         fnd. mc01_curr_yr_hmths_in_arrears = upd_rec. mc01_curr_yr_hmths_in_arrears ,
         fnd. mc01_prev_yr_enq_client = upd_rec. mc01_prev_yr_enq_client ,
         fnd. mc01_prev_yr_enq_oth_sub = upd_rec. mc01_prev_yr_enq_oth_sub ,
         fnd. mc01_prev_yr_pos_nlr_loans = upd_rec. mc01_prev_yr_pos_nlr_loans ,
         fnd. mc01_prev_yr_hmnths_inarrears = upd_rec. mc01_prev_yr_hmnths_inarrears ,
         fnd. mc01_cumulative_instal_value = upd_rec. mc01_cumulative_instal_value ,
         fnd. mc01_cumulative_out_balance = upd_rec. mc01_cumulative_out_balance ,
         fnd. mc01_wrst_mnth_in_arrears = upd_rec. mc01_wrst_mnth_in_arrears ,
         fnd. sbc_consumer_no = upd_rec. sbc_consumer_no ,
         fnd. gr01_suburb_code = upd_rec. gr01_suburb_code ,
         fnd. gr01_data_date = upd_rec. gr01_data_date ,
         fnd. gr01_load_date = upd_rec. gr01_load_date ,
         fnd. gr01_index_a = upd_rec. gr01_index_a ,
         fnd. gr01_index_b = upd_rec. gr01_index_b ,
         fnd. gr01_index_c = upd_rec. gr01_index_c ,
         fnd. gr01_index_d = upd_rec. gr01_index_d ,
         fnd. gr01_index_e = upd_rec. gr01_index_e ,
         fnd. gr01_index_f = upd_rec. gr01_index_f ,
         fnd. gr01_index_g = upd_rec. gr01_index_g ,
         fnd. gr01_index_h = upd_rec. gr01_index_h ,
         fnd. gr01_index_i = upd_rec. gr01_index_i ,
         fnd. gr01_index_j = upd_rec. gr01_index_j ,
         fnd. gr01_index_k = upd_rec. gr01_index_k ,
         fnd. gr01_index_l = upd_rec. gr01_index_l ,
         fnd. gr01_index_m = upd_rec. gr01_index_m ,
         fnd. gr01_index_n = upd_rec. gr01_index_n ,
         fnd. gr01_index_o = upd_rec. gr01_index_o ,
         fnd. gr01_index_p = upd_rec. gr01_index_p ,
         fnd. bcc04_dm001al = upd_rec. bcc04_dm001al ,
         fnd. bcc04_dm002al = upd_rec. bcc04_dm002al ,
         fnd. bcc04_dm003al = upd_rec. bcc04_dm003al ,
         fnd. bcc04_dm004al = upd_rec. bcc04_dm004al ,
         fnd. bcc04_dm005al = upd_rec. bcc04_dm005al ,
         fnd. bcc04_dm006al = upd_rec. bcc04_dm006al ,
         fnd. bcc04_dm007al = upd_rec. bcc04_dm007al ,
         fnd. bcc04_eq001al = upd_rec. bcc04_eq001al ,
         fnd. bcc04_eq001cl = upd_rec. bcc04_eq001cl ,
         fnd. bcc04_eq001fn = upd_rec. bcc04_eq001fn ,
         fnd. bcc04_eq002al = upd_rec. bcc04_eq002al ,
         fnd. bcc04_eq002cl = upd_rec. bcc04_eq002cl ,
         fnd. bcc04_eq002fc = upd_rec. bcc04_eq002fc ,
         fnd. bcc04_eq002nl = upd_rec. bcc04_eq002nl ,
         fnd. bcc04_eq003cl = upd_rec. bcc04_eq003cl ,
         fnd. bcc04_eq003fn = upd_rec. bcc04_eq003fn ,
         fnd. bcc04_eq004al = upd_rec. bcc04_eq004al ,
         fnd. bcc04_eq004cl = upd_rec. bcc04_eq004cl ,
         fnd. bcc04_eq004fc = upd_rec. bcc04_eq004fc ,
         fnd. bcc04_eq004nl = upd_rec. bcc04_eq004nl ,
         fnd. bcc04_eq007al = upd_rec. bcc04_eq007al ,
         fnd. bcc04_eq008al = upd_rec. bcc04_eq008al ,
         fnd. bcc04_eq008cl = upd_rec. bcc04_eq008cl ,
         fnd. bcc04_eq008fc = upd_rec. bcc04_eq008fc ,
         fnd. bcc04_ng001al = upd_rec. bcc04_ng001al ,
         fnd. bcc04_ng004al = upd_rec. bcc04_ng004al ,
         fnd. bcc04_ng008al = upd_rec. bcc04_ng008al ,
         fnd. bcc04_ng011al = upd_rec. bcc04_ng011al ,
         fnd. bcc04_ng022al = upd_rec. bcc04_ng022al ,
         fnd. bcc04_ng034al = upd_rec. bcc04_ng034al ,
         fnd. bcc04_pp001al = upd_rec. bcc04_pp001al ,
         fnd. bcc04_pp001cc = upd_rec. bcc04_pp001cc ,
         fnd. bcc04_pp001cl = upd_rec. bcc04_pp001cl ,
         fnd. bcc04_pp001fc = upd_rec. bcc04_pp001fc ,
         fnd. bcc04_pp001fl = upd_rec. bcc04_pp001fl ,
         fnd. bcc04_pp001nl = upd_rec. bcc04_pp001nl ,
         fnd. bcc04_pp001pl = upd_rec. bcc04_pp001pl ,
         fnd. bcc04_pp002al = upd_rec. bcc04_pp002al ,
         fnd. bcc04_pp002fc = upd_rec. bcc04_pp002fc ,
         fnd. bcc04_pp003al = upd_rec. bcc04_pp003al ,
         fnd. bcc04_pp005al = upd_rec. bcc04_pp005al ,
         fnd. bcc04_pp005cc = upd_rec. bcc04_pp005cc ,
         fnd. bcc04_pp005fl = upd_rec. bcc04_pp005fl ,
         fnd. bcc04_pp005pl = upd_rec. bcc04_pp005pl ,
         fnd. bcc04_pp006al = upd_rec. bcc04_pp006al ,
         fnd. bcc04_pp007al = upd_rec. bcc04_pp007al ,
         fnd. bcc04_pp007nl = upd_rec. bcc04_pp007nl ,
         fnd. bcc04_pp008al = upd_rec. bcc04_pp008al ,
         fnd. bcc04_pp008nl = upd_rec. bcc04_pp008nl ,
         fnd. bcc04_pp009nl = upd_rec. bcc04_pp009nl ,
         fnd. bcc04_pp013al = upd_rec. bcc04_pp013al ,
         fnd. bcc04_pp014al = upd_rec. bcc04_pp014al ,
         fnd. bcc04_pp017cc = upd_rec. bcc04_pp017cc ,
         fnd. bcc04_pp020al = upd_rec. bcc04_pp020al ,
         fnd. bcc04_pp027al = upd_rec. bcc04_pp027al ,
         fnd. bcc04_pp027cl = upd_rec. bcc04_pp027cl ,
         fnd. bcc04_pp032cl = upd_rec. bcc04_pp032cl ,
         fnd. bcc04_pp033al = upd_rec. bcc04_pp033al ,
         fnd. bcc04_pp033cc = upd_rec. bcc04_pp033cc ,
         fnd. bcc04_pp034al = upd_rec. bcc04_pp034al ,
         fnd. bcc04_pp035cl = upd_rec. bcc04_pp035cl ,
         fnd. bcc04_pp040cc = upd_rec. bcc04_pp040cc ,
         fnd. bcc04_pp044al = upd_rec. bcc04_pp044al ,
         fnd. bcc04_pp044fc = upd_rec. bcc04_pp044fc ,
         fnd. bcc04_pp044nl = upd_rec. bcc04_pp044nl ,
         fnd. bcc04_pp045al = upd_rec. bcc04_pp045al ,
         fnd. bcc04_pp045fl = upd_rec. bcc04_pp045fl ,
         fnd. bcc04_pp046cc = upd_rec. bcc04_pp046cc ,
         fnd. bcc04_pp050al = upd_rec. bcc04_pp050al ,
         fnd. bcc04_pp050cc = upd_rec. bcc04_pp050cc ,
         fnd. bcc04_pp051al = upd_rec. bcc04_pp051al ,
         fnd. bcc04_pp051cc = upd_rec. bcc04_pp051cc ,
         fnd. bcc04_pp052al = upd_rec. bcc04_pp052al ,
         fnd. bcc04_pp053al = upd_rec. bcc04_pp053al ,
         fnd. bcc04_pp058al = upd_rec. bcc04_pp058al ,
         fnd. bcc04_pp058cl = upd_rec. bcc04_pp058cl ,
         fnd. bcc04_pp058nl = upd_rec. bcc04_pp058nl ,
         fnd. bcc04_pp059al = upd_rec. bcc04_pp059al ,
         fnd. bcc04_pp059fc = upd_rec. bcc04_pp059fc ,
         fnd. bcc04_pp060al = upd_rec. bcc04_pp060al ,
         fnd. bcc04_pp060cc = upd_rec. bcc04_pp060cc ,
         fnd. bcc04_pp060cl = upd_rec. bcc04_pp060cl ,
         fnd. bcc04_pp061al = upd_rec. bcc04_pp061al ,
         fnd. bcc04_pp066al = upd_rec. bcc04_pp066al ,
         fnd. bcc04_pp067al = upd_rec. bcc04_pp067al ,
         fnd. bcc04_pp068al = upd_rec. bcc04_pp068al ,
         fnd. bcc04_pp068cl = upd_rec. bcc04_pp068cl ,
         fnd. bcc04_pp069al = upd_rec. bcc04_pp069al ,
         fnd. bcc04_pp069cc = upd_rec. bcc04_pp069cc ,
         fnd. bcc04_pp069nl = upd_rec. bcc04_pp069nl ,
         fnd. bcc04_pp070al = upd_rec. bcc04_pp070al ,
         fnd. bcc04_pp070cc = upd_rec. bcc04_pp070cc ,
         fnd. bcc04_pp070cl = upd_rec. bcc04_pp070cl ,
         fnd. bcc04_pp070fc = upd_rec. bcc04_pp070fc ,
         fnd. bcc04_pp070nl = upd_rec. bcc04_pp070nl ,
         fnd. bcc04_pp071al = upd_rec. bcc04_pp071al ,
         fnd. bcc04_pp072cc = upd_rec. bcc04_pp072cc ,
         fnd. bcc04_pp074al = upd_rec. bcc04_pp074al ,
         fnd. bcc04_pp078cl = upd_rec. bcc04_pp078cl ,
         fnd. bcc04_pp079al = upd_rec. bcc04_pp079al ,
         fnd. bcc04_pp081cl = upd_rec. bcc04_pp081cl ,
         fnd. bcc04_pp082nl = upd_rec. bcc04_pp082nl ,
         fnd. bcc04_pp100cl = upd_rec. bcc04_pp100cl ,
         fnd. bcc04_pp104cl = upd_rec. bcc04_pp104cl ,
         fnd.last_updated_date          = g_date ,
         fnd. my01_curr_mthly_install = upd_rec. my01_curr_mthly_install
            
            
     where  fnd.consumer_no = upd_rec.consumer_no and
        ( 
         nvl(fnd. consumer_no,0) <> upd_rec. consumer_no OR
         nvl(fnd. application_id,0) <> upd_rec. application_id OR
         nvl(fnd. ait_seq_no,0) <> upd_rec. ait_seq_no OR
         nvl(fnd. identification_type,0) <> upd_rec. identification_type OR
         nvl(fnd. identification_number,0) <> upd_rec. identification_number OR
         nvl(fnd. result_status,0) <> upd_rec. result_status OR
         nvl(fnd. bccfilter1,0) <> upd_rec. bccfilter1 OR
         nvl(fnd. bccfilter2,0) <> upd_rec. bccfilter2 OR
         nvl(fnd. bccfilter3,0) <> upd_rec. bccfilter3 OR
         nvl(fnd. bccfilter4,0) <> upd_rec. bccfilter4 OR
         nvl(fnd. empirica_score,0) <> upd_rec. empirica_score OR
         nvl(fnd. empirica_exclusion_code,0) <> upd_rec. empirica_exclusion_code OR
         nvl(fnd. empirica_indicator,0) <> upd_rec. empirica_indicator OR
         nvl(fnd. bcc_indicator,0) <> upd_rec. bcc_indicator OR
         nvl(fnd. bcc_score,0) <> upd_rec. bcc_score OR
         nvl(fnd. outcome,0) <> upd_rec. outcome OR
         nvl(fnd. reason,0) <> upd_rec. reason OR
         nvl(fnd. response_status,0) <> upd_rec. response_status OR
         nvl(fnd. processing_start_date,'01 JAN 1900') <> upd_rec. processing_start_date OR
         nvl(fnd. processing_time_secs,0) <> upd_rec. processing_time_secs OR
         nvl(fnd. unique_ref_guid,0) <> upd_rec. unique_ref_guid OR
         nvl(fnd. mx01_consumer_number,0) <> upd_rec. mx01_consumer_number OR
         nvl(fnd. mx01_tot_active_accts,0) <> upd_rec. mx01_tot_active_accts OR
         nvl(fnd. mx01_tot_closed_accts_24mths,0) <> upd_rec. mx01_tot_closed_accts_24mths OR
         nvl(fnd. mx01_tot_adverse_accts_24mths,0) <> upd_rec. mx01_tot_adverse_accts_24mths OR
         nvl(fnd. mx01_highest_act_mths_24mths,0) <> upd_rec. mx01_highest_act_mths_24mths OR
         nvl(fnd. mx01_no_revolving_accts,0) <> upd_rec. mx01_no_revolving_accts OR
         nvl(fnd. mx01_no_curr_installment_accts,0) <> upd_rec. mx01_no_curr_installment_accts OR
         nvl(fnd. mx01_no_curr_open_accts,0) <> upd_rec. mx01_no_curr_open_accts OR
         nvl(fnd. mx01_curr_balance,0) <> upd_rec. mx01_curr_balance OR
         nvl(fnd. mx01_curr_balance_ind,0) <> upd_rec. mx01_curr_balance_ind OR
         nvl(fnd. mx01_curr_mthly_install,0) <> upd_rec. mx01_curr_mthly_install OR
         nvl(fnd. mx01_curr_mthly_install_balind,0) <> upd_rec. mx01_curr_mthly_install_balind OR
         nvl(fnd. mx01_ca_amount,0) <> upd_rec. mx01_ca_amount OR
         nvl(fnd. mx01_ca_amnt_bal_ind,0) <> upd_rec. mx01_ca_amnt_bal_ind OR
         nvl(fnd. enq_definite_match_count,0) <> upd_rec. enq_definite_match_count OR
         nvl(fnd. enq_possible_match_count,0) <> upd_rec. enq_possible_match_count OR
         nvl(fnd. enq_matched_consumer_no,0) <> upd_rec. enq_matched_consumer_no OR
         nvl(fnd. enq_possible_consumer_no,0) <> upd_rec. enq_possible_consumer_no OR
         nvl(fnd. enq_possible_adverse_ind,0) <> upd_rec. enq_possible_adverse_ind OR
         nvl(fnd. nc04_consumer_number,0) <> upd_rec. nc04_consumer_number OR
         nvl(fnd. nc04_own_enq_1yrback,0) <> upd_rec. nc04_own_enq_1yrback OR
         nvl(fnd. nc04_own_enq_2yrsback,0) <> upd_rec. nc04_own_enq_2yrsback OR
         nvl(fnd. nc04_own_enq_over_2yrsback,0) <> upd_rec. nc04_own_enq_over_2yrsback OR
         nvl(fnd. nc04_other_enq_1yrback,0) <> upd_rec. nc04_other_enq_1yrback OR
         nvl(fnd. nc04_other_enq_2yrsback,0) <> upd_rec. nc04_other_enq_2yrsback OR
         nvl(fnd. nc04_other_enq_over_2yrsback,0) <> upd_rec. nc04_other_enq_over_2yrsback OR
         nvl(fnd. nc04_judgements_1yrback,0) <> upd_rec. nc04_judgements_1yrback OR
         nvl(fnd. nc04_judgements_2yrsback,0) <> upd_rec. nc04_judgements_2yrsback OR
         nvl(fnd. nc04_judgements_over_2yrsback,0) <> upd_rec. nc04_judgements_over_2yrsback OR
         nvl(fnd. nc04_notices_1yrback,0) <> upd_rec. nc04_notices_1yrback OR
         nvl(fnd. nc04_notices_2yrsback,0) <> upd_rec. nc04_notices_2yrsback OR
         nvl(fnd. nc04_notices_over_2yrsback,0) <> upd_rec. nc04_notices_over_2yrsback OR
         nvl(fnd. nc04_defaults1yrback,0) <> upd_rec. nc04_defaults1yrback OR
         nvl(fnd. nc04_defaults_2yrsback,0) <> upd_rec. nc04_defaults_2yrsback OR
         nvl(fnd. nc04_defaults_over_2yrsback,0) <> upd_rec. nc04_defaults_over_2yrsback OR
         nvl(fnd. nc04_pay_profile_1yrback,0) <> upd_rec. nc04_pay_profile_1yrback OR
         nvl(fnd. nc04_pay_profile_2yrsback,0) <> upd_rec. nc04_pay_profile_2yrsback OR
         nvl(fnd. nc04_pay_profile_over_2yrsback,0) <> upd_rec. nc04_pay_profile_over_2yrsback OR
         nvl(fnd. nc04_trce_alerts_1yrback,0) <> upd_rec. nc04_trce_alerts_1yrback OR
         nvl(fnd. nc04_trce_alerts_2yrsback,0) <> upd_rec. nc04_trce_alerts_2yrsback OR
         nvl(fnd. nc04_trce_alerts_over_2yrsback,0) <> upd_rec. nc04_trce_alerts_over_2yrsback OR
         nvl(fnd. subscriber_code,0) <> upd_rec. subscriber_code OR
         nvl(fnd. client_reference,0) <> upd_rec. client_reference OR
         nvl(fnd. branch_no,0) <> upd_rec. branch_no OR
         nvl(fnd. batch_no,0) <> upd_rec. batch_no OR
         nvl(fnd. em07_consumer_no,0) <> upd_rec. em07_consumer_no OR
         nvl(fnd. em07_empirica_score,0) <> upd_rec. em07_empirica_score OR
         nvl(fnd. em07_exclusion_code,0) <> upd_rec. em07_exclusion_code OR
         nvl(fnd. em07_exclusion_description,0) <> upd_rec. em07_exclusion_description OR
         nvl(fnd. em07_reason_code1,0) <> upd_rec. em07_reason_code1 OR
         nvl(fnd. em07_reason_code2,0) <> upd_rec. em07_reason_code2 OR
         nvl(fnd. em07_reason_code3,0) <> upd_rec. em07_reason_code3 OR
         nvl(fnd. em07_reason_code4,0) <> upd_rec. em07_reason_code4 OR
         nvl(fnd. em07_reason_desc_1,0) <> upd_rec. em07_reason_desc_1 OR
         nvl(fnd. em07_reason_desc_2,0) <> upd_rec. em07_reason_desc_2 OR
         nvl(fnd. em07_reason_desc_3,0) <> upd_rec. em07_reason_desc_3 OR
         nvl(fnd. em07_reason_desc_4,0) <> upd_rec. em07_reason_desc_4 OR
         nvl(fnd. em07_expansion_score,0) <> upd_rec. em07_expansion_score OR
         nvl(fnd. em07_expansion_score_desc,0) <> upd_rec. em07_expansion_score_desc OR
         nvl(fnd. em07_empirica_version,0) <> upd_rec. em07_empirica_version OR
         nvl(fnd. mc01_segment_code,0) <> upd_rec. mc01_segment_code OR
         nvl(fnd. mc01_consumer_no,0) <> upd_rec. mc01_consumer_no OR
         nvl(fnd. mc01_curr_yr_enq_client,0) <> upd_rec. mc01_curr_yr_enq_client OR
         nvl(fnd. mc01_curr_yr_enq_oth_sub,0) <> upd_rec. mc01_curr_yr_enq_oth_sub OR
         nvl(fnd. mc01_curr_yr_pos_nlr_loans,0) <> upd_rec. mc01_curr_yr_pos_nlr_loans OR
         nvl(fnd. mc01_curr_yr_hmths_in_arrears,0) <> upd_rec. mc01_curr_yr_hmths_in_arrears OR
         nvl(fnd. mc01_prev_yr_enq_client,0) <> upd_rec. mc01_prev_yr_enq_client OR
         nvl(fnd. mc01_prev_yr_enq_oth_sub,0) <> upd_rec. mc01_prev_yr_enq_oth_sub OR
         nvl(fnd. mc01_prev_yr_pos_nlr_loans,0) <> upd_rec. mc01_prev_yr_pos_nlr_loans OR
         nvl(fnd. mc01_prev_yr_hmnths_inarrears,0) <> upd_rec. mc01_prev_yr_hmnths_inarrears OR
         nvl(fnd. mc01_cumulative_instal_value,0) <> upd_rec. mc01_cumulative_instal_value OR
         nvl(fnd. mc01_cumulative_out_balance,0) <> upd_rec. mc01_cumulative_out_balance OR
         nvl(fnd. mc01_wrst_mnth_in_arrears,0) <> upd_rec. mc01_wrst_mnth_in_arrears OR
         nvl(fnd. sbc_consumer_no,0) <> upd_rec. sbc_consumer_no OR
         nvl(fnd. gr01_suburb_code,0) <> upd_rec. gr01_suburb_code OR
         nvl(fnd. gr01_data_date,0) <> upd_rec. gr01_data_date OR
         nvl(fnd. gr01_load_date,0) <> upd_rec. gr01_load_date OR
         nvl(fnd. gr01_index_a,0) <> upd_rec. gr01_index_a OR
         nvl(fnd. gr01_index_b,0) <> upd_rec. gr01_index_b OR
         nvl(fnd. gr01_index_c,0) <> upd_rec. gr01_index_c OR
         nvl(fnd. gr01_index_d,0) <> upd_rec. gr01_index_d OR
         nvl(fnd. gr01_index_e,0) <> upd_rec. gr01_index_e OR
         nvl(fnd. gr01_index_f,0) <> upd_rec. gr01_index_f OR
         nvl(fnd. gr01_index_g,0) <> upd_rec. gr01_index_g OR
         nvl(fnd. gr01_index_h,0) <> upd_rec. gr01_index_h OR
         nvl(fnd. gr01_index_i,0) <> upd_rec. gr01_index_i OR
         nvl(fnd. gr01_index_j,0) <> upd_rec. gr01_index_j OR
         nvl(fnd. gr01_index_k,0) <> upd_rec. gr01_index_k OR
         nvl(fnd. gr01_index_l,0) <> upd_rec. gr01_index_l OR
         nvl(fnd. gr01_index_m,0) <> upd_rec. gr01_index_m OR
         nvl(fnd. gr01_index_n,0) <> upd_rec. gr01_index_n OR
         nvl(fnd. gr01_index_o,0) <> upd_rec. gr01_index_o OR
         nvl(fnd. gr01_index_p,0) <> upd_rec. gr01_index_p OR
         nvl(fnd. bcc04_dm001al,0) <> upd_rec. bcc04_dm001al OR
         nvl(fnd. bcc04_dm002al,0) <> upd_rec. bcc04_dm002al OR
         nvl(fnd. bcc04_dm003al,0) <> upd_rec. bcc04_dm003al OR
         nvl(fnd. bcc04_dm004al,0) <> upd_rec. bcc04_dm004al OR
         nvl(fnd. bcc04_dm005al,0) <> upd_rec. bcc04_dm005al OR
         nvl(fnd. bcc04_dm006al,0) <> upd_rec. bcc04_dm006al OR
         nvl(fnd. bcc04_dm007al,0) <> upd_rec. bcc04_dm007al OR
         nvl(fnd. bcc04_eq001al,0) <> upd_rec. bcc04_eq001al OR
         nvl(fnd. bcc04_eq001cl,0) <> upd_rec. bcc04_eq001cl OR
         nvl(fnd. bcc04_eq001fn,0) <> upd_rec. bcc04_eq001fn OR
         nvl(fnd. bcc04_eq002al,0) <> upd_rec. bcc04_eq002al OR
         nvl(fnd. bcc04_eq002cl,0) <> upd_rec. bcc04_eq002cl OR
         nvl(fnd. bcc04_eq002fc,0) <> upd_rec. bcc04_eq002fc OR
         nvl(fnd. bcc04_eq002nl,0) <> upd_rec. bcc04_eq002nl OR
         nvl(fnd. bcc04_eq003cl,0) <> upd_rec. bcc04_eq003cl OR
         nvl(fnd. bcc04_eq003fn,0) <> upd_rec. bcc04_eq003fn OR
         nvl(fnd. bcc04_eq004al,0) <> upd_rec. bcc04_eq004al OR
         nvl(fnd. bcc04_eq004cl,0) <> upd_rec. bcc04_eq004cl OR
         nvl(fnd. bcc04_eq004fc,0) <> upd_rec. bcc04_eq004fc OR
         nvl(fnd. bcc04_eq004nl,0) <> upd_rec. bcc04_eq004nl OR
         nvl(fnd. bcc04_eq007al,0) <> upd_rec. bcc04_eq007al OR
         nvl(fnd. bcc04_eq008al,0) <> upd_rec. bcc04_eq008al OR
         nvl(fnd. bcc04_eq008cl,0) <> upd_rec. bcc04_eq008cl OR
         nvl(fnd. bcc04_eq008fc,0) <> upd_rec. bcc04_eq008fc OR
         nvl(fnd. bcc04_ng001al,0) <> upd_rec. bcc04_ng001al OR
         nvl(fnd. bcc04_ng004al,0) <> upd_rec. bcc04_ng004al OR
         nvl(fnd. bcc04_ng008al,0) <> upd_rec. bcc04_ng008al OR
         nvl(fnd. bcc04_ng011al,0) <> upd_rec. bcc04_ng011al OR
         nvl(fnd. bcc04_ng022al,0) <> upd_rec. bcc04_ng022al OR
         nvl(fnd. bcc04_ng034al,0) <> upd_rec. bcc04_ng034al OR
         nvl(fnd. bcc04_pp001al,0) <> upd_rec. bcc04_pp001al OR
         nvl(fnd. bcc04_pp001cc,0) <> upd_rec. bcc04_pp001cc OR
         nvl(fnd. bcc04_pp001cl,0) <> upd_rec. bcc04_pp001cl OR
         nvl(fnd. bcc04_pp001fc,0) <> upd_rec. bcc04_pp001fc OR
         nvl(fnd. bcc04_pp001fl,0) <> upd_rec. bcc04_pp001fl OR
         nvl(fnd. bcc04_pp001nl,0) <> upd_rec. bcc04_pp001nl OR
         nvl(fnd. bcc04_pp001pl,0) <> upd_rec. bcc04_pp001pl OR
         nvl(fnd. bcc04_pp002al,0) <> upd_rec. bcc04_pp002al OR
         nvl(fnd. bcc04_pp002fc,0) <> upd_rec. bcc04_pp002fc OR
         nvl(fnd. bcc04_pp003al,0) <> upd_rec. bcc04_pp003al OR
         nvl(fnd. bcc04_pp005al,0) <> upd_rec. bcc04_pp005al OR
         nvl(fnd. bcc04_pp005cc,0) <> upd_rec. bcc04_pp005cc OR
         nvl(fnd. bcc04_pp005fl,0) <> upd_rec. bcc04_pp005fl OR
         nvl(fnd. bcc04_pp005pl,0) <> upd_rec. bcc04_pp005pl OR
         nvl(fnd. bcc04_pp006al,0) <> upd_rec. bcc04_pp006al OR
         nvl(fnd. bcc04_pp007al,0) <> upd_rec. bcc04_pp007al OR
         nvl(fnd. bcc04_pp007nl,0) <> upd_rec. bcc04_pp007nl OR
         nvl(fnd. bcc04_pp008al,0) <> upd_rec. bcc04_pp008al OR
         nvl(fnd. bcc04_pp008nl,0) <> upd_rec. bcc04_pp008nl OR
         nvl(fnd. bcc04_pp009nl,0) <> upd_rec. bcc04_pp009nl OR
         nvl(fnd. bcc04_pp013al,0) <> upd_rec. bcc04_pp013al OR
         nvl(fnd. bcc04_pp014al,0) <> upd_rec. bcc04_pp014al OR
         nvl(fnd. bcc04_pp017cc,0) <> upd_rec. bcc04_pp017cc OR
         nvl(fnd. bcc04_pp020al,0) <> upd_rec. bcc04_pp020al OR
         nvl(fnd. bcc04_pp027al,0) <> upd_rec. bcc04_pp027al OR
         nvl(fnd. bcc04_pp027cl,0) <> upd_rec. bcc04_pp027cl OR
         nvl(fnd. bcc04_pp032cl,0) <> upd_rec. bcc04_pp032cl OR
         nvl(fnd. bcc04_pp033al,0) <> upd_rec. bcc04_pp033al OR
         nvl(fnd. bcc04_pp033cc,0) <> upd_rec. bcc04_pp033cc OR
         nvl(fnd. bcc04_pp034al,0) <> upd_rec. bcc04_pp034al OR
         nvl(fnd. bcc04_pp035cl,0) <> upd_rec. bcc04_pp035cl OR
         nvl(fnd. bcc04_pp040cc,0) <> upd_rec. bcc04_pp040cc OR
         nvl(fnd. bcc04_pp044al,0) <> upd_rec. bcc04_pp044al OR
         nvl(fnd. bcc04_pp044fc,0) <> upd_rec. bcc04_pp044fc OR
         nvl(fnd. bcc04_pp044nl,0) <> upd_rec. bcc04_pp044nl OR
         nvl(fnd. bcc04_pp045al,0) <> upd_rec. bcc04_pp045al OR
         nvl(fnd. bcc04_pp045fl,0) <> upd_rec. bcc04_pp045fl OR
         nvl(fnd. bcc04_pp046cc,0) <> upd_rec. bcc04_pp046cc OR
         nvl(fnd. bcc04_pp050al,0) <> upd_rec. bcc04_pp050al OR
         nvl(fnd. bcc04_pp050cc,0) <> upd_rec. bcc04_pp050cc OR
         nvl(fnd. bcc04_pp051al,0) <> upd_rec. bcc04_pp051al OR
         nvl(fnd. bcc04_pp051cc,0) <> upd_rec. bcc04_pp051cc OR
         nvl(fnd. bcc04_pp052al,0) <> upd_rec. bcc04_pp052al OR
         nvl(fnd. bcc04_pp053al,0) <> upd_rec. bcc04_pp053al OR
         nvl(fnd. bcc04_pp058al,0) <> upd_rec. bcc04_pp058al OR
         nvl(fnd. bcc04_pp058cl,0) <> upd_rec. bcc04_pp058cl OR
         nvl(fnd. bcc04_pp058nl,0) <> upd_rec. bcc04_pp058nl OR
         nvl(fnd. bcc04_pp059al,0) <> upd_rec. bcc04_pp059al OR
         nvl(fnd. bcc04_pp059fc,0) <> upd_rec. bcc04_pp059fc OR
         nvl(fnd. bcc04_pp060al,0) <> upd_rec. bcc04_pp060al OR
         nvl(fnd. bcc04_pp060cc,0) <> upd_rec. bcc04_pp060cc OR
         nvl(fnd. bcc04_pp060cl,0) <> upd_rec. bcc04_pp060cl OR
         nvl(fnd. bcc04_pp061al,0) <> upd_rec. bcc04_pp061al OR
         nvl(fnd. bcc04_pp066al,0) <> upd_rec. bcc04_pp066al OR
         nvl(fnd. bcc04_pp067al,0) <> upd_rec. bcc04_pp067al OR
         nvl(fnd. bcc04_pp068al,0) <> upd_rec. bcc04_pp068al OR
         nvl(fnd. bcc04_pp068cl,0) <> upd_rec. bcc04_pp068cl OR
         nvl(fnd. bcc04_pp069al,0) <> upd_rec. bcc04_pp069al OR
         nvl(fnd. bcc04_pp069cc,0) <> upd_rec. bcc04_pp069cc OR
         nvl(fnd. bcc04_pp069nl,0) <> upd_rec. bcc04_pp069nl OR
         nvl(fnd. bcc04_pp070al,0) <> upd_rec. bcc04_pp070al OR
         nvl(fnd. bcc04_pp070cc,0) <> upd_rec. bcc04_pp070cc OR
         nvl(fnd. bcc04_pp070cl,0) <> upd_rec. bcc04_pp070cl OR
         nvl(fnd. bcc04_pp070fc,0) <> upd_rec. bcc04_pp070fc OR
         nvl(fnd. bcc04_pp070nl,0) <> upd_rec. bcc04_pp070nl OR
         nvl(fnd. bcc04_pp071al,0) <> upd_rec. bcc04_pp071al OR
         nvl(fnd. bcc04_pp072cc,0) <> upd_rec. bcc04_pp072cc OR
         nvl(fnd. bcc04_pp074al,0) <> upd_rec. bcc04_pp074al OR
         nvl(fnd. bcc04_pp078cl,0) <> upd_rec. bcc04_pp078cl OR
         nvl(fnd. bcc04_pp079al,0) <> upd_rec. bcc04_pp079al OR
         nvl(fnd. bcc04_pp081cl,0) <> upd_rec. bcc04_pp081cl OR
         nvl(fnd. bcc04_pp082nl,0) <> upd_rec. bcc04_pp082nl OR
         nvl(fnd. bcc04_pp100cl,0) <> upd_rec. bcc04_pp100cl OR
         nvl(fnd. bcc04_pp104cl,0) <> upd_rec. bcc04_pp104cl OR
         nvl(fnd. my01_curr_mthly_install, 0) <> upd_rec. my01_curr_mthly_install


         );         


   if sql%rowcount = 0 then
        g_recs_nochange:= g_recs_nochange + 1;
   else
        g_recs_updated := g_recs_updated + 1;  
   end if;

   end loop;

      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := substr('FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := substr('FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;

   
--************************************************************************************************** 
-- Send records to hospital where not valid
--**************************************************************************************************

procedure flagged_records_hospital as
begin
     
      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_om4_cr_scores_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with CONSUMER_NO' ,

         cpy. consumer_no ,
         cpy. application_id ,
         cpy. ait_seq_no ,
         cpy. identification_type ,
         cpy. identification_number ,
         cpy. result_status ,
         cpy. bccfilter1 ,
         cpy. bccfilter2 ,
         cpy. bccfilter3 ,
         cpy. bccfilter4 ,
         cpy. empirica_score ,
         cpy. empirica_exclusion_code ,
         cpy. empirica_indicator ,
         cpy. bcc_indicator ,
         cpy. bcc_score ,
         cpy. outcome ,
         cpy. reason ,
         cpy. response_status ,
         cpy. processing_start_date ,
         cpy. processing_time_secs ,
         cpy. unique_ref_guid ,
         cpy. mx01_consumer_number ,
         cpy. mx01_tot_active_accts ,
         cpy. mx01_tot_closed_accts_24mths ,
         cpy. mx01_tot_adverse_accts_24mths ,
         cpy. mx01_highest_act_mths_24mths ,
         cpy. mx01_no_revolving_accts ,
         cpy. mx01_no_curr_installment_accts ,
         cpy. mx01_no_curr_open_accts ,
         cpy. mx01_curr_balance ,
         cpy. mx01_curr_balance_ind ,
         cpy. mx01_curr_mthly_install ,
         cpy. mx01_curr_mthly_install_balind ,
         cpy. mx01_ca_amount ,
         cpy. mx01_ca_amnt_bal_ind ,
         cpy. enq_definite_match_count ,
         cpy. enq_possible_match_count ,
         cpy. enq_matched_consumer_no ,
         cpy. enq_possible_consumer_no ,
         cpy. enq_possible_adverse_ind ,
         cpy. nc04_consumer_number ,
         cpy. nc04_own_enq_1yrback ,
         cpy. nc04_own_enq_2yrsback ,
         cpy. nc04_own_enq_over_2yrsback ,
         cpy. nc04_other_enq_1yrback ,
         cpy. nc04_other_enq_2yrsback ,
         cpy. nc04_other_enq_over_2yrsback ,
         cpy. nc04_judgements_1yrback ,
         cpy. nc04_judgements_2yrsback ,
         cpy. nc04_judgements_over_2yrsback ,
         cpy. nc04_notices_1yrback ,
         cpy. nc04_notices_2yrsback ,
         cpy. nc04_notices_over_2yrsback ,
         cpy. nc04_defaults1yrback ,
         cpy. nc04_defaults_2yrsback ,
         cpy. nc04_defaults_over_2yrsback ,
         cpy. nc04_pay_profile_1yrback ,
         cpy. nc04_pay_profile_2yrsback ,
         cpy. nc04_pay_profile_over_2yrsback ,
         cpy. nc04_trce_alerts_1yrback ,
         cpy. nc04_trce_alerts_2yrsback ,
         cpy. nc04_trce_alerts_over_2yrsback ,
         cpy. subscriber_code ,
         cpy. client_reference ,
         cpy. branch_no ,
         cpy. batch_no ,
         cpy. em07_consumer_no ,
         cpy. em07_empirica_score ,
         cpy. em07_exclusion_code ,
         cpy. em07_exclusion_description ,
         cpy. em07_reason_code1 ,
         cpy. em07_reason_code2 ,
         cpy. em07_reason_code3 ,
         cpy. em07_reason_code4 ,
         cpy. em07_reason_desc_1 ,
         cpy. em07_reason_desc_2 ,
         cpy. em07_reason_desc_3 ,
         cpy. em07_reason_desc_4 ,
         cpy. em07_expansion_score ,
         cpy. em07_expansion_score_desc ,
         cpy. em07_empirica_version ,
         cpy. mc01_segment_code ,
         cpy. mc01_consumer_no ,
         cpy. mc01_curr_yr_enq_client ,
         cpy. mc01_curr_yr_enq_oth_sub ,
         cpy. mc01_curr_yr_pos_nlr_loans ,
         cpy. mc01_curr_yr_hmths_in_arrears ,
         cpy. mc01_prev_yr_enq_client ,
         cpy. mc01_prev_yr_enq_oth_sub ,
         cpy. mc01_prev_yr_pos_nlr_loans ,
         cpy. mc01_prev_yr_hmnths_inarrears ,
         cpy. mc01_cumulative_instal_value ,
         cpy. mc01_cumulative_out_balance ,
         cpy. mc01_wrst_mnth_in_arrears ,
         cpy. sbc_consumer_no ,
         cpy. gr01_suburb_code ,
         cpy. gr01_data_date ,
         cpy. gr01_load_date ,
         cpy. gr01_index_a ,
         cpy. gr01_index_b ,
         cpy. gr01_index_c ,
         cpy. gr01_index_d ,
         cpy. gr01_index_e ,
         cpy. gr01_index_f ,
         cpy. gr01_index_g ,
         cpy. gr01_index_h ,
         cpy. gr01_index_i ,
         cpy. gr01_index_j ,
         cpy. gr01_index_k ,
         cpy. gr01_index_l ,
         cpy. gr01_index_m ,
         cpy. gr01_index_n ,
         cpy. gr01_index_o ,
         cpy. gr01_index_p ,
         cpy. bcc04_dm001al ,
         cpy. bcc04_dm002al ,
         cpy. bcc04_dm003al ,
         cpy. bcc04_dm004al ,
         cpy. bcc04_dm005al ,
         cpy. bcc04_dm006al ,
         cpy. bcc04_dm007al ,
         cpy. bcc04_eq001al ,
         cpy. bcc04_eq001cl ,
         cpy. bcc04_eq001fn ,
         cpy. bcc04_eq002al ,
         cpy. bcc04_eq002cl ,
         cpy. bcc04_eq002fc ,
         cpy. bcc04_eq002nl ,
         cpy. bcc04_eq003cl ,
         cpy. bcc04_eq003fn ,
         cpy. bcc04_eq004al ,
         cpy. bcc04_eq004cl ,
         cpy. bcc04_eq004fc ,
         cpy. bcc04_eq004nl ,
         cpy. bcc04_eq007al ,
         cpy. bcc04_eq008al ,
         cpy. bcc04_eq008cl ,
         cpy. bcc04_eq008fc ,
         cpy. bcc04_ng001al ,
         cpy. bcc04_ng004al ,
         cpy. bcc04_ng008al ,
         cpy. bcc04_ng011al ,
         cpy. bcc04_ng022al ,
         cpy. bcc04_ng034al ,
         cpy. bcc04_pp001al ,
         cpy. bcc04_pp001cc ,
         cpy. bcc04_pp001cl ,
         cpy. bcc04_pp001fc ,
         cpy. bcc04_pp001fl ,
         cpy. bcc04_pp001nl ,
         cpy. bcc04_pp001pl ,
         cpy. bcc04_pp002al ,
         cpy. bcc04_pp002fc ,
         cpy. bcc04_pp003al ,
         cpy. bcc04_pp005al ,
         cpy. bcc04_pp005cc ,
         cpy. bcc04_pp005fl ,
         cpy. bcc04_pp005pl ,
         cpy. bcc04_pp006al ,
         cpy. bcc04_pp007al ,
         cpy. bcc04_pp007nl ,
         cpy. bcc04_pp008al ,
         cpy. bcc04_pp008nl ,
         cpy. bcc04_pp009nl ,
         cpy. bcc04_pp013al ,
         cpy. bcc04_pp014al ,
         cpy. bcc04_pp017cc ,
         cpy. bcc04_pp020al ,
         cpy. bcc04_pp027al ,
         cpy. bcc04_pp027cl ,
         cpy. bcc04_pp032cl ,
         cpy. bcc04_pp033al ,
         cpy. bcc04_pp033cc ,
         cpy. bcc04_pp034al ,
         cpy. bcc04_pp035cl ,
         cpy. bcc04_pp040cc ,
         cpy. bcc04_pp044al ,
         cpy. bcc04_pp044fc ,
         cpy. bcc04_pp044nl ,
         cpy. bcc04_pp045al ,
         cpy. bcc04_pp045fl ,
         cpy. bcc04_pp046cc ,
         cpy. bcc04_pp050al ,
         cpy. bcc04_pp050cc ,
         cpy. bcc04_pp051al ,
         cpy. bcc04_pp051cc ,
         cpy. bcc04_pp052al ,
         cpy. bcc04_pp053al ,
         cpy. bcc04_pp058al ,
         cpy. bcc04_pp058cl ,
         cpy. bcc04_pp058nl ,
         cpy. bcc04_pp059al ,
         cpy. bcc04_pp059fc ,
         cpy. bcc04_pp060al ,
         cpy. bcc04_pp060cc ,
         cpy. bcc04_pp060cl ,
         cpy. bcc04_pp061al ,
         cpy. bcc04_pp066al ,
         cpy. bcc04_pp067al ,
         cpy. bcc04_pp068al ,
         cpy. bcc04_pp068cl ,
         cpy. bcc04_pp069al ,
         cpy. bcc04_pp069cc ,
         cpy. bcc04_pp069nl ,
         cpy. bcc04_pp070al ,
         cpy. bcc04_pp070cc ,
         cpy. bcc04_pp070cl ,
         cpy. bcc04_pp070fc ,
         cpy. bcc04_pp070nl ,
         cpy. bcc04_pp071al ,
         cpy. bcc04_pp072cc ,
         cpy. bcc04_pp074al ,
         cpy. bcc04_pp078cl ,
         cpy. bcc04_pp079al ,
         cpy. bcc04_pp081cl ,
         cpy. bcc04_pp082nl ,
         cpy. bcc04_pp100cl ,
         cpy. bcc04_pp104cl ,
         cpy. my01_curr_mthly_install


              
      from   dwh_wfs_foundation.stg_om4_cr_scores_cpy cpy
       left outer join dwh_wfs_foundation.fnd_wfs_om4_application dep on dep.matched_consumer_no = cpy.consumer_no

      where 
         dep.matched_consumer_no  is null
      
      
--      ) and 

-- Any further validation goes in here - like or xxx.ind not in (0,1) ---    
    
        and sys_process_code = 'N';
         

      g_recs_hospital := g_recs_hospital + sql%rowcount;
      
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := substr('FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := substr('FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm,1,200);
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
    from   dwh_wfs_foundation.stg_om4_cr_scores_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_...._cpy
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
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
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
       l_message := substr(dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := substr(dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;

--end WH_FND_WFS_168U;
end wh_fnd_wfs_168u_20181117;
