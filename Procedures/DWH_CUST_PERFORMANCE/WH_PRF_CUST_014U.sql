--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_014U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_014U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create CUSTOMER MASTER fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_wfs_all_prod
--               Output - dim_wfs_all_prod
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_014U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS ALL PROD EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_wfs_all_prod is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_wfs_all_prod fnd,
              dim_wfs_all_prod prf
      where   fnd.wfs_account_no        = prf.wfs_account_no  and
              fnd.wfs_customer_no       = prf.wfs_customer_no and
              fnd.product_code_no       = prf.product_code_no 
--              fnd.last_updated_date     = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.wfs_account_no,fnd.wfs_customer_no,fnd.product_code_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,2) */ into dim_wfs_all_prod prf
       select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.	*
       from  fnd_wfs_all_prod fnd
       where 
       --fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_wfs_all_prod
       where  wfs_customer_no    = fnd.wfs_customer_no and
              wfs_account_no     = fnd.wfs_account_no and
              product_code_no    = fnd.product_code_no
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



FOR upd_rec IN c_fnd_wfs_all_prod
   loop
     update   dim_wfs_all_prod prf
     set      identity_no                     = upd_rec.identity_no,
              account_status                  = upd_rec.account_status,
              application_no                  = upd_rec.application_no,
              application_score               = upd_rec.application_score,
              behaviour_score01               = upd_rec.behaviour_score01,
              behaviour_score02               = upd_rec.behaviour_score02,
              behaviour_score03               = upd_rec.behaviour_score03,
              behaviour_score04               = upd_rec.behaviour_score04,
              behaviour_score05               = upd_rec.behaviour_score05,
              behaviour_score06               = upd_rec.behaviour_score06,
              behaviour_score07               = upd_rec.behaviour_score07,
              behaviour_score08               = upd_rec.behaviour_score08,
              behaviour_score09               = upd_rec.behaviour_score09,
              behaviour_score10               = upd_rec.behaviour_score10,
              behaviour_score11               = upd_rec.behaviour_score11,
              behaviour_score12               = upd_rec.behaviour_score12,
              propensty_score01               = upd_rec.propensty_score01,
              propensty_score02               = upd_rec.propensty_score02,
              propensty_score03               = upd_rec.propensty_score03,
              propensty_score04               = upd_rec.propensty_score04,
              propensty_score05               = upd_rec.propensty_score05,
              propensty_score06               = upd_rec.propensty_score06,
              propensty_score07               = upd_rec.propensty_score07,
              propensty_score08               = upd_rec.propensty_score08,
              propensty_score09               = upd_rec.propensty_score09,
              propensty_score10               = upd_rec.propensty_score10,
              propensty_score11               = upd_rec.propensty_score11,
              propensty_score12               = upd_rec.propensty_score12,
              attrition_score01               = upd_rec.attrition_score01,
              attrition_score02               = upd_rec.attrition_score02,
              attrition_score03               = upd_rec.attrition_score03,
              attrition_score04               = upd_rec.attrition_score04,
              attrition_score05               = upd_rec.attrition_score05,
              attrition_score06               = upd_rec.attrition_score06,
              attrition_score07               = upd_rec.attrition_score07,
              attrition_score08               = upd_rec.attrition_score08,
              attrition_score09               = upd_rec.attrition_score09,
              attrition_score10               = upd_rec.attrition_score10,
              attrition_score11               = upd_rec.attrition_score11,
              attrition_score12               = upd_rec.attrition_score12,
              date_opened                     = upd_rec.date_opened,
              date_last_pchs                  = upd_rec.date_last_pchs,
              credit_limit                    = upd_rec.credit_limit,
              current_balance                 = upd_rec.current_balance,
              open_to_buy                     = upd_rec.open_to_buy,
              last_pchs_val                   = upd_rec.last_pchs_val,
              pchs_val_ytd                    = upd_rec.pchs_val_ytd,
              pchs_val_ltd                    = upd_rec.pchs_val_ltd,
              store_of_pref1                  = upd_rec.store_of_pref1,
              store_of_pref2                  = upd_rec.store_of_pref2,
              store_of_pref3                  = upd_rec.store_of_pref3,
              block_code1                     = upd_rec.block_code1,
              block_code2                     = upd_rec.block_code2,
              no_of_cards                     = upd_rec.no_of_cards,
              date_last_updated               = upd_rec.date_last_updated,
              chgoff_val                      = upd_rec.chgoff_val,
              chgoff_rsn1                     = upd_rec.chgoff_rsn1,
              chgoff_rsn2                     = upd_rec.chgoff_rsn2,
              chgoff_status                   = upd_rec.chgoff_status,
              times_in_coll                   = upd_rec.times_in_coll,
              date_application                = upd_rec.date_application,
              date_chgoff                     = upd_rec.date_chgoff,
              date_closed                     = upd_rec.date_closed,
              date_highest_bal                = upd_rec.date_highest_bal,
              date_last_activity              = upd_rec.date_last_activity,
              date_last_age                   = upd_rec.date_last_age,
              date_last_crlm                  = upd_rec.date_last_crlm,
              date_last_pymt                  = upd_rec.date_last_pymt,
              date_last_rate_chg              = upd_rec.date_last_rate_chg,
              date_last_reage                 = upd_rec.date_last_reage,
              date_last_reclass               = upd_rec.date_last_reclass,
              date_last_return                = upd_rec.date_last_return,
              date_last_revpmt                = upd_rec.date_last_revpmt,
              date_last_rtnchq                = upd_rec.date_last_rtnchq,
              date_last_stmt                  = upd_rec.date_last_stmt,
              days_till_chgoff                = upd_rec.days_till_chgoff,
              highest_bal_val                 = upd_rec.highest_bal_val,
              prev_credit_class               = upd_rec.prev_credit_class,
              prev_credit_limit               = upd_rec.prev_credit_limit,
              prev_int_val_ytd                = upd_rec.prev_int_val_ytd,
              prev_int_pd_ytd                 = upd_rec.prev_int_pd_ytd,
              market_flag_01                  = upd_rec.market_flag_01,
              market_flag_02                  = upd_rec.market_flag_02,
              market_flag_03                  = upd_rec.market_flag_03,
              market_flag_04                  = upd_rec.market_flag_04,
              market_flag_05                  = upd_rec.market_flag_05,
              market_flag_06                  = upd_rec.market_flag_06,
              market_flag_07                  = upd_rec.market_flag_07,
              market_flag_08                  = upd_rec.market_flag_08,
              market_flag_09                  = upd_rec.market_flag_09,
              market_flag_10                  = upd_rec.market_flag_10,
              market_flag_11                  = upd_rec.market_flag_11,
              market_flag_12                  = upd_rec.market_flag_12,
              market_flag_13                  = upd_rec.market_flag_13,
              market_flag_14                  = upd_rec.market_flag_14,
              market_flag_15                  = upd_rec.market_flag_15,
              market_flag_16                  = upd_rec.market_flag_16,
              market_flag_17                  = upd_rec.market_flag_17,
              market_flag_18                  = upd_rec.market_flag_18,
              market_flag_19                  = upd_rec.market_flag_19,
              market_flag_20                  = upd_rec.market_flag_20,
              last_pymt_val                   = upd_rec.last_pymt_val,
              promotion_code1                 = upd_rec.promotion_code1,
              promotion_code2                 = upd_rec.promotion_code2,
              promotion_code3                 = upd_rec.promotion_code3,
              promotion_code4                 = upd_rec.promotion_code4,
              promotion_status1               = upd_rec.promotion_status1,
              promotion_status2               = upd_rec.promotion_status2,
              promotion_status3               = upd_rec.promotion_status3,
              promotion_status4               = upd_rec.promotion_status4,
              retail_plan_code                = upd_rec.retail_plan_code,
              statmt_flag                     = upd_rec.statmt_flag,
              statmt_msg_no_1                 = upd_rec.statmt_msg_no_1,
              statmt_msg_no_2                 = upd_rec.statmt_msg_no_2,
              write_off_days                  = upd_rec.write_off_days,
              ins_incentv_store               = upd_rec.ins_incentv_store,
              ins_cancel_date                 = upd_rec.ins_cancel_date,
              ins_dt_lst_billed               = upd_rec.ins_dt_lst_billed,
              ins_dt_lst_claim                = upd_rec.ins_dt_lst_claim,
              ins_effectv_date                = upd_rec.ins_effectv_date,
              ins_enrllmnt_state              = upd_rec.ins_enrllmnt_state,
              ins_last_premium                = upd_rec.ins_last_premium,
              ins_premium_mtd                 = upd_rec.ins_premium_mtd,
              ins_premium                     = upd_rec.ins_premium,
              ins_premium_state               = upd_rec.ins_premium_state,
              ins_product                     = upd_rec.ins_product,
              ins_rsn_cancelled               = upd_rec.ins_rsn_cancelled,
              ins_reinstmt_date               = upd_rec.ins_reinstmt_date,
              ins_status                      = upd_rec.ins_status,
              plan_pmt_ovrd_flag              = upd_rec.plan_pmt_ovrd_flag,
              mktg_promo                      = upd_rec.mktg_promo,
              no_of_store_pref                = upd_rec.no_of_store_pref,
              return_mail_cnt                 = upd_rec.return_mail_cnt,
              loan_drawdown_val               = upd_rec.loan_drawdown_val,
              loan_instalment                 = upd_rec.loan_instalment,
              loan_repay_period               = upd_rec.loan_repay_period,
              loan_tracker                    = upd_rec.loan_tracker,
              sds_ref                         = upd_rec.sds_ref,
              test_digit                      = upd_rec.test_digit,
              test_digit_grp                  = upd_rec.test_digit_grp,
              debit_order_flag                = upd_rec.debit_order_flag,
              debit_order_dy                  = upd_rec.debit_order_dy,
              debit_order_due                 = upd_rec.debit_order_due,
              dtlst_accstat_chg               = upd_rec.dtlst_accstat_chg,
              lcp_ind                         = upd_rec.lcp_ind,
              companion_care_ind              = upd_rec.companion_care_ind,
              accident_benft_ind              = upd_rec.accident_benft_ind,
              cbp_ind                         = upd_rec.cbp_ind,
              lbp_ind                         = upd_rec.lbp_ind,
              ptp_status                      = upd_rec.ptp_status,
              date_cred_limit                 = upd_rec.date_cred_limit,
              comp_care_lst_prem              = upd_rec.comp_care_lst_prem,
              comp_care_eff_date              = upd_rec.comp_care_eff_date,
              acc_benft_lst_prem              = upd_rec.acc_benft_lst_prem,
              acc_benft_eff_date              = upd_rec.acc_benft_eff_date,
              overdue_amt                     = upd_rec.overdue_amt,
              min_payment                     = upd_rec.min_payment,
              payment_date                    = upd_rec.payment_date,
              account_contact_id              = upd_rec.account_contact_id,
              ttd_ind                         = upd_rec.ttd_ind,
              bureau_score                    = upd_rec.bureau_score,
              viking_code                     = upd_rec.viking_code,
              viking_date                     = upd_rec.viking_date,
              viking_amt                      = upd_rec.viking_amt,
              debit_order_proj_amt            = upd_rec.debit_order_proj_amt,
              debit_order_br_cd               = upd_rec.debit_order_br_cd,
              debit_order_exp_dt              = upd_rec.debit_order_exp_dt,
              debit_order_acc_type            = upd_rec.debit_order_acc_type,
              debit_order_acc_no              = upd_rec.debit_order_acc_no,
              debit_order_pymt_ind            = upd_rec.debit_order_pymt_ind,
              dd_status                       = upd_rec.dd_status,
              clim_review                     = upd_rec.clim_review,
              dd_load_amt                     = upd_rec.dd_load_amt,
              date_first_purch                = upd_rec.date_first_purch,
              insurance_active_ind            = upd_rec.insurance_active_ind,
              loan_restruct_ind               = upd_rec.loan_restruct_ind,
              loan_restruct_date              = upd_rec.loan_restruct_date,
              residence_id                    = upd_rec.residence_id,
              debit_order_reversal_count      = upd_rec.debit_order_reversal_count,
              debit_order_interim_pmt         = upd_rec.debit_order_interim_pmt,
              debit_order_remitt_method       = upd_rec.debit_order_remitt_method,
              staff_company_code              = upd_rec.staff_company_code,
              write_off_ind                   = upd_rec.write_off_ind,
              write_off_date                  = upd_rec.write_off_date,
              write_off_value                 = upd_rec.write_off_value,
              initiation_fee                  = upd_rec.initiation_fee,
              monthly_service_fee             = upd_rec.monthly_service_fee,
              initial_interest_rate           = upd_rec.initial_interest_rate,
              delivery_method                 = upd_rec.delivery_method,
              delivery_address                = upd_rec.delivery_address,
              LEGAL_STATUS	                  =	upd_rec.	LEGAL_STATUS	,
              LEGAL_STATUS_DATE	              =	upd_rec.	LEGAL_STATUS_DATE	,
              FIRST_PLACEMENT_INDICATOR	      =	upd_rec.	FIRST_PLACEMENT_INDICATOR	,
              FIRST_PLACEMENT_DATE	          =	upd_rec.	FIRST_PLACEMENT_DATE	,
              SECOND_PLACEMENT_INDICATOR	    =	upd_rec.	SECOND_PLACEMENT_INDICATOR	,
              SECOND_PLACEMENT_DATE         	=	upd_rec.	SECOND_PLACEMENT_DATE	,
              THIRD_PLACEMENT_INDICATOR      	=	upd_rec.	THIRD_PLACEMENT_INDICATOR	,
              THIRD_PLACEMENT_DATE	          =	upd_rec.	THIRD_PLACEMENT_DATE	,
              MONTH6_REVIEW_INDICATOR       	=	upd_rec.	MONTH6_REVIEW_INDICATOR	,
              MONTH6_REVIEW_DATE	            =	upd_rec.	MONTH6_REVIEW_DATE	,

              last_updated_date             	=	g_date
     where    prf.wfs_customer_no	            =	upd_rec.	wfs_customer_no  and
              prf.wfs_account_no              = upd_rec.  wfs_account_no   and
              prf.product_code_no             = upd_rec.  product_code_no  and 
            (
              nvl(identity_no                     ,0) <> upd_rec.identity_no or
              nvl(account_status                  ,0) <> upd_rec.account_status or
              nvl(application_no                  ,0) <> upd_rec.application_no or
              nvl(application_score               ,0) <> upd_rec.application_score or
              nvl(behaviour_score01               ,0) <> upd_rec.behaviour_score01 or
              nvl(behaviour_score02               ,0) <> upd_rec.behaviour_score02 or
              nvl(behaviour_score03               ,0) <> upd_rec.behaviour_score03 or
              nvl(behaviour_score04               ,0) <> upd_rec.behaviour_score04 or
              nvl(behaviour_score05               ,0) <> upd_rec.behaviour_score05 or
              nvl(behaviour_score06               ,0) <> upd_rec.behaviour_score06 or
              nvl(behaviour_score07               ,0) <> upd_rec.behaviour_score07 or
              nvl(behaviour_score08               ,0) <> upd_rec.behaviour_score08 or
              nvl(behaviour_score09               ,0) <> upd_rec.behaviour_score09 or
              nvl(behaviour_score10               ,0) <> upd_rec.behaviour_score10 or
              nvl(behaviour_score11               ,0) <> upd_rec.behaviour_score11 or
              nvl(behaviour_score12               ,0) <> upd_rec.behaviour_score12 or
              nvl(propensty_score01               ,0) <> upd_rec.propensty_score01 or
              nvl(propensty_score02               ,0) <> upd_rec.propensty_score02 or
              nvl(propensty_score03               ,0) <> upd_rec.propensty_score03 or
              nvl(propensty_score04               ,0) <> upd_rec.propensty_score04 or
              nvl(propensty_score05               ,0) <> upd_rec.propensty_score05 or
              nvl(propensty_score06               ,0) <> upd_rec.propensty_score06 or
              nvl(propensty_score07               ,0) <> upd_rec.propensty_score07 or
              nvl(propensty_score08               ,0) <> upd_rec.propensty_score08 or
              nvl(propensty_score09               ,0) <> upd_rec.propensty_score09 or
              nvl(propensty_score10               ,0) <> upd_rec.propensty_score10 or
              nvl(propensty_score11               ,0) <> upd_rec.propensty_score11 or
              nvl(propensty_score12               ,0) <> upd_rec.propensty_score12 or
              nvl(attrition_score01               ,0) <> upd_rec.attrition_score01 or
              nvl(attrition_score02               ,0) <> upd_rec.attrition_score02 or
              nvl(attrition_score03               ,0) <> upd_rec.attrition_score03 or
              nvl(attrition_score04               ,0) <> upd_rec.attrition_score04 or
              nvl(attrition_score05               ,0) <> upd_rec.attrition_score05 or
              nvl(attrition_score06               ,0) <> upd_rec.attrition_score06 or
              nvl(attrition_score07               ,0) <> upd_rec.attrition_score07 or
              nvl(attrition_score08               ,0) <> upd_rec.attrition_score08 or
              nvl(attrition_score09               ,0) <> upd_rec.attrition_score09 or
              nvl(attrition_score10               ,0) <> upd_rec.attrition_score10 or
              nvl(attrition_score11               ,0) <> upd_rec.attrition_score11 or
              nvl(attrition_score12               ,0) <> upd_rec.attrition_score12 or
              nvl(date_opened                     ,'1 Jan 1900') <> upd_rec.date_opened or
              nvl(date_last_pchs                  ,'1 Jan 1900') <> upd_rec.date_last_pchs or
              nvl(credit_limit                    ,0) <> upd_rec.credit_limit or
              nvl(current_balance                 ,0) <> upd_rec.current_balance or
              nvl(open_to_buy                     ,0) <> upd_rec.open_to_buy or
              nvl(last_pchs_val                   ,0) <> upd_rec.last_pchs_val or
              nvl(pchs_val_ytd                    ,0) <> upd_rec.pchs_val_ytd or
              nvl(pchs_val_ltd                    ,0) <> upd_rec.pchs_val_ltd or
              nvl(store_of_pref1                  ,0) <> upd_rec.store_of_pref1 or
              nvl(store_of_pref2                  ,0) <> upd_rec.store_of_pref2 or
              nvl(store_of_pref3                  ,0) <> upd_rec.store_of_pref3 or
              nvl(block_code1                     ,0) <> upd_rec.block_code1 or
              nvl(block_code2                     ,0) <> upd_rec.block_code2 or
              nvl(no_of_cards                     ,0) <> upd_rec.no_of_cards or
              nvl(date_last_updated               ,'1 Jan 1900') <> upd_rec.date_last_updated or
              nvl(chgoff_val                      ,0) <> upd_rec.chgoff_val or
              nvl(chgoff_rsn1                     ,0) <> upd_rec.chgoff_rsn1 or
              nvl(chgoff_rsn2                     ,0) <> upd_rec.chgoff_rsn2 or
              nvl(chgoff_status                   ,0) <> upd_rec.chgoff_status or
              nvl(times_in_coll                   ,0) <> upd_rec.times_in_coll or
              nvl(date_application                ,'1 Jan 1900') <> upd_rec.date_application or
              nvl(date_chgoff                     ,'1 Jan 1900') <> upd_rec.date_chgoff or
              nvl(date_closed                     ,'1 Jan 1900') <> upd_rec.date_closed or
              nvl(date_highest_bal                ,'1 Jan 1900') <> upd_rec.date_highest_bal or
              nvl(date_last_activity              ,'1 Jan 1900') <> upd_rec.date_last_activity or
              nvl(date_last_age                   ,'1 Jan 1900') <> upd_rec.date_last_age or
              nvl(date_last_crlm                  ,'1 Jan 1900') <> upd_rec.date_last_crlm or
              nvl(date_last_pymt                  ,'1 Jan 1900') <> upd_rec.date_last_pymt or
              nvl(date_last_rate_chg              ,'1 Jan 1900') <> upd_rec.date_last_rate_chg or
              nvl(date_last_reage                 ,'1 Jan 1900') <> upd_rec.date_last_reage or
              nvl(date_last_reclass               ,'1 Jan 1900') <> upd_rec.date_last_reclass or
              nvl(date_last_return                ,'1 Jan 1900') <> upd_rec.date_last_return or
              nvl(date_last_revpmt                ,'1 Jan 1900') <> upd_rec.date_last_revpmt or
              nvl(date_last_rtnchq                ,'1 Jan 1900') <> upd_rec.date_last_rtnchq or
              nvl(date_last_stmt                  ,'1 Jan 1900') <> upd_rec.date_last_stmt or
              nvl(days_till_chgoff                ,0) <> upd_rec.days_till_chgoff or
              nvl(highest_bal_val                 ,0) <> upd_rec.highest_bal_val or
              nvl(prev_credit_class               ,0) <> upd_rec.prev_credit_class or
              nvl(prev_credit_limit               ,0) <> upd_rec.prev_credit_limit or
              nvl(prev_int_val_ytd                ,0) <> upd_rec.prev_int_val_ytd or
              nvl(prev_int_pd_ytd                 ,0) <> upd_rec.prev_int_pd_ytd or
              nvl(market_flag_01                  ,0) <> upd_rec.market_flag_01 or
              nvl(market_flag_02                  ,0) <> upd_rec.market_flag_02 or
              nvl(market_flag_03                  ,0) <> upd_rec.market_flag_03 or
              nvl(market_flag_04                  ,0) <> upd_rec.market_flag_04 or
              nvl(market_flag_05                  ,0) <> upd_rec.market_flag_05 or
              nvl(market_flag_06                  ,0) <> upd_rec.market_flag_06 or
              nvl(market_flag_07                  ,0) <> upd_rec.market_flag_07 or
              nvl(market_flag_08                  ,0) <> upd_rec.market_flag_08 or
              nvl(market_flag_09                  ,0) <> upd_rec.market_flag_09 or
              nvl(market_flag_10                  ,0) <> upd_rec.market_flag_10 or
              nvl(market_flag_11                  ,0) <> upd_rec.market_flag_11 or
              nvl(market_flag_12                  ,0) <> upd_rec.market_flag_12 or
              nvl(market_flag_13                  ,0) <> upd_rec.market_flag_13 or
              nvl(market_flag_14                  ,0) <> upd_rec.market_flag_14 or
              nvl(market_flag_15                  ,0) <> upd_rec.market_flag_15 or
              nvl(market_flag_16                  ,0) <> upd_rec.market_flag_16 or
              nvl(market_flag_17                  ,0) <> upd_rec.market_flag_17 or
              nvl(market_flag_18                  ,0) <> upd_rec.market_flag_18 or
              nvl(market_flag_19                  ,0) <> upd_rec.market_flag_19 or
              nvl(market_flag_20                  ,0) <> upd_rec.market_flag_20 or
              nvl(last_pymt_val                   ,0) <> upd_rec.last_pymt_val or
              nvl(promotion_code1                 ,0) <> upd_rec.promotion_code1 or
              nvl(promotion_code2                 ,0) <> upd_rec.promotion_code2 or
              nvl(promotion_code3                 ,0) <> upd_rec.promotion_code3 or
              nvl(promotion_code4                 ,0) <> upd_rec.promotion_code4 or
              nvl(promotion_status1               ,0) <> upd_rec.promotion_status1 or
              nvl(promotion_status2               ,0) <> upd_rec.promotion_status2 or
              nvl(promotion_status3               ,0) <> upd_rec.promotion_status3 or
              nvl(promotion_status4               ,0) <> upd_rec.promotion_status4 or
              nvl(retail_plan_code                ,0) <> upd_rec.retail_plan_code or
              nvl(statmt_flag                     ,0) <> upd_rec.statmt_flag or
              nvl(statmt_msg_no_1                 ,0) <> upd_rec.statmt_msg_no_1 or
              nvl(statmt_msg_no_2                 ,0) <> upd_rec.statmt_msg_no_2 or
              nvl(write_off_days                  ,0) <> upd_rec.write_off_days or
              nvl(ins_incentv_store               ,0) <> upd_rec.ins_incentv_store or
              nvl(ins_cancel_date                 ,'1 Jan 1900') <> upd_rec.ins_cancel_date or
              nvl(ins_dt_lst_billed               ,'1 Jan 1900') <> upd_rec.ins_dt_lst_billed or
              nvl(ins_dt_lst_claim                ,'1 Jan 1900') <> upd_rec.ins_dt_lst_claim or
              nvl(ins_effectv_date                ,'1 Jan 1900') <> upd_rec.ins_effectv_date or
              nvl(ins_enrllmnt_state              ,0) <> upd_rec.ins_enrllmnt_state or
              nvl(ins_last_premium                ,0) <> upd_rec.ins_last_premium or
              nvl(ins_premium_mtd                 ,0) <> upd_rec.ins_premium_mtd or
              nvl(ins_premium                     ,0) <> upd_rec.ins_premium or
              nvl(ins_premium_state               ,0) <> upd_rec.ins_premium_state or
              nvl(ins_product                     ,0) <> upd_rec.ins_product or
              nvl(ins_rsn_cancelled               ,0) <> upd_rec.ins_rsn_cancelled or
              nvl(ins_reinstmt_date               ,'1 Jan 1900') <> upd_rec.ins_reinstmt_date or
              nvl(ins_status                      ,0) <> upd_rec.ins_status or
              nvl(plan_pmt_ovrd_flag              ,0) <> upd_rec.plan_pmt_ovrd_flag or
              nvl(mktg_promo                      ,0) <> upd_rec.mktg_promo or
              nvl(no_of_store_pref                ,0) <> upd_rec.no_of_store_pref or
              nvl(return_mail_cnt                 ,0) <> upd_rec.return_mail_cnt or
              nvl(loan_drawdown_val               ,0) <> upd_rec.loan_drawdown_val or
              nvl(loan_instalment                 ,0) <> upd_rec.loan_instalment or
              nvl(loan_repay_period               ,0) <> upd_rec.loan_repay_period or
              nvl(loan_tracker                    ,0) <> upd_rec.loan_tracker or
              nvl(sds_ref                         ,0) <> upd_rec.sds_ref or
              nvl(test_digit                      ,0) <> upd_rec.test_digit or
              nvl(test_digit_grp                  ,0) <> upd_rec.test_digit_grp or
              nvl(debit_order_flag                ,0) <> upd_rec.debit_order_flag or
              nvl(debit_order_dy                  ,0) <> upd_rec.debit_order_dy or
              nvl(debit_order_due                 ,0) <> upd_rec.debit_order_due or
              nvl(dtlst_accstat_chg               ,'1 Jan 1900') <> upd_rec.dtlst_accstat_chg or
              nvl(lcp_ind                         ,0) <> upd_rec.lcp_ind or
              nvl(companion_care_ind              ,0) <> upd_rec.companion_care_ind or
              nvl(accident_benft_ind              ,0) <> upd_rec.accident_benft_ind or
              nvl(cbp_ind                         ,0) <> upd_rec.cbp_ind or
              nvl(lbp_ind                         ,0) <> upd_rec.lbp_ind or
              nvl(ptp_status                      ,0) <> upd_rec.ptp_status or
              nvl(date_cred_limit                 ,'1 Jan 1900') <> upd_rec.date_cred_limit or
              nvl(comp_care_lst_prem              ,0) <> upd_rec.comp_care_lst_prem or
              nvl(comp_care_eff_date              ,'1 Jan 1900') <> upd_rec.comp_care_eff_date or
              nvl(acc_benft_lst_prem              ,0) <> upd_rec.acc_benft_lst_prem or
              nvl(acc_benft_eff_date              ,'1 Jan 1900') <> upd_rec.acc_benft_eff_date or
              nvl(overdue_amt                     ,0) <> upd_rec.overdue_amt or
              nvl(min_payment                     ,0) <> upd_rec.min_payment or
              nvl(payment_date                    ,'1 Jan 1900') <> upd_rec.payment_date or
              nvl(account_contact_id              ,0) <> upd_rec.account_contact_id or
              nvl(ttd_ind                         ,0) <> upd_rec.ttd_ind or
              nvl(bureau_score                    ,0) <> upd_rec.bureau_score or
              nvl(viking_code                     ,0) <> upd_rec.viking_code or
              nvl(viking_date                     ,'1 Jan 1900') <> upd_rec.viking_date or
              nvl(viking_amt                      ,0) <> upd_rec.viking_amt or
              nvl(debit_order_proj_amt            ,0) <> upd_rec.debit_order_proj_amt or
              nvl(debit_order_br_cd               ,0) <> upd_rec.debit_order_br_cd or
              nvl(debit_order_exp_dt              ,'1 Jan 1900') <> upd_rec.debit_order_exp_dt or
              nvl(debit_order_acc_type            ,0) <> upd_rec.debit_order_acc_type or
              nvl(debit_order_acc_no              ,0) <> upd_rec.debit_order_acc_no or
              nvl(debit_order_pymt_ind            ,0) <> upd_rec.debit_order_pymt_ind or
              nvl(dd_status                       ,0) <> upd_rec.dd_status or
              nvl(clim_review                     ,0) <> upd_rec.clim_review or
              nvl(dd_load_amt                     ,0) <> upd_rec.dd_load_amt or
              nvl(date_first_purch                ,'1 Jan 1900') <> upd_rec.date_first_purch or
              nvl(insurance_active_ind            ,0) <> upd_rec.insurance_active_ind or
              nvl(loan_restruct_ind               ,0) <> upd_rec.loan_restruct_ind or
              nvl(loan_restruct_date              ,'1 Jan 1900') <> upd_rec.loan_restruct_date or
              nvl(residence_id                    ,0) <> upd_rec.residence_id or
              nvl(debit_order_reversal_count      ,0) <> upd_rec.debit_order_reversal_count or
              nvl(debit_order_interim_pmt         ,0) <> upd_rec.debit_order_interim_pmt or
              nvl(debit_order_remitt_method       ,0) <> upd_rec.debit_order_remitt_method or
              nvl(staff_company_code              ,0) <> upd_rec.staff_company_code or
              nvl(write_off_ind                   ,0) <> upd_rec.write_off_ind or
              nvl(write_off_date                  ,'1 Jan 1900') <> upd_rec.write_off_date or
              nvl(write_off_value                 ,0) <> upd_rec.write_off_value or
              nvl(initiation_fee                  ,0) <> upd_rec.initiation_fee or
              nvl(monthly_service_fee             ,0) <> upd_rec.monthly_service_fee or
              nvl(initial_interest_rate           ,0) <> upd_rec.initial_interest_rate or
              nvl(delivery_method                 ,0) <> upd_rec.delivery_method or
              nvl(delivery_address                ,0) <> upd_rec.delivery_address or
              nvl(LEGAL_STATUS	                  ,0) <>	upd_rec.	LEGAL_STATUS	or
              nvl(LEGAL_STATUS_DATE	              ,'1 Jan 1900') <>	upd_rec.	LEGAL_STATUS_DATE	or
              nvl(FIRST_PLACEMENT_INDICATOR	      ,0) <>	upd_rec.	FIRST_PLACEMENT_INDICATOR	or
              nvl(FIRST_PLACEMENT_DATE	          ,'1 Jan 1900') <>	upd_rec.	FIRST_PLACEMENT_DATE	or
              nvl(SECOND_PLACEMENT_INDICATOR	    ,0) <>	upd_rec.	SECOND_PLACEMENT_INDICATOR	or
              nvl(SECOND_PLACEMENT_DATE	          ,'1 Jan 1900') <>	upd_rec.	SECOND_PLACEMENT_DATE	or
              nvl(THIRD_PLACEMENT_INDICATOR	      ,0) <>	upd_rec.	THIRD_PLACEMENT_INDICATOR	or
              nvl(THIRD_PLACEMENT_DATE	          ,'1 Jan 1900') <>	upd_rec.	THIRD_PLACEMENT_DATE	or
              nvl(MONTH6_REVIEW_INDICATOR	        ,0) <>	upd_rec.	MONTH6_REVIEW_INDICATOR	or
              nvl(MONTH6_REVIEW_DATE	            ,'1 Jan 1900') <>	upd_rec.	MONTH6_REVIEW_DATE

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
      l_text := 'Purge data on the allprod master every day:- '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      execute immediate 'truncate table dim_wfs_all_prod';

    select count(*)
    into   g_recs_read
    from   fnd_wfs_all_prod
--    where  last_updated_date = g_date
    ;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'UPDATE STATS ON ALL_PROD TABLE'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','DIM_WFS_ALL_PROD',estimate_percent=>1, DEGREE => 32); 

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
end wh_prf_cust_014u;
