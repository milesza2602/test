--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_680U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_680U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        FEB 2017
--  Author:      Linda Schoeman
--  Purpose:     Create OM4 CONSOLIDATION table in the performance layer
--               with input ex VARIOUS OM4 TABLES
--  Tables:      Input  - Various OM4
--               Output - wfs_om4_consolidation
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

g_recs_read            integer       :=  0;
g_recs_updated         integer       :=  0;
g_recs_deleted         integer       :=  0;
g_recs_inserted        integer       :=  0;
g_truncate_count       integer       :=  0;
g_last_mn_fin_year_no  integer;
g_last_mn_fin_month_no integer;
g_cal_year             integer;
g_cal_month            integer;
g_cal_start_date       date;
g_cal_end_date         date;
g_cal_num_days         integer;
g_cal_prev_year        integer;
g_cal_prev_month       integer;
g_cal_yyyymm           integer;
g_cal_prev_yyyymm      integer;

g_stmt                 varchar(300);
g_run_date             date;
g_date                 date := trunc(sysdate);

g_app_number         wfs_om4_consolidation.app_number%type;
g_sa_id              wfs_om4_consolidation.sa_id%type;
g_product_name       wfs_om4_consolidation.product_name%type;
g_application_date1  wfs_om4_consolidation.application_date1%type;
g_recs_duplicate     number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_680U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE OM4 CONSOLIDATION EX VARIOUS OM4 TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor om4_dup1 is
select app_number,sa_id,product_name,application_date1,rowid 
from dwh_wfs_performance.wfs_om4_consolidation
where master_application=0 
and cal_year_no=g_cal_year
and cal_month_no=g_cal_month
and (app_number,sa_id,product_name)
in
(select app_number,sa_id,product_name
from dwh_wfs_performance.wfs_om4_consolidation
where  master_application=0
and cal_year_no=g_cal_year
and cal_month_no=g_cal_month
group by app_number, sa_id,product_name
having count(*) > 1)
order by app_number,sa_id,product_name,application_date1 desc;

cursor om4_dup2 is
select sa_id,product_name, 
application_date1, 
master_application, rowid
from dwh_wfs_performance.wfs_om4_consolidation
where master_application<>0
and cal_year_no=g_cal_year
and cal_month_no=g_cal_month
and (sa_id,product_name)
in
(select sa_id,product_name
from dwh_wfs_performance.wfs_om4_consolidation
where master_application<>0
and cal_year_no=g_cal_year
and cal_month_no=g_cal_month
group by sa_id,product_name
having count(*) > 1)
order by sa_id,product_name,application_date1 desc, master_application desc;
--application_date desc, 
--master_application desc;

cursor om4_dup3 is
select sa_id,product_name,app_number,rowid 
from dwh_wfs_performance.wfs_om4_consolidation
where cal_year_no=g_cal_year
and cal_month_no=g_cal_month
and (app_number,product_name)
in
(select app_number,product_name
from dwh_wfs_performance.wfs_om4_consolidation
where cal_year_no=g_cal_year
and cal_month_no=g_cal_month
group by app_number, product_name
having count(*) > 1)
order by sa_id,product_name,app_number desc;

--**************************************************************************************************
-- Consolidate OM4 tables into a single table.
--**************************************************************************************************
procedure flagged_records_insert as
begin

   insert /*+ APPEND  */ into wfs_om4_consolidation prf
   with 
   om4_join as (
   select /*+ parallel(t1,4) parallel(t2,4) parallel(t3,4) parallel(t4,4)   */ 
      g_cal_year , 
      g_cal_month ,
      t1.credit_applications_id,  
      t1.created_by_user,
      t1.channel,
      t1.hmda_required,
      --t1.entered_time_stamp as application_date,
      to_char(t1.entered_time_stamp,'DD-MON-YY') as application_date1,
      to_char(t1.entered_time_stamp,'YYYYMM') as application_date2,
      t1.APP_NUMBER as application_no,
      t1.decision_party,
      t1.credit_application_status as application_status,
      t1.previous_status,
      t1.status_date_time_stamp,
      t1.combined_cheque_account,
      t1.combined_savings_account,
      t1.possible_duplicate,
      t1.confirmed_duplicate,
      t1.verify_app_address,
      t1.verify_app_income,
      t1.verify_app_employment,
      t1.verify_co_app_address,
      t1.verify_co_app_income,
      t1.verify_co_app_employment,
      t1.decision_party_notified_flag,
      t1.conditions_init_accepted_flag,
      t1.home_mortgage_disclosure_act,
      t1.dup_app_check_performed_flag,
      t1.tenant_code,
      t1.om4_tab,
      t1.poi_required as poirequiredind,
      t1.sa_id as identity_no,
      t1.master_application,
      t1.application_saved_timestamp,
      t1.store_number as store_of_applic,
      t1.agent_type,
      t1.campaign_id,
      t1.promotion_id,
      t1.PARTICIPANTS_COUNT,
      t1.INCLUDE_INSURANCE,
      t1.WORLD_OF_DIFFERENCE,
      t1.MY_SCHOOL_VILLAGE_PLANET,
      t1.PERSONAL_INSURANCE_QUOTE,
      t1.LOC_CORRESPONDENCE,
      t1.LOC_TERMS_CONDITIONS,
      t1.COMM_CHANNEL_FOR_COMMUNICATION,
      t1.COMM_CHANNEL_FOR_STATEMENT,
      t1.COMM_CHANNEL_EMAIL_ADDRESS,
      t1.COMM_CHANNEL_OTHER_EMAIL,
      t1.MARKETING_CONTACT,
      t1.MARKETING_PHONE,
      t1.MARKETING_SMS,
      t1.MARKETING_EMAIL,
      t1.MARKETING_POST,
      t1.MARKETING_AUTHORIZATION,
      t1.AGENT_INITIATED,
      t1.APP_DOC_DOCUMENT_TYPE,
      t1.APP_DOC_DOCUMENT_STATUS,
      t1.APP_DOC_EXTERNAL_ID,
      t1.APP_DOC_DOCUMENT_ID,
      t1.PROOF_TYPE,
      t1.MATCHED_CONSUMER_NO,
      t2.credit_details_id,
      t2.offer_id,
      t2.counter_offer as iscounteroffer,
      t2.product_purpose as loanpurpose,
      t2.product_amount as creditlimit, /*Term_credit_limit changed to product_amount. Sean G 10/08/2016*/
      t2.termin_months,
      t2.total_finance_charge_amount,
      t2.total_payment_amount,
      t2.PRODUCT_NAME as Product_Detail,
      t2.product_category,
      t2.product,
      t2.interest_rate_percent,
      t2.status,
      t2.product_id,
      t2.initiation_fee_selected,
      t2.product_decision_status,
      t2.reason_code as decline_reason,
      t2.decline_overide_selected,
      t2.decision_product_name,
      t2.channel as channel_T2,
      t2.strategy,
      t2.term_product_name,
      t2.term_product_subtype,
      t2.term,
      t2.term_credit_limit,
      t2.affordability_product_name,
      t2.affordability_product_subtype,
      t2.affordability_term,
      t2.affordability_limit_amount,
      t2.affordable_monthly_repayment,
      t2.disposable_income,
      t2.net_income,
      t2.risk_based_product_name,
      t2.risk_based_term,
      t2.risk_based_amount,
      t2.risk_category,
      t2.matrix_decision,
      t2.product_repayment_factor,
      t2.maximum_term,
      t2.application_score as applic_score,
      t2.qualification_product_subtype,
      t2.qualification_logo,
      t2.qualification_org,
      t2.qualification_min_lending_amt,
      t2.qualification_scrooge_ind,
      t2.possible_offers_product_name,
      t2.possible_offers_product_stype,
      t2.possible_offers_credit_limit,
      t2.possible_offers_term,
      t2.proof_req_pct,
      t2.poi_required_ind,
      t2.fica_required_ind,
      t2.interest_rate,
      t2.monthly_fee,
      t2.initiation_fee,
      t2.face_to_face_delivery_fee,
      t2.offer_product_selected,
      t2.possible_offers_pct_id,
      t2.insurance_type,
      t2.insurance_premium,
      t2.payment_frequency,
      t2.max_age_insurance,
      t2.min_age_insurance,
      t2.vat_percentage,
      t2.admin_percentage,
      t2.recalc_offer_only_indicator,
      t2.cr_ext_init_fee_selected,
      t2.cr_ext_init_drawdown_amount,
      t2.credit_ext_appeal_flag,
      t2.credit_ext_customer_accepted,
      t2.credit_ext_pct_id,
      t2.credit_ext_contract_concluded,
      t2.fee_borrower_basic,
      t2.fee_borrower_comprehensive,
      t2.fee_borrower_death,
      t2.fee_partner_basic,
      t2.fee_partner_comprehensive,
      t2.fee_partner_death,
      t2.borrower_balance_protection,
      t2.partner_balance_protection,
      t2.borrower_comprehensive,
      t2.partner_comprehensive,
      t2.borrower_death,
      t2.partner_death,
      t2.admin_fee,
      t2.repayment_method,
      t2.bank_name,
      t2.branch_name,
      t2.branch_code,
      t2.bank_account_number,
      t2.type_of_account,
      t2.account_holder_name,
      t2.account_age_years,
      t2.account_age_months,
      t2.debit_order_date as debitorderdate,
      t2.repayment_percentage,
      t2.delivery_type,
      t2.delivery_store,
      t2.delivery_charge,
      t2.which_address,
      t2.confirmation,
      t2.read_tc,
      t2.agree_contract,
      t2.tc_delivery_channel,
      t2.contract_delivery_channel,
      t2.accepted,
      t2.pl_contract_recorded_telep,
      t2.sign_contract,
      t2.qualification_decline_flag,
      t2.eligibility_decline_flag,
      t2.cancellation_reason,
      t2.insurance_bpi_consent,
      t2.monthly_insurance_fee,
      t2.credit_limit_increase_selected,
      t2.wupdate,
      t2.wupdate_contact_number,
      t2.pl_customer_xds_verification,
      t2.pl_customer_id_verification,
      t2.CREDIT_COST_MULTIPLE,
      t2.WREWARDS_CARD_IND,
      t2.MYSCHOOL_CARD_IND,
      t2.OPT_OUT_REWARD,
      t2.MYSCHOOL_CARD_NO,
      t2.WREWARDS_CARD_NO,
      t2.WREWARD_OFFERED_IND,
      t2.MYSCHOOL_OFFERED_IND,
      t2.OLD_CREDIT_LIMIT,
      t2.PCT_ID,
      t2.PD_INITIATION_FEE_SELECTED,
      t2.MIN_LIVING_EXPENSE_VALUE,
      t2.TOTAL_CREDIT_REPAYMENTS,
      t2.TOTAL_COMMITMENT,
      --  t3.offer_id as offer_id_T3,
      --  t3.credit_applications_id as credit_applications_id_T3,
      t3.origin as src_of_applic,
      t3.duplicate_ind,
      t4.personal_applicant_id,
      --  t4.credit_applications_id as credit_applications_id_T4,
      t4.personal_applicant_type,
      t4.existing_customer,
      t4.credit_report_purchase_flag,
      t4.no_of_cra_reports_purchased,
      t4.score_required,
      t4.has_cheque_account,
      t4.has_savings_account,
      t4.has_unlisted_cheque_account,
      t4.has_unlisted_savings_account,
      t4.residential_id,
      t4.residential_type,
      t4.residential_address_line1,
      t4.residential_city,
      t4.residential_province,
      t4.residential_postal_code,
      t4.residential_country,
      t4.residential_country_code,
      t4.residential_suburb,
      t4.birth_date,
      t4.number_of_dependents,
      t4.marital_status,
      t4.phone_number,
      t4.customer_reference_id,
      t4.customer_reference_type,
      t4.identification_number,
      t4.issue_date,
      t4.title,
      t4.full_name,
      t4.initials,
      t4.surname,
      t4.postal_address_line_1,
      t4.postal_city,
      t4.postal_province,
      t4.postal_postal_code,
      t4.postal_country,
      t4.postal_suburb,
      t4.employment_id,
      t4.our_employee,
      t4.work_phone_number,
      t4.subject_to_regulation,
      t4.occupation_id,
      t4.occupation_type,
      t4.position,
      t4.employment_type,
      t4.is_self_employed,
      t4.end_date,
      t4.employer_name,
      t4.industry_other,
      t4.employee_number,
      t4.total_gross_monthly_income as gross_income,
      t4.net_monthly_income,
      t4.total_monthly_payments,
      t4.monthly_housing_expense,
      t4.net_disposable_income,
      t4.debt_counceling,
      t4.product_name as product_name_T4,
      t4.card_printed,
      t4.card_validated,
      t4.bureau_consent,
      t4.spouse_consent,
      t4.nationality,
      t4.gender,
      t4.phone_type,
      t4.home_phone_number,
      t4.existing_customer_isc_selected,
      t4.existing_customer_cc_selected,
      t4.existing_customer_pl_selected,
      t4.mailing_address_same_as_res,
      t4.customer_number_crm,
      t4.source_of_funds,
      t4.envelope_reference_number,
      t4.country_of_residence,
      t4.not_contacted_for_marketing,
      t4.MONTHLY_MORTGAGE_PAYMENT,
      t4.MONTHLY_RENTAL_PAYMENT,
      t4.MONTHLY_MAINTENANCE_EXPENSES,
      t4.POI_CONSENT,
      t4.POI_BANK_DET_BANK_NAME,
      t4.POI_BANK_DET_BRANCH_NAME,
      t4.POI_BANK_DET_BRANCH_CODE,
      t4.POI_BANK_DET_BANK_ACC_NO,
      t4.POI_BANK_DET_BANK_TYPE_OF_ACC,
      t4.POI_BANK_DET_ACC_HOLDER_NAME,
      t4.POI_BANK_DET_ACC_AGE_YEARS,
      t4.POI_BANK_DET_ACC_AGE_MONTHS,
      t4.EMAIL_ADDRESS,
      t4.TOTAL_ASSET_BALANCE,
      t4.PARTNER_INITIALS,
      t4.PARTNER_SURNAME,
      t4.PARTNER_DOB,
      t4.PARTNER_IDENTITY_NO
   from FND_WFS_OM4_APPLICATION t1
   LEFT JOIN FND_WFS_OM4_CR_DETAIL t2
   on t1.CREDIT_APPLICATIONS_ID = t2.CREDIT_APPLICATIONS_ID
   LEFT JOIN FND_WFS_OM4_OFFER t3
   on t2.OFFER_ID = t3.OFFER_ID
   LEFT JOIN DWH_WFS_FOUNDATION.FND_WFS_OM4_PERSONAL_APPLICANT t4
   on t1.CREDIT_APPLICATIONS_ID = t4.CREDIT_APPLICATIONS_ID
   where t1.entered_time_stamp between g_cal_start_date and g_cal_end_date
   --where t1.entered_time_stamp between '01 Jan 2017' and '31 Jn 2017'
   and T3.ORIGIN = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' 
   and t4.PERSONAL_APPLICANT_TYPE = 'OM_APPLICANT_TYPE_APPLICANT' 
   --and t1.credit_applications_id in('8aa4146359f5750e015a223834164084','8aa4146359f5750e015a222361712110')
   and t3.DUPLICATE_IND = 0),
   
   om4_data as (
   select a.*, 
   case
       WHEN a.master_application = 0 THEN application_no
       ELSE a.master_application
   end as MasterAppFix, 
   case 
       when Channel = 'CallCenter' and Product_Detail = 'StoreCard' and store_of_applic not in (9999,8888) then 'Prospect'
       else case when Channel = 'CallCenter' and Product_Detail in ('CreditCard','PersonalLoan') and store_of_applic ^= 9999 then 'Xsell'
       else case when Channel in ('PaperApp','TTD') and store_of_applic not in (9999,8888)  then 'TTD' 
       else case when store_of_applic in (9999,8888) then 'WEB' end end end 
   end as application_channel,
   case 
       when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' then 1 else 0 
   end AS app_cnt, 
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true'    
        and loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled') then 1 else 0 
   end AS decisioning_outcome, 
   case 
       when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('Complete') and loanpurpose in ('Complete') then 1
       when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('CAMS Create service is not available',
           'Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available') and loanpurpose in ('Complete','Account Created','Pending Card Creation','Pending Card Creation G10D','StoreCard CheckOutCard failed',
           'StoreCard CheckOutCard Completed','Secondary StoreCard CheckOutCard complete','Personal Loan CheckOutCard failed','Primary CIF key generated',
           'Primary Credit Card activate failed','Primary CheckOutCard failed','Primary CheckOutCard Completed','Pending FICA Verification') then 1
       when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('Contract Presentation') and 
           loanpurpose in ('Complete','Account Created','Pending Card Creation','Pending Card Creation G10D','StoreCard CheckOutCard failed',
           'StoreCard CheckOutCard Completed','Secondary StoreCard CheckOutCard complete','Personal Loan CheckOutCard failed','Primary CIF key generated',
           'Primary Credit Card activate failed','Primary CheckOutCard failed','Primary CheckOutCard Completed','Pending FICA Verification') then 1 else 0 
   end AS pa_acc, 
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('Complete') and loanpurpose in ('Complete') then 1
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('CAMS Create service is not available',
             'Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation') and 
             loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled')then 1 
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'true' and application_status in ('Contract Presentation') and 
             loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled') then 1 else 0 
   end as final_outcome,
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false'  and 
            loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled') then 1 else 0 
   end AS bonus_offered, 
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false'  and 
             loanpurpose not in ('Customer Cancelled', 'Customer Undecided','Refer','Appeal','Decline','Requested','Continue') then 1 else 0 
   end AS bonus_apps,
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('Complete') and loanpurpose in ('Complete') then 1
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('CAMS Create service is not available',
             'Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available') then 1
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('Contract Presentation') and 
             loanpurpose in ('Complete','Account Created','Pending Card Creation','Pending Card Creation G10D','StoreCard CheckOutCard failed',
             'StoreCard CheckOutCard Completed','Secondary StoreCard CheckOutCard complete','Personal Loan CheckOutCard failed','Primary CIF key generated',
             'Primary Credit Card activate failed','Primary CheckOutCard failed','Primary CheckOutCard Completed','Pending FICA Verification') then 1 else 0 
   end AS bonus_pa_acc, 
   case when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('Complete') and loanpurpose in ('Complete') then 1
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('CAMS Create service is not available',
             'Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation') and  
             loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue')then 1 
        when src_of_applic = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and iscounteroffer = 'false' and application_status in ('Contract Presentation') and 
             loanpurpose not in ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue') then 1 else 0 
   end as bonus_final_outcome
   from om4_join a ),
      
   APPL AS (
   SELECT /*+ parallel(8) */
   a.credit_applications_id,
   a.user1 as operator_id,
   a.product_type as product_detail,
   case when a.decision = 'Signed' then 'TRUE' else 'FALSE' end as contract_signed,
   case when a.decision = 'Recorded Telephonically' then 'TRUE' else 'FALSE' end as contract_rec_tele
   FROM
      dwh_wfs_foundation.fnd_wfs_om4_workflow a
   WHERE
      -- only started tracking ContractAccepted since 01June2015
   a.activity_timestamp >= '01 Jun 2015' and
   a.activity = 'ContractAccepted' ), 

   PAA_TICKS as (
   SELECT /*+ parallel(a,8) parallel(b,8)  */
   b.app_number as application_no,
   --case when a.operator_id is null or a.operator_id=' ' then 'NOT FOUND' end as a.operator_id1,
   a.operator_id,
   a.product_detail,
   --case when a.operator_id is null then 'NOT FOUND' end as operator_id,
   --coalesce(max(a.contract_signed),'FALSE') as contract_signed,
   --coalesce(max(a.contract_rec_tele),'FALSE') as contract_rec_tele
   max(a.contract_signed) as contract_signed,
   max(a.contract_rec_tele) as contract_rec_tele    
   FROM APPL a  
   LEFT OUTER JOIN dwh_wfs_foundation.fnd_wfs_om4_application b
   on a.credit_applications_id = b.credit_applications_id
   GROUP BY b.app_number, a.operator_id, a.product_detail ),
  
   user_decision3 as(
   select
   a.*,
   b.contract_signed,
   b.contract_rec_tele,
   b.operator_id,
   --case when b.Contract_Signed is not null or b.contract_rec_tele is not null then 1 else 0 
   case when b.Contract_Signed = 'TRUE' or b.contract_rec_tele ='TRUE' then 1 else 0 
   end as PAA_Tick
   from om4_data a
   left join PAA_TICKS b
   on a.application_no = b.application_no 
   and a.product_detail = b.product_detail),

   offer as (   
   SELECT
   a.*,
   CASE
       WHEN iscounteroffer = 'true' AND PAA_Tick = 1 THEN 1 ELSE 0
   END AS pa_accept,
   CASE
       WHEN iscounteroffer = 'false' AND PAA_Tick = 1 THEN 1 ELSE 0
   END AS bonus_pa_accept,
   case
       when app_cnt = 1 then 'Yes' else 'No'
   end as Application,
   case
       when bonus_apps = 1 then 'Yes' else 'No'
   end as Bonus_Application,
   case
       when decisioning_outcome = 1 then 'Yes' else 'No'
   end as approvals,
   case 
       when  bonus_final_outcome = 1 then 'Yes' else 'No'
   end as bonus_approvals
   FROM user_decision3 a),
                 
   offer2 as (
   select a.*,
   case
       when pa_accept = 1 then 'Yes' else 'No'
   end as preagreement,
   case 
       when bonus_pa_accept = 1 then 'Yes' else 'No'
   end as bonus_preagreement,
   CASE   
       when Product_Detail = 'CreditCard' and gross_income between 2000 and 41666 then 'Gold' else 
       case when Product_Detail = 'CreditCard' and gross_income > 41666 then 'Black' end
   END as cc_card_type,
   case 
       --when debitorderdate ^= . then 'Y' else 'N'
       when debitorderdate is not null then 'Y' else 'N'
   end as debit_order_ind 
   from offer a ),
        
   offer3 as (
   select a.*,
   case 
       when PreAgreement = 'Yes' and poirequiredind = 'FALSE' and Approvals = 'Yes' then 'Yes' else 'No'
   end as PreAgreement_POI,
   case 
       when (Bonus_PreAgreement = 'Yes' and poirequiredind = 'FALSE' and Bonus_Approvals = 'Yes') then 'Yes' else 'No'
   end as bonus_preagreement_poi,
   0,
   --Additional fields added - 28/02/2017 LSchoeman
   --Scorecard_seg field must be updated elsewhere as it is dependent on the behaviour_score being populated.
   --case when behaviour_score > 13  and Product_Detail in ('CreditCard','PersonalLoan') then 'EXIST' else 'NEW'
   --end as ScoreCard_Seg,
   '',     
   case when (Application = 'Yes' or Bonus_Application = 'Yes') then 'Yes' else 'No'
   end as true_app,
   case when (Approvals = 'Yes' or Bonus_Approvals = 'Yes') then 'Yes'else 'No'
   end as final_approval
   --case when ((Approvals = 'Yes' and PreAgreement_POI = 'Yes') or (Bonus_Application = 'Yes' 
   --and Bonus_PreAgreement_POI = 'Yes')) then 'Yes' else 'No'
   --end as booked_flag,
   --TRUNC(SYSDATE)
   from offer2 a )
   
   select a.*, 
   case when ((a.Approvals = 'Yes' and a.PreAgreement_POI = 'Yes') or (a.Bonus_Application = 'Yes' 
   and a.bonus_preagreement_poi = 'Yes')) then 'Yes' else 'No'
   end as booked_flag,
   TRUNC(SYSDATE)
   from offer3 a;
          
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
-- Derive behaviour score and scorecard_seg values and update table wfs_om4_consolidation.
--**************************************************************************************************
procedure behaviour_scores as
begin

   MERGE  INTO wfs_om4_consolidation fnd 
   USING (
           select /*+ full(a) parallel(a,4) full(b) */ distinct  b.id_number, max(a.behaviour_score ) behaviour_score
           from dwh_wfs_performance.wfs_stmt_cr_crd a 
           left join dwh_wfs_foundation.FND_WFS_CUSTOMER_ABSA  b 
           on  a.customer_key=b.customer_key
           where cycle_6 = g_cal_prev_yyyymm
           and id_number is not null  
           and length(id_number) = 13 
           and id_number != '0000000000000'
--            and id_number='5506280211088'
           group by b.id_number
           order by b.id_number 
         ) mer_rec
   ON    ( fnd.IDENTIFICATION_NUMBER	          =	 mer_rec.id_number and
           fnd.cal_year_no                     =  g_cal_year and
           fnd.cal_month_no                    =  g_cal_month)
   WHEN MATCHED THEN 
   UPDATE SET
      fnd.	BEHAVIOUR_SCORE01	        =	mer_rec.	behaviour_score	;

   commit;

    l_text := '2ND BEHAVIOUR SCORE AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   MERGE  INTO dwh_wfs_performance.wfs_om4_consolidation fnd 
   USING (
            SELECT /*+ full(a) parallel(a,4) */ DISTINCT identity_no, MAX(behaviour_score01) behaviour_score01
            FROM wfs_all_prod_mnth a 
            WHERE account_status = 'A' 
            and CAL_YEAR_NO      = g_cal_prev_year
            and CAL_MONTH_NO     = g_cal_prev_month
            group by identity_no
            order by identity_no 
         ) mer_rec
   ON    ( fnd.IDENTIFICATION_NUMBER	         =	mer_rec.identity_no and
           fnd.cal_year_no                     =  g_cal_year and
           fnd.cal_month_no                    =  g_cal_month)
   WHEN MATCHED THEN 
   UPDATE SET
      fnd.	BEHAVIOUR_SCORE01	          =	mer_rec.	behaviour_score01	
      WHERE mer_rec.behaviour_score01   > fnd.BEHAVIOUR_SCORE01;
      
   l_text := 'UPDATE SCORECARD_SEG AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   MERGE  INTO dwh_wfs_performance.wfs_om4_consolidation fnd 
   USING (
            SELECT /*+ full(a) parallel(a,4) */ 
            distinct cal_year_no, cal_month_no, credit_details_id--, operator_id
            --product_name, behaviour_score01
            FROM DWH_WFS_PERFORMANCE.WFS_OM4_CONSOLIDATION 
            where CAL_YEAR_NO = g_cal_year
            and CAL_MONTH_NO  = g_cal_month    
            and behaviour_score01>13
            and product_name in('CreditCard', 'PersonalLoan')    
         ) mer_rec
   ON    ( fnd.cal_year_no      =  g_cal_year and
           fnd.cal_month_no     =  g_cal_month and
           fnd.credit_details_id=  mer_rec.credit_details_id)
           --and fnd.operator_id  =  mer_rec.operator_id)
   WHEN MATCHED THEN 
   UPDATE SET
      fnd.scorecard_seg      =	'EXIST';

   commit;
  
   update DWH_WFS_PERFORMANCE.WFS_OM4_CONSOLIDATION
   set scorecard_seg='NEW' where scorecard_seg is null
   and CAL_YEAR_NO = g_cal_year
   and CAL_MONTH_NO  = g_cal_month;    

   commit; 
   
   g_recs_deleted := g_recs_deleted + sql%rowcount;

   exception
      when others then
          l_message := 'SCORECARD_SEG MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise;


end behaviour_scores;
--**************************************************************************************************
--Remove duplicate keys from the om4_consolidated data using cursor om4_dup1.
--**************************************************************************************************
procedure dedup_om4_data1 as
begin

g_app_number        := 0;
g_sa_id             := 0;
g_product_name      := ' ';


for dup_record in om4_dup1
   loop

    if  dup_record.app_number         = g_app_number and
        dup_record.sa_id              = g_sa_id and
        dup_record.product_name       = g_product_name then
        delete from dwh_wfs_performance.wfs_om4_consolidation where
        rowid = dup_record.rowid;
        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;
    
    g_app_number        := dup_record.app_number;
    g_sa_id             := dup_record.sa_id;
    g_product_name      := dup_record.product_name;

    
   end loop;

   commit;

   exception
      when others then
          l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise;

end dedup_om4_data1;
--**************************************************************************************************
--Remove duplicate keys from the om4_consolidated data using cursor om4_dup2.
--**************************************************************************************************
procedure dedup_om4_data2 as
begin
--g_app_number        := 0;
g_sa_id             := 0;
g_product_name      := ' ';


for dup_record in om4_dup2
   loop

    
        --dup_record.app_number         = g_app_number and
     
    if  dup_record.sa_id              = g_sa_id and
        dup_record.product_name       = g_product_name then
        delete from dwh_wfs_performance.wfs_om4_consolidation where
        rowid = dup_record.rowid;
        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;
    
    --g_app_number        := dup_record.app_number;
    g_sa_id             := dup_record.sa_id;
    g_product_name      := dup_record.product_name;

   end loop;

   commit;

   exception
      when others then
          l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise;

end dedup_om4_data2;
--*************************************************************************************************
--Remove duplicate keys from the om4_consolidated data using cursor om4_dup3.
--*************************************************************************************************
procedure dedup_om4_data3 as
begin

g_app_number        := 0;
g_product_name      := ' ';

for dup_record in om4_dup3
   loop

    if  dup_record.app_number         = g_app_number and
        dup_record.product_name       = g_product_name then
        delete from dwh_wfs_performance.wfs_om4_consolidation where
        rowid = dup_record.rowid;
        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;
    
    g_app_number        := dup_record.app_number;
    g_product_name      := dup_record.product_name;
        
   end loop;

   commit;

   exception
      when others then
          l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise;   
   
end dedup_om4_data3;
--**************************************************************************************************
--FINAL BULK INSERT INTO PERMANENT TABLE DWH_WFS_PERFORMANCE.WFS_OM4_CONSOLICATION.
--*************************************************************************************************
--procedure final_bulk_insert as
--begin

   --DO THE FINAL INSERT INTO TABLE DWH_WFS_PERFORMANCE.WFS_OM4_CONSOLIDATION HERE.
   --load from dwh_wfs_performance.om4_consolidation_dedup
   --insert into dwh_wfs_performance.om4_consolidation_dedup;
   
   --l_text := 'BULK INSERT INTO FINAL TABLE AT '||
   --to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   --TRUNCATE THE TEMP 0M4 TABLE.
   --truncate table dwh_wfs_performance.om4_consolidation_dedup;

   --l_text := 'TEMP 0M4 TABLE TRUNCATED AT '||
   --to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--end final_bulk_insert;
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
   if to_char(sysdate,'DY')  <> 'SAT' then
       l_text      := 'This job only runs on Saturday and today'||trunc(sysdate)||' is not that day !';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
   end if;  
   
    l_text      := 'This job only runs on Saturday and today '||trunc(sysdate)||' is that day !';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_last_mn_fin_year_no, g_last_mn_fin_month_no
     from dim_control;

for sub in 0..2
--for sub in 0..0
loop

     --select trunc(sysdate - SUB*31,'MM')-1 as prev_month_end, 
     --add_months(trunc(sysdate - SUB*31,'MM'),-1) as prev_month_start 
     --into g_cal_end_date, g_cal_start_date
     --from dual;
               
     select trunc(add_months(sysdate,-1*SUB),'MM')-1 as prev_month_end, 
     add_months(trunc(add_months(sysdate,-1*SUB),'MM'),-1) as prev_month_start 
     into g_cal_end_date, g_cal_start_date
     from dual;

     g_cal_month := to_char(g_cal_start_date,'MM');
     g_cal_year  := to_char(g_cal_start_date,'YYYY');
     
     if g_cal_month > 1 then
       g_cal_prev_month := g_cal_month - 1; 
       g_cal_prev_year  := g_cal_year;
    else
       g_cal_prev_month := 12;
       g_cal_prev_year  := g_cal_year - 1;
    end if;  

  
    g_cal_yyyymm := g_cal_year * 100 + g_cal_month ;
    g_cal_prev_yyyymm := g_cal_prev_year * 100 + g_cal_prev_month ;
         
    l_text := 'Calendar Year and Month '||g_cal_yyyymm ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    l_text := 'Calendar Previous Year and Month '||g_cal_prev_year||' '||g_cal_prev_month||' '||g_cal_prev_yyyymm ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

    l_text := 'Dates being rolled up '||g_cal_start_date||' thru '||g_cal_end_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Year/Month being procesed '||g_cal_year||' '||g_cal_month;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'BULK DELETE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete from wfs_om4_consolidation
    where  cal_month_no = g_cal_month and
           cal_year_no  = g_cal_year;

    g_recs_deleted :=  sql%rowcount;
    
    commit;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    g_recs_read := g_recs_updated + g_recs_inserted;
    
    l_text := 'UPDATE STATS ON ALL TABLES'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    DBMS_STATS.gather_table_stats ('DWH_WFS_PERFORMANCE','WFS_OM4_CONSOLIDATION',estimate_percent=> dbms_stats.auto_sample_size, DEGREE => 32);
    
    COMMIT;
    
    l_text := 'GET BEHAVIOR SCORES AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    behaviour_scores;
    dedup_om4_data1;
    dedup_om4_data2;
    dedup_om4_data3;
    --final_bulk_insert;
    
end loop;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'UPDATED IN 2ND BEHAVIOUR PASS  '||g_recs_deleted;
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
end wh_prf_wfs_680u;
