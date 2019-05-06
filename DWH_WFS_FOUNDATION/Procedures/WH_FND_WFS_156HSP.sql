--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_156HSP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_156HSP" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- Date:        June 2015
  -- Author:      Jerome Appollis
  -- Purpose:     Create fnd_wfs_om4_cr_detail table in the foundation layer
  --              with input ex staging table from WFS.
  -- Tables:      Input  - stg_om4_cr_detail
  --              Output - fnd_wfs_om4_cr_detail
  -- Packages:    constants, dwh_log, dwh_valid
  --
  -- Maintenance:
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
  g_recs_read      INTEGER := 0;
  g_recs_updated   INTEGER := 0;
  g_recs_inserted  INTEGER := 0;
  g_recs_hospital  INTEGER := 0;
  g_recs_duplicate INTEGER := 0;
  g_truncate_count INTEGER := 0;
  g_cr_detail_id stg_om4_cr_detail.credit_details_id%type;
  g_date DATE := TRUNC(sysdate);
  L_MESSAGE SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_WFS_156U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_TEXT SYS_DWH_LOG.LOG_TEXT%TYPE ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD WFS CREDIT DETAIL DATA';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  CURSOR stg_dup
  IS
    SELECT *
    FROM stg_om4_cr_detail
    WHERE (credit_details_id) IN
      (SELECT credit_details_id
      FROM stg_om4_cr_detail
      GROUP BY credit_details_id
      HAVING COUNT(*) > 1
      )
  ORDER BY credit_details_id,
    sys_source_batch_id DESC ,
    sys_source_sequence_no DESC;
  CURSOR c_stg_wfs_om4_cr_detail_dly
  IS
    SELECT
      /*+ FULL(stg)  parallel (stg,2) */
      stg.*
    FROM stg_om4_cr_detail stg,
      fnd_wfs_om4_cr_detail fnd
    WHERE stg.credit_details_id = fnd.credit_details_id
    AND stg.sys_process_code    = 'N'
      -- Any further validation goes in here - like xxx.ind in (0,1) ---
    ORDER BY stg.credit_details_id,
      stg.sys_source_batch_id,
      stg.sys_source_sequence_no ;
  --**************************************************************************************************
  -- Eliminate duplicates on the very rare occasion they may be present
  --**************************************************************************************************
PROCEDURE remove_duplicates
AS
BEGIN
  g_cr_detail_id := 0;
  FOR dupp_record IN stg_dup
  LOOP
    IF dupp_record.credit_details_id = g_cr_detail_id THEN
      UPDATE stg_om4_cr_detail stg
      SET sys_process_code       = 'D'
      WHERE sys_source_batch_id  = dupp_record.sys_source_batch_id
      AND sys_source_sequence_no = dupp_record.sys_source_sequence_no;
      g_recs_duplicate          := g_recs_duplicate + 1;
    END IF;
    g_cr_detail_id := dupp_record.credit_details_id;
  END LOOP;
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  l_message := 'REMOVE DUPLICATES - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END remove_duplicates;
--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
PROCEDURE flagged_records_insert
AS
BEGIN
  --     g_rec_out.last_updated_date         := g_date;
  INSERT
    /*+ APPEND parallel (fnd,2) */
  INTO fnd_wfs_om4_cr_detail fnd
  SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
      cpy.credit_details_id	,
      cpy.credit_applications_id	,
      cpy.offer_id	,
      cpy.counter_offer	,
      cpy.product_purpose	,
      cpy.product_amount	,
      cpy.termin_months	,
      cpy.total_finance_charge_amount	,
      cpy.total_payment_amount	,
      cpy.product_name	,
      cpy.product_category	,
      cpy.product	,
      cpy.interest_rate_percent	,
      cpy.status	,
      cpy.product_id	,
      cpy.initiation_fee_selected	,
      cpy.product_decision_status	,
      cpy.reason_code	,
      cpy.decline_overide_selected	,
      cpy.decision_product_name	,
      cpy.channel	,
      cpy.strategy	,
      cpy.term_product_name	,
      cpy.term_product_subtype	,
      cpy.term	,
      cpy.term_credit_limit	,
      cpy.affordability_product_name	,
      cpy.affordability_product_subtype	,
      cpy.affordability_term	,
      cpy.affordability_limit_amount	,
      cpy.affordable_monthly_repayment	,
      cpy.disposable_income	,
      cpy.net_income	,
      cpy.risk_based_product_name	,
      cpy.risk_based_term	,
      cpy.risk_based_amount	,
      cpy.risk_category	,
      cpy.matrix_decision	,
      cpy.product_repayment_factor	,
      cpy.maximum_term	,
      cpy.application_score	,
      cpy.qualification_product_subtype	,
      cpy.qualification_logo	,
      cpy.qualification_org	,
      cpy.qualification_min_lending_amt	,
      cpy.qualification_scrooge_ind	,
      cpy.possible_offers_product_name	,
      cpy.possible_offers_product_stype	,
      cpy.possible_offers_credit_limit	,
      cpy.possible_offers_term	,
      cpy.proof_req_pct	,
      cpy.poi_required_ind	,
      cpy.fica_required_ind	,
      cpy.interest_rate	,
      cpy.monthly_fee	,
      cpy.initiation_fee	,
      cpy.face_to_face_delivery_fee	,
      cpy.offer_product_selected	,
      cpy.possible_offers_pct_id	,
      cpy.insurance_type	,
      cpy.insurance_premium	,
      cpy.payment_frequency	,
      cpy.max_age_insurance	,
      cpy.min_age_insurance	,
      cpy.vat_percentage	,
      cpy.admin_percentage	,
      cpy.recalc_offer_only_indicator	,
      cpy.cr_ext_init_fee_selected	,
      cpy.cr_ext_init_drawdown_amount	,
      cpy.credit_ext_appeal_flag	,
      cpy.credit_ext_customer_accepted	,
      cpy.credit_ext_pct_id	,
      cpy.credit_ext_contract_concluded	,
      cpy.fee_borrower_basic	,
      cpy.fee_borrower_comprehensive	,
      cpy.fee_borrower_death	,
      cpy.fee_partner_basic	,
      cpy.fee_partner_comprehensive	,
      cpy.fee_partner_death	,
      cpy.borrower_balance_protection	,
      cpy.partner_balance_protection	,
      cpy.borrower_comprehensive	,
      cpy.partner_comprehensive	,
      cpy.borrower_death	,
      cpy.partner_death	,
 --     cpy.	monthly_insurance_amount	,
      cpy.admin_fee	,
      cpy.repayment_method	,
      cpy.bank_name	,
      cpy.branch_name	,
      cpy.branch_code	,
      cpy.bank_account_number	,
      cpy.type_of_account	,
      cpy.account_holder_name	,
      cpy.account_age_years	,
      cpy.account_age_months	,
      cpy.debit_order_date	,
      cpy.repayment_percentage	,
      cpy.delivery_type	,
      cpy.delivery_store	,
      cpy.delivery_charge	,
      cpy.which_address	,
      cpy.confirmation	,
      cpy.read_tc	,
      cpy.agree_contract	,
      cpy.tc_delivery_channel	,
      cpy.contract_delivery_channel	,
      cpy.accepted	,
      cpy.pl_contract_recorded_telep	,
      cpy.sign_contract	,
      cpy.qualification_decline_flag	,
      cpy.eligibility_decline_flag	,
      cpy.cancellation_reason ,
      cpy.insurance_bpi_consent ,
      cpy.monthly_insurance_fee ,
      cpy.credit_limit_increase_selected ,
      cpy.wupdate ,
      cpy.wupdate_contact_number ,
      cpy.pl_customer_xds_verification ,
      cpy.pl_customer_id_verification ,
      g_date AS last_updated_date
  FROM stg_om4_cr_detail cpy
  WHERE NOT EXISTS
    (SELECT
      /*+ nl_aj */
      *
    FROM fnd_wfs_om4_cr_detail
    WHERE credit_details_id = cpy.credit_details_id
    )
    -- Any further validation goes in here - like xxx.ind in (0,1) ---
  AND sys_process_code = 'N';
  g_recs_inserted     := g_recs_inserted + sql%rowcount;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG INSERT - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG INSERT - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_insert;
--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
PROCEDURE flagged_records_update
AS
BEGIN
  FOR upd_rec IN c_stg_wfs_om4_cr_detail_dly
  LOOP
    UPDATE fnd_wfs_om4_cr_detail fnd
    SET 
        fnd.credit_applications_id	=	upd_rec.	credit_applications_id	,
        fnd.offer_id	=	upd_rec.	offer_id	,
        fnd.counter_offer	=	upd_rec.	counter_offer	,
        fnd.product_purpose	=	upd_rec.	product_purpose	,
        fnd.product_amount	=	upd_rec.	product_amount	,
        fnd.termin_months	=	upd_rec.	termin_months	,
        fnd.total_finance_charge_amount	=	upd_rec.	total_finance_charge_amount	,
        fnd.total_payment_amount	=	upd_rec.	total_payment_amount	,
        fnd.product_name	=	upd_rec.	product_name	,
        fnd.product_category	=	upd_rec.	product_category	,
        fnd.product	=	upd_rec.	product	,
        fnd.interest_rate_percent	=	upd_rec.	interest_rate_percent	,
        fnd.status	=	upd_rec.	status	,
        fnd.product_id	=	upd_rec.	product_id	,
        fnd.initiation_fee_selected	=	upd_rec.	initiation_fee_selected	,
        fnd.product_decision_status	=	upd_rec.	product_decision_status	,
        fnd.reason_code	=	upd_rec.	reason_code	,
        fnd.decline_overide_selected	=	upd_rec.	decline_overide_selected	,
        fnd.decision_product_name	=	upd_rec.	decision_product_name	,
        fnd.channel	=	upd_rec.	channel	,
        fnd.strategy	=	upd_rec.	strategy	,
        fnd.term_product_name	=	upd_rec.	term_product_name	,
        fnd.term_product_subtype	=	upd_rec.	term_product_subtype	,
        fnd.term	=	upd_rec.	term	,
        fnd.term_credit_limit	=	upd_rec.	term_credit_limit	,
        fnd.affordability_product_name	=	upd_rec.	affordability_product_name	,
        fnd.affordability_product_subtype	=	upd_rec.	affordability_product_subtype	,
        fnd.affordability_term	=	upd_rec.	affordability_term	,
        fnd.affordability_limit_amount	=	upd_rec.	affordability_limit_amount	,
        fnd.affordable_monthly_repayment	=	upd_rec.	affordable_monthly_repayment	,
        fnd.disposable_income	=	upd_rec.	disposable_income	,
        fnd.net_income	=	upd_rec.	net_income	,
        fnd.risk_based_product_name	=	upd_rec.	risk_based_product_name	,
        fnd.risk_based_term	=	upd_rec.	risk_based_term	,
        fnd.risk_based_amount	=	upd_rec.	risk_based_amount	,
        fnd.risk_category	=	upd_rec.	risk_category	,
        fnd.matrix_decision	=	upd_rec.	matrix_decision	,
        fnd.product_repayment_factor	=	upd_rec.	product_repayment_factor	,
        fnd.maximum_term	=	upd_rec.	maximum_term	,
        fnd.application_score	=	upd_rec.	application_score	,
        fnd.qualification_product_subtype	=	upd_rec.	qualification_product_subtype	,
        fnd.qualification_logo	=	upd_rec.	qualification_logo	,
        fnd.qualification_org	=	upd_rec.	qualification_org	,
        fnd.qualification_min_lending_amt	=	upd_rec.	qualification_min_lending_amt	,
        fnd.qualification_scrooge_ind	=	upd_rec.	qualification_scrooge_ind	,
        fnd.possible_offers_product_name	=	upd_rec.	possible_offers_product_name	,
        fnd.possible_offers_product_stype	=	upd_rec.	possible_offers_product_stype	,
        fnd.possible_offers_credit_limit	=	upd_rec.	possible_offers_credit_limit	,
        fnd.possible_offers_term	=	upd_rec.	possible_offers_term	,
        fnd.proof_req_pct	=	upd_rec.	proof_req_pct	,
        fnd.poi_required_ind	=	upd_rec.	poi_required_ind	,
        fnd.fica_required_ind	=	upd_rec.	fica_required_ind	,
        fnd.interest_rate	=	upd_rec.	interest_rate	,
        fnd.monthly_fee	=	upd_rec.	monthly_fee	,
        fnd.initiation_fee	=	upd_rec.	initiation_fee	,
        fnd.face_to_face_delivery_fee	=	upd_rec.	face_to_face_delivery_fee	,
        fnd.offer_product_selected	=	upd_rec.	offer_product_selected	,
        fnd.possible_offers_pct_id	=	upd_rec.	possible_offers_pct_id	,
        fnd.insurance_type	=	upd_rec.	insurance_type	,
        fnd.insurance_premium	=	upd_rec.	insurance_premium	,
        fnd.payment_frequency	=	upd_rec.	payment_frequency	,
        fnd.max_age_insurance	=	upd_rec.	max_age_insurance	,
        fnd.min_age_insurance	=	upd_rec.	min_age_insurance	,
        fnd.vat_percentage	=	upd_rec.	vat_percentage	,
        fnd.admin_percentage	=	upd_rec.	admin_percentage	,
        fnd.recalc_offer_only_indicator	=	upd_rec.	recalc_offer_only_indicator	,
        fnd.cr_ext_init_fee_selected	=	upd_rec.	cr_ext_init_fee_selected	,
        fnd.cr_ext_init_drawdown_amount	=	upd_rec.	cr_ext_init_drawdown_amount	,
        fnd.credit_ext_appeal_flag	=	upd_rec.	credit_ext_appeal_flag	,
        fnd.credit_ext_customer_accepted	=	upd_rec.	credit_ext_customer_accepted	,
        fnd.credit_ext_pct_id	=	upd_rec.	credit_ext_pct_id	,
        fnd.credit_ext_contract_concluded	=	upd_rec.	credit_ext_contract_concluded	,
        fnd.fee_borrower_basic	=	upd_rec.	fee_borrower_basic	,
        fnd.fee_borrower_comprehensive	=	upd_rec.	fee_borrower_comprehensive	,
        fnd.fee_borrower_death	=	upd_rec.	fee_borrower_death	,
        fnd.fee_partner_basic	=	upd_rec.	fee_partner_basic	,
        fnd.fee_partner_comprehensive	=	upd_rec.	fee_partner_comprehensive	,
        fnd.fee_partner_death	=	upd_rec.	fee_partner_death	,
        fnd.borrower_balance_protection	=	upd_rec.	borrower_balance_protection	,
        fnd.partner_balance_protection	=	upd_rec.	partner_balance_protection	,
        fnd.borrower_comprehensive	=	upd_rec.	borrower_comprehensive	,
        fnd.partner_comprehensive	=	upd_rec.	partner_comprehensive	,
        fnd.borrower_death	=	upd_rec.	borrower_death	,
        fnd.partner_death	=	upd_rec.	partner_death	,
 --       fnd.monthly_insurance_amount	=	upd_rec.	monthly_insurance_amount	,
        fnd.admin_fee	=	upd_rec.	admin_fee	,
        fnd.repayment_method	=	upd_rec.	repayment_method	,
        fnd.bank_name	=	upd_rec.	bank_name	,
        fnd.branch_name	=	upd_rec.	branch_name	,
        fnd.branch_code	=	upd_rec.	branch_code	,
        fnd.bank_account_number	=	upd_rec.	bank_account_number	,
        fnd.type_of_account	=	upd_rec.	type_of_account	,
        fnd.account_holder_name	=	upd_rec.	account_holder_name	,
        fnd.account_age_years	=	upd_rec.	account_age_years	,
        fnd.account_age_months	=	upd_rec.	account_age_months	,
        fnd.debit_order_date	=	upd_rec.	debit_order_date	,
        fnd.repayment_percentage	=	upd_rec.	repayment_percentage	,
        fnd.delivery_type	=	upd_rec.	delivery_type	,
        fnd.delivery_store	=	upd_rec.	delivery_store	,
        fnd.delivery_charge	=	upd_rec.	delivery_charge	,
        fnd.which_address	=	upd_rec.	which_address	,
        fnd.confirmation	=	upd_rec.	confirmation	,
        fnd.read_tc	=	upd_rec.	read_tc	,
        fnd.agree_contract	=	upd_rec.	agree_contract	,
        fnd.tc_delivery_channel	=	upd_rec.	tc_delivery_channel	,
        fnd.contract_delivery_channel	=	upd_rec.	contract_delivery_channel	,
        fnd.accepted	=	upd_rec.	accepted	,
        fnd.pl_contract_recorded_telep	=	upd_rec.	pl_contract_recorded_telep	,
        fnd.sign_contract	=	upd_rec.	sign_contract	,
        fnd.qualification_decline_flag	=	upd_rec.	qualification_decline_flag	,
        fnd.eligibility_decline_flag	=	upd_rec.	eligibility_decline_flag	,
        fnd.cancellation_reason	=	upd_rec.	cancellation_reason	,
        fnd.insurance_bpi_consent	=	upd_rec.	insurance_bpi_consent	,
        fnd.monthly_insurance_fee	=	upd_rec.	monthly_insurance_fee,
        fnd.credit_limit_increase_selected	=	upd_rec.	credit_limit_increase_selected	,
        fnd.wupdate	=	upd_rec.	wupdate	,
        fnd.wupdate_contact_number	=	upd_rec.	wupdate_contact_number	,
        fnd.pl_customer_xds_verification	=	upd_rec.	pl_customer_xds_verification	,
        fnd.pl_customer_id_verification	=	upd_rec.	pl_customer_id_verification	,
      fnd.last_updated_date                    = g_date
    WHERE fnd.credit_details_id                = upd_rec.credit_details_id
--    AND  upd_rec.SYS_LOAD_DATE > fnd.LAST_UPDATED_DATE
    AND ( 
        NVL(fnd.credit_applications_id	,0) <>	upd_rec.	credit_applications_id	OR
        NVL(fnd.offer_id	,0) <>	upd_rec.	offer_id	OR
        NVL(fnd.counter_offer	,0) <>	upd_rec.	counter_offer	OR
        NVL(fnd.product_purpose	,0) <>	upd_rec.	product_purpose	OR
        NVL(fnd.product_amount	,0) <>	upd_rec.	product_amount	OR
        NVL(fnd.termin_months	,0) <>	upd_rec.	termin_months	OR
        NVL(fnd.total_finance_charge_amount	,0) <>	upd_rec.	total_finance_charge_amount	OR
        NVL(fnd.total_payment_amount	,0) <>	upd_rec.	total_payment_amount	OR
        NVL(fnd.product_name	,0) <>	upd_rec.	product_name	OR
        NVL(fnd.product_category	,0) <>	upd_rec.	product_category	OR
        NVL(fnd.product	,0) <>	upd_rec.	product	OR
        NVL(fnd.interest_rate_percent	,0) <>	upd_rec.	interest_rate_percent	OR
        NVL(fnd.status	,0) <>	upd_rec.	status	OR
        NVL(fnd.product_id	,0) <>	upd_rec.	product_id	OR
        NVL(fnd.initiation_fee_selected	,0) <>	upd_rec.	initiation_fee_selected	OR
        NVL(fnd.product_decision_status	,0) <>	upd_rec.	product_decision_status	OR
        NVL(fnd.reason_code	,0) <>	upd_rec.	reason_code	OR
        NVL(fnd.decline_overide_selected	,0) <>	upd_rec.	decline_overide_selected	OR
        NVL(fnd.decision_product_name	,0) <>	upd_rec.	decision_product_name	OR
        NVL(fnd.channel	,0) <>	upd_rec.	channel	OR
        NVL(fnd.strategy	,0) <>	upd_rec.	strategy	OR
        NVL(fnd.term_product_name	,0) <>	upd_rec.	term_product_name	OR
        NVL(fnd.term_product_subtype	,0) <>	upd_rec.	term_product_subtype	OR
        NVL(fnd.term	,0) <>	upd_rec.	term	OR
        NVL(fnd.term_credit_limit	,0) <>	upd_rec.	term_credit_limit	OR
        NVL(fnd.affordability_product_name	,0) <>	upd_rec.	affordability_product_name	OR
        NVL(fnd.affordability_product_subtype	,0) <>	upd_rec.	affordability_product_subtype	OR
        NVL(fnd.affordability_term	,0) <>	upd_rec.	affordability_term	OR
        NVL(fnd.affordability_limit_amount	,0) <>	upd_rec.	affordability_limit_amount	OR
        NVL(fnd.affordable_monthly_repayment	,0) <>	upd_rec.	affordable_monthly_repayment	OR
        NVL(fnd.disposable_income	,0) <>	upd_rec.	disposable_income	OR
        NVL(fnd.net_income	,0) <>	upd_rec.	net_income	OR
        NVL(fnd.risk_based_product_name	,0) <>	upd_rec.	risk_based_product_name	OR
        NVL(fnd.risk_based_term	,0) <>	upd_rec.	risk_based_term	OR
        NVL(fnd.risk_based_amount	,0) <>	upd_rec.	risk_based_amount	OR
        NVL(fnd.risk_category	,0) <>	upd_rec.	risk_category	OR
        NVL(fnd.matrix_decision	,0) <>	upd_rec.	matrix_decision	OR
        NVL(fnd.product_repayment_factor	,0) <>	upd_rec.	product_repayment_factor	OR
        NVL(fnd.maximum_term	,0) <>	upd_rec.	maximum_term	OR
        NVL(fnd.application_score	,0) <>	upd_rec.	application_score	OR
        NVL(fnd.qualification_product_subtype	,0) <>	upd_rec.	qualification_product_subtype	OR
        NVL(fnd.qualification_logo	,0) <>	upd_rec.	qualification_logo	OR
        NVL(fnd.qualification_org	,0) <>	upd_rec.	qualification_org	OR
        NVL(fnd.qualification_min_lending_amt	,0) <>	upd_rec.	qualification_min_lending_amt	OR
        NVL(fnd.qualification_scrooge_ind	,0) <>	upd_rec.	qualification_scrooge_ind	OR
        NVL(fnd.possible_offers_product_name	,0) <>	upd_rec.	possible_offers_product_name	OR
        NVL(fnd.possible_offers_product_stype	,0) <>	upd_rec.	possible_offers_product_stype	OR
        NVL(fnd.possible_offers_credit_limit	,0) <>	upd_rec.	possible_offers_credit_limit	OR
        NVL(fnd.possible_offers_term	,0) <>	upd_rec.	possible_offers_term	OR
        NVL(fnd.proof_req_pct	,0) <>	upd_rec.	proof_req_pct	OR
        NVL(fnd.poi_required_ind	,0) <>	upd_rec.	poi_required_ind	OR
        NVL(fnd.fica_required_ind	,0) <>	upd_rec.	fica_required_ind	OR
        NVL(fnd.interest_rate	,0) <>	upd_rec.	interest_rate	OR
        NVL(fnd.monthly_fee	,0) <>	upd_rec.	monthly_fee	OR
        NVL(fnd.initiation_fee	,0) <>	upd_rec.	initiation_fee	OR
        NVL(fnd.face_to_face_delivery_fee	,0) <>	upd_rec.	face_to_face_delivery_fee	OR
        NVL(fnd.offer_product_selected	,0) <>	upd_rec.	offer_product_selected	OR
        NVL(fnd.possible_offers_pct_id	,0) <>	upd_rec.	possible_offers_pct_id	OR
        NVL(fnd.insurance_type	,0) <>	upd_rec.	insurance_type	OR
        NVL(fnd.insurance_premium	,0) <>	upd_rec.	insurance_premium	OR
        NVL(fnd.payment_frequency	,0) <>	upd_rec.	payment_frequency	OR
        NVL(fnd.max_age_insurance	,0) <>	upd_rec.	max_age_insurance	OR
        NVL(fnd.min_age_insurance	,0) <>	upd_rec.	min_age_insurance	OR
        NVL(fnd.vat_percentage	,0) <>	upd_rec.	vat_percentage	OR
        NVL(fnd.admin_percentage	,0) <>	upd_rec.	admin_percentage	OR
        NVL(fnd.recalc_offer_only_indicator	,0) <>	upd_rec.	recalc_offer_only_indicator	OR
        NVL(fnd.cr_ext_init_fee_selected	,0) <>	upd_rec.	cr_ext_init_fee_selected	OR
        NVL(fnd.cr_ext_init_drawdown_amount	,0) <>	upd_rec.	cr_ext_init_drawdown_amount	OR
        NVL(fnd.credit_ext_appeal_flag	,0) <>	upd_rec.	credit_ext_appeal_flag	OR
        NVL(fnd.credit_ext_customer_accepted	,0) <>	upd_rec.	credit_ext_customer_accepted	OR
        NVL(fnd.credit_ext_pct_id	,0) <>	upd_rec.	credit_ext_pct_id	OR
        NVL(fnd.credit_ext_contract_concluded	,0) <>	upd_rec.	credit_ext_contract_concluded	OR
        NVL(fnd.fee_borrower_basic	,0) <>	upd_rec.	fee_borrower_basic	OR
        NVL(fnd.fee_borrower_comprehensive	,0) <>	upd_rec.	fee_borrower_comprehensive	OR
        NVL(fnd.fee_borrower_death	,0) <>	upd_rec.	fee_borrower_death	OR
        NVL(fnd.fee_partner_basic	,0) <>	upd_rec.	fee_partner_basic	OR
        NVL(fnd.fee_partner_comprehensive	,0) <>	upd_rec.	fee_partner_comprehensive	OR
        NVL(fnd.fee_partner_death	,0) <>	upd_rec.	fee_partner_death	OR
        NVL(fnd.borrower_balance_protection	,0) <>	upd_rec.	borrower_balance_protection	OR
        NVL(fnd.partner_balance_protection	,0) <>	upd_rec.	partner_balance_protection	OR
        NVL(fnd.borrower_comprehensive	,0) <>	upd_rec.	borrower_comprehensive	OR
        NVL(fnd.partner_comprehensive	,0) <>	upd_rec.	partner_comprehensive	OR
        NVL(fnd.borrower_death	,0) <>	upd_rec.	borrower_death	OR
        NVL(fnd.partner_death	,0) <>	upd_rec.	partner_death	OR
 --       NVL(fnd.monthly_insurance_amount	,0) <>	upd_rec.	monthly_insurance_amount	OR
        NVL(fnd.admin_fee	,0) <>	upd_rec.	admin_fee	OR
        NVL(fnd.repayment_method	,0) <>	upd_rec.	repayment_method	OR
        NVL(fnd.bank_name	,0) <>	upd_rec.	bank_name	OR
        NVL(fnd.branch_name	,0) <>	upd_rec.	branch_name	OR
        NVL(fnd.branch_code	,0) <>	upd_rec.	branch_code	OR
        NVL(fnd.bank_account_number	,0) <>	upd_rec.	bank_account_number	OR
        NVL(fnd.type_of_account	,0) <>	upd_rec.	type_of_account	OR
        NVL(fnd.account_holder_name	,0) <>	upd_rec.	account_holder_name	OR
        NVL(fnd.account_age_years	,0) <>	upd_rec.	account_age_years	OR
        NVL(fnd.account_age_months	,0) <>	upd_rec.	account_age_months	OR
        NVL(fnd.debit_order_date	,NULL) <>	upd_rec.	debit_order_date	OR
        NVL(fnd.repayment_percentage	,0) <>	upd_rec.	repayment_percentage	OR
        NVL(fnd.delivery_type	,0) <>	upd_rec.	delivery_type	OR
        NVL(fnd.delivery_store	,0) <>	upd_rec.	delivery_store	OR
        NVL(fnd.delivery_charge	,0) <>	upd_rec.	delivery_charge	OR
        NVL(fnd.which_address	,0) <>	upd_rec.	which_address	OR
        NVL(fnd.confirmation	,0) <>	upd_rec.	confirmation	OR
        NVL(fnd.read_tc	,0) <>	upd_rec.	read_tc	OR
        NVL(fnd.agree_contract	,0) <>	upd_rec.	agree_contract	OR
        NVL(fnd.tc_delivery_channel	,0) <>	upd_rec.	tc_delivery_channel	OR
        NVL(fnd.contract_delivery_channel	,0) <>	upd_rec.	contract_delivery_channel	OR
        NVL(fnd.accepted	,0) <>	upd_rec.	accepted	OR
        NVL(fnd.pl_contract_recorded_telep	,0) <>	upd_rec.	pl_contract_recorded_telep	OR
        NVL(fnd.sign_contract	,0) <>	upd_rec.	sign_contract	OR
        NVL(fnd.qualification_decline_flag	,0) <>	upd_rec.	qualification_decline_flag	OR
        NVL(fnd.eligibility_decline_flag	,0) <>	upd_rec.	eligibility_decline_flag OR
        NVL(fnd.cancellation_reason	,0) <>	upd_rec.	cancellation_reason OR
        NVL(fnd.insurance_bpi_consent	,0) <>	upd_rec.	insurance_bpi_consent OR
        NVL(fnd.monthly_insurance_fee	,0) <>	upd_rec.	monthly_insurance_fee OR
        NVL(fnd.credit_limit_increase_selected	,0) <>	upd_rec.	credit_limit_increase_selected OR
        NVL(fnd.wupdate	,0) <>	upd_rec.	wupdate OR
        NVL(fnd.wupdate_contact_number	,0) <>	upd_rec.	wupdate_contact_number OR
        NVL(fnd.pl_customer_xds_verification	,0) <>	upd_rec.	pl_customer_xds_verification OR
        NVL(fnd.pl_customer_id_verification	,0) <>	upd_rec.	pl_customer_id_verification   );    
    
    g_recs_updated                            := g_recs_updated + 1;
  END LOOP;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG UPDATE - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG UPDATE - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_update;
--**************************************************************************************************
-- Send records to hospital where not valid
--**************************************************************************************************
PROCEDURE flagged_records_hospital
AS
BEGIN
  INSERT /*+ APPEND parallel (hsp,2) */
  INTO stg_om4_cr_detail_hsp hsp
  SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
    cpy.sys_source_batch_id,
    cpy.sys_source_sequence_no,
    sysdate,
    'Y',
    'DWH',
    cpy.sys_middleware_batch_id,
    'VALIDATION FAIL - REFERENCIAL ERROR',
    cpy.credit_details_id	,
    cpy.credit_applications_id	,
    cpy.offer_id	,
    cpy.counter_offer	,
    cpy.product_purpose	,
    cpy.product_amount	,
    cpy.termin_months	,
    cpy.total_finance_charge_amount	,
    cpy.total_payment_amount	,
    cpy.product_name	,
    cpy.product_category	,
    cpy.product	,
    cpy.interest_rate_percent	,
    cpy.status	,
    cpy.product_id	,
    cpy.initiation_fee_selected	,
    cpy.product_decision_status	,
    cpy.reason_code	,
    cpy.decline_overide_selected	,
    cpy.decision_product_name	,
    cpy.channel	,
    cpy.strategy	,
    cpy.term_product_name	,
    cpy.term_product_subtype	,
    cpy.term	,
    cpy.term_credit_limit	,
    cpy.affordability_product_name	,
    cpy.affordability_product_subtype	,
    cpy.affordability_term	,
    cpy.affordability_limit_amount	,
    cpy.affordable_monthly_repayment	,
    cpy.disposable_income	,
    cpy.net_income	,
    cpy.risk_based_product_name	,
    cpy.risk_based_term	,
    cpy.risk_based_amount	,
    cpy.risk_category	,
    cpy.matrix_decision	,
    cpy.product_repayment_factor	,
    cpy.maximum_term	,
    cpy.application_score	,
    cpy.qualification_product_subtype	,
    cpy.qualification_logo	,
    cpy.qualification_org	,
    cpy.qualification_min_lending_amt	,
    cpy.qualification_scrooge_ind	,
    cpy.possible_offers_product_name	,
    cpy.possible_offers_product_stype	,
    cpy.possible_offers_credit_limit	,
    cpy.possible_offers_term	,
    cpy.proof_req_pct	,
    cpy.poi_required_ind	,
    cpy.fica_required_ind	,
    cpy.interest_rate	,
    cpy.monthly_fee	,
    cpy.initiation_fee	,
    cpy.face_to_face_delivery_fee	,
    cpy.offer_product_selected	,
    cpy.possible_offers_pct_id	,
    cpy.insurance_type	,
    cpy.insurance_premium	,
    cpy.payment_frequency	,
    cpy.max_age_insurance	,
    cpy.min_age_insurance	,
    cpy.vat_percentage	,
    cpy.admin_percentage	,
    cpy.recalc_offer_only_indicator	,
    cpy.cr_ext_init_fee_selected	,
    cpy.cr_ext_init_drawdown_amount	,
    cpy.credit_ext_appeal_flag	,
    cpy.credit_ext_customer_accepted	,
    cpy.credit_ext_pct_id	,
    cpy.credit_ext_contract_concluded	,
    cpy.fee_borrower_basic	,
    cpy.fee_borrower_comprehensive	,
    cpy.fee_borrower_death	,
    cpy.fee_partner_basic	,
    cpy.fee_partner_comprehensive	,
    cpy.fee_partner_death	,
    cpy.borrower_balance_protection	,
    cpy.partner_balance_protection	,
    cpy.borrower_comprehensive	,
    cpy.partner_comprehensive	,
    cpy.borrower_death	,
    cpy.partner_death	,
--    cpy.monthly_insurance_amount	,
    cpy.admin_fee	,
    cpy.repayment_method	,
    cpy.bank_name	,
    cpy.branch_name	,
    cpy.branch_code	,
    cpy.bank_account_number	,
    cpy.type_of_account	,
    cpy.account_holder_name	,
    cpy.account_age_years	,
    cpy.account_age_months	,
    cpy.debit_order_date	,
    cpy.repayment_percentage	,
    cpy.delivery_type	,
    cpy.delivery_store	,
    cpy.delivery_charge	,
    cpy.which_address	,
    cpy.confirmation	,
    cpy.read_tc	,
    cpy.agree_contract	,
    cpy.tc_delivery_channel	,
    cpy.contract_delivery_channel	,
    cpy.accepted	,
    cpy.pl_contract_recorded_telep	,
    cpy.sign_contract	,
    cpy.qualification_decline_flag	,
    cpy.eligibility_decline_flag	,
    cpy.cancellation_reason	,
    cpy.insurance_bpi_consent	,
    cpy.monthly_insurance_fee	,
    cpy.credit_limit_increase_selected	,
    cpy.wupdate	,
    cpy.wupdate_contact_number	,
    cpy.pl_customer_xds_verification	,
    cpy.pl_customer_id_verification	
  FROM stg_om4_cr_detail cpy
  WHERE
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
  g_recs_hospital   := g_recs_hospital + sql%rowcount;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG HOSPITAL - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG HOSPITAL - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_hospital;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Call the bulk routines
  --**************************************************************************************************
  l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  remove_duplicates;
  SELECT COUNT(*)
  INTO g_recs_read
  FROM stg_om4_cr_detail
  WHERE sys_process_code = 'N';
  l_text                := 'BULK UPDATE STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  flagged_records_update;
  l_text := 'BULK INSERT STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  flagged_records_insert;
  --********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************
  --    l_text := 'BULK HOSPITALIZATION STARTED AT '||
  --    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --    flagged_records_hospital;
  --    Taken out for better performance --------------------
  --    update stg_absa_crd_acc_dly
  --    set    sys_process_code = 'Y';
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'DUPLICATE REMOVED '||g_recs_duplicate;                              --Bulk load--
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); --Bulk Load--
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  IF g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital THEN
    l_text       := 'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    p_success := false;
    l_message := 'ERROR - Record counts do not balance see log file';
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
    raise_application_error (-20246,'Record count error - see log files');
  END IF;
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  RAISE;
END WH_FND_WFS_156HSP;
