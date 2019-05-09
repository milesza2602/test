--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_162U_20161027
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_162U_20161027" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- Date:        June 2015
  -- Author:      Jerome Appollis
  -- Purpose:     Create fnd_wfs_om4_personal_applicant table in the foundation layer
  --              with input ex staging table from WFS.
  -- Tables:      Input  - stg_om4_personal_applicant_cpy
  --              Output - fnd_wfs_om4_personal_applicant
  -- Packages:    constants, dwh_log, dwh_valid
  --
  -- Maintenance:
  --  2016-03-11 N Chauhan - added 14 more fields for NCA compliance.
  --  2016-03-15 N Chauhan - added 4 more fields to take off dependency on Informix.
  --   Add a comment to test check-in
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
  
  g_pa_id stg_om4_personal_applicant_cpy.personal_applicant_id%type;
  
  
  g_date DATE := TRUNC(sysdate);
  
  L_MESSAGE SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_WFS_162U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_TEXT SYS_DWH_LOG.LOG_TEXT%TYPE ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD WFS PERSONAL APPLICANT DATA';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  CURSOR stg_dup
IS
    SELECT *
    FROM stg_om4_personal_applicant_cpy
    WHERE (personal_applicant_id) IN
      (SELECT personal_applicant_id
      FROM stg_om4_personal_applicant_cpy
      GROUP BY personal_applicant_id
      HAVING COUNT(*) > 1
      )
  ORDER BY personal_applicant_id,
    sys_source_batch_id DESC ,
    sys_source_sequence_no DESC;
  CURSOR c_stg_wfs_om4_pa_dly
  IS
    SELECT
      /*+ FULL(stg)  parallel (stg,2) */
      stg.*
    FROM stg_om4_personal_applicant_cpy stg,
      fnd_wfs_om4_personal_applicant fnd
    WHERE stg.personal_applicant_id = fnd.personal_applicant_id
    AND stg.sys_process_code    = 'N'
      -- Any further validation goes in here - like xxx.ind in (0,1) ---
    ORDER BY stg.personal_applicant_id,
      stg.sys_source_batch_id,
      stg.sys_source_sequence_no ;
  --**************************************************************************************************
  -- Eliminate duplicates on the very rare occasion they may be present
  --**************************************************************************************************
PROCEDURE remove_duplicates
AS
BEGIN
  g_pa_id := 0;
  FOR dupp_record IN stg_dup
  LOOP
    IF dupp_record.personal_applicant_id = g_pa_id THEN
      UPDATE stg_om4_personal_applicant_cpy stg
      SET sys_process_code       = 'D'
      WHERE sys_source_batch_id  = dupp_record.sys_source_batch_id
      AND sys_source_sequence_no = dupp_record.sys_source_sequence_no;
      g_recs_duplicate          := g_recs_duplicate + 1;
    END IF;
    g_pa_id := dupp_record.personal_applicant_id;
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
  INTO fnd_wfs_om4_personal_applicant fnd
  SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
      cpy.	personal_applicant_id	,
      cpy.	credit_applications_id	,
      cpy.	personal_applicant_type	,
      cpy.	existing_customer	,
      cpy.	credit_report_purchase_flag	,
      cpy.	no_of_cra_reports_purchased	,
      cpy.	score_required	,
      cpy.	has_cheque_account	,
      cpy.	has_savings_account	,
      cpy.	has_unlisted_cheque_account	,
      cpy.	has_unlisted_savings_account	,
      cpy.	residential_id	,
      cpy.	residential_type	,
      cpy.	residential_address_line1	,
      cpy.	residential_city	,
      cpy.	residential_province	,
      cpy.	residential_postal_code	,
      cpy.	residential_country	,
      cpy.	residential_country_code	,
      cpy.	residential_suburb	,
      cpy.	birth_date	,
      cpy.	number_of_dependents	,
      cpy.	marital_status	,
      cpy.	phone_number	,
      cpy.	customer_reference_id	,
      cpy.	customer_reference_type	,
      cpy.	identification_number	,
      cpy.	issue_date	,
      cpy.	title	,
      cpy.	full_name	,
      cpy.	initials	,
      cpy.	surname	,
      cpy.	postal_address_line_1	,
      cpy.	postal_city	,
      cpy.	postal_province	,
      cpy.	postal_postal_code	,
      cpy.	postal_country	,
      cpy.	postal_suburb	,
      cpy.	employment_id	,
      cpy.	our_employee	,
      cpy.	work_phone_number	,
      cpy.	subject_to_regulation	,
      cpy.	occupation_id	,
      cpy.	occupation_type	,
      cpy.	position	,
      cpy.	employment_type	,
      cpy.	is_self_employed	,
      cpy.	end_date	,
      cpy.	employer_name	,
      cpy.	industry_other	,
      cpy.	employee_number	,
      cpy.	total_gross_monthly_income	,
      cpy.	net_monthly_income	,
      cpy.	total_monthly_payments	,
      cpy.	monthly_housing_expense 	,
      cpy.	net_disposable_income	,
      cpy.	debt_counceling	,
      cpy.	product_name	,
      cpy.	card_printed	,
      cpy.	card_validated	,
      cpy.	bureau_consent	,
      cpy.	spouse_consent	,
      cpy.	nationality	,
      cpy.	gender	,
      cpy.	phone_type	,
      cpy.	home_phone_number	,
      cpy.	existing_customer_isc_selected	,
      cpy.	existing_customer_cc_selected	,
      cpy.	existing_customer_pl_selected	,
      cpy.	mailing_address_same_as_res	,
      cpy.	customer_number_crm	,
      cpy.	source_of_funds	,
      cpy.	envelope_reference_number	,
      cpy.	country_of_residence	,
      cpy.	not_contacted_for_marketing	,
      g_date AS last_updated_date ,
      cpy. monthly_mortgage_payment ,
      cpy. monthly_rental_payment ,
      cpy. monthly_maintenance_expenses ,
      cpy. poi_consent ,
      cpy. poi_bank_det_bank_name ,
      cpy. poi_bank_det_branch_name ,
      cpy. poi_bank_det_branch_code ,
      cpy. poi_bank_det_bank_acc_no ,
      cpy. poi_bank_det_bank_type_of_acc ,
      cpy. poi_bank_det_acc_holder_name ,
      cpy. poi_bank_det_acc_age_years ,
      cpy. poi_bank_det_acc_age_months ,
      cpy. email_address ,
      cpy. total_asset_balance ,
      cpy. partner_initials ,
      cpy. partner_surname ,
      cpy. partner_dob ,
      cpy. partner_identity_no

      
  FROM stg_om4_personal_applicant_cpy cpy
  WHERE NOT EXISTS
    (SELECT
      /*+ nl_aj */
      *
    FROM fnd_wfs_om4_personal_applicant
    WHERE personal_applicant_id = cpy.personal_applicant_id
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
  FOR upd_rec IN c_stg_wfs_om4_pa_dly
  LOOP    
    UPDATE fnd_wfs_om4_personal_applicant fnd
    SET 
      fnd.	credit_applications_id	=	upd_rec.	credit_applications_id	,
      fnd.	personal_applicant_type	=	upd_rec.	personal_applicant_type	,
      fnd.	existing_customer	=	upd_rec.	existing_customer	,
      fnd.	credit_report_purchase_flag	=	upd_rec.	credit_report_purchase_flag	,
      fnd.	no_of_cra_reports_purchased	=	upd_rec.	no_of_cra_reports_purchased	,
      fnd.	score_required	=	upd_rec.	score_required	,
      fnd.	has_cheque_account	=	upd_rec.	has_cheque_account	,
      fnd.	has_savings_account	=	upd_rec.	has_savings_account	,
      fnd.	has_unlisted_cheque_account	=	upd_rec.	has_unlisted_cheque_account	,
      fnd.	has_unlisted_savings_account	=	upd_rec.	has_unlisted_savings_account	,
      fnd.	residential_id	=	upd_rec.	residential_id	,
      fnd.	residential_type	=	upd_rec.	residential_type	,
      fnd.	residential_address_line1	=	upd_rec.	residential_address_line1	,
      fnd.	residential_city	=	upd_rec.	residential_city	,
      fnd.	residential_province	=	upd_rec.	residential_province	,
      fnd.	residential_postal_code	=	upd_rec.	residential_postal_code	,
      fnd.	residential_country	=	upd_rec.	residential_country	,
      fnd.	residential_country_code	=	upd_rec.	residential_country_code	,
      fnd.	residential_suburb	=	upd_rec.	residential_suburb	,
      fnd.	birth_date	=	upd_rec.	birth_date	,
      fnd.	number_of_dependents	=	upd_rec.	number_of_dependents	,
      fnd.	marital_status	=	upd_rec.	marital_status	,
      fnd.	phone_number	=	upd_rec.	phone_number	,
      fnd.	customer_reference_id	=	upd_rec.	customer_reference_id	,
      fnd.	customer_reference_type	=	upd_rec.	customer_reference_type	,
      fnd.	identification_number	=	upd_rec.	identification_number	,
      fnd.	issue_date	=	upd_rec.	issue_date	,
      fnd.	title	=	upd_rec.	title	,
      fnd.	full_name	=	upd_rec.	full_name	,
      fnd.	initials	=	upd_rec.	initials	,
      fnd.	surname	=	upd_rec.	surname	,
      fnd.	postal_address_line_1	=	upd_rec.	postal_address_line_1	,
      fnd.	postal_city	=	upd_rec.	postal_city	,
      fnd.	postal_province	=	upd_rec.	postal_province	,
      fnd.	postal_postal_code	=	upd_rec.	postal_postal_code	,
      fnd.	postal_country	=	upd_rec.	postal_country	,
      fnd.	postal_suburb	=	upd_rec.	postal_suburb	,
      fnd.	employment_id	=	upd_rec.	employment_id	,
      fnd.	our_employee	=	upd_rec.	our_employee	,
      fnd.	work_phone_number	=	upd_rec.	work_phone_number	,
      fnd.	subject_to_regulation	=	upd_rec.	subject_to_regulation	,
      fnd.	occupation_id	=	upd_rec.	occupation_id	,
      fnd.	occupation_type	=	upd_rec.	occupation_type	,
      fnd.	position	=	upd_rec.	position	,
      fnd.	employment_type	=	upd_rec.	employment_type	,
      fnd.	is_self_employed	=	upd_rec.	is_self_employed	,
      fnd.	end_date	=	upd_rec.	end_date	,
      fnd.	employer_name	=	upd_rec.	employer_name	,
      fnd.	industry_other	=	upd_rec.	industry_other	,
      fnd.	employee_number	=	upd_rec.	employee_number	,
      fnd.	total_gross_monthly_income	=	upd_rec.	total_gross_monthly_income	,
      fnd.	net_monthly_income	=	upd_rec.	net_monthly_income	,
      fnd.	total_monthly_payments	=	upd_rec.	total_monthly_payments	,
      fnd.	monthly_housing_expense	=	upd_rec.	monthly_housing_expense	,
      fnd.	net_disposable_income	=	upd_rec.	net_disposable_income	,
      fnd.	debt_counceling	=	upd_rec.	debt_counceling	,
      fnd.	product_name	=	upd_rec.	product_name	,
      fnd.	card_printed	=	upd_rec.	card_printed	,
      fnd.	card_validated	=	upd_rec.	card_validated	,
      fnd.	bureau_consent	=	upd_rec.	bureau_consent	,
      fnd.	spouse_consent	=	upd_rec.	spouse_consent	,
      fnd.	nationality	=	upd_rec.	nationality	,
      fnd.	gender	=	upd_rec.	gender	,
      fnd.	phone_type	=	upd_rec.	phone_type	,
      fnd.	home_phone_number	=	upd_rec.	home_phone_number	,
      fnd.	existing_customer_isc_selected	=	upd_rec.	existing_customer_isc_selected	,
      fnd.	existing_customer_cc_selected	=	upd_rec.	existing_customer_cc_selected	,
      fnd.	existing_customer_pl_selected	=	upd_rec.	existing_customer_pl_selected	,
      fnd.	mailing_address_same_as_res	=	upd_rec.	mailing_address_same_as_res	,
      fnd.	customer_number_crm	=	upd_rec.	customer_number_crm	,
      fnd.	source_of_funds	=	upd_rec.	source_of_funds	,
      fnd.	envelope_reference_number	=	upd_rec.	envelope_reference_number	,
      fnd.	country_of_residence	=	upd_rec.	country_of_residence	,
      fnd.	not_contacted_for_marketing	=	upd_rec.	not_contacted_for_marketing	,
      fnd.last_updated_date                 = g_date ,
      fnd. monthly_mortgage_payment = upd_rec. monthly_mortgage_payment ,
      fnd. monthly_rental_payment = upd_rec. monthly_rental_payment ,
      fnd. monthly_maintenance_expenses = upd_rec. monthly_maintenance_expenses ,
      fnd. poi_consent = upd_rec. poi_consent ,
      fnd. poi_bank_det_bank_name = upd_rec. poi_bank_det_bank_name ,
      fnd. poi_bank_det_branch_name = upd_rec. poi_bank_det_branch_name ,
      fnd. poi_bank_det_branch_code = upd_rec. poi_bank_det_branch_code ,
      fnd. poi_bank_det_bank_acc_no = upd_rec. poi_bank_det_bank_acc_no ,
      fnd. poi_bank_det_bank_type_of_acc = upd_rec. poi_bank_det_bank_type_of_acc ,
      fnd. poi_bank_det_acc_holder_name = upd_rec. poi_bank_det_acc_holder_name ,
      fnd. poi_bank_det_acc_age_years = upd_rec. poi_bank_det_acc_age_years ,
      fnd. poi_bank_det_acc_age_months = upd_rec. poi_bank_det_acc_age_months ,
      fnd. email_address = upd_rec. email_address ,
      fnd. total_asset_balance = upd_rec. total_asset_balance ,
      fnd. partner_initials = upd_rec. partner_initials ,
      fnd. partner_surname = upd_rec. partner_surname ,
      fnd. partner_dob = upd_rec. partner_dob ,
      fnd. partner_identity_no = upd_rec. partner_identity_no


    WHERE fnd.personal_applicant_id         = upd_rec.personal_applicant_id
    AND ( 
      NVL(fnd.	credit_applications_id	,0) <>	upd_rec.	credit_applications_id	OR
      NVL(fnd.	personal_applicant_type	,0) <>	upd_rec.	personal_applicant_type	OR
      NVL(fnd.	existing_customer	,0) <>	upd_rec.	existing_customer	OR
      NVL(fnd.	credit_report_purchase_flag	,0) <>	upd_rec.	credit_report_purchase_flag	OR
      NVL(fnd.	no_of_cra_reports_purchased	,0) <>	upd_rec.	no_of_cra_reports_purchased	OR
      NVL(fnd.	score_required	,0) <>	upd_rec.	score_required	OR
      NVL(fnd.	has_cheque_account	,0) <>	upd_rec.	has_cheque_account	OR
      NVL(fnd.	has_savings_account	,0) <>	upd_rec.	has_savings_account	OR
      NVL(fnd.	has_unlisted_cheque_account	,0) <>	upd_rec.	has_unlisted_cheque_account	OR
      NVL(fnd.	has_unlisted_savings_account	,0) <>	upd_rec.	has_unlisted_savings_account	OR
      NVL(fnd.	residential_id	,0) <>	upd_rec.	residential_id	OR
      NVL(fnd.	residential_type	,0) <>	upd_rec.	residential_type	OR
      NVL(fnd.	residential_address_line1	,0) <>	upd_rec.	residential_address_line1	OR
      NVL(fnd.	residential_city	,0) <>	upd_rec.	residential_city	OR
      NVL(fnd.	residential_province	,0) <>	upd_rec.	residential_province	OR
      NVL(fnd.	residential_postal_code	,0) <>	upd_rec.	residential_postal_code	OR
      NVL(fnd.	residential_country	,0) <>	upd_rec.	residential_country	OR
      NVL(fnd.	residential_country_code	,0) <>	upd_rec.	residential_country_code	OR
      NVL(fnd.	residential_suburb	,0) <>	upd_rec.	residential_suburb	OR
      NVL(fnd.	birth_date	,NULL) <>	upd_rec.	birth_date	OR
      NVL(fnd.	number_of_dependents	,0) <>	upd_rec.	number_of_dependents	OR
      NVL(fnd.	marital_status	,0) <>	upd_rec.	marital_status	OR
      NVL(fnd.	phone_number	,0) <>	upd_rec.	phone_number	OR
      NVL(fnd.	customer_reference_id	,0) <>	upd_rec.	customer_reference_id	OR
      NVL(fnd.	customer_reference_type	,0) <>	upd_rec.	customer_reference_type	OR
      NVL(fnd.	identification_number	,0) <>	upd_rec.	identification_number	OR
      NVL(fnd.	issue_date	,NULL) <>	upd_rec.	issue_date	OR
      NVL(fnd.	title	,0) <>	upd_rec.	title	OR
      NVL(fnd.	full_name	,0) <>	upd_rec.	full_name	OR
      NVL(fnd.	initials	,0) <>	upd_rec.	initials	OR
      NVL(fnd.	surname	,0) <>	upd_rec.	surname	OR
      NVL(fnd.	postal_address_line_1	,0) <>	upd_rec.	postal_address_line_1	OR
      NVL(fnd.	postal_city	,0) <>	upd_rec.	postal_city	OR
      NVL(fnd.	postal_province	,0) <>	upd_rec.	postal_province	OR
      NVL(fnd.	postal_postal_code	,0) <>	upd_rec.	postal_postal_code	OR
      NVL(fnd.	postal_country	,0) <>	upd_rec.	postal_country	OR
      NVL(fnd.	postal_suburb	,0) <>	upd_rec.	postal_suburb	OR
      NVL(fnd.	employment_id	,0) <>	upd_rec.	employment_id	OR
      NVL(fnd.	our_employee	,0) <>	upd_rec.	our_employee	OR
      NVL(fnd.	work_phone_number	,0) <>	upd_rec.	work_phone_number	OR
      NVL(fnd.	subject_to_regulation	,0) <>	upd_rec.	subject_to_regulation	OR
      NVL(fnd.	occupation_id	,0) <>	upd_rec.	occupation_id	OR
      NVL(fnd.	occupation_type	,0) <>	upd_rec.	occupation_type	OR
      NVL(fnd.	position	,0) <>	upd_rec.	position	OR
      NVL(fnd.	employment_type	,0) <>	upd_rec.	employment_type	OR
      NVL(fnd.	is_self_employed	,0) <>	upd_rec.	is_self_employed	OR
      NVL(fnd.	end_date	,NULL) <>	upd_rec.	end_date	OR
      NVL(fnd.	employer_name	,0) <>	upd_rec.	employer_name	OR
      NVL(fnd.	industry_other	,0) <>	upd_rec.	industry_other	OR
      NVL(fnd.	employee_number	,0) <>	upd_rec.	employee_number	OR
      NVL(fnd.	total_gross_monthly_income	,0) <>	upd_rec.	total_gross_monthly_income	OR
      NVL(fnd.	net_monthly_income	,0) <>	upd_rec.	net_monthly_income	OR
      NVL(fnd.	total_monthly_payments	,0) <>	upd_rec.	total_monthly_payments	OR
      NVL(fnd.	monthly_housing_expense	,0) <>	upd_rec.	monthly_housing_expense	OR
      NVL(fnd.	net_disposable_income	,0) <>	upd_rec.	net_disposable_income	OR
      NVL(fnd.	debt_counceling	,0) <>	upd_rec.	debt_counceling	OR
      NVL(fnd.	product_name	,0) <>	upd_rec.	product_name	OR
      NVL(fnd.	card_printed	,0) <>	upd_rec.	card_printed	OR
      NVL(fnd.	card_validated	,0) <>	upd_rec.	card_validated	OR
      NVL(fnd.	bureau_consent	,0) <>	upd_rec.	bureau_consent	OR
      NVL(fnd.	spouse_consent	,0) <>	upd_rec.	spouse_consent	OR
      NVL(fnd.	nationality	,0) <>	upd_rec.	nationality	OR
      NVL(fnd.	gender	,0) <>	upd_rec.	gender	OR
      NVL(fnd.	phone_type	,0) <>	upd_rec.	phone_type	OR
      NVL(fnd.	home_phone_number	,0) <>	upd_rec.	home_phone_number	OR
      NVL(fnd.	existing_customer_isc_selected	,0) <>	upd_rec.	existing_customer_isc_selected	OR
      NVL(fnd.	existing_customer_cc_selected	,0) <>	upd_rec.	existing_customer_cc_selected	OR
      NVL(fnd.	existing_customer_pl_selected	,0) <>	upd_rec.	existing_customer_pl_selected	OR
      NVL(fnd.	mailing_address_same_as_res	,0) <>	upd_rec.	mailing_address_same_as_res	OR
      NVL(fnd.	customer_number_crm	,0) <>	upd_rec.	customer_number_crm	OR
      NVL(fnd.	source_of_funds	,0) <>	upd_rec.	source_of_funds	OR
      NVL(fnd.	envelope_reference_number	,0) <>	upd_rec.	envelope_reference_number	OR
      NVL(fnd.	country_of_residence	,0) <>	upd_rec.	country_of_residence	OR
      NVL(fnd.	not_contacted_for_marketing	,0) <>	upd_rec.	not_contacted_for_marketing OR
      NVL(fnd. monthly_mortgage_payment,0) <> upd_rec. monthly_mortgage_payment OR
      NVL(fnd. monthly_rental_payment,0) <> upd_rec. monthly_rental_payment OR
      NVL(fnd. monthly_maintenance_expenses,0) <> upd_rec. monthly_maintenance_expenses OR
      NVL(fnd. poi_consent,0) <> upd_rec. poi_consent OR
      NVL(fnd. poi_bank_det_bank_name,0) <> upd_rec. poi_bank_det_bank_name OR
      NVL(fnd. poi_bank_det_branch_name,0) <> upd_rec. poi_bank_det_branch_name OR
      NVL(fnd. poi_bank_det_branch_code,0) <> upd_rec. poi_bank_det_branch_code OR
      NVL(fnd. poi_bank_det_bank_acc_no,0) <> upd_rec. poi_bank_det_bank_acc_no OR
      NVL(fnd. poi_bank_det_bank_type_of_acc,0) <> upd_rec. poi_bank_det_bank_type_of_acc OR
      NVL(fnd. poi_bank_det_acc_holder_name,0) <> upd_rec. poi_bank_det_acc_holder_name OR
      NVL(fnd. poi_bank_det_acc_age_years,0) <> upd_rec. poi_bank_det_acc_age_years OR
      NVL(fnd. poi_bank_det_acc_age_months,0) <> upd_rec. poi_bank_det_acc_age_months OR
      NVL(fnd. email_address,0) <> upd_rec. email_address OR
      NVL(fnd. total_asset_balance,0) <> upd_rec. total_asset_balance OR
      NVL(fnd. partner_initials,0) <> upd_rec. partner_initials OR
      NVL(fnd. partner_surname,0) <> upd_rec. partner_surname OR
      NVL(fnd. partner_dob,0) <> upd_rec. partner_dob OR
      NVL(fnd. partner_identity_no,0) <> upd_rec. partner_identity_no
       
      );
    
    g_recs_updated                              := g_recs_updated + 1;
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
  INTO stg_om4_personal_applicant_hsp hsp
 SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
    cpy.sys_source_batch_id,
    cpy.sys_source_sequence_no,
    sysdate,
    'Y',
    'DWH',
    cpy.sys_middleware_batch_id,
    'VALIDATION FAIL - REFERENCIAL ERROR',
    cpy.	personal_applicant_id	,
    cpy.	credit_applications_id	,
    cpy.	personal_applicant_type	,
    cpy.	existing_customer	,
    cpy.	credit_report_purchase_flag	,
    cpy.	no_of_cra_reports_purchased	,
    cpy.	score_required	,
    cpy.	has_cheque_account	,
    cpy.	has_savings_account	,
    cpy.	has_unlisted_cheque_account	,
    cpy.	has_unlisted_savings_account	,
    cpy.	residential_id	,
    cpy.	residential_type	,
    cpy.	residential_address_line1	,
    cpy.	residential_city	,
    cpy.	residential_province	,
    cpy.	residential_postal_code	,
    cpy.	residential_country	,
    cpy.	residential_country_code	,
    cpy.	residential_suburb	,
    cpy.	birth_date	,
    cpy.	number_of_dependents	,
    cpy.	marital_status	,
    cpy.	phone_number	,
    cpy.	customer_reference_id	,
    cpy.	customer_reference_type	,
    cpy.	identification_number	,
    cpy.	issue_date	,
    cpy.	title	,
    cpy.	full_name	,
    cpy.	initials	,
    cpy.	surname	,
    cpy.	postal_address_line_1	,
    cpy.	postal_city	,
    cpy.	postal_province	,
    cpy.	postal_postal_code	,
    cpy.	postal_country	,
    cpy.	postal_suburb	,
    cpy.	employment_id	,
    cpy.	our_employee	,
    cpy.	work_phone_number	,
    cpy.	subject_to_regulation	,
    cpy.	occupation_id	,
    cpy.	occupation_type	,
    cpy.	position	,
    cpy.	employment_type	,
    cpy.	is_self_employed	,
    cpy.	end_date	,
    cpy.	employer_name	,
    cpy.	industry_other	,
    cpy.	employee_number	,
    cpy.	total_gross_monthly_income	,
    cpy.	net_monthly_income	,
    cpy.	total_monthly_payments	,
    cpy.	monthly_housing_expense	,
    cpy.	net_disposable_income	,
    cpy.	debt_counceling	,
    cpy.	product_name	,
    cpy.	card_printed	,
    cpy.	card_validated	,
    cpy.	bureau_consent	,
    cpy.	spouse_consent	,
    cpy.	nationality	,
    cpy.	gender	,
    cpy.	phone_type	,
    cpy.	home_phone_number	,
    cpy.	existing_customer_isc_selected	,
    cpy.	existing_customer_cc_selected	,
    cpy.	existing_customer_pl_selected	,
    cpy.	mailing_address_same_as_res	,
    cpy.	customer_number_crm	,
    cpy.	source_of_funds	,
    cpy.	envelope_reference_number	,
    cpy.	country_of_residence	,
    cpy.	not_contacted_for_marketing	,
    cpy. monthly_mortgage_payment ,
    cpy. monthly_rental_payment ,
    cpy. monthly_maintenance_expenses ,
    cpy. poi_consent ,
    cpy. poi_bank_det_bank_name ,
    cpy. poi_bank_det_branch_name ,
    cpy. poi_bank_det_branch_code ,
    cpy. poi_bank_det_bank_acc_no ,
    cpy. poi_bank_det_bank_type_of_acc ,
    cpy. poi_bank_det_acc_holder_name ,
    cpy. poi_bank_det_acc_age_years ,
    cpy. poi_bank_det_acc_age_months ,
    cpy. email_address ,
    cpy. total_asset_balance ,
    cpy. partner_initials ,
    cpy. partner_surname ,
    cpy. partner_dob ,
    cpy. partner_identity_no



  FROM stg_om4_personal_applicant_cpy cpy
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
  FROM stg_om4_personal_applicant_cpy
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
  --    update stg_absa_crd_acc_dly_cpy
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
END WH_FND_WFS_162U_20161027;
