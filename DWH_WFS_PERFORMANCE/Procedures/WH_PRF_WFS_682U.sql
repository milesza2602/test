--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_682U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_682U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Description  WFS New Business Report - Detailed applications and their statuses for all applications.
  --  Date:        2018-07-25
  --  Author:      Nhlaka dlamini
  --  Purpose:     Update  WFS_NBR_MAIN_PRODUCT_APPL base temp table in the performance layer
  --               with input ex
  --                    fnd_wfs_om4_application
  --                    fnd_wfs_om4_cr_detail
  --                    fnd_wfs_om4_offer
  --                    fnd_wfs_om4_workflow
  --
  --               THIS JOB RUNS DAILY
  --  Tables:      Input  -
  --                    fnd_wfs_om4_application
  --                    fnd_wfs_om4_cr_detail
  --                    fnd_wfs_om4_offer
  --                    fnd_wfs_om4_workflow
  --               Output - WFS_NBR_MAIN_PRODUCT_APPL
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  2018-07-27 N Dlamini - created based on WH_PRF_WFS_690U.
  --  2018-08-03 N Dlamini - Added standard metadata on procedure.
  --  2018-09-11 N Dlamini - Removed Activations from this procedure and added to WH_PRF_WFS_690 as fix to Activations not matching with Qlikview NBR APP
  --  2018-10-09 N Dlamini - Changed the DEA_IND source table from OM4 Workflow table to OM4 Personal Applicant table as per business request.
  --  2018-11-17 N Dlamini - Added New rule for Product_Offered Column to cater for PH1 Rule change
  -- 
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  -- TEMP_TABLE_DDL  - Variable to hold the DDL of the temp table
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_recs_deleted  INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_sub           INTEGER := 0;
  g_rec_out wfs_product_activation%rowtype;
  g_found      BOOLEAN;
  g_date       DATE := TRUNC(sysdate);
  g_start_week NUMBER ;
  g_end_week   NUMBER ;
  g_yesterday  DATE := TRUNC(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  g_stmt  VARCHAR2(300);
  g_yr_00 NUMBER;
  g_qt_00 NUMBER;
  g_ctas CLOB;
  g_table_exists INTEGER;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WFS_682U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'WFS NBR Main Product Applications';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'WFS_NBR_TMP_MAIN_PRODUCT_APPL Load Started At '|| TO_CHAR(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD TABLE: '||'WFS_NBR_TMP_MAIN_PRODUCT_APPL' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE immediate 'ALTER SESSION ENABLE PARALLEL DML';
  EXECUTE immediate 'TRUNCATE TABLE DWH_WFS_PERFORMANCE.WFS_NBR_TMP_MAIN_PRODUCT_APPL';
  l_text := 'WFS_NBR_TMP_MAIN_PRODUCT_APPL Completed Truncate At '|| TO_CHAR(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  INSERT
    /*+ APPEND */
  INTO dwh_wfs_performance.wfs_nbr_tmp_main_product_appl
    (
      app_number,
      ma_entered_time_stamp,
      ma_app_number,
      split_app,
      customer_application,
      customer_product,
      customer_application_product,
      entered_time_stamp,
      app_date,
      application_agent,
      credit_applications_id,
      credit_application_status,
      app_channel,
      channel,
      campaign_id,
      promotion_id,
      product_name,
      product_amount,
      counter_offer,
      request_type,
      product_purpose,
      sa_id,
      poi_required,
      store_number,
      created_by,
      fee_borrower_comprehensive,
      fee_borrower_basic,
      delivery_type,
      delivery_store,
      risk_category,
      paper_app_created_by_user,
      created_by_user,
      product_selected,
      product_offered,
      product_accepted,
      bonus_product_offered,
      bonus_product_accepted,
      product_complete,
      bonus_product_complete,
      insurance_type,
      bpi_count,
      email_count,
      product_offer_date,
      product_offer_agent,
      product_offer_accept_date,
      product_offer_accept_agent,
      pre_agreement_accept_date,
      pre_agreement_accept_agent,
      dea_ind,
      latest_dea_date,
      dea_consent_agent,
      customer_undecided_date,
      customer_undecided_agent
    )
 WITH fnd_wfs_tmp_master_application AS
  (SELECT
    /*+ FULL(APPL) PARALLEL(APPL,4) PARALLEL(MAPP,4) */
    CASE
      WHEN appl.master_application = 0
      THEN appl.app_number
      ELSE appl.master_application
    END AS ma_app_number ,
    CASE
      WHEN appl.master_application <> 0
      THEN 1
      ELSE 0
    END                     AS split_app ,
    mapp.entered_time_stamp AS ma_entered_time_stamp ,
    appl.master_application ,
    appl.sa_id
    || '_'
    ||
    CASE
      WHEN appl.master_application = 0
      THEN appl.app_number
      ELSE appl.master_application
    END AS customer_application ,
    appl.app_number ,
    appl.credit_applications_id ,
    appl.entered_time_stamp ,
    appl.credit_application_status ,
    CASE
      WHEN mapp.entered_time_stamp IS NULL
      THEN TO_CHAR(appl.entered_time_stamp,'DD/MM/YYYY')
      ELSE TO_CHAR(mapp.entered_time_stamp,'DD/MM/YYYY')
    END AS app_date ,
    appl.sa_id ,
    appl.channel ,
    appl.campaign_id ,
    appl.promotion_id ,
    appl.comm_channel_for_statement ,
    appl.store_number ,
    appl.created_by_user ,
    appl.poi_required ,
    appl.paper_app_created_by_user
  FROM dwh_wfs_foundation.fnd_wfs_om4_application appl
  LEFT OUTER JOIN dwh_wfs_foundation.fnd_wfs_om4_application mapp
  ON appl.master_application     = mapp.app_number
  AND appl.master_application   <> 0
  WHERE appl.entered_time_stamp >= add_months (TRUNC (sysdate,'YEAR'), -48) -- Changed by Nhlaka Dlamini from ADD_MONTHS (TRUNC (SYSDATE,'YEAR'), -12) to include all the missing data
  ),
  fnd_wfs_tmp_prod_application AS
  (SELECT
    /*+ FULL(CRD) PARALLEL(CRD,4) PARALLEL(APPS,4) PARALLEL(OFFER,4) */
    apps.ma_app_number,
    apps.split_app,
    apps.ma_entered_time_stamp,
    apps.master_application,
    apps.customer_application,
    apps.app_number,
    apps.credit_applications_id,
    apps.entered_time_stamp,
    apps.credit_application_status,
    apps.app_date,
    apps.sa_id,
    apps.channel,
    apps.campaign_id,
    apps.promotion_id,
    apps.comm_channel_for_statement,
    apps.store_number,
    apps.created_by_user,
    apps.poi_required,
    apps.paper_app_created_by_user,
    apps.customer_application
    || '_'
    || crd.product_name AS customer_application_product,
    apps.sa_id
    || '_'
    || crd.product_name AS customer_product ,
    crd.product_name ,
    crd.counter_offer ,
    crd.offer_id ,
    crd.credit_details_id ,
    crd.product_purpose,
    to_number(crd.product_amount)product_amount ,
    to_number(crd.fee_borrower_basic) fee_borrower_basic,
    to_number(crd.fee_borrower_comprehensive)fee_borrower_comprehensive ,
    crd.delivery_type --added 02022017
    ,
    crd.delivery_store --added 02022017
    ,
    crd.risk_category --added 03072017
  FROM fnd_wfs_tmp_master_application apps
  INNER JOIN dwh_wfs_foundation.fnd_wfs_om4_cr_detail crd
  ON crd.credit_applications_id = apps.credit_applications_id
  INNER JOIN dwh_wfs_foundation.fnd_wfs_om4_offer offer
  ON offer.offer_id                = crd.offer_id
  AND offer.credit_applications_id = apps.credit_applications_id
  AND offer.origin                 = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE'
  AND offer.duplicate_ind          = 0
  ),
  fnd_wfs_tmp_product_appl AS
  (SELECT ma_app_number,
    split_app,
    ma_entered_time_stamp,
    master_application,
    customer_application,
    app_number,
    credit_applications_id,
    entered_time_stamp,
    credit_application_status,
    app_date,
    sa_id,
    channel,
    campaign_id,
    promotion_id,
    comm_channel_for_statement,
    store_number,
    created_by_user,
    poi_required,
    customer_application_product,
    customer_product,
    product_name,
    counter_offer,
    offer_id,
    credit_details_id,
    product_purpose,
    product_amount,
    fee_borrower_basic,
    fee_borrower_comprehensive,
    delivery_type,
    delivery_store,
    risk_category,
    rn,
    paper_app_created_by_user
  FROM
    (SELECT
      /*+ FULL(PROD_APP) PARALLEL(PROD_APP,4) */
      ma_app_number,
      split_app,
      ma_entered_time_stamp,
      master_application,
      customer_application,
      app_number,
      credit_applications_id,
      entered_time_stamp,
      credit_application_status,
      app_date,
      sa_id,
      channel,
      to_number(campaign_id)campaign_id,
      to_number(promotion_id)promotion_id,
      comm_channel_for_statement,
      to_number(store_number)store_number,
      created_by_user,
      poi_required,
      customer_application_product,
      customer_product,
      product_name,
      counter_offer,
      offer_id,
      credit_details_id,
      product_purpose,
      to_number(product_amount)product_amount,
      to_number(fee_borrower_basic)fee_borrower_basic ,
      to_number(fee_borrower_comprehensive)fee_borrower_comprehensive,
      delivery_type,
      delivery_store,
      risk_category,
      row_number() over (partition BY ma_app_number,product_name order by ma_app_number,product_name,app_number DESC) AS rn, --ENTERED_TIME_STAMP DESC Commented by Nhlaka due to timestamp being the same for some applications,WF.ACTIVITY_TIMESTAMP DESC) as RN
      paper_app_created_by_user
    FROM fnd_wfs_tmp_prod_application prod_app
    WHERE prod_app.product_name IS NOT NULL
    )
  WHERE rn = 1
  ),
  fnd_wfs_tmp_main_product_appl AS
  (SELECT
    /*+ FULL(WF) PARALLEL(WF,4) PARALLEL (APPL,4) PARALLEL (PACT,4) */
    appl.ma_entered_time_stamp ,
    appl.ma_app_number ,
    appl.split_app ,
    appl.customer_application ,
    appl.customer_product ,
    appl.customer_application_product ,
    appl.app_number ,
    appl.entered_time_stamp ,
    CASE
      WHEN appl.ma_entered_time_stamp IS NULL
      THEN TO_CHAR(appl.entered_time_stamp,'DD/MM/YYYY')
      ELSE TO_CHAR(appl.ma_entered_time_stamp,'DD/MM/YYYY')
    END AS app_date,
    appl.credit_applications_id ,
    appl.credit_application_status ,
    appl.channel AS app_channel,
    CASE
      WHEN appl.channel          = 'CallCenter'
      AND appl.product_name      = 'StoreCard'
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'Prospect'
      WHEN appl.channel          = 'CallCenter'
      AND appl.product_name     IN ('CreditCard','PersonalLoan')
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'Xsell'
      WHEN appl.channel         IN ('PaperApp','TTD')
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'TTD'
      WHEN appl.store_number IN (9999,8888,7777)
      THEN 'WEB'
      WHEN appl.store_number IN (9990,8880,7770)
      THEN 'One App'
      WHEN appl.store_number IN (9991,8881,7771)
      THEN 'SMS'
    END AS channel,
    appl.campaign_id ,
    appl.promotion_id ,
    appl.product_name ,
    appl.product_amount ,
    appl.counter_offer ,
    CASE appl.counter_offer
      WHEN 'true'
      THEN 'Requested'
      ELSE 'Bonus'
    END AS request_type ,
    appl.product_purpose ,
    appl.sa_id ,
    appl.poi_required ,
    appl.store_number ,
    lower(appl.created_by_user) AS created_by ,
    appl.fee_borrower_comprehensive ,
    appl.fee_borrower_basic ,
    appl.delivery_type ,             --added 02022017
    appl.delivery_store ,            --added 02022017
    appl.risk_category ,             --added 03072017
    appl.paper_app_created_by_user , --added 13092017
    appl.created_by_user,            --added 13092017
    CASE
      WHEN appl.counter_offer = 'true'
      THEN 1
      ELSE 0
    END AS product_selected ,
    CASE
        WHEN appl.counter_offer                 = 'true'
         AND appl.credit_application_status NOT IN ('Hurdle 0.5 Complete','Hurdle 1 Data Capture','New','Withdrawn')
         AND appl.product_purpose NOT           IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
        THEN 1
        WHEN appl.counter_offer             = 'true'
         AND appl.credit_application_status IN ('Hurdle 0.5 Complete','Hurdle 1 Data Capture','New','Withdrawn')
         AND appl.product_purpose           IN ('Continue')
        THEN 0
        WHEN appl.counter_offer       = 'true'
         AND appl.product_purpose NOT IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
        THEN 1
      END                                AS product_offered , --Decisioning Outcome
    /*Commented by N Dlamini due to PH1 Change request*/
--    CASE
--      WHEN appl.counter_offer       = 'true'
--      AND appl.product_purpose NOT IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
--      THEN 1
--      ELSE 0
--    END AS product_offered,--Decisioning Outcome
    CASE
      WHEN appl.counter_offer       = 'true'
      AND appl.product_purpose NOT IN ('Requested','Continue','Decline','Appeal','Refer','Customer Cancelled','Customer Undecided')
      THEN 1
      ELSE 0
    END AS product_accepted, --Initial
    CASE
      WHEN appl.counter_offer       = 'false'
      AND appl.product_purpose NOT IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      ELSE 0
    END AS bonus_product_offered, --Bonus Offered
    CASE
      WHEN appl.counter_offer       = 'false'
      AND appl.product_purpose NOT IN ('Requested','Continue','Decline','Appeal','Refer','Customer Cancelled','Customer Undecided')
      THEN 1
      ELSE 0
    END AS bonus_product_accepted, --Bonus App
    CASE
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('Complete')
      AND appl.product_purpose           IN ('Complete')
      THEN 1
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('CAMS Create service is not available','Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      ELSE 0
    END AS product_complete, --Final Outcome
    CASE
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('Complete')
      AND appl.product_purpose           IN ('Complete')
      THEN 1
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('CAMS Create service is not available','Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue')
      THEN 1
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue')
      THEN 1
      ELSE 0
    END AS bonus_product_complete, --Bonus Final Outcome
    CASE
      WHEN ((appl.fee_borrower_comprehensive = 0
      AND appl.fee_borrower_basic            = 0)
      OR (appl.fee_borrower_comprehensive   IS NULL
      AND appl.fee_borrower_basic           IS NULL))
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 'None'
      WHEN(appl.fee_borrower_basic         IS NULL
      OR appl.fee_borrower_basic            = 0)
      AND (appl.fee_borrower_comprehensive IS NOT NULL
      OR appl.fee_borrower_comprehensive   <> 0)
      AND appl.product_purpose NOT         IN ('Decline','Customer Cancelled')
      THEN 'Comprehensive'
      WHEN (appl.fee_borrower_comprehensive IS NULL
      OR appl.fee_borrower_comprehensive     = 0)
      AND (appl.fee_borrower_basic          IS NOT NULL
      OR appl.fee_borrower_basic            <> 0)
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 'Basic'
      ELSE ''
    END AS insurance_type ,
    CASE
      WHEN ((appl.fee_borrower_comprehensive = 0
      AND appl.fee_borrower_basic            = 0)
      OR (appl.fee_borrower_comprehensive   IS NULL
      AND appl.fee_borrower_basic           IS NULL))
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 0
      WHEN(appl.fee_borrower_basic         IS NULL
      OR appl.fee_borrower_basic            = 0)
      AND (appl.fee_borrower_comprehensive IS NOT NULL
      OR appl.fee_borrower_comprehensive   <> 0)
      AND appl.product_purpose NOT         IN ('Decline','Customer Cancelled')
      THEN 1
      WHEN (appl.fee_borrower_comprehensive IS NULL
      OR appl.fee_borrower_comprehensive     = 0)
      AND (appl.fee_borrower_basic          IS NOT NULL
      OR appl.fee_borrower_basic            <> 0)
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 1
      ELSE 0
    END AS bpi_count ,
    CASE
      WHEN appl.comm_channel_for_statement IN ('E-mail','Other E-Mail','MMS')
      THEN 1
      ELSE 0
    END AS email_count,
    MIN(
    CASE
      WHEN wf.activity = 'ContractAccepted'
      THEN TO_CHAR(wf.activity_timestamp,'DD/MM/YYYY')
      ELSE NULL
    END )AS pre_agreement_accept_date,
    MIN(
    CASE
      WHEN wf.activity = 'ContractAccepted'
      THEN user1
      ELSE NULL
    END )AS pre_agreement_accept_agent, -- Added by Nhlaka Dlamini on the 16th August 2018 Replaced PAA_USER column  Due to Maturation requirements
    MIN(
    CASE
      WHEN wf.activity IN ('PRODUCT OFFERED-CreditCard','PRODUCT OFFERED-StoreCard','PRODUCT OFFERED-PersonalLoan')
      THEN TO_CHAR(wf.activity_timestamp,'DD/MM/YYYY')
      ELSE NULL
    END )AS product_offer_date,
    MIN(
    CASE
      WHEN wf.activity IN ('PRODUCT OFFERED-CreditCard','PRODUCT OFFERED-StoreCard','PRODUCT OFFERED-PersonalLoan')
      THEN user1
      ELSE NULL
    END )AS product_offer_agent,
    MIN(
    CASE
      WHEN wf.activity IN ('PRODUCT OFFERED-CreditCard','PRODUCT OFFERED-StoreCard','PRODUCT OFFERED-PersonalLoan')
      AND wf.status     = 'Accepted By Customer'
      THEN TO_CHAR(wf.activity_timestamp,'DD/MM/YYYY')
      ELSE NULL
    END ) AS product_offer_accept_date,
    MIN(
    CASE
      WHEN wf.activity IN ('PRODUCT OFFERED-CreditCard','PRODUCT OFFERED-StoreCard','PRODUCT OFFERED-PersonalLoan')
      AND wf.status     = 'Accepted By Customer'
      THEN user1
      ELSE NULL
    END ) AS product_offer_accept_agent
  FROM fnd_wfs_tmp_product_appl appl
  LEFT OUTER JOIN dwh_wfs_foundation.fnd_wfs_om4_workflow wf
  ON appl.credit_applications_id = wf.credit_applications_id
  AND wf.product_type            = appl.product_name
  WHERE appl.rn                  = 1
  GROUP BY appl.ma_entered_time_stamp ,
    appl.ma_app_number ,
    appl.split_app ,
    appl.customer_application ,
    appl.customer_product,
    appl.customer_application_product ,
    appl.app_number ,
    appl.entered_time_stamp ,
    CASE
      WHEN appl.ma_entered_time_stamp IS NULL
      THEN TO_CHAR(appl.entered_time_stamp,'DD/MM/YYYY')
      ELSE TO_CHAR(appl.ma_entered_time_stamp,'DD/MM/YYYY')
    END ,
    appl.credit_applications_id ,
    appl.credit_application_status ,
    appl.channel ,
    CASE
      WHEN appl.channel          = 'CallCenter'
      AND appl.product_name      = 'StoreCard'
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'Prospect'
      WHEN appl.channel          = 'CallCenter'
      AND appl.product_name     IN ('CreditCard','PersonalLoan')
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'Xsell'
      WHEN appl.channel         IN ('PaperApp','TTD')
      AND appl.store_number NOT IN (9999,8888,7777,9990,8880,7770,9991,8881,7771)
      THEN 'TTD'
      WHEN appl.store_number IN (9999,8888,7777)
      THEN 'WEB'
      WHEN appl.store_number IN (9990,8880,7770)
      THEN 'One App'
      WHEN appl.store_number IN (9991,8881,7771)
      THEN 'SMS'
    END ,
    appl.campaign_id ,
    appl.promotion_id ,
    appl.product_name ,
    appl.product_amount ,
    appl.counter_offer ,
    CASE appl.counter_offer
      WHEN 'true'
      THEN 'Requested'
      ELSE 'Bonus'
    END ,
    appl.product_purpose ,
    appl.sa_id ,
    appl.poi_required ,
    appl.store_number ,
    lower(appl.created_by_user) ,
    appl.fee_borrower_comprehensive ,
    appl.fee_borrower_basic ,
    appl.delivery_type --added 02022017
    ,
    appl.delivery_store --added 02022017
    ,
    appl.risk_category --added 03072017
    ,
    appl.paper_app_created_by_user --added 13092017
    ,
    appl.created_by_user --added 13092017
    ,
    CASE appl.counter_offer
      WHEN 'true'
      THEN 1
      ELSE 0
    END ,
    CASE
      WHEN appl.counter_offer       = 'true'
      AND appl.product_purpose NOT IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      ELSE 0
    END, --Decisioning Outcome
   CASE
      WHEN appl.counter_offer       = 'true'
      AND appl.product_purpose NOT IN ('Requested','Continue','Decline','Appeal','Refer','Customer Cancelled','Customer Undecided')
      THEN 1
      ELSE 0
    END --Initial
    ,
    CASE
      WHEN appl.counter_offer       = 'false'
      AND appl.product_purpose NOT IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      ELSE 0
    END --Bonus Offered
    ,
    CASE
      WHEN appl.counter_offer       = 'false'
      AND appl.product_purpose NOT IN ('Requested','Continue','Decline','Appeal','Refer','Customer Cancelled','Customer Undecided')
      THEN 1
      ELSE 0
    END --Bonus App
    ,
    CASE
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('Complete')
      AND appl.product_purpose           IN ('Complete')
      THEN 1
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('CAMS Create service is not available','Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      WHEN appl.counter_offer             = 'true'
      AND appl.credit_application_status IN ('Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled')
      THEN 1
      ELSE 0
    END --Final Outcome
    ,
    CASE
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('Complete')
      AND appl.product_purpose           IN ('Complete')
      THEN 1
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('CAMS Create service is not available','Vision Not Retrieved','C2 Create Update Not Retrieved','FICA service is not available','Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue')
      THEN 1
      WHEN appl.counter_offer             = 'false'
      AND appl.credit_application_status IN ('Contract Presentation')
      AND appl.product_purpose NOT       IN ('Appeal','Decline','Requested','Refer','Customer Cancelled','Continue')
      THEN 1
      ELSE 0
    END --Bonus Final Outcome
    ,
    CASE
      WHEN ((appl.fee_borrower_comprehensive = 0
      AND appl.fee_borrower_basic            = 0)
      OR (appl.fee_borrower_comprehensive   IS NULL
      AND appl.fee_borrower_basic           IS NULL))
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 'None'
      WHEN(appl.fee_borrower_basic         IS NULL
      OR appl.fee_borrower_basic            = 0)
      AND (appl.fee_borrower_comprehensive IS NOT NULL
      OR appl.fee_borrower_comprehensive   <> 0)
      AND appl.product_purpose NOT         IN ('Decline','Customer Cancelled')
      THEN 'Comprehensive'
      WHEN (appl.fee_borrower_comprehensive IS NULL
      OR appl.fee_borrower_comprehensive     = 0)
      AND (appl.fee_borrower_basic          IS NOT NULL
      OR appl.fee_borrower_basic            <> 0)
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 'Basic'
      ELSE ''
    END ,
    CASE
      WHEN ((appl.fee_borrower_comprehensive = 0
      AND appl.fee_borrower_basic            = 0)
      OR (appl.fee_borrower_comprehensive   IS NULL
      AND appl.fee_borrower_basic           IS NULL))
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 0
      WHEN(appl.fee_borrower_basic         IS NULL
      OR appl.fee_borrower_basic            = 0)
      AND (appl.fee_borrower_comprehensive IS NOT NULL
      OR appl.fee_borrower_comprehensive   <> 0)
      AND appl.product_purpose NOT         IN ('Decline','Customer Cancelled')
      THEN 1
      WHEN (appl.fee_borrower_comprehensive IS NULL
      OR appl.fee_borrower_comprehensive     = 0)
      AND (appl.fee_borrower_basic          IS NOT NULL
      OR appl.fee_borrower_basic            <> 0)
      AND appl.product_purpose NOT          IN ('Decline','Customer Cancelled')
      THEN 1
      ELSE 0
    END ,
    CASE
      WHEN appl.comm_channel_for_statement IN ('E-mail','Other E-Mail','MMS')
      THEN 1
      ELSE 0
    END
  ),
  dea_query AS
  (SELECT credit_applications_id,
    product_type,
    user1,
    latest_dea_date,
    row_number() over (partition BY credit_applications_id order by latest_dea_date DESC) rn
  FROM
    (SELECT
      /*+ FULL(WF) PARALLEL(WF,4) PARALLEL (APPL,4) */
      DISTINCT appl.credit_applications_id ,
      wf.product_type,
      wf.user1,
      TO_CHAR(MAX(activity_timestamp),'DD/MM/YYYY') AS latest_dea_date
    FROM dwh_wfs_foundation.fnd_wfs_om4_workflow wf
    INNER JOIN fnd_wfs_tmp_main_product_appl appl
    ON (wf.credit_applications_id = appl.credit_applications_id)
    WHERE (wf.activity LIKE 'DEA Consent%')
    AND (wf.decision = 'true')
    GROUP BY appl.credit_applications_id ,
      wf.product_type ,
      wf.user1
    )
  ),
  /*Commented by N Dlamini to ensure that DEA Flag rule is the same as the NBR App Rule*/
  --  dea_query as
  --  (
  -- select credit_applications_id,
  --        product_type,
  --        user1,
  --        latest_dea_date,
  --        row_number() over (partition by credit_applications_id order by latest_dea_date desc) rn
  -- from
  -- (select
  --    /*+ FULL(WF) PARALLEL(WF,4) PARALLEL (APPL,4) */
  --    distinct appl.credit_applications_id ,
  --    wf.product_type,
  --    wf.user1,
  --    to_char(max(activity_timestamp),'DD/MM/YYYY') as latest_dea_date
  --  from dwh_wfs_foundation.fnd_wfs_om4_workflow wf
  --  inner join fnd_wfs_tmp_main_product_appl appl
  --  on (wf.credit_applications_id = appl.credit_applications_id)
  --  where (wf.activity like 'DEA Consent%')
  --    and (wf.decision = 'true')
  --  group by appl.credit_applications_id , wf.product_type , wf.user1
  --  )),
  fnd_wfs_tmp_customer_undecided AS
  (SELECT credit_applications_id,
    product_type,
    customer_undecided_agent,
    customer_undecided_date
  FROM
    (SELECT
      /*+ FULL(WF) PARALLEL(WF,4) */
      DISTINCT wf.credit_applications_id,
      wf.product_type,
      wf.user1                                 AS customer_undecided_agent,
      TO_CHAR(activity_timestamp,'DD/MM/YYYY') AS customer_undecided_date,
      row_number() over (partition BY wf.credit_applications_id order by activity_timestamp ASC) rn
    FROM dwh_wfs_foundation.fnd_wfs_om4_workflow wf
    WHERE status = 'Customer Undecided'
    )
  WHERE rn = 1
  ),
  all_nbr_applications AS
  (SELECT
    /*+ FULL(main_appl) PARALLEL(main_appl,4)PARALLEL(dea,4) PARALLEL(undecided,4) PARALLEL(perso,4) */
    DISTINCT main_appl.app_number,
    main_appl.ma_entered_time_stamp,
    main_appl.ma_app_number,
    main_appl.split_app,
    main_appl.customer_application,
    main_appl.customer_product,
    main_appl.customer_application_product,
    main_appl.entered_time_stamp,
    to_date(main_appl.app_date,'DD/MM/YYYY')app_date,
    CASE
      WHEN main_appl.created_by_user = 'guest'
      THEN main_appl.paper_app_created_by_user
      ELSE main_appl.created_by_user
    END AS application_agent,
    main_appl.credit_applications_id,
    main_appl.credit_application_status,
    main_appl.app_channel,
    main_appl.channel,
    main_appl.campaign_id,
    main_appl.promotion_id,
    main_appl.product_name,
    main_appl.product_amount,
    main_appl.counter_offer,
    main_appl.request_type,
    main_appl.product_purpose,
    main_appl.sa_id,
    main_appl.poi_required,
    main_appl.store_number,
    main_appl.created_by,
    main_appl.fee_borrower_comprehensive,
    main_appl.fee_borrower_basic,
    main_appl.delivery_type,
    main_appl.delivery_store,
    main_appl.risk_category,
    main_appl.paper_app_created_by_user,
    main_appl.created_by_user,
    main_appl.product_selected,
    main_appl.product_offered,
    main_appl.product_accepted,
    main_appl.bonus_product_offered,
    main_appl.bonus_product_accepted,
    main_appl.product_complete,
    main_appl.bonus_product_complete,
    main_appl.insurance_type,
    main_appl.bpi_count,
    main_appl.email_count,
    to_date(main_appl.product_offer_date,'DD/MM/YYYY')product_offer_date,
    main_appl.product_offer_agent,
    to_date(main_appl.product_offer_accept_date,'DD/MM/YYYY')product_offer_accept_date,
    main_appl.product_offer_accept_agent,
    to_date(main_appl.pre_agreement_accept_date,'DD/MM/YYYY')pre_agreement_accept_date,
    main_appl.pre_agreement_accept_agent,
    to_date(dea.latest_dea_date,'DD/MM/YYYY') latest_dea_date,
    CASE
      WHEN perso.poi_consent = 'false'
      THEN 'No'
      ELSE 'Yes'
    END     AS dea_ind,
    dea.user1 AS dea_consent_agent,
    to_date(undecided.customer_undecided_date,'DD/MM/YYYY')customer_undecided_date,
    undecided.customer_undecided_agent
  FROM fnd_wfs_tmp_main_product_appl main_appl
  LEFT OUTER JOIN dea_query dea
  ON main_appl.credit_applications_id = dea.credit_applications_id
  AND dea.rn                          = 1
  LEFT OUTER JOIN dwh_wfs_foundation.fnd_wfs_om4_personal_applicant perso
    ON main_appl.credit_applications_id = perso.credit_applications_id
    AND main_appl.product_name          = perso.product_name
    AND perso.personal_applicant_type = 'OM_APPLICANT_TYPE_APPLICANT'
  LEFT OUTER JOIN fnd_wfs_tmp_customer_undecided undecided
  ON main_appl.credit_applications_id = undecided.credit_applications_id
  AND main_appl.product_name          = undecided.product_type
  )
SELECT app_number,
  ma_entered_time_stamp,
  ma_app_number,
  split_app,
  customer_application,
  customer_product,
  customer_application_product,
  entered_time_stamp,
  app_date,
  application_agent,
  credit_applications_id,
  credit_application_status,
  app_channel,
  channel,
  campaign_id,
  promotion_id,
  product_name,
  product_amount,
  counter_offer,
  request_type,
  product_purpose,
  sa_id,
  poi_required,
  store_number,
  created_by,
  fee_borrower_comprehensive,
  fee_borrower_basic,
  delivery_type,
  delivery_store,
  risk_category,
  paper_app_created_by_user,
  created_by_user,
  product_selected,
  product_offered,
  product_accepted,
  bonus_product_offered,
  bonus_product_accepted,
  product_complete,
  bonus_product_complete,
  insurance_type,
  bpi_count,
  email_count,
  product_offer_date,
  product_offer_agent,
  product_offer_accept_date,
  product_offer_accept_agent,
  pre_agreement_accept_date,
  pre_agreement_accept_agent,
  dea_ind,
  latest_dea_date,
  dea_consent_agent,
  customer_undecided_date,
  customer_undecided_agent
FROM all_nbr_applications;
g_recs_read     := g_recs_read     + sql%rowcount;
g_recs_inserted := g_recs_inserted + sql%rowcount;
COMMIT;
l_text := 'WFS_NBR_TMP_MAIN_PRODUCT_APPL Insert completed at '||TO_CHAR(sysdate,('DD MON YYYY HH24:MI:SS'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
COMMIT;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('DD MON YYYY  HH24:MI:SS'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_deleted||g_recs_deleted;
l_text := l_text||'';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_run_completed ||sysdate;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_draw_line;
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
  raise;
END WH_PRF_WFS_682U;
