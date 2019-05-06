--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_690U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_690U" (p_forall_limit in integer,p_success out boolean)
as
  --**************************************************************************************************
  --  Description  WFS New Business Report - WFS New Business Report - Fact table in the performance layer, that has the detailed applications with all the data needed for querying and report development in Qlikview.
  --  Date:        2018-07-25
  --  Author:      Nhlaka dlamini
  --  Purpose:     Update  WFS_NBR_APPLICATION_DETAIL Target Fact Table in the performance layer
  --               with input ex
  --                    WFS_NBR_TMP_BASE_POI
  --                    WFS_NBR_TMP_BASE_FICA
  --                    WFS_NBR_TMP_BOOKED_ACC
  --                    WFS_NBR_TMP_MAIN_PRODUCT_APPL
  --                    VS_CALENDAR_WFS
  --                    APEX_DWH_WFS_STORES
  --
  --               THIS JOB RUNS DAILY
  --  Tables:      Input  -
  --                    WFS_NBR_TMP_BASE_POI
  --                    WFS_NBR_TMP_BASE_FICA
  --                    WFS_NBR_TMP_BOOKED_ACC
  --                    WFS_NBR_TMP_MAIN_PRODUCT_APPL
  --                    VS_CALENDAR_WFS
  --                    APEX_DWH_WFS_STORES
  --               Output - WFS_NBR_APPLICATION_DETAIL
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  2018-07-23 N Dlamini - created 
  --  2018-08-20 N Dlamini - Added log table meta data
  --  2018-08-23 N Dlamini - Changed the date conversions
  --  2018-09-11 N Dlamini - Added Activations to this procedure as fix to Activations not matching with Qlikview NBR APP
  --  2018-10-09 N Dlamini - Added Maturation columns & other NBR missing columns.

  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  
    g_forall_limit  integer := dwh_constants.vc_forall_limit;
    g_recs_read     integer := 0;
    g_recs_updated  integer := 0;
    g_recs_inserted integer := 0;
    g_recs_hospital integer := 0;
    g_recs_deleted  integer := 0;
    g_error_count   number  := 0;
    g_error_index   number  := 0;
    g_count         number  := 0;
    g_sub           integer := 0;
    g_found      boolean;
    g_date       date := trunc(sysdate);
    g_start_week number ;
    g_end_week   number ;
    g_yesterday  date := trunc(sysdate) - 1;
    g_fin_day_no dim_calendar.fin_day_no%type;
    g_stmt  varchar2(300);
    g_yr_00 number;
    g_qt_00 number;
  g_ctas clob;
  g_table_exists integer;
    l_message sys_dwh_errlog.log_text%type;
    l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WFS_690U';
    l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
    l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
    l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
    l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
    l_text sys_dwh_log.log_text%type ;
    l_description sys_dwh_log_summary.log_description%type   := 'WFS NBR Application Detail';
    l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************

begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit  := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text    := dwh_constants.vc_log_draw_line;
    l_text := 'WFS_NBR_APPLICATION_DETAIL load STARTED AT '|| to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD TABLE: '||'WFS_NBR_APPLICATION_DETAIL' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'ALTER SESSION ENABLE PARALLEL DML';

    execute immediate 'TRUNCATE TABLE DWH_WFS_PERFORMANCE.WFS_NBR_APPLICATION_DETAIL';

  l_text := 'WFS_NBR_APPLICATION_DETAIL Completed Truncate At '|| to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


insert
  /* +APPEND */
into dwh_wfs_performance.wfs_nbr_application_detail a
  (
    wfs_year_week,
    wfs_year,
    wfs_year_quarter,
    wfs_year_month,
    entered_time_stamp,
    app_date,
    application_agent,
    master_app_number,
    split_app_ind,
    app_number,
    channel,
    app_channel,
    campaign_id,
    promotion_id,
    product_name,
    request_type,
    credit_application_status,
    credit_applications_id,
    product_purpose,
    insurance_type,
    poi_required,
    customer_application_product,
    app_ind,
    product_selected_ind,
    product_offered_ind,
    product_accepted_ind,
    bonus_product_offered_ind,
    bonus_product_accepted_ind,
    ph1_ind,
    paa_ind,
    paa_with_poi_ind,
    paa_with_poi_pending_wf_ind,
    paa_with_poi_declined_wf_ind,
    paa_with_poi_expired_wf_ind,
    paa_with_poi_satisfied_wf_ind,
    fica_pending_wf_ind,
    fica_satisfied_wf_ind,
    product_complete_ind,
    bonus_product_complete_ind,
    booked_account_ind,
    product_activated_ind,
    bpi_count_ind,
    email_count_ind,
    activation_date,
    customer_undecided_date,
    customer_undecided_agent,
    pre_agreement_accept_date,
    pre_agreement_accept_agent,
    product_offer_date,
    product_offer_agent,
    product_offer_accept_date,
    product_offer_accept_agent,
    sa_id,
    staff_number,
    store_number,
    location_name,
    tier,
    delivery_store,
    delivery_type,
    app_in_flight,
    dea_flag,
    dea_consent_agent,
    latest_dea_date,
    first_poi_success_wf_date,
    first_poi_unsuccessful_wf_date,
    first_poi_wf_date,
    first_fica_pending_wf_date,
    first_fica_successful_wf_date,
    latest_poi_decline_wf_date,
    poi_expiry_wf_date,
    booked_account_date ,
    risk_category,
    solicitation,
    application_accepted_ind,
    credit_limit,
    promotion_category,
    additional_prod_selected_ind,
    product_activated_same_day_ind,
    both_selected_and_accepted_ind
  )
SELECT wfs_year_week,
  wfs_year,
  wfs_year_quarter,
  wfs_year_month,
  entered_time_stamp,
  app_date,
  application_agent,
  master_app_number,
  split_app AS split_app_ind,
  app_number,
  channel,
  app_channel,
  campaign_id,
  promotion_id,
  product_name,
  request_type,
  credit_application_status,
  credit_applications_id,
  product_purpose,
  insurance_type,
  poi_required,
  customer_application_product,
  app                       AS app_ind,
  product_selected          AS product_selected_ind,
  product_offered           AS product_offered_ind,
  product_accepted          AS product_accepted_ind,
  bonus_product_offered     AS bonus_product_offered_ind,
  bonus_product_accepted    AS bonus_product_accepted_ind,
  ph1                       AS ph1_ind,
  paa                       AS paa_ind,
  paa_with_poi              AS paa_with_poi_ind,
  paa_with_poi_pending_wf   AS paa_with_poi_pending_wf_ind,
  paa_with_poi_declined_wf  AS paa_with_poi_declined_wf_ind,
  paa_with_poi_expired_wf   AS paa_with_poi_expired_wf_ind,
  paa_with_poi_satisfied_wf AS paa_with_poi_satisfied_wf_ind,
  fica_pending_wf           AS fica_pending_wf_ind,
  fica_satisfied_wf         AS fica_satisfied_wf_ind,
  product_complete          AS product_complete_ind,
  bonus_product_complete    AS bonus_product_complete_ind,
  booked_account            AS booked_account_ind,
  product_activated         AS product_activated_ind,
  bpi_count                 AS bpi_count_ind,
  email_count               AS email_count_ind,
  activation_date,
  customer_undecided_date,
  customer_undecided_agent,
  pre_agreement_accept_date,
  pre_agreement_accept_agent,
  product_offer_date,
  product_offer_agent,
  product_offer_accept_date,
  product_offer_accept_agent,
  sa_id,
  staff_number,
  store_number,
  STORE_NAME AS location_name,
  store_tier AS tier,
  delivery_store,
  delivery_type,
  app_in_flight,
  dea_ind AS dea_flag,
  dea_consent_agent,
  latest_dea_date,
  first_wf_poi_date_success      AS first_poi_success_wf_date,
  first_wf_poi_date_unsuccessful AS first_poi_unsuccessful_wf_date,
  first_wf_poi_date              AS first_poi_wf_date,
  first_fica_pending_date_wf     AS first_fica_pending_wf_date,
  first_fica_successful_date_wf  AS first_fica_successful_wf_date,
  latest_poi_decline_date_wf     AS latest_poi_decline_wf_date,
  poi_expiry_date_wf             AS poi_expiry_wf_date,
  booked_account_date ,
  risk_category,
  solicitation,
  application_accepted AS application_accepted_ind,
  credit_limit,
  promotion_category,
  additional_product_selected AS additional_prod_selected_ind,
  product_activated_same_day  AS product_activated_same_day_ind,
  both_selected_and_accepted  AS both_selected_and_accepted_ind
FROM
  (SELECT wfs_year_week,
    wfs_year,
    wfs_year_quarter,
    wfs_year_month,
    entered_time_stamp,
    app_date,
    application_agent,
    master_app_number,
    split_app,
    app_number,
    channel,
    app_channel,
    campaign_id,
    promotion_id,
    product_name,
    request_type,
    credit_application_status,
    credit_applications_id,
    product_purpose,
    insurance_type,
    poi_required,
    customer_application_product,
    CASE
      WHEN app = 1
      THEN COUNT (DISTINCT customer_application_product)
      ELSE 0
    END AS app,
    CASE
      WHEN product_selected = 1
      THEN COUNT (DISTINCT customer_application_product)
      ELSE 0
    END AS product_selected,
    CASE
      WHEN product_offered = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS product_offered,
    CASE
      WHEN product_accepted = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS product_accepted,
    CASE
      WHEN bonus_product_offered = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS bonus_product_offered,
    CASE
      WHEN bonus_product_accepted = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS bonus_product_accepted,
    CASE
      WHEN ph1 = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS ph1,
    CASE
      WHEN pre_agreement_accepted = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa,
    CASE
      WHEN pre_agreement_accepted_poi = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa_with_poi,
    CASE
      WHEN pre_agreement_accepted = 1
      AND paa_with_poi_pending_wf = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa_with_poi_pending_wf,
    CASE
      WHEN pre_agreement_accepted  = 1
      AND paa_with_poi_declined_wf = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa_with_poi_declined_wf,
    CASE
      WHEN pre_agreement_accepted = 1
      AND paa_with_poi_expired_wf = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa_with_poi_expired_wf,
    CASE
      WHEN pre_agreement_accepted = 1
      AND paa_with_poi_wf         = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS paa_with_poi_satisfied_wf,
    CASE
      WHEN pre_agreement_accepted_poi = 1
      AND fica_pending                = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS fica_pending_wf,
    CASE
      WHEN pre_agreement_accepted_poi = 1
      AND fica_wf                     = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS fica_satisfied_wf,
    CASE
      WHEN product_complete = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS product_complete,
    CASE
      WHEN bonus_product_complete = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS bonus_product_complete,
    CASE
      WHEN booked_accounts_wf = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS booked_account,
    CASE
      WHEN product_activated = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS product_activated,
    CASE
      WHEN bpi_count = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END AS bpi_count,
    CASE
      WHEN email_count = 1
      THEN COUNT(DISTINCT customer_application_product)
      ELSE 0
    END                        AS email_count,
    activation_date            AS activation_date,
    customer_undecided_date    AS customer_undecided_date,
    customer_undecided_agent   AS customer_undecided_agent,
    pre_agreement_accept_date  AS pre_agreement_accept_date,
    pre_agreement_accept_agent AS pre_agreement_accept_agent,
    product_offer_date         AS product_offer_date,
    product_offer_agent        AS product_offer_agent,
    product_offer_accept_date  AS product_offer_accept_date,
    product_offer_accept_agent AS product_offer_accept_agent,
    product_activated          AS activated,
    sa_id                      AS sa_id,
    CASE
      WHEN created_by = 'guest'
      THEN regexp_replace(guest_number,'[[:alpha:]]')
      ELSE created_by
    END                         AS staff_number,
    store_number                AS store_number,
    location_name               AS store_name,
    tier                        AS store_tier,
    delivery_store              AS delivery_store,
    delivery_type               AS delivery_type,
    app_in_flight               AS app_in_flight,
    dea_ind                     AS dea_ind,
    dea_consent_agent           AS dea_consent_agent,
    latest_dea_date             AS latest_dea_date,
    first_successful_poi_date   AS first_wf_poi_date_success,
    first_unsuccessful_poi_date AS first_wf_poi_date_unsuccessful,
    first_wf_poi_date           AS first_wf_poi_date,
    first_fica_pending_date     AS first_fica_pending_date_wf,
    first_fica_successful_date  AS first_fica_successful_date_wf,
    latest_poi_decline_date     AS latest_poi_decline_date_wf,
    poi_expiry_date             AS poi_expiry_date_wf,
    booked_account_date         AS booked_account_date,
    risk_category               AS risk_category,
    solicitation                AS solicitation,
    application_accepted        AS application_accepted,
    credit_limit                AS credit_limit,
    CASE
      WHEN promotion_id IN (7,9,11,13,15,17,19,21,26,27,28,403,407)
      THEN 'Prospecting'
      WHEN promotion_id IN (6,8,10,12,14,16,18,20,23,24,25,29,30,35,36,37,38,39,40,41,400,404)
      THEN 'X-Sell'
    END AS promotion_category, -- Hardcoded these promotion codes from the excel file
    CASE
      WHEN product_name          = 'CreditCard'
      AND bonus_product_accepted = 1
      AND booked_accounts_wf     = 1
      THEN 1
      ELSE 0
    END AS additional_product_selected,
    CASE
      WHEN product_activated = 1
      AND app_date           = activation_date
      THEN COUNT (DISTINCT customer_application_product)
      ELSE 0
    END AS product_activated_same_day,
    CASE
      WHEN product_name      = 'CreditCard'
      AND product_accepted   = 1
      AND booked_accounts_wf = 0
      THEN 1
      ELSE 0
    END AS both_selected_and_accepted
  FROM
    (SELECT app ,
      ph1 ,
      application_accepted ,
      pre_agreement_accepted ,
      pre_agreement_accepted_poi ,
      CASE
        WHEN pre_agreement_accepted    = 1
        AND first_successful_poi_date IS NOT NULL
        THEN 1
        ELSE 0
      END AS paa_with_poi_wf,
      CASE
        WHEN pre_agreement_accepted    = 1
        AND first_successful_poi_date IS NULL
        AND first_wf_poi_date         IS NOT NULL
        AND latest_poi_decline_date   IS NULL
        AND poi_expiry_date           IS NULL
        THEN 1
        ELSE 0
      END AS paa_with_poi_pending_wf,
      CASE
        WHEN pre_agreement_accepted    = 1
        AND first_successful_poi_date IS NULL
        AND first_wf_poi_date         IS NOT NULL
        AND latest_poi_decline_date   IS NOT NULL
        AND poi_expiry_date           IS NULL
        THEN 1
        ELSE 0
      END AS paa_with_poi_declined_wf,
      CASE
        WHEN pre_agreement_accepted    = 1
        AND first_successful_poi_date IS NULL
        AND first_wf_poi_date         IS NOT NULL
        AND poi_expiry_date           IS NOT NULL
        THEN 1
        ELSE 0
      END AS paa_with_poi_expired_wf,
      CASE
        WHEN first_fica_successful_date IS NULL
        AND first_fica_pending_date     IS NULL
        THEN 0
        WHEN first_fica_successful_date IS NULL
        AND first_fica_pending_date     IS NOT NULL
        THEN 1
        ELSE 0
      END AS fica_pending,
      CASE
        WHEN first_fica_successful_date IS NULL
        THEN 0
        ELSE 1
      END AS fica_wf,
      CASE
        WHEN booked_account_date IS NULL
        THEN 0
        ELSE 1
      END AS booked_accounts_wf,
      CASE
        WHEN booked_account_date IS NULL
        THEN 0
        WHEN pre_agreement_accepted_poi = 1
        THEN 1
        ELSE 0
      END AS booked_accounts_paa_poi,
      CASE
        WHEN pre_agreement_accepted_poi = 1
        THEN product_amount
        ELSE 0
      END AS credit_limit,
      CASE
        WHEN promotion_id > 0
        THEN 'Solicited'
        ELSE 'UnSolicited'
      END AS solicitation,
      CASE
        WHEN created_by_user = 'guest'
        THEN paper_app_created_by_user
        ELSE SUBSTR(paper_app_created_by_user, -LENGTH(paper_app_created_by_user),7)
      END AS guest_number,
      product_activated ,
      master_entered_time_stamp ,
      master_app_number ,
      split_app ,
      customer_application ,
      customer_product ,
      customer_application_product ,
      app_number ,
      entered_time_stamp ,
      app_date ,
      application_agent,
      credit_applications_id ,
      credit_application_status ,
      app_channel ,
      channel ,
      campaign_id ,
      promotion_id ,
      product_name ,
      product_amount ,
      counter_offer ,
      request_type ,
      product_purpose ,
      sa_id ,
      poi_required ,
      store_number ,
      created_by ,
      fee_borrower_comprehensive ,
      fee_borrower_basic ,
      delivery_type ,
      delivery_store ,
      risk_category, --added 03072017
      paper_app_created_by_user,
      created_by_user,
      product_selected ,
      product_offered ,
      product_accepted ,
      bonus_product_offered ,
      bonus_product_accepted,
      product_complete,
      bonus_product_complete ,
      insurance_type ,
      bpi_count ,
      email_count ,
      customer_undecided_agent,
      customer_undecided_date,
      pre_agreement_accept_date,
      pre_agreement_accept_agent,
      product_offer_date,
      product_offer_agent,
      product_offer_accept_date,
      product_offer_accept_agent,
      to_date(activation_date,'DD/MM/YYYY')activation_date, --Added Activations to this procedure as fix to Activations not matching with Qlikview NBR APP
      wfs_year ,
      wfs_year_quarter ,
      wfs_year_month ,
      wfs_year_week,
      CASE
        WHEN store_number LIKE '%904%'
        THEN 'Unknown'
        WHEN LENGTH(tier)= 0
        THEN 'Unknown'
        ELSE tier
      END AS tier ,
      location_name ,
      first_fica_pending_date ,
      first_fica_successful_date ,
      first_successful_poi_date,
      first_successful_poi_status ,
      first_unsuccessful_poi_date ,
      first_unsuccessful_poi_status ,
      first_wf_poi_date ,
      latest_poi_date ,
      latest_poi_status ,
      poi_expiry_date ,
      latest_poi_decline_date,
      latest_poi_decline_status ,
      poi_product_purpose ,
      app_in_flight ,
      latest_dea_date,
      dea_consent_agent,
      dea_ind,
      booked_account_date
    FROM
      (SELECT
        /*+ FULL(MAIN_APPL) PARALLEL(MAIN_APPL,4) PARALLEL(BASE_POI,4) PARALLEL(BASE_FICA,4) PARALLEL(BOOKED_ACC,4) PARALLEL(STORES,4) */
        DISTINCT main_appl.app_number, --App No,
        main_appl.app_date,            --App Date,
        main_appl.application_agent,
        fin_cal.wfs_year,         --AS Fin Year,
        fin_cal.wfs_year_quarter, --AS Fin Quarter,
        fin_cal.wfs_year_month,   --AS Fin Month,
        fin_cal.wfs_year_week,    --AS Fin Week,
        main_appl.store_number,   --AS Store Number,
        CASE
          WHEN stores.tier IS NULL
          THEN 'Unknown'
          ELSE tier
        END AS tier ,                                                --AS Store Tier,
        stores.location_name,                                        --AS Store Name,
        main_appl.ma_entered_time_stamp AS master_entered_time_stamp,--Master App Timestamp,
        main_appl.ma_app_number         AS master_app_number ,       --Master App No,
        main_appl.split_app,
        main_appl.customer_application,
        main_appl.customer_product,
        main_appl.customer_application_product,
        main_appl.entered_time_stamp,        --App Timestamp
        main_appl.credit_applications_id,    --App Id,
        main_appl.credit_application_status, --App Status,
        main_appl.app_channel,
        main_appl.channel,
        main_appl.campaign_id,
        main_appl.promotion_id,
        main_appl.product_name, --Product
        main_appl.product_amount,
        main_appl.counter_offer,
        main_appl.request_type,
        main_appl.product_purpose,
        main_appl.sa_id,
        main_appl.poi_required,
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
        main_appl.customer_undecided_agent,
        main_appl.customer_undecided_date,
        main_appl.pre_agreement_accept_date,
        main_appl.pre_agreement_accept_agent,
        main_appl.product_offer_date,
        main_appl.product_offer_agent,
        main_appl.product_offer_accept_date,
        main_appl.product_offer_accept_agent,
        TO_CHAR(MIN(pact.activation_date),'DD/MM/YYYY') AS activation_date, --Added Activations to this procedure as fix to Activations not matching with Qlikview NBR APP
        CASE
          WHEN trim(main_appl.credit_application_status) IN ('Declined','Expired','Withdrawn','Complete','FRAUD Declined','Cancelled','CASA Declined','POI Expired')
          THEN 'No'
          ELSE 'Yes'
        END AS app_in_flight,
        main_appl.latest_dea_date ,
        main_appl.dea_ind,
        main_appl.dea_consent_agent,
        base_fica.first_fica_pending_date,
        base_fica.first_fica_successful_date,
        base_poi.first_successful_poi_date ,             --AS FIRST_SUCCESSFUL_POI_DATE,
        base_poi.first_successful_poi_status ,           --AS FIRST_SUCCESSFUL_POI_STATUS,
        base_poi.first_unsuccessful_poi_date ,           --AS FIRST_UNSUCCESSFUL_POI_DATE,
        base_poi.first_unsuccessful_poi_status ,         --AS FIRST_UNSUCCESSFUL_POI_STATUS,
        base_poi.latest_poi_decline_date ,               --AS LATEST_POI_DECLINE_DATE,
        base_poi.latest_poi_decline_status ,             --AS Latest POI Decline Status,
        base_poi.latest_poi_date ,                       --AS LATEST_POI_DATE,
        base_poi.latest_poi_status ,                     --AS LATEST_POI_STATUS,
        base_poi.first_wf_poi_date ,                     --AS FIRST_WF_POI_DATE,
        base_poi.poi_expiry_date ,                       --AS POI_EXPIRY_DATE ,
        base_poi.product_purpose AS poi_product_purpose, --AS POI Product Purpose,
        booked_acc.booked_account_date ,                 --AS Booked Account Date,
        CASE
          WHEN main_appl.product_selected     = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END AS app,
        CASE
          WHEN main_appl.product_offered      = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END AS ph1,
        CASE
          WHEN main_appl.product_accepted     = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END AS application_accepted,
        CASE
          WHEN main_appl.pre_agreement_accept_date IS NULL
          THEN 0
          ELSE 1
        END AS pre_agreement_accepted,
        CASE
          WHEN main_appl.pre_agreement_accept_date IS NULL
          THEN 0
          WHEN main_appl.poi_required = 'FALSE'
          THEN 1
          ELSE 0
        END AS pre_agreement_accepted_poi,
        CASE
          WHEN pact.activation_date IS NULL
          THEN 0
          ELSE 1
        END AS product_activated
      FROM
        --dwh_wfs_performance.wfs_nbr_tmp_product_appl_test main_appl
        dwh_wfs_performance.wfs_nbr_tmp_main_product_appl main_appl
      LEFT OUTER JOIN dwh_wfs_performance.vs_calendar_wfs fin_cal
      ON TRUNC (main_appl.app_date) = fin_cal.calendar_date
      LEFT OUTER JOIN apex_app_wfs_01.apex_dwh_wfs_stores stores
      ON main_appl.store_number = stores.location_no
      LEFT OUTER JOIN dwh_wfs_performance.wfs_nbr_tmp_base_poi base_poi
      ON main_appl.credit_applications_id = base_poi.credit_applications_id
      AND trim(main_appl.product_name)    = trim(base_poi.product_type)
      LEFT OUTER JOIN dwh_wfs_performance.wfs_nbr_tmp_base_fica base_fica
      ON main_appl.credit_applications_id = base_fica.credit_applications_id
      AND trim(main_appl.product_name)    = trim(base_fica.product_type)
      LEFT OUTER JOIN dwh_wfs_performance.wfs_nbr_tmp_booked_acc booked_acc
      ON main_appl.credit_applications_id = booked_acc.credit_applications_id
      AND trim(main_appl.product_name)    = trim(booked_acc.product_type)
      LEFT OUTER JOIN dwh_wfs_performance.wfs_product_activation pact --Added Activations to this procedure as fix to Activations not matching with Qlikview NBR APP
      ON pact.credit_applications_id   = main_appl.credit_applications_id
      AND trim(main_appl.product_name) = trim(pact.product_name)
      GROUP BY main_appl.app_number,
        main_appl.app_date,
        main_appl.application_agent,
        fin_cal.wfs_year,
        fin_cal.wfs_year_quarter,
        fin_cal.wfs_year_month,
        fin_cal.wfs_year_week,
        main_appl.store_number,
        CASE
          WHEN stores.tier IS NULL
          THEN 'Unknown'
          ELSE tier
        END ,
        stores.location_name,
        main_appl.ma_entered_time_stamp ,
        main_appl.ma_app_number ,
        main_appl.split_app,
        main_appl.customer_application,
        main_appl.customer_product,
        main_appl.customer_application_product,
        main_appl.entered_time_stamp,
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
        main_appl.customer_undecided_agent,
        main_appl.customer_undecided_date,
        main_appl.pre_agreement_accept_date,
        main_appl.pre_agreement_accept_agent,
        main_appl.product_offer_date,
        main_appl.product_offer_agent,
        main_appl.product_offer_accept_date,
        main_appl.product_offer_accept_agent,
        CASE
          WHEN trim(main_appl.credit_application_status) IN ('Declined','Expired','Withdrawn','Complete','FRAUD Declined','Cancelled','CASA Declined','POI Expired')
          THEN 'No'
          ELSE 'Yes'
        END,
        main_appl.latest_dea_date ,
        dea_consent_agent,
        main_appl.dea_ind,
        base_fica.first_fica_pending_date,
        base_fica.first_fica_successful_date,
        base_poi.first_successful_poi_date ,
        base_poi.first_successful_poi_status ,
        base_poi.first_unsuccessful_poi_date ,
        base_poi.first_unsuccessful_poi_status ,
        base_poi.latest_poi_decline_date ,
        base_poi.latest_poi_decline_status ,
        base_poi.latest_poi_date ,
        base_poi.latest_poi_status ,
        base_poi.first_wf_poi_date ,
        base_poi.poi_expiry_date ,
        base_poi.product_purpose ,
        booked_acc.booked_account_date ,
        CASE
          WHEN main_appl.product_selected     = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END ,
        CASE
          WHEN main_appl.product_offered      = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END ,
        CASE
          WHEN main_appl.product_accepted     = 1
          OR main_appl.bonus_product_accepted = 1
          THEN 1
          ELSE 0
        END ,
        CASE
          WHEN main_appl.pre_agreement_accept_date IS NULL
          THEN 0
          ELSE 1
        END ,
        CASE
          WHEN main_appl.pre_agreement_accept_date IS NULL
          THEN 0
          WHEN main_appl.poi_required = 'FALSE'
          THEN 1
          ELSE 0
        END ,
        CASE
          WHEN pact.activation_date IS NULL
          THEN 0
          ELSE 1
        END
      )
    )
  GROUP BY wfs_year_week,
    wfs_year,
    wfs_year_quarter,
    wfs_year_month,
    entered_time_stamp,
    app_date,
    application_agent,
    master_app_number,
    split_app,
    app_number,
    channel,
    app_channel,
    campaign_id,
    promotion_id,
    product_name,
    request_type,
    credit_application_status,
    credit_applications_id,
    product_purpose,
    insurance_type,
    poi_required,
    customer_application_product,
    app,
    product_selected,
    product_offered,
    product_accepted,
    bonus_product_offered,
    bonus_product_accepted,
    ph1,
    pre_agreement_accepted,
    pre_agreement_accepted_poi,
    paa_with_poi_pending_wf,
    paa_with_poi_declined_wf,
    paa_with_poi_expired_wf,
    paa_with_poi_wf,
    fica_pending,
    fica_wf,
    product_complete,
    bonus_product_complete,
    booked_accounts_wf,
    product_activated,
    bpi_count,
    email_count,
    activation_date ,
    customer_undecided_date ,
    customer_undecided_agent ,
    pre_agreement_accept_date ,
    pre_agreement_accept_agent ,
    product_offer_date ,
    product_offer_agent ,
    product_offer_accept_date ,
    product_offer_accept_agent ,
    product_activated ,
    sa_id ,
    CASE
      WHEN created_by = 'guest'
      THEN regexp_replace(guest_number,'[[:alpha:]]')
      ELSE created_by
    END,
    store_number ,
    location_name ,
    tier ,
    delivery_store ,
    delivery_type ,
    app_in_flight ,
    dea_ind ,
    dea_consent_agent ,
    latest_dea_date ,
    first_successful_poi_date ,
    first_unsuccessful_poi_date ,
    first_wf_poi_date ,
    first_fica_pending_date ,
    first_fica_successful_date ,
    latest_poi_decline_date ,
    poi_expiry_date ,
    booked_account_date ,
    risk_category ,
    solicitation ,
    application_accepted ,
    credit_limit ,
    CASE
      WHEN promotion_id IN (7,9,11,13,15,17,19,21,26,27,28,403,407)
      THEN 'Prospecting'
      WHEN promotion_id IN (6,8,10,12,14,16,18,20,23,24,25,29,30,35,36,37,38,39,40,41,400,404)
      THEN 'X-Sell'
    END , -- Hardcoded these promotion codes from the excel file
    CASE
      WHEN product_name          = 'CreditCard'
      AND bonus_product_accepted = 1
      AND booked_accounts_wf     = 1
      THEN 1
      ELSE 0
    END ,
    app_date,
    CASE
      WHEN product_name      = 'CreditCard'
      AND product_accepted   = 1
      AND booked_accounts_wf = 0
      THEN 1
      ELSE 0
    END
  ); 

    g_recs_read     := g_recs_read     + sql%rowcount;
    g_recs_inserted := g_recs_inserted + sql%rowcount;

  commit;

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('DD MON YYYY  HH24:MI:SS'));
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

    commit;
    p_success := true;


  exception

  when dwh_errors.e_insert_error then
    l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    rollback;
    p_success := false;
    raise;

  when others then
    l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    rollback;
    p_success := false;
    raise;


end WH_PRF_WFS_690U;
