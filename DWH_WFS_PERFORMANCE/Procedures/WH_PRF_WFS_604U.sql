--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_604U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_604U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Product Activations - Credit Cards
--  Date:        2016-10-06
--  Author:      Naresh Chauhan
--  Purpose:     Update  WFS_PRODUCT_ACTIVATION fact table in the performance layer
--               with input ex 
--                    wfs_open_application
--                    fnd_wfs_customer_absa
--                    fnd_wfs_card_accnt_type
--                    fnd_wfs_crd_acc_dly
--                    fnd_wfs_crd_txn_dly
--               for Credit Cards
--  
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    wfs_open_application
--                    fnd_wfs_customer_absa
--                    fnd_wfs_card_accnt_type
--                    fnd_wfs_crd_acc_dly
--                    fnd_wfs_crd_txn_dly
--                    
--               Output - WFS_PRODUCT_ACTIVATION
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2016-10-09 N Chauhan - created - based on WH_PRF_CUST_322U
--  2016-10-26 N Chauhan - Queries optimised.
--  2016-12-02 N Chauhan - delete old credit card applications 
--                          by customer with activated credit card.
--  2016-12-06 N Chauhan - exclude from crd_acc_dly cust_key/acc already activated 
--  2016-12-14 N Chauhan - use account_no, not wfs_account_no for already activated check
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            WFS_PRODUCT_ACTIVATION%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_604U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Product Activations update for Credit Cards';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS Product Activations (cc) update STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD TABLE: '||'WFS_PRODUCT_ACTIVATION' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Main loop
--**************************************************************************************************

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
   execute immediate 'alter session enable parallel dml';

   insert  /*+ Append parallel(a, 4) */
   into dwh_wfs_performance.wfs_product_activation a
   WITH cust_key
        AS                           -- get customer_key for open applications
          (SELECT /*+ full(apps) parallel(cust_absa,4) full(cust_absa) parallel (apps,4) */ 
                  DISTINCT apps.app_number,
                           20 AS product_code_no,
                           apps.product_name,
                           apps.credit_applications_id,
                           cust_absa.customer_key,
                           apps.sa_id,
                           apps.application_timestamp
             FROM dwh_wfs_foundation.fnd_wfs_customer_absa cust_absa
                  INNER JOIN dwh_wfs_performance.wfs_open_application apps
                     ON (    TO_CHAR (apps.sa_id) = cust_absa.id_number
                         AND apps.product_name = 'CreditCard')),
        all_absa_acc_no
        AS             -- absa account numbers with customer key, for txn dly,
                                              -- includes transferred accounts
         (SELECT /*+ full(ck) full(acc_typ) parallel(cad,4)   full(cad) */ 
                 DISTINCT
                 ck.app_number,
                 ck.product_code_no,
                 ck.product_name,
                 ck.credit_applications_id,
                 ck.sa_id,
                 ck.application_timestamp,
                 FIRST_VALUE (
                    cad.customer_key)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS customer_key,
                 FIRST_VALUE (
                    cad.account_number)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS account_number,
                 FIRST_VALUE (
                    cad.closed_date)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS closed_date,
                 FIRST_VALUE (
                    cad.open_date)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS open_date,
                 FIRST_VALUE (
                    cad.card_account_status_code)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS card_account_status_code,
                 FIRST_VALUE (
                    cad.status_date)
                 OVER (
                    PARTITION BY cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS status_date,
                 FIRST_VALUE (
                    cad.information_date)
                 OVER (
                    PARTITION BY cad.account_number,
                                 cad.customer_key,
                                 cad.card_account_status_code
                    ORDER BY cad.information_date ASC)
                    AS information_date,
                 TRIM (
                    FIRST_VALUE (
                       cad.transfer_account_number)
                    OVER (
                       PARTITION BY cad.customer_key,
                                    cad.card_account_status_code
                       ORDER BY cad.information_date ASC))
                    AS transfer_account_number
            FROM fnd_wfs_crd_acc_dly cad
                 INNER JOIN
                 dwh_wfs_foundation.FND_WFS_CARD_ACCNT_TYPE acc_typ
                    ON (acc_typ.card_accnt_type_code =
                           cad.card_account_type_code)
                 INNER JOIN cust_key ck
                    ON (ck.customer_key = cad.customer_key)
           WHERE     cad.card_account_status_code IN ('XFA', 'AAA')
                 AND cad.information_date > TRUNC (SYSDATE - 10)
                 /* for initial full take-on,   */
                 /* remove this date filter to run this for the full cad table  ************/
 
        ),
        original_absa_acc_no
        AS                    -- exclude subsequent account nos. if acc tranferred
          ( SELECT /*+ full(absa) parallel(absa,4) */ 
                    absa.customer_key,
                    MAX (absa.app_number) AS app_number,
                    MAX (absa.product_code_no) AS product_code_no,
                    MAX (absa.product_name) AS product_name,
                    MAX (absa.credit_applications_id)
                       AS credit_applications_id,
                    CASE
                       WHEN absa.transfer_account_number IS NULL
                       THEN
                          account_number
                       WHEN absa.card_account_status_code = 'XFA'
                       THEN
                          account_number
                       ELSE
                          transfer_account_number
                    END
                       AS account_number,
                    MAX (absa.sa_id) AS sa_id,
                    MAX (card_account_status_code) AS card_account_status_code,
                    MAX (absa.application_timestamp) AS application_timestamp,
                    MAX (absa.information_date) AS activation_date
               FROM all_absa_acc_no absa
           GROUP BY absa.customer_key,
                     CASE
                       WHEN absa.transfer_account_number IS NULL
                       THEN
                          account_number
                       WHEN absa.card_account_status_code = 'XFA'
                       THEN
                          account_number
                       ELSE
                          transfer_account_number
                    END
          ),
        original_absa_not_activated
        as                -- exclude cust_key/acc already activated
        ( select  /*+ parallel(absa,4) parallel(act,4) full(act) */ 
            absa.* 
          from original_absa_acc_no absa
               left outer join WFS_PRODUCT_ACTIVATION act
                         on (act.ACCOUNT_NUMBER = absa.ACCOUNT_NUMBER and 
                             act.CUSTOMER_KEY = absa.CUSTOMER_KEY and 
                             act.product_code_no = absa.product_code_no  )
          WHERE act.ACCOUNT_NUMBER is null  -- exclude cust_key/acc already in WFS_PRODUCT_ACTIVATION
         )
          
 
   SELECT /*+ full(absa) parallel(txn,4) parallel (absa,4) full(txn) */ 
          DISTINCT
          absa.app_number,
          absa.product_code_no,
          absa.product_name,
          absa.credit_applications_id,
          absa.account_number,
          absa.customer_key,
          NULL AS wfs_account_no,
          NULL AS wfs_customer_no,
          absa.sa_id,
          NULL AS txn_type_code_sc_pl,
          FIRST_VALUE (txn.card_txn_type_code)
             OVER (PARTITION BY txn.account_number ORDER BY txn.txn_date)
             AS txn_type_code_cc,
          absa.application_timestamp,
          FIRST_VALUE (txn.txn_date)
             OVER (PARTITION BY txn.account_number ORDER BY txn.txn_date)
             AS activation_date,
          FIRST_VALUE (txn.card_amt)
             OVER (PARTITION BY txn.account_number ORDER BY txn.txn_date)
             AS activation_amount,
          SYSDATE AS last_updated_date
     FROM dwh_wfs_foundation.fnd_wfs_crd_txn_dly txn
          INNER JOIN original_absa_not_activated absa
             ON (absa.account_number = txn.account_number)
          left outer join WFS_PRODUCT_ACTIVATION act
             on (act.app_number = absa.app_number and 
                 act.product_code_no = absa.product_code_no)
    WHERE act.app_number is null  -- exclude what is already in WFS_PRODUCT_ACTIVATION
         and txn.txn_date >= absa.application_timestamp
         AND txn.card_txn_type_code IN (
             1035, 1072, 1135, 1295, 1036, 1071, 1136, 1296, 1039, 1289, 1040,
             1290, 1263, 1147, 1029, 1030, 1021, 1053, 1022, 1054, 1037, 1038, 
             1031, 1032, 1063, 1067, 1064, 1070, 1025, 1045, 1291, 1337, 1339,
             1120, 1600, 1016, 1026, 1046, 1338, 1601, 1120, 1017, 1292, 1028  )

          AND txn.txn_date > SYSDATE-10;  
          -- check last 10 days in case this load hasn't run for a few days.
                 /* for initial full take-on,   */
                 /* remove this date filter to run this for the full txn table  ************/
  
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  commit;

  l_text := 'WFS Product Activations (cc) Insert completed, delete started at '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



  -- delete activated products from wfs_open_application
    
  delete 
  from dwh_wfs_performance.wfs_open_application apps
  where (APP_NUMBER, PRODUCT_NAME) in (
    select APP_NUMBER, PRODUCT_NAME 
    from dwh_wfs_performance.wfs_product_activation
  );

  g_recs_deleted     :=  g_recs_deleted + SQL%ROWCOUNT;
   
  commit;


  
  -- delete old credit card applications by customer with activated credit card.

/*  not required anymore because of load fix

  delete 
  from (
    select * from dwh_wfs_performance.wfs_open_application 
    where (app_number, product_name) in 
      (
       with 
       already_activated as 
         (
          select 
          distinct 
          sa_id, activation_date
          from dwh_wfs_performance.WFS_PRODUCT_ACTIVATION 
          where product_code_no=20  -- CreditCard 
          )
       select o.app_number, o.product_name 
       from dwh_wfs_performance.wfs_open_application o
       inner join already_activated a on ( a.sa_id = o.sa_id  )
       where 
         o.product_name='CreditCard' and 
         o.application_timestamp <= a.activation_date
      )
  )
  ;
  
  g_recs_deleted     :=  g_recs_deleted + SQL%ROWCOUNT;
  
  */
  
  commit;

      
    

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    l_text :=  l_text||'  -  FROM TABLE WFS_OPEN_APPLICATION';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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


end wh_prf_wfs_604u;
