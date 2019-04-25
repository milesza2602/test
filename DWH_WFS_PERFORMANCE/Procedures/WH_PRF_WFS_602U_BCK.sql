--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_602U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_602U_BCK" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Product Activations - Store cards and Personal Loans
--  Date:        2016-10-06
--  Author:      Naresh Chauhan
--  Purpose:     Update  WFS_PRODUCT_ACTIVATION fact table in the performance layer
--               with input ex 
--                    wfs_open_application
--                    fnd_wfs_tran
--                    dim_wfs_all_prod
--                    fnd_wfs_tran_day
--               for Store cards and Personal Loans
--  
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    wfs_open_application
--                    dim_wfs_all_prod
--               Output - WFS_PRODUCT_ACTIVATION
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2016-10-09 N Chauhan - created - based on WH_PRF_CUST_322U
--  2016-10-26 N Chauhan - Queries optimised.
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_602U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Product Activations update for Store cards and Personal Loans';
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

    l_text := 'WFS Product Activations (sc & pl) update STARTED AT '||
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

  insert into dwh_wfs_performance.wfs_product_activation
  WITH tran
     AS (SELECT /*+ full(tran) */
               tran.tran_code
           FROM fnd_wfs_tran tran
          WHERE tran.tran_code_desc IN
                   ('PL DRAWDOWN',
                    'PL DRAWDOWN VIA TELEGRAPHIC TRANSFER DR',
                    'ABSA PL DRAWDOWN',
                    'PURCHASE',
                    'WASTE PURCHASE',
                    'IN-THE-BAG PURCHASE',
                    'ITB PURCHASE',
                    'CARD REPLACEMENT FEE')),
     open_app_cus
     AS (SELECT /*+ full(apps) parallel(apps,4) */
               apps.app_number,
                apps.credit_applications_id,
                apps.APPLICATION_TIMESTAMP,
                apps.sa_id,
                all_prd.wfs_customer_no,
                all_prd.wfs_account_no,
                all_prd.product_code_no,
                all_prd.DATE_FIRST_PURCH
           FROM dwh_wfs_performance.wfs_open_application apps
                INNER JOIN
                (SELECT /*+ full(cus) parallel(cus, 4) */ -- changed added full(cus)
                       APPLICATION_NO,
                        product_code_no,
                        wfs_account_no,
                        wfs_customer_no,
                        DATE_FIRST_PURCH
                   FROM dwh_cust_foundation.fnd_wfs_all_prod cus) all_prd
                   ON apps.app_number = all_prd.APPLICATION_NO),
     activations_all
     AS (SELECT /*+  full(open_app_cus) full(tran) parallel(t1,4) full(t1) */ -- changed added full(t1)
               oa_cus.app_number,
                t1.product_code_no,
                CASE
                   WHEN t1.product_code_no IN (1,
                                               2,
                                               3,
                                               7,
                                               8,
                                               9,
                                               2,
                                               4,
                                               5,
                                               6,
                                               21)
                   THEN
                      'StoreCard'
                   WHEN t1.product_code_no IN (11, 15, 16)
                   THEN
                      'PersonalLoan'
                   ELSE
                      NULL
                END
                   AS product_name,
                oa_cus.credit_applications_id,
                t1.wfs_account_no,
                t1.wfs_customer_no,
                oa_cus.sa_id,
                t1.tran_code,
                oa_cus.APPLICATION_TIMESTAMP,
                --t1.tran_posting_date,
                FIRST_VALUE (
                   t1.TRAN_EFFECTV_DATE)
                OVER (
                   PARTITION BY t1.wfs_account_no,
                                t1.wfs_customer_no,
                                t1.product_code_no
                   ORDER BY t1.tran_posting_date ASC)
                   AS dt_first_purchase,
                FIRST_VALUE (
                   t1.tran_code)
                OVER (
                   PARTITION BY t1.wfs_account_no,
                                t1.wfs_customer_no,
                                t1.product_code_no
                   ORDER BY t1.tran_posting_date ASC)
                   AS first_tran_code,
                FIRST_VALUE (
                   t1.tran_value)
                OVER (
                   PARTITION BY t1.wfs_account_no,
                                t1.wfs_customer_no,
                                t1.product_code_no
                   ORDER BY t1.tran_posting_date ASC)
                   AS first_tran_value
           FROM fnd_wfs_tran_day t1
                INNER JOIN open_app_cus oa_cus
                   ON (    oa_cus.wfs_customer_no = t1.wfs_customer_no
                       AND oa_cus.wfs_account_no = t1.wfs_account_no
                       AND oa_cus.product_code_no = t1.product_code_no
                       AND oa_cus.DATE_FIRST_PURCH <= t1.TRAN_EFFECTV_DATE)
                INNER JOIN tran
                   ON (t1.tran_code = tran.tran_code AND t1.tran_value > 0)
          WHERE (   t1.product_code_no IN (1,
                                           2,
                                           3,
                                           7,
                                           8,
                                           9,
                                           2,
                                           4,
                                           5,
                                           6,
                                           21)
                 OR t1.product_code_no IN (11, 15, 16)) -- to filter out irrelavant tran_day recs with indexed field
         )
  SELECT /*+ full(act_all) parallel(act_all,4) */ -- changed added act_all to parallel hint
       DISTINCT act_all.app_number,
                act_all.product_code_no,
                act_all.product_name,
                act_all.credit_applications_id,
                NULL AS ACCOUNT_NUMBER,
                NULL AS CUSTOMER_KEY,
                act_all.wfs_account_no,
                act_all.wfs_customer_no,
                act_all.sa_id,
                act_all.first_tran_code AS TXN_TYPE_CODE_SC_PL,
                NULL AS TXN_TYPE_CODE_CC,
                act_all.APPLICATION_TIMESTAMP,
                act_all.dt_first_purchase AS activation_date,
                act_all.first_tran_value AS tran_value,
                TRUNC (SYSDATE) AS LAST_UPDATED_DATE
    FROM activations_all act_all
          left outer join WFS_PRODUCT_ACTIVATION act
             on (act.app_number = act_all.app_number and 
                 act.product_code_no = act_all.product_code_no)
    WHERE act.app_number is null  -- exclude what is already in WFS_PRODUCT_ACTIVATION
  ;
 
  

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  commit;

  l_text := 'WFS Product Activations (sc & pl) Insert completed, delete started at '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  -- delete activated products from wfs_open_application
    
  delete from dwh_wfs_performance.wfs_open_application apps
  where (APP_NUMBER, PRODUCT_NAME) in (
    select APP_NUMBER, PRODUCT_NAME 
    from dwh_wfs_performance.wfs_product_activation
  );

  g_recs_deleted     :=  g_recs_deleted + SQL%ROWCOUNT;
    
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

end wh_prf_wfs_602u_bck;
