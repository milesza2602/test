--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_600U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_600U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Product Activations - open applications update
--  Date:        2016-10-06
--  Author:      Naresh Chauhan
--  Purpose:     Update  WFS_OPEN_APPLICATION - intermediate table in the performance layer 
--               or product applications still to be examined for activations. 
--               with input ex 
--                    fnd_wfs_om4_workflow
--                    fnd_wfs_om4_cr_detail
--                    fnd_wfs_om4_offer
--                    fnd_wfs_om4_application
--                    
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    fnd_wfs_om4_workflow
--                    fnd_wfs_om4_cr_detail
--                    fnd_wfs_om4_offer
--                    fnd_wfs_om4_application
--               Output - WFS_OPEN_APPLICATION
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2016-10-09 N Chauhan - created - based on WH_PRF_CUST_322U
--  2016-10-26 N Chauhan - Queries optimised.
--  2016-12-02 N Chauhan - remove applications that have disqualified since inserted.
--  2016-12-14 N Chauhan - remove remnant already activated applications.
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
g_rec_out            WFS_OPEN_APPLICATION%rowtype;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_600U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Product Activations - open applications update';
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

    l_text := 'WFS Product open applications update STARTED AT '||
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
    l_text := 'LOAD TABLE: '||'WFS_OPEN_APPLICATION' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';


    insert /*+ Append parallel(oa, 4) */
    into dwh_WFS_PERFORMANCE.WFS_OPEN_APPLICATION oa

    WITH  
        min_wf  -- earliest workflow record
        AS (SELECT /*+ parallel(wf,4)  */  -- lower cost with filter on indesx and without full(wf) hint 
                    wf.credit_applications_id,
                     MIN (wf.activity_timestamp) AS first_activity
                FROM fnd_wfs_om4_workflow wf
                where wf.ACTIVITY_TIMESTAMP > sysdate - 10   -- scan last 10 days for new entries
            GROUP BY wf.credit_applications_id
            ),
        crd_dtl_offr
        AS (SELECT /*+ parallel(cr_dtl,4) full(cr_dtl)  */ 
                    DISTINCT
                   cr_dtl.credit_applications_id, cr_dtl.product_name
              FROM fnd_wfs_om4_cr_detail cr_dtl
                   INNER JOIN fnd_wfs_om4_offer offer
                      ON (    cr_dtl.offer_id = offer.offer_id
                          AND offer.origin =
                                 'OFFER_ORIGIN_TYPE_DECISION_SERVICE'
                          AND offer.duplicate_ind = 0)
            )
    SELECT /*+ parallel(app,4) full(app) full(opn) full(act) */ 
         app.app_number,
          crd_dtl_offr.product_name,
          app.credit_applications_id,
          app.sa_id,
          NVL (min_wf.first_activity, app.entered_time_stamp)
             AS application_timestamp,
          SYSDATE AS LAST_UPDATED_DATE
     FROM fnd_wfs_om4_application app
          LEFT JOIN min_wf
             ON (app.credit_applications_id = min_wf.credit_applications_id)
          LEFT JOIN crd_dtl_offr
             ON (app.credit_applications_id = crd_dtl_offr.credit_applications_id)
          Left outer join WFS_OPEN_APPLICATION opn
             on (opn.app_number = app.app_number and 
                 opn.product_name = crd_dtl_offr.product_name)
          left outer join WFS_PRODUCT_ACTIVATION act
             on (act.app_number = app.app_number and 
                 act.product_name = crd_dtl_offr.product_name)
    WHERE crd_dtl_offr.product_name IS NOT NULL
     and opn.app_number is null  -- exclude what already in WFS_OPEN_APPLICATION
     and act.app_number is null  -- exclude what already in WFS_PRODUCT_ACTIVATION
     ;              

    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

    -- For Credit Cards, remove any existing open applications 
    -- whose cr_detail.product_purpose has changed in the meantime,  
    -- to disqualify from being activated.

    delete /*+ full(t), parallel(t,4) */
    from (
    select  /*+ parallel(oap, 4) */
    * from dwh_wfs_performance.wfs_open_application oap 
    where (app_number, product_name) in (
    with 
    keep_cc_open_apps as    -- determine what to keep to eliminate the rest.
    (
    select /*+ parallel(a,4) parallel(t,4) parallel(o,4)  */ 
    a.app_number,
    a.product_name
       from 
         dwh_wfs_performance.wfs_open_application a, 
         fnd_wfs_om4_cr_detail t, 
         fnd_wfs_om4_offer o
       where 
        a.product_name = 'CreditCard' and 
        t.credit_applications_id = a.credit_applications_id and
        o.offer_id = t.offer_id and 
        o.origin = 'OFFER_ORIGIN_TYPE_DECISION_SERVICE' and  
        o.duplicate_ind = 0 and
        t.product_purpose not in ('Decline', 'Customer Cancelled') 
       group by
    a.app_number,
    a.product_name
    )
    select /*+ parallel(a,4) full(k) */
       a.app_number,
       a.product_name
       from 
         dwh_wfs_performance.wfs_open_application a  
         left outer join keep_cc_open_apps k     
            on (k.app_number = a.app_number and k.product_name=a.product_name)
         where a.product_name = 'CreditCard'
         and k.app_number is null
         group by a.app_number, a.product_name, k.app_number
    )) t
    ;
    
    g_recs_deleted :=  g_recs_deleted + SQL%ROWCOUNT;
    
    commit;

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


end wh_prf_wfs_600u;
