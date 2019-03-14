-- ****** Object: Procedure W7131037.WH_PRF_CUST_384E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_384E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
as
--**************************************************************************************************
--  Date:        APR 2016
--  Author:      Theo Filander
--  Purpose:     Create the Monthly Customer Experience interface for SVOC.
--               with input ex cust_basket_item table from performance layer.
--               THIS JOB RUNS DAILY
--  Tables:      Input  - OUT_DWH_SVOC_DAILY
--                      - FND_SVOC_MAPPING
--               Output - OUT_DWH_SVOC_DAILY_SK
--  Packages:    constants, dwh_log, dwh_valid
--
--  Remarks:
--  10 July 2017 - Theo Filander
--                 Roll the date from OUT_DWH_SVOC_DAILY to OUT_DWH_SVOC_DAILY_SK
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
g_found              boolean;
g_date               date          := trunc(sysdate);


g_run_date           date          := NVL(p_run_date,trunc(sysdate));


g_stmt               varchar2(300);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_384E';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE DAILY SUBSCRIBER KEY SVOC DAILY DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

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

    l_text := 'BUILD OF SVOC DAILY TO OUT_DWH_SVOC_DAILY_SK STARTED AT '||
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

        execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Main loop
--**************************************************************************************************

    l_text := 'EXTRACT DATA CREATED ON :- '||TO_CHAR(g_run_date,'DD MON YY');
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    l_text := 'Truncate OUT_DWH_SVOC_DAILY_SK.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table "W7131037"."OUT_DWH_SVOC_DAILY_SK"';
    commit;

    l_text := 'UPDATE STATS ON OUT_DWH_SVOC_DAILY_SK TABLES_SK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','OUT_DWH_SVOC_DAILY_SK',estimate_percent=>1, DEGREE => 32);

    commit;

    l_text := 'Populate TEMP_SUBSCRIBER_UCOUNT. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table TEMP_SUBSCRIBER_UCOUNT';
    commit;

    l_text := 'UPDATE STATS ON TEMP_SUBSCRIBER_UCOUNT TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','TEMP_SUBSCRIBER_UCOUNT',estimate_percent=>1, DEGREE => 32);

    insert /*+ Parallel(ts,6)  */ into W7131037.temp_subscriber_ucount ts
    select  distinct
           subscriber_key,
           cast(case when cast(ucm as varchar2(30)) != '0' then 'UCount Matched'
                     when cast(ucr as varchar2(30)) != '0' then 'UCount Redeemed'
                     when cast(ucp as varchar2(30)) != '0' then 'UCount Potential'
                     else NULL
                end as varchar2(50)) ucount_cust_type
      from (
            select /*+ Parallel(ci,6) Full(ci) Parallel(sm,6) Full(sm) */
                   subscriber_key,
                   ucount_cust_type
              from W7131037.out_dwh_svoc_daily ci
             inner join W7131037.fnd_svoc_mapping sm on ci.primary_customer_identifier = sm.source_key
             where sm.source = 'C2'
             and rownum <=100000
             group by subscriber_key,
                      ucount_cust_type
            )
            PIVOT
            (
             count(ucount_cust_type)
             for ucount_cust_type in ( 'UCount Matched' as ucm,'UCount Redeemed' as ucr,'UCount Potential' as ucp )
            );
        commit;
        l_text := 'UPDATE STATS ON TEMP_SUBSCRIBER_UCOUNT TABLE';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        DBMS_STATS.gather_table_stats ('W7131037','TEMP_SUBSCRIBER_UCOUNT',estimate_percent=>1, DEGREE => 32);
        commit;

        l_text := 'Populate OUT_DWH_SVOC_DAILY_SK. ' ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert into W7131037.OUT_DWH_SVOC_DAILY_SK sk
    select
           uc.subscriber_key,
           uc.ucount_cust_type,
           max(wfs_app_reg_ind)                 wfs_app_reg_ind,
           max(one_app_registration_indicator)  one_app_registration_indicator,
           min(one_app_registration_date)       one_app_registration_date,
           max(last_one_app_login_date)         last_one_app_login_date,
           max(last_one_app_order_date)         last_one_app_order_date,
           min(last_wifi_login_date)            last_wifi_login_date,
           max(last_online_login_date)          last_online_login_date,
           max(last_online_order_date)          last_online_order_date,
           min(nownow_app_registration_ind)     nownow_app_registration_ind,
           min(nownow_app_registration_date)    nownow_app_registration_date,
           min(last_nownow_app_login_date)      last_nownow_app_login_date,
           min(last_nownow_app_order_date)      last_nownow_app_order_date,
           max(last_vitality_purchase_date)     last_vitality_purchase_date,
           sum(no_of_vouchers_issued_past3m)    no_of_vouchers_issued_past3m,
           sum(no_of_vouchers_redeemed_past3m)  no_of_vouchers_redeemed_past3m,
           sum(number_of_active_vouchers)       number_of_active_vouchers,
           min(dm_customer_type)                dm_customer_type,
           min(customer_location)               customer_location,
           trunc(sysdate)      create_date
      from W7131037.temp_subscriber_ucount uc
     inner join W7131037.fnd_svoc_mapping sm on sm.subscriber_key = uc.subscriber_key
     inner join W7131037.out_dwh_svoc_daily ci on ci.primary_customer_identifier = sm.source_key
     where sm.source = 'C2'
     group by uc.subscriber_key,
              uc.ucount_cust_type,
              trunc(sysdate);

    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;
    l_text := 'UPDATE STATS ON OUT_DWH_SVOC_DAILY_SK TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','OUT_DWH_SVOC_DAILY_SK',estimate_percent=>1, DEGREE => 32);
    commit;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
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
       rollback;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

end "WH_PRF_CUST_384E";
