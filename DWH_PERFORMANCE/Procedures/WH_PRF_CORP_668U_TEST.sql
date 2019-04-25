--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_668U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_668U_TEST" 
                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Combines PO and BOC into Contract Chain Item WK.
--               Contract tables will contain data for CHBD only.
--  Tables:      Input  - RTL_CONTRACT_CHAIN_ITEM_WKQ_po,
--                        RTL_CONTRACT_CHAIN_ITEM_WKQ_boc
--               Output - RTL_CONTRACT_CHAIN_ITEM_WKQ
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
  --
  --  25 MAY 2010 - DEFECT 3805 - remove sk2_item_no processing
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--  test migration
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_668U_TEST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'COMBINES PO AND BOC TO CONTRACT CHAIN ITEM WK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'COMBINE PO and BOC to RTL_CONTRACT_CHAIN_ITEM_WKQ STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';
    execute immediate 'truncate table dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WKQ';
    l_text := 'TABLE RTL_CONTRACT_CHAIN_ITEM_WKQ TRUNCATED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'ALTER TABLE dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WKQ DISABLE CONSTRAINT PK_N_RTL_CNTRCT_CHN_ITM_WKQ';
    l_text := 'CONSTRAINT DISABLED ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert   /*+ parallel(cont_wk,4) */ into dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WKQ cont_wk
    select   /*+ PARALLEL(WPO,4) FULL(WPO) PARALLEL(WBOC,4) FULL(WBOC) */
             nvl(wpo.sk1_contract_no,wboc.sk1_contract_no) sk1_contract_no,
             nvl(wpo.sk1_chain_no,wboc.sk1_chain_no) sk1_chain_no,
             nvl(wpo.sk1_item_no,wboc.sk1_item_no) sk1_item_no,
             nvl(wpo.fin_year_no,wboc.fin_year_no) fin_year_no,
             nvl(wpo.fin_week_no,wboc.fin_week_no) fin_week_no,
             nvl(wpo.fin_week_code,wboc.fin_week_code) fin_week_code,
             nvl(wpo.this_week_start_date,wboc.this_week_start_date) this_week_start_date,
             wboc.contract_status_code,
             wboc.contract_qty,
             wboc.contract_selling,
             wboc.contract_cost,
             wpo.actl_grn_qty,
             wpo.actl_grn_selling,
             wpo.actl_grn_cost,
             wpo.amended_po_qty,
             wpo.amended_po_selling,
             wpo.amended_po_cost,
             wpo.bc_shipment_qty,
             wpo.bc_shipment_selling,
             wpo.bc_shipment_cost,
             wpo.po_grn_qty,
             wpo.po_grn_selling,
             wpo.po_grn_cost,
             wpo.latest_po_qty,
             wpo.latest_po_selling,
             wpo.latest_po_cost,
             wboc.boc_qty,
             wboc.boc_selling,
             wboc.boc_cost,
             wboc.boc_qty_all_time,
             wboc.boc_selling_all_time,
             wboc.boc_cost_all_time,
             (case when 1 = 2 then 0 end) num_du,   -- to force a null value
             wpo.num_weighted_days_to_deliver,
             g_date last_updated_date
    from     dwh_performance.rtl_contract_chain_item_wk_po wpo
    full     outer join dwh_performance.rtl_contract_chain_item_wk_boc wboc
             on    wpo.sk1_contract_no     = wboc.sk1_contract_no
             and   wpo.sk1_chain_no        = wboc.sk1_chain_no
             and   wpo.sk1_item_no         = wboc.sk1_item_no
             and   wpo.fin_year_no         = wboc.fin_year_no
             and   wpo.fin_week_no         = wboc.fin_week_no;

    g_recs_read     := g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    commit;
    
    execute immediate 'ALTER TABLE dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WKQ ENABLE CONSTRAINT PK_N_RTL_CNTRCT_CHN_ITM_WKQ';
    l_text := 'CONSTRAINT ENABLED ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'STARTING GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_CONTRACT_CHAIN_ITEM_WKQ', DEGREE => 32);
    commit;
    
    l_text := 'DONE GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end wh_prf_corp_668u_test;
