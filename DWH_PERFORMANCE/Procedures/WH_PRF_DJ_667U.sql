--------------------------------------------------------
--  DDL for Procedure WH_PRF_DJ_667U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_DJ_667U" 
                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2015
--  Author:      Alfonso Joshua
--  Purpose:     Rollup to rtl_contrct_chn_item_wk_po_dj.
--               Only for CHBD
--  Tables:      Input  - rtl_po_supchain_loc_item_dy_dj
--               Output - rtl_contrct_chn_item_wk_po_dj
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_inserted      integer       :=  0;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_DJ_667U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP TO CONTRACT CHAIN ITEM WK PO';
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
    l_text := 'ROLLUP OF rtl_contrct_chn_item_wk_po_dj EX PO SUPCHAIN STARTED '||
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

    execute immediate 'truncate table dwh_performance.rtl_contrct_chn_item_wk_po_dj';
    l_text := 'TABLE rtl_contrct_chn_item_wk_po_dj TRUNCATED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /* append */ into rtl_contrct_chn_item_wk_po_dj
    select   lid.sk1_contract_no,
             dl.sk1_chain_no,
             lid.sk1_item_no,
             dc.fin_year_no,
             dc.fin_week_no,
             max(dc.fin_week_code) fin_week_code,
             max(dc.this_week_start_date) this_week_start_date,
             max(lid.sk2_item_no) sk2_item_no,
             sum(lid.actl_grn_qty) actl_grn_qty,
             sum(lid.actl_grn_selling) actl_grn_selling,
             sum(lid.actl_grn_cost) actl_grn_cost,
             sum(lid.latest_po_qty) latest_po_qty,
             sum(lid.latest_po_selling) latest_po_selling,
             sum(lid.latest_po_cost) latest_po_cost,
             (case when 1 = 2 then 0 end) num_du,   -- to force a null value
             sum(lid.num_weighted_days_to_deliver) num_weighted_days_to_deliver,
             g_date last_updated_date,
             sum(lid.amended_po_qty) amended_po_qty,
             sum(lid.amended_po_selling) amended_po_selling,
             sum(lid.amended_po_cost) amended_po_cost,
             sum(lid.bc_shipment_qty) bc_shipment_qty,
             sum(lid.bc_shipment_selling) bc_shipment_selling,
             sum(lid.bc_shipment_cost) bc_shipment_cost,
             sum(lid.po_grn_qty) po_grn_qty,
             sum(lid.po_grn_selling) po_grn_selling,
             sum(lid.po_grn_cost) po_grn_cost,
             lid.sk1_chain_code_ind
    from     rtl_po_supchain_loc_item_dy_dj lid
    join     dim_location dl                  on  lid.sk1_location_no = dl.sk1_location_no
    join     dim_item di                      on  lid.sk1_item_no     = di.sk1_item_no
    join     dim_calendar dc                  on  lid.tran_date       = dc.calendar_date
    where    di.business_unit_no in(51,52,53,54,55)
    group by lid.sk1_contract_no,
             dl.sk1_chain_no,
             lid.sk1_item_no,
             dc.fin_year_no,
             dc.fin_week_no,
             lid.sk1_chain_code_ind;

    g_recs_read     := g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    commit;

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

end wh_prf_dj_667u;
