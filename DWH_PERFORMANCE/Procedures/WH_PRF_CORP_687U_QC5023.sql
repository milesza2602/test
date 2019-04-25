--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_687U_QC5023
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_687U_QC5023" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:         JULY 2014 ---  DATAFIX FOR QC5023
--  Author:       W LYTTLE
--  Purpose:      Rollup of PO data needed for JDA Assort Commitment
--  Tables:       Input  - rtl_po_supchain_loc_item_dy
--                Output - rtl_po_chain_sc_wk_QC5023
--  Packages:     constants, dwh_log, dwh_valid
--  Maintenance:
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
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            rtl_po_chain_sc_wk%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_687U_QC5023';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ADDS SHIPMENT INFO TO PO COMBINATION FACT';
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
    l_text := 'DATAFIX OF RTL_PO_CHAIN_SC_WK EX PERFORMANCE STARTED '||
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


    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

   INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.rtl_po_chain_sc_wk_QC5023
        select  
        /*+ PARALLEL (PO, 4) */
               po.sk1_po_no,
              dl.sk1_chain_no,
              di.sk1_style_colour_no,
              cal.fin_year_no,
              cal.fin_week_no,
              NULL FIN_WEEK_CODE,
              sum(po.original_po_qty)     original_po_qty,
              sum(po.original_po_selling) original_po_selling,
              sum(po.original_po_cost)    original_po_cost,
              sum(po.amended_po_qty)      amended_po_qty,
              sum(po.amended_po_selling)  amended_po_selling,
              sum(po.amended_po_cost)     amended_po_cost,
              sum(po.latest_po_qty)       latest_po_qty,
              sum(po.latest_po_selling)   latest_po_selling,
              sum(po.latest_po_cost)      latest_po_cost,
              null latest_po_cases,
              G_DATE LAST_UPDATED_DATE
    from      rtl_po_supchain_loc_item_dy po, 
    dim_item di, dim_location dl, dim_calendar cal, dim_contract con, dim_supply_chain_type sct
    where     po.sk1_item_no     = di.sk1_item_no
     and      po.sk1_location_no = dl.sk1_location_no
     and      po.tran_date       = cal.calendar_date
     and      po.po_ind            = 1
     and      di.business_unit_no  <> 50
     and      po.sk1_contract_no   = con.sk1_contract_no
     and      con.sk1_contract_no  = 0
     and      po.sk1_supply_chain_no = sct.sk1_supply_chain_no
     and      sct.supply_chain_code  = 'WH'
    group by po.sk1_po_no, dl.sk1_chain_no, di.sk1_style_colour_no, cal.fin_year_no, cal.fin_week_no;
   g_recs_read:=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT; 
    COMMIT;
  
  
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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



END WH_PRF_CORP_687U_QC5023;
