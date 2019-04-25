--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_645U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_645U" 
                                                                (p_forall_limit in integer,p_success out boolean) as
  --**************************************************************************************************
  --  date:        jan 2009
  --  author:      wendy lyttle
  --  purpose:     create cscwk boc rollup fact table in the performance layer
  --               with input ex ciwk(boc+po) table from performance layer.
  --  tables:      input  - rtl_contract_chain_item_wk
  --               output - rtl_contract_chain_sc_wk
  --  packages:    constants, dwh_log, dwh_valid
  --
  --  maintenance:
  --  12 Feb 2009 - DEFECT 850 - Data selected for rtl_contract_chain_sc_wk
  --                           needs to use post_date and not last_date_updated
  --  31 Mar 2009 - defect 1280 - ODWHUAT program 'wh_prf_corp_645u'
  --                              aborted (RTL_CONTRACT_CHAIN_SC_WK)
  --  1 April 2009 - defect 1313 - Remove last_updated_date from
  --                               select in cursor in wh_prf_corp_645u
  -- 23 APRIL 2009 - DEFECT 1159 - Column NUM_WEIGHTED_DAYS_TO_DELIVER to be
  --                                included in RTL_CONTRACT_CHAIN_SC_WK
  --                             - removed contract_status_code from UPDATE
  --                               key field selection
  -- 4 May 2009 - defect 1532 - this_week_start_date is not being updated
  --                           on rtl_contract_chain_sc_wk
  -- 5 May 2009 - defect 1537 - ACTL_GRN... and   num_du Fields  in table
  --                             RTL_CONTRACT_CHAIN_SC_WK do not get populated
  --   27 July 2009 - defect 2147 - FND-PRF Procedures that over-write SUMS
  --                                with new values, rather than re-SUMMING for
  --                                the primary key
  --
  --  naming conventions
  --  g_  -  global variable
  --  l_  -  log table variable
  --  a_  -  array variable
  --  v_  -  local variable as found in packages
  --  p_  -  parameter
  --  c_  -  prefix to cursor
  --**************************************************************************************************
g_forall_limit       integer := dwh_constants.vc_forall_limit;
g_recs_read          integer := 0;
g_recs_inserted      integer := 0;
g_date               date;

l_message sys_dwh_errlog.log_text%type;
l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_645U';
l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
l_text sys_dwh_log.log_text%type ;
l_description sys_dwh_log_summary.log_description%type   := 'ROLL UP THE BOC+PO ITEMS to STYLE-COLOUR';
l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit  := p_forall_limit;
   end if;
   p_success := false;
   l_text    := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'ROLLUP OF rtl_contract_chain_sc_wk EX DAY LEVEL STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';

    execute immediate 'truncate table dwh_performance.rtl_contract_chain_sc_wk';
    l_text := 'TABLE rtl_contract_chain_sc_wk TRUNCATED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert   /* parallel(scwk,4) */ into rtl_contract_chain_sc_wk scwk
    select   /*+ parallel(cciw,4) full(cciw) full(di) */ cciw.sk1_contract_no,
             cciw.sk1_chain_no,
             di.sk1_style_colour_no,
             cciw.fin_year_no,
             cciw.fin_week_no,
             max(cciw.fin_week_code) fin_week_code,
             max(cciw.this_week_start_date) this_week_start_date,
             sum(cciw.actl_grn_qty) actl_grn_qty,
             sum(cciw.actl_grn_selling) actl_grn_selling,
             sum(cciw.actl_grn_cost) actl_grn_cost,
             max(cciw.contract_status_code) contract_status_code,
             sum(cciw.contract_qty) contract_qty,
             sum(cciw.contract_selling) contract_selling,
             sum(cciw.contract_cost) contract_cost,
             sum(cciw.amended_po_qty) amended_po_qty,
             sum(cciw.amended_po_selling) amended_po_selling,
             sum(cciw.amended_po_cost) amended_po_cost,
             sum(cciw.bc_shipment_qty) bc_shipment_qty,
             sum(cciw.bc_shipment_selling) bc_shipment_selling,
             sum(cciw.bc_shipment_cost) bc_shipment_cost,
             sum(cciw.po_grn_qty) po_grn_qty,
             sum(cciw.po_grn_selling) po_grn_selling,
             sum(cciw.po_grn_cost) po_grn_cost,
             sum(cciw.latest_po_qty) latest_po_qty,
             sum(cciw.latest_po_selling) latest_po_selling,
             sum(cciw.latest_po_cost) latest_po_cost,
             sum(cciw.boc_qty) boc_qty,
             sum(cciw.boc_selling) boc_selling,
             sum(cciw.boc_cost) boc_cost,
             sum(cciw.boc_qty_all_time) boc_qty_all_time,
             sum(cciw.boc_selling_all_time) boc_selling_all_time,
             sum(cciw.boc_cost_all_time) boc_cost_all_time,
             g_date last_updated_date,
             (case when 1 = 2 then 0 end) num_du,   -- to force a null value
             sum(cciw.num_weighted_days_to_deliver) num_weighted_days_to_deliver
    from     rtl_contract_chain_item_wk cciw
    join     dim_item di     on  cciw.sk1_item_no = di.sk1_item_no
    group by cciw.sk1_contract_no,
             cciw.sk1_chain_no,
             di.sk1_style_colour_no,
             cciw.fin_year_no,
             cciw.fin_week_no;

    g_recs_read     := g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    commit;
    
    l_text := 'STARTING GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_sc_wk', DEGREE => 32);
    commit;
    
    l_text := 'DONE GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
   l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := dwh_constants.vc_log_run_completed ||sysdate;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := ' ';
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

end wh_prf_corp_645u;
