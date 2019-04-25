--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_502DJFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_502DJFIX" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
-- backup of current live wh_prf_corp_502u - taken on 22 nov 2010 for qc3977
-- new version to replace wh_prf_corp_502u
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LIWk Dense rollup fact table in the performance layer
--               with input ex lid dense table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - rtl_loc_item_wk_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -Tien Cheng
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
g_rec_out            rtl_loc_item_wk_rms_sparse%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_502U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS SPARSE PERFORMANCE to WEEK';
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
    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_sparse EX DAY LEVEL STARTED '||
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

--    select this_week_start_date-35
--    into   g_start_date
--    from   dim_calendar
--    where  calendar_date = g_date;
    
    g_date       := '9 AUG 2015';
    g_start_date := '18 MAY 2015';
    

    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

INSERT /*+ APPEND */ INTO dwh_performance.rtl_loc_item_wk_rms_sparse rtl_liwrs
   select   /*+  parallel (2) */ 
            lid.sk1_location_no as sk1_location_no,
            lid.sk1_item_no as sk1_item_no,
            dc.fin_year_no as fin_year_no,
            dc.fin_week_no as fin_week_no,
            max(lid.sk2_location_no) sk2_location_no,
            max(lid.sk2_item_no) sk2_item_no ,
            max(dc.fin_week_code) as fin_week_code,
            max(dc.this_week_start_date) as this_week_start_date,
            sum(lid.prom_sales_qty) prom_sales_qty,
            sum(lid.prom_sales) prom_sales,
            sum(lid.prom_sales_cost) prom_sales_cost,
            sum(lid.prom_sales_fr_cost) prom_sales_fr_cost,
            sum(lid.prom_sales_margin) prom_sales_margin,
            sum(lid.franchise_prom_sales) franchise_prom_sales,
            sum(lid.franchise_prom_sales_margin) franchise_prom_sales_margin,
            sum(lid.prom_discount_no) prom_discount_no,
            sum(lid.ho_prom_discount_amt) ho_prom_discount_amt,
            sum(lid.ho_prom_discount_qty) ho_prom_discount_qty,
            sum(lid.st_prom_discount_amt) st_prom_discount_amt,
            sum(lid.st_prom_discount_qty) st_prom_discount_qty,
            sum(lid.clear_sales_qty) clear_sales_qty,
            sum(lid.clear_sales) clear_sales,
            sum(lid.clear_sales_cost) clear_sales_cost,
            sum(lid.clear_sales_fr_cost) clear_sales_fr_cost,
            sum(lid.clear_sales_margin) clear_sales_margin,
            sum(lid.franchise_clear_sales) franchise_clear_sales,
            sum(lid.franchise_clear_sales_margin) franchise_clear_sales_margin,
            sum(lid.waste_qty) waste_qty,
            sum(lid.waste_selling) waste_selling,
            sum(lid.waste_cost) waste_cost,
            sum(lid.waste_fr_cost) waste_fr_cost,
            sum(lid.shrink_qty) shrink_qty,
            sum(lid.shrink_selling) shrink_selling,
            sum(lid.shrink_cost) shrink_cost,
            sum(lid.shrink_fr_cost) shrink_fr_cost,
            sum(lid.gain_qty) gain_qty,
            sum(lid.gain_selling) gain_selling,
            sum(lid.gain_cost) gain_cost,
            sum(lid.gain_fr_cost) gain_fr_cost,
            sum(lid.grn_qty) grn_qty,
            sum(lid.grn_cases) grn_cases,
            sum(lid.grn_selling) grn_selling,
            sum(lid.grn_cost) grn_cost,
            sum(lid.grn_fr_cost) grn_fr_cost,
            sum(lid.grn_margin) grn_margin,
            sum(lid.shrinkage_qty) shrinkage_qty,
            sum(lid.shrinkage_selling) shrinkage_selling,
            sum(lid.shrinkage_cost) shrinkage_cost,
            sum(lid.shrinkage_fr_cost) shrinkage_fr_cost,
            sum(lid.abs_shrinkage_qty) abs_shrinkage_qty,
            sum(lid.abs_shrinkage_selling) abs_shrinkage_selling,
            sum(lid.abs_shrinkage_cost) abs_shrinkage_cost,
            sum(lid.abs_shrinkage_fr_cost) abs_shrinkage_fr_cost,
            sum(lid.claim_qty) claim_qty,
            sum(lid.claim_selling) claim_selling,
            sum(lid.claim_cost) claim_cost,
            sum(lid.claim_fr_cost) claim_fr_cost,
            sum(lid.self_supply_qty) self_supply_qty,
            sum(lid.self_supply_selling) self_supply_selling,
            sum(lid.self_supply_cost) self_supply_cost,
            sum(lid.self_supply_fr_cost) self_supply_fr_cost,
            sum(lid.wac_adj_amt) wac_adj_amt,
            sum(lid.invoice_adj_qty) invoice_adj_qty,
            sum(lid.invoice_adj_selling) invoice_adj_selling,
            sum(lid.invoice_adj_cost) invoice_adj_cost,
            sum(lid.rndm_mass_pos_var) rndm_mass_pos_var,
            sum(lid.mkup_selling) mkup_selling,
            sum(lid.mkup_cancel_selling) mkup_cancel_selling,
            sum(lid.mkdn_selling) mkdn_selling,
            sum(lid.mkdn_cancel_selling) mkdn_cancel_selling,
            sum(lid.prom_mkdn_qty) prom_mkdn_qty,
            sum(lid.prom_mkdn_selling) prom_mkdn_selling,
            sum(lid.clear_mkdn_selling) clear_mkdn_selling,
            sum(lid.mkdn_sales_qty) mkdn_sales_qty,
            sum(lid.mkdn_sales) mkdn_sales,
            sum(lid.mkdn_sales_cost) mkdn_sales_cost,
            sum(lid.net_mkdn) net_mkdn,
            sum(lid.rtv_qty) rtv_qty,
            sum(lid.rtv_cases) rtv_cases,
            sum(lid.rtv_selling) rtv_selling,
            sum(lid.rtv_cost) rtv_cost,
            sum(lid.rtv_fr_cost) rtv_fr_cost,
            sum(lid.sdn_out_qty) sdn_out_qty,
            sum(lid.sdn_out_selling) sdn_out_selling,
            sum(lid.sdn_out_cost) sdn_out_cost,
            sum(lid.sdn_out_fr_cost) sdn_out_fr_cost,
            sum(lid.sdn_out_cases) sdn_out_cases,
            sum(lid.ibt_in_qty) ibt_in_qty,
            sum(lid.ibt_in_selling) ibt_in_selling,
            sum(lid.ibt_in_cost) ibt_in_cost,
            sum(lid.ibt_in_fr_cost) ibt_in_fr_cost,
            sum(lid.ibt_out_qty) ibt_out_qty,
            sum(lid.ibt_out_selling) ibt_out_selling,
            sum(lid.ibt_out_cost) ibt_out_cost,
            sum(lid.ibt_out_fr_cost) ibt_out_fr_cost,
            sum(lid.net_ibt_qty) net_ibt_qty,
            sum(lid.net_ibt_selling) net_ibt_selling,
            sum(lid.shrink_excl_some_dept_cost) shrink_excl_some_dept_cost,
            sum(lid.gain_excl_some_dept_cost) gain_excl_some_dept_cost,
            sum(lid.net_waste_qty) net_waste_qty,
            sum(lid.trunked_qty) trunked_qty,
            sum(lid.trunked_cases) trunked_cases,
            sum(lid.trunked_selling) trunked_selling,
            sum(lid.trunked_cost) trunked_cost,
            sum(lid.dc_delivered_qty) dc_delivered_qty,
            sum(lid.dc_delivered_cases) dc_delivered_cases,
            sum(lid.dc_delivered_selling) dc_delivered_selling,
            sum(lid.dc_delivered_cost) dc_delivered_cost,
            sum(lid.net_inv_adj_qty) net_inv_adj_qty,
            sum(lid.net_inv_adj_selling) net_inv_adj_selling,
            sum(lid.net_inv_adj_cost) net_inv_adj_cost,
            sum(lid.net_inv_adj_fr_cost) net_inv_adj_fr_cost,
            sum(lid.ch_alloc_qty) ch_alloc_qty,
            sum(lid.ch_alloc_selling) ch_alloc_selling,
            g_date last_update_date,
            sum(nvl(lid.abs_shrinkage_cases,0)) abs_shrinkage_cases,
            sum(nvl(lid.abs_shrinkage_selling_dept,0)) abs_shrinkage_selling_dept,
            sum(nvl(lid.abs_shrinkage_cost_dept,0)) abs_shrinkage_cost_dept,
            sum(nvl(lid.abs_shrinkage_qty_dept,0)) abs_shrinkage_qty_dept,
            sum(nvl(lid.abs_shrinkage_cases_dept,0)) abs_shrinkage_cases_dept
   from     dwh_performance.rtl_loc_item_dy_rms_sparse lid,
            dwh_performance.dim_calendar dc
   where    lid.post_date         = dc.calendar_date and
            lid.post_date         between G_start_date and G_date
   group by lid.sk1_location_no, lid.sk1_item_no, dc.fin_year_no, dc.fin_week_no;
-- FOLLOWING CODE REPLACED 5-MAY-2010
--   select   lid.sk1_location_no as sk1_location_no,
--            lid.sk1_item_no as sk1_item_no,
--            dc.fin_year_no as fin_year_no,
--            dc.fin_week_no as fin_week_no,
--            max(lid.sk2_location_no) sk2_location_no,
--            max(lid.sk2_item_no) sk2_item_no ,
--            max(dc.fin_week_code) as fin_week_code,
--            max(dc.this_week_start_date) as this_week_start_date,
--            sum(lid.prom_sales_qty) prom_sales_qty,
--            sum(lid.prom_sales) prom_sales,
--            sum(lid.prom_sales_cost) prom_sales_cost,
--            sum(lid.prom_sales_fr_cost) prom_sales_fr_cost,
--            sum(lid.prom_sales_margin) prom_sales_margin,
--            sum(lid.franchise_prom_sales) franchise_prom_sales,
--            sum(lid.franchise_prom_sales_margin) franchise_prom_sales_margin,
--            sum(lid.prom_discount_no) prom_discount_no,
--            sum(lid.ho_prom_discount_amt) ho_prom_discount_amt,
--            sum(lid.ho_prom_discount_qty) ho_prom_discount_qty,
--            sum(lid.st_prom_discount_amt) st_prom_discount_amt,
--            sum(lid.st_prom_discount_qty) st_prom_discount_qty,
--            sum(lid.clear_sales_qty) clear_sales_qty,
--            sum(lid.clear_sales) clear_sales,
--            sum(lid.clear_sales_cost) clear_sales_cost,
--            sum(lid.clear_sales_fr_cost) clear_sales_fr_cost,
--            sum(lid.clear_sales_margin) clear_sales_margin,
--            sum(lid.franchise_clear_sales) franchise_clear_sales,
--            sum(lid.franchise_clear_sales_margin) franchise_clear_sales_margin,
---            sum(lid.waste_qty) waste_qty,
--            sum(lid.waste_selling) waste_selling,
--            sum(lid.waste_cost) waste_cost,
--            sum(lid.waste_fr_cost) waste_fr_cost,
--            sum(lid.shrink_qty) shrink_qty,
--            sum(lid.shrink_selling) shrink_selling,
--            sum(lid.shrink_cost) shrink_cost,
--            sum(lid.shrink_fr_cost) shrink_fr_cost,
--            sum(lid.gain_qty) gain_qty,
--            sum(lid.gain_selling) gain_selling,
--            sum(lid.gain_cost) gain_cost,
--            sum(lid.gain_fr_cost) gain_fr_cost,
--            sum(lid.grn_qty) grn_qty,
--            sum(lid.grn_cases) grn_cases,
--            sum(lid.grn_selling) grn_selling,
--            sum(lid.grn_cost) grn_cost,
--            sum(lid.grn_fr_cost) grn_fr_cost,
--            sum(lid.grn_margin) grn_margin,
--            sum(lid.shrinkage_qty) shrinkage_qty,
--            sum(lid.shrinkage_selling) shrinkage_selling,
--            sum(lid.shrinkage_cost) shrinkage_cost,
--            sum(lid.shrinkage_fr_cost) shrinkage_fr_cost,
--            sum(lid.abs_shrinkage_qty) abs_shrinkage_qty,
--            sum(lid.abs_shrinkage_selling) abs_shrinkage_selling,
--            sum(lid.abs_shrinkage_cost) abs_shrinkage_cost,
--            sum(lid.abs_shrinkage_fr_cost) abs_shrinkage_fr_cost,
--            sum(lid.claim_qty) claim_qty,
--            sum(lid.claim_selling) claim_selling,
--            sum(lid.claim_cost) claim_cost,
--            sum(lid.claim_fr_cost) claim_fr_cost,
--            sum(lid.self_supply_qty) self_supply_qty,
--            sum(lid.self_supply_selling) self_supply_selling,
--            sum(lid.self_supply_cost) self_supply_cost,
--            sum(lid.self_supply_fr_cost) self_supply_fr_cost,
--            sum(lid.wac_adj_amt) wac_adj_amt,
--            sum(lid.invoice_adj_qty) invoice_adj_qty,
--            sum(lid.invoice_adj_selling) invoice_adj_selling,
--            sum(lid.invoice_adj_cost) invoice_adj_cost,
--            sum(lid.rndm_mass_pos_var) rndm_mass_pos_var,
--            sum(lid.mkup_selling) mkup_selling,
--            sum(lid.mkup_cancel_selling) mkup_cancel_selling,
--            sum(lid.mkdn_selling) mkdn_selling,
--            sum(lid.mkdn_cancel_selling) mkdn_cancel_selling,
--            sum(lid.prom_mkdn_qty) prom_mkdn_qty,
--            sum(lid.prom_mkdn_selling) prom_mkdn_selling,
--            sum(lid.clear_mkdn_selling) clear_mkdn_selling,
--           sum(lid.mkdn_sales_qty) mkdn_sales_qty,
--            sum(lid.mkdn_sales) mkdn_sales,
--            sum(lid.mkdn_sales_cost) mkdn_sales_cost,
--            sum(lid.net_mkdn) net_mkdn,
--            sum(lid.rtv_qty) rtv_qty,
--            sum(lid.rtv_cases) rtv_cases,
--            sum(lid.rtv_selling) rtv_selling,
--            sum(lid.rtv_cost) rtv_cost,
--            sum(lid.rtv_fr_cost) rtv_fr_cost,
--            sum(lid.sdn_out_qty) sdn_out_qty,
--            sum(lid.sdn_out_selling) sdn_out_selling,
--            sum(lid.sdn_out_cost) sdn_out_cost,
--            sum(lid.sdn_out_fr_cost) sdn_out_fr_cost,
--            sum(lid.sdn_out_cases) sdn_out_cases,
--            sum(lid.ibt_in_qty) ibt_in_qty,
--            sum(lid.ibt_in_selling) ibt_in_selling,
--            sum(lid.ibt_in_cost) ibt_in_cost,
--            sum(lid.ibt_in_fr_cost) ibt_in_fr_cost,
--            sum(lid.ibt_out_qty) ibt_out_qty,
--            sum(lid.ibt_out_selling) ibt_out_selling,
--            sum(lid.ibt_out_cost) ibt_out_cost,
--            sum(lid.ibt_out_fr_cost) ibt_out_fr_cost,
--            sum(lid.net_ibt_qty) net_ibt_qty,
--            sum(lid.net_ibt_selling) net_ibt_selling,
--            sum(lid.shrink_excl_some_dept_cost) shrink_excl_some_dept_cost,
--            sum(lid.gain_excl_some_dept_cost) gain_excl_some_dept_cost,
--            sum(lid.net_waste_qty) net_waste_qty,
--            sum(lid.trunked_qty) trunked_qty,
--            sum(lid.trunked_cases) trunked_cases,
--            sum(lid.trunked_selling) trunked_selling,
--            sum(lid.trunked_cost) trunked_cost,
--            sum(lid.dc_delivered_qty) dc_delivered_qty,
--            sum(lid.dc_delivered_cases) dc_delivered_cases,
--            sum(lid.dc_delivered_selling) dc_delivered_selling,
--            sum(lid.dc_delivered_cost) dc_delivered_cost,
--            sum(lid.net_inv_adj_qty) net_inv_adj_qty,
--            sum(lid.net_inv_adj_selling) net_inv_adj_selling,
--            sum(lid.net_inv_adj_cost) net_inv_adj_cost,
--            sum(lid.net_inv_adj_fr_cost) net_inv_adj_fr_cost,
--            sum(lid.ch_alloc_qty) ch_alloc_qty,
--            sum(lid.ch_alloc_selling) ch_alloc_selling,
--            g_date last_update_date,
--            sum(lid.abs_shrinkage_cases) abs_shrinkage_cases,
--            sum(lid.abs_shrinkage_selling_dept) abs_shrinkage_selling_dept,
--            sum(lid.abs_shrinkage_cost_dept) abs_shrinkage_cost_dept,
--            sum(lid.abs_shrinkage_qty_dept) abs_shrinkage_qty_dept,
--            sum(lid.abs_shrinkage_cases_dept) abs_shrinkage_cases_dept
--   from     rtl_loc_item_dy_rms_sparse lid,
--            dim_calendar dc
--   where    lid.post_date         = dc.calendar_date and
--            lid.post_date         between g_start_date and g_date
--   group by dc.fin_year_no,dc.fin_week_no,
--            lid.sk1_item_no,lid.sk1_location_no;
----- CODE REPLACE FINISHED
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

commit;

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

end wh_prf_corp_502djfix;
