--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_515U_WLFIX2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_515U_WLFIX2" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      M Munnik
--  Purpose:     Rollup from rtl_loc_item_dy_rms_sparse to rtl_loc_sc_wk_rms_sparse.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - rtl_loc_sc_wk_rms_sparse
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sql_trunc_partition  varchar2(100);
g_rec_out            rtl_loc_sc_wk_rms_sparse%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date         date          ;
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_515U_WLFIX2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS SPARSE FROM ITEM_DY TO STYLE_COLOUR_WK';
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

    l_text := 'ROLLUP OF rtl_loc_sc_wk_rms_sparse EX DAY LEVEL STARTED AT '||
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

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=200000000';
execute immediate 'alter session enable parallel dml';

g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20171_1';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20171_2';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20171_3';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20171_4';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20172_5';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



g_start_date := '27 june 2016';
g_end_date := '31 jul 2016';

    l_text := 'Period of rollup = '||g_start_date||'- '|| g_end_date; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND */ into rtl_loc_sc_wk_rms_sparse rtl_lswrs 
 select   /*+ USE_HASH (lid, di, dc) PARALLEL (lid, 2) */
            lid.sk1_location_no as sk1_location_no,
            di.sk1_style_colour_no as sk1_style_colour_no,
            dc.fin_year_no fin_year_no,
            dc.fin_week_no fin_week_no,
            max(dc.fin_week_code) fin_week_code,
            max(dc.this_week_start_date) this_week_start_date,
            max(lid.sk2_location_no) sk2_location_no,
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
            g_date as last_updated_date
   from     rtl_loc_item_dy_rms_sparse lid
   join     dim_item di          on lid.sk1_item_no = di.sk1_item_no
   join     dim_calendar dc      on lid.post_date   = dc.calendar_date
   where    lid.post_date        between g_start_date and g_end_date
   group by lid.sk1_location_no,
            di.sk1_style_colour_no,
            dc.fin_year_no,
            dc.fin_week_no;
  g_recs_read:=SQL%ROWCOUNT;
  g_recs_inserted:=SQL%ROWCOUNT;
  g_recs_updated:=SQL%ROWCOUNT;

    commit;

g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20172_6';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20172_7';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20172_8';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20172_9';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20173_10';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_sql_trunc_partition := 'alter table dwh_performance.rtl_loc_sc_wk_rms_sparse truncate SUBPARTITION  RTL_LCSCWRMS_M20173_11';
EXECUTE IMMEDIATE g_sql_trunc_partition;
commit;
l_text := g_sql_trunc_partition;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


g_start_date := '1 aug 2016';
g_end_date := '11 sep 2016';

    l_text := 'Period of rollup = '||g_start_date||'- '|| g_end_date; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND */ into rtl_loc_sc_wk_rms_sparse rtl_lswrs 
 select   /*+ USE_HASH (lid, di, dc) PARALLEL (lid, 2) */
            lid.sk1_location_no as sk1_location_no,
            di.sk1_style_colour_no as sk1_style_colour_no,
            dc.fin_year_no fin_year_no,
            dc.fin_week_no fin_week_no,
            max(dc.fin_week_code) fin_week_code,
            max(dc.this_week_start_date) this_week_start_date,
            max(lid.sk2_location_no) sk2_location_no,
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
            g_date as last_updated_date
   from     rtl_loc_item_dy_rms_sparse lid
   join     dim_item di          on lid.sk1_item_no = di.sk1_item_no
   join     dim_calendar dc      on lid.post_date   = dc.calendar_date
   where    lid.post_date        between g_start_date and g_end_date
   group by lid.sk1_location_no,
            di.sk1_style_colour_no,
            dc.fin_year_no,
            dc.fin_week_no;
  g_recs_read:=SQL%ROWCOUNT;
  g_recs_inserted:=SQL%ROWCOUNT;
  g_recs_updated:=SQL%ROWCOUNT;

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

end WH_PRF_CORP_515U_WLFIX2;
