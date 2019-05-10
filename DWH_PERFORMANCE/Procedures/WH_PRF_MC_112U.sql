--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_112U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_112U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        MAR 2018
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID Sparse sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer for Multi Currency Africa.
--  Tables:      Input  - fnd_mc_loc_item_dy_rms_sale
--               Output - rtl_mc_loc_item_dy_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
--  2019-05-10 - Paul W - Altered hints in cursor to improve query performance
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
g_rec_out            rtl_mc_loc_item_dy_rms_sparse%rowtype;

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                     rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_112U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS SPARSE SALES FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_mc_loc_item_dy_rms_sparse%rowtype index by binary_integer;
type tbl_array_u is table of rtl_mc_loc_item_dy_rms_sparse%rowtype index by binary_integer;
a_tbl_merge         tbl_array_i;
a_empty_set_i       tbl_array_i;

a_count             integer       := 0;
a_count_m          integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_mc_loc_item_dy_rms_sale is
   select /*+ parallel (FND_LID,6) */ 
          fnd_lid.*,
          di.standard_uom_code,di.business_unit_no,di.vat_rate_perc,di.sk1_department_no,di.sk1_item_no,
          dl.chain_no,dl.sk1_location_no,
          decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
          nvl(fnd_li.clearance_ind,0) clearance_ind,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          dd.jv_dept_ind, dd.packaging_dept_ind, dd.gifting_dept_ind,
          dd.non_core_dept_ind, dd.bucket_dept_ind, dd.book_magazine_dept_ind
   from   fnd_mc_loc_item_dy_rms_sale fnd_lid,
          dim_item di,
          dim_location dl,
          fnd_location_item fnd_li,
          dim_item_hist dih,
          dim_location_hist dlh,
          dim_department dd
   where  fnd_lid.last_updated_date  = g_date and
          fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          fnd_lid.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date and
          fnd_lid.location_no        = dlh.location_no and
          fnd_lid.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          di.sk1_department_no       = dd.sk1_department_no and
          fnd_lid.item_no            = fnd_li.item_no(+) and
          fnd_lid.location_no        = fnd_li.location_no(+) and
          ((
          fnd_lid.prom_sales_qty       ||
          fnd_lid.ho_prom_discount_qty ||
          fnd_lid.st_prom_discount_qty ||
          fnd_lid.clear_sales_qty      ||
          fnd_lid.waste_qty            ||
          fnd_lid.shrink_qty           ||
          fnd_lid.gain_qty             ||
          fnd_lid.grn_qty              ||
          fnd_lid.claim_qty            ||
          fnd_lid.self_supply_qty      ||
          fnd_lid.wac_adj_amt_opr          ||
          fnd_lid.invoice_adj_qty      ||
          fnd_lid.rndm_mass_pos_var    ||
          fnd_lid.mkup_selling_opr         ||
          fnd_lid.mkup_cancel_selling_opr  ||
          fnd_lid.mkdn_selling_opr         ||
          fnd_lid.mkdn_cancel_selling_opr  ||
          fnd_lid.clear_mkdn_selling_opr   ||
          fnd_lid.rtv_qty              ||
          fnd_lid.sdn_out_qty          ||
          fnd_lid.ibt_in_qty           ||
          fnd_lid.ibt_out_qty) is not null
          )
   order by fnd_lid.post_date, di.sk1_item_no, dl.sk1_location_no;

g_rec_in                   c_fnd_mc_loc_item_dy_rms_sale%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_mc_loc_item_dy_rms_sale%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.prom_sales_qty                  := g_rec_in.prom_sales_qty;
   g_rec_out.prom_sales                      := g_rec_in.prom_sales_opr;
   g_rec_out.prom_sales_cost                 := g_rec_in.prom_sales_cost_opr;
   g_rec_out.prom_discount_no                := nvl(g_rec_in.prom_discount_no,0);
   g_rec_out.ho_prom_discount_amt            := g_rec_in.ho_prom_discount_amt_opr;
   g_rec_out.ho_prom_discount_qty            := g_rec_in.ho_prom_discount_qty;
   g_rec_out.st_prom_discount_amt            := g_rec_in.st_prom_discount_amt_opr;
   g_rec_out.st_prom_discount_qty            := g_rec_in.st_prom_discount_qty;
   g_rec_out.clear_sales_qty                 := g_rec_in.clear_sales_qty;
   g_rec_out.clear_sales                     := g_rec_in.clear_sales_opr;
   g_rec_out.clear_sales_cost                := g_rec_in.clear_sales_cost_opr;
   g_rec_out.waste_qty                       := g_rec_in.waste_qty;
   g_rec_out.waste_selling                   := g_rec_in.waste_selling_opr;
   g_rec_out.waste_cost                      := g_rec_in.waste_cost_opr;
   g_rec_out.shrink_qty                      := g_rec_in.shrink_qty;
   g_rec_out.shrink_selling                  := g_rec_in.shrink_selling_opr;
   g_rec_out.shrink_cost                     := g_rec_in.shrink_cost_opr;
   g_rec_out.gain_qty                        := g_rec_in.gain_qty;
   g_rec_out.gain_selling                    := g_rec_in.gain_selling_opr;
   g_rec_out.gain_cost                       := g_rec_in.gain_cost_opr;
   g_rec_out.grn_qty                         := g_rec_in.grn_qty;
   g_rec_out.grn_selling                     := g_rec_in.grn_selling_opr;
   g_rec_out.grn_cost                        := g_rec_in.grn_cost_opr;
   g_rec_out.claim_qty                       := g_rec_in.claim_qty;
   g_rec_out.claim_selling                   := g_rec_in.claim_selling_opr;
   g_rec_out.claim_cost                      := g_rec_in.claim_cost_opr;
   g_rec_out.self_supply_qty                 := g_rec_in.self_supply_qty;
   g_rec_out.self_supply_selling             := g_rec_in.self_supply_selling_opr;
   g_rec_out.self_supply_cost                := g_rec_in.self_supply_cost_opr;
   g_rec_out.wac_adj_amt                     := g_rec_in.wac_adj_amt_opr;
   g_rec_out.invoice_adj_qty                 := g_rec_in.invoice_adj_qty;
   g_rec_out.invoice_adj_selling             := g_rec_in.invoice_adj_selling_opr;
   g_rec_out.invoice_adj_cost                := g_rec_in.invoice_adj_cost_opr;
   g_rec_out.rndm_mass_pos_var               := g_rec_in.rndm_mass_pos_var;
   g_rec_out.mkup_selling                    := g_rec_in.mkup_selling_opr;
   g_rec_out.mkup_cancel_selling             := g_rec_in.mkup_cancel_selling_opr;
   g_rec_out.mkdn_selling                    := g_rec_in.mkdn_selling_opr;
   g_rec_out.mkdn_cancel_selling             := g_rec_in.mkdn_cancel_selling_opr;
   g_rec_out.clear_mkdn_selling              := g_rec_in.clear_mkdn_selling_opr;
   g_rec_out.rtv_qty                         := g_rec_in.rtv_qty;
   g_rec_out.rtv_selling                     := g_rec_in.rtv_selling_opr;
   g_rec_out.rtv_cost                        := g_rec_in.rtv_cost_opr;
   g_rec_out.sdn_out_qty                     := g_rec_in.sdn_out_qty;
   g_rec_out.sdn_out_selling                 := g_rec_in.sdn_out_selling_opr;
   g_rec_out.sdn_out_cost                    := g_rec_in.sdn_out_cost_opr;
   g_rec_out.ibt_in_qty                      := g_rec_in.ibt_in_qty;
   g_rec_out.ibt_in_selling                  := g_rec_in.ibt_in_selling_opr;
   g_rec_out.ibt_in_cost                     := g_rec_in.ibt_in_cost_opr;
   g_rec_out.ibt_out_qty                     := g_rec_in.ibt_out_qty;
   g_rec_out.ibt_out_selling                 := g_rec_in.ibt_out_selling_opr;
   g_rec_out.ibt_out_cost                    := g_rec_in.ibt_out_cost_opr;
   g_rec_out.last_updated_date               := g_date;
-- MULTI - CURRENCY ---   
   g_rec_out.prom_sales_local                      := g_rec_in.prom_sales_local;
   g_rec_out.prom_sales_cost_local                 := g_rec_in.prom_sales_cost_local;
   g_rec_out.ho_prom_discount_amt_local            := g_rec_in.ho_prom_discount_amt_local;
   g_rec_out.st_prom_discount_amt_local            := g_rec_in.st_prom_discount_amt_local;
   g_rec_out.clear_sales_local                     := g_rec_in.clear_sales_local;
   g_rec_out.clear_sales_cost_local                := g_rec_in.clear_sales_cost_local;
   g_rec_out.waste_selling_local                   := g_rec_in.waste_selling_local;
   g_rec_out.waste_cost_local                      := g_rec_in.waste_cost_local;
   g_rec_out.shrink_selling_local                  := g_rec_in.shrink_selling_local;
   g_rec_out.shrink_cost_local                     := g_rec_in.shrink_cost_local;
   g_rec_out.gain_selling_local                    := g_rec_in.gain_selling_local;
   g_rec_out.gain_cost_local                       := g_rec_in.gain_cost_local;
   g_rec_out.grn_selling_local                     := g_rec_in.grn_selling_local;
   g_rec_out.grn_cost_local                        := g_rec_in.grn_cost_local;
   g_rec_out.claim_selling_local                   := g_rec_in.claim_selling_local;
   g_rec_out.claim_cost_local                      := g_rec_in.claim_cost_local;
   g_rec_out.self_supply_selling_local             := g_rec_in.self_supply_selling_local;
   g_rec_out.self_supply_cost_local                := g_rec_in.self_supply_cost_local;
   g_rec_out.wac_adj_amt_local                     := g_rec_in.wac_adj_amt_local;
   g_rec_out.invoice_adj_selling_local             := g_rec_in.invoice_adj_selling_local;
   g_rec_out.invoice_adj_cost_local                := g_rec_in.invoice_adj_cost_local;
   g_rec_out.mkup_selling_local                    := g_rec_in.mkup_selling_local;
   g_rec_out.mkup_cancel_selling_local             := g_rec_in.mkup_cancel_selling_local;
   g_rec_out.mkdn_selling_local                    := g_rec_in.mkdn_selling_local;
   g_rec_out.mkdn_cancel_selling_local             := g_rec_in.mkdn_cancel_selling_local;
   g_rec_out.clear_mkdn_selling_local              := g_rec_in.clear_mkdn_selling_local;
   g_rec_out.rtv_selling_local                     := g_rec_in.rtv_selling_local;
   g_rec_out.rtv_cost_local                        := g_rec_in.rtv_cost_local;
   g_rec_out.sdn_out_selling_local                 := g_rec_in.sdn_out_selling_local;
   g_rec_out.sdn_out_cost_local                    := g_rec_in.sdn_out_cost_local;
   g_rec_out.ibt_in_selling_local                  := g_rec_in.ibt_in_selling_local;
   g_rec_out.ibt_in_cost_local                     := g_rec_in.ibt_in_cost_local;
   g_rec_out.ibt_out_selling_local                 := g_rec_in.ibt_out_selling_local;
   g_rec_out.ibt_out_cost_local                    := g_rec_in.ibt_out_cost_local;
 

-- RESET - Value add and calculated fields added to performance layer
   g_rec_out.prom_sales_fr_cost              := '';
   g_rec_out.prom_sales_margin               := '';
   g_rec_out.franchise_prom_sales            := '';
   g_rec_out.franchise_prom_sales_margin     := '';
   g_rec_out.clear_sales_fr_cost             := '';
   g_rec_out.clear_sales_margin              := '';
   g_rec_out.franchise_clear_sales           := '';
   g_rec_out.franchise_clear_sales_margin    := '';
   g_rec_out.waste_fr_cost                   := '';
   g_rec_out.shrink_fr_cost                  := '';
   g_rec_out.gain_fr_cost                    := '';
   g_rec_out.grn_cases                       := '';
   g_rec_out.grn_fr_cost                     := '';
   g_rec_out.grn_margin                      := '';
   g_rec_out.shrinkage_qty                   := '';
   g_rec_out.shrinkage_selling               := '';
   g_rec_out.shrinkage_cost                  := '';
   g_rec_out.shrinkage_fr_cost               := '';
   g_rec_out.abs_shrinkage_qty               := '';
   g_rec_out.abs_shrinkage_selling           := '';
   g_rec_out.abs_shrinkage_cost              := '';
   g_rec_out.abs_shrinkage_fr_cost           := '';
   g_rec_out.claim_fr_cost                   := '';
   g_rec_out.self_supply_fr_cost             := '';
   g_rec_out.prom_mkdn_qty                   := '';
   g_rec_out.prom_mkdn_selling               := '';
   g_rec_out.mkdn_sales_qty                  := '';
   g_rec_out.mkdn_sales                      := '';
   g_rec_out.mkdn_sales_cost                 := '';
   g_rec_out.net_mkdn                        := '';
   g_rec_out.rtv_cases                       := '';
   g_rec_out.rtv_fr_cost                     := '';
   g_rec_out.sdn_out_fr_cost                 := '';
   g_rec_out.sdn_out_cases                   := '';
   g_rec_out.ibt_in_fr_cost                  := '';
   g_rec_out.ibt_out_fr_cost                 := '';
   g_rec_out.net_ibt_qty                     := '';
   g_rec_out.net_ibt_selling                 := '';
   g_rec_out.shrink_excl_some_dept_cost      := '';
   g_rec_out.gain_excl_some_dept_cost        := '';
   g_rec_out.net_waste_qty                   := '';
   g_rec_out.trunked_qty                     := 0;
   g_rec_out.trunked_cases                   := 0;
   g_rec_out.trunked_selling                 := 0;
   g_rec_out.trunked_cost                    := 0;
   g_rec_out.dc_delivered_qty                := 0;
   g_rec_out.dc_delivered_cases              := 0;
   g_rec_out.dc_delivered_selling            := 0;
   g_rec_out.dc_delivered_cost               := 0;
   g_rec_out.net_inv_adj_qty                 := '';
   g_rec_out.net_inv_adj_selling             := '';
   g_rec_out.net_inv_adj_cost                := '';
   g_rec_out.net_inv_adj_fr_cost             := '';
--MULTI CURRENCY --   
   g_rec_out.prom_sales_fr_cost_local               := '';
   g_rec_out.prom_sales_margin_local                := '';
   g_rec_out.franchise_prom_sales_local             := '';
   g_rec_out.frnch_prom_sales_margin_local      := '';
   g_rec_out.clear_sales_fr_cost_local              := '';
   g_rec_out.clear_sales_margin_local               := '';
   g_rec_out.franchise_clear_sales_local            := '';
   g_rec_out.frnch_clear_sales_margin_local     := '';
   g_rec_out.waste_fr_cost_local                    := '';
   g_rec_out.shrink_fr_cost_local                   := '';
   g_rec_out.gain_fr_cost_local                     := '';
   g_rec_out.grn_fr_cost_local                      := '';
   g_rec_out.grn_margin_local                       := '';
   g_rec_out.shrinkage_selling_local                := '';
   g_rec_out.shrinkage_cost_local                   := '';
   g_rec_out.shrinkage_fr_cost_local                := '';
   g_rec_out.abs_shrinkage_selling_local            := '';
   g_rec_out.abs_shrinkage_cost_local               := '';
   g_rec_out.abs_shrinkage_fr_cost_local            := '';
   g_rec_out.claim_fr_cost_local                    := '';
   g_rec_out.self_supply_fr_cost_local              := '';
   g_rec_out.prom_mkdn_selling_local                := '';
   g_rec_out.mkdn_sales_local                       := '';
   g_rec_out.mkdn_sales_cost_local                  := '';
   g_rec_out.net_mkdn_local                         := '';
   g_rec_out.rtv_fr_cost_local                      := '';
   g_rec_out.sdn_out_fr_cost_local                  := '';
   g_rec_out.ibt_in_fr_cost_local                   := '';
   g_rec_out.ibt_out_fr_cost_local                  := '';
   g_rec_out.net_ibt_selling_local                  := '';
   g_rec_out.shrink_excl_some_dept_cost_lcl       := '';
   g_rec_out.gain_excl_some_dept_cost_local         := '';
   g_rec_out.trunked_selling_local                  := 0;
   g_rec_out.trunked_cost_local                     := 0;
   g_rec_out.dc_delivered_selling_local             := 0;
   g_rec_out.dc_delivered_cost_local                := 0;
   g_rec_out.net_inv_adj_selling_local              := '';
   g_rec_out.net_inv_adj_cost_local                 := '';
   g_rec_out.net_inv_adj_fr_cost_local              := '';

-- CALCULATE - Value add and calculated fields added to performance layer
   g_rec_out.prom_sales_margin     := nvl(g_rec_out.prom_sales,0)  - nvl(g_rec_out.prom_sales_cost,0);
   g_rec_out.clear_sales_margin    := nvl(g_rec_out.clear_sales,0) - nvl(g_rec_out.clear_sales_cost,0);
   g_rec_out.grn_margin            := nvl(g_rec_out.grn_selling,0) - nvl(g_rec_out.grn_cost,0);
   g_rec_out.shrinkage_qty         := nvl(g_rec_out.shrink_qty,0)  + nvl(g_rec_out.gain_qty,0);
   g_rec_out.shrinkage_selling     := nvl(g_rec_out.shrink_selling,0)  + nvl(g_rec_out.gain_selling,0);
   g_rec_out.shrinkage_cost        := nvl(g_rec_out.shrink_cost,0)     + nvl(g_rec_out.gain_cost,0);
   g_rec_out.abs_shrinkage_qty     := nvl(abs(g_rec_out.shrink_qty),0) + nvl(abs(g_rec_out.gain_qty),0);
   g_rec_out.abs_shrinkage_selling := nvl(abs(g_rec_out.shrink_selling),0)  + nvl(abs(g_rec_out.gain_selling),0);
   g_rec_out.abs_shrinkage_cost    := nvl(abs(g_rec_out.shrink_cost),0)     + nvl(abs(g_rec_out.gain_cost),0);
   g_rec_out.prom_mkdn_qty         := nvl(g_rec_out.ho_prom_discount_qty,0) + nvl(g_rec_out.st_prom_discount_qty,0);
   g_rec_out.prom_mkdn_selling     := nvl(g_rec_out.ho_prom_discount_amt,0) + nvl(g_rec_out.st_prom_discount_amt,0);
   g_rec_out.mkdn_sales_qty        := nvl(g_rec_out.clear_sales_qty,0)  + nvl(g_rec_out.prom_sales_qty,0);
   g_rec_out.mkdn_sales            := nvl(g_rec_out.clear_sales,0)      + nvl(g_rec_out.prom_sales,0);
   g_rec_out.mkdn_sales_cost       := nvl(g_rec_out.clear_sales_cost,0) + nvl(g_rec_out.prom_sales_cost,0);
   g_rec_out.net_mkdn              := nvl(g_rec_out.mkdn_selling,0)     + nvl(g_rec_out.clear_mkdn_selling,0) -
                                      nvl(g_rec_out.mkdn_cancel_selling,0) + nvl(g_rec_out.mkup_cancel_selling,0) -
                                      nvl(g_rec_out.mkup_selling,0)   + nvl(g_rec_out.prom_mkdn_selling,0);
   g_rec_out.net_ibt_qty           := nvl(g_rec_out.ibt_in_qty,0)     - nvl(g_rec_out.ibt_out_qty,0);
   g_rec_out.net_ibt_selling       := nvl(g_rec_out.ibt_in_selling,0) - nvl(g_rec_out.ibt_out_selling,0);
   g_rec_out.net_inv_adj_qty       := nvl(g_rec_out.waste_qty,0)       + nvl(g_rec_out.shrink_qty,0) + nvl(g_rec_out.gain_qty,0) +
                                      nvl(g_rec_out.self_supply_qty,0) + nvl(g_rec_out.claim_qty,0) ;
   g_rec_out.net_inv_adj_selling   := nvl(g_rec_out.waste_selling,0)       + nvl(g_rec_out.shrink_selling,0) + nvl(g_rec_out.gain_selling,0) +
                                      nvl(g_rec_out.self_supply_selling,0) + nvl(g_rec_out.claim_selling,0) ;
   g_rec_out.net_inv_adj_cost      := nvl(g_rec_out.waste_cost,0)       + nvl(g_rec_out.shrink_cost,0) + nvl(g_rec_out.gain_cost,0) +
                                      nvl(g_rec_out.self_supply_cost,0) + nvl(g_rec_out.claim_cost,0) ;

    if g_rec_in.jv_dept_ind      = 0 and g_rec_in.packaging_dept_ind    = 0 and
      g_rec_in.gifting_dept_ind  = 0 and g_rec_in.non_core_dept_ind      = 0 and
      g_rec_in.bucket_dept_ind   = 0 and g_rec_in.book_magazine_dept_ind = 0 then
      g_rec_out.shrink_excl_some_dept_cost := g_rec_out.shrink_cost;
      g_rec_out.gain_excl_some_dept_cost   := g_rec_out.gain_cost;
   end if;

-- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).
   if g_rec_in.business_unit_no = 50 then
      g_rec_out.grn_cases     := round((nvl(g_rec_out.grn_qty,0)/g_rec_in.num_units_per_tray),0);
      g_rec_out.rtv_cases     := round((nvl(g_rec_out.rtv_qty,0)/g_rec_in.num_units_per_tray),0);
      g_rec_out.sdn_out_cases := round((nvl(g_rec_out.sdn_out_qty,0)/g_rec_in.num_units_per_tray),0);
   end if;

   if g_rec_in.chain_no = 20 then   /* franchise stores 'FR','EX','EN','BX' */
      begin
         select debtors_commission_perc
         into   g_debtors_commission_perc
         from   rtl_loc_dept_dy
         where  sk1_location_no       = g_rec_out.sk1_location_no and
                sk1_department_no     = g_rec_in.sk1_department_no and
                post_date             = g_rec_out.post_date;
         exception
            when no_data_found then
              g_debtors_commission_perc := 0;
      end;
      if g_debtors_commission_perc is null then
         g_debtors_commission_perc := 0;
      end if;
--- g_debtors_commission_perc := 10; For testing

      g_rec_out.prom_sales_fr_cost           := nvl(g_rec_out.prom_sales_cost,0) + round((nvl(g_rec_out.prom_sales_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_prom_sales         := g_rec_out.prom_sales;
      g_rec_out.franchise_prom_sales_margin  := nvl(g_rec_out.franchise_prom_sales,0) - nvl(g_rec_out.prom_sales_fr_cost,0);
      g_rec_out.clear_sales_fr_cost          := nvl(g_rec_out.clear_sales_cost,0) + round((nvl(g_rec_out.clear_sales_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_clear_sales        := g_rec_out.clear_sales;
      g_rec_out.franchise_clear_sales_margin := nvl(g_rec_out.franchise_clear_sales,0) - nvl(g_rec_out.clear_sales_fr_cost,0);

      g_rec_out.waste_fr_cost                := nvl(g_rec_out.waste_cost,0)  + round((nvl(g_rec_out.waste_cost,0)  * g_debtors_commission_perc / 100),2);
      g_rec_out.shrink_fr_cost               := nvl(g_rec_out.shrink_cost,0) + round((nvl(g_rec_out.shrink_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.gain_fr_cost                 := nvl(g_rec_out.gain_cost,0)   + round((nvl(g_rec_out.gain_cost,0)   * g_debtors_commission_perc / 100),2);
      g_rec_out.grn_fr_cost                  := nvl(g_rec_out.grn_cost,0)    + round((nvl(g_rec_out.grn_cost,0)    * g_debtors_commission_perc / 100),2);
   g_rec_out.shrinkage_fr_cost        := (nvl(g_rec_out.shrink_cost,0)     + nvl(g_rec_out.gain_cost,0)) +
                                         round((nvl(g_rec_out.shrink_cost,0)     + nvl(g_rec_out.gain_cost,0)) * g_debtors_commission_perc / 100,2)  ;
   g_rec_out.abs_shrinkage_fr_cost    := (nvl(abs(g_rec_out.shrink_cost),0)     + nvl(abs(g_rec_out.gain_cost),0)) +
                                         round((nvl(abs(g_rec_out.shrink_cost),0)     + nvl(abs(g_rec_out.gain_cost),0)) * g_debtors_commission_perc / 100,2)       ;
      g_rec_out.claim_fr_cost                := nvl(g_rec_out.claim_cost,0)       + round((nvl(g_rec_out.claim_cost,0)       * g_debtors_commission_perc / 100),2);

      g_rec_out.self_supply_fr_cost          := nvl(g_rec_out.self_supply_cost,0) + round((nvl(g_rec_out.self_supply_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.rtv_fr_cost                  := nvl(g_rec_out.rtv_cost,0)      + round((nvl(g_rec_out.rtv_cost,0)     * g_debtors_commission_perc / 100),2);
      g_rec_out.sdn_out_fr_cost              := nvl(g_rec_out.sdn_out_cost,0)  + round((nvl(g_rec_out.sdn_out_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.ibt_in_fr_cost               := nvl(g_rec_out.ibt_in_cost,0)   + round((nvl(g_rec_out.ibt_in_cost,0)  * g_debtors_commission_perc / 100),2);
      g_rec_out.ibt_out_fr_cost              := nvl(g_rec_out.ibt_out_cost,0)  + round((nvl(g_rec_out.ibt_out_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.net_inv_adj_fr_cost          := nvl(g_rec_out.waste_fr_cost,0)       + nvl(g_rec_out.shrink_fr_cost,0) + nvl(g_rec_out.gain_fr_cost,0) +
                                                nvl(g_rec_out.self_supply_fr_cost,0) + nvl(g_rec_out.claim_fr_cost,0) ;

   end if;
--sales
------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- MULTICURRENCY CALC FOR AFRICA STORES IN LOCAL CURRENCY ------------------
-- CALCULATE - Value add and calculated fields added to performance layer
   g_rec_out.prom_sales_margin_local     := nvl(g_rec_out.prom_sales_local,0)  - nvl(g_rec_out.prom_sales_cost_local,0);
   g_rec_out.clear_sales_margin_local    := nvl(g_rec_out.clear_sales_local,0) - nvl(g_rec_out.clear_sales_cost_local,0);
   g_rec_out.grn_margin_local            := nvl(g_rec_out.grn_selling_local,0) - nvl(g_rec_out.grn_cost_local,0);
   g_rec_out.shrinkage_selling_local     := nvl(g_rec_out.shrink_selling_local,0)  + nvl(g_rec_out.gain_selling_local,0);
   g_rec_out.shrinkage_cost_local        := nvl(g_rec_out.shrink_cost_local,0)     + nvl(g_rec_out.gain_cost_local,0);
   g_rec_out.abs_shrinkage_selling_local := nvl(abs(g_rec_out.shrink_selling_local),0)  + nvl(abs(g_rec_out.gain_selling_local),0);
   g_rec_out.abs_shrinkage_cost_local    := nvl(abs(g_rec_out.shrink_cost_local),0)     + nvl(abs(g_rec_out.gain_cost_local),0);
   g_rec_out.prom_mkdn_selling_local     := nvl(g_rec_out.ho_prom_discount_amt_local,0) + nvl(g_rec_out.st_prom_discount_amt_local,0);
   g_rec_out.mkdn_sales_local            := nvl(g_rec_out.clear_sales_local,0)      + nvl(g_rec_out.prom_sales_local,0);
   g_rec_out.mkdn_sales_cost_local       := nvl(g_rec_out.clear_sales_cost_local,0) + nvl(g_rec_out.prom_sales_cost_local,0);
   g_rec_out.net_mkdn_local              := nvl(g_rec_out.mkdn_selling_local,0)     + nvl(g_rec_out.clear_mkdn_selling_local,0) -
                                      nvl(g_rec_out.mkdn_cancel_selling_local,0) + nvl(g_rec_out.mkup_cancel_selling_local,0) -
                                      nvl(g_rec_out.mkup_selling_local,0)   + nvl(g_rec_out.prom_mkdn_selling_local,0);
   g_rec_out.net_ibt_selling_local       := nvl(g_rec_out.ibt_in_selling_local,0) - nvl(g_rec_out.ibt_out_selling_local,0);
   g_rec_out.net_inv_adj_selling_local   := nvl(g_rec_out.waste_selling_local,0)       + nvl(g_rec_out.shrink_selling_local,0) + nvl(g_rec_out.gain_selling_local,0) +
                                      nvl(g_rec_out.self_supply_selling_local,0) + nvl(g_rec_out.claim_selling_local,0) ;
   g_rec_out.net_inv_adj_cost_local      := nvl(g_rec_out.waste_cost_local,0)       + nvl(g_rec_out.shrink_cost_local,0) + nvl(g_rec_out.gain_cost_local,0) +
                                      nvl(g_rec_out.self_supply_cost_local,0) + nvl(g_rec_out.claim_cost_local,0) ;

    if g_rec_in.jv_dept_ind      = 0 and g_rec_in.packaging_dept_ind    = 0 and
      g_rec_in.gifting_dept_ind  = 0 and g_rec_in.non_core_dept_ind      = 0 and
      g_rec_in.bucket_dept_ind   = 0 and g_rec_in.book_magazine_dept_ind = 0 then
      g_rec_out.shrink_excl_some_dept_cost_lcl := g_rec_out.shrink_cost_local;
      g_rec_out.gain_excl_some_dept_cost_local   := g_rec_out.gain_cost_local;
   end if;
   if g_rec_in.chain_no = 20 then   /* franchise stores 'FR','EX','EN','BX' */
      g_rec_out.prom_sales_fr_cost_local           := nvl(g_rec_out.prom_sales_cost_local,0) + round((nvl(g_rec_out.prom_sales_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_prom_sales_local         := g_rec_out.prom_sales_local;
      g_rec_out.frnch_prom_sales_margin_local  := nvl(g_rec_out.franchise_prom_sales_local,0) - nvl(g_rec_out.prom_sales_fr_cost_local,0);
      g_rec_out.clear_sales_fr_cost_local          := nvl(g_rec_out.clear_sales_cost_local,0) + round((nvl(g_rec_out.clear_sales_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_clear_sales_local        := g_rec_out.clear_sales_local;
      g_rec_out.frnch_clear_sales_margin_local := nvl(g_rec_out.franchise_clear_sales_local,0) - nvl(g_rec_out.clear_sales_fr_cost_local,0);

      g_rec_out.waste_fr_cost_local                := nvl(g_rec_out.waste_cost_local,0)  + round((nvl(g_rec_out.waste_cost_local,0)  * g_debtors_commission_perc / 100),2);
      g_rec_out.shrink_fr_cost_local               := nvl(g_rec_out.shrink_cost_local,0) + round((nvl(g_rec_out.shrink_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.gain_fr_cost_local                 := nvl(g_rec_out.gain_cost_local,0)   + round((nvl(g_rec_out.gain_cost_local,0)   * g_debtors_commission_perc / 100),2);
      g_rec_out.grn_fr_cost_local                  := nvl(g_rec_out.grn_cost_local,0)    + round((nvl(g_rec_out.grn_cost_local,0)    * g_debtors_commission_perc / 100),2);
      g_rec_out.shrinkage_fr_cost_local        := (nvl(g_rec_out.shrink_cost_local,0)     + nvl(g_rec_out.gain_cost_local,0)) +
                                         round((nvl(g_rec_out.shrink_cost_local,0)     + nvl(g_rec_out.gain_cost_local,0)) * g_debtors_commission_perc / 100,2)  ;
      g_rec_out.abs_shrinkage_fr_cost_local    := (nvl(abs(g_rec_out.shrink_cost_local),0)     + nvl(abs(g_rec_out.gain_cost_local),0)) +
                                         round((nvl(abs(g_rec_out.shrink_cost_local),0)     + nvl(abs(g_rec_out.gain_cost_local),0)) * g_debtors_commission_perc / 100,2)       ;
      g_rec_out.claim_fr_cost_local                := nvl(g_rec_out.claim_cost_local,0)       + round((nvl(g_rec_out.claim_cost_local,0)       * g_debtors_commission_perc / 100),2);

      g_rec_out.self_supply_fr_cost_local          := nvl(g_rec_out.self_supply_cost_local,0) + round((nvl(g_rec_out.self_supply_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.rtv_fr_cost_local                  := nvl(g_rec_out.rtv_cost_local,0)      + round((nvl(g_rec_out.rtv_cost_local,0)     * g_debtors_commission_perc / 100),2);
      g_rec_out.sdn_out_fr_cost_local              := nvl(g_rec_out.sdn_out_cost_local,0)  + round((nvl(g_rec_out.sdn_out_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.ibt_in_fr_cost_local               := nvl(g_rec_out.ibt_in_cost_local,0)   + round((nvl(g_rec_out.ibt_in_cost_local,0)  * g_debtors_commission_perc / 100),2);
      g_rec_out.ibt_out_fr_cost_local              := nvl(g_rec_out.ibt_out_cost_local,0)  + round((nvl(g_rec_out.ibt_out_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.net_inv_adj_fr_cost_local          := nvl(g_rec_out.waste_fr_cost_local,0)       + nvl(g_rec_out.shrink_fr_cost_local,0) + nvl(g_rec_out.gain_fr_cost_local,0) +
                                                nvl(g_rec_out.self_supply_fr_cost_local,0) + nvl(g_rec_out.claim_fr_cost_local,0) ;
   end if;

----------------------------------------------------------------------------------------------------------------------------------------------------------------


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin

    forall i in a_tbl_merge.first .. a_tbl_merge.last
    save exceptions
MERGE /*+ USE_HASH(rtl_lidrs ,mer_rlidrs)*/ INTO rtl_mc_loc_item_dy_rms_sparse rtl_lidrs
USING
(select a_tbl_merge(i).sk1_location_no              AS    SK1_LOCATION_NO,
        a_tbl_merge(i).sk1_item_no                  AS    SK1_ITEM_NO,
        a_tbl_merge(i).post_date                    AS    POST_DATE,
        a_tbl_merge(i).sk2_location_no              AS    SK2_LOCATION_NO,
        a_tbl_merge(i).sk2_item_no                  AS    SK2_ITEM_NO,
        a_tbl_merge(i).prom_sales_qty               AS    PROM_SALES_QTY,
        a_tbl_merge(i).prom_sales                   AS    PROM_SALES,
        a_tbl_merge(i).prom_sales_cost              AS    PROM_SALES_COST,
        a_tbl_merge(i).prom_sales_fr_cost           AS    PROM_SALES_FR_COST,
        a_tbl_merge(i).prom_sales_margin            AS    PROM_SALES_MARGIN,
        a_tbl_merge(i).franchise_prom_sales         AS    FRANCHISE_PROM_SALES,
        a_tbl_merge(i).franchise_prom_sales_margin  AS    FRANCHISE_PROM_SALES_MARGIN,
        a_tbl_merge(i).prom_discount_no             AS    PROM_DISCOUNT_NO,
        a_tbl_merge(i).ho_prom_discount_amt         AS    HO_PROM_DISCOUNT_AMT,
        a_tbl_merge(i).ho_prom_discount_qty         AS    HO_PROM_DISCOUNT_QTY,
        a_tbl_merge(i).st_prom_discount_amt         AS    ST_PROM_DISCOUNT_AMT,
        a_tbl_merge(i).st_prom_discount_qty         AS    ST_PROM_DISCOUNT_QTY,
        a_tbl_merge(i).clear_sales_qty              AS    CLEAR_SALES_QTY,
        a_tbl_merge(i).clear_sales                  AS    CLEAR_SALES,
        a_tbl_merge(i).clear_sales_cost             AS    CLEAR_SALES_COST,
        a_tbl_merge(i).clear_sales_fr_cost          AS    CLEAR_SALES_FR_COST,
        a_tbl_merge(i).clear_sales_margin           AS    CLEAR_SALES_MARGIN,
        a_tbl_merge(i).franchise_clear_sales        AS    FRANCHISE_CLEAR_SALES,
        a_tbl_merge(i).franchise_clear_sales_margin AS    FRANCHISE_CLEAR_SALES_MARGIN,
        a_tbl_merge(i).waste_qty                    AS    WASTE_QTY,
        a_tbl_merge(i).waste_selling                AS    WASTE_SELLING,
        a_tbl_merge(i).waste_cost                   AS    WASTE_COST,
        a_tbl_merge(i).waste_fr_cost                AS    WASTE_FR_COST,
        a_tbl_merge(i).shrink_qty                   AS    SHRINK_QTY,
        a_tbl_merge(i).shrink_selling               AS    SHRINK_SELLING,
        a_tbl_merge(i).shrink_cost                  AS    SHRINK_COST,
        a_tbl_merge(i).shrink_fr_cost               AS    SHRINK_FR_COST,
        a_tbl_merge(i).gain_qty                     AS    GAIN_QTY,
        a_tbl_merge(i).gain_selling                 AS    GAIN_SELLING,
        a_tbl_merge(i).gain_cost                    AS    GAIN_COST,
        a_tbl_merge(i).gain_fr_cost                 AS    GAIN_FR_COST,
        a_tbl_merge(i).grn_qty                      AS    GRN_QTY,
        a_tbl_merge(i).grn_cases                    AS    GRN_CASES,
        a_tbl_merge(i).grn_selling                  AS    GRN_SELLING,
        a_tbl_merge(i).grn_cost                     AS    GRN_COST,
        a_tbl_merge(i).grn_fr_cost                  AS    GRN_FR_COST,
        a_tbl_merge(i).grn_margin                   AS    GRN_MARGIN,
        a_tbl_merge(i).shrinkage_qty                AS    SHRINKAGE_QTY,
        a_tbl_merge(i).shrinkage_selling            AS    SHRINKAGE_SELLING,
        a_tbl_merge(i).shrinkage_cost               AS    SHRINKAGE_COST,
        a_tbl_merge(i).shrinkage_fr_cost            AS    SHRINKAGE_FR_COST,
        a_tbl_merge(i).abs_shrinkage_qty            AS    ABS_SHRINKAGE_QTY,
        a_tbl_merge(i).abs_shrinkage_selling        AS    ABS_SHRINKAGE_SELLING,
        a_tbl_merge(i).abs_shrinkage_cost           AS    ABS_SHRINKAGE_COST,
        a_tbl_merge(i).abs_shrinkage_fr_cost        AS    ABS_SHRINKAGE_FR_COST,
        a_tbl_merge(i).claim_qty                    AS    CLAIM_QTY,
        a_tbl_merge(i).claim_selling                AS    CLAIM_SELLING,
        a_tbl_merge(i).claim_cost                   AS    CLAIM_COST,
        a_tbl_merge(i).claim_fr_cost                AS    CLAIM_FR_COST,
        a_tbl_merge(i).self_supply_qty              AS    SELF_SUPPLY_QTY,
        a_tbl_merge(i).self_supply_selling          AS    SELF_SUPPLY_SELLING,
        a_tbl_merge(i).self_supply_cost             AS    SELF_SUPPLY_COST,
        a_tbl_merge(i).self_supply_fr_cost          AS    SELF_SUPPLY_FR_COST,
        a_tbl_merge(i).wac_adj_amt                  AS    WAC_ADJ_AMT,
        a_tbl_merge(i).invoice_adj_qty              AS    INVOICE_ADJ_QTY,
        a_tbl_merge(i).invoice_adj_selling          AS    INVOICE_ADJ_SELLING,
        a_tbl_merge(i).invoice_adj_cost             AS    INVOICE_ADJ_COST,
        a_tbl_merge(i).rndm_mass_pos_var            AS    RNDM_MASS_POS_VAR,
        a_tbl_merge(i).mkup_selling                 AS    MKUP_SELLING,
        a_tbl_merge(i).mkup_cancel_selling          AS    MKUP_CANCEL_SELLING,
        a_tbl_merge(i).mkdn_selling                 AS    MKDN_SELLING,
        a_tbl_merge(i).mkdn_cancel_selling          AS    MKDN_CANCEL_SELLING,
        a_tbl_merge(i).prom_mkdn_qty                AS    PROM_MKDN_QTY,
        a_tbl_merge(i).prom_mkdn_selling            AS    PROM_MKDN_SELLING,
        a_tbl_merge(i).clear_mkdn_selling           AS    CLEAR_MKDN_SELLING,
        a_tbl_merge(i).mkdn_sales_qty               AS    MKDN_SALES_QTY,
        a_tbl_merge(i).mkdn_sales                   AS    MKDN_SALES,
        a_tbl_merge(i).mkdn_sales_cost              AS    MKDN_SALES_COST,
        a_tbl_merge(i).net_mkdn                     AS    NET_MKDN,
        a_tbl_merge(i).rtv_qty                      AS    RTV_QTY,
        a_tbl_merge(i).rtv_cases                    AS    RTV_CASES,
        a_tbl_merge(i).rtv_selling                  AS    RTV_SELLING,
        a_tbl_merge(i).rtv_cost                     AS    RTV_COST,
        a_tbl_merge(i).rtv_fr_cost                  AS    RTV_FR_COST,
        a_tbl_merge(i).sdn_out_qty                  AS    SDN_OUT_QTY,
        a_tbl_merge(i).sdn_out_selling              AS    SDN_OUT_SELLING,
        a_tbl_merge(i).sdn_out_cost                 AS    SDN_OUT_COST,
        a_tbl_merge(i).sdn_out_fr_cost              AS    SDN_OUT_FR_COST,
        a_tbl_merge(i).sdn_out_cases                AS    SDN_OUT_CASES,
        a_tbl_merge(i).ibt_in_qty                   AS    IBT_IN_QTY,
        a_tbl_merge(i).ibt_in_selling               AS    IBT_IN_SELLING,
        a_tbl_merge(i).ibt_in_cost                  AS    IBT_IN_COST,
        a_tbl_merge(i).ibt_in_fr_cost               AS    IBT_IN_FR_COST,
        a_tbl_merge(i).ibt_out_qty                  AS    IBT_OUT_QTY,
        a_tbl_merge(i).ibt_out_selling              AS    IBT_OUT_SELLING,
        a_tbl_merge(i).ibt_out_cost                 AS    IBT_OUT_COST,
        a_tbl_merge(i).ibt_out_fr_cost              AS    IBT_OUT_FR_COST,
        a_tbl_merge(i).net_ibt_qty                  AS    NET_IBT_QTY,
        a_tbl_merge(i).net_ibt_selling              AS    NET_IBT_SELLING,
        a_tbl_merge(i).shrink_excl_some_dept_cost   AS    SHRINK_EXCL_SOME_DEPT_COST,
        a_tbl_merge(i).gain_excl_some_dept_cost     AS    GAIN_EXCL_SOME_DEPT_COST,
        a_tbl_merge(i).net_waste_qty                AS    NET_WASTE_QTY,
        a_tbl_merge(i).trunked_qty                  AS    TRUNKED_QTY,
        a_tbl_merge(i).trunked_cases                AS    TRUNKED_CASES,
        a_tbl_merge(i).trunked_selling              AS    TRUNKED_SELLING,
        a_tbl_merge(i).trunked_cost                 AS    TRUNKED_COST,
        a_tbl_merge(i).dc_delivered_qty             AS    DC_DELIVERED_QTY,
        a_tbl_merge(i).dc_delivered_cases           AS    DC_DELIVERED_CASES,
        a_tbl_merge(i).dc_delivered_selling         AS    DC_DELIVERED_SELLING,
        a_tbl_merge(i).dc_delivered_cost            AS    DC_DELIVERED_COST,
        a_tbl_merge(i).net_inv_adj_qty              AS    NET_INV_ADJ_QTY,
        a_tbl_merge(i).net_inv_adj_selling          AS    NET_INV_ADJ_SELLING,
        a_tbl_merge(i).net_inv_adj_cost             AS    NET_INV_ADJ_COST,
        a_tbl_merge(i).net_inv_adj_fr_cost          AS    NET_INV_ADJ_FR_COST,
        a_tbl_merge(i).last_updated_date            AS    LAST_UPDATED_DATE,
        a_tbl_merge(i).ch_alloc_qty                 AS    CH_ALLOC_QTY,
        a_tbl_merge(i).ch_alloc_selling             AS    CH_ALLOC_SELLING,
-- MULTICURRENCY --        
        a_tbl_merge(i).	IBT_OUT_SELLING_LOCAL	      AS	IBT_OUT_SELLING_LOCAL	,
        a_tbl_merge(i).	IBT_OUT_COST_LOCAL	        AS	IBT_OUT_COST_LOCAL	,
        a_tbl_merge(i).	IBT_OUT_FR_COST_LOCAL	      AS	IBT_OUT_FR_COST_LOCAL	,
        a_tbl_merge(i).	NET_IBT_SELLING_LOCAL	      AS	NET_IBT_SELLING_LOCAL	,
        a_tbl_merge(i).	SHRINK_EXCL_SOME_DEPT_COST_LCL	AS	SHRINK_EXCL_SOME_DEPT_COST_LCL	,
        a_tbl_merge(i).	GAIN_EXCL_SOME_DEPT_COST_LOCAL	AS	GAIN_EXCL_SOME_DEPT_COST_LOCAL	,
        a_tbl_merge(i).	TRUNKED_SELLING_LOCAL	      AS	TRUNKED_SELLING_LOCAL	,
        a_tbl_merge(i).	TRUNKED_COST_LOCAL	        AS	TRUNKED_COST_LOCAL	,
        a_tbl_merge(i).	DC_DELIVERED_SELLING_LOCAL	AS	DC_DELIVERED_SELLING_LOCAL	,
        a_tbl_merge(i).	DC_DELIVERED_COST_LOCAL	    AS	DC_DELIVERED_COST_LOCAL	,
        a_tbl_merge(i).	NET_INV_ADJ_SELLING_LOCAL	  AS	NET_INV_ADJ_SELLING_LOCAL	,
        a_tbl_merge(i).	NET_INV_ADJ_COST_LOCAL	    AS	NET_INV_ADJ_COST_LOCAL	,
        a_tbl_merge(i).	NET_INV_ADJ_FR_COST_LOCAL	  AS	NET_INV_ADJ_FR_COST_LOCAL	,
        a_tbl_merge(i).	CH_ALLOC_SELLING_LOCAL	    AS	CH_ALLOC_SELLING_LOCAL	,
        a_tbl_merge(i).	ABS_SHRINKAGE_SELLING_DEPT_LCL	AS	ABS_SHRINKAGE_SELLING_DEPT_LCL	,
        a_tbl_merge(i).	ABS_SHRINKAGE_COST_DEPT_LOCAL	  AS	ABS_SHRINKAGE_COST_DEPT_LOCAL	,
        a_tbl_merge(i).	PROM_SALES_LOCAL	          AS	PROM_SALES_LOCAL	,
        a_tbl_merge(i).	PROM_SALES_COST_LOCAL	      AS	PROM_SALES_COST_LOCAL	,
        a_tbl_merge(i).	PROM_SALES_FR_COST_LOCAL	  AS	PROM_SALES_FR_COST_LOCAL	,
        a_tbl_merge(i).	PROM_SALES_MARGIN_LOCAL	    AS	PROM_SALES_MARGIN_LOCAL	,
        a_tbl_merge(i).	FRANCHISE_PROM_SALES_LOCAL	AS	FRANCHISE_PROM_SALES_LOCAL	,
        a_tbl_merge(i).	FRNCH_PROM_SALES_MARGIN_LOCAL	AS	FRNCH_PROM_SALES_MARGIN_LOCAL	,
        a_tbl_merge(i).	PROM_DISCOUNT_NO_LOCAL	    AS	PROM_DISCOUNT_NO_LOCAL	,
        a_tbl_merge(i).	HO_PROM_DISCOUNT_AMT_LOCAL	AS	HO_PROM_DISCOUNT_AMT_LOCAL	,
        a_tbl_merge(i).	ST_PROM_DISCOUNT_AMT_LOCAL	AS	ST_PROM_DISCOUNT_AMT_LOCAL	,
        a_tbl_merge(i).	CLEAR_SALES_LOCAL	          AS	CLEAR_SALES_LOCAL	,
        a_tbl_merge(i).	CLEAR_SALES_COST_LOCAL	    AS	CLEAR_SALES_COST_LOCAL	,
        a_tbl_merge(i).	CLEAR_SALES_FR_COST_LOCAL	  AS	CLEAR_SALES_FR_COST_LOCAL	,
        a_tbl_merge(i).	CLEAR_SALES_MARGIN_LOCAL	  AS	CLEAR_SALES_MARGIN_LOCAL	,
        a_tbl_merge(i).	FRANCHISE_CLEAR_SALES_LOCAL	AS	FRANCHISE_CLEAR_SALES_LOCAL	,
        a_tbl_merge(i).	FRNCH_CLEAR_SALES_MARGIN_LOCAL	AS	FRNCH_CLEAR_SALES_MARGIN_LOCAL	,
        a_tbl_merge(i).	WASTE_SELLING_LOCAL	        AS	WASTE_SELLING_LOCAL	,
        a_tbl_merge(i).	WASTE_COST_LOCAL	          AS	WASTE_COST_LOCAL	,
        a_tbl_merge(i).	WASTE_FR_COST_LOCAL	        AS	WASTE_FR_COST_LOCAL	,
        a_tbl_merge(i).	SHRINK_SELLING_LOCAL	      AS	SHRINK_SELLING_LOCAL	,
        a_tbl_merge(i).	SHRINK_COST_LOCAL	          AS	SHRINK_COST_LOCAL	,
        a_tbl_merge(i).	SHRINK_FR_COST_LOCAL	      AS	SHRINK_FR_COST_LOCAL	,
        a_tbl_merge(i).	GAIN_SELLING_LOCAL	        AS	GAIN_SELLING_LOCAL	,
        a_tbl_merge(i).	GAIN_COST_LOCAL	            AS	GAIN_COST_LOCAL	,
        a_tbl_merge(i).	GAIN_FR_COST_LOCAL        	AS	GAIN_FR_COST_LOCAL	,
        a_tbl_merge(i).	GRN_SELLING_LOCAL	          AS	GRN_SELLING_LOCAL	,
        a_tbl_merge(i).	GRN_COST_LOCAL	            AS	GRN_COST_LOCAL	,
        a_tbl_merge(i).	GRN_FR_COST_LOCAL	          AS	GRN_FR_COST_LOCAL	,
        a_tbl_merge(i).	GRN_MARGIN_LOCAL	          AS	GRN_MARGIN_LOCAL	,
        a_tbl_merge(i).	SHRINKAGE_SELLING_LOCAL	    AS	SHRINKAGE_SELLING_LOCAL	,
        a_tbl_merge(i).	SHRINKAGE_COST_LOCAL	      AS	SHRINKAGE_COST_LOCAL	,
        a_tbl_merge(i).	SHRINKAGE_FR_COST_LOCAL	    AS	SHRINKAGE_FR_COST_LOCAL	,
        a_tbl_merge(i).	ABS_SHRINKAGE_SELLING_LOCAL	AS	ABS_SHRINKAGE_SELLING_LOCAL	,
        a_tbl_merge(i).	ABS_SHRINKAGE_COST_LOCAL	  AS	ABS_SHRINKAGE_COST_LOCAL	,
        a_tbl_merge(i).	ABS_SHRINKAGE_FR_COST_LOCAL	AS	ABS_SHRINKAGE_FR_COST_LOCAL	,
        a_tbl_merge(i).	CLAIM_SELLING_LOCAL	        AS	CLAIM_SELLING_LOCAL	,
        a_tbl_merge(i).	CLAIM_COST_LOCAL	          AS	CLAIM_COST_LOCAL	,
        a_tbl_merge(i).	CLAIM_FR_COST_LOCAL       	AS	CLAIM_FR_COST_LOCAL	,
        a_tbl_merge(i).	SELF_SUPPLY_SELLING_LOCAL 	AS	SELF_SUPPLY_SELLING_LOCAL	,
        a_tbl_merge(i).	SELF_SUPPLY_COST_LOCAL	    AS	SELF_SUPPLY_COST_LOCAL	,
        a_tbl_merge(i).	SELF_SUPPLY_FR_COST_LOCAL	  AS	SELF_SUPPLY_FR_COST_LOCAL	,
        a_tbl_merge(i).	WAC_ADJ_AMT_LOCAL	          AS	WAC_ADJ_AMT_LOCAL	,
        a_tbl_merge(i).	INVOICE_ADJ_SELLING_LOCAL	  AS	INVOICE_ADJ_SELLING_LOCAL	,
        a_tbl_merge(i).	INVOICE_ADJ_COST_LOCAL	    AS	INVOICE_ADJ_COST_LOCAL	,
        a_tbl_merge(i).	MKUP_SELLING_LOCAL	        AS	MKUP_SELLING_LOCAL	,
        a_tbl_merge(i).	MKUP_CANCEL_SELLING_LOCAL	  AS	MKUP_CANCEL_SELLING_LOCAL	,
        a_tbl_merge(i).	MKDN_SELLING_LOCAL	        AS	MKDN_SELLING_LOCAL	,
        a_tbl_merge(i).	MKDN_CANCEL_SELLING_LOCAL	  AS	MKDN_CANCEL_SELLING_LOCAL	,
        a_tbl_merge(i).	PROM_MKDN_SELLING_LOCAL	    AS	PROM_MKDN_SELLING_LOCAL	,
        a_tbl_merge(i).	CLEAR_MKDN_SELLING_LOCAL	  AS	CLEAR_MKDN_SELLING_LOCAL	,
        a_tbl_merge(i).	MKDN_SALES_LOCAL	          AS	MKDN_SALES_LOCAL	,
        a_tbl_merge(i).	MKDN_SALES_COST_LOCAL	      AS	MKDN_SALES_COST_LOCAL	,
        a_tbl_merge(i).	NET_MKDN_LOCAL	            AS	NET_MKDN_LOCAL	,
        a_tbl_merge(i).	RTV_SELLING_LOCAL	          AS	RTV_SELLING_LOCAL	,
        a_tbl_merge(i).	RTV_COST_LOCAL	            AS	RTV_COST_LOCAL	,
        a_tbl_merge(i).	RTV_FR_COST_LOCAL	          AS	RTV_FR_COST_LOCAL	,
        a_tbl_merge(i).	SDN_OUT_SELLING_LOCAL     	AS	SDN_OUT_SELLING_LOCAL	,
        a_tbl_merge(i).	SDN_OUT_COST_LOCAL	        AS	SDN_OUT_COST_LOCAL	,
        a_tbl_merge(i).	SDN_OUT_FR_COST_LOCAL	      AS	SDN_OUT_FR_COST_LOCAL	,
        a_tbl_merge(i).	IBT_IN_SELLING_LOCAL	      AS	IBT_IN_SELLING_LOCAL	,
        a_tbl_merge(i).	IBT_IN_COST_LOCAL	          AS	IBT_IN_COST_LOCAL	,
        a_tbl_merge(i).	IBT_IN_FR_COST_LOCAL	      AS	IBT_IN_FR_COST_LOCAL	 
        
                
from dual) mer_rlidrs
ON (mer_rlidrs.SK1_LOCATION_NO = rtl_lidrs.SK1_LOCATION_NO
    and mer_rlidrs.SK1_ITEM_NO = rtl_lidrs.SK1_ITEM_NO
    and mer_rlidrs.POST_DATE = rtl_lidrs.POST_DATE)
WHEN MATCHED
THEN
UPDATE
SET           prom_sales_qty                  = mer_rlidrs.prom_sales_qty,
              prom_sales                      = mer_rlidrs.prom_sales,
              prom_sales_cost                 = mer_rlidrs.prom_sales_cost,
              prom_sales_fr_cost              = mer_rlidrs.prom_sales_fr_cost,
              prom_sales_margin               = mer_rlidrs.prom_sales_margin,
              franchise_prom_sales            = mer_rlidrs.franchise_prom_sales,
              franchise_prom_sales_margin     = mer_rlidrs.franchise_prom_sales_margin,
              prom_discount_no                = mer_rlidrs.prom_discount_no,
              ho_prom_discount_amt            = mer_rlidrs.ho_prom_discount_amt,
              ho_prom_discount_qty            = mer_rlidrs.ho_prom_discount_qty,
              st_prom_discount_amt            = mer_rlidrs.st_prom_discount_amt,
              st_prom_discount_qty            = mer_rlidrs.st_prom_discount_qty,
              clear_sales_qty                 = mer_rlidrs.clear_sales_qty,
              clear_sales                     = mer_rlidrs.clear_sales,
              clear_sales_cost                = mer_rlidrs.clear_sales_cost,
              clear_sales_fr_cost             = mer_rlidrs.clear_sales_fr_cost,
              clear_sales_margin              = mer_rlidrs.clear_sales_margin,
              franchise_clear_sales           = mer_rlidrs.franchise_clear_sales,
              franchise_clear_sales_margin    = mer_rlidrs.franchise_clear_sales_margin,
              waste_qty                       = mer_rlidrs.waste_qty,
              waste_selling                   = mer_rlidrs.waste_selling,
              waste_cost                      = mer_rlidrs.waste_cost,
              waste_fr_cost                   = mer_rlidrs.waste_fr_cost,
              shrink_qty                      = mer_rlidrs.shrink_qty,
              shrink_selling                  = mer_rlidrs.shrink_selling,
              shrink_cost                     = mer_rlidrs.shrink_cost,
              shrink_fr_cost                  = mer_rlidrs.shrink_fr_cost,
              gain_qty                        = mer_rlidrs.gain_qty,
              gain_selling                    = mer_rlidrs.gain_selling,
              gain_cost                       = mer_rlidrs.gain_cost,
              gain_fr_cost                    = mer_rlidrs.gain_fr_cost,
              grn_qty                         = mer_rlidrs.grn_qty,
              grn_cases                       = mer_rlidrs.grn_cases,
              grn_selling                     = mer_rlidrs.grn_selling,
              grn_cost                        = mer_rlidrs.grn_cost,
              grn_fr_cost                     = mer_rlidrs.grn_fr_cost,
              grn_margin                      = mer_rlidrs.grn_margin,
              shrinkage_qty                   = mer_rlidrs.shrinkage_qty,
              shrinkage_selling               = mer_rlidrs.shrinkage_selling,
              shrinkage_cost                  = mer_rlidrs.shrinkage_cost,
              shrinkage_fr_cost               = mer_rlidrs.shrinkage_fr_cost,
              abs_shrinkage_qty               = mer_rlidrs.abs_shrinkage_qty,
              abs_shrinkage_selling           = mer_rlidrs.abs_shrinkage_selling,
              abs_shrinkage_cost              = mer_rlidrs.abs_shrinkage_cost,
              abs_shrinkage_fr_cost           = mer_rlidrs.abs_shrinkage_fr_cost,
              claim_qty                       = mer_rlidrs.claim_qty,
              claim_selling                   = mer_rlidrs.claim_selling,
              claim_cost                      = mer_rlidrs.claim_cost,
              claim_fr_cost                   = mer_rlidrs.claim_fr_cost,
              self_supply_qty                 = mer_rlidrs.self_supply_qty,
              self_supply_selling             = mer_rlidrs.self_supply_selling,
              self_supply_cost                = mer_rlidrs.self_supply_cost,
              self_supply_fr_cost             = mer_rlidrs.self_supply_fr_cost,
              wac_adj_amt                     = mer_rlidrs.wac_adj_amt,
              invoice_adj_qty                 = mer_rlidrs.invoice_adj_qty,
              invoice_adj_selling             = mer_rlidrs.invoice_adj_selling,
              invoice_adj_cost                = mer_rlidrs.invoice_adj_cost,
              rndm_mass_pos_var               = mer_rlidrs.rndm_mass_pos_var,
              mkup_selling                    = mer_rlidrs.mkup_selling,
              mkup_cancel_selling             = mer_rlidrs.mkup_cancel_selling,
              mkdn_selling                    = mer_rlidrs.mkdn_selling,
              mkdn_cancel_selling             = mer_rlidrs.mkdn_cancel_selling,
              prom_mkdn_qty                   = mer_rlidrs.prom_mkdn_qty,
              prom_mkdn_selling               = mer_rlidrs.prom_mkdn_selling,
              clear_mkdn_selling              = mer_rlidrs.clear_mkdn_selling,
              mkdn_sales_qty                  = mer_rlidrs.mkdn_sales_qty,
              mkdn_sales                      = mer_rlidrs.mkdn_sales,
              mkdn_sales_cost                 = mer_rlidrs.mkdn_sales_cost,
              net_mkdn                        = mer_rlidrs.net_mkdn,
              rtv_qty                         = mer_rlidrs.rtv_qty,
              rtv_cases                       = mer_rlidrs.rtv_cases,
              rtv_selling                     = mer_rlidrs.rtv_selling,
              rtv_cost                        = mer_rlidrs.rtv_cost,
              rtv_fr_cost                     = mer_rlidrs.rtv_fr_cost,
              sdn_out_qty                     = mer_rlidrs.sdn_out_qty,
              sdn_out_selling                 = mer_rlidrs.sdn_out_selling,
              sdn_out_cost                    = mer_rlidrs.sdn_out_cost,
              sdn_out_fr_cost                 = mer_rlidrs.sdn_out_fr_cost,
              sdn_out_cases                   = mer_rlidrs.sdn_out_cases,
              ibt_in_qty                      = mer_rlidrs.ibt_in_qty,
              ibt_in_selling                  = mer_rlidrs.ibt_in_selling,
              ibt_in_cost                     = mer_rlidrs.ibt_in_cost,
              ibt_in_fr_cost                  = mer_rlidrs.ibt_in_fr_cost,
              ibt_out_qty                     = mer_rlidrs.ibt_out_qty,
              ibt_out_selling                 = mer_rlidrs.ibt_out_selling,
              ibt_out_cost                    = mer_rlidrs.ibt_out_cost,
              ibt_out_fr_cost                 = mer_rlidrs.ibt_out_fr_cost,
              net_ibt_qty                     = mer_rlidrs.net_ibt_qty,
              net_ibt_selling                 = mer_rlidrs.net_ibt_selling,
              shrink_excl_some_dept_cost      = mer_rlidrs.shrink_excl_some_dept_cost,
              gain_excl_some_dept_cost        = mer_rlidrs.gain_excl_some_dept_cost,
              net_waste_qty                   = mer_rlidrs.net_waste_qty,
--              dc_delivered_qty                = mer_rlidrs.dc_delivered_qty,
--              dc_delivered_cases              = mer_rlidrs.dc_delivered_cases,
--              dc_delivered_selling            = mer_rlidrs.dc_delivered_selling,
--              dc_delivered_cost               = mer_rlidrs.dc_delivered_cost,
              net_inv_adj_qty                 = mer_rlidrs.net_inv_adj_qty,
              net_inv_adj_selling             = mer_rlidrs.net_inv_adj_selling,
              net_inv_adj_cost                = mer_rlidrs.net_inv_adj_cost,
              net_inv_adj_fr_cost             = mer_rlidrs.net_inv_adj_fr_cost,
--MC--              
              
              IBT_OUT_SELLING_LOCAL	          = mer_rlidrs.	IBT_OUT_SELLING_LOCAL	,
              IBT_OUT_COST_LOCAL	            = mer_rlidrs.	IBT_OUT_COST_LOCAL	,
              IBT_OUT_FR_COST_LOCAL	          = mer_rlidrs.	IBT_OUT_FR_COST_LOCAL	,
              NET_IBT_SELLING_LOCAL	          = mer_rlidrs.	NET_IBT_SELLING_LOCAL	,
              SHRINK_EXCL_SOME_DEPT_COST_LCL	= mer_rlidrs.	SHRINK_EXCL_SOME_DEPT_COST_LCL	,
              GAIN_EXCL_SOME_DEPT_COST_LOCAL	= mer_rlidrs.	GAIN_EXCL_SOME_DEPT_COST_LOCAL	,
              TRUNKED_SELLING_LOCAL	          = mer_rlidrs.	TRUNKED_SELLING_LOCAL	,
              TRUNKED_COST_LOCAL	            = mer_rlidrs.	TRUNKED_COST_LOCAL	,
              DC_DELIVERED_SELLING_LOCAL	    = mer_rlidrs.	DC_DELIVERED_SELLING_LOCAL	,
              DC_DELIVERED_COST_LOCAL	        = mer_rlidrs.	DC_DELIVERED_COST_LOCAL	,
              NET_INV_ADJ_SELLING_LOCAL	      = mer_rlidrs.	NET_INV_ADJ_SELLING_LOCAL	,
              NET_INV_ADJ_COST_LOCAL	        = mer_rlidrs.	NET_INV_ADJ_COST_LOCAL	,
              NET_INV_ADJ_FR_COST_LOCAL     	= mer_rlidrs.	NET_INV_ADJ_FR_COST_LOCAL	,
              CH_ALLOC_SELLING_LOCAL	        = mer_rlidrs.	CH_ALLOC_SELLING_LOCAL	,
              ABS_SHRINKAGE_SELLING_DEPT_LCL	= mer_rlidrs.	ABS_SHRINKAGE_SELLING_DEPT_LCL	,
              ABS_SHRINKAGE_COST_DEPT_LOCAL	  = mer_rlidrs.	ABS_SHRINKAGE_COST_DEPT_LOCAL	,
              PROM_SALES_LOCAL	              = mer_rlidrs.	PROM_SALES_LOCAL	,
              PROM_SALES_COST_LOCAL	          = mer_rlidrs.	PROM_SALES_COST_LOCAL	,
              PROM_SALES_FR_COST_LOCAL	      = mer_rlidrs.	PROM_SALES_FR_COST_LOCAL	,
              PROM_SALES_MARGIN_LOCAL	        = mer_rlidrs.	PROM_SALES_MARGIN_LOCAL	,
              FRANCHISE_PROM_SALES_LOCAL	    = mer_rlidrs.	FRANCHISE_PROM_SALES_LOCAL	,
              FRNCH_PROM_SALES_MARGIN_LOCAL	  = mer_rlidrs.	FRNCH_PROM_SALES_MARGIN_LOCAL	,
              PROM_DISCOUNT_NO_LOCAL	        = mer_rlidrs.	PROM_DISCOUNT_NO_LOCAL	,
              HO_PROM_DISCOUNT_AMT_LOCAL	    = mer_rlidrs.	HO_PROM_DISCOUNT_AMT_LOCAL	,
              ST_PROM_DISCOUNT_AMT_LOCAL	    = mer_rlidrs.	ST_PROM_DISCOUNT_AMT_LOCAL	,
              CLEAR_SALES_LOCAL	              = mer_rlidrs.	CLEAR_SALES_LOCAL	,
              CLEAR_SALES_COST_LOCAL	        = mer_rlidrs.	CLEAR_SALES_COST_LOCAL	,
              CLEAR_SALES_FR_COST_LOCAL     	= mer_rlidrs.	CLEAR_SALES_FR_COST_LOCAL	,
              CLEAR_SALES_MARGIN_LOCAL	      = mer_rlidrs.	CLEAR_SALES_MARGIN_LOCAL	,
              FRANCHISE_CLEAR_SALES_LOCAL   	= mer_rlidrs.	FRANCHISE_CLEAR_SALES_LOCAL	,
              FRNCH_CLEAR_SALES_MARGIN_LOCAL	= mer_rlidrs.	FRNCH_CLEAR_SALES_MARGIN_LOCAL	,
              WASTE_SELLING_LOCAL	            = mer_rlidrs.	WASTE_SELLING_LOCAL	,
              WASTE_COST_LOCAL	              = mer_rlidrs.	WASTE_COST_LOCAL	,
              WASTE_FR_COST_LOCAL	            = mer_rlidrs.	WASTE_FR_COST_LOCAL	,
              SHRINK_SELLING_LOCAL	          = mer_rlidrs.	SHRINK_SELLING_LOCAL	,
              SHRINK_COST_LOCAL	              = mer_rlidrs.	SHRINK_COST_LOCAL	,
              SHRINK_FR_COST_LOCAL	          = mer_rlidrs.	SHRINK_FR_COST_LOCAL	,
              GAIN_SELLING_LOCAL	            = mer_rlidrs.	GAIN_SELLING_LOCAL	,
              GAIN_COST_LOCAL	                = mer_rlidrs.	GAIN_COST_LOCAL	,
              GAIN_FR_COST_LOCAL            	= mer_rlidrs.	GAIN_FR_COST_LOCAL	,
              GRN_SELLING_LOCAL             	= mer_rlidrs.	GRN_SELLING_LOCAL	,
              GRN_COST_LOCAL	                = mer_rlidrs.	GRN_COST_LOCAL	,
              GRN_FR_COST_LOCAL             	= mer_rlidrs.	GRN_FR_COST_LOCAL	,
              GRN_MARGIN_LOCAL	              = mer_rlidrs.	GRN_MARGIN_LOCAL	,
              SHRINKAGE_SELLING_LOCAL         = mer_rlidrs.	SHRINKAGE_SELLING_LOCAL	,
              SHRINKAGE_COST_LOCAL	          = mer_rlidrs.	SHRINKAGE_COST_LOCAL	,
              SHRINKAGE_FR_COST_LOCAL	        = mer_rlidrs.	SHRINKAGE_FR_COST_LOCAL	,
              ABS_SHRINKAGE_SELLING_LOCAL	    = mer_rlidrs.	ABS_SHRINKAGE_SELLING_LOCAL	,
              ABS_SHRINKAGE_COST_LOCAL	      = mer_rlidrs.	ABS_SHRINKAGE_COST_LOCAL	,
              ABS_SHRINKAGE_FR_COST_LOCAL	    = mer_rlidrs.	ABS_SHRINKAGE_FR_COST_LOCAL	,
              CLAIM_SELLING_LOCAL           	= mer_rlidrs.	CLAIM_SELLING_LOCAL	,
              CLAIM_COST_LOCAL	              = mer_rlidrs.	CLAIM_COST_LOCAL	,
              CLAIM_FR_COST_LOCAL	            = mer_rlidrs.	CLAIM_FR_COST_LOCAL	,
              SELF_SUPPLY_SELLING_LOCAL	      = mer_rlidrs.	SELF_SUPPLY_SELLING_LOCAL	,
              SELF_SUPPLY_COST_LOCAL	        = mer_rlidrs.	SELF_SUPPLY_COST_LOCAL	,
              SELF_SUPPLY_FR_COST_LOCAL	      = mer_rlidrs.	SELF_SUPPLY_FR_COST_LOCAL	,
              WAC_ADJ_AMT_LOCAL	              = mer_rlidrs.	WAC_ADJ_AMT_LOCAL	,
              INVOICE_ADJ_SELLING_LOCAL     	= mer_rlidrs.	INVOICE_ADJ_SELLING_LOCAL	,
              INVOICE_ADJ_COST_LOCAL	        = mer_rlidrs.	INVOICE_ADJ_COST_LOCAL	,
              MKUP_SELLING_LOCAL	            = mer_rlidrs.	MKUP_SELLING_LOCAL	,
              MKUP_CANCEL_SELLING_LOCAL	      = mer_rlidrs.	MKUP_CANCEL_SELLING_LOCAL	,
              MKDN_SELLING_LOCAL	            = mer_rlidrs.	MKDN_SELLING_LOCAL	,
              MKDN_CANCEL_SELLING_LOCAL	      = mer_rlidrs.	MKDN_CANCEL_SELLING_LOCAL	,
              PROM_MKDN_SELLING_LOCAL	        = mer_rlidrs.	PROM_MKDN_SELLING_LOCAL	,
              CLEAR_MKDN_SELLING_LOCAL	      = mer_rlidrs.	CLEAR_MKDN_SELLING_LOCAL	,
              MKDN_SALES_LOCAL	              = mer_rlidrs.	MKDN_SALES_LOCAL	,
              MKDN_SALES_COST_LOCAL         	= mer_rlidrs.	MKDN_SALES_COST_LOCAL	,
              NET_MKDN_LOCAL	                = mer_rlidrs.	NET_MKDN_LOCAL	,
              RTV_SELLING_LOCAL             	= mer_rlidrs.	RTV_SELLING_LOCAL	,
              RTV_COST_LOCAL	                = mer_rlidrs.	RTV_COST_LOCAL	,
              RTV_FR_COST_LOCAL	              = mer_rlidrs.	RTV_FR_COST_LOCAL	,
              SDN_OUT_SELLING_LOCAL         	= mer_rlidrs.	SDN_OUT_SELLING_LOCAL	,
              SDN_OUT_COST_LOCAL	            = mer_rlidrs.	SDN_OUT_COST_LOCAL	,
              SDN_OUT_FR_COST_LOCAL	          = mer_rlidrs.	SDN_OUT_FR_COST_LOCAL	,
              IBT_IN_SELLING_LOCAL	          = mer_rlidrs.	IBT_IN_SELLING_LOCAL	,
              IBT_IN_COST_LOCAL	              = mer_rlidrs.	IBT_IN_COST_LOCAL	,
              IBT_IN_FR_COST_LOCAL	          = mer_rlidrs.	IBT_IN_FR_COST_LOCAL	,
          
              last_updated_date               = mer_rlidrs.last_updated_date
WHEN NOT MATCHED
THEN
INSERT       (SK1_LOCATION_NO,
              SK1_ITEM_NO,
              POST_DATE,
              SK2_LOCATION_NO,
              SK2_ITEM_NO,
              PROM_SALES_QTY,
              PROM_SALES,
              PROM_SALES_COST,
              PROM_SALES_FR_COST,
              PROM_SALES_MARGIN,
              FRANCHISE_PROM_SALES,
              FRANCHISE_PROM_SALES_MARGIN,
              PROM_DISCOUNT_NO,
              HO_PROM_DISCOUNT_AMT,
              HO_PROM_DISCOUNT_QTY,
              ST_PROM_DISCOUNT_AMT,
              ST_PROM_DISCOUNT_QTY,
              CLEAR_SALES_QTY,
              CLEAR_SALES,
              CLEAR_SALES_COST,
              CLEAR_SALES_FR_COST,
              CLEAR_SALES_MARGIN,
              FRANCHISE_CLEAR_SALES,
              FRANCHISE_CLEAR_SALES_MARGIN,
              WASTE_QTY,
              WASTE_SELLING,
              WASTE_COST,
              WASTE_FR_COST,
              SHRINK_QTY,
              SHRINK_SELLING,
              SHRINK_COST,
              SHRINK_FR_COST,
              GAIN_QTY,
              GAIN_SELLING,
              GAIN_COST,
              GAIN_FR_COST,
              GRN_QTY,
              GRN_CASES,
              GRN_SELLING,
              GRN_COST,
              GRN_FR_COST,
              GRN_MARGIN,
              SHRINKAGE_QTY,
              SHRINKAGE_SELLING,
              SHRINKAGE_COST,
              SHRINKAGE_FR_COST,
              ABS_SHRINKAGE_QTY,
              ABS_SHRINKAGE_SELLING,
              ABS_SHRINKAGE_COST,
              ABS_SHRINKAGE_FR_COST,
              CLAIM_QTY,
              CLAIM_SELLING,
              CLAIM_COST,
              CLAIM_FR_COST,
              SELF_SUPPLY_QTY,
              SELF_SUPPLY_SELLING,
              SELF_SUPPLY_COST,
              SELF_SUPPLY_FR_COST,
              WAC_ADJ_AMT,
              INVOICE_ADJ_QTY,
              INVOICE_ADJ_SELLING,
              INVOICE_ADJ_COST,
              RNDM_MASS_POS_VAR,
              MKUP_SELLING,
              MKUP_CANCEL_SELLING,
              MKDN_SELLING,
              MKDN_CANCEL_SELLING,
              PROM_MKDN_QTY,
              PROM_MKDN_SELLING,
              CLEAR_MKDN_SELLING,
              MKDN_SALES_QTY,
              MKDN_SALES,
              MKDN_SALES_COST,
              NET_MKDN,
              RTV_QTY,
              RTV_CASES,
              RTV_SELLING,
              RTV_COST,
              RTV_FR_COST,
              SDN_OUT_QTY,
              SDN_OUT_SELLING,
              SDN_OUT_COST,
              SDN_OUT_FR_COST,
              SDN_OUT_CASES,
              IBT_IN_QTY,
              IBT_IN_SELLING,
              IBT_IN_COST,
              IBT_IN_FR_COST,
              IBT_OUT_QTY,
              IBT_OUT_SELLING,
              IBT_OUT_COST,
              IBT_OUT_FR_COST,
              NET_IBT_QTY,
              NET_IBT_SELLING,
              SHRINK_EXCL_SOME_DEPT_COST,
              GAIN_EXCL_SOME_DEPT_COST,
              NET_WASTE_QTY,
              TRUNKED_QTY,
              TRUNKED_CASES,
              TRUNKED_SELLING,
              TRUNKED_COST,
              DC_DELIVERED_QTY,
              DC_DELIVERED_CASES,
              DC_DELIVERED_SELLING,
              DC_DELIVERED_COST,
              NET_INV_ADJ_QTY,
              NET_INV_ADJ_SELLING,
              NET_INV_ADJ_COST,
              NET_INV_ADJ_FR_COST,
              LAST_UPDATED_DATE,
              CH_ALLOC_QTY,
              CH_ALLOC_SELLING,
--MC--              
              IBT_OUT_SELLING_LOCAL,
              IBT_OUT_COST_LOCAL,
              IBT_OUT_FR_COST_LOCAL,
              NET_IBT_SELLING_LOCAL,
              SHRINK_EXCL_SOME_DEPT_COST_LCL,
              GAIN_EXCL_SOME_DEPT_COST_LOCAL,
              TRUNKED_SELLING_LOCAL,
              TRUNKED_COST_LOCAL,
              DC_DELIVERED_SELLING_LOCAL,
              DC_DELIVERED_COST_LOCAL,
              NET_INV_ADJ_SELLING_LOCAL,
              NET_INV_ADJ_COST_LOCAL,
              NET_INV_ADJ_FR_COST_LOCAL,
              CH_ALLOC_SELLING_LOCAL,
              ABS_SHRINKAGE_SELLING_DEPT_LCL,
              ABS_SHRINKAGE_COST_DEPT_LOCAL,
              PROM_SALES_LOCAL,
              PROM_SALES_COST_LOCAL,
              PROM_SALES_FR_COST_LOCAL,
              PROM_SALES_MARGIN_LOCAL,
              FRANCHISE_PROM_SALES_LOCAL,
              FRNCH_PROM_SALES_MARGIN_LOCAL,
              PROM_DISCOUNT_NO_LOCAL,
              HO_PROM_DISCOUNT_AMT_LOCAL,
              ST_PROM_DISCOUNT_AMT_LOCAL,
              CLEAR_SALES_LOCAL,
              CLEAR_SALES_COST_LOCAL,
              CLEAR_SALES_FR_COST_LOCAL,
              CLEAR_SALES_MARGIN_LOCAL,
              FRANCHISE_CLEAR_SALES_LOCAL,
              FRNCH_CLEAR_SALES_MARGIN_LOCAL,
              WASTE_SELLING_LOCAL,
              WASTE_COST_LOCAL,
              WASTE_FR_COST_LOCAL,
              SHRINK_SELLING_LOCAL,
              SHRINK_COST_LOCAL,
              SHRINK_FR_COST_LOCAL,
              GAIN_SELLING_LOCAL,
              GAIN_COST_LOCAL,
              GAIN_FR_COST_LOCAL,
              GRN_SELLING_LOCAL,
              GRN_COST_LOCAL,
              GRN_FR_COST_LOCAL,
              GRN_MARGIN_LOCAL,
              SHRINKAGE_SELLING_LOCAL,
              SHRINKAGE_COST_LOCAL,
              SHRINKAGE_FR_COST_LOCAL,
              ABS_SHRINKAGE_SELLING_LOCAL,
              ABS_SHRINKAGE_COST_LOCAL,
              ABS_SHRINKAGE_FR_COST_LOCAL,
              CLAIM_SELLING_LOCAL,
              CLAIM_COST_LOCAL,
              CLAIM_FR_COST_LOCAL,
              SELF_SUPPLY_SELLING_LOCAL,
              SELF_SUPPLY_COST_LOCAL,
              SELF_SUPPLY_FR_COST_LOCAL,
              WAC_ADJ_AMT_LOCAL,
              INVOICE_ADJ_SELLING_LOCAL,
              INVOICE_ADJ_COST_LOCAL,
              MKUP_SELLING_LOCAL,
              MKUP_CANCEL_SELLING_LOCAL,
              MKDN_SELLING_LOCAL,
              MKDN_CANCEL_SELLING_LOCAL,
              PROM_MKDN_SELLING_LOCAL,
              CLEAR_MKDN_SELLING_LOCAL,
              MKDN_SALES_LOCAL,
              MKDN_SALES_COST_LOCAL,
              NET_MKDN_LOCAL,
              RTV_SELLING_LOCAL,
              RTV_COST_LOCAL,
              RTV_FR_COST_LOCAL,
              SDN_OUT_SELLING_LOCAL,
              SDN_OUT_COST_LOCAL,
              SDN_OUT_FR_COST_LOCAL,
              IBT_IN_SELLING_LOCAL,
              IBT_IN_COST_LOCAL,
              IBT_IN_FR_COST_LOCAL)
VALUES(
              mer_rlidrs.SK1_LOCATION_NO,
              mer_rlidrs.SK1_ITEM_NO,
              mer_rlidrs.POST_DATE,
              mer_rlidrs.SK2_LOCATION_NO,
              mer_rlidrs.SK2_ITEM_NO,
              mer_rlidrs.PROM_SALES_QTY,
              mer_rlidrs.PROM_SALES,
              mer_rlidrs.PROM_SALES_COST,
              mer_rlidrs.PROM_SALES_FR_COST,
              mer_rlidrs.PROM_SALES_MARGIN,
              mer_rlidrs.FRANCHISE_PROM_SALES,
              mer_rlidrs.FRANCHISE_PROM_SALES_MARGIN,
              mer_rlidrs.PROM_DISCOUNT_NO,
              mer_rlidrs.HO_PROM_DISCOUNT_AMT,
              mer_rlidrs.HO_PROM_DISCOUNT_QTY,
              mer_rlidrs.ST_PROM_DISCOUNT_AMT,
              mer_rlidrs.ST_PROM_DISCOUNT_QTY,
              mer_rlidrs.CLEAR_SALES_QTY,
              mer_rlidrs.CLEAR_SALES,
              mer_rlidrs.CLEAR_SALES_COST,
              mer_rlidrs.CLEAR_SALES_FR_COST,
              mer_rlidrs.CLEAR_SALES_MARGIN,
              mer_rlidrs.FRANCHISE_CLEAR_SALES,
              mer_rlidrs.FRANCHISE_CLEAR_SALES_MARGIN,
              mer_rlidrs.WASTE_QTY,
              mer_rlidrs.WASTE_SELLING,
              mer_rlidrs.WASTE_COST,
              mer_rlidrs.WASTE_FR_COST,
              mer_rlidrs.SHRINK_QTY,
              mer_rlidrs.SHRINK_SELLING,
              mer_rlidrs.SHRINK_COST,
              mer_rlidrs.SHRINK_FR_COST,
              mer_rlidrs.GAIN_QTY,
              mer_rlidrs.GAIN_SELLING,
              mer_rlidrs.GAIN_COST,
              mer_rlidrs.GAIN_FR_COST,
              mer_rlidrs.GRN_QTY,
              mer_rlidrs.GRN_CASES,
              mer_rlidrs.GRN_SELLING,
              mer_rlidrs.GRN_COST,
              mer_rlidrs.GRN_FR_COST,
              mer_rlidrs.GRN_MARGIN,
              mer_rlidrs.SHRINKAGE_QTY,
              mer_rlidrs.SHRINKAGE_SELLING,
              mer_rlidrs.SHRINKAGE_COST,
              mer_rlidrs.SHRINKAGE_FR_COST,
              mer_rlidrs.ABS_SHRINKAGE_QTY,
              mer_rlidrs.ABS_SHRINKAGE_SELLING,
              mer_rlidrs.ABS_SHRINKAGE_COST,
              mer_rlidrs.ABS_SHRINKAGE_FR_COST,
              mer_rlidrs.CLAIM_QTY,
              mer_rlidrs.CLAIM_SELLING,
              mer_rlidrs.CLAIM_COST,
              mer_rlidrs.CLAIM_FR_COST,
              mer_rlidrs.SELF_SUPPLY_QTY,
              mer_rlidrs.SELF_SUPPLY_SELLING,
              mer_rlidrs.SELF_SUPPLY_COST,
              mer_rlidrs.SELF_SUPPLY_FR_COST,
              mer_rlidrs.WAC_ADJ_AMT,
              mer_rlidrs.INVOICE_ADJ_QTY,
              mer_rlidrs.INVOICE_ADJ_SELLING,
              mer_rlidrs.INVOICE_ADJ_COST,
              mer_rlidrs.RNDM_MASS_POS_VAR,
              mer_rlidrs.MKUP_SELLING,
              mer_rlidrs.MKUP_CANCEL_SELLING,
              mer_rlidrs.MKDN_SELLING,
              mer_rlidrs.MKDN_CANCEL_SELLING,
              mer_rlidrs.PROM_MKDN_QTY,
              mer_rlidrs.PROM_MKDN_SELLING,
              mer_rlidrs.CLEAR_MKDN_SELLING,
              mer_rlidrs.MKDN_SALES_QTY,
              mer_rlidrs.MKDN_SALES,
              mer_rlidrs.MKDN_SALES_COST,
              mer_rlidrs.NET_MKDN,
              mer_rlidrs.RTV_QTY,
              mer_rlidrs.RTV_CASES,
              mer_rlidrs.RTV_SELLING,
              mer_rlidrs.RTV_COST,
              mer_rlidrs.RTV_FR_COST,
              mer_rlidrs.SDN_OUT_QTY,
              mer_rlidrs.SDN_OUT_SELLING,
              mer_rlidrs.SDN_OUT_COST,
              mer_rlidrs.SDN_OUT_FR_COST,
              mer_rlidrs.SDN_OUT_CASES,
              mer_rlidrs.IBT_IN_QTY,
              mer_rlidrs.IBT_IN_SELLING,
              mer_rlidrs.IBT_IN_COST,
              mer_rlidrs.IBT_IN_FR_COST,
              mer_rlidrs.IBT_OUT_QTY,
              mer_rlidrs.IBT_OUT_SELLING,
              mer_rlidrs.IBT_OUT_COST,
              mer_rlidrs.IBT_OUT_FR_COST,
              mer_rlidrs.NET_IBT_QTY,
              mer_rlidrs.NET_IBT_SELLING,
              mer_rlidrs.SHRINK_EXCL_SOME_DEPT_COST,
              mer_rlidrs.GAIN_EXCL_SOME_DEPT_COST,
              mer_rlidrs.NET_WASTE_QTY,
              mer_rlidrs.TRUNKED_QTY,
              mer_rlidrs.TRUNKED_CASES,
              mer_rlidrs.TRUNKED_SELLING,
              mer_rlidrs.TRUNKED_COST,
              mer_rlidrs.DC_DELIVERED_QTY,
              mer_rlidrs.DC_DELIVERED_CASES,
              mer_rlidrs.DC_DELIVERED_SELLING,
              mer_rlidrs.DC_DELIVERED_COST,
              mer_rlidrs.NET_INV_ADJ_QTY,
              mer_rlidrs.NET_INV_ADJ_SELLING,
              mer_rlidrs.NET_INV_ADJ_COST,
              mer_rlidrs.NET_INV_ADJ_FR_COST,
              mer_rlidrs.LAST_UPDATED_DATE,
              mer_rlidrs.CH_ALLOC_QTY,
              mer_rlidrs.CH_ALLOC_SELLING,
--MC--              
              mer_rlidrs.	IBT_OUT_SELLING_LOCAL	,
              mer_rlidrs.	IBT_OUT_COST_LOCAL	,
              mer_rlidrs.	IBT_OUT_FR_COST_LOCAL	,
              mer_rlidrs.	NET_IBT_SELLING_LOCAL	,
              mer_rlidrs.	SHRINK_EXCL_SOME_DEPT_COST_LCL	,
              mer_rlidrs.	GAIN_EXCL_SOME_DEPT_COST_LOCAL	,
              mer_rlidrs.	TRUNKED_SELLING_LOCAL	,
              mer_rlidrs.	TRUNKED_COST_LOCAL	,
              mer_rlidrs.	DC_DELIVERED_SELLING_LOCAL	,
              mer_rlidrs.	DC_DELIVERED_COST_LOCAL	,
              mer_rlidrs.	NET_INV_ADJ_SELLING_LOCAL	,
              mer_rlidrs.	NET_INV_ADJ_COST_LOCAL	,
              mer_rlidrs.	NET_INV_ADJ_FR_COST_LOCAL	,
              mer_rlidrs.	CH_ALLOC_SELLING_LOCAL	,
              mer_rlidrs.	ABS_SHRINKAGE_SELLING_DEPT_LCL	,
              mer_rlidrs.	ABS_SHRINKAGE_COST_DEPT_LOCAL	,
              mer_rlidrs.	PROM_SALES_LOCAL	,
              mer_rlidrs.	PROM_SALES_COST_LOCAL	,
              mer_rlidrs.	PROM_SALES_FR_COST_LOCAL	,
              mer_rlidrs.	PROM_SALES_MARGIN_LOCAL	,
              mer_rlidrs.	FRANCHISE_PROM_SALES_LOCAL	,
              mer_rlidrs.	FRNCH_PROM_SALES_MARGIN_LOCAL	,
              mer_rlidrs.	PROM_DISCOUNT_NO_LOCAL	,
              mer_rlidrs.	HO_PROM_DISCOUNT_AMT_LOCAL	,
              mer_rlidrs.	ST_PROM_DISCOUNT_AMT_LOCAL	,
              mer_rlidrs.	CLEAR_SALES_LOCAL	,
              mer_rlidrs.	CLEAR_SALES_COST_LOCAL	,
              mer_rlidrs.	CLEAR_SALES_FR_COST_LOCAL	,
              mer_rlidrs.	CLEAR_SALES_MARGIN_LOCAL	,
              mer_rlidrs.	FRANCHISE_CLEAR_SALES_LOCAL	,
              mer_rlidrs.	FRNCH_CLEAR_SALES_MARGIN_LOCAL	,
              mer_rlidrs.	WASTE_SELLING_LOCAL	,
              mer_rlidrs.	WASTE_COST_LOCAL	,
              mer_rlidrs.	WASTE_FR_COST_LOCAL	,
              mer_rlidrs.	SHRINK_SELLING_LOCAL	,
              mer_rlidrs.	SHRINK_COST_LOCAL	,
              mer_rlidrs.	SHRINK_FR_COST_LOCAL	,
              mer_rlidrs.	GAIN_SELLING_LOCAL	,
              mer_rlidrs.	GAIN_COST_LOCAL	,
              mer_rlidrs.	GAIN_FR_COST_LOCAL	,
              mer_rlidrs.	GRN_SELLING_LOCAL	,
              mer_rlidrs.	GRN_COST_LOCAL	,
              mer_rlidrs.	GRN_FR_COST_LOCAL	,
              mer_rlidrs.	GRN_MARGIN_LOCAL	,
              mer_rlidrs.	SHRINKAGE_SELLING_LOCAL	,
              mer_rlidrs.	SHRINKAGE_COST_LOCAL	,
              mer_rlidrs.	SHRINKAGE_FR_COST_LOCAL	,
              mer_rlidrs.	ABS_SHRINKAGE_SELLING_LOCAL	,
              mer_rlidrs.	ABS_SHRINKAGE_COST_LOCAL	,
              mer_rlidrs.	ABS_SHRINKAGE_FR_COST_LOCAL	,
              mer_rlidrs.	CLAIM_SELLING_LOCAL	,
              mer_rlidrs.	CLAIM_COST_LOCAL	,
              mer_rlidrs.	CLAIM_FR_COST_LOCAL	,
              mer_rlidrs.	SELF_SUPPLY_SELLING_LOCAL	,
              mer_rlidrs.	SELF_SUPPLY_COST_LOCAL	,
              mer_rlidrs.	SELF_SUPPLY_FR_COST_LOCAL	,
              mer_rlidrs.	WAC_ADJ_AMT_LOCAL	,
              mer_rlidrs.	INVOICE_ADJ_SELLING_LOCAL	,
              mer_rlidrs.	INVOICE_ADJ_COST_LOCAL	,
              mer_rlidrs.	MKUP_SELLING_LOCAL	,
              mer_rlidrs.	MKUP_CANCEL_SELLING_LOCAL	,
              mer_rlidrs.	MKDN_SELLING_LOCAL	,
              mer_rlidrs.	MKDN_CANCEL_SELLING_LOCAL	,
              mer_rlidrs.	PROM_MKDN_SELLING_LOCAL	,
              mer_rlidrs.	CLEAR_MKDN_SELLING_LOCAL	,
              mer_rlidrs.	MKDN_SALES_LOCAL	,
              mer_rlidrs.	MKDN_SALES_COST_LOCAL	,
              mer_rlidrs.	NET_MKDN_LOCAL	,
              mer_rlidrs.	RTV_SELLING_LOCAL	,
              mer_rlidrs.	RTV_COST_LOCAL	,
              mer_rlidrs.	RTV_FR_COST_LOCAL	,
              mer_rlidrs.	SDN_OUT_SELLING_LOCAL	,
              mer_rlidrs.	SDN_OUT_COST_LOCAL	,
              mer_rlidrs.	SDN_OUT_FR_COST_LOCAL	,
              mer_rlidrs.	IBT_IN_SELLING_LOCAL	,
              mer_rlidrs.	IBT_IN_COST_LOCAL	,
              mer_rlidrs.	IBT_IN_FR_COST_LOCAL
              
              );

       ----------------------------------PUT NEW MERGE HERE--------------------------------------

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_merge;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m) := g_rec_out;


   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_merge;
      a_tbl_merge  := a_empty_set_i;
      a_count_m    := 0;
      a_count      := 0;
      commit;
   end if;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

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

    l_text := 'LOAD OF rtl_mc_loc_item_dy_rms_sparse EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--    begin
--    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
--    end;


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--    g_date := '21 APR 2009'; --For testing only
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_mc_loc_item_dy_rms_sale;
    fetch c_fnd_mc_loc_item_dy_rms_sale bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_mc_loc_item_dy_rms_sale bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_mc_loc_item_dy_rms_sale;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_merge;
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


END WH_PRF_MC_112U;
