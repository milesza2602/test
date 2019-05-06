--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_112U_WLTEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_112U_WLTEST" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Sean Le Roux
--  Purpose:     Create RMS LID Sparse sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_sale
--               Output - rtl_loc_item_dy_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
--  21 august 2014 - wendy  - removing merge and changing cursor
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
g_rec_out            rtl_loc_item_dy_rms_sparse%rowtype;

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                     rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_112U_WLTEST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS SPARSE SALES FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
a_tbl_merge         tbl_array_i;
a_empty_set_i       tbl_array_i;

a_count             integer       := 0;
a_count_m          integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_dy_rms_sale is
with selext as (
 select /*+ parallel(fnd_lid,4) */
          fnd_lid.*
   from   fnd_rtl_loc_item_dy_rms_sale fnd_lid
   where  fnd_lid.last_updated_date  = g_date and
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
          fnd_lid.wac_adj_amt          ||
          fnd_lid.invoice_adj_qty      ||
          fnd_lid.rndm_mass_pos_var    ||
          fnd_lid.mkup_selling         ||
          fnd_lid.mkup_cancel_selling  ||
          fnd_lid.mkdn_selling         ||
          fnd_lid.mkdn_cancel_selling  ||
          fnd_lid.clear_mkdn_selling   ||
          fnd_lid.rtv_qty              ||
          fnd_lid.sdn_out_qty          ||
          fnd_lid.ibt_in_qty           ||
          fnd_lid.ibt_out_qty) is not null
          ),
selfnd as ( 
          fnd_lid.*,
          di.standard_uom_code,di.business_unit_no,di.vat_rate_perc,di.sk1_department_no,di.sk1_item_no,
          dl.chain_no,dl.sk1_location_no,
          decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
          nvl(fnd_li.clearance_ind,0) clearance_ind,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          dd.jv_dept_ind, dd.packaging_dept_ind, dd.gifting_dept_ind,
          dd.non_core_dept_ind, dd.bucket_dept_ind, dd.book_magazine_dept_ind
   from   selext fnd_lid,
          dim_item di,
          dim_location dl,
          fnd_location_item fnd_li,
          dim_item_hist dih,
          dim_location_hist dlh,
          dim_department dd
   where  fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          fnd_lid.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date and
          fnd_lid.location_no        = dlh.location_no and
          fnd_lid.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          di.sk1_department_no       = dd.sk1_department_no and
          fnd_lid.item_no            = fnd_li.item_no(+) and
          fnd_lid.location_no        = fnd_li.location_no(+) )
          select sf.*, rtl.post_date rec_exists
          from selfnd sf,
          rtl_loc_item_dy_rms_sparse rtl
          where sf.sk1_item_no = rtl.sk1_item_no(+)
          and sf.sk1_location_no = rtl.sk1_location_no(+)
          and sf.post_date = rtl.post_date(+)
          ;

g_rec_in                   c_fnd_rtl_loc_item_dy_rms_sale%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_rms_sale%rowtype;
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
   g_rec_out.prom_sales                      := g_rec_in.prom_sales;
   g_rec_out.prom_sales_cost                 := g_rec_in.prom_sales_cost;
   g_rec_out.prom_discount_no                := nvl(g_rec_in.prom_discount_no,0);
   g_rec_out.ho_prom_discount_amt            := g_rec_in.ho_prom_discount_amt;
   g_rec_out.ho_prom_discount_qty            := g_rec_in.ho_prom_discount_qty;
   g_rec_out.st_prom_discount_amt            := g_rec_in.st_prom_discount_amt;
   g_rec_out.st_prom_discount_qty            := g_rec_in.st_prom_discount_qty;
   g_rec_out.clear_sales_qty                 := g_rec_in.clear_sales_qty;
   g_rec_out.clear_sales                     := g_rec_in.clear_sales;
   g_rec_out.clear_sales_cost                := g_rec_in.clear_sales_cost;
   g_rec_out.waste_qty                       := g_rec_in.waste_qty;
   g_rec_out.waste_selling                   := g_rec_in.waste_selling;
   g_rec_out.waste_cost                      := g_rec_in.waste_cost;
   g_rec_out.shrink_qty                      := g_rec_in.shrink_qty;
   g_rec_out.shrink_selling                  := g_rec_in.shrink_selling;
   g_rec_out.shrink_cost                     := g_rec_in.shrink_cost;
   g_rec_out.gain_qty                        := g_rec_in.gain_qty;
   g_rec_out.gain_selling                    := g_rec_in.gain_selling;
   g_rec_out.gain_cost                       := g_rec_in.gain_cost;
   g_rec_out.grn_qty                         := g_rec_in.grn_qty;
   g_rec_out.grn_selling                     := g_rec_in.grn_selling;
   g_rec_out.grn_cost                        := g_rec_in.grn_cost;
   g_rec_out.claim_qty                       := g_rec_in.claim_qty;
   g_rec_out.claim_selling                   := g_rec_in.claim_selling;
   g_rec_out.claim_cost                      := g_rec_in.claim_cost;
   g_rec_out.self_supply_qty                 := g_rec_in.self_supply_qty;
   g_rec_out.self_supply_selling             := g_rec_in.self_supply_selling;
   g_rec_out.self_supply_cost                := g_rec_in.self_supply_cost;
   g_rec_out.wac_adj_amt                     := g_rec_in.wac_adj_amt;
   g_rec_out.invoice_adj_qty                 := g_rec_in.invoice_adj_qty;
   g_rec_out.invoice_adj_selling             := g_rec_in.invoice_adj_selling;
   g_rec_out.invoice_adj_cost                := g_rec_in.invoice_adj_cost;
   g_rec_out.rndm_mass_pos_var               := g_rec_in.rndm_mass_pos_var;
   g_rec_out.mkup_selling                    := g_rec_in.mkup_selling;
   g_rec_out.mkup_cancel_selling             := g_rec_in.mkup_cancel_selling;
   g_rec_out.mkdn_selling                    := g_rec_in.mkdn_selling;
   g_rec_out.mkdn_cancel_selling             := g_rec_in.mkdn_cancel_selling;
   g_rec_out.clear_mkdn_selling              := g_rec_in.clear_mkdn_selling;
   g_rec_out.rtv_qty                         := g_rec_in.rtv_qty;
   g_rec_out.rtv_selling                     := g_rec_in.rtv_selling;
   g_rec_out.rtv_cost                        := g_rec_in.rtv_cost;
   g_rec_out.sdn_out_qty                     := g_rec_in.sdn_out_qty;
   g_rec_out.sdn_out_selling                 := g_rec_in.sdn_out_selling;
   g_rec_out.sdn_out_cost                    := g_rec_in.sdn_out_cost;
   g_rec_out.ibt_in_qty                      := g_rec_in.ibt_in_qty;
   g_rec_out.ibt_in_selling                  := g_rec_in.ibt_in_selling;
   g_rec_out.ibt_in_cost                     := g_rec_in.ibt_in_cost;
   g_rec_out.ibt_out_qty                     := g_rec_in.ibt_out_qty;
   g_rec_out.ibt_out_selling                 := g_rec_in.ibt_out_selling;
   g_rec_out.ibt_out_cost                    := g_rec_in.ibt_out_cost;
   g_rec_out.last_updated_date               := g_date;

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

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

-**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into rtl_loc_item_dy_rms_sparse values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update rtl_loc_item_dy_rms_sparse
      set    SET           prom_sales_qty                  = a_tbl_update(i).prom_sales_qty,
              prom_sales                      = a_tbl_update(i).prom_sales,
              prom_sales_cost                 = a_tbl_update(i).prom_sales_cost,
              prom_sales_fr_cost              = a_tbl_update(i).prom_sales_fr_cost,
              prom_sales_margin               = a_tbl_update(i).prom_sales_margin,
              franchise_prom_sales            = a_tbl_update(i).franchise_prom_sales,
              franchise_prom_sales_margin     = a_tbl_update(i).franchise_prom_sales_margin,
              prom_discount_no                = a_tbl_update(i).prom_discount_no,
              ho_prom_discount_amt            = a_tbl_update(i).ho_prom_discount_amt,
              ho_prom_discount_qty            = a_tbl_update(i).ho_prom_discount_qty,
              st_prom_discount_amt            = a_tbl_update(i).st_prom_discount_amt,
              st_prom_discount_qty            = a_tbl_update(i).st_prom_discount_qty,
              clear_sales_qty                 = a_tbl_update(i).clear_sales_qty,
              clear_sales                     = a_tbl_update(i).clear_sales,
              clear_sales_cost                = a_tbl_update(i).clear_sales_cost,
              clear_sales_fr_cost             = a_tbl_update(i).clear_sales_fr_cost,
              clear_sales_margin              = a_tbl_update(i).clear_sales_margin,
              franchise_clear_sales           = a_tbl_update(i).franchise_clear_sales,
              franchise_clear_sales_margin    = a_tbl_update(i).franchise_clear_sales_margin,
              waste_qty                       = a_tbl_update(i).waste_qty,
              waste_selling                   = a_tbl_update(i).waste_selling,
              waste_cost                      = a_tbl_update(i).waste_cost,
              waste_fr_cost                   = a_tbl_update(i).waste_fr_cost,
              shrink_qty                      = a_tbl_update(i).shrink_qty,
              shrink_selling                  = a_tbl_update(i).shrink_selling,
              shrink_cost                     = a_tbl_update(i).shrink_cost,
              shrink_fr_cost                  = a_tbl_update(i).shrink_fr_cost,
              gain_qty                        = a_tbl_update(i).gain_qty,
              gain_selling                    = a_tbl_update(i).gain_selling,
              gain_cost                       = a_tbl_update(i).gain_cost,
              gain_fr_cost                    = a_tbl_update(i).gain_fr_cost,
              grn_qty                         = a_tbl_update(i).grn_qty,
              grn_cases                       = a_tbl_update(i).grn_cases,
              grn_selling                     = a_tbl_update(i).grn_selling,
              grn_cost                        = a_tbl_update(i).grn_cost,
              grn_fr_cost                     = a_tbl_update(i).grn_fr_cost,
              grn_margin                      = a_tbl_update(i).grn_margin,
              shrinkage_qty                   = a_tbl_update(i).shrinkage_qty,
              shrinkage_selling               = a_tbl_update(i).shrinkage_selling,
              shrinkage_cost                  = a_tbl_update(i).shrinkage_cost,
              shrinkage_fr_cost               = a_tbl_update(i).shrinkage_fr_cost,
              abs_shrinkage_qty               = a_tbl_update(i).abs_shrinkage_qty,
              abs_shrinkage_selling           = a_tbl_update(i).abs_shrinkage_selling,
              abs_shrinkage_cost              = a_tbl_update(i).abs_shrinkage_cost,
              abs_shrinkage_fr_cost           = a_tbl_update(i).abs_shrinkage_fr_cost,
              claim_qty                       = a_tbl_update(i).claim_qty,
              claim_selling                   = a_tbl_update(i).claim_selling,
              claim_cost                      = a_tbl_update(i).claim_cost,
              claim_fr_cost                   = a_tbl_update(i).claim_fr_cost,
              self_supply_qty                 = a_tbl_update(i).self_supply_qty,
              self_supply_selling             = a_tbl_update(i).self_supply_selling,
              self_supply_cost                = a_tbl_update(i).self_supply_cost,
              self_supply_fr_cost             = a_tbl_update(i).self_supply_fr_cost,
              wac_adj_amt                     = a_tbl_update(i).wac_adj_amt,
              invoice_adj_qty                 = a_tbl_update(i).invoice_adj_qty,
              invoice_adj_selling             = a_tbl_update(i).invoice_adj_selling,
              invoice_adj_cost                = a_tbl_update(i).invoice_adj_cost,
              rndm_mass_pos_var               = a_tbl_update(i).rndm_mass_pos_var,
              mkup_selling                    = a_tbl_update(i).mkup_selling,
              mkup_cancel_selling             = a_tbl_update(i).mkup_cancel_selling,
              mkdn_selling                    = a_tbl_update(i).mkdn_selling,
              mkdn_cancel_selling             = a_tbl_update(i).mkdn_cancel_selling,
              prom_mkdn_qty                   = a_tbl_update(i).prom_mkdn_qty,
              prom_mkdn_selling               = a_tbl_update(i).prom_mkdn_selling,
              clear_mkdn_selling              = a_tbl_update(i).clear_mkdn_selling,
              mkdn_sales_qty                  = a_tbl_update(i).mkdn_sales_qty,
              mkdn_sales                      = a_tbl_update(i).mkdn_sales,
              mkdn_sales_cost                 = a_tbl_update(i).mkdn_sales_cost,
              net_mkdn                        = a_tbl_update(i).net_mkdn,
              rtv_qty                         = a_tbl_update(i).rtv_qty,
              rtv_cases                       = a_tbl_update(i).rtv_cases,
              rtv_selling                     = a_tbl_update(i).rtv_selling,
              rtv_cost                        = a_tbl_update(i).rtv_cost,
              rtv_fr_cost                     = a_tbl_update(i).rtv_fr_cost,
              sdn_out_qty                     = a_tbl_update(i).sdn_out_qty,
              sdn_out_selling                 = a_tbl_update(i).sdn_out_selling,
              sdn_out_cost                    = a_tbl_update(i).sdn_out_cost,
              sdn_out_fr_cost                 = a_tbl_update(i).sdn_out_fr_cost,
              sdn_out_cases                   = a_tbl_update(i).sdn_out_cases,
              ibt_in_qty                      = a_tbl_update(i).ibt_in_qty,
              ibt_in_selling                  = a_tbl_update(i).ibt_in_selling,
              ibt_in_cost                     = a_tbl_update(i).ibt_in_cost,
              ibt_in_fr_cost                  = a_tbl_update(i).ibt_in_fr_cost,
              ibt_out_qty                     = a_tbl_update(i).ibt_out_qty,
              ibt_out_selling                 = a_tbl_update(i).ibt_out_selling,
              ibt_out_cost                    = a_tbl_update(i).ibt_out_cost,
              ibt_out_fr_cost                 = a_tbl_update(i).ibt_out_fr_cost,
              net_ibt_qty                     = a_tbl_update(i).net_ibt_qty,
              net_ibt_selling                 = a_tbl_update(i).net_ibt_selling,
              shrink_excl_some_dept_cost      = a_tbl_update(i).shrink_excl_some_dept_cost,
              gain_excl_some_dept_cost        = a_tbl_update(i).gain_excl_some_dept_cost,
              net_waste_qty                   = a_tbl_update(i).net_waste_qty,
--              dc_delivered_qty                = a_tbl_update(i).dc_delivered_qty,
--              dc_delivered_cases              = a_tbl_update(i).dc_delivered_cases,
--              dc_delivered_selling            = a_tbl_update(i).dc_delivered_selling,
--              dc_delivered_cost               = a_tbl_update(i).dc_delivered_cost,
              net_inv_adj_qty                 = a_tbl_update(i).net_inv_adj_qty,
              net_inv_adj_selling             = a_tbl_update(i).net_inv_adj_selling,
              net_inv_adj_cost                = a_tbl_update(i).net_inv_adj_cost,
              net_inv_adj_fr_cost             = a_tbl_update(i).net_inv_adj_fr_cost,
              last_updated_date               = a_tbl_update(i).last_updated_date
      where  sk1_item_no                      = a_tbl_update(i).sk1_item_no  
       and  sk1_location_no                      = a_tbl_update(i).sk1_location_no  
       and  post_date                      = a_tbl_update(i).post_date;
      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_loc_item_dy_rms_sparse
   where  sk1_item_no        = g_rec_out.sk1_item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;

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
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_loc_item_dy_rms_sparse STARTED AT '||
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
-- Get start and end dates for current season - only for C&H
--**************************************************************************************************
    select season_start_date, season_end_date
      into l_season_start_date, l_season_end_date
      from dim_calendar
     where calendar_date = g_date;

--**************************************************************************************************
    open c_first_sale_date;
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
    while a_item_input.count > 0
    loop
      for i in 1 .. a_item_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_item_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
    end loop;
    close c_first_sale_date;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;


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


END WH_PRF_CORP_112U_WLTEST;
