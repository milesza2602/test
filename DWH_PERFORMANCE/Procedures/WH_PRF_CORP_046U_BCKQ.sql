--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046U_BCKQ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046U_BCKQ" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Location Item fact table in the performance layer
--               with input ex RMS fnd_location_item table from foundation layer.
--  Tables:      Input  - fnd_location_item
--               Output - W6005682.RTL_LOCATION_ITEM_QS
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 Jan 2009 - Defect 491 - remove primary_country_code and replace with sk1_primary_country_code
--                             remove primary_supplier_no and replace with sk1_primary_supplier_no
--
--  09 May 2011 - (NJ) Defect 4282 - added a field called sale_alert_ind.
--  19 May 2011 - Defect 2981 - Add a new measure to be derived (min_shelf_life)
--                            - Add base measures min_shelf_life_tolerance & max_shelf_life_tolerance
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
g_recs_tol           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            W6005682.RTL_LOCATION_ITEM_QS%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_046BCK'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.RTL_LOCATION_ITEM_QS%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.RTL_LOCATION_ITEM_QS%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

g_from_loc_no       integer;
g_to_loc_no         integer;

cursor c_fnd_location_item is
   select fli.*,
          to_number(translate(decode(nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
          as    NEXT_WK_DELIV_PATTERN,
          to_number(translate(decode(nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
          as    THIS_WK_DELIV_PATTERN,
          di.sk1_item_no,
          dl.sk1_location_no,
          dc.sk1_country_code,  -- TD-491
          ds.sk1_supplier_no     -- TD-491
   from   fnd_location_item fli,
          dim_item di,
          dim_location dl,
          dim_country dc,
          dim_supplier ds
   where  fli.item_no                = di.item_no  and
          fli.location_no            = dl.location_no and
          fli.primary_country_code   = dc.country_code(+) and    -- TD-491
          fli.primary_supplier_no    = ds.supplier_no and        -- TD-491
          fli.last_updated_date      = g_date and
          fli.location_no        between g_from_loc_no and g_to_loc_no
          --fli.location_no        between p_from_loc_no and p_to_loc_no
--          and          di.group_no        = 3
          ;



-- For input bulk collect --
type stg_array is table of c_fnd_location_item%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_location_item%rowtype;

cursor c_fnd_shelf_life_tol is
   select li.sk1_item_no,
          li.sk1_location_no,
          nvl(di.min_shelf_life_tolerance,0) min_shelf_life_tolerance,
          nvl(di.max_shelf_life_tolerance,0) max_shelf_life_tolerance,
          case
          when   nvl(li.num_shelf_life_days,0) = 0 then 0
          when   di.min_shelf_life_tolerance is null then nvl(li.num_shelf_life_days,0)
          else   li.num_shelf_life_days - di.min_shelf_life_tolerance
          end as min_sl
   from   W6005682.RTL_LOCATION_ITEM_QS li,
          dim_item di
   where  li.sk1_item_no             = di.sk1_item_no  and
          di.business_unit_no        = 50 and
           (li.min_shelf_life_tolerance       <> nvl(di.min_shelf_life_tolerance,0) or
            li.max_shelf_life_tolerance       <> nvl(di.max_shelf_life_tolerance,0) or
            case
            when   nvl(li.num_shelf_life_days,0) = 0 then 0
            else   li.num_shelf_life_days - nvl(di.min_shelf_life_tolerance,0)  
            end    <> li.min_shelf_life or
            li.min_shelf_life_tolerance       is null or
            li.max_shelf_life_tolerance       is null or
            li.min_shelf_life                 is null) ;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.supply_chain_type               := g_rec_in.supply_chain_type;
   g_rec_out.next_wk_deliv_pattern_code      := g_rec_in.next_wk_deliv_pattern;
   g_rec_out.this_wk_deliv_pattern_code      := g_rec_in.this_wk_deliv_pattern;
   g_rec_out.this_wk_catalog_ind             := g_rec_in.this_wk_catalog_ind;
   g_rec_out.next_wk_catalog_ind             := g_rec_in.next_wk_catalog_ind;
   g_rec_out.num_shelf_life_days             := g_rec_in.num_shelf_life_days;
   g_rec_out.num_units_per_tray              := g_rec_in.num_units_per_tray;
   g_rec_out.direct_perc                     := g_rec_in.direct_perc;
   g_rec_out.model_stock                     := g_rec_in.model_stock;
   g_rec_out.this_wk_cross_dock_ind          := g_rec_in.this_wk_cross_dock_ind;
   g_rec_out.next_wk_cross_dock_ind          := g_rec_in.next_wk_cross_dock_ind;
   g_rec_out.this_wk_direct_supplier_no      := g_rec_in.this_wk_direct_supplier_no;
   g_rec_out.next_wk_direct_supplier_no      := g_rec_in.next_wk_direct_supplier_no;
   g_rec_out.unit_pick_ind                   := g_rec_in.unit_pick_ind;
   g_rec_out.store_order_calc_code           := g_rec_in.store_order_calc_code;
   g_rec_out.safety_stock_factor             := g_rec_in.safety_stock_factor;
   g_rec_out.min_order_qty                   := g_rec_in.min_order_qty;
   g_rec_out.profile_id                      := g_rec_in.profile_id;
   g_rec_out.sub_profile_id                  := g_rec_in.sub_profile_id;
   g_rec_out.reg_rsp                         := g_rec_in.reg_rsp;
   g_rec_out.selling_rsp                     := g_rec_in.selling_rsp;
   g_rec_out.selling_uom_code                := g_rec_in.selling_uom_code;
   g_rec_out.prom_rsp                        := g_rec_in.prom_rsp;
   g_rec_out.prom_selling_rsp                := g_rec_in.prom_selling_rsp;
   g_rec_out.prom_selling_uom_code           := g_rec_in.prom_selling_uom_code;
   g_rec_out.clearance_ind                   := g_rec_in.clearance_ind;
   g_rec_out.taxable_ind                     := g_rec_in.taxable_ind;
   g_rec_out.pos_item_desc                   := g_rec_in.pos_item_desc;
   g_rec_out.pos_short_desc                  := g_rec_in.pos_short_desc;
   g_rec_out.num_ti_pallet_tier_cases        := g_rec_in.num_ti_pallet_tier_cases;
   g_rec_out.num_hi_pallet_tier_cases        := g_rec_in.num_hi_pallet_tier_cases;
   g_rec_out.store_ord_mult_unit_type_code   := g_rec_in.store_ord_mult_unit_type_code;
   g_rec_out.loc_item_status_code            := g_rec_in.loc_item_status_code;
   g_rec_out.loc_item_stat_code_update_date  := g_rec_in.loc_item_stat_code_update_date;
   g_rec_out.avg_natural_daily_waste_perc    := g_rec_in.avg_natural_daily_waste_perc;
   g_rec_out.meas_of_each                    := g_rec_in.meas_of_each;
   g_rec_out.meas_of_price                   := g_rec_in.meas_of_price;
   g_rec_out.rsp_uom_code                    := g_rec_in.rsp_uom_code;
   g_rec_out.primary_variant_item_no         := g_rec_in.primary_variant_item_no;
   g_rec_out.primary_cost_pack_item_no       := g_rec_in.primary_cost_pack_item_no;
--   g_rec_out.primary_supplier_no             := g_rec_in.primary_supplier_no;     -- TD-491
   g_rec_out.sk1_primary_supplier_no         := g_rec_in.sk1_supplier_no        ;   -- TD-491
--   g_rec_out.primary_country_code            := g_rec_in.primary_country_code;    -- TD-491
   g_rec_out.sk1_primary_country_code        := g_rec_in.sk1_country_code;          -- TD-491
   g_rec_out.receive_as_pack_type            := g_rec_in.receive_as_pack_type;
   g_rec_out.source_method_loc_type          := g_rec_in.source_method_loc_type;
   g_rec_out.source_location_no              := g_rec_in.source_location_no;
   g_rec_out.wh_supply_chain_type_ind        := g_rec_in.wh_supply_chain_type_ind;
   g_rec_out.launch_date                     := g_rec_in.launch_date;
   g_rec_out.pos_qty_key_option_code         := g_rec_in.pos_qty_key_option_code;
   g_rec_out.pos_manual_price_entry_code     := g_rec_in.pos_manual_price_entry_code;
   g_rec_out.deposit_code                    := g_rec_in.deposit_code;
   g_rec_out.food_stamp_ind                  := g_rec_in.food_stamp_ind;
   g_rec_out.pos_wic_ind                     := g_rec_in.pos_wic_ind;
   g_rec_out.proportional_tare_perc          := g_rec_in.proportional_tare_perc;
   g_rec_out.fixed_tare_value                := g_rec_in.fixed_tare_value;
   g_rec_out.fixed_tare_uom_code             := g_rec_in.fixed_tare_uom_code;
   g_rec_out.pos_reward_eligible_ind         := g_rec_in.pos_reward_eligible_ind;
   g_rec_out.comparable_natl_brand_item_no   := g_rec_in.comparable_natl_brand_item_no;
   g_rec_out.return_policy_code              := g_rec_in.return_policy_code;
   g_rec_out.red_flag_alert_ind              := g_rec_in.red_flag_alert_ind;
   g_rec_out.pos_marketing_club_code         := g_rec_in.pos_marketing_club_code;
   g_rec_out.report_code                     := g_rec_in.report_code;
   g_rec_out.num_req_select_shelf_life_days  := g_rec_in.num_req_select_shelf_life_days;
   g_rec_out.num_req_rcpt_shelf_life_days    := g_rec_in.num_req_rcpt_shelf_life_days;
   g_rec_out.num_invst_buy_shelf_life_days   := g_rec_in.num_invst_buy_shelf_life_days;
   g_rec_out.rack_size_code                  := g_rec_in.rack_size_code;
   g_rec_out.full_pallet_item_reorder_ind    := g_rec_in.full_pallet_item_reorder_ind;
   g_rec_out.in_store_market_basket_code     := g_rec_in.in_store_market_basket_code;
   g_rec_out.storage_location_bin_id         := g_rec_in.storage_location_bin_id;
   g_rec_out.alt_storage_location_bin_id     := g_rec_in.alt_storage_location_bin_id;
   g_rec_out.store_reorder_ind               := g_rec_in.store_reorder_ind;
   g_rec_out.returnable_ind                  := g_rec_in.returnable_ind;
   g_rec_out.refundable_ind                  := g_rec_in.refundable_ind;
   g_rec_out.back_order_ind                  := g_rec_in.back_order_ind;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.min_shelf_life_tolerance        := 0;
   g_rec_out.max_shelf_life_tolerance        := 0;
   g_rec_out.min_shelf_life                  := 0;

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sale_alert_ind                  := g_rec_in.sale_alert_ind;    --Added


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into W6005682.RTL_LOCATION_ITEM_QS values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no;
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
       update W6005682.RTL_LOCATION_ITEM_QS
       set    supply_chain_type               = a_tbl_update(i).supply_chain_type,
              next_wk_deliv_pattern_code      = a_tbl_update(i).next_wk_deliv_pattern_code,
              this_wk_deliv_pattern_code      = a_tbl_update(i).this_wk_deliv_pattern_code,
              this_wk_catalog_ind             = a_tbl_update(i).this_wk_catalog_ind,
              next_wk_catalog_ind             = a_tbl_update(i).next_wk_catalog_ind,
              num_shelf_life_days             = a_tbl_update(i).num_shelf_life_days,
              num_units_per_tray              = a_tbl_update(i).num_units_per_tray,
              direct_perc                     = a_tbl_update(i).direct_perc,
              model_stock                     = a_tbl_update(i).model_stock,
              this_wk_cross_dock_ind          = a_tbl_update(i).this_wk_cross_dock_ind,
              next_wk_cross_dock_ind          = a_tbl_update(i).next_wk_cross_dock_ind,
              this_wk_direct_supplier_no      = a_tbl_update(i).this_wk_direct_supplier_no,
              next_wk_direct_supplier_no      = a_tbl_update(i).next_wk_direct_supplier_no,
              unit_pick_ind                   = a_tbl_update(i).unit_pick_ind,
              store_order_calc_code           = a_tbl_update(i).store_order_calc_code,
              safety_stock_factor             = a_tbl_update(i).safety_stock_factor,
              min_order_qty                   = a_tbl_update(i).min_order_qty,
              profile_id                      = a_tbl_update(i).profile_id,
              sub_profile_id                  = a_tbl_update(i).sub_profile_id,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              selling_rsp                     = a_tbl_update(i).selling_rsp,
              selling_uom_code                = a_tbl_update(i).selling_uom_code,
              prom_rsp                        = a_tbl_update(i).prom_rsp,
              prom_selling_rsp                = a_tbl_update(i).prom_selling_rsp,
              prom_selling_uom_code           = a_tbl_update(i).prom_selling_uom_code,
              clearance_ind                   = a_tbl_update(i).clearance_ind,
              taxable_ind                     = a_tbl_update(i).taxable_ind,
              pos_item_desc                   = a_tbl_update(i).pos_item_desc,
              pos_short_desc                  = a_tbl_update(i).pos_short_desc,
              num_ti_pallet_tier_cases        = a_tbl_update(i).num_ti_pallet_tier_cases,
              num_hi_pallet_tier_cases        = a_tbl_update(i).num_hi_pallet_tier_cases,
              store_ord_mult_unit_type_code   = a_tbl_update(i).store_ord_mult_unit_type_code,
              loc_item_status_code            = a_tbl_update(i).loc_item_status_code,
              loc_item_stat_code_update_date  = a_tbl_update(i).loc_item_stat_code_update_date,
              avg_natural_daily_waste_perc    = a_tbl_update(i).avg_natural_daily_waste_perc,
              meas_of_each                    = a_tbl_update(i).meas_of_each,
              meas_of_price                   = a_tbl_update(i).meas_of_price,
              rsp_uom_code                    = a_tbl_update(i).rsp_uom_code,
              primary_variant_item_no         = a_tbl_update(i).primary_variant_item_no,
              primary_cost_pack_item_no       = a_tbl_update(i).primary_cost_pack_item_no,
              sk1_primary_supplier_no         = a_tbl_update(i).sk1_primary_supplier_no,
              sk1_primary_country_code        = a_tbl_update(i).sk1_primary_country_code, -- TD-491
--              primary_country_code            = a_tbl_update(i).primary_country_code,   -- TD-491
              receive_as_pack_type            = a_tbl_update(i).receive_as_pack_type,
              source_method_loc_type          = a_tbl_update(i).source_method_loc_type,
              source_location_no              = a_tbl_update(i).source_location_no,
              wh_supply_chain_type_ind        = a_tbl_update(i).wh_supply_chain_type_ind,
              launch_date                     = a_tbl_update(i).launch_date,
              pos_qty_key_option_code         = a_tbl_update(i).pos_qty_key_option_code,
              pos_manual_price_entry_code     = a_tbl_update(i).pos_manual_price_entry_code,
              deposit_code                    = a_tbl_update(i).deposit_code,
              food_stamp_ind                  = a_tbl_update(i).food_stamp_ind,
              pos_wic_ind                     = a_tbl_update(i).pos_wic_ind,
              proportional_tare_perc          = a_tbl_update(i).proportional_tare_perc,
              fixed_tare_value                = a_tbl_update(i).fixed_tare_value,
              fixed_tare_uom_code             = a_tbl_update(i).fixed_tare_uom_code,
              pos_reward_eligible_ind         = a_tbl_update(i).pos_reward_eligible_ind,
              comparable_natl_brand_item_no   = a_tbl_update(i).comparable_natl_brand_item_no,
              return_policy_code              = a_tbl_update(i).return_policy_code,
              red_flag_alert_ind              = a_tbl_update(i).red_flag_alert_ind,
              pos_marketing_club_code         = a_tbl_update(i).pos_marketing_club_code,
              report_code                     = a_tbl_update(i).report_code,
              num_req_select_shelf_life_days  = a_tbl_update(i).num_req_select_shelf_life_days,
              num_req_rcpt_shelf_life_days    = a_tbl_update(i).num_req_rcpt_shelf_life_days,
              num_invst_buy_shelf_life_days   = a_tbl_update(i).num_invst_buy_shelf_life_days,
              rack_size_code                  = a_tbl_update(i).rack_size_code,
              full_pallet_item_reorder_ind    = a_tbl_update(i).full_pallet_item_reorder_ind,
              in_store_market_basket_code     = a_tbl_update(i).in_store_market_basket_code,
              storage_location_bin_id         = a_tbl_update(i).storage_location_bin_id,
              alt_storage_location_bin_id     = a_tbl_update(i).alt_storage_location_bin_id,
              store_reorder_ind               = a_tbl_update(i).store_reorder_ind,
              returnable_ind                  = a_tbl_update(i).returnable_ind,
              refundable_ind                  = a_tbl_update(i).refundable_ind,
              back_order_ind                  = a_tbl_update(i).back_order_ind,
              sale_alert_ind                  = a_tbl_update(i).sale_alert_ind,         --ADDED
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_item_no                     = a_tbl_update(i).sk1_item_no  and
              sk1_location_no                 = a_tbl_update(i).sk1_location_no ;
--              and
--              last_updated_date               < a_tbl_update(i).last_updated_date   -- use if rerun to avoid updating already updated records


       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_location_no;
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
   from   W6005682.RTL_LOCATION_ITEM_QS
   where  sk1_item_no      = g_rec_out.sk1_item_no  and
          sk1_location_no  = g_rec_out.sk1_location_no;

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
-- Update the Min &Max shelf life days ex dim_item
--**************************************************************************************************
procedure shelf_life_days_update as
begin


   for shelf_life_record in c_fnd_shelf_life_tol
   loop
     update W6005682.RTL_LOCATION_ITEM_QS
     set    min_shelf_life_tolerance        = shelf_life_record.min_shelf_life_tolerance,
            max_shelf_life_tolerance        = shelf_life_record.max_shelf_life_tolerance,
            min_shelf_life                  = shelf_life_record.min_sl,
            last_updated_date               = g_date
     where  sk1_item_no                     = shelf_life_record.sk1_item_no   and
            sk1_location_no                 = shelf_life_record.sk1_location_no ;

     g_recs_tol  := g_recs_tol  + sql%rowcount;
   end loop;
   exception
     when others then
       l_message := 'Update error min/max tolerance '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end shelf_life_days_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=200000000';
execute immediate 'alter session enable parallel dml';


    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF W6005682.RTL_LOCATION_ITEM_QS EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    if p_from_loc_no = 0 then
       g_from_loc_no := 0;
       g_to_loc_no := 500;
       --_to_loc_no := g_to_loc_no;
    end if;

    if p_from_loc_no = 351 then
       g_from_loc_no := 501;
       g_to_loc_no := 2000;
       --p_from_loc_no := g_from_loc_no;
       --p_to_loc_no := g_to_loc_no;
    end if;

    if p_from_loc_no = 491 then
       g_from_loc_no := 2001;
       g_to_loc_no := 10000;
       --p_from_loc_no := g_from_loc_no;
    end if;

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_location_item;
    fetch c_fnd_location_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_location_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_location_item;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;



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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;

    if p_to_loc_no > 1000 then
       shelf_life_days_update;
    end if;

    l_text :=  dwh_constants.vc_log_records_updated||'SL Days ex Item '||g_recs_tol;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    p_success := true;
    commit;
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
end wh_prf_corp_046u_bckq;
