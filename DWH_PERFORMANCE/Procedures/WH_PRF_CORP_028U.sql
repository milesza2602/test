--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_028U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_028U" (p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Generate the location dimention  sk type 2 load program
--  Tables:      Input  - dim_location
--               Output - dim_location_hist
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--  25 Nov 2008 - defect 228 - Add sunday_store_trade_ind to DIM_LOCATION
--  30 Jan 2009 - defect 491 - Remove country_code and country_name from table
--  06 feb 2009  - defect 698 - To remove st_dist_cost_group_no from table.
---  26 AUGUST 2014 - QC5310 - S4S project 3 new columns
--                                 ST_S4S_SHAPE_OF_CHAIN_CODE	,ST_S4S_SHAPE_OF_CHAIN_DESC	,FICA_IND	
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
g_recs_read         integer       :=  0;
g_recs_inserted     integer       :=  0;
g_recs_updated      integer       :=  0;

g_rec_out           dim_location_hist%rowtype;

g_found             boolean;
g_insert_rec        boolean;
g_date              date          := trunc(sysdate);
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type  := 'WH_PRF_CORP_028U';
l_name              sys_dwh_log.log_name%type               := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type        := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type        := dwh_constants.vc_log_script_rtl_prf_md ;
l_procedure_name    sys_dwh_log.log_procedure_name%type     := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'GENERATE SK2 VERSION OF LOCATION MASTER EX RMS';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_dim_location is
   select *
   from   dim_location;

g_rec_in            c_dim_location%rowtype;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.location_name                   := g_rec_in.location_name;
   g_rec_out.sk1_fd_zone_group_zone_no       := g_rec_in.sk1_fd_zone_group_zone_no;
   g_rec_out.sk1_ch_zone_group_zone_no       := g_rec_in.sk1_ch_zone_group_zone_no;
   g_rec_out.sk1_district_no                 := g_rec_in.sk1_district_no;
   g_rec_out.district_no                     := g_rec_in.district_no;
   g_rec_out.district_name                   := g_rec_in.district_name;
   g_rec_out.sk1_region_no                   := g_rec_in.sk1_region_no;
   g_rec_out.region_no                       := g_rec_in.region_no;
   g_rec_out.region_name                     := g_rec_in.region_name;
   g_rec_out.sk1_area_no                     := g_rec_in.sk1_area_no;
   g_rec_out.area_no                         := g_rec_in.area_no;
   g_rec_out.area_name                       := g_rec_in.area_name;
   g_rec_out.sk1_chain_no                    := g_rec_in.sk1_chain_no;
   g_rec_out.chain_no                        := g_rec_in.chain_no;
   g_rec_out.chain_name                      := g_rec_in.chain_name;
   g_rec_out.sk1_company_no                  := g_rec_in.sk1_company_no;
   g_rec_out.company_no                      := g_rec_in.company_no;
   g_rec_out.company_name                    := g_rec_in.company_name;
   g_rec_out.loc_type                        := g_rec_in.loc_type;
   g_rec_out.address_line_1                  := g_rec_in.address_line_1;
   g_rec_out.address_line_2                  := g_rec_in.address_line_2;
   g_rec_out.city_name                       := g_rec_in.city_name;
   g_rec_out.county_code                     := g_rec_in.county_code;
   g_rec_out.province_state_code             := g_rec_in.province_state_code;
--   g_rec_out.country_code                    := g_rec_in.country_code;
--   g_rec_out.country_name                    := g_rec_in.country_name;
   g_rec_out.sk1_country_code                := g_rec_in.sk1_country_code;
   g_rec_out.postal_code                     := g_rec_in.postal_code;
   g_rec_out.changed_address_ind             := g_rec_in.changed_address_ind;
   g_rec_out.email_address                   := g_rec_in.email_address;
   g_rec_out.channel_no                      := g_rec_in.channel_no;
   g_rec_out.vat_region_no                   := g_rec_in.vat_region_no;
   g_rec_out.stock_holding_ind               := g_rec_in.stock_holding_ind;
   g_rec_out.forecastable_ind                := g_rec_in.forecastable_ind;
   g_rec_out.num_store_leadtime_days         := g_rec_in.num_store_leadtime_days;
   g_rec_out.currency_code                   := g_rec_in.currency_code;
   g_rec_out.st_short_name                   := g_rec_in.st_short_name;
   g_rec_out.st_abbrev_name                  := g_rec_in.st_abbrev_name;
   g_rec_out.st_scndry_name                  := g_rec_in.st_scndry_name;
   g_rec_out.st_fax_no                       := g_rec_in.st_fax_no;
   g_rec_out.st_phone_no                     := g_rec_in.st_phone_no;
   g_rec_out.st_manager_name                 := g_rec_in.st_manager_name;
   g_rec_out.st_franchise_owner_name         := g_rec_in.st_franchise_owner_name;
   g_rec_out.st_sister_store_no              := g_rec_in.st_sister_store_no;
   g_rec_out.st_vat_incl_rsp_ind             := g_rec_in.st_vat_incl_rsp_ind;
   g_rec_out.st_open_date                    := g_rec_in.st_open_date;
   g_rec_out.st_close_date                   := g_rec_in.st_close_date;
   g_rec_out.st_acquired_date                := g_rec_in.st_acquired_date;
   g_rec_out.st_remodeled_date               := g_rec_in.st_remodeled_date;
   g_rec_out.st_format_no                    := g_rec_in.st_format_no;
   g_rec_out.st_format_name                  := g_rec_in.st_format_name;
   g_rec_out.st_class_code                   := g_rec_in.st_class_code;
   g_rec_out.st_mall_name                    := g_rec_in.st_mall_name;
   g_rec_out.st_shop_centre_type             := g_rec_in.st_shop_centre_type;
   g_rec_out.st_num_total_square_feet        := g_rec_in.st_num_total_square_feet;
   g_rec_out.st_num_selling_square_feet      := g_rec_in.st_num_selling_square_feet;
   g_rec_out.st_linear_distance              := g_rec_in.st_linear_distance;
   g_rec_out.st_language_no                  := g_rec_in.st_language_no;
   g_rec_out.st_integrated_pos_ind           := g_rec_in.st_integrated_pos_ind;
   g_rec_out.st_orig_currency_code           := g_rec_in.st_orig_currency_code;
   g_rec_out.st_store_type                   := g_rec_in.st_store_type;
   g_rec_out.st_value_of_chain_clip_code     := g_rec_in.st_value_of_chain_clip_code;
   g_rec_out.st_ww_online_picking_ind        := g_rec_in.st_ww_online_picking_ind;
   g_rec_out.st_food_sell_store_ind          := g_rec_in.st_food_sell_store_ind;
   g_rec_out.st_ww_online_picking_rgn_code   := g_rec_in.st_ww_online_picking_rgn_code;
   g_rec_out.st_geo_territory_code           := g_rec_in.st_geo_territory_code;
   g_rec_out.st_generation_code              := g_rec_in.st_generation_code;
   g_rec_out.st_site_locality_code           := g_rec_in.st_site_locality_code;
   g_rec_out.st_selling_space_clip_code      := g_rec_in.st_selling_space_clip_code;
   g_rec_out.st_dun_bradstreet_id            := g_rec_in.st_dun_bradstreet_id;
   g_rec_out.st_dun_bradstreet_loc_id        := g_rec_in.st_dun_bradstreet_loc_id;
   g_rec_out.st_chbd_hanging_set_ind         := g_rec_in.st_chbd_hanging_set_ind;
   g_rec_out.st_chbd_rpl_rgn_leadtime_code   := g_rec_in.st_chbd_rpl_rgn_leadtime_code;
   g_rec_out.st_chbd_val_chain_clip_code     := g_rec_in.st_chbd_val_chain_clip_code;
   g_rec_out.st_fd_sell_space_clip_code      := g_rec_in.st_fd_sell_space_clip_code;
   g_rec_out.st_fd_store_format_code         := g_rec_in.st_fd_store_format_code;
   g_rec_out.st_fd_value_of_chain_clip_code  := g_rec_in.st_fd_value_of_chain_clip_code;
   g_rec_out.st_fd_units_sold_clip_code      := g_rec_in.st_fd_units_sold_clip_code;
   g_rec_out.st_fd_customer_type_clip_code   := g_rec_in.st_fd_customer_type_clip_code;
   g_rec_out.st_pos_type                     := g_rec_in.st_pos_type;
   g_rec_out.st_pos_tran_no_generated_code   := g_rec_in.st_pos_tran_no_generated_code;
   g_rec_out.st_shape_of_the_chain_code      := g_rec_in.st_shape_of_the_chain_code;
   g_rec_out.st_receiving_ind                := g_rec_in.st_receiving_ind;
--   g_rec_out.st_dist_cost_group_no           := g_rec_in.st_dist_cost_group_no;
   g_rec_out.st_default_wh_no                := g_rec_in.st_default_wh_no;
   g_rec_out.st_chbd_closest_wh_no           := g_rec_in.st_chbd_closest_wh_no;
   g_rec_out.st_prom_zone_no                 := g_rec_in.st_prom_zone_no;
   g_rec_out.st_prom_zone_desc               := g_rec_in.st_prom_zone_desc;
   g_rec_out.st_transfer_zone_no             := g_rec_in.st_transfer_zone_no;
   g_rec_out.st_transfer_zone_desc           := g_rec_in.st_transfer_zone_desc;
   g_rec_out.st_num_stop_order_days          := g_rec_in.st_num_stop_order_days;
   g_rec_out.st_num_start_order_days         := g_rec_in.st_num_start_order_days;
   g_rec_out.wh_discipline_type              := g_rec_in.wh_discipline_type;
   g_rec_out.wh_store_no                     := g_rec_in.wh_store_no;
   g_rec_out.wh_supply_chain_ind             := g_rec_in.wh_supply_chain_ind;
   g_rec_out.wh_primary_supply_chain_type    := g_rec_in.wh_primary_supply_chain_type;
   g_rec_out.wh_value_add_supplier_no        := g_rec_in.wh_value_add_supplier_no;
   g_rec_out.wh_fd_zone_group_no             := g_rec_in.wh_fd_zone_group_no;
   g_rec_out.wh_fd_zone_no                   := g_rec_in.wh_fd_zone_no;
   g_rec_out.wh_triceps_customer_code        := g_rec_in.wh_triceps_customer_code;
   g_rec_out.wh_primary_virtual_wh_no        := g_rec_in.wh_primary_virtual_wh_no;
   g_rec_out.wh_physical_wh_no               := g_rec_in.wh_physical_wh_no;
   g_rec_out.wh_redist_wh_ind                := g_rec_in.wh_redist_wh_ind;
   g_rec_out.wh_rpl_ind                      := g_rec_in.wh_rpl_ind;
   g_rec_out.wh_virtual_wh_rpl_wh_no         := g_rec_in.wh_virtual_wh_rpl_wh_no;
   g_rec_out.wh_virtual_wh_restricted_ind    := g_rec_in.wh_virtual_wh_restricted_ind;
   g_rec_out.wh_virtual_wh_protected_ind     := g_rec_in.wh_virtual_wh_protected_ind;
   g_rec_out.wh_invest_buy_wh_ind            := g_rec_in.wh_invest_buy_wh_ind;
   g_rec_out.wh_invst_buy_wh_auto_clear_ind  := g_rec_in.wh_invst_buy_wh_auto_clear_ind;
   g_rec_out.wh_virtual_wh_invst_buy_wh_no   := g_rec_in.wh_virtual_wh_invst_buy_wh_no;
   g_rec_out.wh_virtual_wh_tier_type         := g_rec_in.wh_virtual_wh_tier_type;
   g_rec_out.wh_break_pack_ind               := g_rec_in.wh_break_pack_ind;
   g_rec_out.wh_delivery_policy_code         := g_rec_in.wh_delivery_policy_code;
   g_rec_out.wh_rounding_seq_no              := g_rec_in.wh_rounding_seq_no;
   g_rec_out.wh_inv_repl_seq_no              := g_rec_in.wh_inv_repl_seq_no;
   g_rec_out.wh_flow_supply_chain_ind        := g_rec_in.wh_flow_supply_chain_ind;
   g_rec_out.wh_xd_supply_chain_ind          := g_rec_in.wh_xd_supply_chain_ind;
   g_rec_out.wh_hs_supply_chain_ind          := g_rec_in.wh_hs_supply_chain_ind;
   g_rec_out.wh_export_wh_ind                := g_rec_in.wh_export_wh_ind;
   g_rec_out.wh_import_wh_ind                := g_rec_in.wh_import_wh_ind;
   g_rec_out.wh_domestic_wh_ind              := g_rec_in.wh_domestic_wh_ind;
   g_rec_out.wh_rtv_wh_ind                   := g_rec_in.wh_rtv_wh_ind;
   g_rec_out.wh_org_hrchy_type               := g_rec_in.wh_org_hrchy_type;
   g_rec_out.wh_org_hrchy_value              := g_rec_in.wh_org_hrchy_value;
   g_rec_out.store_age_month_no              := g_rec_in.store_age_month_no;
   g_rec_out.store_age_clip                  := g_rec_in.store_age_clip;
   g_rec_out.store_age_clip_desc             := g_rec_in.store_age_clip_desc;
   g_rec_out.active_store_ind                := g_rec_in.active_store_ind;
   g_rec_out.ownership_no                    := g_rec_in.ownership_no;
   g_rec_out.ownership_name                  := g_rec_in.ownership_name;
   g_rec_out.wh_org_hrchy_type_desc          := g_rec_in.wh_org_hrchy_type_desc;
   g_rec_out.wh_org_hrchy_value_desc         := g_rec_in.wh_org_hrchy_value_desc;
   g_rec_out.fin_period_open_year_status     := g_rec_in.fin_period_open_year_status;
   g_rec_out.fin_period_open_yr_status_desc  := g_rec_in.fin_period_open_yr_status_desc;
   g_rec_out.store_pos_active_ind            := g_rec_in.store_pos_active_ind;
   g_rec_out.new_store_trading_mtg_ind       := g_rec_in.new_store_trading_mtg_ind;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sunday_store_trade_ind          := g_rec_in.sunday_store_trade_ind;
--qc5310
   g_rec_out.ST_S4S_SHAPE_OF_CHAIN_CODE              := g_rec_in.ST_S4S_SHAPE_OF_CHAIN_CODE;               
   g_rec_out.ST_S4S_SHAPE_OF_CHAIN_DESC              := g_rec_in.ST_S4S_SHAPE_OF_CHAIN_DESC;               
   g_rec_out.FICA_IND              := g_rec_in.FICA_IND;              
---qc5310

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_rec_out.location_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Write valid data out to the location master table
--**************************************************************************************************
procedure local_write_output as
begin
   dwh_valid.did_location_change
   (g_rec_out.location_no,g_rec_out.district_no,g_rec_out.region_no,g_rec_out.area_no,g_rec_out.chain_no,
    g_rec_out.company_no,g_insert_rec);

   if g_insert_rec then
      g_rec_out.sk2_location_no       := location_hierachy_seq.nextval;
      g_rec_out.sk2_active_from_date  := trunc(sysdate);
      g_rec_out.sk2_active_to_date    := dwh_constants.sk_to_date;
      insert into dim_location_hist values g_rec_out;
      g_recs_inserted                 := g_recs_inserted + sql%rowcount;
   else
      update dim_location_hist
      set    location_name                   = g_rec_out.location_name,
             sk1_fd_zone_group_zone_no       = g_rec_out.sk1_fd_zone_group_zone_no,
             sk1_ch_zone_group_zone_no       = g_rec_out.sk1_ch_zone_group_zone_no,
             sk1_district_no                 = g_rec_out.sk1_district_no,
             district_no                     = g_rec_out.district_no,
             district_name                   = g_rec_out.district_name,
             sk1_region_no                   = g_rec_out.sk1_region_no,
             region_no                       = g_rec_out.region_no,
             region_name                     = g_rec_out.region_name,
             sk1_area_no                     = g_rec_out.sk1_area_no,
             area_no                         = g_rec_out.area_no,
             area_name                       = g_rec_out.area_name,
             sk1_chain_no                    = g_rec_out.sk1_chain_no,
             chain_no                        = g_rec_out.chain_no,
             chain_name                      = g_rec_out.chain_name,
             sk1_company_no                  = g_rec_out.sk1_company_no,
             company_no                      = g_rec_out.company_no,
             company_name                    = g_rec_out.company_name,
             loc_type                        = g_rec_out.loc_type,
             address_line_1                  = g_rec_out.address_line_1,
             address_line_2                  = g_rec_out.address_line_2,
             city_name                       = g_rec_out.city_name,
             county_code                     = g_rec_out.county_code,
             province_state_code             = g_rec_out.province_state_code,
             sk1_country_code                = g_rec_out.sk1_country_code,
--             country_code                    = g_rec_out.country_code,
--             country_name                    = g_rec_out.country_name,
             postal_code                     = g_rec_out.postal_code,
             changed_address_ind             = g_rec_out.changed_address_ind,
             email_address                   = g_rec_out.email_address,
             channel_no                      = g_rec_out.channel_no,
             vat_region_no                   = g_rec_out.vat_region_no,
             stock_holding_ind               = g_rec_out.stock_holding_ind,
             forecastable_ind                = g_rec_out.forecastable_ind,
             num_store_leadtime_days         = g_rec_out.num_store_leadtime_days,
             currency_code                   = g_rec_out.currency_code,
             st_short_name                   = g_rec_out.st_short_name,
             st_abbrev_name                  = g_rec_out.st_abbrev_name,
             st_scndry_name                  = g_rec_out.st_scndry_name,
             st_fax_no                       = g_rec_out.st_fax_no,
             st_phone_no                     = g_rec_out.st_phone_no,
             st_manager_name                 = g_rec_out.st_manager_name,
             st_franchise_owner_name         = g_rec_out.st_franchise_owner_name,
             st_sister_store_no              = g_rec_out.st_sister_store_no,
             st_vat_incl_rsp_ind             = g_rec_out.st_vat_incl_rsp_ind,
             st_open_date                    = g_rec_out.st_open_date,
             st_close_date                   = g_rec_out.st_close_date,
             st_acquired_date                = g_rec_out.st_acquired_date,
             st_remodeled_date               = g_rec_out.st_remodeled_date,
             st_format_no                    = g_rec_out.st_format_no,
             st_format_name                  = g_rec_out.st_format_name,
             st_class_code                   = g_rec_out.st_class_code,
             st_mall_name                    = g_rec_out.st_mall_name,
             st_shop_centre_type             = g_rec_out.st_shop_centre_type,
             st_num_total_square_feet        = g_rec_out.st_num_total_square_feet,
             st_num_selling_square_feet      = g_rec_out.st_num_selling_square_feet,
             st_linear_distance              = g_rec_out.st_linear_distance,
             st_language_no                  = g_rec_out.st_language_no,
             st_integrated_pos_ind           = g_rec_out.st_integrated_pos_ind,
             st_orig_currency_code           = g_rec_out.st_orig_currency_code,
             st_store_type                   = g_rec_out.st_store_type,
             st_value_of_chain_clip_code     = g_rec_out.st_value_of_chain_clip_code,
             st_ww_online_picking_ind        = g_rec_out.st_ww_online_picking_ind,
             st_food_sell_store_ind          = g_rec_out.st_food_sell_store_ind,
             st_ww_online_picking_rgn_code   = g_rec_out.st_ww_online_picking_rgn_code,
             st_geo_territory_code           = g_rec_out.st_geo_territory_code,
             st_generation_code              = g_rec_out.st_generation_code,
             st_site_locality_code           = g_rec_out.st_site_locality_code,
             st_selling_space_clip_code      = g_rec_out.st_selling_space_clip_code,
             st_dun_bradstreet_id            = g_rec_out.st_dun_bradstreet_id,
             st_dun_bradstreet_loc_id        = g_rec_out.st_dun_bradstreet_loc_id,
             st_chbd_hanging_set_ind         = g_rec_out.st_chbd_hanging_set_ind,
             st_chbd_rpl_rgn_leadtime_code   = g_rec_out.st_chbd_rpl_rgn_leadtime_code,
             st_chbd_val_chain_clip_code     = g_rec_out.st_chbd_val_chain_clip_code,
             st_fd_sell_space_clip_code      = g_rec_out.st_fd_sell_space_clip_code,
             st_fd_store_format_code         = g_rec_out.st_fd_store_format_code,
             st_fd_value_of_chain_clip_code  = g_rec_out.st_fd_value_of_chain_clip_code,
             st_fd_units_sold_clip_code      = g_rec_out.st_fd_units_sold_clip_code,
             st_fd_customer_type_clip_code   = g_rec_out.st_fd_customer_type_clip_code,
             st_pos_type                     = g_rec_out.st_pos_type,
             st_pos_tran_no_generated_code   = g_rec_out.st_pos_tran_no_generated_code,
             st_shape_of_the_chain_code      = g_rec_out.st_shape_of_the_chain_code,
             st_receiving_ind                = g_rec_out.st_receiving_ind,
--             st_dist_cost_group_no           = g_rec_out.st_dist_cost_group_no,
             st_default_wh_no                = g_rec_out.st_default_wh_no,
             st_chbd_closest_wh_no           = g_rec_out.st_chbd_closest_wh_no,
             st_prom_zone_no                 = g_rec_out.st_prom_zone_no,
             st_prom_zone_desc               = g_rec_out.st_prom_zone_desc,
             st_transfer_zone_no             = g_rec_out.st_transfer_zone_no,
             st_transfer_zone_desc           = g_rec_out.st_transfer_zone_desc,
             st_num_stop_order_days          = g_rec_out.st_num_stop_order_days,
             st_num_start_order_days         = g_rec_out.st_num_start_order_days,
             wh_discipline_type              = g_rec_out.wh_discipline_type,
             wh_store_no                     = g_rec_out.wh_store_no,
             wh_supply_chain_ind             = g_rec_out.wh_supply_chain_ind,
             wh_primary_supply_chain_type    = g_rec_out.wh_primary_supply_chain_type,
             wh_value_add_supplier_no        = g_rec_out.wh_value_add_supplier_no,
             wh_fd_zone_group_no             = g_rec_out.wh_fd_zone_group_no,
             wh_fd_zone_no                   = g_rec_out.wh_fd_zone_no,
             wh_triceps_customer_code        = g_rec_out.wh_triceps_customer_code,
             wh_primary_virtual_wh_no        = g_rec_out.wh_primary_virtual_wh_no,
             wh_physical_wh_no               = g_rec_out.wh_physical_wh_no,
             wh_redist_wh_ind                = g_rec_out.wh_redist_wh_ind,
             wh_rpl_ind                      = g_rec_out.wh_rpl_ind,
             wh_virtual_wh_rpl_wh_no         = g_rec_out.wh_virtual_wh_rpl_wh_no,
             wh_virtual_wh_restricted_ind    = g_rec_out.wh_virtual_wh_restricted_ind,
             wh_virtual_wh_protected_ind     = g_rec_out.wh_virtual_wh_protected_ind,
             wh_invest_buy_wh_ind            = g_rec_out.wh_invest_buy_wh_ind,
             wh_invst_buy_wh_auto_clear_ind  = g_rec_out.wh_invst_buy_wh_auto_clear_ind,
             wh_virtual_wh_invst_buy_wh_no   = g_rec_out.wh_virtual_wh_invst_buy_wh_no,
             wh_virtual_wh_tier_type         = g_rec_out.wh_virtual_wh_tier_type,
             wh_break_pack_ind               = g_rec_out.wh_break_pack_ind,
             wh_delivery_policy_code         = g_rec_out.wh_delivery_policy_code,
             wh_rounding_seq_no              = g_rec_out.wh_rounding_seq_no,
             wh_inv_repl_seq_no              = g_rec_out.wh_inv_repl_seq_no,
             wh_flow_supply_chain_ind        = g_rec_out.wh_flow_supply_chain_ind,
             wh_xd_supply_chain_ind          = g_rec_out.wh_xd_supply_chain_ind,
             wh_hs_supply_chain_ind          = g_rec_out.wh_hs_supply_chain_ind,
             wh_export_wh_ind                = g_rec_out.wh_export_wh_ind,
             wh_import_wh_ind                = g_rec_out.wh_import_wh_ind,
             wh_domestic_wh_ind              = g_rec_out.wh_domestic_wh_ind,
             wh_rtv_wh_ind                   = g_rec_out.wh_rtv_wh_ind,
             wh_org_hrchy_type               = g_rec_out.wh_org_hrchy_type,
             wh_org_hrchy_value              = g_rec_out.wh_org_hrchy_value,
             store_age_month_no              = g_rec_out.store_age_month_no,
             store_age_clip                  = g_rec_out.store_age_clip,
             store_age_clip_desc             = g_rec_out.store_age_clip_desc,
             active_store_ind                = g_rec_out.active_store_ind,
             ownership_no                    = g_rec_out.ownership_no,
             ownership_name                  = g_rec_out.ownership_name,
             wh_org_hrchy_type_desc          = g_rec_out.wh_org_hrchy_type_desc,
             wh_org_hrchy_value_desc         = g_rec_out.wh_org_hrchy_value_desc,
             fin_period_open_year_status     = g_rec_out.fin_period_open_year_status,
             fin_period_open_yr_status_desc  = g_rec_out.fin_period_open_yr_status_desc,
             store_pos_active_ind            = g_rec_out.store_pos_active_ind,
             new_store_trading_mtg_ind       = g_rec_out.new_store_trading_mtg_ind,
             last_updated_date               = g_rec_out.last_updated_date,
             sunday_store_trade_ind          = g_rec_out.sunday_store_trade_ind,
             ST_S4S_SHAPE_OF_CHAIN_CODE     = g_rec_out.ST_S4S_SHAPE_OF_CHAIN_CODE,               
             ST_S4S_SHAPE_OF_CHAIN_DESC      = g_rec_out.ST_S4S_SHAPE_OF_CHAIN_DESC,              
             FICA_IND                        = g_rec_out.FICA_IND   
      where  location_no                     = g_rec_out.location_no and
             sk2_active_to_date              = dwh_constants.sk_to_date;

      g_recs_updated              := g_recs_updated + sql%rowcount;
   end if;

-- *************************************************************************************************
-- Update old versions of the same location with details not linked to SCD attributes
-- This avoids having different location names for history locations and will be done as
-- required by the business
-- NOT REQUIRED BUSINESS SHOULD SEE HISTORY AS IT WAS
--   update dim_location_hist
--   set    location_name            = g_rec_out.location_name,
--          date_last_updated    = g_rec_out.date_last_updated
--   where  location_no              = g_rec_out.location_no and
--          sk2_active_to_date   <> dwh_constants.sk_to_date;
-- *************************************************************************************************

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' '||g_rec_out.location_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_out.location_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF LOCATION MASTER SK2 VERSION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
    for v_dim_location in c_dim_location
    loop
      g_recs_read := g_recs_read + 1;

      if g_recs_read mod 10000 = 0 then
         l_text := dwh_constants.vc_log_records_processed||
         to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      g_rec_in := v_dim_location;
      local_address_variable;
      local_write_output;

    end loop;

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
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_out.location_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_prf_corp_028u;
