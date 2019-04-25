--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_026U_01APR
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_026U_01APR" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Christie Koorts
--  Purpose:     Create location dimension table in the performance layer
--               with added value ex foundation layer location table.
--  Tables:      Input  -   fnd_location, dim_district,
--                          dim_zone, dim_country
--               Output -   dim_location
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--
--  Maintenance:
--  25 Nov 2008 - defect 228 - Add sunday_store_trade_ind to DIM_LOCATION
--                             and DIM_LOCATION_HIST
--               - defect 316 - Initial design of DIM_COUNTRY
--               - defect 318 - Removing COUNTRY_NAME from all Foundation tables
--  9 jAN 2009   - defect 278 - Added null check for reading DIM_ZONE for warehouses
--  30 Jan 2009  - defect 491 - To remove country_code and country_name from table.
--  06 feb 2009  - defect 698 - To remove st_dist_cost_group_no from table.
--  13 FEB 2009  - defect 688 - DIM_LOCATION fields with ETL comments in design
--                              but NULL values in ODWH UAT
--  04 Mar 2009 - defect 929 - NULL values on
--                             DIM_LOCATION.WH_ORG_HRCHY_TYPE_DESC.
--                             Therefore need to derive description
--  18 Mar 2009 - defect 1153 - provide values where null NULL values for
--                              columns :
--                              LOC_TYPE (5343);
--                              ST_OPEN_DATE (5370);
--                              ST_CLOSE_DATE (5371);
--                              ST_SHOP_CENTRE_TYPE (5378);
--                              ST_STORE_TYPE (5385);
--                              ST_VALUE_OF_CHAIN_CLIP_CODE (5386);
--                              ST_SITE_LOCALITY_CODE (5392);
--                              ST_SHAPE_OF_THE_CHAIN_CODE (5406);
--                              WH_DISCIPLINE_TYPE (5417).
--   23 March 2009 - defect 1202 - ACTIVE_STORE_IND to be set to zero
--                                  for Warehouses on DIM_LOCATION
---  23 March 2009 - defect 1203 - Update ST_CLOSE_DATE
--                                 to 31 December 2099  on DIM_LOCATION
---  14 April 2009 - defect 1321 - NULL values on
--                                 DIM_LOCATION.WH_ORG_HRCHY_TYPE_DESC
--                                 & WH_ORG_HRCHY_VALUE_DESC
---  26 AUGUST 2014 - QC5310 - S4S project 3 new columns
--                                 ST_S4S_SHAPE_OF_CHAIN_CODE	,ST_S4S_SHAPE_OF_CHAIN_DESC	,FICA_IND	
---  27 OCTOBER 2015         -  - DJ project 43 new columns
--                                adding    H1_MP_STORE_ALT_AREA_NO, SK1_H1_MP_STORE_ALT_AREA_NO, AXIMA_IND, REPLEN_STORE_IND 
--   13 november 2015 w lyttle 2015  - add check for nulls
--
--   10 Jul 2017 - add sk1_store_tier_no for DJ Tier Information
--   29 Aug 2017 - add new column (linked_location_no) for linked JV store (Food Services Reporting Project)
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_location%rowtype;
g_fd_zone_no         fnd_zone_store.zone_no%type;
g_ch_zone_no         fnd_zone_store.zone_no%type;
g_found              boolean;
g_date               date       ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_026U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_LOCATION EX FND_LOCATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_location%rowtype index by binary_integer;
type tbl_array_u is table of dim_location%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_location is
   select fl.*,
          dd.sk1_district_no,
          dd.district_name,
          dd.sk1_region_no,
          dd.region_no,
          dd.region_name,
          dd.sk1_area_no,
          dd.area_no,
          dd.area_name,
          dd.sk1_chain_no,
          dd.chain_no,
          dd.chain_name,
          dd.sk1_company_no,
          dd.company_no,
          dd.company_name,
          fl.location_no||' - '||fl.location_name location_long_desc,
          dd.district_long_desc,
          dd.region_long_desc,
          dd.area_long_desc,
          dd.chain_long_desc,
          dd.company_long_desc,
          dct.country_name cntry_name,
          dct.sk1_country_code,
          dc.chain_name as wh_org_hrchy_value_desc,
          CASE WHEN dd.chain_no in (40)                     THEN 1003   ELSE 9951             END H1_MP_STORE_ALT_AREA_NO, 
          CASE WHEN dd.chain_no in (40)                     THEN 15742  ELSE 246              END SK1_H1_MP_STORE_ALT_AREA_NO, 
          case when fl.LOCATION_NO IN (4250,4205,4260,4206) THEN 1      ELSE 0                end AXIMA_IND,
          case when fl.LOCATION_NO IN (4042)                THEN 1      ELSE 0                end REPLEN_STORE_IND,
          fl.store_tier_no sk1_store_tier_no
from fnd_location fl,
        dim_district dd,
        dim_country dct,
        dim_chain dc
   where fl.district_no  = dd.district_no and
         fl.country_code = dct.country_code and
         fl.wh_org_hrchy_value = dc.chain_no(+)
         ;

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_location%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_location%rowtype;
a_stg_input      stg_array;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.location_no                    := g_rec_in.location_no;
   g_rec_out.location_name                  := g_rec_in.location_name;
   g_rec_out.sk1_district_no                := g_rec_in.sk1_district_no;
   g_rec_out.district_no                    := g_rec_in.district_no;
   g_rec_out.district_name                  := g_rec_in.district_name;
   g_rec_out.sk1_region_no                  := g_rec_in.sk1_region_no;
   g_rec_out.region_no                      := g_rec_in.region_no;
   g_rec_out.region_name                    := g_rec_in.region_name;
   g_rec_out.sk1_area_no                    := g_rec_in.sk1_area_no;
   g_rec_out.area_no                        := g_rec_in.area_no;
   g_rec_out.area_name                      := g_rec_in.area_name;
   g_rec_out.sk1_chain_no                   := g_rec_in.sk1_chain_no;
   g_rec_out.chain_no                       := g_rec_in.chain_no;
   g_rec_out.chain_name                     := g_rec_in.chain_name;
   g_rec_out.sk1_company_no                 := g_rec_in.sk1_company_no;
   g_rec_out.company_no                     := g_rec_in.company_no;
   g_rec_out.company_name                   := g_rec_in.company_name;
   g_rec_out.loc_type                       := nvl(g_rec_in.loc_type,'-');
   g_rec_out.address_line_1                 := g_rec_in.address_line_1;
   g_rec_out.address_line_2                 := g_rec_in.address_line_2;
   g_rec_out.city_name                      := g_rec_in.city_name;
   g_rec_out.county_code                    := g_rec_in.county_code;
   g_rec_out.province_state_code            := g_rec_in.province_state_code;
--   g_rec_out.country_code                   := g_rec_in.country_code; -- TD-491 to remove column
   g_rec_out.sk1_country_code               := g_rec_in.sk1_country_code;
--   g_rec_out.country_name                   := g_rec_in.cntry_name; -- TD-491 to remove column
   g_rec_out.postal_code                    := g_rec_in.postal_code;
   g_rec_out.changed_address_ind            := g_rec_in.changed_address_ind;
   g_rec_out.email_address                  := g_rec_in.email_address;
   g_rec_out.channel_no                     := g_rec_in.channel_no;
   g_rec_out.vat_region_no                  := g_rec_in.vat_region_no;
   g_rec_out.stock_holding_ind              := g_rec_in.stock_holding_ind;
   g_rec_out.forecastable_ind               := g_rec_in.forecastable_ind;
   g_rec_out.num_store_leadtime_days        := g_rec_in.num_store_leadtime_days;
   g_rec_out.currency_code                  := g_rec_in.currency_code;
   g_rec_out.st_short_name                  := g_rec_in.st_short_name;
   g_rec_out.st_abbrev_name                 := g_rec_in.st_abbrev_name;
   g_rec_out.st_scndry_name                 := g_rec_in.st_scndry_name;
   g_rec_out.st_fax_no                      := g_rec_in.st_fax_no;
   g_rec_out.st_phone_no                    := g_rec_in.st_phone_no;
   g_rec_out.st_manager_name                := g_rec_in.st_manager_name;
   g_rec_out.st_franchise_owner_name        := g_rec_in.st_franchise_owner_name;
   g_rec_out.st_sister_store_no             := g_rec_in.st_sister_store_no;
   g_rec_out.st_vat_incl_rsp_ind            := g_rec_in.st_vat_incl_rsp_ind;
   g_rec_out.st_open_date                   := nvl(g_rec_in.st_open_date,to_date('1901-12-31','yyyy-mm-dd'));
   g_rec_out.st_close_date                  := nvl(g_rec_in.st_close_date,to_date('2099-12-31','yyyy-mm-dd'));
   g_rec_out.st_acquired_date               := g_rec_in.st_acquired_date;
   g_rec_out.st_remodeled_date              := g_rec_in.st_remodeled_date;
   g_rec_out.st_format_no                   := g_rec_in.st_format_no;
   g_rec_out.st_format_name                 := g_rec_in.st_format_name;
   g_rec_out.st_class_code                  := g_rec_in.st_class_code;
   g_rec_out.st_mall_name                   := g_rec_in.st_mall_name;
   g_rec_out.st_shop_centre_type            := nvl(g_rec_in.st_shop_centre_type,'-');
   g_rec_out.st_num_total_square_feet       := g_rec_in.st_num_total_square_feet;
   g_rec_out.st_num_selling_square_feet     := g_rec_in.st_num_selling_square_feet;
   g_rec_out.st_linear_distance             := g_rec_in.st_linear_distance;
   g_rec_out.st_language_no                 := g_rec_in.st_language_no;
   g_rec_out.st_integrated_pos_ind          := g_rec_in.st_integrated_pos_ind;
   g_rec_out.st_orig_currency_code          := g_rec_in.st_orig_currency_code;
   g_rec_out.st_store_type                  := nvl(g_rec_in.st_store_type,'-');
   g_rec_out.st_value_of_chain_clip_code    := nvl(g_rec_in.st_value_of_chain_clip_code,'-');
   g_rec_out.st_ww_online_picking_ind       := g_rec_in.st_ww_online_picking_ind;
   g_rec_out.st_food_sell_store_ind         := g_rec_in.st_food_sell_store_ind;
   g_rec_out.st_ww_online_picking_rgn_code  := g_rec_in.st_ww_online_picking_rgn_code;
   g_rec_out.st_geo_territory_code          := g_rec_in.st_geo_territory_code;
   g_rec_out.st_generation_code             := g_rec_in.st_generation_code;
   g_rec_out.st_site_locality_code          := nvl(g_rec_in.st_site_locality_code,'-');
   g_rec_out.st_selling_space_clip_code     := g_rec_in.st_selling_space_clip_code;
   g_rec_out.st_dun_bradstreet_id           := g_rec_in.st_dun_bradstreet_id;
   g_rec_out.st_dun_bradstreet_loc_id       := g_rec_in.st_dun_bradstreet_loc_id;
   g_rec_out.st_chbd_hanging_set_ind        := g_rec_in.st_chbd_hanging_set_ind;
   g_rec_out.st_chbd_rpl_rgn_leadtime_code  := g_rec_in.st_chbd_rpl_rgn_leadtime_code;
   g_rec_out.st_chbd_val_chain_clip_code    := g_rec_in.st_chbd_val_chain_clip_code;
   g_rec_out.st_fd_sell_space_clip_code     := g_rec_in.st_fd_sell_space_clip_code;
   g_rec_out.st_fd_store_format_code        := g_rec_in.st_fd_store_format_code;
   g_rec_out.st_fd_value_of_chain_clip_code := g_rec_in.st_fd_value_of_chain_clip_code;
   g_rec_out.st_fd_units_sold_clip_code     := g_rec_in.st_fd_units_sold_clip_code;
   g_rec_out.st_fd_customer_type_clip_code  := g_rec_in.st_fd_customer_type_clip_code;
   g_rec_out.st_pos_type                    := g_rec_in.st_pos_type;
   g_rec_out.st_pos_tran_no_generated_code  := g_rec_in.st_pos_tran_no_generated_code;
   g_rec_out.st_shape_of_the_chain_code     := nvl(g_rec_in.st_shape_of_the_chain_code,'-');
   g_rec_out.st_receiving_ind               := g_rec_in.st_receiving_ind;
--   g_rec_out.st_dist_cost_group_no          := g_rec_in.st_dist_cost_group_no;
   g_rec_out.st_default_wh_no               := g_rec_in.st_default_wh_no;
   g_rec_out.st_chbd_closest_wh_no          := g_rec_in.st_chbd_closest_wh_no;
   g_rec_out.st_prom_zone_no                := g_rec_in.st_prom_zone_no;
   g_rec_out.st_prom_zone_desc              := g_rec_in.st_prom_zone_desc;
   g_rec_out.st_transfer_zone_no            := g_rec_in.st_transfer_zone_no;
   g_rec_out.st_transfer_zone_desc          := g_rec_in.st_transfer_zone_desc;
   g_rec_out.st_num_stop_order_days         := g_rec_in.st_num_stop_order_days;
   g_rec_out.st_num_start_order_days        := g_rec_in.st_num_start_order_days;
   g_rec_out.wh_discipline_type             := nvl(g_rec_in.wh_discipline_type,'-');
   g_rec_out.wh_store_no                    := g_rec_in.wh_store_no;
   g_rec_out.wh_supply_chain_ind            := g_rec_in.wh_supply_chain_ind;
   g_rec_out.wh_primary_supply_chain_type   := g_rec_in.wh_primary_supply_chain_type;
   g_rec_out.wh_value_add_supplier_no       := g_rec_in.wh_value_add_supplier_no;
   g_rec_out.wh_fd_zone_group_no            := g_rec_in.wh_zone_group_no;
   g_rec_out.wh_fd_zone_no                  := g_rec_in.wh_zone_no;
   g_rec_out.wh_triceps_customer_code       := g_rec_in.wh_triceps_customer_code;
   g_rec_out.wh_primary_virtual_wh_no       := g_rec_in.wh_primary_virtual_wh_no;
   g_rec_out.wh_physical_wh_no              := g_rec_in.wh_physical_wh_no;
   g_rec_out.wh_redist_wh_ind               := g_rec_in.wh_redist_wh_ind;
   g_rec_out.wh_rpl_ind                     := g_rec_in.wh_rpl_ind;
   g_rec_out.wh_virtual_wh_rpl_wh_no        := g_rec_in.wh_virtual_wh_rpl_wh_no;
   g_rec_out.wh_virtual_wh_restricted_ind   := g_rec_in.wh_virtual_wh_restricted_ind;
   g_rec_out.wh_virtual_wh_protected_ind    := g_rec_in.wh_virtual_wh_protected_ind;
   g_rec_out.wh_invest_buy_wh_ind           := g_rec_in.wh_invest_buy_wh_ind;
   g_rec_out.wh_invst_buy_wh_auto_clear_ind := g_rec_in.wh_invst_buy_wh_auto_clear_ind;
   g_rec_out.wh_virtual_wh_invst_buy_wh_no  := g_rec_in.wh_virtual_wh_invst_buy_wh_no;
   g_rec_out.wh_virtual_wh_tier_type        := g_rec_in.wh_virtual_wh_tier_type;
   g_rec_out.wh_break_pack_ind              := g_rec_in.wh_break_pack_ind;
   g_rec_out.wh_delivery_policy_code        := g_rec_in.wh_delivery_policy_code;
   g_rec_out.wh_rounding_seq_no             := g_rec_in.wh_rounding_seq_no;
   g_rec_out.wh_inv_repl_seq_no             := g_rec_in.wh_inv_repl_seq_no;
   g_rec_out.wh_flow_supply_chain_ind       := g_rec_in.wh_flow_supply_chain_ind;
   g_rec_out.wh_xd_supply_chain_ind         := g_rec_in.wh_xd_supply_chain_ind;
   g_rec_out.wh_hs_supply_chain_ind         := g_rec_in.wh_hs_supply_chain_ind;
   g_rec_out.wh_export_wh_ind               := g_rec_in.wh_export_wh_ind;
   g_rec_out.wh_import_wh_ind               := g_rec_in.wh_import_wh_ind;
   g_rec_out.wh_domestic_wh_ind             := g_rec_in.wh_domestic_wh_ind;
   g_rec_out.wh_rtv_wh_ind                  := g_rec_in.wh_rtv_wh_ind;
   g_rec_out.wh_org_hrchy_type              := g_rec_in.wh_org_hrchy_type;
   g_rec_out.wh_org_hrchy_value             := g_rec_in.wh_org_hrchy_value;
   g_rec_out.store_size_format              := g_rec_in.store_size_format;               --ADDED
/* QC5061 - New MP Location columns */
   g_rec_out.store_size_cluster             := 0;
   g_rec_out.sk1_customer_class_code        := 0;
   g_rec_out.customer_class_long_desc       := '0 - Not Assigned';
   g_rec_out.customer_class_short_desc      := 'Not Assigned';
   g_rec_out.customer_classification        := 0;
   g_rec_out.sk1_store_cluster_code         := 0;
   g_rec_out.store_cluster_long_desc        := '0 - Not Assigned';
   g_rec_out.store_cluster_short_desc       := 'Not Assigned';   
   
---  27 OCTOBER 2015         -  - DJ project 43 new columns
   g_rec_out.H1_MP_STORE_ALT_AREA_NO             := g_rec_in.H1_MP_STORE_ALT_AREA_NO;
   g_rec_out.SK1_H1_MP_STORE_ALT_AREA_NO         := g_rec_in.SK1_H1_MP_STORE_ALT_AREA_NO;
   g_rec_out.AXIMA_IND                           := g_rec_in.AXIMA_IND;
   g_rec_out.REPLEN_STORE_IND                    := g_rec_in.REPLEN_STORE_IND;
---  27 OCTOBER 2015         -  - DJ project 43 new columns

--qc5310
   g_rec_out.ST_S4S_SHAPE_OF_CHAIN_CODE              := g_rec_in.ST_S4S_SHAPE_OF_CHAIN_CODE;               
   g_rec_out.ST_S4S_SHAPE_OF_CHAIN_DESC              := g_rec_in.ST_S4S_SHAPE_OF_CHAIN_DESC;               
   g_rec_out.FICA_IND              := g_rec_in.FICA_IND;              
---qc5310

-- ETL TBC
   case
   when g_rec_out.wh_org_hrchy_type = 1 then
        g_rec_out.wh_org_hrchy_type_desc        := 'Company';
   when g_rec_out.wh_org_hrchy_type = 10 then
        g_rec_out.wh_org_hrchy_type_desc        := 'Chain';
   when g_rec_out.wh_org_hrchy_type = 20 then
        g_rec_out.wh_org_hrchy_type_desc        := 'Area';
   when g_rec_out.wh_org_hrchy_type = 30 then
        g_rec_out.wh_org_hrchy_type_desc        := 'Region';
   when g_rec_out.wh_org_hrchy_type = 40 then
        g_rec_out.wh_org_hrchy_type_desc        := 'District';
   when g_rec_out.wh_org_hrchy_type = 50 then
        g_rec_out.wh_org_hrchy_type_desc        := 'Store';
   else g_rec_out.wh_org_hrchy_type_desc        := null;
   end case;

   if g_rec_out.st_close_date is not null then
      g_rec_out.store_age_month_no   := 9999;
      g_rec_out.store_age_clip       := 8;
      g_rec_out.store_age_clip_desc  := 'Closed / Converted';
   else
      if g_rec_out.st_open_date > g_date then
         g_rec_out.store_age_month_no := 0;
      else
         g_rec_out.store_age_month_no := round(((g_date - g_rec_out.st_open_date) / 30.44),0);  -- avrg no of days in 1 month
      end if;
      case
      when g_rec_out.store_age_month_no = 0 then
           g_rec_out.store_age_clip      := 0;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 1 and 3 then
           g_rec_out.store_age_clip      := 1;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 4 and 6 then
           g_rec_out.store_age_clip      := 2;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 7 and 9 then
           g_rec_out.store_age_clip      := 3;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 10 and 12 then
           g_rec_out.store_age_clip      := 4;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 13 and 24 then
           g_rec_out.store_age_clip      := 5;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no between 25 and 36 then
           g_rec_out.store_age_clip      := 6;
           g_rec_out.store_age_clip_desc := 'To Open';
      when g_rec_out.store_age_month_no > 36 then
           g_rec_out.store_age_clip      := 7;
           g_rec_out.store_age_clip_desc := 'To Open';
      else
           g_rec_out.store_age_clip      := 9;
           g_rec_out.store_age_clip_desc := 'Error/Unknown';
      end case;
   end if;

   g_rec_out.active_store_ind               := 0;
   if g_rec_out.loc_type = 'S' then
      if g_rec_out.st_close_date is null or
         g_rec_out.st_close_date >= g_date then
         g_rec_out.active_store_ind         := 1;
      end if;
   end if;
--17 MAR 2018 FIX TO SET ACTIVE STORE IND = 0 WHERE TRIAL STORES WERE SET UP - SCEWS MISSING STORE COUNTS IN SMS PROGRAMS---
-- MUST BE RESET ON MONDAY 19TH MAR 2018 AFTER FIX AT SOURCE TO CLOSE DATE
/*
IF g_rec_out.location_no IN (
3578,
3566,
3594,
3599,
3567,
3568,
3569,
3570,
3571,
3572,
3573,
3574,
3575,
3576,
3577,
3579,
3580,
3581,
3582,
3583,
3584,
3586,
3587,
3588,
3589,
3590,
3591,
3592,
3593)
THEN
g_rec_out.active_store_ind         := 0;
END IF;   
*/

--FIX CODE ABOVE-----------------------------------------------------------------------------------------------------   
   
--   if g_rec_out.loc_type = 'W' then
--         g_rec_out.active_store_ind         := null;
--   end if;

--   g_rec_out.ownership_no                   := g_rec_in.ownership_no;
--   g_rec_out.ownership_name                 := g_rec_in.ownership_name;
--   g_rec_out.wh_org_hrchy_type_desc         := g_rec_in.wh_org_hrchy_type_desc;
   g_rec_out.wh_org_hrchy_value_desc        := g_rec_in.wh_org_hrchy_value_desc;
--   g_rec_out.fin_period_open_year_status    := g_rec_in.fin_period_open_year_status;
--   g_rec_out.fin_period_open_yr_status_desc := g_rec_in.fin_period_open_yr_status_desc;
--   g_rec_out.store_pos_active_ind           := g_rec_in.store_pos_active_ind;
--   g_rec_out.new_store_trading_mtg_ind      := g_rec_in.new_store_trading_mtg_ind;
   g_rec_out.location_long_desc             := g_rec_in.location_long_desc;
   g_rec_out.district_long_desc             := g_rec_in.district_long_desc;
   g_rec_out.region_long_desc               := g_rec_in.region_long_desc;
   g_rec_out.area_long_desc                 := g_rec_in.area_long_desc;
   g_rec_out.chain_long_desc                := g_rec_in.chain_long_desc;
   g_rec_out.company_long_desc              := g_rec_in.company_long_desc;
   g_rec_out.total                          := 'TOTAL';
   g_rec_out.total_desc                     := 'ALL LOCATION';
   g_rec_out.last_updated_date              := g_date;
   g_rec_out.sunday_store_trade_ind         := g_rec_in.sunday_store_trade_ind;
   g_rec_out.store_cluster                  := upper(g_rec_in.store_cluster)  ;
   if g_rec_out.store_cluster is null then
      g_rec_out.store_cluster := 'NO CLUSTER';
   end if;
   g_rec_out.sk1_ch_zone_group_zone_no      := 0;
   g_rec_out.sk1_fd_zone_group_zone_no      := 0;
   g_rec_out.linked_location_no             := g_rec_in.linked_location_no;


/*
   if g_rec_out.loc_type = 'W'
   and g_rec_out.wh_fd_zone_no is not null
   then
      select sk1_zone_group_zone_no
      into   g_rec_out.sk1_fd_zone_group_zone_no
      from   dim_zone
      where  zone_group_no = 1 and
             zone_no       = g_rec_out.wh_fd_zone_no;
   end if;
*/
-- NB!!   Note: (A de Wet Dec 2008)
-- Zone number should have been supplied on the 'fnd_location' foundation load
--from RMS but RMS cannot do this.!
-- Therefore no ref integrity check could be done on the load of locations
--in the FND layer as would be normal!!
-- We thus have to do this lookup and make the assumption that the zone will
--always be on the fnd_zone_store table .
-- We have checked with RMS and they have confirmed that there will always be
--a zone no for ZG 1 and 2. (Lee, Lia)
-- We thus will allow the program to fail this build if any of the selects
--below return zero rows as this indicates major
-- relational integrity issues which will need to be resolved before continuing.
--A default value on 'no data found'
-- could be considered but that default will need to exist on dim_zone!!
--Else OLAP will crash)
   if g_rec_out.wh_store_no is null then
      g_rec_out.wh_store_no := 473;
   end if;
   
   if g_rec_out.loc_type = 'W' then
      select zone_no
      into   g_fd_zone_no
      from   fnd_zone_store
      where  zone_group_no = 1 and
             location_no   = g_rec_out.wh_store_no;
             
      select sk1_zone_group_zone_no
      into   g_rec_out.sk1_fd_zone_group_zone_no
      from   dim_zone
      where  zone_group_no = 1 and
             zone_no       = g_fd_zone_no;
      
      -- ADDED 11 SEPTEMBER 2015       
      select sk1_location_no 
        into g_rec_out.sk1_loc_no_wh_physical_wh_no
        from dim_location
       where location_no = g_rec_out.wh_physical_wh_no;
             
   end if;
   
   if g_rec_out.loc_type = 'S' then
     begin
      select zone_no
      into   g_fd_zone_no
      from   fnd_zone_store
      where  zone_group_no = 1 and
             location_no   = g_rec_out.location_no;

--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,'1');
      
      exception
        when no_data_found then
          dbms_output.put_line('g_rec_out.location_no = '||g_rec_out.location_no );
      end;

      select zone_no
      into   g_ch_zone_no
      from   fnd_zone_store
      where  zone_group_no = 2 and
             location_no   = g_rec_out.location_no;
             
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,'2');

      select sk1_zone_group_zone_no
      into   g_rec_out.sk1_fd_zone_group_zone_no
      from   dim_zone
      where  zone_group_no = 1 and
             zone_no       = g_fd_zone_no;
             
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,'3');

      select sk1_zone_group_zone_no
      into   g_rec_out.sk1_ch_zone_group_zone_no
      from   dim_zone
      where  zone_group_no = 2 and
             zone_no       = g_ch_zone_no;
             
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,'4');

      g_rec_out.sk1_loc_no_wh_physical_wh_no := '';
   end if;
   
   g_rec_out.wh_fd_zone_group_no            := 1;
   g_rec_out.wh_fd_zone_no                  := g_fd_zone_no;

   if g_rec_out.loc_type = 'W' and
      g_rec_out.wh_discipline_type = '-1650
      ' then
      g_rec_out.wh_fd_zone_group_no            := 0;
      g_rec_out.wh_fd_zone_no                  := 0;
      g_rec_out.sk1_fd_zone_group_zone_no      := 0;
   end if;
   
   -- DEFAULT TAX PERC, CATCH-ALL FOR WHEN NO OTHER VAT / GST RATE IS FOUND.
   if g_rec_out.vat_region_no = 1000 then
      g_rec_out.default_tax_region_no_perc := '14';
   else
      g_rec_out.default_tax_region_no_perc := '10';
   end if;
   
   g_rec_out.SK1_STORE_TIER_NO              := g_rec_in.SK1_STORE_TIER_NO;

if G_REC_OUT.WH_STORE_NO is null
or  G_FD_ZONE_NO is null
or G_REC_OUT.WH_FD_ZONE_NO is null
or G_REC_OUT.SK1_FD_ZONE_GROUP_ZONE_NO is null
or G_CH_ZONE_NO is null
or G_REC_OUT.SK1_FD_ZONE_GROUP_ZONE_NO is null
or G_REC_OUT.SK1_CH_ZONE_GROUP_ZONE_NO is null
then 
       L_text := 'value is null='||
       G_REC_OUT.WH_STORE_NO
||'*'|| G_FD_ZONE_NO
||'*'|| G_REC_OUT.WH_FD_ZONE_NO
||'*'|| G_REC_OUT.SK1_FD_ZONE_GROUP_ZONE_NO
||'*'||G_CH_ZONE_NO
||'*'|| G_REC_OUT.SK1_FD_ZONE_GROUP_ZONE_NO
||'*'||G_REC_OUT.SK1_CH_ZONE_GROUP_ZONE_NO
       ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end if;

   exception
      when others then
--      dbms_output.put_line('*** local_address_variable***');
--      dbms_output.put_line('g_rec_out.location_no='||g_rec_out.location_no||' g_rec_out.loc_type='||g_rec_out.loc_type);
--      dbms_output.put_line('g_rec_out.wh_store_no='||g_rec_out.wh_store_no||' g_fd_zone_no='||g_fd_zone_no);
--      dbms_output.put_line('g_rec_out.wh_fd_zone_no='||g_rec_out.wh_fd_zone_no||' g_rec_out.sk1_fd_zone_group_zone_no='||g_rec_out.sk1_fd_zone_group_zone_no);
--       dbms_output.put_line('g_fd_zone_no='||g_fd_zone_no);
--        dbms_output.put_line('g_ch_zone_no='||g_ch_zone_no);
--       dbms_output.put_line('g_rec_out.sk1_fd_zone_group_zone_no='||g_rec_out.sk1_fd_zone_group_zone_no);
--       dbms_output.put_line('g_rec_out.sk1_ch_zone_group_zone_no='||g_rec_out.sk1_ch_zone_group_zone_no);
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dim_location values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).location_no;
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
      update dim_location
      set    location_name                  = a_tbl_update(i).location_name,
             sk1_fd_zone_group_zone_no      = a_tbl_update(i).sk1_fd_zone_group_zone_no,
             sk1_ch_zone_group_zone_no      = a_tbl_update(i).sk1_ch_zone_group_zone_no,
             sk1_district_no                = a_tbl_update(i).sk1_district_no,
             district_no                    = a_tbl_update(i).district_no,
             district_name                  = a_tbl_update(i).district_name,
             sk1_region_no                  = a_tbl_update(i).sk1_region_no,
             region_no                      = a_tbl_update(i).region_no,
             region_name                    = a_tbl_update(i).region_name,
             sk1_area_no                    = a_tbl_update(i).sk1_area_no,
             area_no                        = a_tbl_update(i).area_no,
             area_name                      = a_tbl_update(i).area_name,
             sk1_chain_no                   = a_tbl_update(i).sk1_chain_no,
             chain_no                       = a_tbl_update(i).chain_no,
             chain_name                     = a_tbl_update(i).chain_name,
             sk1_company_no                 = a_tbl_update(i).sk1_company_no,
             company_no                     = a_tbl_update(i).company_no,
             company_name                   = a_tbl_update(i).company_name,
             loc_type                       = a_tbl_update(i).loc_type,
             address_line_1                 = a_tbl_update(i).address_line_1,
             address_line_2                 = a_tbl_update(i).address_line_2,
             city_name                      = a_tbl_update(i).city_name,
             county_code                    = a_tbl_update(i).county_code,
             province_state_code            = a_tbl_update(i).province_state_code,
             sk1_country_code               = a_tbl_update(i).sk1_country_code,
--             country_code                   = a_tbl_update(i).country_code, -- TD-491 to remove column
--             country_name                   = a_tbl_update(i).country_name, -- TD-491 to remove column
             postal_code                    = a_tbl_update(i).postal_code,
             changed_address_ind            = a_tbl_update(i).changed_address_ind,
             email_address                  = a_tbl_update(i).email_address,
             channel_no                     = a_tbl_update(i).channel_no,
             vat_region_no                  = a_tbl_update(i).vat_region_no,
             stock_holding_ind              = a_tbl_update(i).stock_holding_ind,
             forecastable_ind               = a_tbl_update(i).forecastable_ind,
             num_store_leadtime_days        = a_tbl_update(i).num_store_leadtime_days,
             currency_code                  = a_tbl_update(i).currency_code,
             st_short_name                  = a_tbl_update(i).st_short_name,
             st_abbrev_name                 = a_tbl_update(i).st_abbrev_name,
             st_scndry_name                 = a_tbl_update(i).st_scndry_name,
             st_fax_no                      = a_tbl_update(i).st_fax_no,
             st_phone_no                    = a_tbl_update(i).st_phone_no,
             st_manager_name                = a_tbl_update(i).st_manager_name,
             st_franchise_owner_name        = a_tbl_update(i).st_franchise_owner_name,
             st_sister_store_no             = a_tbl_update(i).st_sister_store_no,
             st_vat_incl_rsp_ind            = a_tbl_update(i).st_vat_incl_rsp_ind,
             st_open_date                   = a_tbl_update(i).st_open_date,
             st_close_date                  = a_tbl_update(i).st_close_date,
             st_acquired_date               = a_tbl_update(i).st_acquired_date,
             st_remodeled_date              = a_tbl_update(i).st_remodeled_date,
             st_format_no                   = a_tbl_update(i).st_format_no,
             st_format_name                 = a_tbl_update(i).st_format_name,
             st_class_code                  = a_tbl_update(i).st_class_code,
             st_mall_name                   = a_tbl_update(i).st_mall_name,
             st_shop_centre_type            = a_tbl_update(i).st_shop_centre_type,
             st_num_total_square_feet       = a_tbl_update(i).st_num_total_square_feet,
             st_num_selling_square_feet     = a_tbl_update(i).st_num_selling_square_feet,
             st_linear_distance             = a_tbl_update(i).st_linear_distance,
             st_language_no                 = a_tbl_update(i).st_language_no,
             st_integrated_pos_ind          = a_tbl_update(i).st_integrated_pos_ind,
             st_orig_currency_code          = a_tbl_update(i).st_orig_currency_code,
             st_store_type                  = a_tbl_update(i).st_store_type,
             st_value_of_chain_clip_code    = a_tbl_update(i).st_value_of_chain_clip_code,
             st_ww_online_picking_ind       = a_tbl_update(i).st_ww_online_picking_ind,
             st_food_sell_store_ind         = a_tbl_update(i).st_food_sell_store_ind,
             st_ww_online_picking_rgn_code  = a_tbl_update(i).st_ww_online_picking_rgn_code,
             st_geo_territory_code          = a_tbl_update(i).st_geo_territory_code,
             st_generation_code             = a_tbl_update(i).st_generation_code,
             st_site_locality_code          = a_tbl_update(i).st_site_locality_code,
             st_selling_space_clip_code     = a_tbl_update(i).st_selling_space_clip_code,
             st_dun_bradstreet_id           = a_tbl_update(i).st_dun_bradstreet_id,
             st_dun_bradstreet_loc_id       = a_tbl_update(i).st_dun_bradstreet_loc_id,
             st_chbd_hanging_set_ind        = a_tbl_update(i).st_chbd_hanging_set_ind,
             st_chbd_rpl_rgn_leadtime_code  = a_tbl_update(i).st_chbd_rpl_rgn_leadtime_code,
             st_chbd_val_chain_clip_code    = a_tbl_update(i).st_chbd_val_chain_clip_code,
             st_fd_sell_space_clip_code     = a_tbl_update(i).st_fd_sell_space_clip_code,
             st_fd_store_format_code        = a_tbl_update(i).st_fd_store_format_code,
             st_fd_value_of_chain_clip_code = a_tbl_update(i).st_fd_value_of_chain_clip_code,
             st_fd_units_sold_clip_code     = a_tbl_update(i).st_fd_units_sold_clip_code,
             st_fd_customer_type_clip_code  = a_tbl_update(i).st_fd_customer_type_clip_code,
             st_pos_type                    = a_tbl_update(i).st_pos_type,
             st_pos_tran_no_generated_code  = a_tbl_update(i).st_pos_tran_no_generated_code,
             st_shape_of_the_chain_code     = a_tbl_update(i).st_shape_of_the_chain_code,
             st_receiving_ind               = a_tbl_update(i).st_receiving_ind,
--             st_dist_cost_group_no          = a_tbl_update(i).st_dist_cost_group_no,
             st_default_wh_no               = a_tbl_update(i).st_default_wh_no,
             st_chbd_closest_wh_no          = a_tbl_update(i).st_chbd_closest_wh_no,
             st_prom_zone_no                = a_tbl_update(i).st_prom_zone_no,
             st_prom_zone_desc              = a_tbl_update(i).st_prom_zone_desc,
             st_transfer_zone_no            = a_tbl_update(i).st_transfer_zone_no,
             st_transfer_zone_desc          = a_tbl_update(i).st_transfer_zone_desc,
             st_num_stop_order_days         = a_tbl_update(i).st_num_stop_order_days,
             st_num_start_order_days        = a_tbl_update(i).st_num_start_order_days,
             wh_discipline_type             = a_tbl_update(i).wh_discipline_type,
             wh_store_no                    = a_tbl_update(i).wh_store_no,
             wh_supply_chain_ind            = a_tbl_update(i).wh_supply_chain_ind,
             wh_primary_supply_chain_type   = a_tbl_update(i).wh_primary_supply_chain_type,
             wh_value_add_supplier_no       = a_tbl_update(i).wh_value_add_supplier_no,
             wh_fd_zone_group_no            = a_tbl_update(i).wh_fd_zone_group_no,
             wh_fd_zone_no                  = a_tbl_update(i).wh_fd_zone_no,
             wh_triceps_customer_code       = a_tbl_update(i).wh_triceps_customer_code,
             wh_primary_virtual_wh_no       = a_tbl_update(i).wh_primary_virtual_wh_no,
             wh_physical_wh_no              = a_tbl_update(i).wh_physical_wh_no,
             wh_redist_wh_ind               = a_tbl_update(i).wh_redist_wh_ind,
             wh_rpl_ind                     = a_tbl_update(i).wh_rpl_ind,
             wh_virtual_wh_rpl_wh_no        = a_tbl_update(i).wh_virtual_wh_rpl_wh_no,
             wh_virtual_wh_restricted_ind   = a_tbl_update(i).wh_virtual_wh_restricted_ind,
             wh_virtual_wh_protected_ind    = a_tbl_update(i).wh_virtual_wh_protected_ind,
             wh_invest_buy_wh_ind           = a_tbl_update(i).wh_invest_buy_wh_ind,
             wh_invst_buy_wh_auto_clear_ind = a_tbl_update(i).wh_invst_buy_wh_auto_clear_ind,
             wh_virtual_wh_invst_buy_wh_no  = a_tbl_update(i).wh_virtual_wh_invst_buy_wh_no,
             wh_virtual_wh_tier_type        = a_tbl_update(i).wh_virtual_wh_tier_type,
             wh_break_pack_ind              = a_tbl_update(i).wh_break_pack_ind,
             wh_delivery_policy_code        = a_tbl_update(i).wh_delivery_policy_code,
             wh_rounding_seq_no             = a_tbl_update(i).wh_rounding_seq_no,
             wh_inv_repl_seq_no             = a_tbl_update(i).wh_inv_repl_seq_no,
             wh_flow_supply_chain_ind       = a_tbl_update(i).wh_flow_supply_chain_ind,
             wh_xd_supply_chain_ind         = a_tbl_update(i).wh_xd_supply_chain_ind,
             wh_hs_supply_chain_ind         = a_tbl_update(i).wh_hs_supply_chain_ind,
             wh_export_wh_ind               = a_tbl_update(i).wh_export_wh_ind,
             wh_import_wh_ind               = a_tbl_update(i).wh_import_wh_ind,
             wh_domestic_wh_ind             = a_tbl_update(i).wh_domestic_wh_ind,
             wh_rtv_wh_ind                  = a_tbl_update(i).wh_rtv_wh_ind,
             wh_org_hrchy_type              = a_tbl_update(i).wh_org_hrchy_type,
             wh_org_hrchy_value             = a_tbl_update(i).wh_org_hrchy_value,
             store_age_month_no             = a_tbl_update(i).store_age_month_no,
             store_age_clip                 = a_tbl_update(i).store_age_clip,
             store_age_clip_desc            = a_tbl_update(i).store_age_clip_desc,
             active_store_ind               = a_tbl_update(i).active_store_ind,
             ownership_no                   = a_tbl_update(i).ownership_no,
             ownership_name                 = a_tbl_update(i).ownership_name,
             wh_org_hrchy_type_desc         = a_tbl_update(i).wh_org_hrchy_type_desc,
             wh_org_hrchy_value_desc        = a_tbl_update(i).wh_org_hrchy_value_desc,
             fin_period_open_year_status    = a_tbl_update(i).fin_period_open_year_status,
             fin_period_open_yr_status_desc = a_tbl_update(i).fin_period_open_yr_status_desc,
             store_pos_active_ind           = a_tbl_update(i).store_pos_active_ind,
             new_store_trading_mtg_ind      = a_tbl_update(i).new_store_trading_mtg_ind,
             location_long_desc             = a_tbl_update(i).location_long_desc,
             district_long_desc             = a_tbl_update(i).district_long_desc,
             region_long_desc               = a_tbl_update(i).region_long_desc,
             area_long_desc                 = a_tbl_update(i).area_long_desc,
             chain_long_desc                = a_tbl_update(i).chain_long_desc,
             company_long_desc              = a_tbl_update(i).company_long_desc,
             total                          = a_tbl_update(i).total,
             total_desc                     = a_tbl_update(i).total_desc,
             last_updated_date              = a_tbl_update(i).last_updated_date,
             sunday_store_trade_ind         = a_tbl_update(i).sunday_store_trade_ind,
             store_cluster                  = a_tbl_update(i).store_cluster,
             store_size_format              = a_tbl_update(i).store_size_format,
             ST_S4S_SHAPE_OF_CHAIN_CODE     = a_tbl_update(i).ST_S4S_SHAPE_OF_CHAIN_CODE,               
             ST_S4S_SHAPE_OF_CHAIN_DESC     = a_tbl_update(i).ST_S4S_SHAPE_OF_CHAIN_DESC,              
             FICA_IND                       = a_tbl_update(i).FICA_IND   ,
             default_tax_region_no_perc     = a_tbl_update(i).default_tax_region_no_perc,
             sk1_loc_no_wh_physical_wh_no   = a_tbl_update(i).sk1_loc_no_wh_physical_wh_no,
             H1_MP_STORE_ALT_AREA_NO        = a_tbl_update(i).H1_MP_STORE_ALT_AREA_NO,
             SK1_H1_MP_STORE_ALT_AREA_NO    = a_tbl_update(i).SK1_H1_MP_STORE_ALT_AREA_NO,
             AXIMA_IND                      = a_tbl_update(i).AXIMA_IND,
             REPLEN_STORE_IND               = a_tbl_update(i).REPLEN_STORE_IND,

/* QC5061 - remove from update after go-live of WH_PRF_MP_032U */
/*             STORE_SIZE_CLUSTER             = A_TBL_UPDATE(I).STORE_SIZE_CLUSTER,
             SK1_CUSTOMER_CLASS_CODE        = A_TBL_UPDATE(I).SK1_CUSTOMER_CLASS_CODE,
             CUSTOMER_CLASS_LONG_DESC       = A_TBL_UPDATE(I).CUSTOMER_CLASS_LONG_DESC,
             CUSTOMER_CLASS_SHORT_DESC      = A_TBL_UPDATE(I).CUSTOMER_CLASS_SHORT_DESC,
             CUSTOMER_CLASSIFICATION        = A_TBL_UPDATE(I).CUSTOMER_CLASSIFICATION,
             SK1_STORE_CLUSTER_CODE         = A_TBL_UPDATE(I).SK1_STORE_CLUSTER_CODE,
             STORE_CLUSTER_LONG_DESC        = A_TBL_UPDATE(I).STORE_CLUSTER_LONG_DESC,
             STORE_CLUSTER_SHORT_DESC       = a_tbl_update(i).STORE_CLUSTER_SHORT_DESC */
             
             sk1_store_tier_no              = a_tbl_update(i).sk1_store_tier_no,
             linked_location_no             = a_tbl_update(i).linked_location_no
      where  location_no                    = a_tbl_update(i).location_no;

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
                       ' '||a_tbl_update(g_error_index).location_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := dwh_valid.dim_location(g_rec_out.location_no);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_location_no  := location_hierachy_seq.nextval;
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
            dbms_output.put_line('*** local_write_output others ***');
            dbms_output.put_line('g_rec_out.location_no='||g_rec_out.location_no||' g_rec_out.loc_type='||g_rec_out.loc_type);
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_location EX FND_LOCATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '16/AUG/17';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_location;
    fetch c_fnd_location bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;

    fetch c_fnd_location bulk collect into a_stg_input limit g_forall_limit;

    end loop;

    close c_fnd_location;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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

end wh_prf_corp_026u_01apr;
