--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_030U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_030U" 
  ( p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        August 2008
  --  Author:      Alastair de Wet
  --  Purpose:     Create location dimention table in the foundation layer
  --               with input ex staging table from RMS.
  --  Tables:      Input  - stg_rms_location_cpy
  --               Output - fnd_location
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  25 Nov 2008 - defect 317- Removing COUNTRY_NAME from all Staging tables
  --                defect 318- Removing COUNTRY_NAME from all Foundation tables
  --  06 feb 2009 - defect 698 - To remove st_dist_cost_store_no from table.
  --  04 Mar 2009 - defect 929 - NULL values on
  --                             DIM_LOCATION.WH_ORG_HRCHY_TYPE_DESC.
  --                             Therefore need to derive description
  --  18 April 2011 4263       -Planned Location and Product Hierarchy
  --                             restructure development
  --  10 Jul 2017              - add store_tier_no for DJ Tier Information
  --  29 Aug 2017              - add new column (linked_location_no) for linked JV store (Food Services Reporting Project)
  --  November 2017       -- addition of DC Region for Multi-currency (Bhavesh Valodia)
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := 10000;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_hospital_text stg_rms_location_hsp.sys_process_msg%type;
  g_rec_out fnd_location%rowtype;
  g_rec_in stg_rms_location_cpy%rowtype;
  g_found BOOLEAN;
  g_valid BOOLEAN;

--- qc4263
g_restructure_ind    dim_control.restructure_ind%type;
g_district_no   fnd_location.district_no%type;
--- qc4263

  --g_date              date          := to_char(sysdate,('dd mon yyyy'));
  g_date DATE := TRUNC(sysdate);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_030U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE LOCATION MASTERDATA EX RMS';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF stg_rms_location_cpy%rowtype;
  a_stg_input stg_array;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF fnd_location%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF fnd_location%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  a_count_u INTEGER := 0;
  -- For arrays used to update the staging table process_code --
type staging_array1
IS
  TABLE OF stg_rms_location_cpy.sys_source_batch_id%type INDEX BY binary_integer;
type staging_array2
IS
  TABLE OF stg_rms_location_cpy.sys_source_sequence_no%type INDEX BY binary_integer;
  a_staging1 staging_array1;
  a_staging2 staging_array2;
  a_empty_set_s1 staging_array1;
  a_empty_set_s2 staging_array2;
  a_count_stg INTEGER := 0;
  CURSOR c_stg_rms_location
  IS
     SELECT *
       FROM stg_rms_location_cpy
      WHERE sys_process_code = 'N'
   ORDER BY loc_type;
  -- order by only where sequencing is essential to the correct loading of data
  --**************************************************************************************************
  -- Process, transform and validate the data read from the input interface
  --**************************************************************************************************
PROCEDURE local_address_variables
AS
BEGIN
  g_hospital                               := 'N';
  g_rec_out.location_no                    := g_rec_in.location_no;
  g_rec_out.location_name                  := g_rec_in.location_name;
  g_rec_out.loc_type                       := g_rec_in.loc_type;
  g_rec_out.district_no                    := g_rec_in.district_no;
  g_rec_out.address_line_1                 := g_rec_in.address_line_1;
  g_rec_out.address_line_2                 := g_rec_in.address_line_2;
  g_rec_out.city_name                      := g_rec_in.city_name;
  g_rec_out.county_code                    := g_rec_in.county_code;
  g_rec_out.province_state_code            := g_rec_in.province_state_code;
  g_rec_out.country_code                   := g_rec_in.country_code;
  --   g_rec_out.country_name                    := g_rec_in.country_name;
  g_rec_out.postal_code                    := g_rec_in.postal_code;
  g_rec_out.changed_address_ind            := g_rec_in.changed_address_ind;
  g_rec_out.email_address                  := g_rec_in.email_address;
  g_rec_out.channel_no                     := g_rec_in.channel_no;
  g_rec_out.vat_region_no                  := g_rec_in.vat_region_no;
  g_rec_out.stock_holding_ind              := g_rec_in.stock_holding_ind;
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
  g_rec_out.st_open_date                   := g_rec_in.st_open_date;
  g_rec_out.st_close_date                  := g_rec_in.st_close_date;
  g_rec_out.st_acquired_date               := g_rec_in.st_acquired_date;
  g_rec_out.st_remodeled_date              := g_rec_in.st_remodeled_date;
  g_rec_out.st_format_no                   := g_rec_in.st_format_no;
  g_rec_out.st_format_name                 := g_rec_in.st_format_name;
  g_rec_out.st_class_code                  := g_rec_in.st_class_code;
  g_rec_out.st_mall_name                   := g_rec_in.st_mall_name;
  g_rec_out.st_shop_centre_type            := g_rec_in.st_shop_centre_type;
  g_rec_out.st_num_total_square_feet       := g_rec_in.st_num_total_square_feet;
  g_rec_out.st_num_selling_square_feet     := g_rec_in.st_num_selling_square_feet;
  g_rec_out.st_linear_distance             := g_rec_in.st_linear_distance;
  g_rec_out.st_language_no                 := g_rec_in.st_language_no;
  g_rec_out.st_integrated_pos_ind          := g_rec_in.st_integrated_pos_ind;
  g_rec_out.st_orig_currency_code          := g_rec_in.st_orig_currency_code;
  g_rec_out.st_store_type                  := g_rec_in.st_store_type;
  g_rec_out.st_value_of_chain_clip_code    := g_rec_in.st_value_of_chain_clip_code;
  g_rec_out.st_ww_online_picking_ind       := g_rec_in.st_ww_online_picking_ind;
  g_rec_out.st_food_sell_store_ind         := g_rec_in.st_food_sell_store_ind;
  g_rec_out.st_ww_online_picking_rgn_code  := g_rec_in.st_ww_online_picking_rgn_code;
  g_rec_out.st_geo_territory_code          := g_rec_in.st_geo_territory_code;
  g_rec_out.st_generation_code             := g_rec_in.st_generation_code;
  g_rec_out.st_site_locality_code          := g_rec_in.st_site_locality_code;
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
  g_rec_out.st_shape_of_the_chain_code     := g_rec_in.st_shape_of_the_chain_code;
  g_rec_out.st_receiving_ind               := g_rec_in.st_receiving_ind;
  g_rec_out.st_default_wh_no               := g_rec_in.st_default_wh_no;
  g_rec_out.st_chbd_closest_wh_no          := g_rec_in.st_chbd_closest_wh_no;
  g_rec_out.st_prom_zone_no                := g_rec_in.st_prom_zone_no;
  g_rec_out.st_prom_zone_desc              := g_rec_in.st_prom_zone_desc;
  g_rec_out.st_transfer_zone_no            := g_rec_in.st_transfer_zone_no;
  g_rec_out.st_transfer_zone_desc          := g_rec_in.st_transfer_zone_desc;
  g_rec_out.st_num_stop_order_days         := g_rec_in.st_num_stop_order_days;
  g_rec_out.st_num_start_order_days        := g_rec_in.st_num_start_order_days;
  g_rec_out.wh_discipline_type             := g_rec_in.wh_discipline_type;
  g_rec_out.wh_store_no                    := g_rec_in.wh_store_no;
  g_rec_out.wh_supply_chain_ind            := g_rec_in.wh_supply_chain_ind;
  g_rec_out.wh_primary_supply_chain_type   := g_rec_in.wh_primary_supply_chain_type;
  g_rec_out.wh_value_add_supplier_no       := g_rec_in.wh_value_add_supplier_no;
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
  g_rec_out.source_data_status_code        := g_rec_in.source_data_status_code;
  g_rec_out.forecastable_ind               := 0;
  g_rec_out.wh_zone_group_no               := 1;
--   g_rec_out.st_dist_cost_store_no           := 1;
  g_rec_out.last_updated_date              := g_date;
  g_rec_out.num_store_leadtime_days        := 0;
  g_rec_out.store_tier_no                  := -1;
  g_rec_out.linked_location_no             := g_rec_in.linked_location_no;
  g_rec_out.dc_region_no                    := g_rec_in.dc_region_no;  -- NO VALIDATION AS THIS IS ONLY PLACE WHERE MASTER DATA FOR DC_REGION COMES IN

   if g_restructure_ind = 0 then
      begin
        select district_no
        into   g_district_no
        from   fnd_location
        where  location_no = g_rec_out.location_no;

        exception
        when no_data_found then
          g_district_no := g_rec_out.district_no;
      end;

      if g_district_no <> g_rec_out.district_no then
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_location',g_rec_out.location_no,g_district_no,g_rec_out.district_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure hierarchy ';
         l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.location_no||' '||g_rec_out.district_no  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;


  IF g_rec_out.loc_type = 'W' AND g_rec_out.district_no IS NULL AND g_rec_out.wh_primary_virtual_wh_no IS NULL THEN
    BEGIN
      --        dbms_output.put_line('Virtual WH  '||g_rec_out.wh_physical_wh_no||' '||g_rec_out.location_no);
       SELECT district_no
         INTO g_rec_out.district_no
         FROM fnd_location
        WHERE location_no = g_rec_out.wh_physical_wh_no;
    EXCEPTION
    WHEN no_data_found THEN
      l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);
    WHEN OTHERS THEN
      l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);
      raise;
    END;
  END IF ;
  IF NOT dwh_valid.fnd_district(g_rec_out.district_no) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_district_not_found;
    l_text          := dwh_constants.vc_district_not_found||g_rec_out.location_no||' '||g_rec_out.district_no ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    RETURN;
  END IF;
  --   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
  --     g_hospital      := 'Y';
  --     g_hospital_text := dwh_constants.vc_invalid_source_code;
  --     return;
  --   end if;
  IF NOT dwh_valid.indicator_field(g_rec_out.CHANGED_ADDRESS_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.STOCK_HOLDING_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_VAT_INCL_RSP_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_INTEGRATED_POS_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_WW_ONLINE_PICKING_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_FOOD_SELL_STORE_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_CHBD_HANGING_SET_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.ST_RECEIVING_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_SUPPLY_CHAIN_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_REDIST_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_RPL_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_VIRTUAL_WH_PROTECTED_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_INVEST_BUY_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_INVST_BUY_WH_AUTO_CLEAR_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_BREAK_PACK_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_FLOW_SUPPLY_CHAIN_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_XD_SUPPLY_CHAIN_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_HS_SUPPLY_CHAIN_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_EXPORT_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_IMPORT_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_DOMESTIC_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
  IF NOT dwh_valid.indicator_field(g_rec_out.WH_RTV_WH_IND) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_invalid_indicator;
  END IF;
EXCEPTION
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_address_variables;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
PROCEDURE local_write_hospital
AS
BEGIN
  g_rec_in.sys_load_date        := sysdate;
  g_rec_in.sys_load_system_name := 'DWH';
  g_rec_in.sys_process_code     := 'Y';
  g_rec_in.sys_process_msg      := g_hospital_text;
   INSERT INTO stg_rms_location_hsp VALUES g_rec_in;

  g_recs_hospital := g_recs_hospital + sql%rowcount;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lh_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lh_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_write_hospital;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
PROCEDURE local_bulk_insert
AS
BEGIN
  forall i IN a_tbl_insert.first .. a_tbl_insert.last
  SAVE exceptions
   INSERT INTO fnd_location VALUES a_tbl_insert(i);

  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
    (-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_insert(g_error_index).location_no;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_insert;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_update
AS
BEGIN
  forall i IN a_tbl_update.first .. a_tbl_update.last
  SAVE exceptions
   UPDATE fnd_location
  SET location_name                = a_tbl_update(i).location_name      ,
    loc_type                       = a_tbl_update(i).loc_type           ,
    district_no                    = a_tbl_update(i).district_no        ,
    address_line_1                 = a_tbl_update(i).address_line_1     ,
    address_line_2                 = a_tbl_update(i).address_line_2     ,
    city_name                      = a_tbl_update(i).city_name          ,
    county_code                    = a_tbl_update(i).county_code        ,
    province_state_code            = a_tbl_update(i).province_state_code,
    country_code                   = a_tbl_update(i).country_code       ,
    --          country_name                    = a_tbl_update(i).country_name,
    postal_code                    = a_tbl_update(i).postal_code                   ,
    changed_address_ind            = a_tbl_update(i).changed_address_ind           ,
    email_address                  = a_tbl_update(i).email_address                 ,
    channel_no                     = a_tbl_update(i).channel_no                    ,
    vat_region_no                  = a_tbl_update(i).vat_region_no                 ,
    stock_holding_ind              = a_tbl_update(i).stock_holding_ind             ,
    currency_code                  = a_tbl_update(i).currency_code                 ,
    st_short_name                  = a_tbl_update(i).st_short_name                 ,
    st_abbrev_name                 = a_tbl_update(i).st_abbrev_name                ,
    st_scndry_name                 = a_tbl_update(i).st_scndry_name                ,
    st_fax_no                      = a_tbl_update(i).st_fax_no                     ,
    st_phone_no                    = a_tbl_update(i).st_phone_no                   ,
    st_manager_name                = a_tbl_update(i).st_manager_name               ,
    st_franchise_owner_name        = a_tbl_update(i).st_franchise_owner_name       ,
    st_sister_store_no             = a_tbl_update(i).st_sister_store_no            ,
    st_vat_incl_rsp_ind            = a_tbl_update(i).st_vat_incl_rsp_ind           ,
    st_open_date                   = a_tbl_update(i).st_open_date                  ,
    st_close_date                  = a_tbl_update(i).st_close_date                 ,
    st_acquired_date               = a_tbl_update(i).st_acquired_date              ,
    st_remodeled_date              = a_tbl_update(i).st_remodeled_date             ,
    st_format_no                   = a_tbl_update(i).st_format_no                  ,
    st_format_name                 = a_tbl_update(i).st_format_name                ,
    st_class_code                  = a_tbl_update(i).st_class_code                 ,
    st_mall_name                   = a_tbl_update(i).st_mall_name                  ,
    st_shop_centre_type            = a_tbl_update(i).st_shop_centre_type           ,
    st_num_total_square_feet       = a_tbl_update(i).st_num_total_square_feet      ,
    st_num_selling_square_feet     = a_tbl_update(i).st_num_selling_square_feet    ,
    st_linear_distance             = a_tbl_update(i).st_linear_distance            ,
    st_language_no                 = a_tbl_update(i).st_language_no                ,
    st_integrated_pos_ind          = a_tbl_update(i).st_integrated_pos_ind         ,
    st_orig_currency_code          = a_tbl_update(i).st_orig_currency_code         ,
    st_store_type                  = a_tbl_update(i).st_store_type                 ,
    st_value_of_chain_clip_code    = a_tbl_update(i).st_value_of_chain_clip_code   ,
    st_ww_online_picking_ind       = a_tbl_update(i).st_ww_online_picking_ind      ,
    st_food_sell_store_ind         = a_tbl_update(i).st_food_sell_store_ind        ,
    st_ww_online_picking_rgn_code  = a_tbl_update(i).st_ww_online_picking_rgn_code ,
    st_geo_territory_code          = a_tbl_update(i).st_geo_territory_code         ,
    st_generation_code             = a_tbl_update(i).st_generation_code            ,
    st_site_locality_code          = a_tbl_update(i).st_site_locality_code         ,
    st_selling_space_clip_code     = a_tbl_update(i).st_selling_space_clip_code    ,
    st_dun_bradstreet_id           = a_tbl_update(i).st_dun_bradstreet_id          ,
    st_dun_bradstreet_loc_id       = a_tbl_update(i).st_dun_bradstreet_loc_id      ,
    st_chbd_hanging_set_ind        = a_tbl_update(i).st_chbd_hanging_set_ind       ,
    st_chbd_rpl_rgn_leadtime_code  = a_tbl_update(i).st_chbd_rpl_rgn_leadtime_code ,
    st_chbd_val_chain_clip_code    = a_tbl_update(i).st_chbd_val_chain_clip_code   ,
    st_fd_sell_space_clip_code     = a_tbl_update(i).st_fd_sell_space_clip_code    ,
    st_fd_store_format_code        = a_tbl_update(i).st_fd_store_format_code       ,
    st_fd_value_of_chain_clip_code = a_tbl_update(i).st_fd_value_of_chain_clip_code,
    st_fd_units_sold_clip_code     = a_tbl_update(i).st_fd_units_sold_clip_code    ,
    st_fd_customer_type_clip_code  = a_tbl_update(i).st_fd_customer_type_clip_code ,
    st_pos_type                    = a_tbl_update(i).st_pos_type                   ,
    st_pos_tran_no_generated_code  = a_tbl_update(i).st_pos_tran_no_generated_code ,
    st_shape_of_the_chain_code     = a_tbl_update(i).st_shape_of_the_chain_code    ,
    st_receiving_ind               = a_tbl_update(i).st_receiving_ind              ,
    st_default_wh_no               = a_tbl_update(i).st_default_wh_no              ,
    st_chbd_closest_wh_no          = a_tbl_update(i).st_chbd_closest_wh_no         ,
    st_prom_zone_no                = a_tbl_update(i).st_prom_zone_no               ,
    st_prom_zone_desc              = a_tbl_update(i).st_prom_zone_desc             ,
    st_transfer_zone_no            = a_tbl_update(i).st_transfer_zone_no           ,
    st_transfer_zone_desc          = a_tbl_update(i).st_transfer_zone_desc         ,
    st_num_stop_order_days         = a_tbl_update(i).st_num_stop_order_days        ,
    st_num_start_order_days        = a_tbl_update(i).st_num_start_order_days       ,
    wh_discipline_type             = a_tbl_update(i).wh_discipline_type            ,
    wh_store_no                    = a_tbl_update(i).wh_store_no                   ,
    wh_supply_chain_ind            = a_tbl_update(i).wh_supply_chain_ind           ,
    wh_primary_supply_chain_type   = a_tbl_update(i).wh_primary_supply_chain_type  ,
    wh_value_add_supplier_no       = a_tbl_update(i).wh_value_add_supplier_no      ,
    wh_triceps_customer_code       = a_tbl_update(i).wh_triceps_customer_code      ,
    wh_primary_virtual_wh_no       = a_tbl_update(i).wh_primary_virtual_wh_no      ,
    wh_physical_wh_no              = a_tbl_update(i).wh_physical_wh_no             ,
    wh_redist_wh_ind               = a_tbl_update(i).wh_redist_wh_ind              ,
    wh_rpl_ind                     = a_tbl_update(i).wh_rpl_ind                    ,
    wh_virtual_wh_rpl_wh_no        = a_tbl_update(i).wh_virtual_wh_rpl_wh_no       ,
    wh_virtual_wh_restricted_ind   = a_tbl_update(i).wh_virtual_wh_restricted_ind  ,
    wh_virtual_wh_protected_ind    = a_tbl_update(i).wh_virtual_wh_protected_ind   ,
    wh_invest_buy_wh_ind           = a_tbl_update(i).wh_invest_buy_wh_ind          ,
    wh_invst_buy_wh_auto_clear_ind = a_tbl_update(i).wh_invst_buy_wh_auto_clear_ind,
    wh_virtual_wh_invst_buy_wh_no  = a_tbl_update(i).wh_virtual_wh_invst_buy_wh_no ,
    wh_virtual_wh_tier_type        = a_tbl_update(i).wh_virtual_wh_tier_type       ,
    wh_break_pack_ind              = a_tbl_update(i).wh_break_pack_ind             ,
    wh_delivery_policy_code        = a_tbl_update(i).wh_delivery_policy_code       ,
    wh_rounding_seq_no             = a_tbl_update(i).wh_rounding_seq_no            ,
    wh_inv_repl_seq_no             = a_tbl_update(i).wh_inv_repl_seq_no            ,
    wh_flow_supply_chain_ind       = a_tbl_update(i).wh_flow_supply_chain_ind      ,
    wh_xd_supply_chain_ind         = a_tbl_update(i).wh_xd_supply_chain_ind        ,
    wh_hs_supply_chain_ind         = a_tbl_update(i).wh_hs_supply_chain_ind        ,
    wh_export_wh_ind               = a_tbl_update(i).wh_export_wh_ind              ,
    wh_import_wh_ind               = a_tbl_update(i).wh_import_wh_ind              ,
    wh_domestic_wh_ind             = a_tbl_update(i).wh_domestic_wh_ind            ,
    wh_rtv_wh_ind                  = a_tbl_update(i).wh_rtv_wh_ind                 ,
    wh_org_hrchy_type              = a_tbl_update(i).wh_org_hrchy_type             ,
    wh_org_hrchy_value             = a_tbl_update(i).wh_org_hrchy_value            ,
    source_data_status_code        = a_tbl_update(i).source_data_status_code       ,
    last_updated_date              = a_tbl_update(i).last_updated_date             ,
    linked_location_no             = a_tbl_update(i).linked_location_no ,
    dc_region_no                      = a_tbl_update(i).dc_region_no
    WHERE location_no              = a_tbl_update(i).location_no ;


    g_recs_updated := g_recs_updated + a_tbl_update.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).location_no;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_update;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_staging_update
AS
BEGIN
  forall i IN a_staging1.first .. a_staging1.last
  SAVE exceptions
   UPDATE stg_rms_location_cpy
  SET sys_process_code        = 'Y'
    WHERE sys_source_batch_id = a_staging1(i)
  AND sys_source_sequence_no  = a_staging2(i);
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_staging_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  g_found := dwh_valid.fnd_location(g_rec_out.location_no);
  -- Check if insert of item already in insert array and change to put duplicate in update array
  IF a_count_i > 0 AND NOT g_found THEN
    FOR i     IN a_tbl_insert.first .. a_tbl_insert.last
    LOOP
      IF a_tbl_insert(i).location_no = g_rec_out.location_no THEN
        g_found                     := TRUE;
      END IF;
    END LOOP;
  END IF;
  -- Place data into and array for later writing to table in bulk
  IF NOT g_found THEN
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  ELSE
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  END IF;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  --   if a_count > 1000 then
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_staging1   := a_empty_set_s1;
    a_staging2   := a_empty_set_s2;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
    a_count_stg  := 0;
    COMMIT;
  END IF;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_write_output;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF FND_LOCATION EX RMS STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--- qc4263
--**************************************************************************************************
-- Look up restructure_ind from dim_control
--**************************************************************************************************
    select  restructure_ind
    into      g_restructure_ind
    from    dim_control;
    l_text := 'RESTRUCTURE_IND IS:- '||g_restructure_ind;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--- qc4263
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  OPEN c_stg_rms_location;
  FETCH c_stg_rms_location bulk collect INTO a_stg_input limit g_forall_limit;

  WHILE a_stg_input.count > 0
  LOOP
    FOR i IN 1 .. a_stg_input.count
    LOOP
      g_recs_read            := g_recs_read + 1;
      IF g_recs_read mod 100000 = 0 THEN
        l_text               := dwh_constants.vc_log_records_processed|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;
      g_rec_in                := a_stg_input(i);
      a_count_stg             := a_count_stg + 1;
      a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
      a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
      local_address_variables;
      IF g_hospital = 'Y' THEN
        local_write_hospital;
      ELSE
        local_write_output;
      END IF;
    END LOOP;
    FETCH c_stg_rms_location bulk collect INTO a_stg_input limit g_forall_limit;
  END LOOP;
  CLOSE c_stg_rms_location;
  --**************************************************************************************************
  -- At end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  local_bulk_staging_update;
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END wh_fnd_corp_030u;
