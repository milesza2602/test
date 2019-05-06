--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_038U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_038U" 
                                                                (p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Generate the item dimention  sk type 2 load program
--  Tables:      Input  - dim_item
--               Output - dim_item_hist
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--  18 Feb 2009 - defect 737 - Rename fields most_recent_MERCH_SEASON_NO
--                             and most_recent_MERCH_PHASE_NO to
--                             MOST_RECENT_MERCH_SEASON_NO and
--                             MOST_RECENT_MERCH_PHASE_NO
--                             on tables DIM_ITEM, DIM_ITEM_HIST
--                             and DIM_LEV1_DIFF1
-- 19 May 2009 - defect 508  - Cater for Intactix measures (display_subclass_name,display_class_name,display_group_name)
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
g_recs_updated      integer       :=  0;
g_recs_inserted     integer       :=  0;
g_recs_hospital     integer       :=  0;

g_rec_out           dim_item_hist%rowtype;
g_count             integer       :=  0;
g_found             boolean;
g_insert_rec        boolean;
g_date              date          := trunc(sysdate);
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type  := 'WH_PRF_CORP_038U';
l_name              sys_dwh_log.log_name%type               := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type        := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type        := dwh_constants.vc_log_script_rtl_prf_md ;
l_procedure_name    sys_dwh_log.log_procedure_name%type     := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'GENERATE SK2 VERSION OF ITEM MASTER EX RMS';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_dim_item is
   select *
   from   dim_item;

g_rec_in            c_dim_item%rowtype;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.item_desc                       := g_rec_in.item_desc;
   g_rec_out.item_short_desc                 := g_rec_in.item_short_desc;
   g_rec_out.item_upper_desc                 := g_rec_in.item_upper_desc;
   g_rec_out.item_scndry_desc                := g_rec_in.item_scndry_desc;
   g_rec_out.sk1_subclass_no                 := g_rec_in.sk1_subclass_no;
   g_rec_out.subclass_no                     := g_rec_in.subclass_no;
   g_rec_out.subclass_name                   := g_rec_in.subclass_name;
   g_rec_out.sk1_class_no                    := g_rec_in.sk1_class_no;
   g_rec_out.class_no                        := g_rec_in.class_no;
   g_rec_out.class_name                      := g_rec_in.class_name;
   g_rec_out.sk1_department_no               := g_rec_in.sk1_department_no;
   g_rec_out.department_no                   := g_rec_in.department_no;
   g_rec_out.department_name                 := g_rec_in.department_name;
   g_rec_out.sk1_subgroup_no                 := g_rec_in.sk1_subgroup_no;
   g_rec_out.subgroup_no                     := g_rec_in.subgroup_no;
   g_rec_out.subgroup_name                   := g_rec_in.subgroup_name;
   g_rec_out.sk1_group_no                    := g_rec_in.sk1_group_no;
   g_rec_out.group_no                        := g_rec_in.group_no;
   g_rec_out.group_name                      := g_rec_in.group_name;
   g_rec_out.sk1_business_unit_no            := g_rec_in.sk1_business_unit_no;
   g_rec_out.business_unit_no                := g_rec_in.business_unit_no;
   g_rec_out.business_unit_name              := g_rec_in.business_unit_name;
   g_rec_out.sk1_company_no                  := g_rec_in.sk1_company_no;
   g_rec_out.company_no                      := g_rec_in.company_no;
   g_rec_out.company_name                    := g_rec_in.company_name;
   g_rec_out.item_status_code                := g_rec_in.item_status_code;
   g_rec_out.item_level_no                   := g_rec_in.item_level_no;
   g_rec_out.tran_level_no                   := g_rec_in.tran_level_no;
   g_rec_out.tran_ind                        := g_rec_in.tran_ind;
   g_rec_out.primary_ref_item_ind            := g_rec_in.primary_ref_item_ind;
   g_rec_out.fd_product_no                   := g_rec_in.fd_product_no;
   g_rec_out.item_parent_no                  := g_rec_in.item_parent_no;
   g_rec_out.item_grandparent_no             := g_rec_in.item_grandparent_no;
   g_rec_out.item_level1_no                  := g_rec_in.item_level1_no;
   g_rec_out.item_level2_no                  := g_rec_in.item_level2_no;
   g_rec_out.rpl_ind                         := g_rec_in.rpl_ind;
   g_rec_out.item_no_type                    := g_rec_in.item_no_type;
   g_rec_out.format_id                       := g_rec_in.format_id;
   g_rec_out.upc_prefix_no                   := g_rec_in.upc_prefix_no;
   g_rec_out.diff_1_code                     := g_rec_in.diff_1_code;
   g_rec_out.diff_2_code                     := g_rec_in.diff_2_code;
   g_rec_out.diff_3_code                     := g_rec_in.diff_3_code;
   g_rec_out.diff_4_code                     := g_rec_in.diff_4_code;
   g_rec_out.diff_1_code_desc                := g_rec_in.diff_1_code_desc;
   g_rec_out.diff_2_code_desc                := g_rec_in.diff_2_code_desc;
   g_rec_out.diff_3_code_desc                := g_rec_in.diff_3_code_desc;
   g_rec_out.diff_4_code_desc                := g_rec_in.diff_4_code_desc;
   g_rec_out.diff_1_diff_type                := g_rec_in.diff_1_diff_type;
   g_rec_out.diff_2_diff_type                := g_rec_in.diff_2_diff_type;
   g_rec_out.diff_3_diff_type                := g_rec_in.diff_3_diff_type;
   g_rec_out.diff_4_diff_type                := g_rec_in.diff_4_diff_type;
   g_rec_out.diff_1_type_desc                := g_rec_in.diff_1_type_desc;
   g_rec_out.diff_2_type_desc                := g_rec_in.diff_2_type_desc;
   g_rec_out.diff_3_type_desc                := g_rec_in.diff_3_type_desc;
   g_rec_out.diff_4_type_desc                := g_rec_in.diff_4_type_desc;
   g_rec_out.diff_type_colour_diff_code      := g_rec_in.diff_type_colour_diff_code;
   g_rec_out.diff_type_prim_size_diff_code   := g_rec_in.diff_type_prim_size_diff_code;
   g_rec_out.diff_type_scnd_size_diff_code   := g_rec_in.diff_type_scnd_size_diff_code;
   g_rec_out.diff_type_fragrance_diff_code   := g_rec_in.diff_type_fragrance_diff_code;
   g_rec_out.diff_1_diff_group_code          := g_rec_in.diff_1_diff_group_code;
   g_rec_out.diff_2_diff_group_code          := g_rec_in.diff_2_diff_group_code;
   g_rec_out.diff_3_diff_group_code          := g_rec_in.diff_3_diff_group_code;
   g_rec_out.diff_4_diff_group_code          := g_rec_in.diff_4_diff_group_code;
   g_rec_out.diff_1_diff_group_desc          := g_rec_in.diff_1_diff_group_desc;
   g_rec_out.diff_2_diff_group_desc          := g_rec_in.diff_2_diff_group_desc;
   g_rec_out.diff_3_diff_group_desc          := g_rec_in.diff_3_diff_group_desc;
   g_rec_out.diff_4_diff_group_desc          := g_rec_in.diff_4_diff_group_desc;
   g_rec_out.diff_1_display_seq              := g_rec_in.diff_1_display_seq;
   g_rec_out.diff_2_display_seq              := g_rec_in.diff_2_display_seq;
   g_rec_out.diff_3_display_seq              := g_rec_in.diff_3_display_seq;
   g_rec_out.diff_4_display_seq              := g_rec_in.diff_4_display_seq;
   g_rec_out.item_aggr_ind                   := g_rec_in.item_aggr_ind;
   g_rec_out.diff_1_aggr_ind                 := g_rec_in.diff_1_aggr_ind;
   g_rec_out.diff_2_aggr_ind                 := g_rec_in.diff_2_aggr_ind;
   g_rec_out.diff_3_aggr_ind                 := g_rec_in.diff_3_aggr_ind;
   g_rec_out.diff_4_aggr_ind                 := g_rec_in.diff_4_aggr_ind;
   g_rec_out.retail_zone_group_no            := g_rec_in.retail_zone_group_no;
   g_rec_out.cost_zone_group_no              := g_rec_in.cost_zone_group_no;
   g_rec_out.standard_uom_code               := g_rec_in.standard_uom_code;
   g_rec_out.standard_uom_desc               := g_rec_in.standard_uom_desc;
   g_rec_out.standard_uom_class_code         := g_rec_in.standard_uom_class_code;
   g_rec_out.uom_conv_factor                 := g_rec_in.uom_conv_factor;
   g_rec_out.package_size                    := g_rec_in.package_size;
   g_rec_out.package_uom_code                := g_rec_in.package_uom_code;
   g_rec_out.package_uom_desc                := g_rec_in.package_uom_desc;
   g_rec_out.package_uom_class_code          := g_rec_in.package_uom_class_code;
   g_rec_out.merchandise_item_ind            := g_rec_in.merchandise_item_ind;
   g_rec_out.store_ord_mult_unit_type_code   := g_rec_in.store_ord_mult_unit_type_code;
   g_rec_out.ext_sys_forecast_ind            := g_rec_in.ext_sys_forecast_ind;
   g_rec_out.primary_currency_original_rsp   := g_rec_in.primary_currency_original_rsp;
   g_rec_out.mfg_recommended_rsp             := g_rec_in.mfg_recommended_rsp;
   g_rec_out.retail_label_type               := g_rec_in.retail_label_type;
   g_rec_out.retail_label_value              := g_rec_in.retail_label_value;
   g_rec_out.handling_temp_code              := g_rec_in.handling_temp_code;
   g_rec_out.handling_sensitivity_code       := g_rec_in.handling_sensitivity_code;
   g_rec_out.random_mass_ind                 := g_rec_in.random_mass_ind;
   g_rec_out.first_received_date             := g_rec_in.first_received_date;
   g_rec_out.last_received_date              := g_rec_in.last_received_date;
   g_rec_out.most_recent_received_qty        := g_rec_in.most_recent_received_qty;
   g_rec_out.waste_type                      := g_rec_in.waste_type;
   g_rec_out.avg_waste_perc                  := g_rec_in.avg_waste_perc;
   g_rec_out.default_waste_perc              := g_rec_in.default_waste_perc;
   g_rec_out.constant_dimension_ind          := g_rec_in.constant_dimension_ind;
   g_rec_out.pack_item_ind                   := g_rec_in.pack_item_ind;
   g_rec_out.pack_item_simple_ind            := g_rec_in.pack_item_simple_ind;
   g_rec_out.pack_item_inner_pack_ind        := g_rec_in.pack_item_inner_pack_ind;
   g_rec_out.pack_item_sellable_unit_ind     := g_rec_in.pack_item_sellable_unit_ind;
   g_rec_out.pack_item_orderable_ind         := g_rec_in.pack_item_orderable_ind;
   g_rec_out.pack_item_type                  := g_rec_in.pack_item_type;
   g_rec_out.pack_item_receivable_type       := g_rec_in.pack_item_receivable_type;
   g_rec_out.item_comment                    := g_rec_in.item_comment;
   g_rec_out.item_service_level_type         := g_rec_in.item_service_level_type;
   g_rec_out.gift_wrap_ind                   := g_rec_in.gift_wrap_ind;
   g_rec_out.ship_alone_ind                  := g_rec_in.ship_alone_ind;
   g_rec_out.origin_item_ext_src_sys_name    := g_rec_in.origin_item_ext_src_sys_name;
   g_rec_out.banded_item_ind                 := g_rec_in.banded_item_ind;
   g_rec_out.static_mass                     := g_rec_in.static_mass;
   g_rec_out.ext_ref_id                      := g_rec_in.ext_ref_id;
   g_rec_out.create_date                     := g_rec_in.create_date;
   g_rec_out.size_id                         := g_rec_in.size_id;
   g_rec_out.color_id                        := g_rec_in.color_id;
   g_rec_out.style_colour_no                 := g_rec_in.style_colour_no;
   g_rec_out.style_no                        := g_rec_in.style_no;
   g_rec_out.buying_ind                      := g_rec_in.buying_ind;
   g_rec_out.selling_ind                     := g_rec_in.selling_ind;
   g_rec_out.product_class                   := g_rec_in.product_class;
   g_rec_out.fd_discipline_type              := g_rec_in.fd_discipline_type;
   g_rec_out.handling_method_code            := g_rec_in.handling_method_code;
   g_rec_out.handling_method_name            := g_rec_in.handling_method_name;
   g_rec_out.display_method_code             := g_rec_in.display_method_code;
   g_rec_out.display_method_name             := g_rec_in.display_method_name;
   g_rec_out.tray_size_code                  := g_rec_in.tray_size_code;
   g_rec_out.segregation_ind                 := g_rec_in.segregation_ind;
   g_rec_out.outer_case_barcode              := g_rec_in.outer_case_barcode;
   g_rec_out.rpl_merch_season_no             := g_rec_in.rpl_merch_season_no;
   g_rec_out.prod_catg_code                  := g_rec_in.prod_catg_code;
   g_rec_out.supp_comment                    := g_rec_in.supp_comment;
   g_rec_out.rdf_forecst_ind                 := g_rec_in.rdf_forecst_ind;
   g_rec_out.item_launch_date                := g_rec_in.item_launch_date;
   g_rec_out.product_profile_code            := g_rec_in.product_profile_code;
   g_rec_out.vat_rate_perc                   := g_rec_in.vat_rate_perc;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.primary_supplier_no             := g_rec_in.primary_supplier_no;
   g_rec_out.sk1_merch_season_phase_no       := g_rec_in.sk1_merch_season_phase_no;
   g_rec_out.most_recent_merch_season_no     := g_rec_in.most_recent_merch_season_no;
   g_rec_out.most_recent_merch_phase_no      := g_rec_in.most_recent_merch_phase_no;
   g_rec_out.live_on_rms_date                := g_rec_in.live_on_rms_date;
   g_rec_out.base_rsp                        := g_rec_in.base_rsp;
   g_rec_out.base_rsp_excl_vat               := g_rec_in.base_rsp_excl_vat;
   g_rec_out.vat_code                        := g_rec_in.vat_code;
   g_rec_out.sk1_style_no                    := g_rec_in.sk1_style_no;
   g_rec_out.sk1_style_colour_no             := g_rec_in.sk1_style_colour_no;
   g_rec_out.supply_chain_type               := g_rec_in.supply_chain_type;
   g_rec_out.rp_catlg_ind                    := g_rec_in.rp_catlg_ind;
   g_rec_out.next_cost_price                 := g_rec_in.next_cost_price;
   g_rec_out.next_cost_price_effective_date  := g_rec_in.next_cost_price_effective_date;
   g_rec_out.rpl_ind_long_desc               := g_rec_in.rpl_ind_long_desc;
   g_rec_out.display_subclass_name           := g_rec_in.display_subclass_name;
   g_rec_out.display_class_name              := g_rec_in.display_class_name;
   g_rec_out.display_group_name              := g_rec_in.display_group_name;
   g_rec_out.multi_supplier_item_ind         := g_rec_in.multi_supplier_item_ind;
   g_rec_out.sk1_diff_1_range_no             := g_rec_in.sk1_diff_1_range_no;
   --QC3851
   g_rec_out.store_scanned_order_ind	       := g_rec_in.store_scanned_order_ind;
   g_rec_out.max_scanned_order_cases	       := g_rec_in.max_scanned_order_cases;

   g_rec_out.last_updated_date               := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_rec_out.item_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   dwh_valid.did_item_change
   (g_rec_out.item_no,g_rec_out.subclass_no,g_rec_out.class_no,g_rec_out.department_no,g_rec_out.subgroup_no,
    g_rec_out.group_no,g_rec_out.business_unit_no,g_rec_out.company_no,g_insert_rec);

   if g_insert_rec then
      g_rec_out.sk2_item_no           := merch_hierachy_seq.nextval;
      g_rec_out.sk2_active_from_date  := trunc(sysdate);
      g_rec_out.sk2_active_to_date    := dwh_constants.sk_to_date;

      select count(*)
      into   g_count
      from   dim_item_hist
      where  item_no = g_rec_out.item_no;
      if g_count = 0 then
         g_rec_out.sk2_active_from_date  := '1 jan 2001';
      end if;

      insert into dim_item_hist values g_rec_out;
      g_recs_inserted                 := g_recs_inserted + sql%rowcount;
   else
      update dim_item_hist
      set    item_no                         = g_rec_out.item_no,
             item_desc                       = g_rec_out.item_desc,
             item_short_desc                 = g_rec_out.item_short_desc,
             item_upper_desc                 = g_rec_out.item_upper_desc,
             item_scndry_desc                = g_rec_out.item_scndry_desc,
             sk1_subclass_no                 = g_rec_out.sk1_subclass_no,
             subclass_no                     = g_rec_out.subclass_no,
             subclass_name                   = g_rec_out.subclass_name,
             sk1_class_no                    = g_rec_out.sk1_class_no,
             class_no                        = g_rec_out.class_no,
             class_name                      = g_rec_out.class_name,
             sk1_department_no               = g_rec_out.sk1_department_no,
             department_no                   = g_rec_out.department_no,
             department_name                 = g_rec_out.department_name,
             sk1_subgroup_no                 = g_rec_out.sk1_subgroup_no,
             subgroup_no                     = g_rec_out.subgroup_no,
             subgroup_name                   = g_rec_out.subgroup_name,
             sk1_group_no                    = g_rec_out.sk1_group_no,
             group_no                        = g_rec_out.group_no,
             group_name                      = g_rec_out.group_name,
             sk1_business_unit_no            = g_rec_out.sk1_business_unit_no,
             business_unit_no                = g_rec_out.business_unit_no,
             business_unit_name              = g_rec_out.business_unit_name,
             sk1_company_no                  = g_rec_out.sk1_company_no,
             company_no                      = g_rec_out.company_no,
             company_name                    = g_rec_out.company_name,
             item_status_code                = g_rec_out.item_status_code,
             item_level_no                   = g_rec_out.item_level_no,
             tran_level_no                   = g_rec_out.tran_level_no,
             tran_ind                        = g_rec_out.tran_ind,
             primary_ref_item_ind            = g_rec_out.primary_ref_item_ind,
             fd_product_no                   = g_rec_out.fd_product_no,
             item_parent_no                  = g_rec_out.item_parent_no,
             item_grandparent_no             = g_rec_out.item_grandparent_no,
             item_level1_no                  = g_rec_out.item_level1_no,
             item_level2_no                  = g_rec_out.item_level2_no,
             rpl_ind                         = g_rec_out.rpl_ind,
             item_no_type                    = g_rec_out.item_no_type,
             format_id                       = g_rec_out.format_id,
             upc_prefix_no                   = g_rec_out.upc_prefix_no,
             diff_1_code                     = g_rec_out.diff_1_code,
             diff_2_code                     = g_rec_out.diff_2_code,
             diff_3_code                     = g_rec_out.diff_3_code,
             diff_4_code                     = g_rec_out.diff_4_code,
             diff_1_code_desc                = g_rec_out.diff_1_code_desc,
             diff_2_code_desc                = g_rec_out.diff_2_code_desc,
             diff_3_code_desc                = g_rec_out.diff_3_code_desc,
             diff_4_code_desc                = g_rec_out.diff_4_code_desc,
             diff_1_diff_type                = g_rec_out.diff_1_diff_type,
             diff_2_diff_type                = g_rec_out.diff_2_diff_type,
             diff_3_diff_type                = g_rec_out.diff_3_diff_type,
             diff_4_diff_type                = g_rec_out.diff_4_diff_type,
             diff_1_type_desc                = g_rec_out.diff_1_type_desc,
             diff_2_type_desc                = g_rec_out.diff_2_type_desc,
             diff_3_type_desc                = g_rec_out.diff_3_type_desc,
             diff_4_type_desc                = g_rec_out.diff_4_type_desc,
             diff_type_colour_diff_code      = g_rec_out.diff_type_colour_diff_code,
             diff_type_prim_size_diff_code   = g_rec_out.diff_type_prim_size_diff_code,
             diff_type_scnd_size_diff_code   = g_rec_out.diff_type_scnd_size_diff_code,
             diff_type_fragrance_diff_code   = g_rec_out.diff_type_fragrance_diff_code,
             diff_1_diff_group_code          = g_rec_out.diff_1_diff_group_code,
             diff_2_diff_group_code          = g_rec_out.diff_2_diff_group_code,
             diff_3_diff_group_code          = g_rec_out.diff_3_diff_group_code,
             diff_4_diff_group_code          = g_rec_out.diff_4_diff_group_code,
             diff_1_diff_group_desc          = g_rec_out.diff_1_diff_group_desc,
             diff_2_diff_group_desc          = g_rec_out.diff_2_diff_group_desc,
             diff_3_diff_group_desc          = g_rec_out.diff_3_diff_group_desc,
             diff_4_diff_group_desc          = g_rec_out.diff_4_diff_group_desc,
             diff_1_display_seq              = g_rec_out.diff_1_display_seq,
             diff_2_display_seq              = g_rec_out.diff_2_display_seq,
             diff_3_display_seq              = g_rec_out.diff_3_display_seq,
             diff_4_display_seq              = g_rec_out.diff_4_display_seq,
             item_aggr_ind                   = g_rec_out.item_aggr_ind,
             diff_1_aggr_ind                 = g_rec_out.diff_1_aggr_ind,
             diff_2_aggr_ind                 = g_rec_out.diff_2_aggr_ind,
             diff_3_aggr_ind                 = g_rec_out.diff_3_aggr_ind,
             diff_4_aggr_ind                 = g_rec_out.diff_4_aggr_ind,
             retail_zone_group_no            = g_rec_out.retail_zone_group_no,
             cost_zone_group_no              = g_rec_out.cost_zone_group_no,
             standard_uom_code               = g_rec_out.standard_uom_code,
             standard_uom_desc               = g_rec_out.standard_uom_desc,
             standard_uom_class_code         = g_rec_out.standard_uom_class_code,
             uom_conv_factor                 = g_rec_out.uom_conv_factor,
             package_size                    = g_rec_out.package_size,
             package_uom_code                = g_rec_out.package_uom_code,
             package_uom_desc                = g_rec_out.package_uom_desc,
             package_uom_class_code          = g_rec_out.package_uom_class_code,
             merchandise_item_ind            = g_rec_out.merchandise_item_ind,
             store_ord_mult_unit_type_code   = g_rec_out.store_ord_mult_unit_type_code,
             ext_sys_forecast_ind            = g_rec_out.ext_sys_forecast_ind,
             primary_currency_original_rsp   = g_rec_out.primary_currency_original_rsp,
             mfg_recommended_rsp             = g_rec_out.mfg_recommended_rsp,
             retail_label_type               = g_rec_out.retail_label_type,
             retail_label_value              = g_rec_out.retail_label_value,
             handling_temp_code              = g_rec_out.handling_temp_code,
             handling_sensitivity_code       = g_rec_out.handling_sensitivity_code,
             random_mass_ind                 = g_rec_out.random_mass_ind,
             first_received_date             = g_rec_out.first_received_date,
             last_received_date              = g_rec_out.last_received_date,
             most_recent_received_qty        = g_rec_out.most_recent_received_qty,
             waste_type                      = g_rec_out.waste_type,
             avg_waste_perc                  = g_rec_out.avg_waste_perc,
             default_waste_perc              = g_rec_out.default_waste_perc,
             constant_dimension_ind          = g_rec_out.constant_dimension_ind,
             pack_item_ind                   = g_rec_out.pack_item_ind,
             pack_item_simple_ind            = g_rec_out.pack_item_simple_ind,
             pack_item_inner_pack_ind        = g_rec_out.pack_item_inner_pack_ind,
             pack_item_sellable_unit_ind     = g_rec_out.pack_item_sellable_unit_ind,
             pack_item_orderable_ind         = g_rec_out.pack_item_orderable_ind,
             pack_item_type                  = g_rec_out.pack_item_type,
             pack_item_receivable_type       = g_rec_out.pack_item_receivable_type,
             item_comment                    = g_rec_out.item_comment,
             item_service_level_type         = g_rec_out.item_service_level_type,
             gift_wrap_ind                   = g_rec_out.gift_wrap_ind,
             ship_alone_ind                  = g_rec_out.ship_alone_ind,
             origin_item_ext_src_sys_name    = g_rec_out.origin_item_ext_src_sys_name,
             banded_item_ind                 = g_rec_out.banded_item_ind,
             static_mass                     = g_rec_out.static_mass,
             ext_ref_id                      = g_rec_out.ext_ref_id,
             create_date                     = g_rec_out.create_date,
             size_id                         = g_rec_out.size_id,
             color_id                        = g_rec_out.color_id,
             style_colour_no                 = g_rec_out.style_colour_no,
             style_no                        = g_rec_out.style_no,
             buying_ind                      = g_rec_out.buying_ind,
             selling_ind                     = g_rec_out.selling_ind,
             product_class                   = g_rec_out.product_class,
             fd_discipline_type              = g_rec_out.fd_discipline_type,
             handling_method_code            = g_rec_out.handling_method_code,
             handling_method_name            = g_rec_out.handling_method_name,
             display_method_code             = g_rec_out.display_method_code,
             display_method_name             = g_rec_out.display_method_name,
             tray_size_code                  = g_rec_out.tray_size_code,
             segregation_ind                 = g_rec_out.segregation_ind,
             outer_case_barcode              = g_rec_out.outer_case_barcode,
             rpl_merch_season_no             = g_rec_out.rpl_merch_season_no,
             prod_catg_code                  = g_rec_out.prod_catg_code,
             supp_comment                    = g_rec_out.supp_comment,
             rdf_forecst_ind                 = g_rec_out.rdf_forecst_ind,
             item_launch_date                = g_rec_out.item_launch_date,
             product_profile_code            = g_rec_out.product_profile_code,
             vat_rate_perc                   = g_rec_out.vat_rate_perc,
             sk1_supplier_no                 = g_rec_out.sk1_supplier_no,
             primary_supplier_no             = g_rec_out.primary_supplier_no,
             sk1_merch_season_phase_no       = g_rec_out.sk1_merch_season_phase_no,
             most_recent_merch_season_no     = g_rec_out.most_recent_merch_season_no,
             most_recent_merch_phase_no      = g_rec_out.most_recent_merch_phase_no,
             live_on_rms_date                = g_rec_out.live_on_rms_date,
             base_rsp                        = g_rec_out.base_rsp,
             base_rsp_excl_vat               = g_rec_out.base_rsp_excl_vat,
             vat_code                        = g_rec_out.vat_code,
             sk1_style_colour_no             = g_rec_out.sk1_style_colour_no,
             sk1_style_no                    = g_rec_out.sk1_style_no,
             supply_chain_type               = g_rec_out.supply_chain_type,
             rp_catlg_ind                    = g_rec_out.rp_catlg_ind,
             next_cost_price                 = g_rec_out.next_cost_price,
             next_cost_price_effective_date  = g_rec_out.next_cost_price_effective_date,
             rpl_ind_long_desc               = g_rec_out.rpl_ind_long_desc,
             display_subclass_name           = g_rec_out.display_subclass_name,
             display_class_name              = g_rec_out.display_class_name,
             display_group_name              = g_rec_out.display_group_name,
             multi_supplier_item_ind         = g_rec_out.multi_supplier_item_ind,
             sk1_diff_1_range_no             = g_rec_out.sk1_diff_1_range_no,
             store_scanned_order_ind	       = g_rec_out.store_scanned_order_ind,
             max_scanned_order_cases	       = g_rec_out.max_scanned_order_cases,
             last_updated_date               = g_rec_out.last_updated_date
      where  item_no                         = g_rec_out.item_no
      and    sk2_active_to_date              = dwh_constants.sk_to_date;

      g_recs_updated              := g_recs_updated + sql%rowcount;
   end if;

-- *************************************************************************************************
-- Update old versions of the same item with details not linked to SCD attributes
-- This avoids having different item names for history items and will be done as
-- required by the business
-- NOT REQUIRED BUSINESS SHOULD SEE HISTORY AS IT WAS
--   update dim_item_hist
--   set    item_name            = g_rec_out.item_name,
--          date_last_updated    = g_rec_out.date_last_updated
--   where  item_no              = g_rec_out.item_no and
--          sk2_active_to_date   <> dwh_constants.sk_to_date;

-- *************************************************************************************************

--   if sql%notfound then
--      dbms_output.put_line('No item to update');

  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' '||g_rec_out.item_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_out.item_no;
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

    l_text := 'LOAD OF ITEM MASTER SK2 VERSION STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
    for v_dim_item in c_dim_item
    loop
      g_recs_read := g_recs_read + 1;

      if g_recs_read mod 100000 = 0 then
         l_text := dwh_constants.vc_log_records_processed||
         to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      g_rec_in := v_dim_item;
      local_address_variable;
      local_write_output;

    end loop;

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
    p_success := true;

  exception
       when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_out.item_no;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_prf_corp_038u;
