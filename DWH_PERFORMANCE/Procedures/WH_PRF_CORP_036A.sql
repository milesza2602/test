--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_036A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_036A" 
                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Item master data table in the performance layer
--               with added value ex foundation layer Item mast and merch hierachy table.
--  Tables:      Input  - fnd_item, dim_subclass
--               Output - dim_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  18 Feb 2009 - defect 737 - Rename fields most_recent_MERCH_SEASON_NO
--                             and most_recent_MERCH_PHASE_NO to
--                             MOST_RECENT_MERCH_SEASON_NO and
--                             MOST_RECENT_MERCH_PHASE_NO
--                             on tables DIM_ITEM, DIM_ITEM_HIST
--                             and DIM_LEV1_DIFF1
--  18 March 2009 - defect 1153 - Replace NULLs with 'standard' values for some
--                                Performance layer Dimension table attributes.
--                              - columns are :
--                                 ITEM_UPPER_DESC (5589);
--                                 FD_PRODUCT_NO (5595);
--                                 DIFF_1_CODE;
--                                 DIFF_2_CODE;
--                                 DIFF_3_CODE;
--                                 DIFF_4_CODE;
--                                 STANDARD_UOM_CODE (5619);
--                                 STATIC_MASS (5657);
--                                 PRODUCT_CLASS (5666);
--                                 FD_DISCIPLINE_TYPE (5667);
--                                 HANDLING_METHOD_CODE (5668);
--                                 DISPLAY_METHOD_CODE (5670).
-- 3 April 2009 - defect 1181 - All cubes: Item level 1 Description not comming
--                             through : displaying as Item level 1 Description?
--                            - the update of these fields will be done in _036b
-- 30 April 2009 - defect 636 - Measures with a data type of text are causing.0
--                               issues in SSAS
-- 19 May 2009 - defect 508   - Cater for Intactix measures (display_subclass_name,display_class_name,display_group_name)
--
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
--
-- 26 June 2009 - Defect 1920 - Design issue - Incorrect vat percentage allocation based - changed vat_region_no = 1000 in select statement
--                              previous selection value was 1
-- 16 June 2010 - A nation in mourning - Bafana lost to 3 - 0 in the world cup - Ag Shame!!
-- 17 June 2010 - Defect 3851 - Add 2 new columns store scanned and max scanned.
-- 19 May 2011  - Defect 2981 - Add 2 new measures to table (min/max_shelf_life_tolerance)
--
-- 17 July 2014 -    --Chg 28028 : catered for product classes that were not being loaded correctly
--
-- 25 September 2014  - Chg 38403 : Removed the lookup to fnd_item_vat_rate to get most recent record by adding active_ind to the table
--
-- 20 February  2018  - MultiCurrency fields added:
--                        -> NEXT_COST_PRICE_LOCAL
--                           NEXT_COST_PRICE_OPR
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_item%rowtype;
g_rec_in             fnd_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_today_date         date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_036A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_ITEM EX FND_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of fnd_item%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_item%rowtype index by binary_integer;
type tbl_array_u is table of dim_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_item is
   select *
   from fnd_item;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.DIFF_1_CODE_DESC          := '- ';
   g_rec_out.DIFF_2_CODE_DESC          := '- ';
   g_rec_out.DIFF_3_CODE_DESC          := '- ';
   g_rec_out.DIFF_4_CODE_DESC          := '- ';
   g_rec_out.DIFF_1_DIFF_TYPE          := '- ';
   g_rec_out.DIFF_2_DIFF_TYPE          := '- ';
   g_rec_out.DIFF_3_DIFF_TYPE          := '- ';
   g_rec_out.DIFF_4_DIFF_TYPE          := '- ';
   g_rec_out.DIFF_1_TYPE_DESC          := '- ';
   g_rec_out.DIFF_2_TYPE_DESC          := '- ';
   g_rec_out.DIFF_3_TYPE_DESC          := '- ';
   g_rec_out.DIFF_4_TYPE_DESC          := '- ';
   g_rec_out.SK1_MERCH_SEASON_PHASE_NO := '0';
   g_rec_out.most_recent_MERCH_SEASON_NO  := '0';
   g_rec_out.most_recent_MERCH_PHASE_NO   := '0';
   g_rec_out.NEXT_COST_PRICE                := null;
   g_rec_out.NEXT_COST_PRICE_EFFECTIVE_DATE := null;
   
   g_rec_out.NEXT_COST_PRICE_LOCAL           := null;   --MC
   g_rec_out.NEXT_COST_PRICE_OPR             := null;   --MC

   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.item_desc                       := g_rec_in.item_desc;
   g_rec_out.item_short_desc                 := trim(g_rec_in.item_short_desc);
   g_rec_out.item_upper_desc                 := nvl((trim(g_rec_in.item_upper_desc)),'No Value');
   g_rec_out.item_scndry_desc                := g_rec_in.item_scndry_desc;
   g_rec_out.item_status_code                := g_rec_in.item_status_code;
   g_rec_out.item_level_no                   := g_rec_in.item_level_no;
   g_rec_out.tran_level_no                   := g_rec_in.tran_level_no;
   g_rec_out.primary_ref_item_ind            := g_rec_in.primary_ref_item_ind;
   g_rec_out.fd_product_no                   := nvl(g_rec_in.fd_product_no,0);
   g_rec_out.item_parent_no                  := g_rec_in.item_parent_no;
   g_rec_out.item_grandparent_no             := g_rec_in.item_grandparent_no;
   g_rec_out.item_level1_no                  := g_rec_in.item_level1_no;
   g_rec_out.item_level2_no                  := g_rec_in.item_level2_no;
   g_rec_out.subclass_no                     := g_rec_in.subclass_no;
   g_rec_out.class_no                        := g_rec_in.class_no;
   g_rec_out.department_no                   := g_rec_in.department_no;
   g_rec_out.rpl_ind                         := g_rec_in.rpl_ind;
   g_rec_out.item_no_type                    := g_rec_in.item_no_type;
   g_rec_out.format_id                       := g_rec_in.format_id;
   g_rec_out.upc_prefix_no                   := g_rec_in.upc_prefix_no;
   g_rec_out.diff_1_code                     :=  nvl(g_rec_in.diff_1_code, '-');
   g_rec_out.diff_2_code                     :=  nvl(g_rec_in.diff_2_code, '-');
   g_rec_out.diff_3_code                     :=  nvl(g_rec_in.diff_3_code, '-');
   g_rec_out.diff_4_code                     :=  nvl(g_rec_in.diff_4_code, '-');
   g_rec_out.item_aggr_ind                   := g_rec_in.item_aggr_ind;
   g_rec_out.diff_1_aggr_ind                 := g_rec_in.diff_1_aggr_ind;
   g_rec_out.diff_2_aggr_ind                 := g_rec_in.diff_2_aggr_ind;
   g_rec_out.diff_3_aggr_ind                 := g_rec_in.diff_3_aggr_ind;
   g_rec_out.diff_4_aggr_ind                 := g_rec_in.diff_4_aggr_ind;
   g_rec_out.retail_zone_group_no            := g_rec_in.retail_zone_group_no;
   g_rec_out.cost_zone_group_no              := g_rec_in.cost_zone_group_no;
   g_rec_out.standard_uom_code               := nvl(g_rec_in.standard_uom_code, '-');
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
   g_rec_out.static_mass                     := nvl(g_rec_in.static_mass,0);
   g_rec_out.ext_ref_id                      := g_rec_in.ext_ref_id;
   g_rec_out.create_date                     := g_rec_in.create_date;
   g_rec_out.size_id                         := g_rec_in.size_id;
   g_rec_out.color_id                        := g_rec_in.color_id;
   g_rec_out.style_colour_no                 := g_rec_in.style_colour_no;
   g_rec_out.style_no                        := g_rec_in.style_no;
   g_rec_out.buying_ind                      := g_rec_in.buying_ind;
   g_rec_out.selling_ind                     := g_rec_in.selling_ind;
   Case g_rec_in.product_class
        when '1' then g_rec_out.product_class := 1 ;
        when '2'  then g_rec_out.product_class := 2;
        when '3'  then g_rec_out.product_class := 3;
        when '4' then g_rec_out.product_class := 4;
        when '5' then g_rec_out.product_class := 5;
        when '6' then g_rec_out.product_class := 6;   --Chg 28028
        when '7' then g_rec_out.product_class := 7;   --Chg 28028
        when '8' then g_rec_out.product_class := 8;   --Chg 28028
        else g_rec_out.product_class := 0;
   end case;
   g_rec_out.fd_discipline_type              := nvl(g_rec_in.fd_discipline_type,'-');
   g_rec_out.handling_method_code            := nvl(g_rec_in.handling_method_code,'-');
   g_rec_out.handling_method_name            := g_rec_in.handling_method_name;
   g_rec_out.display_method_code             := nvl(g_rec_in.display_method_code,'-');
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
   g_rec_out.live_on_rms_date                := g_rec_in.live_on_rms_date;
   g_rec_out.primary_supplier_no             := nvl(g_rec_in.primary_supplier_no,0);
   g_rec_out.rp_catlg_ind                    := g_rec_in.rp_catlg_ind;
   g_rec_out.display_subclass_name           := g_rec_in.display_subclass_name;
   g_rec_out.display_class_name              := g_rec_in.display_class_name;
   g_rec_out.display_group_name              := g_rec_in.display_group_name;
--QC3851
   g_rec_out.store_scanned_order_ind	       := g_rec_in.store_scanned_order_ind;
   g_rec_out.max_scanned_order_cases	       := g_rec_in.max_scanned_order_cases;
   g_rec_out.min_shelf_life_tolerance        := g_rec_in.min_shelf_life_tolerance;
   g_rec_out.max_shelf_life_tolerance        := g_rec_in.max_shelf_life_tolerance;
   
   --Foods Renewal Fields
   g_rec_out.LOOSE_PROD_IND                  := g_rec_in.LOOSE_PROD_IND;
   g_rec_out.VAR_WEIGHT_IND                  := g_rec_in.VAR_WEIGHT_IND;

   g_rec_out.last_updated_date               := g_date;

   g_rec_out.tran_ind      := 0;
   g_rec_out.vat_code      := 'S';
   g_rec_out.vat_rate_perc := 0;
   g_rec_out.base_rsp      := 1;

   if g_rec_out.item_level_no = g_rec_out.tran_level_no then
      g_rec_out.tran_ind := 1;
   end if ;

   begin
     select vat_rate_perc, vat_code
       into g_rec_out.vat_rate_perc, g_rec_out.vat_code
       from fnd_item_vat_rate 
      where item_no          = g_rec_out.item_no 
        and vat_region_no    = 1000   
        and active_ind       = 1 
        and active_from_date <= g_date;
 
 -- code removed by adding an active indicator on the fnd_item_vat_rate table to remove the need for this lookup.
 -- removed on 25 September 2015
 --    active_from_date = (select max(active_from_date) from fnd_item_vat_rate b where active_from_date <= g_date 
 --                        and a.item_no = b.item_no 
 --                        and b.vat_region_no    = 1000) and

      exception
         when no_data_found then
           g_rec_out.vat_code      := 'S';
           g_rec_out.vat_rate_perc := 0;
   end;
   begin
      select sk1_supplier_no
      into   g_rec_out.sk1_supplier_no
      from   dim_supplier
      where  supplier_no          = g_rec_out.primary_supplier_no;

      exception
         when no_data_found then
           g_rec_out.sk1_supplier_no      := 0;
   end;
   begin
      select case when count(*) > 1 then 1 else 0 end
      into   g_rec_out.multi_supplier_item_ind
      from   rtl_item_supplier
      where  sk1_item_no = (select sk1_item_no from dim_item where item_no = g_rec_out.item_no);

      exception
         when no_data_found then
            g_rec_out.multi_supplier_item_ind := 0;
   end;
   begin
      select max(sk1_diff_range_no)
      into   g_rec_out.sk1_diff_1_range_no
      from   dim_diff_range
      where  diff_1_code = g_rec_in.diff_1_code;

      exception
         when no_data_found then
           g_rec_out.sk1_diff_1_range_no := 0;
   end;
   if g_rec_out.sk1_diff_1_range_no is null then
      g_rec_out.sk1_diff_1_range_no := 0;
   end if;
   begin
      select max(reg_rsp)
      into   g_rec_out.base_rsp
      from   fnd_zone_item
      where  item_no          = g_rec_out.item_no and
             base_retail_ind  = 1 ;

      exception
         when no_data_found then
           g_rec_out.base_rsp      := 1;
   end;
   g_rec_out.base_rsp_excl_vat := round(g_rec_out.base_rsp * 100 / (g_rec_out.vat_rate_perc + 100),2);

---------------------------------------------------------
-- Look up hierachy
---------------------------------------------------------
   dwh_lookup.dim_subclass_hierachy(g_rec_out.subclass_no,g_rec_out.class_no,g_rec_out.department_no,g_rec_out.subclass_name,g_rec_out.sk1_subclass_no,
                                         g_rec_out.class_name,g_rec_out.sk1_class_no,
                                         g_rec_out.department_name,g_rec_out.sk1_department_no,
                                         g_rec_out.subgroup_no,g_rec_out.subgroup_name,g_rec_out.sk1_subgroup_no,
                                         g_rec_out.group_no,g_rec_out.group_name,g_rec_out.sk1_group_no,
                                         g_rec_out.business_unit_no,g_rec_out.business_unit_name,g_rec_out.sk1_business_unit_no,
                                         g_rec_out.company_no,g_rec_out.company_name,g_rec_out.sk1_company_no);

 ---------------------------------------------------------
-- Future Cost
---------------------------------------------------------
-- select what exists now for comparison to what is coming in to see if it should get updated or not!
   begin
      select next_cost_price,next_cost_price_effective_date, 
             next_cost_price_local, next_cost_price_opr                                                     --MC
      into   g_rec_out.next_cost_price, g_rec_out.next_cost_price_effective_date,
             g_rec_out.next_cost_price_local, g_rec_out.next_cost_price_opr                                 --MC
      from   dim_item
      where  item_no = g_rec_out.item_no;
      exception
         when no_data_found then
           g_rec_out.next_cost_price                := null;
           g_rec_out.next_cost_price_effective_date := null;
           g_rec_out.next_cost_price_local          := null;                                                --MC
           g_rec_out.next_cost_price_opr            := null;                                                --MC
   end;

--   Once release then unblock this code to process future cost
   if g_rec_out.business_unit_no <> 50 and g_rec_in.next_cost_price_effective_date is not null then
      case  g_rec_out.item_status_code
         when 'A' then
            if g_rec_in.next_cost_price_effective_date  > g_date and
               (g_rec_out.next_cost_price_effective_date is null or
                g_rec_in.next_cost_price_effective_date  <= g_rec_out.next_cost_price_effective_date) then
                g_rec_out.next_cost_price                := g_rec_in.next_cost_price;
                g_rec_out.next_cost_price_effective_date := g_rec_in.next_cost_price_effective_date;
                g_rec_out.next_cost_price_local          := g_rec_in.next_cost_price_local;                 --MC
                g_rec_out.next_cost_price_opr            := g_rec_in.next_cost_price_opr;                   --MC
            end if;
         else
            g_rec_out.next_cost_price                    := null;
            g_rec_out.next_cost_price_effective_date     := null;
            g_rec_out.next_cost_price_local              := null;                                           --MC
            g_rec_out.next_cost_price_opr                := null;                                           --MC
      end case;
   end if;
---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------
   g_rec_out.item_long_desc             := g_rec_out.item_no||' - '||g_rec_out.item_upper_desc;
   --
   -- These descriptions are updated in wh_prf_corp_036b
   --g_rec_out.item_level1_desc           := 'ITEM LEVEL1 DESCRIPTION?';
   --g_rec_out.item_level1_long_desc      := g_rec_out.item_level1_no||' - '||g_rec_out.item_level1_desc;
   --
   g_rec_out.subclass_long_desc         := g_rec_out.subclass_no||' - '||g_rec_out.subclass_name;
   g_rec_out.class_long_desc            := g_rec_out.class_no||' - '||g_rec_out.class_name;
   g_rec_out.department_long_desc       := g_rec_out.department_no||' - '||g_rec_out.department_name;
   g_rec_out.subgroup_long_desc         := g_rec_out.subgroup_no||' - '||g_rec_out.subgroup_name;
   g_rec_out.group_long_desc            := g_rec_out.group_no||' - '||g_rec_out.group_name;
   g_rec_out.business_unit_long_desc    := g_rec_out.business_unit_no||' - '||g_rec_out.business_unit_name;
   g_rec_out.company_long_desc          := g_rec_out.company_no||' - '||g_rec_out.company_name;
   g_rec_out.total                      := 'TOTAL';
   g_rec_out.total_desc                 := 'ALL PRODUCT';
   if g_rec_out.rpl_ind = 1 then
      g_rec_out.rpl_ind_long_desc       := '1 - RPL';
   else
      g_rec_out.rpl_ind_long_desc       := '0 - FAST';
   end if;

   exception
      when others then
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
      insert into dim_item values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).item_no;
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
      update dim_item
      set    item_desc                       = a_tbl_update(i).item_desc,
             item_short_desc                 = a_tbl_update(i).item_short_desc,
             item_upper_desc                 = a_tbl_update(i).item_upper_desc,
             item_scndry_desc                = a_tbl_update(i).item_scndry_desc,
             sk1_subclass_no                 = a_tbl_update(i).sk1_subclass_no,
             subclass_no                     = a_tbl_update(i).subclass_no,
             subclass_name                   = a_tbl_update(i).subclass_name,
             sk1_class_no                    = a_tbl_update(i).sk1_class_no,
             class_no                        = a_tbl_update(i).class_no,
             class_name                      = a_tbl_update(i).class_name,
             sk1_department_no               = a_tbl_update(i).sk1_department_no,
             department_no                   = a_tbl_update(i).department_no,
             department_name                 = a_tbl_update(i).department_name,
             sk1_subgroup_no                 = a_tbl_update(i).sk1_subgroup_no,
             subgroup_no                     = a_tbl_update(i).subgroup_no,
             subgroup_name                   = a_tbl_update(i).subgroup_name,
             sk1_group_no                    = a_tbl_update(i).sk1_group_no,
             group_no                        = a_tbl_update(i).group_no,
             group_name                      = a_tbl_update(i).group_name,
             sk1_business_unit_no            = a_tbl_update(i).sk1_business_unit_no,
             business_unit_no                = a_tbl_update(i).business_unit_no,
             business_unit_name              = a_tbl_update(i).business_unit_name,
             sk1_company_no                  = a_tbl_update(i).sk1_company_no,
             company_no                      = a_tbl_update(i).company_no,
             company_name                    = a_tbl_update(i).company_name,
             item_status_code                = a_tbl_update(i).item_status_code,
             item_level_no                   = a_tbl_update(i).item_level_no,
             tran_level_no                   = a_tbl_update(i).tran_level_no,
             tran_ind                        = a_tbl_update(i).tran_ind,
             primary_ref_item_ind            = a_tbl_update(i).primary_ref_item_ind,
             fd_product_no                   = a_tbl_update(i).fd_product_no,
             item_parent_no                  = a_tbl_update(i).item_parent_no,
             item_grandparent_no             = a_tbl_update(i).item_grandparent_no,
             item_level1_no                  = a_tbl_update(i).item_level1_no,
             item_level2_no                  = a_tbl_update(i).item_level2_no,
             rpl_ind                         = a_tbl_update(i).rpl_ind,
             item_no_type                    = a_tbl_update(i).item_no_type,
             format_id                       = a_tbl_update(i).format_id,
             upc_prefix_no                   = a_tbl_update(i).upc_prefix_no,
             diff_1_code                     = a_tbl_update(i).diff_1_code,
             diff_2_code                     = a_tbl_update(i).diff_2_code,
             diff_3_code                     = a_tbl_update(i).diff_3_code,
             diff_4_code                     = a_tbl_update(i).diff_4_code,
             item_aggr_ind                   = a_tbl_update(i).item_aggr_ind,
             diff_1_aggr_ind                 = a_tbl_update(i).diff_1_aggr_ind,
             diff_2_aggr_ind                 = a_tbl_update(i).diff_2_aggr_ind,
             diff_3_aggr_ind                 = a_tbl_update(i).diff_3_aggr_ind,
             diff_4_aggr_ind                 = a_tbl_update(i).diff_4_aggr_ind,
             retail_zone_group_no            = a_tbl_update(i).retail_zone_group_no,
             cost_zone_group_no              = a_tbl_update(i).cost_zone_group_no,
             standard_uom_code               = a_tbl_update(i).standard_uom_code,
             standard_uom_desc               = a_tbl_update(i).standard_uom_desc,
             standard_uom_class_code         = a_tbl_update(i).standard_uom_class_code,
             uom_conv_factor                 = a_tbl_update(i).uom_conv_factor,
             package_size                    = a_tbl_update(i).package_size,
             package_uom_code                = a_tbl_update(i).package_uom_code,
             package_uom_desc                = a_tbl_update(i).package_uom_desc,
             package_uom_class_code          = a_tbl_update(i).package_uom_class_code,
             merchandise_item_ind            = a_tbl_update(i).merchandise_item_ind,
             store_ord_mult_unit_type_code   = a_tbl_update(i).store_ord_mult_unit_type_code,
             ext_sys_forecast_ind            = a_tbl_update(i).ext_sys_forecast_ind,
             primary_currency_original_rsp   = a_tbl_update(i).primary_currency_original_rsp,
             mfg_recommended_rsp             = a_tbl_update(i).mfg_recommended_rsp,
             retail_label_type               = a_tbl_update(i).retail_label_type,
             retail_label_value              = a_tbl_update(i).retail_label_value,
             handling_temp_code              = a_tbl_update(i).handling_temp_code,
             handling_sensitivity_code       = a_tbl_update(i).handling_sensitivity_code,
             random_mass_ind                 = a_tbl_update(i).random_mass_ind,
             first_received_date             = a_tbl_update(i).first_received_date,
             last_received_date              = a_tbl_update(i).last_received_date,
             most_recent_received_qty        = a_tbl_update(i).most_recent_received_qty,
             waste_type                      = a_tbl_update(i).waste_type,
             avg_waste_perc                  = a_tbl_update(i).avg_waste_perc,
             default_waste_perc              = a_tbl_update(i).default_waste_perc,
             constant_dimension_ind          = a_tbl_update(i).constant_dimension_ind,
             pack_item_ind                   = a_tbl_update(i).pack_item_ind,
             pack_item_simple_ind            = a_tbl_update(i).pack_item_simple_ind,
             pack_item_inner_pack_ind        = a_tbl_update(i).pack_item_inner_pack_ind,
             pack_item_sellable_unit_ind     = a_tbl_update(i).pack_item_sellable_unit_ind,
             pack_item_orderable_ind         = a_tbl_update(i).pack_item_orderable_ind,
             pack_item_type                  = a_tbl_update(i).pack_item_type,
             pack_item_receivable_type       = a_tbl_update(i).pack_item_receivable_type,
             item_comment                    = a_tbl_update(i).item_comment,
             item_service_level_type         = a_tbl_update(i).item_service_level_type,
             gift_wrap_ind                   = a_tbl_update(i).gift_wrap_ind,
             ship_alone_ind                  = a_tbl_update(i).ship_alone_ind,
             origin_item_ext_src_sys_name    = a_tbl_update(i).origin_item_ext_src_sys_name,
             banded_item_ind                 = a_tbl_update(i).banded_item_ind,
             static_mass                     = a_tbl_update(i).static_mass,
             ext_ref_id                      = a_tbl_update(i).ext_ref_id,
             create_date                     = a_tbl_update(i).create_date,
             size_id                         = a_tbl_update(i).size_id,
             color_id                        = a_tbl_update(i).color_id,
             style_colour_no                 = a_tbl_update(i).style_colour_no,
             style_no                        = a_tbl_update(i).style_no,
             buying_ind                      = a_tbl_update(i).buying_ind,
             selling_ind                     = a_tbl_update(i).selling_ind,
             product_class                   = a_tbl_update(i).product_class,
             fd_discipline_type              = a_tbl_update(i).fd_discipline_type,
             handling_method_code            = a_tbl_update(i).handling_method_code,
             handling_method_name            = a_tbl_update(i).handling_method_name,
             display_method_code             = a_tbl_update(i).display_method_code,
             display_method_name             = a_tbl_update(i).display_method_name,
             tray_size_code                  = a_tbl_update(i).tray_size_code,
             segregation_ind                 = a_tbl_update(i).segregation_ind,
             outer_case_barcode              = a_tbl_update(i).outer_case_barcode,
             rpl_merch_season_no             = a_tbl_update(i).rpl_merch_season_no,
             prod_catg_code                  = a_tbl_update(i).prod_catg_code,
             supp_comment                    = a_tbl_update(i).supp_comment,
             rdf_forecst_ind                 = a_tbl_update(i).rdf_forecst_ind,
             item_launch_date                = a_tbl_update(i).item_launch_date,
             product_profile_code            = a_tbl_update(i).product_profile_code,
             vat_rate_perc                   = a_tbl_update(i).vat_rate_perc,
             sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no,
             primary_supplier_no             = a_tbl_update(i).primary_supplier_no,
             item_long_desc                  = a_tbl_update(i).item_long_desc,
             subclass_long_desc              = a_tbl_update(i).subclass_long_desc,
             class_long_desc                 = a_tbl_update(i).class_long_desc,
             department_long_desc            = a_tbl_update(i).department_long_desc,
             subgroup_long_desc              = a_tbl_update(i).subgroup_long_desc,
             group_long_desc                 = a_tbl_update(i).group_long_desc,
             business_unit_long_desc         = a_tbl_update(i).business_unit_long_desc,
             company_long_desc               = a_tbl_update(i).company_long_desc,
             total                           = a_tbl_update(i).total,
             total_desc                      = a_tbl_update(i).total_desc,
             live_on_rms_date                = a_tbl_update(i).live_on_rms_date,
             base_rsp                        = a_tbl_update(i).base_rsp,
             base_rsp_excl_vat               = a_tbl_update(i).base_rsp_excl_vat,
             vat_code                        = a_tbl_update(i).vat_code,
             next_cost_price                 = a_tbl_update(i).next_cost_price,
             next_cost_price_effective_date  = a_tbl_update(i).next_cost_price_effective_date,
             rpl_ind_long_desc               = a_tbl_update(i).rpl_ind_long_desc,
             rp_catlg_ind                    = a_tbl_update(i).rp_catlg_ind,
             display_subclass_name           = a_tbl_update(i).display_subclass_name,
             display_class_name              = a_tbl_update(i).display_class_name,
             display_group_name              = a_tbl_update(i).display_group_name,
             last_updated_date               = a_tbl_update(i).last_updated_date,
             multi_supplier_item_ind         = a_tbl_update(i).multi_supplier_item_ind,
             sk1_diff_1_range_no             = a_tbl_update(i).sk1_diff_1_range_no,
             store_scanned_order_ind	       = a_tbl_update(i).store_scanned_order_ind,
             max_scanned_order_cases	       = a_tbl_update(i).max_scanned_order_cases,
             min_shelf_life_tolerance        = a_tbl_update(i).min_shelf_life_tolerance,
             max_shelf_life_tolerance        = a_tbl_update(i).max_shelf_life_tolerance,
             loose_prod_ind                  = a_tbl_update(i).loose_prod_ind,            --Foods Renewal
             var_weight_ind                  = a_tbl_update(i).var_weight_ind,            --Foods Renewal
             next_cost_price_local           = a_tbl_update(i).next_cost_price_local,     --MC
             next_cost_price_opr             = a_tbl_update(i).next_cost_price_opr        --MC
      where  item_no                         = a_tbl_update(i).item_no;

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
                       ' '||a_tbl_update(g_error_index).item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := dwh_valid.dim_item(g_rec_out.item_no);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_item_no  := merch_hierachy_seq.nextval;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF DIM_ITEM EX FND_ITEM STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- reset future cost prices where < today
--**************************************************************************************************
    l_text := 'SET FUTURE COST TO NULLS WHERE EFFECTIVE DATE < TODAY BATCH DATE - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    update /*+ PARALLEL(a,6) */ dim_item a
    set    a.NEXT_COST_PRICE                = null,
           a.NEXT_COST_PRICE_EFFECTIVE_DATE = null,
           a.NEXT_COST_PRICE_LOCAL          = null,         --MC
           a.NEXT_COST_PRICE_OPR            = null          --MC
    where  a.NEXT_COST_PRICE_EFFECTIVE_DATE <= g_date and
           a.business_unit_no               <> 50;
    commit;

--**************************************************************************************************
    open c_fnd_item;
    fetch c_fnd_item bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_item;
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

end wh_prf_corp_036a;
