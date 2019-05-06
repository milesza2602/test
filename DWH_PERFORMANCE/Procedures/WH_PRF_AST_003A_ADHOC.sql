--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_003A_ADHOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_003A_ADHOC" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2012
--  Author:      Alfonso Joshua
--  Purpose:     Load ASSORT UDA Dimension data in the performance layer
--               with data ex foundation layer tables.
--               This is the first step in 2 steps I will be following in loading
--               UDA data into the ASSORT UDA table.
--
--  1st Step:    Will load data from fnd_ast_sc_uda that does not exist in
--               dim_sc_uda into dim_ast_sc_uda.
--
--  2nd Step:    RMS (Realised data) WH_PRF_AST_003U
--
--  Tables:      Input  -   fnd_ast_sc_uda
--               Output -   dim_ast_sc_uda
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
--  g_  -  Global variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out                              dim_ast_sc_uda %rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_003A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_bam;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ASSORT PLACEHOLDER SC UDA EX Foundation';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_ast_sc_uda %rowtype index by binary_integer;
type tbl_array_u is table of dim_ast_sc_uda %rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- FROM GOOGLE
-- LENGTH(TRIM(TRANSLATE(string1, ' +-.0123456789', ' ')))
-- string1 is a string value being tested for numeric
-- This will return a null value if string1 is numeric.
-- It will return a value "greater than 0" if string1 contains any non-numeric characters.

cursor c_fnd_ast_sc_uda is
   select *
     from (
           select lev.sk1_style_colour_no,
                  bam_uda.style_colour_no,
                  bam_uda.uda_no,
                  nvl(bam_uda.uda_value_desc,'No Value') uda_desc
           from   fnd_ast_sc_uda bam_uda,
                  dim_ast_lev1_diff1 lev
--                  fnd_uda_value uda_val
           where  bam_uda.style_colour_no = lev.style_colour_no
            and   (length(trim(translate(bam_uda.uda_value_no_or_text_or_date,'+-.0123456789',' '))) is null)
            and   bam_uda.uda_no in (002,003,022,023,009,010,014,015,016,017,018,019,020,021,025,102,104,
                                     300,303,306,307,309,310,313,316,317,319,320,321,323,324,325,327,
                                     329,330,331,333,334,1002,1003,1103,1105,2301,2402,2601,3104,3202)
--            and   bam_uda.style_colour_no not in (select rms_uda.style_colour_no
--                                                  from   dim_sc_uda rms_uda)
            and   bam_uda.last_updated_date                       between '08 feb 15' and '12 feb 15')
--            and   to_number(bam_uda.uda_value_no_or_text_or_date) = uda_val.uda_value_no)
--            and   bam_uda.uda_no                                  = uda_val.uda_no)

        pivot(
              max(uda_desc) for uda_no in ( 002 as uda_002,
                                            003 as uda_003,
                                            009 as uda_009,
                                            010 as uda_010,
                                            014 as uda_014,
                                            015 as uda_015,
                                            016 as uda_016,
                                            017 as uda_017,
                                            018 as uda_018,
                                            019 as uda_019,
                                            020 as uda_020,
                                            021 as uda_021,
                                            022 as uda_022,
                                            023 as uda_023,
                                            025 as uda_025,
                                            102 as uda_102,
                                            104 as uda_104,
                                            300 as uda_300,
                                            303 as uda_303,
                                            306 as uda_306,
                                            307 as uda_307,
                                            309 as uda_309,
                                            310 as uda_310,
                                            313 as uda_313,
                                            316 as uda_316,
                                            317 as uda_317,
                                            319 as uda_319,
                                            320 as uda_320,
                                            321 as uda_321,
                                            323 as uda_323,
                                            324 as uda_324,
                                            325 as uda_325,
                                            327 as uda_327,
                                            329 as uda_329,
                                            330 as uda_330,
                                            331 as uda_331,
                                            333 as uda_333,
                                            334 as uda_334,
                                           1002 as uda_1002,
                                           1003 as uda_1003,
                                           1103 as uda_1103,
                                           1105 as uda_1105,
                                           2301 as uda_2301,
                                           2402 as uda_2402,
                                           2601 as uda_2601,
                                           3104 as uda_3104,
                                           3202 as uda_3202)
             );

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_ast_sc_uda%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_ast_sc_uda%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

     g_rec_out.sk1_style_colour_no             := g_rec_in.sk1_style_colour_no;
     g_rec_out.style_colour_no                 := g_rec_in.style_colour_no;
     g_rec_out.knit_woven_desc_002             := nvl(g_rec_in.uda_002,'No Value');
     g_rec_out.classic_highlight_desc_003      := nvl(g_rec_in.uda_003,'No Value');
     g_rec_out.good_business_journey_desc_009  := nvl(g_rec_in.uda_009,'No Value');
     g_rec_out.product_category_desc_010       := nvl(g_rec_in.uda_010,'No Value');
     g_rec_out.product_category_desc_014       := nvl(g_rec_in.uda_014,'No Value');
     g_rec_out.theme_desc_015                  := nvl(g_rec_in.uda_015,'No Value');
     g_rec_out.range_desc_016                  := nvl(g_rec_in.uda_016,'No Value');
     g_rec_out.variant_desc_017                := nvl(g_rec_in.uda_017,'No Value');
     g_rec_out.product_feature_desc_018        := nvl(g_rec_in.uda_018,'No Value');
     g_rec_out.diff_ranges_desc_019            := nvl(g_rec_in.uda_019,'No Value');
     g_rec_out.lifestyle_desc_020              := nvl(g_rec_in.uda_020,'No Value');
     g_rec_out.country_of_origin_desc_022      := nvl(g_rec_in.uda_022,'No Value');
     g_rec_out.source_strat_desc_023           := nvl(g_rec_in.uda_023,'No Value');
     g_rec_out.speed_to_market_desc_021        := nvl(g_rec_in.uda_021,'No Value');
     g_rec_out.product_category_desc_025       := nvl(g_rec_in.uda_025,'No Value');
     g_rec_out.character_desc_102              := nvl(g_rec_in.uda_102,'No Value');
     g_rec_out.range_structure_ch_desc_104     := nvl(g_rec_in.uda_104,'No Value');
     g_rec_out.cust_segmentation_desc_300      := nvl(g_rec_in.uda_300,'No Value');
     g_rec_out.garment_length_desc_303         := nvl(g_rec_in.uda_303,'No Value');
     g_rec_out.price_tier_desc_306             := nvl(g_rec_in.uda_306,'No Value');
     g_rec_out.print_type_desc_307             := nvl(g_rec_in.uda_307,'No Value');
     g_rec_out.top_vs_bottom_desc_309          := nvl(g_rec_in.uda_309,'No Value');
     g_rec_out.plain_vs_design_desc_310        := nvl(g_rec_in.uda_310,'No Value');
     g_rec_out.material_desc_313               := nvl(g_rec_in.uda_313,'No Value');
     g_rec_out.lifestyle_desc_316              := nvl(g_rec_in.uda_316,'No Value');
     g_rec_out.sleeve_length_desc_317          := nvl(g_rec_in.uda_317,'No Value');
     g_rec_out.single_multiple_desc_319        := nvl(g_rec_in.uda_319,'No Value');
     g_rec_out.sub_brands_desc_320             := nvl(g_rec_in.uda_320,'No Value');
     g_rec_out.shoe_heel_height_desc_321       := nvl(g_rec_in.uda_321,'No Value');
     g_rec_out.fit_desc_323                    := nvl(g_rec_in.uda_323,'No Value');
     g_rec_out.waist_drop_desc_324             := nvl(g_rec_in.uda_324,'No Value');
     g_rec_out.neck_line_desc_325              := nvl(g_rec_in.uda_325,'No Value');
     g_rec_out.silhouette_desc_327             := nvl(g_rec_in.uda_327,'No Value');
     g_rec_out.lighting_desc_329               := nvl(g_rec_in.uda_329,'No Value');
     g_rec_out.gender_desc_330                 := nvl(g_rec_in.uda_330,'No Value');
     g_rec_out.event_buy_desc_331              := nvl(g_rec_in.uda_331,'No Value');
     g_rec_out.product_category_desc_333       := nvl(g_rec_in.uda_333,'No Value');
     g_rec_out.fabric_type_desc_334            := nvl(g_rec_in.uda_334,'No Value');
     g_rec_out.fragrance_brand_desc_1002       := nvl(g_rec_in.uda_1002,'No Value');
     g_rec_out.fragrance_house_desc_1003       := nvl(g_rec_in.uda_1003,'No Value');
     g_rec_out.brand_type_desc_1103            := nvl(g_rec_in.uda_1103,'No Value');
     g_rec_out.brand_category_desc_1105        := nvl(g_rec_in.uda_1105,'No Value');
     g_rec_out.headqtr_assortment_desc_2301    := nvl(g_rec_in.uda_2301,'No Value');
     g_rec_out.handbag_sizing_desc_2601        := nvl(g_rec_in.uda_2601,'No Value');
     g_rec_out.shoe_heel_shape_desc_2402       := nvl(g_rec_in.uda_2402,'No Value');
     g_rec_out.product_defference_3104         := nvl(g_rec_in.uda_3104,'No Value');
     g_rec_out.great_value_desc_3202           := nvl(g_rec_in.uda_3202,'No Value');
     g_rec_out.last_updated_date               := g_date;
--JDA Assort Phase II
     g_rec_out.curtain_hang_method_desc_314    := 'No Value';
     g_rec_out.curtain_lining_desc_315         := 'No Value';
     g_rec_out.gifting_desc_326                := 'No Value';
     g_rec_out.merch_class_desc_100            := 'No Value';
     g_rec_out.range_segmentation_desc_322     := 'No Value';
     g_rec_out.royalties_desc_106              := 'No Value';
     g_rec_out.sock_length_desc_318            := 'No Value';
     g_rec_out.cellular_item_desc_904          := 'No Value';
     g_rec_out.product_sample_status_desc_304  := 'No Value';
     g_rec_out.utilise_ref_item_desc_508       := 'No Value';
     g_rec_out.price_marked_ind_desc_903       := 'No Value';
     g_rec_out.digital_brands_desc_1503        := 'No Value';
     g_rec_out.digital_offer_desc_1504         := 'No Value';
     g_rec_out.digital_price_bands_desc_1505   := 'No Value';
     g_rec_out.digital_design_desc_1506        := 'No Value';
     g_rec_out.digital_data_desc_1507          := 'No Value';
     g_rec_out.merch_usage_desc_1907           := 'No Value';
     g_rec_out.kids_age_desc_335               := 'No Value';
     g_rec_out.availability_desc_540           := 'No Value';
     g_rec_out.planning_manager_desc_561       := 'No Value';
     g_rec_out.commercial_manager_desc_562     := 'No Value';
     g_rec_out.merchandise_category_desc_100   := 'No Value';
     g_rec_out.product_class_desc_507          := 'No Value';
     g_rec_out.product_group_scaling_desc_501  := 'No Value';
     g_rec_out.organics_desc_550               := 'No Value';
     g_rec_out.kidz_desc_552                   := 'No Value';
     g_rec_out.free_range_desc_554             := 'No Value';
     g_rec_out.vegetarian_desc_551             := 'No Value';
     g_rec_out.slimmers_choice_desc_553        := 'No Value';
     g_rec_out.kosher_desc_555                 := 'No Value';
     g_rec_out.halaal_desc_556                 := 'No Value';
     g_rec_out.branded_item_desc_911           := 'No Value';
     g_rec_out.import_item_desc_1601           := 'No Value';
     g_rec_out.new_line_launch_date_desc_543   := 'No Value';
     g_rec_out.foods_range_structure_desc_560  := 'No Value';
     g_rec_out.food_main_shop_desc_2803        := 'No Value';
     g_rec_out.hot_item_desc_2804              := 'No Value';
     g_rec_out.beauty_brnd_prd_dtl_desc_2501   := 'No Value';
     g_rec_out.beauty_gift_range_desc_2503     := 'No Value';
     g_rec_out.planning_item_ind_2901          := 'No Value';
     g_rec_out.stock_error_ind_desc_699        := 'No Value';
     g_rec_out.product_grouping_desc_332       := 'No Value';
     g_rec_out.brand_gifting_desc_1104         := 'No Value';
     g_rec_out.marketing_use_only_desc_2102    := 'No Value';
     g_rec_out.digital_segmentation_desc_3001  := 'No Value';
     g_rec_out.digital_genre_desc_3002         := 'No Value';
     g_rec_out.loose_item_desc_910             := 'No Value';
     g_rec_out.variable_weight_item_desc_905   := 'No Value';
     g_rec_out.shorts_longlife_desc_542        := 'No Value';
     g_rec_out.new_line_indicator_desc_3502    := 'No Value';

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
      insert into dim_ast_sc_uda  values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).style_colour_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table.
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i IN a_tbl_update.FIRST .. a_tbl_update.LAST
      save EXCEPTIONS
      UPDATE dim_ast_sc_uda
         set style_colour_no                    = a_tbl_update(i).style_colour_no,
             knit_woven_desc_002                = a_tbl_update(i).knit_woven_desc_002,
             classic_highlight_desc_003         = a_tbl_update(i).classic_highlight_desc_003,
             good_business_journey_desc_009     = a_tbl_update(i).good_business_journey_desc_009,
             product_category_desc_010          = a_tbl_update(i).product_category_desc_010,
             product_category_desc_014          = a_tbl_update(i).product_category_desc_014,
             theme_desc_015                     = a_tbl_update(i).theme_desc_015,
             range_desc_016                     = a_tbl_update(i).range_desc_016,
             variant_desc_017                   = a_tbl_update(i).variant_desc_017,
             product_feature_desc_018           = a_tbl_update(i).product_feature_desc_018,
             diff_ranges_desc_019               = a_tbl_update(i).diff_ranges_desc_019,
             lifestyle_desc_020                 = a_tbl_update(i).lifestyle_desc_020,
             speed_to_market_desc_021           = a_tbl_update(i).speed_to_market_desc_021,
             country_of_origin_desc_022         = a_tbl_update(i).country_of_origin_desc_022,
             source_strat_desc_023              = a_tbl_update(i).source_strat_desc_023,
             product_category_desc_025          = a_tbl_update(i).product_category_desc_025,
             character_desc_102                 = a_tbl_update(i).character_desc_102,
             range_structure_ch_desc_104        = a_tbl_update(i).range_structure_ch_desc_104,
             cust_segmentation_desc_300         = a_tbl_update(i).cust_segmentation_desc_300,
             garment_length_desc_303            = a_tbl_update(i).garment_length_desc_303,
             price_tier_desc_306                = a_tbl_update(i).price_tier_desc_306,
             print_type_desc_307                = a_tbl_update(i).print_type_desc_307,
             top_vs_bottom_desc_309             = a_tbl_update(i).top_vs_bottom_desc_309,
             plain_vs_design_desc_310           = a_tbl_update(i).plain_vs_design_desc_310,
             material_desc_313                  = a_tbl_update(i).material_desc_313,
             lifestyle_desc_316                 = a_tbl_update(i).lifestyle_desc_316,
             sleeve_length_desc_317             = a_tbl_update(i).sleeve_length_desc_317,
             single_multiple_desc_319           = a_tbl_update(i).single_multiple_desc_319,
             sub_brands_desc_320                = a_tbl_update(i).sub_brands_desc_320,
             shoe_heel_height_desc_321          = a_tbl_update(i).shoe_heel_height_desc_321,
             fit_desc_323                       = a_tbl_update(i).fit_desc_323,
             waist_drop_desc_324                = a_tbl_update(i).waist_drop_desc_324,
             neck_line_desc_325                 = a_tbl_update(i).neck_line_desc_325,
             silhouette_desc_327                = a_tbl_update(i).silhouette_desc_327,
             lighting_desc_329                  = a_tbl_update(i).lighting_desc_329,
             gender_desc_330                    = a_tbl_update(i).gender_desc_330,
             event_buy_desc_331                 = a_tbl_update(i).event_buy_desc_331,
             product_category_desc_333          = a_tbl_update(i).product_category_desc_333,
             fabric_type_desc_334               = a_tbl_update(i).fabric_type_desc_334,
             fragrance_brand_desc_1002          = a_tbl_update(i).fragrance_brand_desc_1002,
             fragrance_house_desc_1003          = a_tbl_update(i).fragrance_house_desc_1003,
             brand_type_desc_1103               = a_tbl_update(i).brand_type_desc_1103,
             brand_category_desc_1105           = a_tbl_update(i).brand_category_desc_1105,
             headqtr_assortment_desc_2301       = a_tbl_update(i).headqtr_assortment_desc_2301,
             shoe_heel_shape_desc_2402          = a_tbl_update(i).shoe_heel_shape_desc_2402,
             handbag_sizing_desc_2601           = a_tbl_update(i).handbag_sizing_desc_2601,
             product_defference_3104            = a_tbl_update(i).product_defference_3104,
             great_value_desc_3202              = a_tbl_update(i).great_value_desc_3202,
             last_updated_date                  = a_tbl_update(i).last_updated_date,
-- JDA Assort Phase II
             curtain_hang_method_desc_314    = a_tbl_update(i).curtain_hang_method_desc_314,
             curtain_lining_desc_315         = a_tbl_update(i).curtain_lining_desc_315,
             gifting_desc_326                = a_tbl_update(i).gifting_desc_326,
             merch_class_desc_100            = a_tbl_update(i).merch_class_desc_100,
             range_segmentation_desc_322     = a_tbl_update(i).range_segmentation_desc_322,
             royalties_desc_106              = a_tbl_update(i).royalties_desc_106,
             sock_length_desc_318            = a_tbl_update(i).sock_length_desc_318,
             cellular_item_desc_904          = a_tbl_update(i).cellular_item_desc_904,
             product_sample_status_desc_304  = a_tbl_update(i).product_sample_status_desc_304,
             utilise_ref_item_desc_508       = a_tbl_update(i).utilise_ref_item_desc_508,
             price_marked_ind_desc_903       = a_tbl_update(i).price_marked_ind_desc_903,
             digital_brands_desc_1503        = a_tbl_update(i).digital_brands_desc_1503,
             digital_offer_desc_1504         = a_tbl_update(i).digital_offer_desc_1504,
             digital_price_bands_desc_1505   = a_tbl_update(i).digital_price_bands_desc_1505,
             digital_design_desc_1506        = a_tbl_update(i).digital_design_desc_1506,
             digital_data_desc_1507          = a_tbl_update(i).digital_data_desc_1507,
             merch_usage_desc_1907           = a_tbl_update(i).merch_usage_desc_1907,
             kids_age_desc_335               = a_tbl_update(i).kids_age_desc_335,
             availability_desc_540           = a_tbl_update(i).availability_desc_540,
             planning_manager_desc_561       = a_tbl_update(i).planning_manager_desc_561,
             commercial_manager_desc_562     = a_tbl_update(i).commercial_manager_desc_562,
             merchandise_category_desc_100   = a_tbl_update(i).merchandise_category_desc_100,
             product_class_desc_507          = a_tbl_update(i).product_class_desc_507,
             product_group_scaling_desc_501  = a_tbl_update(i).product_group_scaling_desc_501,
             organics_desc_550               = a_tbl_update(i).organics_desc_550,
             kidz_desc_552                   = a_tbl_update(i).kidz_desc_552,
             free_range_desc_554             = a_tbl_update(i).free_range_desc_554,
             vegetarian_desc_551             = a_tbl_update(i).vegetarian_desc_551,
             slimmers_choice_desc_553        = a_tbl_update(i).slimmers_choice_desc_553,
             kosher_desc_555                 = a_tbl_update(i).kosher_desc_555,
             halaal_desc_556                 = a_tbl_update(i).halaal_desc_556,
             branded_item_desc_911           = a_tbl_update(i).branded_item_desc_911,
             import_item_desc_1601           = a_tbl_update(i).import_item_desc_1601,
             new_line_launch_date_desc_543   = a_tbl_update(i).new_line_launch_date_desc_543,
             foods_range_structure_desc_560  = a_tbl_update(i).foods_range_structure_desc_560,
             food_main_shop_desc_2803        = a_tbl_update(i).food_main_shop_desc_2803,
             hot_item_desc_2804              = a_tbl_update(i).hot_item_desc_2804,
             beauty_brnd_prd_dtl_desc_2501   = a_tbl_update(i).beauty_brnd_prd_dtl_desc_2501,
             beauty_gift_range_desc_2503     = a_tbl_update(i).beauty_gift_range_desc_2503,
             planning_item_ind_2901          = a_tbl_update(i).planning_item_ind_2901,
             stock_error_ind_desc_699        = a_tbl_update(i).stock_error_ind_desc_699,
             product_grouping_desc_332       = a_tbl_update(i).product_grouping_desc_332,
             brand_gifting_desc_1104         = a_tbl_update(i).brand_gifting_desc_1104,
             marketing_use_only_desc_2102    = a_tbl_update(i).marketing_use_only_desc_2102,
             digital_segmentation_desc_3001  = a_tbl_update(i).digital_segmentation_desc_3001,
             digital_genre_desc_3002         = a_tbl_update(i).digital_genre_desc_3002,
             loose_item_desc_910             = a_tbl_update(i).loose_item_desc_910,
             variable_weight_item_desc_905   = a_tbl_update(i).variable_weight_item_desc_905,
             shorts_longlife_desc_542        = a_tbl_update(i).shorts_longlife_desc_542,
             new_line_indicator_desc_3502    = a_tbl_update(i).new_line_indicator_desc_3502
       where sk1_style_colour_no                    = a_tbl_update(i).sk1_style_colour_no;

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
                       ' '||a_tbl_update(g_error_index).style_colour_no;
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
     into g_count
     FROM dim_ast_sc_uda
    where sk1_style_colour_no = g_rec_out.sk1_style_colour_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Generate the SK1 number for the associated Natural number, then
-- Place the record into an array for later bulk writing.
   if not g_found then
--      g_rec_out.sk1_style_colour_no := merch_hierachy_seq.nextval;
      a_count_i                     := a_count_i + 1;
      a_tbl_insert(a_count_i)       := g_rec_out;
   else
      a_count_u                     := a_count_u + 1;
      a_tbl_update(a_count_u)       := g_rec_out;
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
    l_text := 'LOAD DIM_AST_SC_UDA EX FND_AST_SC_UDA STARTED AT '||
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

--    execute immediate 'truncate table dwh_performance.dim_ast_sc_uda';
--    l_text := 'TABLE temp_loc_item_dy_rdf_sysfcst TRUNCATED.';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_ast_sc_uda;
    fetch c_fnd_ast_sc_uda bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_ast_sc_uda bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_ast_sc_uda;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
      local_bulk_insert;
      local_bulk_update;
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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

end WH_PRF_AST_003A_ADHOC;
