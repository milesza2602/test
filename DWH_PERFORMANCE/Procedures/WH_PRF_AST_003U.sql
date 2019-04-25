--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_003U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_003U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date     :  July 2012
--  Author   :  Alfonso Joshua
--  Purpose  :  Load ASSORT UDA Dimension data in the performance layer
--              with data ex foundation layer tables.
--              This is the Second step of the 2 steps required in loading
--              UDA data into the JDA UDA table.
--
-- 1st Step  :  See a program called WH_PRF_AST_003A - Placeholder Data Load
--
--  2nd Step :  From the Current Procedure, will load data from dim_sc_uda that
--              does not exist in dim_ast_sc_uda into dim_ast_sc_uda.
--
-- 29 Dec 2017 - A. Ugolini  - Add field WEARING_SEASON_DESC_7501 on DIM_AST_SC_UDA
--
--  Tables   :  Input  -   dim_sc_uda, dim_ast_sc_uda
--              Output -   dim_ast_sc_uda
--  Packages :  constants, dwh_log, dwh_valid
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
g_rec_out                dim_ast_sc_uda %rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_003U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_bam;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD REALISED ASSORT UDA EX RMS';
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

cursor c_dim_sc_uda is
        select
              rms.sk1_style_colour_no,
              rms.style_colour_no,
              rms.character_desc_102,
              rms.range_structure_ch_desc_104,
              rms.cust_segmentation_desc_300,
              rms.garment_length_desc_303,
              rms.price_tier_desc_306,
              rms.print_type_desc_307,
              rms.top_vs_bottom_desc_309,
              rms.plain_vs_design_desc_310,
              rms.material_desc_313,
              rms.lifestyle_desc_316,
              rms.sleeve_length_desc_317,
              rms.single_multiple_desc_319,
              rms.sub_brands_desc_320,
              rms.shoe_heel_height_desc_321,
              rms.fit_desc_323,
              rms.waist_drop_desc_324,
              rms.neck_line_desc_325,
              rms.silhouette_desc_327,
              rms.lighting_desc_329,
              rms.gender_desc_330,
              rms.event_buy_desc_331,
              rms.product_category_desc_333,
              rms.fabric_type_desc_334,
              rms.fragrance_brand_desc_1002,
              rms.fragrance_house_desc_1003,
              rms.brand_type_desc_1103,
              rms.brand_category_desc_1105,
              rms.headqtr_assortment_desc_2301,
              rms.shoe_heel_shape_desc_2402,
              rms.handbag_sizing_desc_2601,
              rms.great_value_desc_3202,
              rms.product_defference_3104,
              rms.wearing_season_desc_7501,
-- added for JDA Phase II
              rms.curtain_hang_method_desc_314,
              rms.curtain_lining_desc_315,
              rms.gifting_desc_326,
              rms.merch_class_desc_100,
              rms.range_segmentation_desc_322,
              rms.royalties_desc_106,
              rms.sock_length_desc_318,
              rms.cellular_item_desc_904,
              rms.product_sample_status_desc_304,
              rms.utilise_ref_item_desc_508,
              rms.price_marked_ind_desc_903,
              rms.digital_brands_desc_1503,
              rms.digital_offer_desc_1504,
              rms.digital_price_bands_desc_1505,
              rms.digital_design_desc_1506,
              rms.digital_data_desc_1507,
              rms.merch_usage_desc_1907,
              rms.kids_age_desc_335,
              rms.availability_desc_540,
              rms.planning_manager_desc_561,
              rms.commercial_manager_desc_562,
              rms.merchandise_category_desc_100,
              rms.product_class_desc_507,
              rms.product_group_scaling_desc_501,
              rms.organics_desc_550,
              rms.kidz_desc_552,
              rms.free_range_desc_554,
              rms.vegetarian_desc_551,
              rms.slimmers_choice_desc_553,
              rms.kosher_desc_555,
              rms.halaal_desc_556,
              rms.branded_item_desc_911,
              rms.import_item_desc_1601,
              rms.new_line_launch_date_desc_543,
              rms.foods_range_structure_desc_560,
              rms.food_main_shop_desc_2803,
              rms.hot_item_desc_2804,
              rms.beauty_brnd_prd_dtl_desc_2501,
              rms.beauty_gift_range_desc_2503,
              rms.planning_item_ind_2901,
              rms.stock_error_ind_desc_699,
              rms.product_grouping_desc_332,
              rms.brand_gifting_desc_1104,
              rms.marketing_use_only_desc_2102,
              rms.digital_segmentation_desc_3001,
              rms.digital_genre_desc_3002,
              rms.loose_item_desc_910,
              rms.variable_weight_item_desc_905,
              rms.shorts_longlife_desc_542,
              rms.new_line_indicator_desc_3502,
              ast.knit_woven_desc_002        ,
              ast.classic_highlight_desc_003      ,
              ast.good_business_journey_desc_009  ,
              ast.product_category_desc_010       ,
              ast.product_category_desc_014       ,
              ast.theme_desc_015                  ,
              ast.range_desc_016                  ,
              ast.variant_desc_017                ,
              ast.product_feature_desc_018        ,
              ast.diff_ranges_desc_019            ,
              ast.lifestyle_desc_020              ,
              ast.speed_to_market_desc_021        ,
              ast.country_of_origin_desc_022      ,
              ast.source_strat_desc_023,
              ast.product_category_desc_025
        from  dim_sc_uda rms
         left outer join dim_ast_sc_uda ast          on rms.sk1_style_colour_no          = ast.sk1_style_colour_no;
--         and                                            substr(ast.style_colour_no,1,2) <> 99;


-- Input record declared as cursor%rowtype
g_rec_in             c_dim_sc_uda%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_dim_sc_uda%rowtype;
a_stg_input      stg_array;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

     g_rec_out.sk1_style_colour_no             := g_rec_in.sk1_style_colour_no;
     g_rec_out.style_colour_no                 := g_rec_in.style_colour_no;
     g_rec_out.knit_woven_desc_002             := g_rec_in.knit_woven_desc_002;
     g_rec_out.classic_highlight_desc_003      := g_rec_in.classic_highlight_desc_003;
     g_rec_out.good_business_journey_desc_009  := g_rec_in.good_business_journey_desc_009;
     g_rec_out.product_category_desc_010       := g_rec_in.product_category_desc_010;
     g_rec_out.product_category_desc_014       := g_rec_in.product_category_desc_014;
     g_rec_out.theme_desc_015                  := g_rec_in.theme_desc_015;
     g_rec_out.range_desc_016                  := g_rec_in.range_desc_016;
     g_rec_out.variant_desc_017                := g_rec_in.variant_desc_017;
     g_rec_out.product_feature_desc_018        := g_rec_in.product_feature_desc_018;
     g_rec_out.diff_ranges_desc_019            := g_rec_in.diff_ranges_desc_019;
     g_rec_out.lifestyle_desc_020              := g_rec_in.lifestyle_desc_020;
     g_rec_out.speed_to_market_desc_021        := g_rec_in.speed_to_market_desc_021;
     g_rec_out.country_of_origin_desc_022      := g_rec_in.country_of_origin_desc_022;
     g_rec_out.source_strat_desc_023           := g_rec_in.source_strat_desc_023;
     g_rec_out.product_category_desc_025       := g_rec_in.product_category_desc_025;
     g_rec_out.character_desc_102              := g_rec_in.character_desc_102;
     g_rec_out.range_structure_ch_desc_104     := g_rec_in.range_structure_ch_desc_104;
     g_rec_out.cust_segmentation_desc_300      := g_rec_in.cust_segmentation_desc_300;
     g_rec_out.garment_length_desc_303         := g_rec_in.garment_length_desc_303;
     g_rec_out.price_tier_desc_306             := g_rec_in.price_tier_desc_306;
     g_rec_out.print_type_desc_307             := g_rec_in.print_type_desc_307;
     g_rec_out.top_vs_bottom_desc_309          := g_rec_in.top_vs_bottom_desc_309 ;
     g_rec_out.plain_vs_design_desc_310        := g_rec_in.plain_vs_design_desc_310;
     g_rec_out.material_desc_313               := g_rec_in.material_desc_313;
     g_rec_out.lifestyle_desc_316              := g_rec_in.lifestyle_desc_316;
     g_rec_out.sleeve_length_desc_317          := g_rec_in.sleeve_length_desc_317 ;
     g_rec_out.single_multiple_desc_319        := g_rec_in.single_multiple_desc_319;
     g_rec_out.sub_brands_desc_320             := g_rec_in.sub_brands_desc_320 ;
     g_rec_out.shoe_heel_height_desc_321       := g_rec_in.shoe_heel_height_desc_321 ;
     g_rec_out.fit_desc_323                    := g_rec_in.fit_desc_323;
     g_rec_out.waist_drop_desc_324             := g_rec_in.waist_drop_desc_324;
     g_rec_out.neck_line_desc_325              := g_rec_in.neck_line_desc_325;
     g_rec_out.silhouette_desc_327             := g_rec_in.silhouette_desc_327;
     g_rec_out.lighting_desc_329               := g_rec_in.lighting_desc_329;
     g_rec_out.gender_desc_330                 := g_rec_in.gender_desc_330 ;
     g_rec_out.event_buy_desc_331              := g_rec_in.event_buy_desc_331;
     g_rec_out.product_category_desc_333       := g_rec_in.product_category_desc_333;
     g_rec_out.fabric_type_desc_334            := g_rec_in.fabric_type_desc_334;
     g_rec_out.fragrance_brand_desc_1002       := g_rec_in.fragrance_brand_desc_1002;
     g_rec_out.fragrance_house_desc_1003       := g_rec_in.fragrance_house_desc_1003;
     g_rec_out.brand_type_desc_1103            := g_rec_in.brand_type_desc_1103;
     g_rec_out.brand_category_desc_1105        := g_rec_in.brand_category_desc_1105;
     g_rec_out.headqtr_assortment_desc_2301    := g_rec_in.headqtr_assortment_desc_2301;
     g_rec_out.shoe_heel_shape_desc_2402       := g_rec_in.shoe_heel_shape_desc_2402;
     g_rec_out.handbag_sizing_desc_2601        := g_rec_in.handbag_sizing_desc_2601;
     g_rec_out.product_defference_3104         := g_rec_in.product_defference_3104;
     g_rec_out.great_value_desc_3202           := g_rec_in.great_value_desc_3202;
     g_rec_out.wearing_season_desc_7501        := g_rec_in.wearing_season_desc_7501;
     g_rec_out.last_updated_date               := g_date;
-- JDA Phase II
     g_rec_out.curtain_hang_method_desc_314    := g_rec_in.curtain_hang_method_desc_314;
     g_rec_out.curtain_lining_desc_315         := g_rec_in.curtain_lining_desc_315;
     g_rec_out.gifting_desc_326                := g_rec_in.gifting_desc_326;
     g_rec_out.merch_class_desc_100            := g_rec_in.merch_class_desc_100;
     g_rec_out.range_segmentation_desc_322     := g_rec_in.range_segmentation_desc_322;
     g_rec_out.royalties_desc_106              := g_rec_in.royalties_desc_106;
     g_rec_out.sock_length_desc_318            := g_rec_in.sock_length_desc_318;
     g_rec_out.cellular_item_desc_904          := g_rec_in.cellular_item_desc_904;
     g_rec_out.product_sample_status_desc_304  := g_rec_in.product_sample_status_desc_304;
     g_rec_out.utilise_ref_item_desc_508       := g_rec_in.utilise_ref_item_desc_508;
     g_rec_out.price_marked_ind_desc_903       := g_rec_in.price_marked_ind_desc_903;
     g_rec_out.digital_brands_desc_1503        := g_rec_in.digital_brands_desc_1503;
     g_rec_out.digital_offer_desc_1504         := g_rec_in.digital_offer_desc_1504;
     g_rec_out.digital_price_bands_desc_1505   := g_rec_in.digital_price_bands_desc_1505;
     g_rec_out.digital_design_desc_1506        := g_rec_in.digital_design_desc_1506;
     g_rec_out.digital_data_desc_1507          := g_rec_in.digital_data_desc_1507;
     g_rec_out.merch_usage_desc_1907           := g_rec_in.merch_usage_desc_1907;
     g_rec_out.kids_age_desc_335               := g_rec_in.kids_age_desc_335;
     g_rec_out.availability_desc_540           := g_rec_in.availability_desc_540;
     g_rec_out.planning_manager_desc_561       := g_rec_in.planning_manager_desc_561;
     g_rec_out.commercial_manager_desc_562     := g_rec_in.commercial_manager_desc_562;
     g_rec_out.merchandise_category_desc_100   := g_rec_in.merchandise_category_desc_100;
     g_rec_out.product_class_desc_507          := g_rec_in.product_class_desc_507;
     g_rec_out.product_group_scaling_desc_501  := g_rec_in.product_group_scaling_desc_501;
     g_rec_out.organics_desc_550               := g_rec_in.organics_desc_550;
     g_rec_out.kidz_desc_552                   := g_rec_in.kidz_desc_552;
     g_rec_out.free_range_desc_554             := g_rec_in.free_range_desc_554;
     g_rec_out.vegetarian_desc_551             := g_rec_in.vegetarian_desc_551;
     g_rec_out.slimmers_choice_desc_553        := g_rec_in.slimmers_choice_desc_553;
     g_rec_out.kosher_desc_555                 := g_rec_in.kosher_desc_555;
     g_rec_out.halaal_desc_556                 := g_rec_in.halaal_desc_556;
     g_rec_out.branded_item_desc_911           := g_rec_in.branded_item_desc_911;
     g_rec_out.import_item_desc_1601           := g_rec_in.import_item_desc_1601;
     g_rec_out.new_line_launch_date_desc_543   := g_rec_in.new_line_launch_date_desc_543;
     g_rec_out.foods_range_structure_desc_560  := g_rec_in.foods_range_structure_desc_560;
     g_rec_out.food_main_shop_desc_2803        := g_rec_in.food_main_shop_desc_2803;
     g_rec_out.hot_item_desc_2804              := g_rec_in.hot_item_desc_2804;
     g_rec_out.beauty_brnd_prd_dtl_desc_2501   := g_rec_in.beauty_brnd_prd_dtl_desc_2501;
     g_rec_out.beauty_gift_range_desc_2503     := g_rec_in.beauty_gift_range_desc_2503;
     g_rec_out.planning_item_ind_2901          := g_rec_in.planning_item_ind_2901;
     g_rec_out.stock_error_ind_desc_699        := g_rec_in.stock_error_ind_desc_699;
     g_rec_out.product_grouping_desc_332       := g_rec_in.product_grouping_desc_332;
     g_rec_out.brand_gifting_desc_1104         := g_rec_in.brand_gifting_desc_1104;
     g_rec_out.marketing_use_only_desc_2102    := g_rec_in.marketing_use_only_desc_2102;
     g_rec_out.digital_segmentation_desc_3001  := g_rec_in.digital_segmentation_desc_3001;
     g_rec_out.digital_genre_desc_3002         := g_rec_in.digital_genre_desc_3002;
     g_rec_out.loose_item_desc_910             := g_rec_in.loose_item_desc_910;
     g_rec_out.variable_weight_item_desc_905   := g_rec_in.variable_weight_item_desc_905;
     g_rec_out.shorts_longlife_desc_542        := g_rec_in.shorts_longlife_desc_542;
     g_rec_out.new_line_indicator_desc_3502    := g_rec_in.new_line_indicator_desc_3502;


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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no;
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
      update dim_ast_sc_uda
         set style_colour_no                 = a_tbl_update(i).style_colour_no,
             knit_woven_desc_002             = a_tbl_update(i).knit_woven_desc_002,
             classic_highlight_desc_003      = a_tbl_update(i).classic_highlight_desc_003,
             good_business_journey_desc_009  = a_tbl_update(i).good_business_journey_desc_009,
             product_category_desc_010       = a_tbl_update(i).product_category_desc_010,
             product_category_desc_014       = a_tbl_update(i).product_category_desc_014,
             theme_desc_015                  = a_tbl_update(i).theme_desc_015,
             range_desc_016                  = a_tbl_update(i).range_desc_016,
             variant_desc_017                = a_tbl_update(i).variant_desc_017,
             product_feature_desc_018        = a_tbl_update(i).product_feature_desc_018,
             diff_ranges_desc_019            = a_tbl_update(i).diff_ranges_desc_019,
             lifestyle_desc_020              = a_tbl_update(i).lifestyle_desc_020,
             speed_to_market_desc_021        = a_tbl_update(i).speed_to_market_desc_021,
             country_of_origin_desc_022      = a_tbl_update(i).country_of_origin_desc_022,
             source_strat_desc_023           = a_tbl_update(i).source_strat_desc_023,
             product_category_desc_025       = a_tbl_update(i).product_category_desc_025,
             character_desc_102              = a_tbl_update(i).character_desc_102,
             range_structure_ch_desc_104     = a_tbl_update(i).range_structure_ch_desc_104,
             cust_segmentation_desc_300      = a_tbl_update(i).cust_segmentation_desc_300,
             garment_length_desc_303         = a_tbl_update(i).garment_length_desc_303,
             price_tier_desc_306             = a_tbl_update(i).price_tier_desc_306,
             print_type_desc_307             = a_tbl_update(i).print_type_desc_307,
             top_vs_bottom_desc_309          = a_tbl_update(i).top_vs_bottom_desc_309 ,
             plain_vs_design_desc_310        = a_tbl_update(i).plain_vs_design_desc_310,
             material_desc_313               = a_tbl_update(i).material_desc_313,
             lifestyle_desc_316              = a_tbl_update(i).lifestyle_desc_316,
             sleeve_length_desc_317          = a_tbl_update(i).sleeve_length_desc_317 ,
             single_multiple_desc_319        = a_tbl_update(i).single_multiple_desc_319,
             sub_brands_desc_320             = a_tbl_update(i).sub_brands_desc_320,
             shoe_heel_height_desc_321       = a_tbl_update(i).shoe_heel_height_desc_321,
             fit_desc_323                    = a_tbl_update(i).fit_desc_323,
             waist_drop_desc_324             = a_tbl_update(i).waist_drop_desc_324,
             neck_line_desc_325              = a_tbl_update(i).neck_line_desc_325,
             silhouette_desc_327             = a_tbl_update(i).silhouette_desc_327,
             lighting_desc_329               = a_tbl_update(i).lighting_desc_329,
             gender_desc_330                 = a_tbl_update(i).gender_desc_330,
             event_buy_desc_331              = a_tbl_update(i).event_buy_desc_331,
             product_category_desc_333       = a_tbl_update(i).product_category_desc_333,
             fabric_type_desc_334            = a_tbl_update(i).fabric_type_desc_334,
             fragrance_brand_desc_1002       = a_tbl_update(i).fragrance_brand_desc_1002,
             fragrance_house_desc_1003       = a_tbl_update(i).fragrance_house_desc_1003,
             brand_type_desc_1103            = a_tbl_update(i).brand_type_desc_1103,
             brand_category_desc_1105        = a_tbl_update(i).brand_category_desc_1105,
             headqtr_assortment_desc_2301    = a_tbl_update(i).headqtr_assortment_desc_2301,
             shoe_heel_shape_desc_2402       = a_tbl_update(i).shoe_heel_shape_desc_2402,
             handbag_sizing_desc_2601        = a_tbl_update(i).handbag_sizing_desc_2601,
             product_defference_3104         = a_tbl_update(i).product_defference_3104,
             great_value_desc_3202           = a_tbl_update(i).great_value_desc_3202,
             wearing_season_desc_7501        = a_tbl_update(i).wearing_season_desc_7501,
             last_updated_date               = a_tbl_update(i).last_updated_date,
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
      where  sk1_style_colour_no             = a_tbl_update(i).sk1_style_colour_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no;
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
   from dim_ast_sc_uda
   where sk1_style_colour_no = g_rec_out.sk1_style_colour_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD DIM_AST_SC_UDA EX DIM_SC_UDA STARTED AT '||
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

--**************************************************************************************************
    open c_dim_sc_uda;
    fetch c_dim_sc_uda bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_dim_sc_uda bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_sc_uda;
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

end wh_prf_ast_003u;
