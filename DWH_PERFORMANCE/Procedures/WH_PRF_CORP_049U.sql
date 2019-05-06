--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_049U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_049U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Christie Koorts
--  Purpose:     Create syle_colour uda table in the performance layer
--               with added value ex foundation layer uda tables.
--  Tables:      Input  -   dim_item_uda, dim_item
--               Output -   dim_sc_uda
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  21 Feb 2009 - defect 540 -Initial Design of DIM_SC_UDA
--  09 March 2009 - defect 1074 - Add STOCK_ERROR_IND_DESC_699
--                                  to DIM_ITEM_UDA and DIM_SC_UDA
--  23 jUNE 2009 - DEFECT 1131 - Missing Columns in DIM_ITEM_UDA and DIM_SC_UDA
--                              PRODUCT_GROUPING_DESC_332
--                              BRAND_GIFTING_DESC_1104
--                              MARKETING_USE_ONLY_DESC_2102
--                              DIGITAL_SEGMENTATION_DESC_3001
--                              DIGITAL_GENRE_DESC_3002
--                              PRODUCT_DEFFERENCE_3104--
-- 21 dec 2009 - defect 2491 - Add field GREAT_VALUE_DESC_3202 on DIM_ITEM_UDA
--                             & DIM_SC_UDA
-- 12 Aug 2010 - defect 3735 - Add field NEW_LINE_INDICATOR_DESC_3502 on DIM_ITEM_UDA
--                             & DIM_SC_UDA
-- 29 Dec 2017 - A. Ugolini  - Add field WEARING_SEASON_DESC_7501 on DIM_ITEM_UDA
--                             & DIM_SC_UDA

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
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_sc_uda%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;
w_count              number        :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_049U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE STYLE COLOUR UDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.dim_sc_uda%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.dim_sc_uda%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_dim_item_uda is
   select di.SK1_STYLE_COLOUR_NO as SK1_STYLE_COLOUR_NO,
          di.STYLE_COLOUR_NO as STYLE_COLOUR_NO,
nvl((max((decode(diu.CHARACTER_DESC_102,'No Value', null, diu.CHARACTER_DESC_102)))),'No Value')as CHARACTER_DESC_102,
nvl((max((decode(diu.CURTAIN_HANG_METHOD_DESC_314 ,'No Value', null, diu.CURTAIN_HANG_METHOD_DESC_314 )))),'No Value')as CURTAIN_HANG_METHOD_DESC_314 ,
nvl((max((decode(diu.CURTAIN_LINING_DESC_315 ,'No Value', null, diu.CURTAIN_LINING_DESC_315 )))),'No Value')as CURTAIN_LINING_DESC_315 ,
nvl((max((decode(diu.CUST_SEGMENTATION_DESC_300 ,'No Value', null, diu.CUST_SEGMENTATION_DESC_300 )))),'No Value')as CUST_SEGMENTATION_DESC_300 ,
nvl((max((decode(diu.EVENT_BUY_DESC_331 ,'No Value', null, diu.EVENT_BUY_DESC_331 )))),'No Value')as EVENT_BUY_DESC_331 ,
nvl((max((decode(diu.FABRIC_TYPE_DESC_334 ,'No Value', null, diu.FABRIC_TYPE_DESC_334 )))),'No Value')as FABRIC_TYPE_DESC_334 ,
nvl((max((decode(diu.FIT_DESC_323 ,'No Value', null, diu.FIT_DESC_323 )))),'No Value')as FIT_DESC_323 ,
nvl((max((decode(diu.GENDER_DESC_330 ,'No Value', null, diu.GENDER_DESC_330 )))),'No Value')as GENDER_DESC_330 ,
nvl((max((decode(diu.GIFTING_DESC_326 ,'No Value', null, diu.GIFTING_DESC_326 )))),'No Value')as GIFTING_DESC_326 ,
nvl((max((decode(diu.GARMENT_LENGTH_DESC_303 ,'No Value', null, diu.GARMENT_LENGTH_DESC_303 )))),'No Value')as GARMENT_LENGTH_DESC_303 ,
nvl((max((decode(diu.LIFESTYLE_DESC_316 ,'No Value', null, diu.LIFESTYLE_DESC_316 )))),'No Value')as LIFESTYLE_DESC_316 ,
nvl((max((decode(diu.LIGHTING_DESC_329 ,'No Value', null, diu.LIGHTING_DESC_329 )))),'No Value')as LIGHTING_DESC_329 ,
nvl((max((decode(diu.MATERIAL_DESC_313 ,'No Value', null, diu.MATERIAL_DESC_313 )))),'No Value')as MATERIAL_DESC_313 ,
nvl((max((decode(diu.MERCH_CLASS_DESC_100 ,'No Value', null, diu.MERCH_CLASS_DESC_100 )))),'No Value')as MERCH_CLASS_DESC_100 ,
nvl((max((decode(diu.NECK_LINE_DESC_325 ,'No Value', null, diu.NECK_LINE_DESC_325 )))),'No Value')as NECK_LINE_DESC_325 ,
nvl((max((decode(diu.PLAIN_VS_DESIGN_DESC_310 ,'No Value', null, diu.PLAIN_VS_DESIGN_DESC_310 )))),'No Value')as PLAIN_VS_DESIGN_DESC_310 ,
nvl((max((decode(diu.PRICE_TIER_DESC_306 ,'No Value', null, diu.PRICE_TIER_DESC_306 )))),'No Value')as PRICE_TIER_DESC_306 ,
nvl((max((decode(diu.PRINT_TYPE_DESC_307 ,'No Value', null, diu.PRINT_TYPE_DESC_307 )))),'No Value')as PRINT_TYPE_DESC_307 ,
nvl((max((decode(diu.RANGE_SEGMENTATION_DESC_322 ,'No Value', null, diu.RANGE_SEGMENTATION_DESC_322 )))),'No Value')as RANGE_SEGMENTATION_DESC_322 ,
nvl((max((decode(diu.RANGE_STRUCTURE_CH_DESC_104 ,'No Value', null, diu.RANGE_STRUCTURE_CH_DESC_104 )))),'No Value')as RANGE_STRUCTURE_CH_DESC_104 ,
nvl((max((decode(diu.ROYALTIES_DESC_106,'No Value', null, diu.ROYALTIES_DESC_106)))),'No Value')as ROYALTIES_DESC_106,
nvl((max((decode(diu.SHOE_HEEL_HEIGHT_DESC_321 ,'No Value', null, diu.SHOE_HEEL_HEIGHT_DESC_321 )))),'No Value')as SHOE_HEEL_HEIGHT_DESC_321 ,
nvl((max((decode(diu.SILHOUETTE_DESC_327,'No Value', null, diu.SILHOUETTE_DESC_327)))),'No Value')as SILHOUETTE_DESC_327,
nvl((max((decode(diu.SINGLE_MULTIPLE_DESC_319 ,'No Value', null, diu.SINGLE_MULTIPLE_DESC_319 )))),'No Value')as SINGLE_MULTIPLE_DESC_319 ,
nvl((max((decode(diu.SLEEVE_LENGTH_DESC_317 ,'No Value', null, diu.SLEEVE_LENGTH_DESC_317 )))),'No Value')as SLEEVE_LENGTH_DESC_317 ,
nvl((max((decode(diu.SOCK_LENGTH_DESC_318 ,'No Value', null, diu.SOCK_LENGTH_DESC_318 )))),'No Value')as SOCK_LENGTH_DESC_318 ,
nvl((max((decode(diu.SUB_BRANDS_DESC_320 ,'No Value', null, diu.SUB_BRANDS_DESC_320 )))),'No Value')as SUB_BRANDS_DESC_320 ,
nvl((max((decode(diu.TOP_VS_BOTTOM_DESC_309 ,'No Value', null, diu.TOP_VS_BOTTOM_DESC_309 )))),'No Value')as TOP_VS_BOTTOM_DESC_309 ,
nvl((max((decode(diu.WAIST_DROP_DESC_324 ,'No Value', null, diu.WAIST_DROP_DESC_324 )))),'No Value')as WAIST_DROP_DESC_324 ,
nvl((max((decode(diu.CELLULAR_ITEM_DESC_904 ,'No Value', null, diu.CELLULAR_ITEM_DESC_904 )))),'No Value')as CELLULAR_ITEM_DESC_904 ,
nvl((max((decode(diu.PRODUCT_SAMPLE_STATUS_DESC_304 ,'No Value', null, diu.PRODUCT_SAMPLE_STATUS_DESC_304 )))),'No Value')as PRODUCT_SAMPLE_STATUS_DESC_304 ,
nvl((max((decode(diu.UTILISE_REF_ITEM_DESC_508 ,'No Value', null, diu.UTILISE_REF_ITEM_DESC_508 )))),'No Value')as UTILISE_REF_ITEM_DESC_508 ,
nvl((max((decode(diu.PRICE_MARKED_IND_DESC_903 ,'No Value', null, diu.PRICE_MARKED_IND_DESC_903 )))),'No Value')as PRICE_MARKED_IND_DESC_903 ,
nvl((max((decode(diu.DIGITAL_BRANDS_DESC_1503 ,'No Value', null, diu.DIGITAL_BRANDS_DESC_1503 )))),'No Value')as DIGITAL_BRANDS_DESC_1503 ,
nvl((max((decode(diu.DIGITAL_OFFER_DESC_1504 ,'No Value', null, diu.DIGITAL_OFFER_DESC_1504 )))),'No Value')as DIGITAL_OFFER_DESC_1504 ,
nvl((max((decode(diu.DIGITAL_PRICE_BANDS_DESC_1505 ,'No Value', null, diu.DIGITAL_PRICE_BANDS_DESC_1505 )))),'No Value')as DIGITAL_PRICE_BANDS_DESC_1505 ,
nvl((max((decode(diu.DIGITAL_DESIGN_DESC_1506 ,'No Value', null, diu.DIGITAL_DESIGN_DESC_1506 )))),'No Value')as DIGITAL_DESIGN_DESC_1506 ,
nvl((max((decode(diu.DIGITAL_DATA_DESC_1507 ,'No Value', null, diu.DIGITAL_DATA_DESC_1507 )))),'No Value')as DIGITAL_DATA_DESC_1507 ,
nvl((max((decode(diu.MERCH_USAGE_DESC_1907 ,'No Value', null, diu.MERCH_USAGE_DESC_1907 )))),'No Value')as MERCH_USAGE_DESC_1907 ,
nvl((max((decode(diu.KIDS_AGE_DESC_335 ,'No Value', null, diu.KIDS_AGE_DESC_335 )))),'No Value')as KIDS_AGE_DESC_335 ,
nvl((max((decode(diu.AVAILABILITY_DESC_540 ,'No Value', null, diu.AVAILABILITY_DESC_540 )))),'No Value')as AVAILABILITY_DESC_540 ,
nvl((max((decode(diu.PLANNING_MANAGER_DESC_561 ,'No Value', null, diu.PLANNING_MANAGER_DESC_561 )))),'No Value')as PLANNING_MANAGER_DESC_561 ,
nvl((max((decode(diu.COMMERCIAL_MANAGER_DESC_562 ,'No Value', null, diu.COMMERCIAL_MANAGER_DESC_562 )))),'No Value')as COMMERCIAL_MANAGER_DESC_562 ,
nvl((max((decode(diu.MERCHANDISE_CATEGORY_DESC_100 ,'No Value', null, diu.MERCHANDISE_CATEGORY_DESC_100 )))),'No Value')as MERCHANDISE_CATEGORY_DESC_100 ,
nvl((max((decode(diu.PRODUCT_CLASS_DESC_507 ,'No Value', null, diu.PRODUCT_CLASS_DESC_507 )))),'No Value')as PRODUCT_CLASS_DESC_507 ,
nvl((max((decode(diu.PRODUCT_GROUP_SCALING_DESC_501 ,'No Value', null, diu.PRODUCT_GROUP_SCALING_DESC_501 )))),'No Value')as PRODUCT_GROUP_SCALING_DESC_501 ,
nvl((max((decode(diu.ORGANICS_DESC_550 ,'No Value', null, diu.ORGANICS_DESC_550 )))),'No Value')as ORGANICS_DESC_550 ,
nvl((max((decode(diu.KIDZ_DESC_552 ,'No Value', null, diu.KIDZ_DESC_552 )))),'No Value')as KIDZ_DESC_552 ,
nvl((max((decode(diu.FREE_RANGE_DESC_554 ,'No Value', null, diu.FREE_RANGE_DESC_554 )))),'No Value')as FREE_RANGE_DESC_554 ,
nvl((max((decode(diu.VEGETARIAN_DESC_551 ,'No Value', null, diu.VEGETARIAN_DESC_551 )))),'No Value')as VEGETARIAN_DESC_551 ,
nvl((max((decode(diu.SLIMMERS_CHOICE_DESC_553 ,'No Value', null, diu.SLIMMERS_CHOICE_DESC_553 )))),'No Value')as SLIMMERS_CHOICE_DESC_553 ,
nvl((max((decode(diu.KOSHER_DESC_555 ,'No Value', null, diu.KOSHER_DESC_555 )))),'No Value')as KOSHER_DESC_555 ,
nvl((max((decode(diu.HALAAL_DESC_556 ,'No Value', null, diu.HALAAL_DESC_556 )))),'No Value')as HALAAL_DESC_556 ,
nvl((max((decode(diu.BRANDED_ITEM_DESC_911 ,'No Value', null, diu.BRANDED_ITEM_DESC_911 )))),'No Value')as BRANDED_ITEM_DESC_911 ,
nvl((max((decode(diu.IMPORT_ITEM_DESC_1601 ,'No Value', null, diu.IMPORT_ITEM_DESC_1601 )))),'No Value')as IMPORT_ITEM_DESC_1601 ,
nvl((max((decode(diu.NEW_LINE_LAUNCH_DATE_DESC_543 ,'No Value', null, diu.NEW_LINE_LAUNCH_DATE_DESC_543 )))),'No Value')as NEW_LINE_LAUNCH_DATE_DESC_543 ,
nvl((max((decode(diu.FOODS_RANGE_STRUCTURE_DESC_560,'No Value', null, diu.FOODS_RANGE_STRUCTURE_DESC_560)))),'No Value')as FOODS_RANGE_STRUCTURE_DESC_560,
nvl((max((decode(diu.FRAGRANCE_BRAND_DESC_1002 ,'No Value', null, diu.FRAGRANCE_BRAND_DESC_1002 )))),'No Value')as FRAGRANCE_BRAND_DESC_1002 ,
nvl((max((decode(diu.FRAGRANCE_HOUSE_DESC_1003 ,'No Value', null, diu.FRAGRANCE_HOUSE_DESC_1003 )))),'No Value')as FRAGRANCE_HOUSE_DESC_1003 ,
nvl((max((decode(diu.HEADQTR_ASSORTMENT_DESC_2301 ,'No Value', null, diu.HEADQTR_ASSORTMENT_DESC_2301 )))),'No Value')as HEADQTR_ASSORTMENT_DESC_2301 ,
nvl((max((decode(diu.FOOD_MAIN_SHOP_DESC_2803 ,'No Value', null, diu.FOOD_MAIN_SHOP_DESC_2803 )))),'No Value')as FOOD_MAIN_SHOP_DESC_2803 ,
nvl((max((decode(diu.HOT_ITEM_DESC_2804 ,'No Value', null, diu.HOT_ITEM_DESC_2804 )))),'No Value')as HOT_ITEM_DESC_2804 ,
nvl((max((decode(diu.PRODUCT_CATEGORY_DESC_333 ,'No Value', null, diu.PRODUCT_CATEGORY_DESC_333 )))),'No Value')as PRODUCT_CATEGORY_DESC_333 ,
nvl((max((decode(diu.BRAND_TYPE_DESC_1103 ,'No Value', null, diu.BRAND_TYPE_DESC_1103 )))),'No Value')as BRAND_TYPE_DESC_1103 ,
nvl((max((decode(diu.BRAND_CATEGORY_DESC_1105 ,'No Value', null, diu.BRAND_CATEGORY_DESC_1105 )))),'No Value')as BRAND_CATEGORY_DESC_1105 ,
nvl((max((decode(diu.SHOE_HEEL_SHAPE_DESC_2402 ,'No Value', null, diu.SHOE_HEEL_SHAPE_DESC_2402 )))),'No Value')as SHOE_HEEL_SHAPE_DESC_2402 ,
nvl((max((decode(diu.BEAUTY_BRND_PRD_DTL_DESC_2501 ,'No Value', null, diu.BEAUTY_BRND_PRD_DTL_DESC_2501 )))),'No Value')as BEAUTY_BRND_PRD_DTL_DESC_2501 ,
nvl((max((decode(diu.BEAUTY_GIFT_RANGE_DESC_2503 ,'No Value', null, diu.BEAUTY_GIFT_RANGE_DESC_2503 )))),'No Value')as BEAUTY_GIFT_RANGE_DESC_2503 ,
nvl((max((decode(diu.HANDBAG_SIZING_DESC_2601 ,'No Value', null, diu.HANDBAG_SIZING_DESC_2601 )))),'No Value')as HANDBAG_SIZING_DESC_2601 ,
nvl((max((decode(diu.PLANNING_ITEM_IND_2901,'No Value', null, diu.PLANNING_ITEM_IND_2901)))),'No Value')as PLANNING_ITEM_IND_2901,
nvl((max((decode(diu.STOCK_ERROR_IND_DESC_699,'No Value', null, diu.STOCK_ERROR_IND_DESC_699)))),'No Value')as STOCK_ERROR_IND_DESC_699,
max(diu.LAST_UPDATED_DATE) last_updated_date,
nvl((max((decode(diu.PRODUCT_GROUPING_DESC_332,'No Value', null, diu.PRODUCT_GROUPING_DESC_332)))),'No Value')as PRODUCT_GROUPING_DESC_332,
nvl((max((decode(diu.BRAND_GIFTING_DESC_1104,'No Value', null, diu.BRAND_GIFTING_DESC_1104)))),'No Value')as BRAND_GIFTING_DESC_1104,
nvl((max((decode(diu.MARKETING_USE_ONLY_DESC_2102,'No Value', null, diu.MARKETING_USE_ONLY_DESC_2102)))),'No Value')as MARKETING_USE_ONLY_DESC_2102,
nvl((max((decode(diu.DIGITAL_SEGMENTATION_DESC_3001,'No Value', null, diu.DIGITAL_SEGMENTATION_DESC_3001)))),'No Value')as  DIGITAL_SEGMENTATION_DESC_3001,
nvl((max((decode(diu.DIGITAL_GENRE_DESC_3002,'No Value', null, diu.DIGITAL_GENRE_DESC_3002)))),'No Value')as DIGITAL_GENRE_DESC_3002,
nvl((max((decode(diu.PRODUCT_DEFFERENCE_3104,'No Value', null, diu.PRODUCT_DEFFERENCE_3104)))),'No Value')as PRODUCT_DEFFERENCE_3104,
nvl((max((decode(diu.LOOSE_ITEM_DESC_910,'No Value', null, diu.LOOSE_ITEM_DESC_910)))),'No Value')as LOOSE_ITEM_DESC_910,
nvl((max((decode(diu.VARIABLE_WEIGHT_ITEM_DESC_905,'No Value', null, diu.VARIABLE_WEIGHT_ITEM_DESC_905)))),'No Value')as VARIABLE_WEIGHT_ITEM_DESC_905,
nvl((max((decode(diu.SHORTS_LONGLIFE_DESC_542,'No Value', null, diu.SHORTS_LONGLIFE_DESC_542)))),'No Value')as SHORTS_LONGLIFE_DESC_542,
nvl((max((decode(diu.GREAT_VALUE_DESC_3202,'No Value', null, diu.GREAT_VALUE_DESC_3202)))),'No Value')as GREAT_VALUE_DESC_3202,
nvl((max((decode(diu.NEW_LINE_INDICATOR_DESC_3502,'No Value', null, diu.NEW_LINE_INDICATOR_DESC_3502)))),'No Value')as NEW_LINE_INDICATOR_DESC_3502,
nvl((max((decode(diu.WEARING_SEASON_DESC_7501,'No Value', null, diu.WEARING_SEASON_DESC_7501)))),'No Value')as WEARING_SEASON_DESC_7501
          FROM DWH_PERFORMANCE.DIM_ITEM_UDA DIU, DWH_PERFORMANCE.DIM_ITEM DI
          WHERE DI.SK1_ITEM_NO = DIU.SK1_ITEM_NO
      --    AND DI.ITEM_NO = 6001009052148
      --            and di.sk1_style_colour_no = 7389558
      and di.sk1_style_colour_no is not null
      and di.item_level_no = di.tran_level_no
                    group by di.SK1_STYLE_COLOUR_NO, di.STYLE_COLOUR_NO ;
--
-- Input record declared as cursor%rowtype
g_rec_in             c_dim_item_uda%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_dim_item_uda%rowtype;
a_stg_input      stg_array;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--   where last_updated_date >= g_yesterday;
--   order by district_no

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.SK1_STYLE_COLOUR_NO            := g_rec_in.SK1_STYLE_COLOUR_NO;
   g_rec_out.STYLE_COLOUR_NO                := g_rec_in.STYLE_COLOUR_NO;
   g_rec_out.CHARACTER_DESC_102             := g_rec_in.CHARACTER_DESC_102;
   g_rec_out.CURTAIN_HANG_METHOD_DESC_314   := g_rec_in.CURTAIN_HANG_METHOD_DESC_314;
   g_rec_out.CURTAIN_LINING_DESC_315        := g_rec_in.CURTAIN_LINING_DESC_315;
   g_rec_out.CUST_SEGMENTATION_DESC_300     := g_rec_in.CUST_SEGMENTATION_DESC_300;
   g_rec_out.EVENT_BUY_DESC_331             := g_rec_in.EVENT_BUY_DESC_331;
   g_rec_out.FABRIC_TYPE_DESC_334           := g_rec_in.FABRIC_TYPE_DESC_334;
   g_rec_out.FIT_DESC_323                   := g_rec_in.FIT_DESC_323;
   g_rec_out.GENDER_DESC_330                := g_rec_in.GENDER_DESC_330;
   g_rec_out.GIFTING_DESC_326               := g_rec_in.GIFTING_DESC_326;
   g_rec_out.GARMENT_LENGTH_DESC_303        := g_rec_in.GARMENT_LENGTH_DESC_303;
   g_rec_out.LIFESTYLE_DESC_316             := g_rec_in.LIFESTYLE_DESC_316;
   g_rec_out.LIGHTING_DESC_329              := g_rec_in.LIGHTING_DESC_329;
   g_rec_out.MATERIAL_DESC_313              := g_rec_in.MATERIAL_DESC_313;
   g_rec_out.MERCH_CLASS_DESC_100           := g_rec_in.MERCH_CLASS_DESC_100;
   g_rec_out.NECK_LINE_DESC_325             := g_rec_in.NECK_LINE_DESC_325;
   g_rec_out.PLAIN_VS_DESIGN_DESC_310       := g_rec_in.PLAIN_VS_DESIGN_DESC_310;
   g_rec_out.PRICE_TIER_DESC_306            := g_rec_in.PRICE_TIER_DESC_306;
   g_rec_out.PRINT_TYPE_DESC_307            := g_rec_in.PRINT_TYPE_DESC_307;
   g_rec_out.RANGE_SEGMENTATION_DESC_322    := g_rec_in.RANGE_SEGMENTATION_DESC_322;
   g_rec_out.RANGE_STRUCTURE_CH_DESC_104    := g_rec_in.RANGE_STRUCTURE_CH_DESC_104;
   g_rec_out.ROYALTIES_DESC_106             := g_rec_in.ROYALTIES_DESC_106;
   g_rec_out.SHOE_HEEL_HEIGHT_DESC_321      := g_rec_in.SHOE_HEEL_HEIGHT_DESC_321;
   g_rec_out.SILHOUETTE_DESC_327            := g_rec_in.SILHOUETTE_DESC_327;
   g_rec_out.SINGLE_MULTIPLE_DESC_319       := g_rec_in.SINGLE_MULTIPLE_DESC_319;
   g_rec_out.SLEEVE_LENGTH_DESC_317         := g_rec_in.SLEEVE_LENGTH_DESC_317;
   g_rec_out.SOCK_LENGTH_DESC_318           := g_rec_in.SOCK_LENGTH_DESC_318;
   g_rec_out.SUB_BRANDS_DESC_320            := g_rec_in.SUB_BRANDS_DESC_320;
   g_rec_out.TOP_VS_BOTTOM_DESC_309         := g_rec_in.TOP_VS_BOTTOM_DESC_309;
   g_rec_out.WAIST_DROP_DESC_324            := g_rec_in.WAIST_DROP_DESC_324;
   g_rec_out.CELLULAR_ITEM_DESC_904         := g_rec_in.CELLULAR_ITEM_DESC_904;
   g_rec_out.PRODUCT_SAMPLE_STATUS_DESC_304 := g_rec_in.PRODUCT_SAMPLE_STATUS_DESC_304;
   g_rec_out.UTILISE_REF_ITEM_DESC_508      := g_rec_in.UTILISE_REF_ITEM_DESC_508;
   g_rec_out.PRICE_MARKED_IND_DESC_903      := g_rec_in.PRICE_MARKED_IND_DESC_903;
   g_rec_out.DIGITAL_BRANDS_DESC_1503       := g_rec_in.DIGITAL_BRANDS_DESC_1503;
   g_rec_out.DIGITAL_OFFER_DESC_1504        := g_rec_in.DIGITAL_OFFER_DESC_1504;
   g_rec_out.DIGITAL_PRICE_BANDS_DESC_1505  := g_rec_in.DIGITAL_PRICE_BANDS_DESC_1505;
   g_rec_out.DIGITAL_DESIGN_DESC_1506       := g_rec_in.DIGITAL_DESIGN_DESC_1506;
   g_rec_out.DIGITAL_DATA_DESC_1507         := g_rec_in.DIGITAL_DATA_DESC_1507;
   g_rec_out.MERCH_USAGE_DESC_1907          := g_rec_in.MERCH_USAGE_DESC_1907;
   g_rec_out.KIDS_AGE_DESC_335              := g_rec_in.KIDS_AGE_DESC_335;
   g_rec_out.AVAILABILITY_DESC_540          := g_rec_in.AVAILABILITY_DESC_540;
   g_rec_out.PLANNING_MANAGER_DESC_561      := g_rec_in.PLANNING_MANAGER_DESC_561;
   g_rec_out.COMMERCIAL_MANAGER_DESC_562    := g_rec_in.COMMERCIAL_MANAGER_DESC_562;
   g_rec_out.MERCHANDISE_CATEGORY_DESC_100  := g_rec_in.MERCHANDISE_CATEGORY_DESC_100;
   g_rec_out.PRODUCT_CLASS_DESC_507         := g_rec_in.PRODUCT_CLASS_DESC_507;
   g_rec_out.PRODUCT_GROUP_SCALING_DESC_501 := g_rec_in.PRODUCT_GROUP_SCALING_DESC_501;
   g_rec_out.ORGANICS_DESC_550              := g_rec_in.ORGANICS_DESC_550;
   g_rec_out.KIDZ_DESC_552                  := g_rec_in.KIDZ_DESC_552;
   g_rec_out.FREE_RANGE_DESC_554            := g_rec_in.FREE_RANGE_DESC_554;
   g_rec_out.VEGETARIAN_DESC_551            := g_rec_in.VEGETARIAN_DESC_551;
   g_rec_out.SLIMMERS_CHOICE_DESC_553       := g_rec_in.SLIMMERS_CHOICE_DESC_553;
   g_rec_out.KOSHER_DESC_555                := g_rec_in.KOSHER_DESC_555;
   g_rec_out.HALAAL_DESC_556                := g_rec_in.HALAAL_DESC_556;
   g_rec_out.BRANDED_ITEM_DESC_911          := g_rec_in.BRANDED_ITEM_DESC_911;
   g_rec_out.IMPORT_ITEM_DESC_1601          := g_rec_in.IMPORT_ITEM_DESC_1601;
   g_rec_out.NEW_LINE_LAUNCH_DATE_DESC_543  := g_rec_in.NEW_LINE_LAUNCH_DATE_DESC_543;
   g_rec_out.FOODS_RANGE_STRUCTURE_DESC_560 := g_rec_in.FOODS_RANGE_STRUCTURE_DESC_560;
   g_rec_out.FRAGRANCE_BRAND_DESC_1002      := g_rec_in.FRAGRANCE_BRAND_DESC_1002;
   g_rec_out.FRAGRANCE_HOUSE_DESC_1003      := g_rec_in.FRAGRANCE_HOUSE_DESC_1003;
   g_rec_out.HEADQTR_ASSORTMENT_DESC_2301   := g_rec_in.HEADQTR_ASSORTMENT_DESC_2301;
   g_rec_out.FOOD_MAIN_SHOP_DESC_2803       := g_rec_in.FOOD_MAIN_SHOP_DESC_2803;
   g_rec_out.HOT_ITEM_DESC_2804             := g_rec_in.HOT_ITEM_DESC_2804;
   g_rec_out.PRODUCT_CATEGORY_DESC_333      := g_rec_in.PRODUCT_CATEGORY_DESC_333;
   g_rec_out.BRAND_TYPE_DESC_1103           := g_rec_in.BRAND_TYPE_DESC_1103;
   g_rec_out.BRAND_CATEGORY_DESC_1105       := g_rec_in.BRAND_CATEGORY_DESC_1105;
   g_rec_out.SHOE_HEEL_SHAPE_DESC_2402      := g_rec_in.SHOE_HEEL_SHAPE_DESC_2402;
   g_rec_out.BEAUTY_BRND_PRD_DTL_DESC_2501  := g_rec_in.BEAUTY_BRND_PRD_DTL_DESC_2501;
   g_rec_out.BEAUTY_GIFT_RANGE_DESC_2503    := g_rec_in.BEAUTY_GIFT_RANGE_DESC_2503;
   g_rec_out.PLANNING_ITEM_IND_2901         := g_rec_in.PLANNING_ITEM_IND_2901;
   g_rec_out.HANDBAG_SIZING_DESC_2601       := g_rec_in.HANDBAG_SIZING_DESC_2601;
   g_rec_out.STOCK_ERROR_IND_DESC_699       := g_rec_in.STOCK_ERROR_IND_DESC_699;
   g_rec_out.last_updated_date              := g_date;

   g_rec_out.PRODUCT_GROUPING_DESC_332         := nvl(g_rec_in.PRODUCT_GROUPING_DESC_332,'No Value');
   g_rec_out.BRAND_GIFTING_DESC_1104           := nvl(g_rec_in.BRAND_GIFTING_DESC_1104 ,'No Value');
   g_rec_out.MARKETING_USE_ONLY_DESC_2102      := nvl(g_rec_in.MARKETING_USE_ONLY_DESC_2102,'No Value');
   g_rec_out.DIGITAL_SEGMENTATION_DESC_3001    := nvl(g_rec_in.DIGITAL_SEGMENTATION_DESC_3001,'No Value');
   g_rec_out.DIGITAL_GENRE_DESC_3002           := nvl(g_rec_in.DIGITAL_GENRE_DESC_3002,'No Value');
   g_rec_out.PRODUCT_DEFFERENCE_3104           := nvl(g_rec_in.PRODUCT_DEFFERENCE_3104,'No Value');
   g_rec_out.LOOSE_ITEM_DESC_910               := g_rec_in.LOOSE_ITEM_DESC_910;
   g_rec_out.VARIABLE_WEIGHT_ITEM_DESC_905     := g_rec_in.VARIABLE_WEIGHT_ITEM_DESC_905;
   g_rec_out.SHORTS_LONGLIFE_DESC_542          := g_rec_in.SHORTS_LONGLIFE_DESC_542;
   g_rec_out.GREAT_VALUE_DESC_3202             := g_rec_in.GREAT_VALUE_DESC_3202;
   g_rec_out.NEW_LINE_INDICATOR_DESC_3502      := g_rec_in.NEW_LINE_INDICATOR_DESC_3502;
   g_rec_out.WEARING_SEASON_DESC_7501          := g_rec_in.WEARING_SEASON_DESC_7501;

   exception
      when others then
      DBMS_OUTPUT.PUT_LINE(' LAV - SK1_STYLE_COLOUR_NO='||g_rec_in.SK1_STYLE_COLOUR_NO||' SK1_STYLE_COLOUR_NO='||g_rec_in.SK1_STYLE_COLOUR_NO);
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
      insert into dim_sc_uda values a_tbl_insert(i);
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
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update DWH_PERFORMANCE.dim_sc_uda
      set    SK1_STYLE_COLOUR_NO            = a_tbl_update(i).SK1_STYLE_COLOUR_NO,
             STYLE_COLOUR_NO                = a_tbl_update(i).STYLE_COLOUR_NO,
             CHARACTER_DESC_102             = a_tbl_update(i).CHARACTER_DESC_102,
             CURTAIN_HANG_METHOD_DESC_314   = a_tbl_update(i).CURTAIN_HANG_METHOD_DESC_314,
             CURTAIN_LINING_DESC_315        = a_tbl_update(i).CURTAIN_LINING_DESC_315,
             CUST_SEGMENTATION_DESC_300     = a_tbl_update(i).CUST_SEGMENTATION_DESC_300,
             EVENT_BUY_DESC_331             = a_tbl_update(i).EVENT_BUY_DESC_331,
             FABRIC_TYPE_DESC_334           = a_tbl_update(i).FABRIC_TYPE_DESC_334,
             FIT_DESC_323                   = a_tbl_update(i).FIT_DESC_323,
             GENDER_DESC_330                = a_tbl_update(i).GENDER_DESC_330,
             GIFTING_DESC_326               = a_tbl_update(i).GIFTING_DESC_326,
             GARMENT_LENGTH_DESC_303        = a_tbl_update(i).GARMENT_LENGTH_DESC_303,
             LIFESTYLE_DESC_316             = a_tbl_update(i).LIFESTYLE_DESC_316,
             LIGHTING_DESC_329              = a_tbl_update(i).LIGHTING_DESC_329,
             MATERIAL_DESC_313              = a_tbl_update(i).MATERIAL_DESC_313,
             MERCH_CLASS_DESC_100           = a_tbl_update(i).MERCH_CLASS_DESC_100,
             NECK_LINE_DESC_325             = a_tbl_update(i).NECK_LINE_DESC_325,
             PLAIN_VS_DESIGN_DESC_310       = a_tbl_update(i).PLAIN_VS_DESIGN_DESC_310,
             PRICE_TIER_DESC_306            = a_tbl_update(i).PRICE_TIER_DESC_306,
             PRINT_TYPE_DESC_307            = a_tbl_update(i).PRINT_TYPE_DESC_307,
             RANGE_SEGMENTATION_DESC_322    = a_tbl_update(i).RANGE_SEGMENTATION_DESC_322,
             RANGE_STRUCTURE_CH_DESC_104    = a_tbl_update(i).RANGE_STRUCTURE_CH_DESC_104,
             ROYALTIES_DESC_106             = a_tbl_update(i).ROYALTIES_DESC_106,
             SHOE_HEEL_HEIGHT_DESC_321      = a_tbl_update(i).SHOE_HEEL_HEIGHT_DESC_321,
             SILHOUETTE_DESC_327            = a_tbl_update(i).SILHOUETTE_DESC_327,
             SINGLE_MULTIPLE_DESC_319       = a_tbl_update(i).SINGLE_MULTIPLE_DESC_319,
             SLEEVE_LENGTH_DESC_317         = a_tbl_update(i).SLEEVE_LENGTH_DESC_317,
             SOCK_LENGTH_DESC_318           = a_tbl_update(i).SOCK_LENGTH_DESC_318,
               SUB_BRANDS_DESC_320          = a_tbl_update(i).SUB_BRANDS_DESC_320,
             TOP_VS_BOTTOM_DESC_309         = a_tbl_update(i).TOP_VS_BOTTOM_DESC_309,
             WAIST_DROP_DESC_324            = a_tbl_update(i).WAIST_DROP_DESC_324,
             CELLULAR_ITEM_DESC_904         = a_tbl_update(i).CELLULAR_ITEM_DESC_904,
             PRODUCT_SAMPLE_STATUS_DESC_304 = a_tbl_update(i).PRODUCT_SAMPLE_STATUS_DESC_304,
             UTILISE_REF_ITEM_DESC_508      = a_tbl_update(i).UTILISE_REF_ITEM_DESC_508,
             PRICE_MARKED_IND_DESC_903      = a_tbl_update(i).PRICE_MARKED_IND_DESC_903,
             DIGITAL_BRANDS_DESC_1503       = a_tbl_update(i).DIGITAL_BRANDS_DESC_1503,
             DIGITAL_OFFER_DESC_1504        = a_tbl_update(i).DIGITAL_OFFER_DESC_1504,
             DIGITAL_PRICE_BANDS_DESC_1505  = a_tbl_update(i).DIGITAL_PRICE_BANDS_DESC_1505,
             DIGITAL_DESIGN_DESC_1506       = a_tbl_update(i).DIGITAL_DESIGN_DESC_1506,
             DIGITAL_DATA_DESC_1507         = a_tbl_update(i).DIGITAL_DATA_DESC_1507,
             MERCH_USAGE_DESC_1907          = a_tbl_update(i).MERCH_USAGE_DESC_1907,
             KIDS_AGE_DESC_335              = a_tbl_update(i).KIDS_AGE_DESC_335,
             AVAILABILITY_DESC_540          = a_tbl_update(i).AVAILABILITY_DESC_540,
             PLANNING_MANAGER_DESC_561      = a_tbl_update(i).PLANNING_MANAGER_DESC_561,
             COMMERCIAL_MANAGER_DESC_562    = a_tbl_update(i).COMMERCIAL_MANAGER_DESC_562,
             MERCHANDISE_CATEGORY_DESC_100  = a_tbl_update(i).MERCHANDISE_CATEGORY_DESC_100,
             PRODUCT_CLASS_DESC_507         = a_tbl_update(i).PRODUCT_CLASS_DESC_507,
             PRODUCT_GROUP_SCALING_DESC_501 = a_tbl_update(i).PRODUCT_GROUP_SCALING_DESC_501,
             ORGANICS_DESC_550              = a_tbl_update(i).ORGANICS_DESC_550,
             KIDZ_DESC_552                  = a_tbl_update(i).KIDZ_DESC_552,
             FREE_RANGE_DESC_554            = a_tbl_update(i).FREE_RANGE_DESC_554,
             VEGETARIAN_DESC_551            = a_tbl_update(i).VEGETARIAN_DESC_551,
             SLIMMERS_CHOICE_DESC_553       = a_tbl_update(i).SLIMMERS_CHOICE_DESC_553,
             KOSHER_DESC_555                = a_tbl_update(i).KOSHER_DESC_555,
             HALAAL_DESC_556                = a_tbl_update(i).HALAAL_DESC_556,
             BRANDED_ITEM_DESC_911          = a_tbl_update(i).BRANDED_ITEM_DESC_911,
             IMPORT_ITEM_DESC_1601          = a_tbl_update(i).IMPORT_ITEM_DESC_1601,
             NEW_LINE_LAUNCH_DATE_DESC_543  = a_tbl_update(i).NEW_LINE_LAUNCH_DATE_DESC_543,
             FOODS_RANGE_STRUCTURE_DESC_560 = a_tbl_update(i).FOODS_RANGE_STRUCTURE_DESC_560,
             FRAGRANCE_BRAND_DESC_1002      = a_tbl_update(i).FRAGRANCE_BRAND_DESC_1002,
             FRAGRANCE_HOUSE_DESC_1003      = a_tbl_update(i).FRAGRANCE_HOUSE_DESC_1003,
             HEADQTR_ASSORTMENT_DESC_2301   = a_tbl_update(i).HEADQTR_ASSORTMENT_DESC_2301,
             FOOD_MAIN_SHOP_DESC_2803       = a_tbl_update(i).FOOD_MAIN_SHOP_DESC_2803,
             HOT_ITEM_DESC_2804             = a_tbl_update(i).HOT_ITEM_DESC_2804,
             PRODUCT_CATEGORY_DESC_333      = a_tbl_update(i).PRODUCT_CATEGORY_DESC_333,
             BRAND_TYPE_DESC_1103           = a_tbl_update(i).BRAND_TYPE_DESC_1103,
             BRAND_CATEGORY_DESC_1105       = a_tbl_update(i).BRAND_CATEGORY_DESC_1105,
             SHOE_HEEL_SHAPE_DESC_2402      = a_tbl_update(i).SHOE_HEEL_SHAPE_DESC_2402,
             BEAUTY_BRND_PRD_DTL_DESC_2501  = a_tbl_update(i).BEAUTY_BRND_PRD_DTL_DESC_2501,
             BEAUTY_GIFT_RANGE_DESC_2503    = a_tbl_update(i).BEAUTY_GIFT_RANGE_DESC_2503,
             HANDBAG_SIZING_DESC_2601       = a_tbl_update(i).HANDBAG_SIZING_DESC_2601,
             PLANNING_ITEM_IND_2901         = a_tbl_update(i).PLANNING_ITEM_IND_2901,
             STOCK_ERROR_IND_DESC_699       = a_tbl_update(i).STOCK_ERROR_IND_DESC_699,
             LAST_UPDATED_DATE              = a_tbl_update(i).LAST_UPDATED_DATE,
             PRODUCT_GROUPING_DESC_332      = a_tbl_update(i).PRODUCT_GROUPING_DESC_332,
             BRAND_GIFTING_DESC_1104        = a_tbl_update(i).BRAND_GIFTING_DESC_1104 ,
             MARKETING_USE_ONLY_DESC_2102   = a_tbl_update(i).MARKETING_USE_ONLY_DESC_2102 ,
             DIGITAL_SEGMENTATION_DESC_3001 = a_tbl_update(i).DIGITAL_SEGMENTATION_DESC_3001 ,
             DIGITAL_GENRE_DESC_3002        = a_tbl_update(i).DIGITAL_GENRE_DESC_3002 ,
             PRODUCT_DEFFERENCE_3104        = a_tbl_update(i).PRODUCT_DEFFERENCE_3104,
             LOOSE_ITEM_DESC_910            = a_tbl_update(i).LOOSE_ITEM_DESC_910,
             VARIABLE_WEIGHT_ITEM_DESC_905  = a_tbl_update(i).VARIABLE_WEIGHT_ITEM_DESC_905,
             SHORTS_LONGLIFE_DESC_542       = a_tbl_update(i).SHORTS_LONGLIFE_DESC_542,
             GREAT_VALUE_DESC_3202          = a_tbl_update(i).GREAT_VALUE_DESC_3202,
             NEW_LINE_INDICATOR_DESC_3502   = a_tbl_update(i).NEW_LINE_INDICATOR_DESC_3502,
             WEARING_SEASON_DESC_7501       = a_tbl_update(i).WEARING_SEASON_DESC_7501
      where  SK1_STYLE_COLOUR_NO            = a_tbl_update(i).SK1_STYLE_COLOUR_NO;

      g_recs_updated := g_recs_updated + a_tbl_update.count;

   exception
      when others then
      DBMS_OUTPUT.PUT_LINE(' LBU - SK1_STYLE_COLOUR_NO='||a_tbl_update(g_error_index).SK1_style_colour_no||' SK1_STYLE_COLOUR_NO='||a_tbl_update(g_error_index).style_colour_no);
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
   from DWH_PERFORMANCE.dim_sc_uda
   where sk1_style_colour_no = g_rec_out.sk1_style_colour_no;

   if g_count > 0 then
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
      DBMS_OUTPUT.PUT_LINE('dwh_errors.e_insert_error');
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
   DBMS_OUTPUT.PUT_LINE('LWO OTHER ERRORS');
    dbms_output.put_line('Xa_count='||a_count||' a_count_i='||a_count_i||' a_count_u='||a_count_u);
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
DBMS_OUTPUT.ENABLE(10000000);
    dbms_output.put_line('Loading data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD DIM_SC_UDA EX dim_item_uda STARTED AT '||
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
    open c_dim_item_uda;
    fetch c_dim_item_uda bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_dim_item_uda bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_item_uda;
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
end wh_prf_corp_049u;
