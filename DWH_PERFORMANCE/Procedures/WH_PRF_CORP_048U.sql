--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_048U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_048U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Christie Koorts
--  Purpose:     Load item uda dimension table in the performance layer
--               with data ex foundation layer uda tables.
--  Tables:      Input  -   fnd_item_uda, fnd_uda
--               Output -   dim_item_uda
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
-- 13 Jul 2015 -             - Add field BRAND_STRATEGY_DESC_5601 on DIM_ITEM_UDA
--                             & DIM_SC_UDA
-- 29 Dec 2017 - A. Ugolini  - Add field WEARING_SEASON_DESC_7501 on DIM_ITEM_UDA
--                             & DIM_SC_UDA

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
g_rec_out            dim_item_uda%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_048U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DIM_ITEM_UDA EX FND_ITEM_UDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_item_uda%rowtype index by binary_integer;
type tbl_array_u is table of dim_item_uda%rowtype index by binary_integer;
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

cursor c_fnd_item_uda is
   select *
   from (select di.sk1_item_no,
                fiu.item_no,
                fiu.uda_no,
                nvl(fuv.uda_value_desc,'No Value') uda_desc
         from   fnd_item_uda fiu
         join   dim_item di         on  fiu.item_no                                 = di.item_no
         join   fnd_uda_value fuv   on  fiu.uda_no                                  = fuv.uda_no
                                    and to_number(fiu.uda_value_no_or_text_or_date) = fuv.uda_value_no
         where  (length(trim(translate(fiu.uda_value_no_or_text_or_date, ' +-.0123456789', ' '))) is null)
         and    fiu.uda_no in (100,102,104,106,300,303,304,306,307,309,310,313,314,315,316,317,318,
                               319,320,321,322,323,324,325,326,327,329,330,331,332,333,334,335,
                               501,507,508,540,543,550,551,552,553,554,555,556,560,561,562,
                               699,903,904,911,
                               1002,1003,1103,1104,1105,1503,1504,1505,1506,1507,1601,1907,2102,
                               2301,2402,2501,2503,2601,2803,2804,2901,3001,3002,3104,910,905,542,
                               3202,3502,4901,
                               5102, 5104, 5105, 5106,
                               5402,5403,5404,5406,5407,5408,5409,5301,5103,5601,7501) 
         union
         select di.sk1_item_no,
                fiu.item_no,
                fiu.uda_no,
                nvl(fiu.uda_value_no_or_text_or_date,'No Value') uda_desc
         from   fnd_item_uda fiu
         join   dim_item di         on  fiu.item_no = di.item_no
         where  (length(trim(translate(fiu.uda_value_no_or_text_or_date, ' +-.0123456789', ' '))) is not null)
         and    fiu.uda_no in (100,102,104,106,300,303,304,306,307,309,310,313,314,315,316,317,318,
                               319,320,321,322,323,324,325,326,327,329,330,331,332,333,334,335,
                               501,507,508,540,543,550,551,552,553,554,555,556,560,561,562,
                               699,903,904,911,
                               1002,1003,1103,1104,1105,1503,1504,1505,1506,1507,1601,1907,2102,
                               2301,2402,2501,2503,2601,2803,2804,2901,3001,3002,3104,910,905,542,
                               3202,3502,4901,
                               5102, 5104, 5105, 5106,
                               5402,5403,5404,5406,5407,5408,5409,5301,5103,5601,7501)) 
        pivot
        (max(uda_desc) for uda_no in (100 as uda_100,
                                      102 as uda_102,
                                      104 as uda_104,
                                      106 as uda_106,
                                      300 as uda_300,
                                      303 as uda_303,
                                      304 as uda_304,
                                      306 as uda_306,
                                      307 as uda_307,
                                      309 as uda_309,
                                      310 as uda_310,
                                      313 as uda_313,
                                      314 as uda_314,
                                      315 as uda_315,
                                      316 as uda_316,
                                      317 as uda_317,
                                      318 as uda_318,
                                      319 as uda_319,
                                      320 as uda_320,
                                      321 as uda_321,
                                      322 as uda_322,
                                      323 as uda_323,
                                      324 as uda_324,
                                      325 as uda_325,
                                      326 as uda_326,
                                      327 as uda_327,
                                      329 as uda_329,
                                      330 as uda_330,
                                      331 as uda_331,
                                      332 as uda_332,
                                      333 as uda_333,
                                      334 as uda_334,
                                      335 as uda_335,
                                      501 as uda_501,
                                      507 as uda_507,
                                      508 as uda_508,
                                      540 as uda_540,
                                      543 as uda_543,
                                      550 as uda_550,
                                      551 as uda_551,
                                      552 as uda_552,
                                      553 as uda_553,
                                      554 as uda_554,
                                      555 as uda_555,
                                      556 as uda_556,
                                      560 as uda_560,
                                      561 as uda_561,
                                      562 as uda_562,
                                      699 as uda_699,
                                      903 as uda_903,
                                      904 as uda_904,
                                      911 as uda_911,
                                      1002 as uda_1002,
                                      1003 as uda_1003,
                                      1103 as uda_1103,
                                      1104 as uda_1104,
                                      1105 as uda_1105,
                                      1503 as uda_1503,
                                      1504 as uda_1504,
                                      1505 as uda_1505,
                                      1506 as uda_1506,
                                      1507 as uda_1507,
                                      1601 as uda_1601,
                                      1907 as uda_1907,
                                      2102 AS uda_2102,
                                      2301 as uda_2301,
                                      2402 as uda_2402,
                                      2501 as uda_2501,
                                      2503 as uda_2503,
                                      2601 as uda_2601,
                                      2803 as uda_2803,
                                      2804 as uda_2804,
                                      2901 as uda_2901,
                                      3001 as uda_3001,
                                      3002 as uda_3002,
                                      3104 as uda_3104,
                                      910 as uda_910,
                                      905 as uda_905,
                                      542 as uda_542,
                                      3202 as uda_3202,
                                      3502 as uda_3502,
                                      4901 as uda_4901,
                                      5102 as uda_5102,                 --Foods Renewal
                                      5104 as uda_5104,                 --Foods Renewal
                                      5105 as uda_5105,                 --Foods Renewal
                                      5106 as uda_5106,                 --Foods Renewal
                                      5402 as uda_5402,
                                      5403 as uda_5403,
                                      5404 as uda_5404,
                                      5406 as uda_5406,
                                      5407 as uda_5407,
                                      5408 as uda_5408,
                                      5409 as uda_5409,
                                      5301 as uda_5301,
                                      5103 as uda_5103,
                                      5601 as uda_5601,
                                      7501 as uda_7501)); 

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_item_uda%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_item_uda%rowtype;
a_stg_input      stg_array;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_item_no                       := g_rec_in.sk1_item_no;
   g_rec_out.item_no                           := g_rec_in.item_no;
   g_rec_out.merch_class_desc_100              := nvl(g_rec_in.uda_100,'No Value');
   g_rec_out.character_desc_102                := nvl(g_rec_in.uda_102,'No Value');
   g_rec_out.range_structure_ch_desc_104       := nvl(g_rec_in.uda_104,'No Value');
   g_rec_out.royalties_desc_106                := nvl(g_rec_in.uda_106,'No Value');
   g_rec_out.cust_segmentation_desc_300        := nvl(g_rec_in.uda_300,'No Value');
   g_rec_out.garment_length_desc_303           := nvl(g_rec_in.uda_303,'No Value');
   g_rec_out.product_sample_status_desc_304    := nvl(g_rec_in.uda_304,'No Value');
   g_rec_out.price_tier_desc_306               := nvl(g_rec_in.uda_306,'No Value');
   g_rec_out.print_type_desc_307               := nvl(g_rec_in.uda_307,'No Value');
   g_rec_out.top_vs_bottom_desc_309            := nvl(g_rec_in.uda_309,'No Value');
   g_rec_out.plain_vs_design_desc_310          := nvl(g_rec_in.uda_310,'No Value');
   g_rec_out.material_desc_313                 := nvl(g_rec_in.uda_313,'No Value');
   g_rec_out.curtain_hang_method_desc_314      := nvl(g_rec_in.uda_314,'No Value');
   g_rec_out.curtain_lining_desc_315           := nvl(g_rec_in.uda_315,'No Value');
   g_rec_out.lifestyle_desc_316                := nvl(g_rec_in.uda_316,'No Value');
   g_rec_out.sleeve_length_desc_317            := nvl(g_rec_in.uda_317,'No Value');
   g_rec_out.sock_length_desc_318              := nvl(g_rec_in.uda_318,'No Value');
   g_rec_out.single_multiple_desc_319          := nvl(g_rec_in.uda_319,'No Value');
   g_rec_out.sub_brands_desc_320               := nvl(g_rec_in.uda_320,'No Value');
   g_rec_out.shoe_heel_height_desc_321         := nvl(g_rec_in.uda_321,'No Value');
   g_rec_out.range_segmentation_desc_322       := nvl(g_rec_in.uda_322,'No Value');
   g_rec_out.fit_desc_323                      := nvl(g_rec_in.uda_323,'No Value');
   g_rec_out.waist_drop_desc_324               := nvl(g_rec_in.uda_324,'No Value');
   g_rec_out.neck_line_desc_325                := nvl(g_rec_in.uda_325,'No Value');
   g_rec_out.gifting_desc_326                  := nvl(g_rec_in.uda_326,'No Value');
   g_rec_out.silhouette_desc_327               := nvl(g_rec_in.uda_327,'No Value');
   g_rec_out.lighting_desc_329                 := nvl(g_rec_in.uda_329,'No Value');
   g_rec_out.gender_desc_330                   := nvl(g_rec_in.uda_330,'No Value');
   g_rec_out.event_buy_desc_331                := nvl(g_rec_in.uda_331,'No Value');
   g_rec_out.product_category_desc_333         := nvl(g_rec_in.uda_333,'No Value');
   g_rec_out.fabric_type_desc_334              := nvl(g_rec_in.uda_334,'No Value');
   g_rec_out.kids_age_desc_335                 := nvl(g_rec_in.uda_335,'No Value');
   g_rec_out.product_group_scaling_desc_501    := nvl(g_rec_in.uda_501,'No Value');
   g_rec_out.product_class_desc_507            := nvl(g_rec_in.uda_507,'No Value');
   g_rec_out.utilise_ref_item_desc_508         := nvl(g_rec_in.uda_508,'No Value');
   g_rec_out.availability_desc_540             := nvl(g_rec_in.uda_540,'No Value');
   g_rec_out.new_line_launch_date_desc_543     := nvl(g_rec_in.uda_543,'No Value');
   g_rec_out.organics_desc_550                 := nvl(g_rec_in.uda_550,'No Value');
   g_rec_out.vegetarian_desc_551               := nvl(g_rec_in.uda_551,'No Value');
   g_rec_out.kidz_desc_552                     := nvl(g_rec_in.uda_552,'No Value');
   g_rec_out.slimmers_choice_desc_553          := nvl(g_rec_in.uda_553,'No Value');
   g_rec_out.free_range_desc_554               := nvl(g_rec_in.uda_554,'No Value');
   g_rec_out.kosher_desc_555                   := nvl(g_rec_in.uda_555,'No Value');
   g_rec_out.halaal_desc_556                   := nvl(g_rec_in.uda_556,'No Value');
   g_rec_out.foods_range_structure_desc_560    := nvl(g_rec_in.uda_560,'No Value');
   g_rec_out.planning_manager_desc_561         := nvl(g_rec_in.uda_561,'No Value');
   g_rec_out.commercial_manager_desc_562       := nvl(g_rec_in.uda_562,'No Value');
   g_rec_out.stock_error_ind_desc_699          := nvl(g_rec_in.uda_699,'No Value');
   g_rec_out.price_marked_ind_desc_903         := nvl(g_rec_in.uda_903,'No Value');
   g_rec_out.cellular_item_desc_904            := nvl(g_rec_in.uda_904,'No Value');
   g_rec_out.branded_item_desc_911             := nvl(g_rec_in.uda_911,'No Value');
   g_rec_out.fragrance_brand_desc_1002         := nvl(g_rec_in.uda_1002,'No Value');
   g_rec_out.fragrance_house_desc_1003         := nvl(g_rec_in.uda_1003,'No Value');
   g_rec_out.brand_type_desc_1103              := nvl(g_rec_in.uda_1103,'No Value');
   g_rec_out.brand_category_desc_1105          := nvl(g_rec_in.uda_1105,'No Value');
   g_rec_out.digital_brands_desc_1503          := nvl(g_rec_in.uda_1503,'No Value');
   g_rec_out.digital_offer_desc_1504           := nvl(g_rec_in.uda_1504,'No Value');
   g_rec_out.digital_price_bands_desc_1505     := nvl(g_rec_in.uda_1505,'No Value');
   g_rec_out.digital_design_desc_1506          := nvl(g_rec_in.uda_1506,'No Value');
   g_rec_out.digital_data_desc_1507            := nvl(g_rec_in.uda_1507,'No Value');
   g_rec_out.import_item_desc_1601             := nvl(g_rec_in.uda_1601,'No Value');
   g_rec_out.merch_usage_desc_1907             := nvl(g_rec_in.uda_1907,'No Value');
   g_rec_out.headqtr_assortment_desc_2301      := nvl(g_rec_in.uda_2301,'No Value');
   g_rec_out.shoe_heel_shape_desc_2402         := nvl(g_rec_in.uda_2402,'No Value');
   g_rec_out.beauty_brnd_prd_dtl_desc_2501     := nvl(g_rec_in.uda_2501,'No Value');
   g_rec_out.beauty_gift_range_desc_2503       := nvl(g_rec_in.uda_2503,'No Value');
   g_rec_out.handbag_sizing_desc_2601          := nvl(g_rec_in.uda_2601,'No Value');
   g_rec_out.food_main_shop_desc_2803          := nvl(g_rec_in.uda_2803,'No Value');
   g_rec_out.hot_item_desc_2804                := nvl(g_rec_in.uda_2804,'No Value');
   g_rec_out.planning_item_ind_2901            := nvl(g_rec_in.uda_2901,'No Value');

   g_rec_out.PRODUCT_GROUPING_DESC_332         := nvl(g_rec_in.UDA_332,'No Value');
   g_rec_out.BRAND_GIFTING_DESC_1104           := nvl(g_rec_in.UDA_1104 ,'No Value');
   g_rec_out.MARKETING_USE_ONLY_DESC_2102      := nvl(g_rec_in.UDA_2102,'No Value');
   g_rec_out.DIGITAL_SEGMENTATION_DESC_3001    := nvl(g_rec_in.UDA_3001,'No Value');
   g_rec_out.DIGITAL_GENRE_DESC_3002           := nvl(g_rec_in.UDA_3002,'No Value');
   g_rec_out.PRODUCT_DEFFERENCE_3104           := nvl(g_rec_in.UDA_3104,'No Value');
   g_rec_out.LOOSE_ITEM_DESC_910               := nvl(g_rec_in.UDA_910,'No Value');
   g_rec_out.VARIABLE_WEIGHT_ITEM_DESC_905     := nvl(g_rec_in.UDA_905,'No Value');
   g_rec_out.SHORTS_LONGLIFE_DESC_542          := nvl(g_rec_in.UDA_542,'No Value');
   g_rec_out.GREAT_VALUE_DESC_3202             := nvl(g_rec_in.UDA_3202,'No Value');
   g_rec_out.new_line_indicator_desc_3502      := nvl(g_rec_in.uda_3502,'No Value');
   g_rec_out.shelf_end_of_life_prom_4901       := nvl(g_rec_in.uda_4901,'No Value');

   g_rec_out.SBD_INDICATOR_5102                := nvl(g_rec_in.uda_5102, 'No Value');                 --Foods Renewal
   g_rec_out.MIN_SBD_TOLERANCE_5104            := nvl(g_rec_in.uda_5104, 'No Value');                 --Foods Renewal
   g_rec_out.MAX_SBD_TOLERENCE_5105            := nvl(g_rec_in.uda_5105, 'No Value');                 --Foods Renewal
   g_rec_out.MAX_SCAN_ORDER_PERCENTAGE_5106    := nvl(g_rec_in.uda_5106, 'No Value');                 --Foods Renewal

   g_rec_out.consumption_size_desc_5402        := nvl(g_rec_in.uda_5402, 'No Value');
   g_rec_out.health_and_wellness_desc_5403     := nvl(g_rec_in.uda_5403, 'No Value');
   g_rec_out.foods_for_the_future_desc_5404    := nvl(g_rec_in.uda_5404, 'No Value');
   g_rec_out.cross_business_desc_5406          := nvl(g_rec_in.uda_5406, 'No Value');
   g_rec_out.fabulous_flavours_desc_5407       := nvl(g_rec_in.uda_5407, 'No Value');
   g_rec_out.creative_cook_desc_5408           := nvl(g_rec_in.uda_5408, 'No Value');
   g_rec_out.reasons_to_celebrate_desc_5409    := nvl(g_rec_in.uda_5409, 'No Value');
   g_rec_out.zero_night_eol_promo_desc_5301    := nvl(g_rec_in.uda_5301, 'No Value');
   g_rec_out.product_subclass_desc_5103        := nvl(g_rec_in.uda_5103, 'No Value');
   g_rec_out.brand_strategy_desc_5601          := nvl(g_rec_in.uda_5601, 'No Value');
   g_rec_out.wearing_season_DESC_7501          := nvl(g_rec_in.UDA_7501,'No Value');

   g_rec_out.last_updated_date                 := g_date;

   g_rec_out.merchandise_category_desc_100     := 'No Value' ;

   if g_rec_out.merch_class_desc_100 like '%Food-P%' then
      g_rec_out.merchandise_category_desc_100     := 'P' ;
   end if;
   if g_rec_out.merch_class_desc_100 like '%Food-F%' or
      g_rec_out.merch_class_desc_100 like '%Food-L%' then
      g_rec_out.merchandise_category_desc_100     := 'L' ;
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
      insert into dim_item_uda values a_tbl_insert(i);
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
      update dim_item_uda
      set    item_no                        = a_tbl_update(i).item_no,
             merch_class_desc_100           = a_tbl_update(i).merch_class_desc_100,
             merchandise_category_desc_100  = a_tbl_update(i).merchandise_category_desc_100,
             character_desc_102             = a_tbl_update(i).character_desc_102,
             range_structure_ch_desc_104    = a_tbl_update(i).range_structure_ch_desc_104,
             royalties_desc_106             = a_tbl_update(i).royalties_desc_106,
             cust_segmentation_desc_300     = a_tbl_update(i).cust_segmentation_desc_300,
             garment_length_desc_303        = a_tbl_update(i).garment_length_desc_303,
             product_sample_status_desc_304 = a_tbl_update(i).product_sample_status_desc_304,
             price_tier_desc_306            = a_tbl_update(i).price_tier_desc_306,
             print_type_desc_307            = a_tbl_update(i).print_type_desc_307,
             top_vs_bottom_desc_309         = a_tbl_update(i).top_vs_bottom_desc_309,
             plain_vs_design_desc_310       = a_tbl_update(i).plain_vs_design_desc_310,
             material_desc_313              = a_tbl_update(i).material_desc_313,
             curtain_hang_method_desc_314   = a_tbl_update(i).curtain_hang_method_desc_314,
             curtain_lining_desc_315        = a_tbl_update(i).curtain_lining_desc_315,
             lifestyle_desc_316             = a_tbl_update(i).lifestyle_desc_316,
             sleeve_length_desc_317         = a_tbl_update(i).sleeve_length_desc_317,
             sock_length_desc_318           = a_tbl_update(i).sock_length_desc_318,
             single_multiple_desc_319       = a_tbl_update(i).single_multiple_desc_319,
             sub_brands_desc_320            = a_tbl_update(i).sub_brands_desc_320,
             shoe_heel_height_desc_321      = a_tbl_update(i).shoe_heel_height_desc_321,
             range_segmentation_desc_322    = a_tbl_update(i).range_segmentation_desc_322,
             fit_desc_323                   = a_tbl_update(i).fit_desc_323,
             waist_drop_desc_324            = a_tbl_update(i).waist_drop_desc_324,
             neck_line_desc_325             = a_tbl_update(i).neck_line_desc_325,
             gifting_desc_326               = a_tbl_update(i).gifting_desc_326,
             silhouette_desc_327            = a_tbl_update(i).silhouette_desc_327,
             lighting_desc_329              = a_tbl_update(i).lighting_desc_329,
             gender_desc_330                = a_tbl_update(i).gender_desc_330,
             event_buy_desc_331             = a_tbl_update(i).event_buy_desc_331,
             product_category_desc_333      = a_tbl_update(i).product_category_desc_333,
             fabric_type_desc_334           = a_tbl_update(i).fabric_type_desc_334,
             kids_age_desc_335              = a_tbl_update(i).kids_age_desc_335,
             product_group_scaling_desc_501 = a_tbl_update(i).product_group_scaling_desc_501,
             product_class_desc_507         = a_tbl_update(i).product_class_desc_507,
             utilise_ref_item_desc_508      = a_tbl_update(i).utilise_ref_item_desc_508,
             availability_desc_540          = a_tbl_update(i).availability_desc_540,
             new_line_launch_date_desc_543  = a_tbl_update(i).new_line_launch_date_desc_543,
             organics_desc_550              = a_tbl_update(i).organics_desc_550,
             vegetarian_desc_551            = a_tbl_update(i).vegetarian_desc_551,
             kidz_desc_552                  = a_tbl_update(i).kidz_desc_552,
             slimmers_choice_desc_553       = a_tbl_update(i).slimmers_choice_desc_553,
             free_range_desc_554            = a_tbl_update(i).free_range_desc_554,
             kosher_desc_555                = a_tbl_update(i).kosher_desc_555,
             halaal_desc_556                = a_tbl_update(i).halaal_desc_556,
             foods_range_structure_desc_560 = a_tbl_update(i).foods_range_structure_desc_560,
             planning_manager_desc_561      = a_tbl_update(i).planning_manager_desc_561,
             commercial_manager_desc_562    = a_tbl_update(i).commercial_manager_desc_562,
             stock_error_ind_desc_699       = a_tbl_update(i).stock_error_ind_desc_699,
             price_marked_ind_desc_903      = a_tbl_update(i).price_marked_ind_desc_903,
             cellular_item_desc_904         = a_tbl_update(i).cellular_item_desc_904,
             branded_item_desc_911          = a_tbl_update(i).branded_item_desc_911,
             fragrance_brand_desc_1002      = a_tbl_update(i).fragrance_brand_desc_1002,
             fragrance_house_desc_1003      = a_tbl_update(i).fragrance_house_desc_1003,
             brand_type_desc_1103           = a_tbl_update(i).brand_type_desc_1103,
             brand_category_desc_1105       = a_tbl_update(i).brand_category_desc_1105,
             digital_brands_desc_1503       = a_tbl_update(i).digital_brands_desc_1503,
             digital_offer_desc_1504        = a_tbl_update(i).digital_offer_desc_1504,
             digital_price_bands_desc_1505  = a_tbl_update(i).digital_price_bands_desc_1505,
             digital_design_desc_1506       = a_tbl_update(i).digital_design_desc_1506,
             digital_data_desc_1507         = a_tbl_update(i).digital_data_desc_1507,
             import_item_desc_1601          = a_tbl_update(i).import_item_desc_1601,
             merch_usage_desc_1907          = a_tbl_update(i).merch_usage_desc_1907,
             headqtr_assortment_desc_2301   = a_tbl_update(i).headqtr_assortment_desc_2301,
             shoe_heel_shape_desc_2402      = a_tbl_update(i).shoe_heel_shape_desc_2402,
             beauty_brnd_prd_dtl_desc_2501  = a_tbl_update(i).beauty_brnd_prd_dtl_desc_2501,
             beauty_gift_range_desc_2503    = a_tbl_update(i).beauty_gift_range_desc_2503,
             handbag_sizing_desc_2601       = a_tbl_update(i).handbag_sizing_desc_2601,
             food_main_shop_desc_2803       = a_tbl_update(i).food_main_shop_desc_2803,
             hot_item_desc_2804             = a_tbl_update(i).hot_item_desc_2804,
             planning_item_ind_2901         = a_tbl_update(i).planning_item_ind_2901,
             last_updated_date              = a_tbl_update(i).last_updated_date,
             PRODUCT_GROUPING_DESC_332      = a_tbl_update(i).PRODUCT_GROUPING_DESC_332,
             BRAND_GIFTING_DESC_1104        = a_tbl_update(i).BRAND_GIFTING_DESC_1104 ,
             MARKETING_USE_ONLY_DESC_2102   = a_tbl_update(i).MARKETING_USE_ONLY_DESC_2102 ,
             DIGITAL_SEGMENTATION_DESC_3001 = a_tbl_update(i).DIGITAL_SEGMENTATION_DESC_3001 ,
             DIGITAL_GENRE_DESC_3002        = a_tbl_update(i).DIGITAL_GENRE_DESC_3002 ,
             PRODUCT_DEFFERENCE_3104        = a_tbl_update(i).PRODUCT_DEFFERENCE_3104,
             LOOSE_ITEM_DESC_910            = a_tbl_update(i).LOOSE_ITEM_DESC_910,
             VARIABLE_WEIGHT_ITEM_DESC_905  = a_tbl_update(i).VARIABLE_WEIGHT_ITEM_DESC_905,
             SHORTS_LONGLIFE_DESC_542       = a_tbl_update(i).SHORTS_LONGLIFE_DESC_542,
             great_value_desc_3202          = a_tbl_update(i).great_value_desc_3202,
             new_line_indicator_desc_3502   = a_tbl_update(i).new_line_indicator_desc_3502,
             shelf_end_of_life_prom_4901    = a_tbl_update(i).shelf_end_of_life_prom_4901,
             sbd_indicator_5102             = a_tbl_update(i).sbd_indicator_5102,                   --Foods Renewal
             MIN_SBD_TOLERANCE_5104         = a_tbl_update(i).MIN_SBD_TOLERANCE_5104,               --Foods Renewal
             max_sbd_tolerence_5105         = a_tbl_update(i).max_sbd_tolerence_5105,               --Foods Renewal
             max_scan_order_percentage_5106 = a_tbl_update(i).max_scan_order_percentage_5106,       --Foods Renewal
             consumption_size_desc_5402     = a_tbl_update(i).consumption_size_desc_5402,
             health_and_wellness_desc_5403  = a_tbl_update(i).health_and_wellness_desc_5403,
             foods_for_the_future_desc_5404 = a_tbl_update(i).foods_for_the_future_desc_5404,
             cross_business_desc_5406       = a_tbl_update(i).cross_business_desc_5406,
             fabulous_flavours_desc_5407    = a_tbl_update(i).fabulous_flavours_desc_5407,
             creative_cook_desc_5408        = a_tbl_update(i).creative_cook_desc_5408,
             reasons_to_celebrate_desc_5409 = a_tbl_update(i).reasons_to_celebrate_desc_5409,
             zero_night_eol_promo_desc_5301 = a_tbl_update(i).zero_night_eol_promo_desc_5301,
             product_subclass_desc_5103     = a_tbl_update(i).product_subclass_desc_5103,
             brand_strategy_desc_5601       = a_tbl_update(i).brand_strategy_desc_5601,
             wearing_season_desc_7501       = a_tbl_update(i).wearing_season_desc_7501

      where  sk1_item_no                    = a_tbl_update(i).sk1_item_no;

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

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into g_count
   from dim_item_uda
   where sk1_item_no = g_rec_out.sk1_item_no;

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

    dbms_output.put_line('Loading data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD DIM_ITEM_UDA EX FND_ITEM_UDA STARTED AT '||
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
    open c_fnd_item_uda;
    fetch c_fnd_item_uda bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_item_uda bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_item_uda;
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

end wh_prf_corp_048u;
