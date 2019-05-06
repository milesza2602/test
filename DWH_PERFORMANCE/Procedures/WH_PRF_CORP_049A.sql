--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_049A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_049A" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Oct 2010
--  Author:      Alastair de Wet
--  Purpose:     Identify duplicate UDA names in the item to style_color rollup which are illegal
--               and need to be corrected at source.
--  Tables:      Input  -   dim_item_uda, dim_item
--               Output -   Log file of duplicate UDA description
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 29 Dec 2017 - A. Ugolini  - Add field WEARING_SEASON_DESC_7501 on DIM_ITEM_UDA
--                             & DIM_SC_UDA
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
--g_rec_out            dim_sc_uda%rowtype;
g_found              boolean;
g_uda                Varchar2(30);
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;
w_count              number        :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_049A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'IDENTIFY DUPLICATE UDA NAMES PER ITEM STYLE/COLOR';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
/*
type tbl_array_i is table of DWH_PERFORMANCE.dim_sc_uda%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.dim_sc_uda%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
*/
cursor c_dim_item_uda is
   select di.SK1_STYLE_COLOUR_NO as SK1_STYLE_COLOUR_NO,
          di.STYLE_COLOUR_NO as STYLE_COLOUR_NO,
          COUNT (DISTINCT (nvl(diu.CHARACTER_DESC_102,'No Value'))) as DESC_102,
          COUNT (DISTINCT (nvl(diu.CURTAIN_HANG_METHOD_DESC_314,'No Value'))) as DESC_314,
          COUNT (DISTINCT (nvl(diu.CURTAIN_LINING_DESC_315,'No Value'))) as DESC_315,
          COUNT (DISTINCT (nvl(diu.CUST_SEGMENTATION_DESC_300 ,'No Value'))) as DESC_300,
          COUNT (DISTINCT (nvl(diu.EVENT_BUY_DESC_331,'No Value'))) as DESC_331,
          COUNT (DISTINCT (nvl(diu.FABRIC_TYPE_DESC_334 ,'No Value'))) as DESC_334,
          COUNT (DISTINCT (nvl(diu.FIT_DESC_323,'No Value'))) as DESC_323,
          COUNT (DISTINCT (nvl(diu.GENDER_DESC_330,'No Value'))) as DESC_330,
          COUNT (DISTINCT (nvl(diu.GIFTING_DESC_326 ,'No Value'))) as DESC_326,
          COUNT (DISTINCT (nvl(diu.GARMENT_LENGTH_DESC_303,'No Value'))) as DESC_303,
          COUNT (DISTINCT (nvl(diu.LIFESTYLE_DESC_316,'No Value'))) as DESC_316,
          COUNT (DISTINCT (nvl(diu.LIGHTING_DESC_329,'No Value'))) as DESC_329,
          COUNT (DISTINCT (nvl(diu.MATERIAL_DESC_313,'No Value'))) as DESC_313,
          COUNT (DISTINCT (nvl(diu.MERCH_CLASS_DESC_100,'No Value'))) as DESC_100,
          COUNT (DISTINCT (nvl(diu.NECK_LINE_DESC_325,'No Value'))) as DESC_325,
          COUNT (DISTINCT (nvl(diu.PLAIN_VS_DESIGN_DESC_310,'No Value'))) as DESC_310,
          COUNT (DISTINCT (nvl(diu.PRICE_TIER_DESC_306,'No Value'))) as DESC_306,
          COUNT (DISTINCT (nvl(diu.PRINT_TYPE_DESC_307 ,'No Value'))) as DESC_307,
          COUNT (DISTINCT (nvl(diu.RANGE_SEGMENTATION_DESC_322,'No Value'))) as DESC_322,
          COUNT (DISTINCT (nvl(diu.RANGE_STRUCTURE_CH_DESC_104,'No Value'))) as DESC_104,
          COUNT (DISTINCT (nvl(diu.ROYALTIES_DESC_106,'No Value'))) as DESC_106,
          COUNT (DISTINCT (nvl(diu.SHOE_HEEL_HEIGHT_DESC_321,'No Value'))) as DESC_321,
          COUNT (DISTINCT (nvl(diu.SILHOUETTE_DESC_327,'No Value'))) as DESC_327,
          COUNT (DISTINCT (nvl(diu.SINGLE_MULTIPLE_DESC_319,'No Value'))) as DESC_319,
          COUNT (DISTINCT (nvl(diu.SLEEVE_LENGTH_DESC_317,'No Value'))) as DESC_317,
          COUNT (DISTINCT (nvl(diu.SOCK_LENGTH_DESC_318,'No Value'))) as DESC_318,
          COUNT (DISTINCT (nvl(diu.SUB_BRANDS_DESC_320,'No Value'))) as DESC_320,
          COUNT (DISTINCT (nvl(diu.TOP_VS_BOTTOM_DESC_309,'No Value'))) as DESC_309,
          COUNT (DISTINCT (nvl(diu.WAIST_DROP_DESC_324,'No Value'))) as DESC_324,
          COUNT (DISTINCT (nvl(diu.CELLULAR_ITEM_DESC_904,'No Value'))) as DESC_904,
          COUNT (DISTINCT (nvl(diu.PRODUCT_SAMPLE_STATUS_DESC_304,'No Value'))) as DESC_304,
          COUNT (DISTINCT (nvl(diu.UTILISE_REF_ITEM_DESC_508,'No Value'))) as DESC_508,
          COUNT (DISTINCT (nvl(diu.PRICE_MARKED_IND_DESC_903,'No Value'))) as DESC_903,
          COUNT (DISTINCT (nvl(diu.DIGITAL_BRANDS_DESC_1503,'No Value'))) as DESC_1503,
          COUNT (DISTINCT (nvl(diu.DIGITAL_OFFER_DESC_1504,'No Value'))) as DESC_1504,
          COUNT (DISTINCT (nvl(diu.DIGITAL_PRICE_BANDS_DESC_1505,'No Value'))) as DESC_1505,
          COUNT (DISTINCT (nvl(diu.DIGITAL_DESIGN_DESC_1506,'No Value'))) as DESC_1506,
          COUNT (DISTINCT (nvl(diu.DIGITAL_DATA_DESC_1507 ,'No Value'))) as DESC_1507,
          COUNT (DISTINCT (nvl(diu.MERCH_USAGE_DESC_1907,'No Value'))) as DESC_1907,
          COUNT (DISTINCT (nvl(diu.KIDS_AGE_DESC_335,'No Value'))) as DESC_335,
          COUNT (DISTINCT (nvl(diu.AVAILABILITY_DESC_540,'No Value'))) as DESC_540,
          COUNT (DISTINCT (nvl(diu.PLANNING_MANAGER_DESC_561,'No Value'))) as DESC_561,
          COUNT (DISTINCT (nvl(diu.COMMERCIAL_MANAGER_DESC_562,'No Value'))) as DESC_562,
          COUNT (DISTINCT (nvl(diu.MERCHANDISE_CATEGORY_DESC_100,'No Value'))) as DESC_100A,
          COUNT (DISTINCT (nvl(diu.PRODUCT_CLASS_DESC_507,'No Value'))) as DESC_507,
          COUNT (DISTINCT (nvl(diu.PRODUCT_GROUP_SCALING_DESC_501,'No Value'))) as DESC_501,
          COUNT (DISTINCT (nvl(diu.ORGANICS_DESC_550,'No Value'))) as DESC_550,
          COUNT (DISTINCT (nvl(diu.KIDZ_DESC_552,'No Value'))) as DESC_552,
          COUNT (DISTINCT (nvl(diu.FREE_RANGE_DESC_554,'No Value'))) as DESC_554,
          COUNT (DISTINCT (nvl(diu.VEGETARIAN_DESC_551,'No Value'))) as DESC_551,
          COUNT (DISTINCT (nvl(diu.SLIMMERS_CHOICE_DESC_553,'No Value'))) as DESC_553,
          COUNT (DISTINCT (nvl(diu.KOSHER_DESC_555,'No Value'))) as DESC_555,
          COUNT (DISTINCT (nvl(diu.HALAAL_DESC_556,'No Value'))) as DESC_556,
          COUNT (DISTINCT (nvl(diu.BRANDED_ITEM_DESC_911,'No Value'))) as DESC_911,
          COUNT (DISTINCT (nvl(diu.IMPORT_ITEM_DESC_1601 ,'No Value'))) as DESC_1601,
          COUNT (DISTINCT (nvl(diu.NEW_LINE_LAUNCH_DATE_DESC_543,'No Value'))) as DESC_543,
          COUNT (DISTINCT (nvl(diu.FOODS_RANGE_STRUCTURE_DESC_560,'No Value'))) as DESC_560,
          COUNT (DISTINCT (nvl(diu.FRAGRANCE_BRAND_DESC_1002,'No Value'))) as DESC_1002,
          COUNT (DISTINCT (nvl(diu.FRAGRANCE_HOUSE_DESC_1003,'No Value'))) as DESC_1003,
          COUNT (DISTINCT (nvl(diu.HEADQTR_ASSORTMENT_DESC_2301,'No Value'))) as DESC_2301,
          COUNT (DISTINCT (nvl(diu.FOOD_MAIN_SHOP_DESC_2803,'No Value'))) as DESC_2803,
          COUNT (DISTINCT (nvl(diu.HOT_ITEM_DESC_2804,'No Value'))) as DESC_2804,
          COUNT (DISTINCT (nvl(diu.PRODUCT_CATEGORY_DESC_333,'No Value'))) as DESC_333,
          COUNT (DISTINCT (nvl(diu.BRAND_TYPE_DESC_1103,'No Value'))) as DESC_1103,
          COUNT (DISTINCT (nvl(diu.BRAND_CATEGORY_DESC_1105,'No Value'))) as DESC_1105,
          COUNT (DISTINCT (nvl(diu.SHOE_HEEL_SHAPE_DESC_2402,'No Value'))) as DESC_2402,
          COUNT (DISTINCT (nvl(diu.BEAUTY_BRND_PRD_DTL_DESC_2501,'No Value'))) as DESC_2501,
          COUNT (DISTINCT (nvl(diu.BEAUTY_GIFT_RANGE_DESC_2503,'No Value'))) as DESC_2503,
          COUNT (DISTINCT (nvl(diu.HANDBAG_SIZING_DESC_2601,'No Value'))) as DESC_2601,
          COUNT (DISTINCT (nvl(diu.PLANNING_ITEM_IND_2901,'No Value'))) as DESC_2901,
          COUNT (DISTINCT (nvl(diu.STOCK_ERROR_IND_DESC_699,'No Value'))) as DESC_699,
          COUNT (DISTINCT (nvl(diu.PRODUCT_GROUPING_DESC_332,'No Value'))) as DESC_332,
          COUNT (DISTINCT (nvl(diu.BRAND_GIFTING_DESC_1104,'No Value'))) as DESC_1104,
          COUNT (DISTINCT (nvl(diu.MARKETING_USE_ONLY_DESC_2102,'No Value'))) as DESC_2102,
          COUNT (DISTINCT (nvl(diu.DIGITAL_SEGMENTATION_DESC_3001,'No Value'))) as DESC_3001,
          COUNT (DISTINCT (nvl(diu.DIGITAL_GENRE_DESC_3002,'No Value'))) as DESC_3002,
          COUNT (DISTINCT (nvl(diu.PRODUCT_DEFFERENCE_3104,'No Value'))) as DESC_3104,
          COUNT (DISTINCT (nvl(diu.LOOSE_ITEM_DESC_910,'No Value'))) as DESC_910,
          COUNT (DISTINCT (nvl(diu.VARIABLE_WEIGHT_ITEM_DESC_905,'No Value'))) as DESC_905,
          COUNT (DISTINCT (nvl(diu.SHORTS_LONGLIFE_DESC_542,'No Value'))) as DESC_542,
          COUNT (DISTINCT (nvl(diu.GREAT_VALUE_DESC_3202,'No Value'))) as DESC_3202,
          COUNT (DISTINCT (nvl(diu.NEW_LINE_INDICATOR_DESC_3502,'No Value'))) as DESC_3502, 
          COUNT (DISTINCT (nvl(diu.wearing_season_DESC_7501,'No Value'))) as DESC_7501
          FROM DWH_PERFORMANCE.DIM_ITEM_UDA DIU, DWH_PERFORMANCE.DIM_ITEM DI
          WHERE DI.SK1_ITEM_NO = DIU.SK1_ITEM_NO
      and di.sk1_style_colour_no is not null
      and di.item_level_no = di.tran_level_no
          group by di.SK1_STYLE_COLOUR_NO, di.STYLE_COLOUR_NO ;
--
-- Input record declared as cursor%rowtype
g_rec_in             c_dim_item_uda%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_dim_item_uda%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

--   g_rec_out.SK1_STYLE_COLOUR_NO := g_rec_in.SK1_STYLE_COLOUR_NO;
--   g_rec_out.STYLE_COLOUR_NO := g_rec_in.STYLE_COLOUR_NO;
--83
  if g_rec_in.DESC_102  + g_rec_in.DESC_314  + g_rec_in.DESC_315  + g_rec_in.DESC_300  + g_rec_in.DESC_331  + g_rec_in.DESC_334 +
     g_rec_in.DESC_323  + g_rec_in.DESC_330  + g_rec_in.DESC_326  + g_rec_in.DESC_303  + g_rec_in.DESC_316  + g_rec_in.DESC_329 +
     g_rec_in.DESC_313  + g_rec_in.DESC_100  + g_rec_in.DESC_325  + g_rec_in.DESC_310  + g_rec_in.DESC_306  + g_rec_in.DESC_307 +
     g_rec_in.DESC_322  + g_rec_in.DESC_104  + g_rec_in.DESC_106  + g_rec_in.DESC_321  + g_rec_in.DESC_327  + g_rec_in.DESC_319 +
     g_rec_in.DESC_317  + g_rec_in.DESC_318  + g_rec_in.DESC_320  + g_rec_in.DESC_309  + g_rec_in.DESC_324  + g_rec_in.DESC_904 +
     g_rec_in.DESC_304  + g_rec_in.DESC_508  + g_rec_in.DESC_903  + g_rec_in.DESC_1503 + g_rec_in.DESC_1504 + g_rec_in.DESC_1505 +
     g_rec_in.DESC_1506 + g_rec_in.DESC_1507 + g_rec_in.DESC_1907 + g_rec_in.DESC_335  + g_rec_in.DESC_540  + g_rec_in.DESC_561 +
     g_rec_in.DESC_562  + g_rec_in.DESC_100A + g_rec_in.DESC_507  + g_rec_in.DESC_501  + g_rec_in.DESC_550  + g_rec_in.DESC_552 +
     g_rec_in.DESC_554  + g_rec_in.DESC_551  + g_rec_in.DESC_553  + g_rec_in.DESC_555  + g_rec_in.DESC_556  + g_rec_in.DESC_911 +
     g_rec_in.DESC_1601 + g_rec_in.DESC_543  + g_rec_in.DESC_560  + g_rec_in.DESC_1002 + g_rec_in.DESC_1003 + g_rec_in.DESC_2301 +
     g_rec_in.DESC_2803 + g_rec_in.DESC_2804 + g_rec_in.DESC_333  + g_rec_in.DESC_1103 + g_rec_in.DESC_1105 + g_rec_in.DESC_2402 +
     g_rec_in.DESC_2501 + g_rec_in.DESC_2503 + g_rec_in.DESC_2901 + g_rec_in.DESC_2601 + g_rec_in.DESC_699  + g_rec_in.DESC_332 +
     g_rec_in.DESC_1104 + g_rec_in.DESC_2102 + g_rec_in.DESC_3001 + g_rec_in.DESC_3002 + g_rec_in.DESC_3104 + g_rec_in.DESC_910 +
     g_rec_in.DESC_905  + g_rec_in.DESC_542  + g_rec_in.DESC_3202 + g_rec_in.DESC_3502 + g_rec_in.DESC_7501 
      > 83 then
       if g_rec_in.DESC_3502 > 1 then g_uda := 'NEW_LINE_INDICATOR_DESC_3502';    end if;
       if g_rec_in.DESC_3202 > 1 then g_uda := 'GREAT_VALUE_DESC_3202';           end if;
       if g_rec_in.DESC_905  > 1 then g_uda := 'VARIABLE_WEIGHT_ITEM_DESC_905';   end if;
       if g_rec_in.DESC_542  > 1 then g_uda := 'SHORTS_LONGLIFE_DESC_542';        end if;
       if g_rec_in.DESC_910  > 1 then g_uda := 'LOOSE_ITEM_DESC_910';             end if;
       if g_rec_in.DESC_3104 > 1 then g_uda := 'PRODUCT_DEFFERENCE_3104';         end if;
       if g_rec_in.DESC_3002 > 1 then g_uda := 'DIGITAL_GENRE_DESC_3002';         end if;
       if g_rec_in.DESC_3001 > 1 then g_uda := 'DIGITAL_SEGMENTATION_DESC_3001';  end if;
       if g_rec_in.DESC_2102 > 1 then g_uda := 'MARKETING_USE_ONLY_DESC_2102';    end if;
       if g_rec_in.DESC_1104 > 1 then g_uda := 'BRAND_GIFTING_DESC_1104';         end if;
       if g_rec_in.DESC_332  > 1 then g_uda := 'PRODUCT_GROUPING_DESC_332';       end if;
       if g_rec_in.DESC_699  > 1 then g_uda := 'STOCK_ERROR_IND_DESC_699';        end if;
       if g_rec_in.DESC_2601 > 1 then g_uda := 'HANDBAG_SIZING_DESC_2601';        end if;
       if g_rec_in.DESC_2901 > 1 then g_uda := 'PLANNING_ITEM_IND_2901';          end if;
       if g_rec_in.DESC_2503 > 1 then g_uda := 'BEAUTY_GIFT_RANGE_DESC_2503';     end if;
       if g_rec_in.DESC_2501 > 1 then g_uda := 'BEAUTY_BRND_PRD_DTL_DESC_2501';   end if;
       if g_rec_in.DESC_2402 > 1 then g_uda := 'SHOE_HEEL_SHAPE_DESC_2402';       end if;
       if g_rec_in.DESC_1105 > 1 then g_uda := 'BRAND_CATEGORY_DESC_1105';        end if;
       if g_rec_in.DESC_1103 > 1 then g_uda := 'BRAND_TYPE_DESC_1103';            end if;
       if g_rec_in.DESC_333  > 1 then g_uda := 'PRODUCT_CATEGORY_DESC_333';       end if;
       if g_rec_in.DESC_2804 > 1 then g_uda := 'HOT_ITEM_DESC_2804';              end if;
       if g_rec_in.DESC_2803 > 1 then g_uda := 'FOOD_MAIN_SHOP_DESC_2803';        end if;
       if g_rec_in.DESC_2301 > 1 then g_uda := 'HEADQTR_ASSORTMENT_DESC_2301';    end if;
       if g_rec_in.DESC_1003 > 1 then g_uda := 'FRAGRANCE_HOUSE_DESC_1003';       end if;
       if g_rec_in.DESC_1002 > 1 then g_uda := 'FRAGRANCE_BRAND_DESC_1002';       end if;
       if g_rec_in.DESC_560  > 1 then g_uda := 'FOODS_RANGE_STRUCTURE_DESC_560';  end if;
       if g_rec_in.DESC_543  > 1 then g_uda := 'NEW_LINE_LAUNCH_DATE_DESC_543';   end if;
       if g_rec_in.DESC_1601 > 1 then g_uda := 'IMPORT_ITEM_DESC_1601 ';          end if;
       if g_rec_in.DESC_911  > 1 then g_uda := 'BRANDED_ITEM_DESC_911';           end if;
       if g_rec_in.DESC_556  > 1 then g_uda := 'HALAAL_DESC_556';                 end if;
       if g_rec_in.DESC_555  > 1 then g_uda := 'KOSHER_DESC_555';                 end if;
       if g_rec_in.DESC_553  > 1 then g_uda := 'SLIMMERS_CHOICE_DESC_553';        end if;
       if g_rec_in.DESC_551  > 1 then g_uda := 'VEGETARIAN_DESC_551';             end if;
       if g_rec_in.DESC_554  > 1 then g_uda := 'FREE_RANGE_DESC_554';             end if;
       if g_rec_in.DESC_552  > 1 then g_uda := 'KIDZ_DESC_552';                   end if;
       if g_rec_in.DESC_550  > 1 then g_uda := 'ORGANICS_DESC_550';               end if;
       if g_rec_in.DESC_501  > 1 then g_uda := 'PRODUCT_GROUP_SCALING_DESC_501';  end if;
       if g_rec_in.DESC_507  > 1 then g_uda := 'PRODUCT_CLASS_DESC_507';          end if;
       if g_rec_in.DESC_100a > 1 then g_uda := 'MERCHANDISE_CATEGORY_DESC_100';   end if;
       if g_rec_in.DESC_562  > 1 then g_uda := 'COMMERCIAL_MANAGER_DESC_562';     end if;
       if g_rec_in.DESC_561  > 1 then g_uda := 'PLANNING_MANAGER_DESC_561';       end if;
       if g_rec_in.DESC_540  > 1 then g_uda := 'AVAILABILITY_DESC_540';           end if;
       if g_rec_in.DESC_335  > 1 then g_uda := 'KIDS_AGE_DESC_335';               end if;
       if g_rec_in.DESC_1907 > 1 then g_uda := 'MERCH_USAGE_DESC_1907';           end if;
       if g_rec_in.DESC_1507 > 1 then g_uda := 'DIGITAL_DATA_DESC_1507 ';         end if;
       if g_rec_in.DESC_1506 > 1 then g_uda := 'DIGITAL_DESIGN_DESC_1506';        end if;
       if g_rec_in.DESC_1505 > 1 then g_uda := 'DIGITAL_PRICE_BANDS_DESC_1505';   end if;
       if g_rec_in.DESC_1504 > 1 then g_uda := 'DIGITAL_OFFER_DESC_1504';         end if;
       if g_rec_in.DESC_1503 > 1 then g_uda := 'DIGITAL_BRANDS_DESC_1503';        end if;
       if g_rec_in.DESC_903  > 1 then g_uda := 'PRICE_MARKED_IND_DESC_903';       end if;
       if g_rec_in.DESC_508  > 1 then g_uda := 'UTILISE_REF_ITEM_DESC_508';       end if;
       if g_rec_in.DESC_304  > 1 then g_uda := 'PRODUCT_SAMPLE_STATUS_DESC_304';  end if;
       if g_rec_in.DESC_904  > 1 then g_uda := 'CELLULAR_ITEM_DESC_904';          end if;
       if g_rec_in.DESC_324  > 1 then g_uda := 'WAIST_DROP_DESC_324';             end if;
       if g_rec_in.DESC_309  > 1 then g_uda := 'TOP_VS_BOTTOM_DESC_309';          end if;
       if g_rec_in.DESC_320  > 1 then g_uda := 'SUB_BRANDS_DESC_320';             end if;
       if g_rec_in.DESC_318  > 1 then g_uda := 'SOCK_LENGTH_DESC_318';            end if;
       if g_rec_in.DESC_317  > 1 then g_uda := 'SLEEVE_LENGTH_DESC_317';          end if;
       if g_rec_in.DESC_319  > 1 then g_uda := 'SINGLE_MULTIPLE_DESC_319';        end if;
       if g_rec_in.DESC_327  > 1 then g_uda := 'SILHOUETTE_DESC_327';             end if;
       if g_rec_in.DESC_321  > 1 then g_uda := 'SHOE_HEEL_HEIGHT_DESC_321';       end if;
       if g_rec_in.DESC_106  > 1 then g_uda := 'ROYALTIES_DESC_106';              end if;
       if g_rec_in.DESC_104  > 1 then g_uda := 'RANGE_STRUCTURE_CH_DESC_104';     end if;
       if g_rec_in.DESC_322  > 1 then g_uda := 'RANGE_SEGMENTATION_DESC_322';     end if;
       if g_rec_in.DESC_307  > 1 then g_uda := 'PRINT_TYPE_DESC_307';             end if;
       if g_rec_in.DESC_306  > 1 then g_uda := 'PRICE_TIER_DESC_306';             end if;
       if g_rec_in.DESC_310  > 1 then g_uda := 'PLAIN_VS_DESIGN_DESC_310';        end if;
       if g_rec_in.DESC_325  > 1 then g_uda := 'NECK_LINE_DESC_325';              end if;
       if g_rec_in.DESC_100  > 1 then g_uda := 'MERCH_CLASS_DESC_100';            end if;
       if g_rec_in.DESC_313  > 1 then g_uda := 'MATERIAL_DESC_313';               end if;
       if g_rec_in.DESC_329  > 1 then g_uda := 'LIGHTING_DESC_329';               end if;
       if g_rec_in.DESC_316  > 1 then g_uda := 'LIFESTYLE_DESC_316';              end if;
       if g_rec_in.DESC_303  > 1 then g_uda := 'GARMENT_LENGTH_DESC_303';         end if;
       if g_rec_in.DESC_326  > 1 then g_uda := 'GIFTING_DESC_326';                end if;
       if g_rec_in.DESC_330  > 1 then g_uda := 'GENDER_DESC_330';                 end if;
       if g_rec_in.DESC_323  > 1 then g_uda := 'FIT_DESC_323';                    end if;
       if g_rec_in.DESC_334  > 1 then g_uda := 'FABRIC_TYPE_DESC_334';            end if;
       if g_rec_in.DESC_331  > 1 then g_uda := 'EVENT_BUY_DESC_331';              end if;
       if g_rec_in.DESC_300  > 1 then g_uda := 'CUST_SEGMENTATION_DESC_300';      end if;
       if g_rec_in.DESC_315  > 1 then g_uda := 'CURTAIN_LINING_DESC_315';         end if;
       if g_rec_in.DESC_314  > 1 then g_uda := 'CURTAIN_HANG_METHOD_DESC_314';    end if;
       if g_rec_in.DESC_102  > 1 then g_uda := 'CHARACTER_DESC_102';              end if;
       if g_rec_in.DESC_7501  > 1 then g_uda := 'WEARING_SEASON_DESC_7501';       end if;
      
      l_text := 'DUPLICATE STYLE_COL UDA = '||g_rec_in.STYLE_COLOUR_NO||' '||G_UDA;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     g_recs_updated := g_recs_updated + 1;
     DBMS_OUTPUT.PUT_LINE('STYLE_COLOUR_NO '||g_rec_in.STYLE_COLOUR_NO||' Has duplicate uda descriptions in UDA '||g_uda);
   end if;  

   exception
      when others then
--      DBMS_OUTPUT.PUT_LINE(' LAV - SK1_STYLE_COLOUR_NO='||g_rec_in.SK1_STYLE_COLOUR_NO||' SK1_STYLE_COLOUR_NO='||g_rec_in.SK1_STYLE_COLOUR_NO);
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
DBMS_OUTPUT.ENABLE(10000000);
--    dbms_output.put_line('Loading data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'DUPLICATE DETECT STARTED AT '||
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
      

      end loop;
    fetch c_dim_item_uda bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_item_uda;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
  
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'Duplicates detected =  '||g_recs_updated;
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
end wh_prf_corp_049a;
