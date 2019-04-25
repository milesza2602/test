--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_NEW_GENERIC_INSERT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_NEW_GENERIC_INSERT" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        10 February 2015
--  Author:      Mapopa Phiri
--  Purpose:     Creating a New Generic structure for new items in Data Ware House (DWH)
--               in a Foundation Layer.
--
-- Method:      This procedure is used to create a new generic item in Data ware house. The way it works is that, you remove the comments on insert statement
--              then edit the insert statement by addining the new items. Once this is done you have to run the procedure to insert new generic items.
--
--  Naming conventions
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
g_truncate_count     INTEGER       :=  0;

g_start_date         date;
g_end_date           date;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_NEW_GENERIC_INSERT';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE A NEW GENERIC STRUCTURE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
--insert into FND_SUBGROUP (SUBGROUP_NO,SUBGROUP_NAME,GROUP_NO,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3004,'COUNTRY ROAD GEN SUBGRP',12,null,TO_DATE('27/APR/09','DD/MON/RR'));
--Insert into FND_SUBGROUP (SUBGROUP_NO,SUBGROUP_NAME,GROUP_NO,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3005,'TRENERY GEN SUBGRP',4,null,to_date('27/APR/09','DD/MON/RR'));
--Insert into FND_SUBGROUP (SUBGROUP_NO,SUBGROUP_NAME,GROUP_NO,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3007,'MIMCO GEN SUBGRP',14,null,to_date('27/APR/09','DD/MON/RR'));
--Insert Into FND_SUBGROUP (Subgroup_No,Subgroup_Name,Group_No,Source_Data_Status_Code,Last_Updated_Date) Values (3009,'WITCHERY GEN SUBGRP',13,Null,To_Date('27/APR/09','DD/MON/RR'));


--insert into FND_DEPARTMENT (DEPARTMENT_NO,DEPARTMENT_NAME,SUBGROUP_NO,BUDGET_INTK_PERC,BUDGET_MKUP_PERC,TOTAL_MARKET_AMT,MARKUP_CALC_TYPE,VAT_IND,OTB_CALC_TYPE,NUM_MAX_AVG_COUNTER_DAYS,AVG_TOLERANCE_PERC,BUYER_NO,MERCH_NO,PROFIT_CALC_TYPE,PURCHASE_TYPE,SOURCE_DATA_STATUS_CODE,JV_DEPT_IND,PACKAGING_DEPT_IND,GIFTING_DEPT_IND,NON_MERCH_DEPT_IND,NON_CORE_DEPT_IND,BUCKET_DEPT_IND,BOOK_MAGAZINE_DEPT_IND,DEPT_PLACEHOLDER_01_IND,DEPT_PLACEHOLDER_02_IND,DEPT_PLACEHOLDER_03_IND,DEPT_PLACEHOLDER_04_IND,DEPT_PLACEHOLDER_05_IND,DEPT_PLACEHOLDER_06_IND,DEPT_PLACEHOLDER_07_IND,DEPT_PLACEHOLDER_08_IND,DEPT_PLACEHOLDER_09_IND,DEPT_PLACEHOLDER_10_IND,LAST_UPDATED_DATE) values (3006,'COUNTRY ROAD GEN DEPT',3004,null,null,null,null,0,null,null,null,null,null,null,null,null,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,TO_DATE('27/APR/09','DD/MON/RR'));
--Insert into FND_DEPARTMENT (DEPARTMENT_NO,DEPARTMENT_NAME,SUBGROUP_NO,BUDGET_INTK_PERC,BUDGET_MKUP_PERC,TOTAL_MARKET_AMT,MARKUP_CALC_TYPE,VAT_IND,OTB_CALC_TYPE,NUM_MAX_AVG_COUNTER_DAYS,AVG_TOLERANCE_PERC,BUYER_NO,MERCH_NO,PROFIT_CALC_TYPE,PURCHASE_TYPE,SOURCE_DATA_STATUS_CODE,JV_DEPT_IND,PACKAGING_DEPT_IND,GIFTING_DEPT_IND,NON_MERCH_DEPT_IND,NON_CORE_DEPT_IND,BUCKET_DEPT_IND,BOOK_MAGAZINE_DEPT_IND,DEPT_PLACEHOLDER_01_IND,DEPT_PLACEHOLDER_02_IND,DEPT_PLACEHOLDER_03_IND,DEPT_PLACEHOLDER_04_IND,DEPT_PLACEHOLDER_05_IND,DEPT_PLACEHOLDER_06_IND,DEPT_PLACEHOLDER_07_IND,DEPT_PLACEHOLDER_08_IND,DEPT_PLACEHOLDER_09_IND,DEPT_PLACEHOLDER_10_IND,LAST_UPDATED_DATE) values (3005,'TRENERY GEN DEPT',3005,null,null,null,null,0,null,null,null,null,null,null,null,null,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,to_date('27/APR/09','DD/MON/RR'));
--Insert into FND_DEPARTMENT (DEPARTMENT_NO,DEPARTMENT_NAME,SUBGROUP_NO,BUDGET_INTK_PERC,BUDGET_MKUP_PERC,TOTAL_MARKET_AMT,MARKUP_CALC_TYPE,VAT_IND,OTB_CALC_TYPE,NUM_MAX_AVG_COUNTER_DAYS,AVG_TOLERANCE_PERC,BUYER_NO,MERCH_NO,PROFIT_CALC_TYPE,PURCHASE_TYPE,SOURCE_DATA_STATUS_CODE,JV_DEPT_IND,PACKAGING_DEPT_IND,GIFTING_DEPT_IND,NON_MERCH_DEPT_IND,NON_CORE_DEPT_IND,BUCKET_DEPT_IND,BOOK_MAGAZINE_DEPT_IND,DEPT_PLACEHOLDER_01_IND,DEPT_PLACEHOLDER_02_IND,DEPT_PLACEHOLDER_03_IND,DEPT_PLACEHOLDER_04_IND,DEPT_PLACEHOLDER_05_IND,DEPT_PLACEHOLDER_06_IND,DEPT_PLACEHOLDER_07_IND,DEPT_PLACEHOLDER_08_IND,DEPT_PLACEHOLDER_09_IND,DEPT_PLACEHOLDER_10_IND,LAST_UPDATED_DATE) values (3007,'MIMCO GEN DEPT',3007,null,null,null,null,0,null,null,null,null,null,null,null,null,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,to_date('27/APR/09','DD/MON/RR'));
--Insert into FND_DEPARTMENT (DEPARTMENT_NO,DEPARTMENT_NAME,SUBGROUP_NO,BUDGET_INTK_PERC,BUDGET_MKUP_PERC,TOTAL_MARKET_AMT,MARKUP_CALC_TYPE,VAT_IND,OTB_CALC_TYPE,NUM_MAX_AVG_COUNTER_DAYS,AVG_TOLERANCE_PERC,BUYER_NO,MERCH_NO,PROFIT_CALC_TYPE,PURCHASE_TYPE,SOURCE_DATA_STATUS_CODE,JV_DEPT_IND,PACKAGING_DEPT_IND,GIFTING_DEPT_IND,NON_MERCH_DEPT_IND,NON_CORE_DEPT_IND,BUCKET_DEPT_IND,BOOK_MAGAZINE_DEPT_IND,DEPT_PLACEHOLDER_01_IND,DEPT_PLACEHOLDER_02_IND,DEPT_PLACEHOLDER_03_IND,DEPT_PLACEHOLDER_04_IND,DEPT_PLACEHOLDER_05_IND,DEPT_PLACEHOLDER_06_IND,DEPT_PLACEHOLDER_07_IND,DEPT_PLACEHOLDER_08_IND,DEPT_PLACEHOLDER_09_IND,DEPT_PLACEHOLDER_10_IND,LAST_UPDATED_DATE) values (3009,'WITCHERY GEN DEPT',3009,null,null,null,null,0,null,null,null,null,null,null,null,null,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,to_date('27/APR/09','DD/MON/RR'));


--insert into FND_CLASS (DEPARTMENT_NO,CLASS_NO,CLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,VAT_IND,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3004,93004,'COUNTRY ROAD GEN CLASS',null,null,0,null,TO_DATE('27/APR/09','DD/MON/RR'));
--Insert into FND_CLASS (DEPARTMENT_NO,CLASS_NO,CLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,VAT_IND,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3005,93005,'TRENERY GEN CLASS',null,null,0,null,to_date('27/APR/09','DD/MON/RR'));
--Insert into FND_CLASS (DEPARTMENT_NO,CLASS_NO,CLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,VAT_IND,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3007,93007,'MIMCO GEN CLASS',null,null,0,null,to_date('27/APR/09','DD/MON/RR'));
--Insert Into Fnd_Class (Department_No,Class_No,Class_Name,Num_Mkup_Multi_Price_Days,Num_Sales_Lead_Time_Weeks,Vat_Ind,Source_Data_Status_Code,Last_Updated_Date) Values (3009,93009,'WITCHERY GEN CLASS',Null,Null,0,Null,To_Date('27/APR/09','DD/MON/RR'));


--Insert into FND_SUBCLASS (DEPARTMENT_NO,CLASS_NO,SUBCLASS_NO,SUBCLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3004,93004,993004,'COUNTRY ROAD GEN DEPT',null,42,null,to_date('27/JAN/15','DD/MON/RR'));
--Insert into FND_SUBCLASS (DEPARTMENT_NO,CLASS_NO,SUBCLASS_NO,SUBCLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3005,93005,993005,'TRENERY GEN DEPT',null,42,null,to_date('27/JAN/15','DD/MON/RR'));
--Insert into FND_SUBCLASS (DEPARTMENT_NO,CLASS_NO,SUBCLASS_NO,SUBCLASS_NAME,NUM_MKUP_MULTI_PRICE_DAYS,NUM_SALES_LEAD_TIME_WEEKS,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE) values (3007,93007,993007,'MIMCO GEN DEPT',null,0,null,to_date('27/JAN/15','DD/MON/RR'));
--Insert Into FND_SUBCLASS (Department_No,Class_No,Subclass_No,Subclass_Name,Num_Mkup_Multi_Price_Days,Num_Sales_Lead_Time_Weeks,Source_Data_Status_Code,Last_Updated_Date) Values (3009,93009,993009,'WITCHERY GEN DEPT',Null,0,Null,To_Date('27/JAN/15','DD/MON/RR'));



--Insert into FND_ITEM (ITEM_NO,ITEM_DESC,ITEM_SHORT_DESC,ITEM_UPPER_DESC,ITEM_SCNDRY_DESC,ITEM_STATUS_CODE,ITEM_LEVEL_NO,TRAN_LEVEL_NO,PRIMARY_REF_ITEM_IND,FD_PRODUCT_NO,ITEM_PARENT_NO,ITEM_GRANDPARENT_NO,ITEM_LEVEL1_NO,ITEM_LEVEL2_NO,SUBCLASS_NO,CLASS_NO,DEPARTMENT_NO,RPL_IND,ITEM_NO_TYPE,FORMAT_ID,UPC_PREFIX_NO,DIFF_1_CODE,DIFF_2_CODE,DIFF_3_CODE,DIFF_4_CODE,ITEM_AGGR_IND,DIFF_1_AGGR_IND,DIFF_2_AGGR_IND,DIFF_3_AGGR_IND,DIFF_4_AGGR_IND,RETAIL_ZONE_GROUP_NO,COST_ZONE_GROUP_NO,STANDARD_UOM_CODE,STANDARD_UOM_DESC,STANDARD_UOM_CLASS_CODE,UOM_CONV_FACTOR,PACKAGE_SIZE,PACKAGE_UOM_CODE,PACKAGE_UOM_DESC,PACKAGE_UOM_CLASS_CODE,MERCHANDISE_ITEM_IND,STORE_ORD_MULT_UNIT_TYPE_CODE,EXT_SYS_FORECAST_IND,PRIMARY_CURRENCY_ORIGINAL_RSP,MFG_RECOMMENDED_RSP,RETAIL_LABEL_TYPE,RETAIL_LABEL_VALUE,HANDLING_TEMP_CODE,HANDLING_SENSITIVITY_CODE,RANDOM_MASS_IND,FIRST_RECEIVED_DATE,LAST_RECEIVED_DATE,MOST_RECENT_RECEIVED_QTY,WASTE_TYPE,AVG_WASTE_PERC,DEFAULT_WASTE_PERC,CONSTANT_DIMENSION_IND,PACK_ITEM_IND,PACK_ITEM_SIMPLE_IND,PACK_ITEM_INNER_PACK_IND,PACK_ITEM_SELLABLE_UNIT_IND,PACK_ITEM_ORDERABLE_IND,PACK_ITEM_TYPE,PACK_ITEM_RECEIVABLE_TYPE,ITEM_COMMENT,ITEM_SERVICE_LEVEL_TYPE,GIFT_WRAP_IND,SHIP_ALONE_IND,ORIGIN_ITEM_EXT_SRC_SYS_NAME,BANDED_ITEM_IND,STATIC_MASS,EXT_REF_ID,CREATE_DATE,SIZE_ID,COLOR_ID,STYLE_COLOUR_NO,STYLE_NO,BUYING_IND,SELLING_IND,PRODUCT_CLASS,FD_DISCIPLINE_TYPE,HANDLING_METHOD_CODE,HANDLING_METHOD_NAME,DISPLAY_METHOD_CODE,DISPLAY_METHOD_NAME,TRAY_SIZE_CODE,SEGREGATION_IND,OUTER_CASE_BARCODE,RPL_MERCH_SEASON_NO,PROD_CATG_CODE,SUPP_COMMENT,RDF_FORECST_IND,ITEM_LAUNCH_DATE,PRODUCT_PROFILE_CODE,LIVE_ON_RMS_DATE,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE,PRIMARY_SUPPLIER_NO,NEXT_COST_PRICE,NEXT_COST_PRICE_EFFECTIVE_DATE,RP_CATLG_IND,DISPLAY_SUBCLASS_NAME,DISPLAY_CLASS_NAME,DISPLAY_GROUP_NAME,STORE_SCANNED_ORDER_IND,MAX_SCANNED_ORDER_CASES,MIN_SHELF_LIFE_TOLERANCE,MAX_SHELF_LIFE_TOLERANCE,LOOSE_PROD_IND,VAR_WEIGHT_IND) values (999999999999993004,'COUNTRY ROAD GEN ITEM','COUNTRY ROAD GEN ITEM','COUNTRY ROAD GEN ITEM','-','-',1,1,0,0,0,0,999999999999993004,999999999999993004,993004,93004,3004,0,'-','-',0,'NO COLOR','NOSIZE','NO_SIZE','-',0,0,0,0,0,2,10,'EA','-','-',0,0,'-','-','-',0,'-',0,0,0,'-',0,'-','-',0,to_date('28/JUN/99','DD/MON/RR'),to_date('28/JUN/99','DD/MON/RR'),0,'-',0,0,0,0,0,0,0,0,'-','-','-','-',0,0,'Hard-coded by ODWH',0,0,'-',to_date('28/JUN/99','DD/MON/RR'),'NOSIZE','NO COLOR',6001009000381,6001009000234,0,0,'-','-','-','-','-','-','-',0,0,'0','-','-',0,to_date('28/JUN/99','DD/MON/RR'),'-',to_date('28/JUN/99','DD/MON/RR'),'-',to_date('27/APR/09','DD/MON/RR'),0,0,to_date('28/JUN/99','DD/MON/RR'),0,null,null,null,null,null,null,null,null,null);
--Insert into FND_ITEM (ITEM_NO,ITEM_DESC,ITEM_SHORT_DESC,ITEM_UPPER_DESC,ITEM_SCNDRY_DESC,ITEM_STATUS_CODE,ITEM_LEVEL_NO,TRAN_LEVEL_NO,PRIMARY_REF_ITEM_IND,FD_PRODUCT_NO,ITEM_PARENT_NO,ITEM_GRANDPARENT_NO,ITEM_LEVEL1_NO,ITEM_LEVEL2_NO,SUBCLASS_NO,CLASS_NO,DEPARTMENT_NO,RPL_IND,ITEM_NO_TYPE,FORMAT_ID,UPC_PREFIX_NO,DIFF_1_CODE,DIFF_2_CODE,DIFF_3_CODE,DIFF_4_CODE,ITEM_AGGR_IND,DIFF_1_AGGR_IND,DIFF_2_AGGR_IND,DIFF_3_AGGR_IND,DIFF_4_AGGR_IND,RETAIL_ZONE_GROUP_NO,COST_ZONE_GROUP_NO,STANDARD_UOM_CODE,STANDARD_UOM_DESC,STANDARD_UOM_CLASS_CODE,UOM_CONV_FACTOR,PACKAGE_SIZE,PACKAGE_UOM_CODE,PACKAGE_UOM_DESC,PACKAGE_UOM_CLASS_CODE,MERCHANDISE_ITEM_IND,STORE_ORD_MULT_UNIT_TYPE_CODE,EXT_SYS_FORECAST_IND,PRIMARY_CURRENCY_ORIGINAL_RSP,MFG_RECOMMENDED_RSP,RETAIL_LABEL_TYPE,RETAIL_LABEL_VALUE,HANDLING_TEMP_CODE,HANDLING_SENSITIVITY_CODE,RANDOM_MASS_IND,FIRST_RECEIVED_DATE,LAST_RECEIVED_DATE,MOST_RECENT_RECEIVED_QTY,WASTE_TYPE,AVG_WASTE_PERC,DEFAULT_WASTE_PERC,CONSTANT_DIMENSION_IND,PACK_ITEM_IND,PACK_ITEM_SIMPLE_IND,PACK_ITEM_INNER_PACK_IND,PACK_ITEM_SELLABLE_UNIT_IND,PACK_ITEM_ORDERABLE_IND,PACK_ITEM_TYPE,PACK_ITEM_RECEIVABLE_TYPE,ITEM_COMMENT,ITEM_SERVICE_LEVEL_TYPE,GIFT_WRAP_IND,SHIP_ALONE_IND,ORIGIN_ITEM_EXT_SRC_SYS_NAME,BANDED_ITEM_IND,STATIC_MASS,EXT_REF_ID,CREATE_DATE,SIZE_ID,COLOR_ID,STYLE_COLOUR_NO,STYLE_NO,BUYING_IND,SELLING_IND,PRODUCT_CLASS,FD_DISCIPLINE_TYPE,HANDLING_METHOD_CODE,HANDLING_METHOD_NAME,DISPLAY_METHOD_CODE,DISPLAY_METHOD_NAME,TRAY_SIZE_CODE,SEGREGATION_IND,OUTER_CASE_BARCODE,RPL_MERCH_SEASON_NO,PROD_CATG_CODE,SUPP_COMMENT,RDF_FORECST_IND,ITEM_LAUNCH_DATE,PRODUCT_PROFILE_CODE,LIVE_ON_RMS_DATE,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE,PRIMARY_SUPPLIER_NO,NEXT_COST_PRICE,NEXT_COST_PRICE_EFFECTIVE_DATE,RP_CATLG_IND,DISPLAY_SUBCLASS_NAME,DISPLAY_CLASS_NAME,DISPLAY_GROUP_NAME,STORE_SCANNED_ORDER_IND,MAX_SCANNED_ORDER_CASES,MIN_SHELF_LIFE_TOLERANCE,MAX_SHELF_LIFE_TOLERANCE,LOOSE_PROD_IND,VAR_WEIGHT_IND) values (999999999999993005,'TRENERY GEN ITEM','TRENERY GEN ITEM','TRENERY GEN ITEM','-','-',1,1,0,0,0,0,999999999999993005,999999999999993005,993005,93005,3005,0,'-','-',0,'NO COLOR','NOSIZE','NO_SIZE','-',0,0,0,0,0,2,10,'EA','-','-',0,0,'-','-','-',0,'-',0,0,0,'-',0,'-','-',0,to_date('28/JUN/99','DD/MON/RR'),to_date('28/JUN/99','DD/MON/RR'),0,'-',0,0,0,0,0,0,0,0,'-','-','-','-',0,0,'Hard-coded by ODWH',0,0,'-',to_date('28/JUN/99','DD/MON/RR'),'NOSIZE','NO COLOR',6001009000382,6001009000235,0,0,'-','-','-','-','-','-','-',0,0,'0','-','-',0,to_date('28/JUN/99','DD/MON/RR'),'-',to_date('28/JUN/99','DD/MON/RR'),'-',to_date('27/APR/09','DD/MON/RR'),0,0,to_date('28/JUN/99','DD/MON/RR'),0,null,null,null,null,null,null,null,null,null);
--Insert into FND_ITEM (ITEM_NO,ITEM_DESC,ITEM_SHORT_DESC,ITEM_UPPER_DESC,ITEM_SCNDRY_DESC,ITEM_STATUS_CODE,ITEM_LEVEL_NO,TRAN_LEVEL_NO,PRIMARY_REF_ITEM_IND,FD_PRODUCT_NO,ITEM_PARENT_NO,ITEM_GRANDPARENT_NO,ITEM_LEVEL1_NO,ITEM_LEVEL2_NO,SUBCLASS_NO,CLASS_NO,DEPARTMENT_NO,RPL_IND,ITEM_NO_TYPE,FORMAT_ID,UPC_PREFIX_NO,DIFF_1_CODE,DIFF_2_CODE,DIFF_3_CODE,DIFF_4_CODE,ITEM_AGGR_IND,DIFF_1_AGGR_IND,DIFF_2_AGGR_IND,DIFF_3_AGGR_IND,DIFF_4_AGGR_IND,RETAIL_ZONE_GROUP_NO,COST_ZONE_GROUP_NO,STANDARD_UOM_CODE,STANDARD_UOM_DESC,STANDARD_UOM_CLASS_CODE,UOM_CONV_FACTOR,PACKAGE_SIZE,PACKAGE_UOM_CODE,PACKAGE_UOM_DESC,PACKAGE_UOM_CLASS_CODE,MERCHANDISE_ITEM_IND,STORE_ORD_MULT_UNIT_TYPE_CODE,EXT_SYS_FORECAST_IND,PRIMARY_CURRENCY_ORIGINAL_RSP,MFG_RECOMMENDED_RSP,RETAIL_LABEL_TYPE,RETAIL_LABEL_VALUE,HANDLING_TEMP_CODE,HANDLING_SENSITIVITY_CODE,RANDOM_MASS_IND,FIRST_RECEIVED_DATE,LAST_RECEIVED_DATE,MOST_RECENT_RECEIVED_QTY,WASTE_TYPE,AVG_WASTE_PERC,DEFAULT_WASTE_PERC,CONSTANT_DIMENSION_IND,PACK_ITEM_IND,PACK_ITEM_SIMPLE_IND,PACK_ITEM_INNER_PACK_IND,PACK_ITEM_SELLABLE_UNIT_IND,PACK_ITEM_ORDERABLE_IND,PACK_ITEM_TYPE,PACK_ITEM_RECEIVABLE_TYPE,ITEM_COMMENT,ITEM_SERVICE_LEVEL_TYPE,GIFT_WRAP_IND,SHIP_ALONE_IND,ORIGIN_ITEM_EXT_SRC_SYS_NAME,BANDED_ITEM_IND,STATIC_MASS,EXT_REF_ID,CREATE_DATE,SIZE_ID,COLOR_ID,STYLE_COLOUR_NO,STYLE_NO,BUYING_IND,SELLING_IND,PRODUCT_CLASS,FD_DISCIPLINE_TYPE,HANDLING_METHOD_CODE,HANDLING_METHOD_NAME,DISPLAY_METHOD_CODE,DISPLAY_METHOD_NAME,TRAY_SIZE_CODE,SEGREGATION_IND,OUTER_CASE_BARCODE,RPL_MERCH_SEASON_NO,PROD_CATG_CODE,SUPP_COMMENT,RDF_FORECST_IND,ITEM_LAUNCH_DATE,PRODUCT_PROFILE_CODE,LIVE_ON_RMS_DATE,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE,PRIMARY_SUPPLIER_NO,NEXT_COST_PRICE,NEXT_COST_PRICE_EFFECTIVE_DATE,RP_CATLG_IND,DISPLAY_SUBCLASS_NAME,DISPLAY_CLASS_NAME,DISPLAY_GROUP_NAME,STORE_SCANNED_ORDER_IND,MAX_SCANNED_ORDER_CASES,MIN_SHELF_LIFE_TOLERANCE,MAX_SHELF_LIFE_TOLERANCE,LOOSE_PROD_IND,VAR_WEIGHT_IND) values (999999999999993007,'MIMCO GEN ITEM','MIMCO GEN ITEM','MIMCO GEN ITEM','-','-',1,1,0,0,0,0,999999999999993007,999999999999993007,993007,93007,3007,0,'-','-',0,'NO COLOR','NOSIZE','NO_SIZE','-',0,0,0,0,0,2,10,'EA','-','-',0,0,'-','-','-',0,'-',0,0,0,'-',0,'-','-',0,to_date('28/JUN/99','DD/MON/RR'),to_date('28/JUN/99','DD/MON/RR'),0,'-',0,0,0,0,0,0,0,0,'-','-','-','-',0,0,'Hard-coded by ODWH',0,0,'-',to_date('28/JUN/99','DD/MON/RR'),'NOSIZE','NO COLOR',6001009000383,6001009000236,0,0,'-','-','-','-','-','-','-',0,0,'0','-','-',0,to_date('28/JUN/99','DD/MON/RR'),'-',to_date('28/JUN/99','DD/MON/RR'),'-',to_date('27/APR/09','DD/MON/RR'),0,0,to_date('28/JUN/99','DD/MON/RR'),0,null,null,null,null,null,null,null,null,null);
--Insert Into FND_ITEM (Item_No,Item_Desc,Item_Short_Desc,Item_Upper_Desc,Item_Scndry_Desc,Item_Status_Code,Item_Level_No,Tran_Level_No,Primary_Ref_Item_Ind,Fd_Product_No,Item_Parent_No,Item_Grandparent_No,Item_Level1_No,Item_Level2_No,Subclass_No,Class_No,Department_No,Rpl_Ind,Item_No_Type,Format_Id,Upc_Prefix_No,Diff_1_Code,Diff_2_Code,Diff_3_Code,Diff_4_Code,Item_Aggr_Ind,Diff_1_Aggr_Ind,Diff_2_Aggr_Ind,Diff_3_Aggr_Ind,Diff_4_Aggr_Ind,Retail_Zone_Group_No,Cost_Zone_Group_No,Standard_Uom_Code,Standard_Uom_Desc,Standard_Uom_Class_Code,Uom_Conv_Factor,Package_Size,Package_Uom_Code,Package_Uom_Desc,Package_Uom_Class_Code,Merchandise_Item_Ind,Store_Ord_Mult_Unit_Type_Code,Ext_Sys_Forecast_Ind,Primary_Currency_Original_Rsp,Mfg_Recommended_Rsp,Retail_Label_Type,Retail_Label_Value,Handling_Temp_Code,Handling_Sensitivity_Code,Random_Mass_Ind,First_Received_Date,Last_Received_Date,Most_Recent_Received_Qty,Waste_Type,Avg_Waste_Perc,Default_Waste_Perc,Constant_Dimension_Ind,Pack_Item_Ind,Pack_Item_Simple_Ind,Pack_Item_Inner_Pack_Ind,Pack_Item_Sellable_Unit_Ind,Pack_Item_Orderable_Ind,Pack_Item_Type,Pack_Item_Receivable_Type,Item_Comment,Item_Service_Level_Type,Gift_Wrap_Ind,Ship_Alone_Ind,Origin_Item_Ext_Src_Sys_Name,Banded_Item_Ind,Static_Mass,Ext_Ref_Id,Create_Date,Size_Id,Color_Id,Style_Colour_No,Style_No,Buying_Ind,Selling_Ind,Product_Class,Fd_Discipline_Type,Handling_Method_Code,Handling_Method_Name,Display_Method_Code,Display_Method_Name,Tray_Size_Code,Segregation_Ind,Outer_Case_Barcode,Rpl_Merch_Season_No,Prod_Catg_Code,Supp_Comment,Rdf_Forecst_Ind,Item_Launch_Date,Product_Profile_Code,Live_On_Rms_Date,Source_Data_Status_Code,Last_Updated_Date,Primary_Supplier_No,Next_Cost_Price,Next_Cost_Price_Effective_Date,Rp_Catlg_Ind,Display_Subclass_Name,Display_Class_Name,Display_Group_Name,Store_Scanned_Order_Ind,Max_Scanned_Order_Cases,Min_Shelf_Life_Tolerance,Max_Shelf_Life_Tolerance,Loose_Prod_Ind,Var_Weight_Ind) Values (999999999999993009,'WITCHERY GEN ITEM','WITCHERY GEN ITEM','WITCHERY GEN ITEM','-','-',1,1,0,0,0,0,999999999999993009,999999999999993009,993009,93009,3009,0,'-','-',0,'NO COLOR','NOSIZE','NO_SIZE','-',0,0,0,0,0,2,10,'EA','-','-',0,0,'-','-','-',0,'-',0,0,0,'-',0,'-','-',0,To_Date('28/JUN/99','DD/MON/RR'),To_Date('28/JUN/99','DD/MON/RR'),0,'-',0,0,0,0,0,0,0,0,'-','-','-','-',0,0,'Hard-coded by ODWH',0,0,'-',To_Date('28/JUN/99','DD/MON/RR'),'NOSIZE','NO COLOR',6001009000384,6001009000237,0,0,'-','-','-','-','-','-','-',0,0,'0','-','-',0,To_Date('28/JUN/99','DD/MON/RR'),'-',To_Date('28/JUN/99','DD/MON/RR'),'-',To_Date('27/APR/09','DD/MON/RR'),0,0,To_Date('28/JUN/99','DD/MON/RR'),0,Null,Null,Null,Null,Null,Null,Null,Null,Null);


  /*
  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
*/

  commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message); 
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
    
end flagged_records_insert;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
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
-- Call the bulk routines
--**************************************************************************************************

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


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
   
end WH_FND_CORP_NEW_GENERIC_INSERT  ;
