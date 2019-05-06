--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046U_WL2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046U_WL2" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
-- change load due to performance issues - wendy lyttle
--**************************************************************************************************
--  Date:        March 2015
--  Author:      Wendy Lyttle
--
--     revamped version
--
--  Purpose:     Create Location Item fact table in the performance layer
--               with input ex RMS fnd_location_item table from foundation layer.
--  Tables:      Input  - fnd_location_item
--               Output - rtl_location_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 Jan 2009 - Defect 491 - remove primary_country_code and replace with sk1_primary_country_code
--                             remove primary_supplier_no and replace with sk1_primary_supplier_no
--
--  09 May 2011 - (NJ) Defect 4282 - added a field called sale_alert_ind.
--  19 May 2011 - Defect 2981 - Add a new measure to be derived (min_shelf_life)
--                            - Add base measures min_shelf_life_tolerance & max_shelf_life_tolerance
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_tol           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_recs               number        :=  0;
g_count              number        :=  0;
g_name               varchar2(40);
g_cnt_rtl            number        :=  0;
g_cnt_backup         number        :=  0;
g_calc               number        :=  0;

g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_046U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


  
  --**************************************************************************************************
  -- Insert into TEMP table
  --**************************************************************************************************
procedure A_INSERT_TEMP as
BEGIN

  l_text := 'A_INSERT_TEMP';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
insert /*+ append */ into DWH_PERFORMANCE.temp_RTL_LOCATION_ITEM 
   with selfnd as (
             select /*+ FULL(fnd) PARALLEL(fnd,8) */
                    fnd.*, 
                    to_number(translate(decode(nvl(fND.NEXT_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fND.NEXT_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
                    as    NEXT_WKDELIVPATTERN,
                    to_number(translate(decode(nvl(fND.THIS_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fND.THIS_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
                    as    THIS_WKDELIVPATTERN,
                    di.sk1_item_no,
                    dl.sk1_location_no,
                    dc.sk1_country_code SK1_PRIMARY_COUNTRY_CODE,  -- TD-491
                    ds.sk1_supplier_no  SK1_PRIMARY_SUPPLIER_NO   -- TD-491
             from   fnd_location_item fND,
                    dim_item di,
                    dim_location dL,
                    dim_country dc,
                    dim_supplier ds
             where  fnd.item_no                = di.item_no  and
                    fnd.location_no            = dl.location_no and
                    fnd.primary_country_code   = dc.country_code(+) and    -- TD-491
                    fnd.primary_supplier_no    = ds.supplier_no )
   select /*+ FULL(RTL) PARALLEL(RTL,8) */
                                 nvl(SF.SK1_LOCATION_NO,rtl.sk1_location_no) sk1_location_no
                               , nvl(SF.SK1_ITEM_NO,rtl.SK1_ITEM_NO) SK1_ITEM_NO
                               , nvl(SF.SUPPLY_CHAIN_TYPE,rtl.SUPPLY_CHAIN_TYPE) SUPPLY_CHAIN_TYPE
                               , nvl(SF.NEXT_WK_DELIV_PATTERN_CODE,rtl.NEXT_WK_DELIV_PATTERN_CODE) NEXT_WK_DELIV_PATTERN_CODE
                               , nvl(SF.THIS_WK_DELIV_PATTERN_CODE,rtl.THIS_WK_DELIV_PATTERN_CODE) THIS_WK_DELIV_PATTERN_CODE
                               , nvl(SF.THIS_WK_CATALOG_IND,rtl.THIS_WK_CATALOG_IND) THIS_WK_CATALOG_IND
                               , nvl(SF.NEXT_WK_CATALOG_IND,rtl.NEXT_WK_CATALOG_IND) NEXT_WK_CATALOG_IND
                               , nvl(SF.NUM_SHELF_LIFE_DAYS,rtl.NUM_SHELF_LIFE_DAYS) NUM_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_UNITS_PER_TRAY,rtl.NUM_UNITS_PER_TRAY) NUM_UNITS_PER_TRAY
                               , nvl(SF.DIRECT_PERC,rtl.DIRECT_PERC) DIRECT_PERC
                               , nvl(SF.MODEL_STOCK,rtl.MODEL_STOCK) MODEL_STOCK
                               , nvl(SF.THIS_WK_CROSS_DOCK_IND,rtl.THIS_WK_CROSS_DOCK_IND) THIS_WK_CROSS_DOCK_IND
                               , nvl(SF.NEXT_WK_CROSS_DOCK_IND,rtl.NEXT_WK_CROSS_DOCK_IND) NEXT_WK_CROSS_DOCK_IND
                               , nvl(SF.THIS_WK_DIRECT_SUPPLIER_NO,rtl.THIS_WK_DIRECT_SUPPLIER_NO) THIS_WK_DIRECT_SUPPLIER_NO
                               , nvl(SF.NEXT_WK_DIRECT_SUPPLIER_NO,rtl.NEXT_WK_DIRECT_SUPPLIER_NO) NEXT_WK_DIRECT_SUPPLIER_NO
                               , nvl(SF.UNIT_PICK_IND,rtl.UNIT_PICK_IND) UNIT_PICK_IND
                               , nvl(SF.STORE_ORDER_CALC_CODE,rtl.STORE_ORDER_CALC_CODE) STORE_ORDER_CALC_CODE
                               , nvl(SF.SAFETY_STOCK_FACTOR,rtl.SAFETY_STOCK_FACTOR) SAFETY_STOCK_FACTOR
                               , nvl(SF.MIN_ORDER_QTY,rtl.MIN_ORDER_QTY) MIN_ORDER_QTY
                               , nvl(SF.PROFILE_ID,rtl.PROFILE_ID) PROFILE_ID
                               , nvl(SF.SUB_PROFILE_ID,rtl.SUB_PROFILE_ID) SUB_PROFILE_ID
                               , nvl(SF.REG_RSP,rtl.REG_RSP) REG_RSP
                               , nvl(SF.SELLING_RSP,rtl.SELLING_RSP) SELLING_RSP
                               , nvl(SF.SELLING_UOM_CODE,rtl.SELLING_UOM_CODE) SELLING_UOM_CODE
                               , nvl(SF.PROM_RSP,rtl.PROM_RSP) PROM_RSP
                               , nvl(SF.PROM_SELLING_RSP,rtl.PROM_SELLING_RSP) PROM_SELLING_RSP
                               , nvl(SF.PROM_SELLING_UOM_CODE,rtl.PROM_SELLING_UOM_CODE) PROM_SELLING_UOM_CODE
                               , nvl(SF.CLEARANCE_IND,rtl.CLEARANCE_IND) CLEARANCE_IND
                               , nvl(SF.TAXABLE_IND,rtl.TAXABLE_IND) TAXABLE_IND
                               , nvl(SF.POS_ITEM_DESC,rtl.POS_ITEM_DESC) POS_ITEM_DESC
                               , nvl(SF.POS_SHORT_DESC,rtl.POS_SHORT_DESC) POS_SHORT_DESC
                               , nvl(SF.NUM_TI_PALLET_TIER_CASES,rtl.NUM_TI_PALLET_TIER_CASES) NUM_TI_PALLET_TIER_CASES
                               , nvl(SF.NUM_HI_PALLET_TIER_CASES,rtl.NUM_HI_PALLET_TIER_CASES) NUM_HI_PALLET_TIER_CASES
                               , nvl(SF.STORE_ORD_MULT_UNIT_TYPE_CODE,rtl.STORE_ORD_MULT_UNIT_TYPE_CODE) STORE_ORD_MULT_UNIT_TYPE_CODE
                               , nvl(SF.LOC_ITEM_STATUS_CODE,rtl.LOC_ITEM_STATUS_CODE) LOC_ITEM_STATUS_CODE
                               , nvl(SF.LOC_ITEM_STAT_CODE_UPDATE_DATE,rtl.LOC_ITEM_STAT_CODE_UPDATE_DATE) LOC_ITEM_STAT_CODE_UPDATE_DATE
                               , nvl(SF.AVG_NATURAL_DAILY_WASTE_PERC,rtl.AVG_NATURAL_DAILY_WASTE_PERC) AVG_NATURAL_DAILY_WASTE_PERC
                               , nvl(SF.MEAS_OF_EACH,rtl.MEAS_OF_EACH) MEAS_OF_EACH
                               , nvl(SF.MEAS_OF_PRICE,rtl.MEAS_OF_PRICE) MEAS_OF_PRICE
                               , nvl(SF.RSP_UOM_CODE,rtl.RSP_UOM_CODE) RSP_UOM_CODE
                               , nvl(SF.PRIMARY_VARIANT_ITEM_NO,rtl.PRIMARY_VARIANT_ITEM_NO) PRIMARY_VARIANT_ITEM_NO
                               , nvl(SF.PRIMARY_COST_PACK_ITEM_NO,rtl.PRIMARY_COST_PACK_ITEM_NO) PRIMARY_COST_PACK_ITEM_NO
                               , nvl(SF.RECEIVE_AS_PACK_TYPE,rtl.RECEIVE_AS_PACK_TYPE) RECEIVE_AS_PACK_TYPE
                               , nvl(SF.SOURCE_METHOD_LOC_TYPE,rtl.SOURCE_METHOD_LOC_TYPE) SOURCE_METHOD_LOC_TYPE
                               , nvl(SF.SOURCE_LOCATION_NO,rtl.SOURCE_LOCATION_NO) SOURCE_LOCATION_NO
                               , nvl(SF.WH_SUPPLY_CHAIN_TYPE_IND,rtl.WH_SUPPLY_CHAIN_TYPE_IND) WH_SUPPLY_CHAIN_TYPE_IND
                               , nvl(SF.LAUNCH_DATE,rtl.LAUNCH_DATE) LAUNCH_DATE
                               , nvl(SF.POS_QTY_KEY_OPTION_CODE,rtl.POS_QTY_KEY_OPTION_CODE) POS_QTY_KEY_OPTION_CODE
                               , nvl(SF.POS_MANUAL_PRICE_ENTRY_CODE,rtl.POS_MANUAL_PRICE_ENTRY_CODE) POS_MANUAL_PRICE_ENTRY_CODE
                               , nvl(SF.DEPOSIT_CODE,rtl.DEPOSIT_CODE) DEPOSIT_CODE
                               , nvl(SF.FOOD_STAMP_IND,rtl.FOOD_STAMP_IND) FOOD_STAMP_IND
                               , nvl(SF.POS_WIC_IND,rtl.POS_WIC_IND) POS_WIC_IND
                               , nvl(SF.PROPORTIONAL_TARE_PERC,rtl.PROPORTIONAL_TARE_PERC) PROPORTIONAL_TARE_PERC
                               , nvl(SF.FIXED_TARE_VALUE,rtl.FIXED_TARE_VALUE) FIXED_TARE_VALUE
                               , nvl(SF.FIXED_TARE_UOM_CODE,rtl.FIXED_TARE_UOM_CODE) FIXED_TARE_UOM_CODE
                               , nvl(SF.POS_REWARD_ELIGIBLE_IND,rtl.POS_REWARD_ELIGIBLE_IND) POS_REWARD_ELIGIBLE_IND
                               , nvl(SF.COMPARABLE_NATL_BRAND_ITEM_NO,rtl.COMPARABLE_NATL_BRAND_ITEM_NO) COMPARABLE_NATL_BRAND_ITEM_NO
                               , nvl(SF.RETURN_POLICY_CODE,rtl.RETURN_POLICY_CODE) RETURN_POLICY_CODE
                               , nvl(SF.RED_FLAG_ALERT_IND,rtl.RED_FLAG_ALERT_IND) RED_FLAG_ALERT_IND
                               , nvl(SF.POS_MARKETING_CLUB_CODE,rtl.POS_MARKETING_CLUB_CODE) POS_MARKETING_CLUB_CODE
                               , nvl(SF.REPORT_CODE,rtl.REPORT_CODE) REPORT_CODE
                               , nvl(SF.NUM_REQ_SELECT_SHELF_LIFE_DAYS,rtl.NUM_REQ_SELECT_SHELF_LIFE_DAYS) NUM_REQ_SELECT_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_REQ_RCPT_SHELF_LIFE_DAYS,rtl.NUM_REQ_RCPT_SHELF_LIFE_DAYS) NUM_REQ_RCPT_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_INVST_BUY_SHELF_LIFE_DAYS,rtl.NUM_INVST_BUY_SHELF_LIFE_DAYS) NUM_INVST_BUY_SHELF_LIFE_DAYS
                               , nvl(SF.RACK_SIZE_CODE,rtl.RACK_SIZE_CODE) RACK_SIZE_CODE
                               , nvl(SF.FULL_PALLET_ITEM_REORDER_IND,rtl.FULL_PALLET_ITEM_REORDER_IND) FULL_PALLET_ITEM_REORDER_IND
                               , nvl(SF.IN_STORE_MARKET_BASKET_CODE,rtl.IN_STORE_MARKET_BASKET_CODE) IN_STORE_MARKET_BASKET_CODE
                               , nvl(SF.STORAGE_LOCATION_BIN_ID,rtl.STORAGE_LOCATION_BIN_ID) STORAGE_LOCATION_BIN_ID
                               , nvl(SF.ALT_STORAGE_LOCATION_BIN_ID,rtl.ALT_STORAGE_LOCATION_BIN_ID) ALT_STORAGE_LOCATION_BIN_ID
                               , nvl(SF.STORE_REORDER_IND,rtl.STORE_REORDER_IND) STORE_REORDER_IND
                               , nvl(SF.RETURNABLE_IND,rtl.RETURNABLE_IND) RETURNABLE_IND
                               , nvl(SF.REFUNDABLE_IND,rtl.REFUNDABLE_IND) REFUNDABLE_IND
                               , nvl(SF.BACK_ORDER_IND,rtl.BACK_ORDER_IND) BACK_ORDER_IND
                               , nvl(SF.LAST_UPDATED_DATE,rtl.LAST_UPDATED_DATE) LAST_UPDATED_DATE
                               , nvl(SF.SK1_PRIMARY_COUNTRY_CODE,rtl.SK1_PRIMARY_COUNTRY_CODE) SK1_PRIMARY_COUNTRY_CODE
                               , nvl(SF.SK1_PRIMARY_SUPPLIER_NO,rtl.SK1_PRIMARY_SUPPLIER_NO) SK1_PRIMARY_SUPPLIER_NO
                               , nvl(RTL.PRODUCT_STATUS_CODE,0) PRODUCT_STATUS_CODE
                               , nvl(RTL.PRODUCT_STATUS_1_CODE,0) PRODUCT_STATUS_1_CODE
                               , nvl(RTL.WAC,0) WAC
                               , nvl(SF.SALE_ALERT_IND,rtl.SALE_ALERT_IND) SALE_ALERT_IND
                               , nvl(rtl.MIN_SHELF_LIFE,0) MIN_SHELF_LIFE
                               , nvl(rtl.MIN_SHELF_LIFE_TOLERANCE,0) MIN_SHELF_LIFE_TOLERANCE
                               , nvl(rtl.MAX_SHELF_LIFE_TOLERANCE,0) MAX_SHELF_LIFE_TOLERANCE
   from selfnd sf
   full outer join rtl_location_item rtL
   on sf.sk1_location_no = rtl.sk1_location_no
   and sf.sk1_item_no = rtl.sk1_item_no
   ;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'TEMP_RTL_LOCATION_ITEM : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in a_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A_INSERT_TEMP;

  --**************************************************************************************************
  -- Remove constraints and indexes from RTL
  --PK_P_RTL_LCTN_ITM	UNIQUE	VALID	NORMAL	N	NO		NO	SK1_ITEM_NO, SK1_LOCATION_NO
  --I10_P_RTL_LCTN_ITM	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
  --B1_RTL_LCTN_ITM	NONUNIQUE	VALID	NORMAL	N	NO		NO	THIS_WK_CATALOG_IND
  --**************************************************************************************************
procedure B_REMOVE_INDEXES_RTL as
BEGIN
       l_text := 'B_REMOVE_INDEXES_RTL';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_P_RTL_LCTN_ITM'
    AND TABLE_NAME        = 'RTL_LOCATION_ITEM';
    
    l_text               := 'alter table dwh_performance.RTL_LOCATION_ITEM drop constraint PK_P_RTL_LCTN_ITM';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_LOCATION_ITEM drop constraint PK_P_RTL_LCTN_ITM');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_RTL_LCTN_ITM does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I10_P_RTL_LCTN_ITM'
    AND TABLE_NAME        = 'RTL_LOCATION_ITEM';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I10_P_RTL_LCTN_ITM';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I10_P_RTL_LCTN_ITM');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index PI10_P_RTL_LCTN_ITM does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'B1_RTL_LCTN_ITM'
    AND TABLE_NAME        = 'RTL_LOCATION_ITEM';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.B1_RTL_LCTN_ITM';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.B1_RTL_LCTN_ITM');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index B1_RTL_LCTN_ITM does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_remove_indexes_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_remove_indexes_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end B_REMOVE_INDEXES_RTL;

  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure C_INSERT_RTL as
BEGIN

  l_text := 'C_INSERT_RTL';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
insert /*+ append */ into DWH_PERFORMANCE.RTL_LOCATION_ITEM 
     SELECT * FROM  DWH_PERFORMANCE.TEMP_rtl_location_item 
   ;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'RTL_LOCATION_ITEM : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in c_insert_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_insert_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_insert_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end C_INSERT_RTL;


--**************************************************************************************************
-- create primary key and index from RTL
--**************************************************************************************************
procedure D_ADD_INDEXES_RTL as
BEGIN

             l_text := 'D_ADD_INDEXES_RTL';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_LOCATION_ITEM';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOCATION_ITEM', DEGREE => 8);

      l_text := 'create INDEX DWH_PERFORMANCE.I10_P_RTL_LCTN_ITM';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I10_P_RTL_LCTN_ITM ON DWH_PERFORMANCE.RTL_LOCATION_ITEM (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I10_P_RTL_LCTN_ITM LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.B1_RTL_LCTN_ITM';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.B1_RTL_LCTN_ITM ON DWH_PERFORMANCE.RTL_LOCATION_ITEM (THIS_WK_CATALOG_IND)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.B1_RTL_LCTN_ITM LOGGING NOPARALLEL') ;
   

   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_indexes_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_indexes_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end D_ADD_INDEXES_RTL;


--**************************************************************************************************
-- create primary key and index
--**************************************************************************************************
procedure E_ADD_PRIMARY_KEY_RTL as
BEGIN
                 l_text := 'E_ADD_PRIMARY_KEY_RTL';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         
         
    l_text          := 'Running GATHER_TABLE_STATS ON RTL_LOCATION_ITEM';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOCATION_ITEM', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_LOCATION_ITEM add constraint PK_P_RTL_LCTN_ITM';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_LOCATION_ITEM add CONSTRAINT PK_P_RTL_LCTN_ITM PRIMARY KEY (SK1_ITEM_NO, SK1_LOCATION_NO)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
  
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in e_add_primary_key_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in e_add_primary_key_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end E_ADD_PRIMARY_KEY_RTL;

--**************************************************************************************************
-- Update the Min &Max shelf life days ex dim_item
--**************************************************************************************************
procedure F_UPDATE_RTL as
begin

                l_text := 'F_UPDATE_RTL';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         
   for shelf_life_record in 
   (select li.sk1_item_no,
          li.sk1_location_no,
          nvl(di.min_shelf_life_tolerance,0) min_shelf_life_tolerance,
          nvl(di.max_shelf_life_tolerance,0) max_shelf_life_tolerance,
          case
          when   nvl(li.num_shelf_life_days,0) = 0 then 0
          when   di.min_shelf_life_tolerance is null then nvl(li.num_shelf_life_days,0)
          else   li.num_shelf_life_days - di.min_shelf_life_tolerance
          end as min_sl
   from   rtl_location_item li,
          dim_item di,
          dim_location dl
   where  li.sk1_item_no             = di.sk1_item_no  
          AND li.sk1_location_no             = dl.sk1_location_no  
          AND di.business_unit_no        = 50 
          AND            (li.min_shelf_life_tolerance       <> nvl(di.min_shelf_life_tolerance,0) or
                      li.max_shelf_life_tolerance       <> nvl(di.max_shelf_life_tolerance,0) or
                      case
                      when   nvl(li.num_shelf_life_days,0) = 0 then 0
                      else   li.num_shelf_life_days - nvl(di.min_shelf_life_tolerance,0)  
                      end    <> li.min_shelf_life or
                      li.min_shelf_life_tolerance       is null or
                      li.max_shelf_life_tolerance       is null or
                      li.min_shelf_life                 is null) 
          AND DL.LOCATION_NO > 1000
          AND DL.SK1_LOCATION_NO = LI.SK1_LOCATION_NO)
   loop
     update rtl_location_item
     set    min_shelf_life_tolerance        = shelf_life_record.min_shelf_life_tolerance,
            max_shelf_life_tolerance        = shelf_life_record.max_shelf_life_tolerance,
            min_shelf_life                  = shelf_life_record.min_sl,
            last_updated_date               = g_date
     where  sk1_item_no                     = shelf_life_record.sk1_item_no   and
            sk1_location_no                 = shelf_life_record.sk1_location_no ;

     g_recs_tol  := g_recs_tol  + sql%rowcount;
   end loop;
   exception
     when others then
       l_message := 'Update error min/max tolerance '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
              l_text := 'error in f_update_rtl';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end F_UPDATE_RTL;


  --**************************************************************************************************
  --
  --
  --                    M  a  i  n    p  r  o  c  e  s  s
  --
  --
  --**************************************************************************************************
BEGIN

  --**************************************************************************************************
  -- adding a trace facility
  --**************************************************************************************************
/*0.	Make sure the session being traced has the alter session privilege, 
i.e. to whom does batch run that this tool will be used for, they need to be granted alter session privs

1/ When tracing a session a trace file is generated on the database server, dwndbprd or dwhdbdev

2. It is written to subdirectory 

/dwhprd_app/oracle/diag/rdbms/dwhprd/dwhprd/trace/

Or 

/app/oracle/diag/rdbms/dwhdev/dwhdev/trace/dwhdev/
/app/oracle/diag/rdbms/dwhdev/dwhuat/trace/dwhuat/


3. The trace filed has a difficult naming convention

4. So add to the tracing code the setting of the file name then you know what to look for as per

Alter session set file_identifier = ‘procedure_name’ || to_char(sysdate,’hh24mmiss_ddmmyyyy’);
Alter session set event 10046 level | NN

     Then you have a date/name stamped unique trace file

Tested as follows we see these trace files

SQL> Alter session set traCEfile_identifier = 'Sean1';

Session altered.

SQL> alter session set events '10046 trace name context forever, level 12';

Session altered.

SQL> select * from dual;

D
-
X

SQL> /

D
-
X

SQL> /

D
-
X

SQL> /

D
-
X

SQL> exit


-rw-r-----    1 oracle   asmadmin        303 29 Aug 11:27 dwhdev_ora_6225946_Sean1.trm
-rw-r-----    1 oracle   asmadmin      23185 29 Aug 11:27 dwhdev_ora_6225946_Sean1.trc


*/

--   execute immediate 'Alter session set file_identifier = ''WH_PRF_CORP_046U_WL'''|| to_char(sysdate,’hh24mmiss_ddmmyyyy’);
   execute immediate 'alter session set events ''10046 trace name context forever, level 12''';
   execute immediate 'alter session set tracefile_identifier=''TABLE_A_NOCOMPRESSION_RUN_01''';

 

  --**************************************************************************************************


    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOCATION_ITEM EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Set dates
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     A_INSERT_TEMP;
  --**************************************************************************************************
  -- Check if backup of RTL_LOCATION_ITEM has been done
  --**************************************************************************************************  
    select count(*) into g_cnt_backup from dwh_performance.TEMP_backup_rtl_location_item;
    select count(*) into g_cnt_rtl from dwh_performance.rtl_location_item;
    
    If g_cnt_backup <> g_cnt_rtl
    OR g_cnt_backup is null
       then 
            l_text := 'RTL_LOCATION_ITEM backup into TEMP_BACKUP_RTL_LOCATION_ITEM not current';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            l_text := '* * * * * FORCING ABORT * * * * * ';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            g_calc := 1/0;
    ELSE        
   
                --**************************************************************************************************
                -- Prepare environment
                --**************************************************************************************************  
                execute immediate 'alter session set workarea_size_policy=manual';
                execute immediate 'alter session set sort_area_size=200000000';
                execute immediate 'alter session enable parallel dml';
                l_text := 'Running GATHER_TABLE_STATS ON FND_LOCATION_ITEM';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_LOCATION_ITEM', DEGREE => 8);
              
                l_text := 'truncate table DWH_PERFORMANCE.TEMP_RTL_LOCATION_ITEM ';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                execute immediate('truncate table DWH_PERFORMANCE.TEMP_RTL_LOCATION_ITEM');
                
                A_INSERT_TEMP;
              
               /*  
                --B_REMOVE_INDEXES_RTL;
                
                l_text := 'Running GATHER_TABLE_STATS ON RTL_LOCATION_ITEM';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOCATION_ITEM', DEGREE => 8);
              
               l_text := 'xxxtruncate table DWH_PERFORMANCE.RTL_LOCATION_ITEM ';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                --execute immediate('xxxtruncate table DWH_PERFORMANCE.RTL_LOCATION_ITEM');
              
                C_INSERT_RTL;
              
                D_ADD_INDEXES_RTL;
              
                E_ADD_PRIMARY_KEY_RTL;
                
                F_UPDATE_RTL;
              */
   END IF;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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


END WH_PRF_CORP_046U_WL2;
