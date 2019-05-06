--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046X
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046X" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        March 2009
--  Author:      Tien Cheng
--  Purpose:     Create Location Item fact table in the performance layer
--               with input ex RMS fnd_location_item table from foundation layer.
--  Tables:      Input  - fnd_location_item
--               Output - rtl_location_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 Jan 2009 - Defect 491 - remove primary_country_code and replace with sk1_primary_country_code
--                             remove primary_supplier_no and replace with sk1_primary_supplier_no
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS

--  Note:
--  This program was rewritten to use one single DML to replace the current cursor insert/update to improve performance,
--  thus it will only write records read/inserted/updated once it has finished running. - Tien Cheng
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
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

cursor c_dim_location is
   select location_no
   from   dim_location
   ORDER BY LOCATION_NO;

g_rec_in            c_dim_location%rowtype;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
begin
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
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_yesterday := g_date - 1;

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

for v_dim_location in c_dim_location
    loop
       g_rec_in := v_dim_location;

      
      l_text := 'About to merge location no '||g_rec_in.location_no||'  '||
      to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     

--MERGE /*+ APPEND USE_HASH(rli,a)*/ INTO rtl_location_item rli USING
MERGE  INTO rtl_location_item rli USING
(
select    dl.sk1_location_no,
          di.sk1_item_no,
          fli.SUPPLY_CHAIN_TYPE,
--          fli.NEXT_WK_DELIV_PATTERN_CODE,
--          fli.THIS_WK_DELIV_PATTERN_CODE,
          to_number(translate(decode(nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1222'))
          as    NEXT_WK_DELIV_PATTERN_CODE,
          to_number(translate(decode(nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1222'))
          as    THIS_WK_DELIV_PATTERN_CODE,
          fli.THIS_WK_CATALOG_IND,
          fli.NEXT_WK_CATALOG_IND,
          fli.NUM_SHELF_LIFE_DAYS,
          fli.NUM_UNITS_PER_TRAY,
          fli.DIRECT_PERC,
          fli.MODEL_STOCK,
          fli.THIS_WK_CROSS_DOCK_IND,
          fli.NEXT_WK_CROSS_DOCK_IND,
          fli.THIS_WK_DIRECT_SUPPLIER_NO,
          fli.NEXT_WK_DIRECT_SUPPLIER_NO,
          fli.UNIT_PICK_IND,
          fli.STORE_ORDER_CALC_CODE,
          fli.SAFETY_STOCK_FACTOR,
          fli.MIN_ORDER_QTY,
          fli.PROFILE_ID,
          fli.SUB_PROFILE_ID,
          fli.REG_RSP,
          fli.SELLING_RSP,
          fli.SELLING_UOM_CODE,
          fli.PROM_RSP,
          fli.PROM_SELLING_RSP,
          fli.PROM_SELLING_UOM_CODE,
          fli.CLEARANCE_IND,
          fli.TAXABLE_IND,
          fli.POS_ITEM_DESC,
          fli.POS_SHORT_DESC,
          fli.NUM_TI_PALLET_TIER_CASES,
          fli.NUM_HI_PALLET_TIER_CASES,
          fli.STORE_ORD_MULT_UNIT_TYPE_CODE,
          fli.LOC_ITEM_STATUS_CODE,
          fli.LOC_ITEM_STAT_CODE_UPDATE_DATE,
          fli.AVG_NATURAL_DAILY_WASTE_PERC,
          fli.MEAS_OF_EACH,
          fli.MEAS_OF_PRICE,
          fli.RSP_UOM_CODE,
          fli.PRIMARY_VARIANT_ITEM_NO,
          fli.PRIMARY_COST_PACK_ITEM_NO,
          fli.RECEIVE_AS_PACK_TYPE,
          fli.SOURCE_METHOD_LOC_TYPE,
          fli.SOURCE_LOCATION_NO,
          fli.WH_SUPPLY_CHAIN_TYPE_IND,
          fli.LAUNCH_DATE,
          fli.POS_QTY_KEY_OPTION_CODE,
          fli.POS_MANUAL_PRICE_ENTRY_CODE,
          fli.DEPOSIT_CODE,
          fli.FOOD_STAMP_IND,
          fli.POS_WIC_IND,
          fli.PROPORTIONAL_TARE_PERC,
          fli.FIXED_TARE_VALUE,
          fli.FIXED_TARE_UOM_CODE,
          fli.POS_REWARD_ELIGIBLE_IND,
          fli.COMPARABLE_NATL_BRAND_ITEM_NO,
          fli.RETURN_POLICY_CODE,
          fli.RED_FLAG_ALERT_IND,
          fli.POS_MARKETING_CLUB_CODE,
          fli.REPORT_CODE,
          fli.NUM_REQ_SELECT_SHELF_LIFE_DAYS,
          fli.NUM_REQ_RCPT_SHELF_LIFE_DAYS,
          fli.NUM_INVST_BUY_SHELF_LIFE_DAYS,
          fli.RACK_SIZE_CODE,
          fli.FULL_PALLET_ITEM_REORDER_IND,
          fli.IN_STORE_MARKET_BASKET_CODE,
          fli.STORAGE_LOCATION_BIN_ID,
          fli.ALT_STORAGE_LOCATION_BIN_ID,
          fli.STORE_REORDER_IND,
          fli.RETURNABLE_IND,
          fli.REFUNDABLE_IND,
          fli.BACK_ORDER_IND,
          g_date as LAST_UPDATED_DATE,
          dc.sk1_country_code,
          ds.sk1_supplier_no

   from   fnd_location_item fli,
          dim_item di,
          dim_location dl,
          dim_country dc,
          dim_supplier ds
   where  fli.item_no                   = di.item_no  and
          fli.location_no               = dl.location_no and
          fli.primary_country_code      = dc.country_code(+) and
          fli.primary_supplier_no       = ds.supplier_no and
          fli.last_updated_date         = g_date and
          fli.location_no               = g_rec_in.location_no
) a
ON  (a.sk1_item_no = rli.sk1_item_no
and a.sk1_location_no = rli.sk1_location_no)
WHEN MATCHED THEN
  update set supply_chain_type               =  a.SUPPLY_CHAIN_TYPE,
             next_wk_deliv_pattern_code      =  a.NEXT_WK_DELIV_PATTERN_CODE,
             this_wk_deliv_pattern_code      =  a.THIS_WK_DELIV_PATTERN_CODE,
             this_wk_catalog_ind             = 	a.THIS_WK_CATALOG_IND,
             next_wk_catalog_ind             = 	a.NEXT_WK_CATALOG_IND,
             num_shelf_life_days             = 	a.NUM_SHELF_LIFE_DAYS,
             num_units_per_tray              = 	a.NUM_UNITS_PER_TRAY,
             direct_perc                     = 	a.DIRECT_PERC,
             model_stock                     = 	a.MODEL_STOCK,
             this_wk_cross_dock_ind          = 	a.THIS_WK_CROSS_DOCK_IND,
             next_wk_cross_dock_ind          = 	a.NEXT_WK_CROSS_DOCK_IND,
             this_wk_direct_supplier_no      = 	a.THIS_WK_DIRECT_SUPPLIER_NO,
             next_wk_direct_supplier_no      = 	a.NEXT_WK_DIRECT_SUPPLIER_NO,
             unit_pick_ind                   = 	a.UNIT_PICK_IND,
             store_order_calc_code           = 	a.STORE_ORDER_CALC_CODE,
             safety_stock_factor             = 	a.SAFETY_STOCK_FACTOR,
             min_order_qty                   = 	a.MIN_ORDER_QTY,
             profile_id                      = 	a.PROFILE_ID,
             sub_profile_id                  = 	a.SUB_PROFILE_ID,
             reg_rsp                         = 	a.REG_RSP,
             selling_rsp                     = 	a.SELLING_RSP,
             selling_uom_code                = 	a.SELLING_UOM_CODE,
             prom_rsp                        = 	a.PROM_RSP,
             prom_selling_rsp                = 	a.PROM_SELLING_RSP,
             prom_selling_uom_code           = 	a.PROM_SELLING_UOM_CODE,
             clearance_ind                   = 	a.CLEARANCE_IND,
             taxable_ind                     = 	a.TAXABLE_IND,
             pos_item_desc                   = 	a.POS_ITEM_DESC,
             pos_short_desc                  = 	a.POS_SHORT_DESC,
             num_ti_pallet_tier_cases        = 	a.NUM_TI_PALLET_TIER_CASES,
             num_hi_pallet_tier_cases        = 	a.NUM_HI_PALLET_TIER_CASES,
             store_ord_mult_unit_type_code   = 	a.STORE_ORD_MULT_UNIT_TYPE_CODE,
             loc_item_status_code            = 	a.LOC_ITEM_STATUS_CODE,
             loc_item_stat_code_update_date  = 	a.LOC_ITEM_STAT_CODE_UPDATE_DATE,
             avg_natural_daily_waste_perc    = 	a.AVG_NATURAL_DAILY_WASTE_PERC,
             meas_of_each                    = 	a.MEAS_OF_EACH,
             meas_of_price                   = 	a.MEAS_OF_PRICE,
             rsp_uom_code                    = 	a.RSP_UOM_CODE,
             primary_variant_item_no         = 	a.PRIMARY_VARIANT_ITEM_NO,
             primary_cost_pack_item_no       = 	a.PRIMARY_COST_PACK_ITEM_NO,
             sk1_primary_supplier_no         = 	a.sk1_supplier_no,
             sk1_primary_country_code        =  a.sk1_country_code,
             receive_as_pack_type            = 	a.RECEIVE_AS_PACK_TYPE,
             source_method_loc_type          = 	a.SOURCE_METHOD_LOC_TYPE,
             source_location_no              = 	a.SOURCE_LOCATION_NO,
             wh_supply_chain_type_ind        = 	a.WH_SUPPLY_CHAIN_TYPE_IND,
             launch_date                     = 	a.LAUNCH_DATE,
             pos_qty_key_option_code         = 	a.POS_QTY_KEY_OPTION_CODE,
             pos_manual_price_entry_code     = 	a.POS_MANUAL_PRICE_ENTRY_CODE,
             deposit_code                    = 	a.DEPOSIT_CODE,
             food_stamp_ind                  = 	a.FOOD_STAMP_IND,
             pos_wic_ind                     = 	a.POS_WIC_IND,
             proportional_tare_perc          =  a.PROPORTIONAL_TARE_PERC,
             fixed_tare_value                = 	a.FIXED_TARE_VALUE,
             fixed_tare_uom_code             = 	a.FIXED_TARE_UOM_CODE,
             pos_reward_eligible_ind         = 	a.POS_REWARD_ELIGIBLE_IND,
             comparable_natl_brand_item_no   = 	a.COMPARABLE_NATL_BRAND_ITEM_NO,
             return_policy_code              = 	a.RETURN_POLICY_CODE,
             red_flag_alert_ind              = 	a.RED_FLAG_ALERT_IND,
             pos_marketing_club_code         = 	a.POS_MARKETING_CLUB_CODE,
             report_code                     = 	a.REPORT_CODE,
             num_req_select_shelf_life_days  = 	a.NUM_REQ_SELECT_SHELF_LIFE_DAYS,
             num_req_rcpt_shelf_life_days    =  a.NUM_REQ_RCPT_SHELF_LIFE_DAYS,
             num_invst_buy_shelf_life_days   = 	a.NUM_INVST_BUY_SHELF_LIFE_DAYS,
             rack_size_code                  = 	a.RACK_SIZE_CODE,
             full_pallet_item_reorder_ind    = 	a.FULL_PALLET_ITEM_REORDER_IND,
             in_store_market_basket_code     = 	a.IN_STORE_MARKET_BASKET_CODE,
             storage_location_bin_id         = 	a.STORAGE_LOCATION_BIN_ID,
             alt_storage_location_bin_id     = 	a.ALT_STORAGE_LOCATION_BIN_ID,
             store_reorder_ind               = 	a.STORE_REORDER_IND,
             returnable_ind                  = 	a.RETURNABLE_IND,
             refundable_ind                  = 	a.REFUNDABLE_IND,
             back_order_ind                  = 	a.BACK_ORDER_IND,
             last_updated_date               = 	a.LAST_UPDATED_DATE
             where  
             rli.last_updated_date           <  a.LAST_UPDATED_DATE
WHEN NOT MATCHED THEN
  insert (
         rli.sk1_location_no,
	       rli.sk1_item_no,
         rli.supply_chain_type,
         rli.next_wk_deliv_pattern_code,
         rli.this_wk_deliv_pattern_code,
         rli.this_wk_catalog_ind,
         rli.next_wk_catalog_ind,
         rli.num_shelf_life_days,
         rli.num_units_per_tray,
         rli.direct_perc,
         rli.model_stock,
         rli.this_wk_cross_dock_ind,
         rli.next_wk_cross_dock_ind,
         rli.this_wk_direct_supplier_no,
         rli.next_wk_direct_supplier_no,
         rli.unit_pick_ind,
         rli.store_order_calc_code,
         rli.safety_stock_factor,
         rli.min_order_qty,
         rli.profile_id,
         rli.sub_profile_id,
         rli.reg_rsp,
         rli.selling_rsp,
         rli.selling_uom_code,
         rli.prom_rsp,
         rli.prom_selling_rsp,
         rli.prom_selling_uom_code,
         rli.clearance_ind,
         rli.taxable_ind,
         rli.pos_item_desc,
         rli.pos_short_desc,
         rli.num_ti_pallet_tier_cases,
         rli.num_hi_pallet_tier_cases,
         rli.store_ord_mult_unit_type_code,
         rli.loc_item_status_code,
         rli.loc_item_stat_code_update_date,
         rli.avg_natural_daily_waste_perc,
         rli.meas_of_each,
         rli.meas_of_price,
         rli.rsp_uom_code,
         rli.primary_variant_item_no,
         rli.primary_cost_pack_item_no,
         rli.receive_as_pack_type,
         rli.source_method_loc_type,
         rli.source_location_no,
         rli.wh_supply_chain_type_ind,
         rli.launch_date,
         rli.pos_qty_key_option_code,
         rli.pos_manual_price_entry_code,
         rli.deposit_code,
         rli.food_stamp_ind,
         rli.pos_wic_ind,
         rli.proportional_tare_perc,
         rli.fixed_tare_value,
         rli.fixed_tare_uom_code,
         rli.pos_reward_eligible_ind,
         rli.comparable_natl_brand_item_no,
         rli.return_policy_code,
         rli.red_flag_alert_ind,
         rli.pos_marketing_club_code,
         rli.report_code,
         rli.num_req_select_shelf_life_days,
         rli.num_req_rcpt_shelf_life_days,
         rli.num_invst_buy_shelf_life_days,
         rli.rack_size_code,
         rli.full_pallet_item_reorder_ind,
         rli.in_store_market_basket_code,
         rli.storage_location_bin_id,
         rli.alt_storage_location_bin_id,
         rli.store_reorder_ind,
         rli.returnable_ind,
         rli.refundable_ind,
         rli.back_order_ind,
         rli.last_updated_date,
         rli.sk1_primary_country_code,
         rli.sk1_primary_supplier_no
        )
  values
         (
          a.sk1_location_no,
          a.sk1_item_no,
          a.SUPPLY_CHAIN_TYPE,
          a.NEXT_WK_DELIV_PATTERN_CODE,
          a.THIS_WK_DELIV_PATTERN_CODE,
          a.THIS_WK_CATALOG_IND,
          a.NEXT_WK_CATALOG_IND,
          a.NUM_SHELF_LIFE_DAYS,
          a.NUM_UNITS_PER_TRAY,
          a.DIRECT_PERC,
          a.MODEL_STOCK,
          a.THIS_WK_CROSS_DOCK_IND,
          a.NEXT_WK_CROSS_DOCK_IND,
          a.THIS_WK_DIRECT_SUPPLIER_NO,
          a.NEXT_WK_DIRECT_SUPPLIER_NO,
          a.UNIT_PICK_IND,
          a.STORE_ORDER_CALC_CODE,
          a.SAFETY_STOCK_FACTOR,
          a.MIN_ORDER_QTY,
          a.PROFILE_ID,
          a.SUB_PROFILE_ID,
          a.REG_RSP,
          a.SELLING_RSP,
          a.SELLING_UOM_CODE,
          a.PROM_RSP,
          a.PROM_SELLING_RSP,
          a.PROM_SELLING_UOM_CODE,
          a.CLEARANCE_IND,
          a.TAXABLE_IND,
          a.POS_ITEM_DESC,
          a.POS_SHORT_DESC,
          a.NUM_TI_PALLET_TIER_CASES,
          a.NUM_HI_PALLET_TIER_CASES,
          a.STORE_ORD_MULT_UNIT_TYPE_CODE,
          a.LOC_ITEM_STATUS_CODE,
          a.LOC_ITEM_STAT_CODE_UPDATE_DATE,
          a.AVG_NATURAL_DAILY_WASTE_PERC,
          a.MEAS_OF_EACH,
          a.MEAS_OF_PRICE,
          a.RSP_UOM_CODE,
          a.PRIMARY_VARIANT_ITEM_NO,
          a.PRIMARY_COST_PACK_ITEM_NO,
          a.RECEIVE_AS_PACK_TYPE,
          a.SOURCE_METHOD_LOC_TYPE,
          a.SOURCE_LOCATION_NO,
          a.WH_SUPPLY_CHAIN_TYPE_IND,
          a.LAUNCH_DATE,
          a.POS_QTY_KEY_OPTION_CODE,
          a.POS_MANUAL_PRICE_ENTRY_CODE,
          a.DEPOSIT_CODE,
          a.FOOD_STAMP_IND,
          a.POS_WIC_IND,
          a.PROPORTIONAL_TARE_PERC,
          a.FIXED_TARE_VALUE,
          a.FIXED_TARE_UOM_CODE,
          a.POS_REWARD_ELIGIBLE_IND,
          a.COMPARABLE_NATL_BRAND_ITEM_NO,
          a.RETURN_POLICY_CODE,
          a.RED_FLAG_ALERT_IND,
          a.POS_MARKETING_CLUB_CODE,
          a.REPORT_CODE,
          a.NUM_REQ_SELECT_SHELF_LIFE_DAYS,
          a.NUM_REQ_RCPT_SHELF_LIFE_DAYS,
          a.NUM_INVST_BUY_SHELF_LIFE_DAYS,
          a.RACK_SIZE_CODE,
          a.FULL_PALLET_ITEM_REORDER_IND,
          a.IN_STORE_MARKET_BASKET_CODE,
          a.STORAGE_LOCATION_BIN_ID,
          a.ALT_STORAGE_LOCATION_BIN_ID,
          a.STORE_REORDER_IND,
          a.RETURNABLE_IND,
          a.REFUNDABLE_IND,
          a.BACK_ORDER_IND,
          a.LAST_UPDATED_DATE,
          a.sk1_country_code,
          a.sk1_supplier_no
);

g_recs_read:=g_recs_read+SQL%ROWCOUNT;
--g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;

      l_text := 'Merged location no '||g_rec_in.location_no||' No of recs '||SQL%ROWCOUNT||'  '||
      to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
    
end loop;  

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED  '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||0;
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
end wh_prf_corp_046x;
