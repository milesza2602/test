--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046U_TST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046U_TST" (p_forall_limit in integer,p_success out boolean, p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
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
g_count              number        :=  0;
g_rec_out            dwh_datafix.aj_rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_046U_TST'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--============================================================================================
procedure main_merge as
begin

   merge /*+ parallel(mer_lid,6) */ into dwh_datafix.aj_rtl_location_item   mer_lid
    using  (
      select /*+ parallel(fli,6) full(fli) */
          fli.*,
          to_number(translate(decode(nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.NEXT_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
          as    NEXT_WK_DELIV_PATTERN,
          to_number(translate(decode(nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fli.THIS_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
          as    THIS_WK_DELIV_PATTERN,
          di.sk1_item_no,
          dl.sk1_location_no,
          dc.sk1_country_code,  -- TD-491
          ds.sk1_supplier_no     -- TD-491
   from   dwh_datafix.aj_fnd_location_item fli,
          dwh_performance.dim_item di,
          dwh_performance.dim_location dl,
          dwh_performance.dim_country dc,
          dwh_performance.dim_supplier ds
   where  fli.item_no                = di.item_no  and
          fli.location_no            = dl.location_no and
          fli.primary_country_code   = dc.country_code(+) and    -- TD-491
          fli.primary_supplier_no    = ds.supplier_no and        -- TD-491
          fli.last_updated_date      = g_date 
 
          
    ) mer_mart
    
    on (mer_lid.sk1_location_no = mer_mart.sk1_location_no
    and mer_lid.sk1_item_no     = mer_mart.sk1_item_no
       )
       
     when matched then
       update 
          set  supply_chain_type               = mer_mart.supply_chain_type,
               next_wk_deliv_pattern_code      = mer_mart.next_wk_deliv_pattern,
               this_wk_deliv_pattern_code      = mer_mart.this_wk_deliv_pattern,
               this_wk_catalog_ind             = mer_mart.this_wk_catalog_ind,
               next_wk_catalog_ind             = mer_mart.next_wk_catalog_ind,
               num_shelf_life_days             = mer_mart.num_shelf_life_days,
               num_units_per_tray              = mer_mart.num_units_per_tray,
               direct_perc                     = mer_mart.direct_perc,
               model_stock                     = mer_mart.model_stock,
               this_wk_cross_dock_ind          = mer_mart.this_wk_cross_dock_ind,
               next_wk_cross_dock_ind          = mer_mart.next_wk_cross_dock_ind,
               this_wk_direct_supplier_no      = mer_mart.this_wk_direct_supplier_no,
               next_wk_direct_supplier_no      = mer_mart.next_wk_direct_supplier_no,
               unit_pick_ind                   = mer_mart.unit_pick_ind,
               store_order_calc_code           = mer_mart.store_order_calc_code,
               safety_stock_factor             = mer_mart.safety_stock_factor,
               min_order_qty                   = mer_mart.min_order_qty,
               profile_id                      = mer_mart.profile_id,
               sub_profile_id                  = mer_mart.sub_profile_id,
               reg_rsp                         = mer_mart.reg_rsp,
               selling_rsp                     = mer_mart.selling_rsp,
               selling_uom_code                = mer_mart.selling_uom_code,
               prom_rsp                        = mer_mart.prom_rsp,
               prom_selling_rsp                = mer_mart.prom_selling_rsp,
               prom_selling_uom_code           = mer_mart.prom_selling_uom_code,
               clearance_ind                   = mer_mart.clearance_ind,
               taxable_ind                     = mer_mart.taxable_ind,
               pos_item_desc                   = mer_mart.pos_item_desc,
               pos_short_desc                  = mer_mart.pos_short_desc,
               num_ti_pallet_tier_cases        = mer_mart.num_ti_pallet_tier_cases,
               num_hi_pallet_tier_cases        = mer_mart.num_hi_pallet_tier_cases,
               store_ord_mult_unit_type_code   = mer_mart.store_ord_mult_unit_type_code,
               loc_item_status_code            = mer_mart.loc_item_status_code,
               loc_item_stat_code_update_date  = mer_mart.loc_item_stat_code_update_date,
               avg_natural_daily_waste_perc    = mer_mart.avg_natural_daily_waste_perc,
               meas_of_each                    = mer_mart.meas_of_each,
               meas_of_price                   = mer_mart.meas_of_price,
               rsp_uom_code                    = mer_mart.rsp_uom_code,
               primary_variant_item_no         = mer_mart.primary_variant_item_no,
               primary_cost_pack_item_no       = mer_mart.primary_cost_pack_item_no,
            --   primary_supplier_no             := mer_mart.primary_supplier_no;     -- TD-491
               sk1_primary_supplier_no         = mer_mart.sk1_supplier_no        ,   -- TD-491
            --   primary_country_code            := mer_mart.primary_country_code;    -- TD-491
               sk1_primary_country_code        = mer_mart.sk1_country_code,          -- TD-491
               receive_as_pack_type            = mer_mart.receive_as_pack_type,
               source_method_loc_type          = mer_mart.source_method_loc_type,
               source_location_no              = mer_mart.source_location_no,
               wh_supply_chain_type_ind        = mer_mart.wh_supply_chain_type_ind,
               launch_date                     = mer_mart.launch_date,
               pos_qty_key_option_code         = mer_mart.pos_qty_key_option_code,
               pos_manual_price_entry_code     = mer_mart.pos_manual_price_entry_code,
               deposit_code                    = mer_mart.deposit_code,
               food_stamp_ind                  = mer_mart.food_stamp_ind,
               pos_wic_ind                     = mer_mart.pos_wic_ind,
               proportional_tare_perc          = mer_mart.proportional_tare_perc,
               fixed_tare_value                = mer_mart.fixed_tare_value,
               fixed_tare_uom_code             = mer_mart.fixed_tare_uom_code,
               pos_reward_eligible_ind         = mer_mart.pos_reward_eligible_ind,
               comparable_natl_brand_item_no   = mer_mart.comparable_natl_brand_item_no,
               return_policy_code              = mer_mart.return_policy_code,
               red_flag_alert_ind              = mer_mart.red_flag_alert_ind,
               pos_marketing_club_code         = mer_mart.pos_marketing_club_code,
               report_code                     = mer_mart.report_code,
               num_req_select_shelf_life_days  = mer_mart.num_req_select_shelf_life_days,
               num_req_rcpt_shelf_life_days    = mer_mart.num_req_rcpt_shelf_life_days,
               num_invst_buy_shelf_life_days   = mer_mart.num_invst_buy_shelf_life_days,
               rack_size_code                  = mer_mart.rack_size_code,
               full_pallet_item_reorder_ind    = mer_mart.full_pallet_item_reorder_ind,
               in_store_market_basket_code     = mer_mart.in_store_market_basket_code,
               storage_location_bin_id         = mer_mart.storage_location_bin_id,
               alt_storage_location_bin_id     = mer_mart.alt_storage_location_bin_id,
               store_reorder_ind               = mer_mart.store_reorder_ind,
               returnable_ind                  = mer_mart.returnable_ind,
               refundable_ind                  = mer_mart.refundable_ind,
               back_order_ind                  = mer_mart.back_order_ind,
               last_updated_date               = g_date,
            
               min_shelf_life_tolerance        = 0,
               max_shelf_life_tolerance        = 0,
               min_shelf_life                  = 0,
 
               sale_alert_ind                  = mer_mart.sale_alert_ind,    --Added
               num_facings                     = mer_mart.num_facings       --added AJ
         
    when not matched then 
       insert ( 
                SK1_LOCATION_NO,
                SK1_ITEM_NO,
                SUPPLY_CHAIN_TYPE,
                NEXT_WK_DELIV_PATTERN_CODE,
                THIS_WK_DELIV_PATTERN_CODE,
                THIS_WK_CATALOG_IND,
                NEXT_WK_CATALOG_IND,
                NUM_SHELF_LIFE_DAYS,
                NUM_UNITS_PER_TRAY,
                DIRECT_PERC,
                MODEL_STOCK,
                THIS_WK_CROSS_DOCK_IND,
                NEXT_WK_CROSS_DOCK_IND,
                THIS_WK_DIRECT_SUPPLIER_NO,
                NEXT_WK_DIRECT_SUPPLIER_NO,
                UNIT_PICK_IND,
                STORE_ORDER_CALC_CODE,
                SAFETY_STOCK_FACTOR,
                MIN_ORDER_QTY,
                PROFILE_ID,
                SUB_PROFILE_ID,
                REG_RSP,
                SELLING_RSP,
                SELLING_UOM_CODE,
                PROM_RSP,
                PROM_SELLING_RSP,
                PROM_SELLING_UOM_CODE,
                CLEARANCE_IND,
                TAXABLE_IND,
                POS_ITEM_DESC,
                POS_SHORT_DESC,
                NUM_TI_PALLET_TIER_CASES,
                NUM_HI_PALLET_TIER_CASES,
                STORE_ORD_MULT_UNIT_TYPE_CODE,
                LOC_ITEM_STATUS_CODE,
                LOC_ITEM_STAT_CODE_UPDATE_DATE,
                AVG_NATURAL_DAILY_WASTE_PERC,
                MEAS_OF_EACH,
                MEAS_OF_PRICE,
                RSP_UOM_CODE,
                PRIMARY_VARIANT_ITEM_NO,
                PRIMARY_COST_PACK_ITEM_NO,
                RECEIVE_AS_PACK_TYPE,
                SOURCE_METHOD_LOC_TYPE,
                SOURCE_LOCATION_NO,
                WH_SUPPLY_CHAIN_TYPE_IND,
                LAUNCH_DATE,
                POS_QTY_KEY_OPTION_CODE,
                POS_MANUAL_PRICE_ENTRY_CODE,
                DEPOSIT_CODE,
                FOOD_STAMP_IND,
                POS_WIC_IND,
                PROPORTIONAL_TARE_PERC,
                FIXED_TARE_VALUE,
                FIXED_TARE_UOM_CODE,
                POS_REWARD_ELIGIBLE_IND,
                COMPARABLE_NATL_BRAND_ITEM_NO,
                RETURN_POLICY_CODE,
                RED_FLAG_ALERT_IND,
                POS_MARKETING_CLUB_CODE,
                REPORT_CODE,
                NUM_REQ_SELECT_SHELF_LIFE_DAYS,
                NUM_REQ_RCPT_SHELF_LIFE_DAYS,
                NUM_INVST_BUY_SHELF_LIFE_DAYS,
                RACK_SIZE_CODE,
                FULL_PALLET_ITEM_REORDER_IND,
                IN_STORE_MARKET_BASKET_CODE,
                STORAGE_LOCATION_BIN_ID,
                ALT_STORAGE_LOCATION_BIN_ID,
                STORE_REORDER_IND,
                RETURNABLE_IND,
                REFUNDABLE_IND,
                BACK_ORDER_IND,
                LAST_UPDATED_DATE,
                SK1_PRIMARY_COUNTRY_CODE,
                SK1_PRIMARY_SUPPLIER_NO,
                --PRODUCT_STATUS_CODE,
                --PRODUCT_STATUS_1_CODE,
                --WAC,
                SALE_ALERT_IND,
                min_shelf_life,
                min_shelf_life_tolerance,
                max_shelf_life_tolerance,
                tax_perc,
                num_facings
                )
         values (
                mer_mart.SK1_LOCATION_NO,
                mer_mart.SK1_ITEM_NO,
                mer_mart.SUPPLY_CHAIN_TYPE,
                mer_mart.NEXT_WK_DELIV_PATTERN,
                mer_mart.THIS_WK_DELIV_PATTERN,
                mer_mart.THIS_WK_CATALOG_IND,
                mer_mart.NEXT_WK_CATALOG_IND,
                mer_mart.NUM_SHELF_LIFE_DAYS,
                mer_mart.NUM_UNITS_PER_TRAY,
                mer_mart.DIRECT_PERC,
                mer_mart.MODEL_STOCK,
                mer_mart.THIS_WK_CROSS_DOCK_IND,
                mer_mart.NEXT_WK_CROSS_DOCK_IND,
                mer_mart.THIS_WK_DIRECT_SUPPLIER_NO,
                mer_mart.NEXT_WK_DIRECT_SUPPLIER_NO,
                mer_mart.UNIT_PICK_IND,
                mer_mart.STORE_ORDER_CALC_CODE,
                mer_mart.SAFETY_STOCK_FACTOR,
                mer_mart.MIN_ORDER_QTY,
                mer_mart.PROFILE_ID,
                mer_mart.SUB_PROFILE_ID,
                mer_mart.REG_RSP,
                mer_mart.SELLING_RSP,
                mer_mart.SELLING_UOM_CODE,
                mer_mart.PROM_RSP,
                mer_mart.PROM_SELLING_RSP,
                mer_mart.PROM_SELLING_UOM_CODE,
                mer_mart.CLEARANCE_IND,
                mer_mart.TAXABLE_IND,
                mer_mart.POS_ITEM_DESC,
                mer_mart.POS_SHORT_DESC,
                mer_mart.NUM_TI_PALLET_TIER_CASES,
                mer_mart.NUM_HI_PALLET_TIER_CASES,
                mer_mart.STORE_ORD_MULT_UNIT_TYPE_CODE,
                mer_mart.LOC_ITEM_STATUS_CODE,
                mer_mart.LOC_ITEM_STAT_CODE_UPDATE_DATE,
                mer_mart.AVG_NATURAL_DAILY_WASTE_PERC,
                mer_mart.MEAS_OF_EACH,
                mer_mart.MEAS_OF_PRICE,
                mer_mart.RSP_UOM_CODE,
                mer_mart.PRIMARY_VARIANT_ITEM_NO,
                mer_mart.PRIMARY_COST_PACK_ITEM_NO,
                mer_mart.RECEIVE_AS_PACK_TYPE,
                mer_mart.SOURCE_METHOD_LOC_TYPE,
                mer_mart.SOURCE_LOCATION_NO,
                mer_mart.WH_SUPPLY_CHAIN_TYPE_IND,
                mer_mart.LAUNCH_DATE,
                mer_mart.POS_QTY_KEY_OPTION_CODE,
                mer_mart.POS_MANUAL_PRICE_ENTRY_CODE,
                mer_mart.DEPOSIT_CODE,
                mer_mart.FOOD_STAMP_IND,
                mer_mart.POS_WIC_IND,
                mer_mart.PROPORTIONAL_TARE_PERC,
                mer_mart.FIXED_TARE_VALUE,
                mer_mart.FIXED_TARE_UOM_CODE,
                mer_mart.POS_REWARD_ELIGIBLE_IND,
                mer_mart.COMPARABLE_NATL_BRAND_ITEM_NO,
                mer_mart.RETURN_POLICY_CODE,
                mer_mart.RED_FLAG_ALERT_IND,
                mer_mart.POS_MARKETING_CLUB_CODE,
                mer_mart.REPORT_CODE,
                mer_mart.NUM_REQ_SELECT_SHELF_LIFE_DAYS,
                mer_mart.NUM_REQ_RCPT_SHELF_LIFE_DAYS,
                mer_mart.NUM_INVST_BUY_SHELF_LIFE_DAYS,
                mer_mart.RACK_SIZE_CODE,
                mer_mart.FULL_PALLET_ITEM_REORDER_IND,
                mer_mart.IN_STORE_MARKET_BASKET_CODE,
                mer_mart.STORAGE_LOCATION_BIN_ID,
                mer_mart.ALT_STORAGE_LOCATION_BIN_ID,
                mer_mart.STORE_REORDER_IND,
                mer_mart.RETURNABLE_IND,
                mer_mart.REFUNDABLE_IND,
                mer_mart.BACK_ORDER_IND,
                g_date,
                mer_mart.SK1_COUNTRY_CODE,
                mer_mart.SK1_SUPPLIER_NO,
                --mer_mart.PRODUCT_STATUS_CODE,
                --mer_mart.PRODUCT_STATUS_1_CODE,
                --mer_mart.WAC,
                mer_mart.SALE_ALERT_IND,
                0,
                0,
                0,
                0,
                mer_mart.num_facings
               )
              
          ;
          
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;

  commit;
  
    l_text := ' MAIN MERGE DONE, STARTING SHELF LIFE UPDATE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end main_merge;

  --**************************************************************************************************
-- Update the Min &Max shelf life days ex dim_item
--**************************************************************************************************
procedure shelf_life_days_update as
begin

merge /*+ parallel(rlid,6) */ into dwh_datafix.aj_rtl_location_item rlid 
  using (
     select /*+ parallel(li,6) full(li) */ 
              li.sk1_item_no,
              li.sk1_location_no,
              nvl(di.min_shelf_life_tolerance,0) min_shelf_life_tolerance,
              nvl(di.max_shelf_life_tolerance,0) max_shelf_life_tolerance,
              case
              when   nvl(li.num_shelf_life_days,0) = 0 then 0
              when   di.min_shelf_life_tolerance is null then nvl(li.num_shelf_life_days,0)
              else   li.num_shelf_life_days - di.min_shelf_life_tolerance
              end as min_sl
       from   dwh_datafix.aj_rtl_location_item li,
              dim_item di
       where  li.sk1_item_no             = di.sk1_item_no  and
              di.business_unit_no        = 50 and
               (li.min_shelf_life_tolerance       <> nvl(di.min_shelf_life_tolerance,0) or
                li.max_shelf_life_tolerance       <> nvl(di.max_shelf_life_tolerance,0) or
                case
                when   nvl(li.num_shelf_life_days,0) = 0 then 0
                else   li.num_shelf_life_days - nvl(di.min_shelf_life_tolerance,0)  
                end    <> li.min_shelf_life or
                li.min_shelf_life_tolerance       is null or
                li.max_shelf_life_tolerance       is null or
                li.min_shelf_life                 is null)
                
        ) mer_mart
        
  on (rlid.sk1_location_no  = mer_mart.sk1_location_no
  and rlid.sk1_item_no      = mer_mart.sk1_item_no)

  when matched then
    update 
       set  min_shelf_life_tolerance        = mer_mart.min_shelf_life_tolerance,
            max_shelf_life_tolerance        = mer_mart.max_shelf_life_tolerance,
            min_shelf_life                  = mer_mart.min_sl,
            last_updated_date               = g_date
   ;
   
 commit;
 
 g_recs_tol :=  g_recs_tol + SQL%ROWCOUNT;
  
  l_text := 'SHELF LIFE UPDATE COMPLETE';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
  exception
     when others then
       l_message := 'Update error min/max tolerance '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
  
end shelf_life_days_update;

--=============================================================================================


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=200000000';
execute immediate 'alter session enable parallel dml';


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
  
--**************************************************************************************************
-- Bulk Merge
--**************************************************************************************************
   
    
    if p_from_loc_no = 0 then
        l_text := 'STARTING MAIN MERGE';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        main_merge;
        shelf_life_days_update;
    else
        l_text := p_from_loc_no || ' - SPINNER NO LONGER RUNNING';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
 
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

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
    
    l_text :=  dwh_constants.vc_log_records_updated||'SL Days ex Item '||g_recs_tol;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    p_success := true;
    commit;
    
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
end wh_prf_corp_046u_tst;
