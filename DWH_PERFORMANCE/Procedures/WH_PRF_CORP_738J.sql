--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_738J
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_738J" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2009
--  Author:      Tien Cheng
--  Purpose:     Update STORE ORDERS ex OM fact table in the performance layer
--               with input ex OM fnd_rtl_loc_item_dy_om_st_ord table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_om_st_ord
--               Output - rtl_loc_item_dy_st_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 April 2009 - defect 689 - Remove fields from STG_OM_ST_ORD (608)...
--                               and related HSP,ARC,CPY tables
--                               Derive the values in the performance layer for:
--                               SHORT_QTY,SALES_VALUE,SALES_QTY,
--                               WASTE_VALUE,WASTE_QTY
-- 25 May 2009 - defect 689 and 1477 - further change.
--                                - Remove reference to fields
--                                  SALES_VALUE,SALES_QTY,WASTE_VALUE,WASTE_QTY
--                                - Add calculation for SHORT_QTY
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
-- 21 Jul 2010 - defect 3840  - Change calculation to scanned_model_stock_qty
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
g_count              number        :=  0;
g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_738J';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
--/*+ APPEND USE_HASH(rtl_lidso ,mer_lidso)*/
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=150000000';
execute immediate 'alter session enable parallel dml';

MERGE  INTO rtl_loc_item_dy_st_ord rtl_lidso USING
(
select    di.sk1_item_no     as sk1_item_no,
          dl.sk1_location_no     as sk1_location_no,
          fnd_lid.post_date    as   post_date,
          fnd_lid.dept_type    as   dept_type,
          fnd_lid.direct_delivery_ind    as direct_delivery_ind,
          fnd_lid.num_store_leadtime_days    as num_store_leadtime_days,
          fnd_lid.boh_1_qty    as   boh_1_qty,
          fnd_lid.boh_1_ind    as   boh_1_ind,
          fnd_lid.boh_2_qty    as   boh_2_qty,
          fnd_lid.boh_3_qty    as   boh_3_qty,
          fnd_lid.sdn_1_qty    as   sdn_1_qty,
          fnd_lid.sdn1_ind    as    sdn1_ind,
          fnd_lid.sdn2_qty    as    sdn2_qty,
          fnd_lid.sdn2_ind    as    sdn2_ind,
          fnd_lid.SDN_1_QTY - fnd_lid.STORE_ORDER1    as   short_qty,
          fnd_lid.day1_estimate    as   day1_estimate,
          fnd_lid.day2_estimate    as   day2_estimate,
          fnd_lid.day3_estimate    as   day3_estimate,
          fnd_lid.safety_qty    as  safety_qty,
          fnd_lid.model_stock    as model_stock,
          fnd_lid.store_order1    as    store_order1,
          fnd_lid.store_order2    as    store_order2,
          fnd_lid.store_order3    as    store_order3,
          to_number(translate(decode(nvl(fnd_lid.DELIVERY_PATTERN,'0'),' ','0',nvl(fnd_lid.DELIVERY_PATTERN,'0')), 'YNO0', '1322'))
          as    delivery_pattern,
          fnd_lid.num_units_per_tray    as  num_units_per_tray,
          fnd_lid.weekly_estimate1    as    weekly_estimate1,
          fnd_lid.weekly_estimate2    as    weekly_estimate2,
          fnd_lid.shelf_life    as  shelf_life,
          fnd_lid.trading_date    as    trading_date,
--          fnd_lid.sales_value    as sales_value,
--          fnd_lid.sales_qty    as   sales_qty,
--          fnd_lid.waste_value    as waste_value,
--          fnd_lid.waste_qty    as   waste_qty,
--          fnd_lid.prod_status_1    as   prod_status_1,
         ( Case
          when fnd_lid.PROD_STATUS_1 = 'A' then 1
          when fnd_lid.PROD_STATUS_1 = 'D' then 4
          when fnd_lid.PROD_STATUS_1 = 'N' then 14
          when fnd_lid.PROD_STATUS_1 = 'O' then 15
          when fnd_lid.PROD_STATUS_1 = 'U' then 21
          when fnd_lid.PROD_STATUS_1 = 'X' then 24
          when fnd_lid.PROD_STATUS_1 = 'Z' then 26
          when fnd_lid.PROD_STATUS_1 IS NULL then 0
          else 0
          end )
               as   prod_status_1,
         ( Case
          when fnd_lid.PROD_STATUS_2 = 'A' then 1
          when fnd_lid.PROD_STATUS_2 = 'D' then 4
          when fnd_lid.PROD_STATUS_2 = 'N' then 14
          when fnd_lid.PROD_STATUS_2 = 'O' then 15
          when fnd_lid.PROD_STATUS_2 = 'U' then 21
          when fnd_lid.PROD_STATUS_2 = 'X' then 24
          when fnd_lid.PROD_STATUS_2 = 'Z' then 26
          when fnd_lid.PROD_STATUS_2 IS NULL then 0
          else 0
          end )
               as   prod_status_2,
          fnd_lid.direct_mu_qty1    as  direct_mu_qty1,
          fnd_lid.direct_mu_qty2    as  direct_mu_qty2,
          fnd_lid.direct_mu_qty3    as  direct_mu_qty3,
          fnd_lid.direct_mu_qty4    as  direct_mu_qty4,
          fnd_lid.direct_mu_qty5    as  direct_mu_qty5,
          fnd_lid.direct_mu_qty6    as  direct_mu_qty6,
          fnd_lid.direct_mu_qty7    as  direct_mu_qty7,
          fnd_lid.direct_mu_ind1    as  direct_mu_ind1,
          fnd_lid.direct_mu_ind2    as  direct_mu_ind2,
          fnd_lid.direct_mu_ind3    as  direct_mu_ind3,
          fnd_lid.direct_mu_ind4    as  direct_mu_ind4,
          fnd_lid.direct_mu_ind5    as  direct_mu_ind5,
          fnd_lid.direct_mu_ind6    as  direct_mu_ind6,
          fnd_lid.direct_mu_ind7    as  direct_mu_ind7,
          fnd_lid.day4_estimate    as   day4_estimate,
          fnd_lid.day5_estimate    as   day5_estimate,
          fnd_lid.day6_estimate    as   day6_estimate,
          fnd_lid.day7_estimate    as   day7_estimate,
          fnd_lid.day1_est_val2    as   day1_est_val2,
          fnd_lid.day2_est_val2    as   day2_est_val2,
          fnd_lid.day3_est_val2    as   day3_est_val2,
          fnd_lid.day4_est_val2    as   day4_est_val2,
          fnd_lid.day5_est_val2    as   day5_est_val2,
          fnd_lid.day6_est_val2    as   day6_est_val2,
          fnd_lid.day7_est_val2    as   day7_est_val2,
          fnd_lid.day1_est_unit2    as  day1_est_unit2,
          fnd_lid.day2_est_unit2    as  day2_est_unit2,
          fnd_lid.day3_est_unit2    as  day3_est_unit2,
          fnd_lid.day4_est_unit2    as  day4_est_unit2,
          fnd_lid.day5_est_unit2    as  day5_est_unit2,
          fnd_lid.day6_est_unit2    as  day6_est_unit2,
          fnd_lid.day7_est_unit2    as  day7_est_unit2,
          fnd_lid.num_units_per_tray2    as num_units_per_tray2,
          fnd_lid.store_model_stock    as   store_model_stock,
          to_number(translate(decode(nvl(fnd_lid.DAY1_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY1_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day1_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY2_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY2_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day2_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY3_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY3_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day3_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY4_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY4_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day4_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY5_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY5_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day5_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY6_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY6_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day6_deliv_pat1,
          to_number(translate(decode(nvl(fnd_lid.DAY7_DELIV_PAT1,'0'),' ','0',nvl(fnd_lid.DAY7_DELIV_PAT1,'0')), 'YNO0', '1322'))
            as day7_deliv_pat1,
          fnd_lid.last_updated_date     as  last_updated_date,
--          fnd_lid.day2_estimate + fnd_lid.safety_qty     as scanned_model_stock_qty,
          fnd_lid.store_order2 + fnd_lid.safety_qty     as scanned_model_stock_qty,
          dih.sk2_item_no     as    sk2_item_no,
          dlh.sk2_location_no     as    sk2_location_no,
          di.item_no            as item_no,
          dl.location_no        as location_no
   from   rtl_loc_item_dy_st_ord_ff fnd_lid,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh
   where  fnd_lid.last_updated_date  = g_date and
          fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          fnd_lid.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date and
          fnd_lid.location_no    = dlh.location_no and
          fnd_lid.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date
) mer_lidso
ON  (mer_lidso.sk1_item_no = rtl_lidso.sk1_item_no
and mer_lidso.sk1_location_no = rtl_lidso.sk1_location_no
and mer_lidso.post_date = rtl_lidso.post_date)
WHEN MATCHED THEN
UPDATE
SET       dept_type                       = mer_lidso.dept_type,
          direct_delivery_ind             = mer_lidso.direct_delivery_ind,
          num_store_leadtime_days         = mer_lidso.num_store_leadtime_days,
          boh_1_qty                       = mer_lidso.boh_1_qty,
          boh_1_ind                       = mer_lidso.boh_1_ind,
          boh_2_qty                       = mer_lidso.boh_2_qty,
          boh_3_qty                       = mer_lidso.boh_3_qty,
          sdn_1_qty                       = mer_lidso.sdn_1_qty,
          sdn1_ind                        = mer_lidso.sdn1_ind,
          sdn2_qty                        = mer_lidso.sdn2_qty,
          sdn2_ind                        = mer_lidso.sdn2_ind,
          short_qty                       = mer_lidso.short_qty,
          day1_estimate                   = mer_lidso.day1_estimate,
          day2_estimate                   = mer_lidso.day2_estimate,
          day3_estimate                   = mer_lidso.day3_estimate,
          safety_qty                      = mer_lidso.safety_qty,
          model_stock                     = mer_lidso.model_stock,
          store_order1                    = mer_lidso.store_order1,
          store_order2                    = mer_lidso.store_order2,
          store_order3                    = mer_lidso.store_order3,
          delivery_pattern                = mer_lidso.delivery_pattern,
          num_units_per_tray              = mer_lidso.num_units_per_tray,
          weekly_estimate1                = mer_lidso.weekly_estimate1,
          weekly_estimate2                = mer_lidso.weekly_estimate2,
          shelf_life                      = mer_lidso.shelf_life,
          trading_date                    = mer_lidso.trading_date,
--          sales_value                     = mer_lidso.sales_value,
--          sales_qty                       = mer_lidso.sales_qty,
--          waste_value                     = mer_lidso.waste_value,
--          waste_qty                       = mer_lidso.waste_qty,
          prod_status_1                   = mer_lidso.prod_status_1,
          prod_status_2                   = mer_lidso.prod_status_2,
          direct_mu_qty1                  = mer_lidso.direct_mu_qty1,
          direct_mu_qty2                  = mer_lidso.direct_mu_qty2,
          direct_mu_qty3                  = mer_lidso.direct_mu_qty3,
          direct_mu_qty4                  = mer_lidso.direct_mu_qty4,
          direct_mu_qty5                  = mer_lidso.direct_mu_qty5,
          direct_mu_qty6                  = mer_lidso.direct_mu_qty6,
          direct_mu_qty7                  = mer_lidso.direct_mu_qty7,
          direct_mu_ind1                  = mer_lidso.direct_mu_ind1,
          direct_mu_ind2                  = mer_lidso.direct_mu_ind2,
          direct_mu_ind3                  = mer_lidso.direct_mu_ind3,
          direct_mu_ind4                  = mer_lidso.direct_mu_ind4,
          direct_mu_ind5                  = mer_lidso.direct_mu_ind5,
          direct_mu_ind6                  = mer_lidso.direct_mu_ind6,
          direct_mu_ind7                  = mer_lidso.direct_mu_ind7,
          day4_estimate                   = mer_lidso.day4_estimate,
          day5_estimate                   = mer_lidso.day5_estimate,
          day6_estimate                   = mer_lidso.day6_estimate,
          day7_estimate                   = mer_lidso.day7_estimate,
          day1_est_val2                   = mer_lidso.day1_est_val2,
          day2_est_val2                   = mer_lidso.day2_est_val2,
          day3_est_val2                   = mer_lidso.day3_est_val2,
          day4_est_val2                   = mer_lidso.day4_est_val2,
          day5_est_val2                   = mer_lidso.day5_est_val2,
          day6_est_val2                   = mer_lidso.day6_est_val2,
          day7_est_val2                   = mer_lidso.day7_est_val2,
          day1_est_unit2                  = mer_lidso.day1_est_unit2,
          day2_est_unit2                  = mer_lidso.day2_est_unit2,
          day3_est_unit2                  = mer_lidso.day3_est_unit2,
          day4_est_unit2                  = mer_lidso.day4_est_unit2,
          day5_est_unit2                  = mer_lidso.day5_est_unit2,
          day6_est_unit2                  = mer_lidso.day6_est_unit2,
          day7_est_unit2                  = mer_lidso.day7_est_unit2,
          num_units_per_tray2             = mer_lidso.num_units_per_tray2,
          store_model_stock               = mer_lidso.store_model_stock,
          day1_deliv_pat1                 = mer_lidso.day1_deliv_pat1,
          day2_deliv_pat1                 = mer_lidso.day2_deliv_pat1,
          day3_deliv_pat1                 = mer_lidso.day3_deliv_pat1,
          day4_deliv_pat1                 = mer_lidso.day4_deliv_pat1,
          day5_deliv_pat1                 = mer_lidso.day5_deliv_pat1,
          day6_deliv_pat1                 = mer_lidso.day6_deliv_pat1,
          day7_deliv_pat1                 = mer_lidso.day7_deliv_pat1,
          scanned_model_stock_qty         = mer_lidso.scanned_model_stock_qty ,
          last_updated_date               = mer_lidso.last_updated_date
WHEN NOT MATCHED THEN
INSERT
(         rtl_lidso.sk1_location_no,
          rtl_lidso.sk1_item_no,
          rtl_lidso.post_date,
          rtl_lidso.sk2_location_no,
          rtl_lidso.sk2_item_no,
          rtl_lidso.dept_type,
          rtl_lidso.direct_delivery_ind,
          rtl_lidso.num_store_leadtime_days,
          rtl_lidso.boh_1_qty,
          rtl_lidso.boh_1_ind,
          rtl_lidso.boh_2_qty,
          rtl_lidso.boh_3_qty,
          rtl_lidso.sdn_1_qty,
          rtl_lidso.sdn1_ind,
          rtl_lidso.sdn2_qty,
          rtl_lidso.sdn2_ind,
          rtl_lidso.short_qty,
          rtl_lidso.day1_estimate,
          rtl_lidso.day2_estimate,
          rtl_lidso.day3_estimate,
          rtl_lidso.safety_qty,
          rtl_lidso.model_stock,
          rtl_lidso.store_order1,
          rtl_lidso.store_order2,
          rtl_lidso.store_order3,
          rtl_lidso.delivery_pattern,
          rtl_lidso.num_units_per_tray,
          rtl_lidso.weekly_estimate1,
          rtl_lidso.weekly_estimate2,
          rtl_lidso.shelf_life,
          rtl_lidso.trading_date,
--          rtl_lidso.sales_value,
--          rtl_lidso.sales_qty,
--          rtl_lidso.waste_value,
--          rtl_lidso.waste_qty,
          rtl_lidso.prod_status_1,
          rtl_lidso.prod_status_2,
          rtl_lidso.direct_mu_qty1,
          rtl_lidso.direct_mu_qty2,
          rtl_lidso.direct_mu_qty3,
          rtl_lidso.direct_mu_qty4,
          rtl_lidso.direct_mu_qty5,
          rtl_lidso.direct_mu_qty6,
          rtl_lidso.direct_mu_qty7,
          rtl_lidso.direct_mu_ind1,
          rtl_lidso.direct_mu_ind2,
          rtl_lidso.direct_mu_ind3,
          rtl_lidso.direct_mu_ind4,
          rtl_lidso.direct_mu_ind5,
          rtl_lidso.direct_mu_ind6,
          rtl_lidso.direct_mu_ind7,
          rtl_lidso.day4_estimate,
          rtl_lidso.day5_estimate,
          rtl_lidso.day6_estimate,
          rtl_lidso.day7_estimate,
          rtl_lidso.day1_est_val2,
          rtl_lidso.day2_est_val2,
          rtl_lidso.day3_est_val2,
          rtl_lidso.day4_est_val2,
          rtl_lidso.day5_est_val2,
          rtl_lidso.day6_est_val2,
          rtl_lidso.day7_est_val2,
          rtl_lidso.day1_est_unit2,
          rtl_lidso.day2_est_unit2,
          rtl_lidso.day3_est_unit2,
          rtl_lidso.day4_est_unit2,
          rtl_lidso.day5_est_unit2,
          rtl_lidso.day6_est_unit2,
          rtl_lidso.day7_est_unit2,
          rtl_lidso.num_units_per_tray2,
          rtl_lidso.store_model_stock,
          rtl_lidso.day1_deliv_pat1,
          rtl_lidso.day2_deliv_pat1,
          rtl_lidso.day3_deliv_pat1,
          rtl_lidso.day4_deliv_pat1,
          rtl_lidso.day5_deliv_pat1,
          rtl_lidso.day6_deliv_pat1,
          rtl_lidso.day7_deliv_pat1,
          rtl_lidso.last_updated_date,
          rtl_lidso.scanned_model_stock_qty)
  values
(         CASE dwh_log.merge_counter(dwh_log.c_inserting)
          WHEN 0 THEN mer_lidso.sk1_location_no
          END,
          mer_lidso.sk1_item_no,
          mer_lidso.post_date,
          mer_lidso.sk2_location_no,
          mer_lidso.sk2_item_no,
          mer_lidso.dept_type,
          mer_lidso.direct_delivery_ind,
          mer_lidso.num_store_leadtime_days,
          mer_lidso.boh_1_qty,
          mer_lidso.boh_1_ind,
          mer_lidso.boh_2_qty,
          mer_lidso.boh_3_qty,
          mer_lidso.sdn_1_qty,
          mer_lidso.sdn1_ind,
          mer_lidso.sdn2_qty,
          mer_lidso.sdn2_ind,
          mer_lidso.short_qty,
          mer_lidso.day1_estimate,
          mer_lidso.day2_estimate,
          mer_lidso.day3_estimate,
          mer_lidso.safety_qty,
          mer_lidso.model_stock,
          mer_lidso.store_order1,
          mer_lidso.store_order2,
          mer_lidso.store_order3,
          mer_lidso.delivery_pattern,
          mer_lidso.num_units_per_tray,
          mer_lidso.weekly_estimate1,
          mer_lidso.weekly_estimate2,
          mer_lidso.shelf_life,
          mer_lidso.trading_date,
--          mer_lidso.sales_value,
--          mer_lidso.sales_qty,
--          mer_lidso.waste_value,
--          mer_lidso.waste_qty,
          mer_lidso.prod_status_1,
          mer_lidso.prod_status_2,
          mer_lidso.direct_mu_qty1,
          mer_lidso.direct_mu_qty2,
          mer_lidso.direct_mu_qty3,
          mer_lidso.direct_mu_qty4,
          mer_lidso.direct_mu_qty5,
          mer_lidso.direct_mu_qty6,
          mer_lidso.direct_mu_qty7,
          mer_lidso.direct_mu_ind1,
          mer_lidso.direct_mu_ind2,
          mer_lidso.direct_mu_ind3,
          mer_lidso.direct_mu_ind4,
          mer_lidso.direct_mu_ind5,
          mer_lidso.direct_mu_ind6,
          mer_lidso.direct_mu_ind7,
          mer_lidso.day4_estimate,
          mer_lidso.day5_estimate,
          mer_lidso.day6_estimate,
          mer_lidso.day7_estimate,
          mer_lidso.day1_est_val2,
          mer_lidso.day2_est_val2,
          mer_lidso.day3_est_val2,
          mer_lidso.day4_est_val2,
          mer_lidso.day5_est_val2,
          mer_lidso.day6_est_val2,
          mer_lidso.day7_est_val2,
          mer_lidso.day1_est_unit2,
          mer_lidso.day2_est_unit2,
          mer_lidso.day3_est_unit2,
          mer_lidso.day4_est_unit2,
          mer_lidso.day5_est_unit2,
          mer_lidso.day6_est_unit2,
          mer_lidso.day7_est_unit2,
          mer_lidso.num_units_per_tray2,
          mer_lidso.store_model_stock,
          mer_lidso.day1_deliv_pat1,
          mer_lidso.day2_deliv_pat1,
          mer_lidso.day3_deliv_pat1,
          mer_lidso.day4_deliv_pat1,
          mer_lidso.day5_deliv_pat1,
          mer_lidso.day6_deliv_pat1,
          mer_lidso.day7_deliv_pat1,
          mer_lidso.last_updated_date,
          mer_lidso.scanned_model_stock_qty);

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);

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
end wh_prf_corp_738j;
