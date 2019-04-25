--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_749U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_749U_MERGE" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2913
--  Author:      Quentin Smit
--  Purpose:     Update STORE ORDERS ex JDA fact table in the performance layer
--
--  Tables:      Input  - dwh_foundation.rtl_loc_item_dy_st_ord_ff
--
--               Output - rtl_loc_item_dy_st_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_749M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM STORE ORDER FACTS EX JDA AND JOIN WITH EXISTING STORE ORDERS';
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

    l_text := 'LOAD OF RTL_LOCATION_ITEM STORE ORDERS EX FOUNDATION STARTED AT '||
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
execute immediate 'alter session set sort_area_size=100000000';
--execute immediate 'alter session enable parallel dml';
execute immediate 'alter session enable parallel dml';

MERGE  /*+ parallel(rtl_lidso, 4) */ INTO rtl_loc_item_dy_st_ord rtl_lidso USING
(
select    /*+ parallel(fnd_lid, 4) */
          fnd_lid.SK1_LOCATION_NO           as SK1_LOCATION_NO,
          fnd_lid.SK1_ITEM_NO               as SK1_ITEM_NO,
          fnd_lid.POST_DATE                 as POST_DATE,
          fnd_lid.SK2_LOCATION_NO           as SK2_LOCATION_NO,
          fnd_lid.SK2_ITEM_NO               as SK2_ITEM_NO,
          fnd_lid.DEPT_TYPE                 as DEPT_TYPE,
          fnd_lid.DIRECT_DELIVERY_IND       as DIRECT_DELIVERY_IND,
          fnd_lid.NUM_STORE_LEADTIME_DAYS   as NUM_STORE_LEADTIME_DAYS,
          fnd_lid.BOH_1_QTY                 as BOH_1_QTY,
          fnd_lid.BOH_1_IND                 as BOH_1_IND,
          fnd_lid.BOH_2_QTY                 as BOH_2_QTY,
          fnd_lid.BOH_3_QTY                 as BOH_3_QTY,
          fnd_lid.SDN_1_QTY                 as SDN_1_QTY,
          fnd_lid.SDN1_IND                  as SDN1_IND,
          fnd_lid.SDN2_QTY                  as SDN2_QTY,
          fnd_lid.SDN2_IND                  as SDN2_IND,
          fnd_lid.SHORT_QTY                 as SHORT_QTY,
          fnd_lid.DAY1_ESTIMATE             as DAY1_ESTIMATE,
          fnd_lid.DAY2_ESTIMATE             as DAY2_ESTIMATE,
          fnd_lid.DAY3_ESTIMATE             as DAY3_ESTIMATE,
          fnd_lid.SAFETY_QTY                as SAFETY_QTY,
          fnd_lid.MODEL_STOCK               as MODEL_STOCK,
          fnd_lid.STORE_ORDER1              as STORE_ORDER1,
          fnd_lid.STORE_ORDER2              as STORE_ORDER2,
          fnd_lid.STORE_ORDER3              as STORE_ORDER3,
          fnd_lid.DELIVERY_PATTERN          as DELIVERY_PATTERN,
          fnd_lid.NUM_UNITS_PER_TRAY        as NUM_UNITS_PER_TRAY,
          fnd_lid.WEEKLY_ESTIMATE1          as WEEKLY_ESTIMATE1,
          fnd_lid.WEEKLY_ESTIMATE2          as WEEKLY_ESTIMATE2,
          fnd_lid.SHELF_LIFE                as SHELF_LIFE,
          fnd_lid.TRADING_DATE              as TRADING_DATE,
          fnd_lid.PROD_STATUS_1             as PROD_STATUS_1,
          fnd_lid.PROD_STATUS_2             as PROD_STATUS_2,
          fnd_lid.DIRECT_MU_QTY1            as DIRECT_MU_QTY1,
          fnd_lid.DIRECT_MU_QTY2            as DIRECT_MU_QTY2,
          fnd_lid.DIRECT_MU_QTY3            as DIRECT_MU_QTY3,
          fnd_lid.DIRECT_MU_QTY4            as DIRECT_MU_QTY4,
          fnd_lid.DIRECT_MU_QTY5            as DIRECT_MU_QTY5,
          fnd_lid.DIRECT_MU_QTY6            as DIRECT_MU_QTY6,
          fnd_lid.DIRECT_MU_QTY7            as DIRECT_MU_QTY7,
          fnd_lid.DIRECT_MU_IND1            as DIRECT_MU_IND1,
          fnd_lid.DIRECT_MU_IND2            as DIRECT_MU_IND2,
          fnd_lid.DIRECT_MU_IND3            as DIRECT_MU_IND3,
          fnd_lid.DIRECT_MU_IND4            as DIRECT_MU_IND4,
          fnd_lid.DIRECT_MU_IND5            as DIRECT_MU_IND5,
          fnd_lid.DIRECT_MU_IND6            as DIRECT_MU_IND6,
          fnd_lid.DIRECT_MU_IND7            as DIRECT_MU_IND7,
          fnd_lid.DAY4_ESTIMATE             as DAY4_ESTIMATE,
          fnd_lid.DAY5_ESTIMATE             as DAY5_ESTIMATE,
          fnd_lid.DAY6_ESTIMATE             as DAY6_ESTIMATE,
          fnd_lid.DAY7_ESTIMATE             as DAY7_ESTIMATE,
          fnd_lid.DAY1_EST_VAL2             as DAY1_EST_VAL2,
          fnd_lid.DAY2_EST_VAL2             as DAY2_EST_VAL2,
          fnd_lid.DAY3_EST_VAL2             as DAY3_EST_VAL2,
          fnd_lid.DAY4_EST_VAL2             as DAY4_EST_VAL2,
          fnd_lid.DAY5_EST_VAL2             as DAY5_EST_VAL2,
          fnd_lid.DAY6_EST_VAL2             as DAY6_EST_VAL2,
          fnd_lid.DAY7_EST_VAL2             as DAY7_EST_VAL2,
          fnd_lid.DAY1_EST_UNIT2            as DAY1_EST_UNIT2,
          fnd_lid.DAY2_EST_UNIT2            as DAY2_EST_UNIT2,
          fnd_lid.DAY3_EST_UNIT2            as DAY3_EST_UNIT2,
          fnd_lid.DAY4_EST_UNIT2            as DAY4_EST_UNIT2,
          fnd_lid.DAY5_EST_UNIT2            as DAY5_EST_UNIT2,
          fnd_lid.DAY6_EST_UNIT2            as DAY6_EST_UNIT2,
          fnd_lid.DAY7_EST_UNIT2            as DAY7_EST_UNIT2,
          fnd_lid.NUM_UNITS_PER_TRAY2       as NUM_UNITS_PER_TRAY2,
          fnd_lid.STORE_MODEL_STOCK         as STORE_MODEL_STOCK,
          fnd_lid.DAY1_DELIV_PAT1           as DAY1_DELIV_PAT1,
          fnd_lid.DAY2_DELIV_PAT1           as DAY2_DELIV_PAT1,
          fnd_lid.DAY3_DELIV_PAT1           as DAY3_DELIV_PAT1,
          fnd_lid.DAY4_DELIV_PAT1           as DAY4_DELIV_PAT1,
          fnd_lid.DAY5_DELIV_PAT1           as DAY5_DELIV_PAT1,
          fnd_lid.DAY6_DELIV_PAT1           as DAY6_DELIV_PAT1,
          fnd_lid.DAY7_DELIV_PAT1           as DAY7_DELIV_PAT1,
          fnd_lid.LAST_UPDATED_DATE         as LAST_UPDATED_DATE,
          fnd_lid.SCANNED_MODEL_STOCK_QTY   as SCANNED_MODEL_STOCK_QTY

   from   rtl_loc_item_dy_st_ord_ff fnd_lid

   where  fnd_lid.last_updated_date  = g_date

) mer_lidso
ON  (mer_lidso.sk1_item_no    = rtl_lidso.sk1_item_no
and mer_lidso.sk1_location_no = rtl_lidso.sk1_location_no
and mer_lidso.post_date       = rtl_lidso.post_date)
WHEN MATCHED THEN
UPDATE
SET       SK2_LOCATION_NO           = mer_lidso.SK2_LOCATION_NO,
          SK2_ITEM_NO               = mer_lidso.SK2_ITEM_NO,
          DEPT_TYPE                 = mer_lidso.DEPT_TYPE,
          DIRECT_DELIVERY_IND       = mer_lidso.DIRECT_DELIVERY_IND,
          NUM_STORE_LEADTIME_DAYS   = mer_lidso.NUM_STORE_LEADTIME_DAYS,
          BOH_1_QTY                 = mer_lidso.BOH_1_QTY,
          BOH_1_IND                 = mer_lidso.BOH_1_IND,
          BOH_2_QTY                 = mer_lidso.BOH_2_QTY,
          BOH_3_QTY                 = mer_lidso.BOH_3_QTY,
          SDN_1_QTY                 = mer_lidso.SDN_1_QTY,
          SDN1_IND                  = mer_lidso.SDN1_IND,
          SDN2_QTY                  = mer_lidso.SDN2_QTY,
          SDN2_IND                  = mer_lidso.SDN2_IND,
          SHORT_QTY                 = mer_lidso.SHORT_QTY,
          DAY1_ESTIMATE             = mer_lidso.DAY1_ESTIMATE,
          DAY2_ESTIMATE             = mer_lidso.DAY2_ESTIMATE,
          DAY3_ESTIMATE             = mer_lidso.DAY3_ESTIMATE,
          SAFETY_QTY                = mer_lidso.SAFETY_QTY,
          MODEL_STOCK               = mer_lidso.MODEL_STOCK,
          STORE_ORDER1              = mer_lidso.STORE_ORDER1,
          STORE_ORDER2              = mer_lidso.STORE_ORDER2,
          STORE_ORDER3              = mer_lidso.STORE_ORDER3,
          DELIVERY_PATTERN          = mer_lidso.DELIVERY_PATTERN,
          NUM_UNITS_PER_TRAY        = mer_lidso.NUM_UNITS_PER_TRAY,
          WEEKLY_ESTIMATE1          = mer_lidso.WEEKLY_ESTIMATE1,
          WEEKLY_ESTIMATE2          = mer_lidso.WEEKLY_ESTIMATE2,
          SHELF_LIFE                = mer_lidso.SHELF_LIFE,
          TRADING_DATE              = mer_lidso.TRADING_DATE,
          PROD_STATUS_1             = mer_lidso.PROD_STATUS_1,
          PROD_STATUS_2             = mer_lidso.PROD_STATUS_2,
          DIRECT_MU_QTY1            = mer_lidso.DIRECT_MU_QTY1,
          DIRECT_MU_QTY2            = mer_lidso.DIRECT_MU_QTY2,
          DIRECT_MU_QTY3            = mer_lidso.DIRECT_MU_QTY3,
          DIRECT_MU_QTY4            = mer_lidso.DIRECT_MU_QTY4,
          DIRECT_MU_QTY5            = mer_lidso.DIRECT_MU_QTY5,
          DIRECT_MU_QTY6            = mer_lidso.DIRECT_MU_QTY6,
          DIRECT_MU_QTY7            = mer_lidso.DIRECT_MU_QTY7,
          DIRECT_MU_IND1            = mer_lidso.DIRECT_MU_IND1,
          DIRECT_MU_IND2            = mer_lidso.DIRECT_MU_IND2,
          DIRECT_MU_IND3            = mer_lidso.DIRECT_MU_IND3,
          DIRECT_MU_IND4            = mer_lidso.DIRECT_MU_IND4,
          DIRECT_MU_IND5            = mer_lidso.DIRECT_MU_IND5,
          DIRECT_MU_IND6            = mer_lidso.DIRECT_MU_IND6,
          DIRECT_MU_IND7            = mer_lidso.DIRECT_MU_IND7,
          DAY4_ESTIMATE             = mer_lidso.DAY4_ESTIMATE,
          DAY5_ESTIMATE             = mer_lidso.DAY5_ESTIMATE,
          DAY6_ESTIMATE             = mer_lidso.DAY6_ESTIMATE,
          DAY7_ESTIMATE             = mer_lidso.DAY7_ESTIMATE,
          DAY1_EST_VAL2             = mer_lidso.DAY1_EST_VAL2,
          DAY2_EST_VAL2             = mer_lidso.DAY2_EST_VAL2,
          DAY3_EST_VAL2             = mer_lidso.DAY3_EST_VAL2,
          DAY4_EST_VAL2             = mer_lidso.DAY4_EST_VAL2,
          DAY5_EST_VAL2             = mer_lidso.DAY5_EST_VAL2,
          DAY6_EST_VAL2             = mer_lidso.DAY6_EST_VAL2,
          DAY7_EST_VAL2             = mer_lidso.DAY7_EST_VAL2,
          DAY1_EST_UNIT2            = mer_lidso.DAY1_EST_UNIT2,
          DAY2_EST_UNIT2            = mer_lidso.DAY2_EST_UNIT2,
          DAY3_EST_UNIT2            = mer_lidso.DAY3_EST_UNIT2,
          DAY4_EST_UNIT2            = mer_lidso.DAY4_EST_UNIT2,
          DAY5_EST_UNIT2            = mer_lidso.DAY5_EST_UNIT2,
          DAY6_EST_UNIT2            = mer_lidso.DAY6_EST_UNIT2,
          DAY7_EST_UNIT2            = mer_lidso.DAY7_EST_UNIT2,
          NUM_UNITS_PER_TRAY2       = mer_lidso.NUM_UNITS_PER_TRAY2,
          STORE_MODEL_STOCK         = mer_lidso.STORE_MODEL_STOCK,
          DAY1_DELIV_PAT1           = mer_lidso.DAY1_DELIV_PAT1,
          DAY2_DELIV_PAT1           = mer_lidso.DAY2_DELIV_PAT1,
          DAY3_DELIV_PAT1           = mer_lidso.DAY3_DELIV_PAT1,
          DAY4_DELIV_PAT1           = mer_lidso.DAY4_DELIV_PAT1,
          DAY5_DELIV_PAT1           = mer_lidso.DAY5_DELIV_PAT1,
          DAY6_DELIV_PAT1           = mer_lidso.DAY6_DELIV_PAT1,
          DAY7_DELIV_PAT1           = mer_lidso.DAY7_DELIV_PAT1,
          LAST_UPDATED_DATE         = mer_lidso.LAST_UPDATED_DATE,
          SCANNED_MODEL_STOCK_QTY   = mer_lidso.SCANNED_MODEL_STOCK_QTY

WHEN NOT MATCHED THEN
INSERT
(         sk1_location_no,
          sk1_item_no,
          post_date,
          SK2_LOCATION_NO,
          SK2_ITEM_NO,
          DEPT_TYPE,
          DIRECT_DELIVERY_IND,
          NUM_STORE_LEADTIME_DAYS,
          BOH_1_QTY,
          BOH_1_IND,
          BOH_2_QTY,
          BOH_3_QTY,
          SDN_1_QTY,
          SDN1_IND,
          SDN2_QTY,
          SDN2_IND,
          SHORT_QTY,
          DAY1_ESTIMATE,
          DAY2_ESTIMATE,
          DAY3_ESTIMATE,
          SAFETY_QTY,
          MODEL_STOCK,
          STORE_ORDER1,
          STORE_ORDER2,
          STORE_ORDER3,
          DELIVERY_PATTERN,
          NUM_UNITS_PER_TRAY,
          WEEKLY_ESTIMATE1,
          WEEKLY_ESTIMATE2,
          SHELF_LIFE,
          TRADING_DATE,
          PROD_STATUS_1,
          PROD_STATUS_2,
          DIRECT_MU_QTY1,
          DIRECT_MU_QTY2,
          DIRECT_MU_QTY3,
          DIRECT_MU_QTY4,
          DIRECT_MU_QTY5,
          DIRECT_MU_QTY6,
          DIRECT_MU_QTY7,
          DIRECT_MU_IND1,
          DIRECT_MU_IND2,
          DIRECT_MU_IND3,
          DIRECT_MU_IND4,
          DIRECT_MU_IND5,
          DIRECT_MU_IND6,
          DIRECT_MU_IND7,
          DAY4_ESTIMATE,
          DAY5_ESTIMATE,
          DAY6_ESTIMATE,
          DAY7_ESTIMATE,
          DAY1_EST_VAL2,
          DAY2_EST_VAL2,
          DAY3_EST_VAL2,
          DAY4_EST_VAL2,
          DAY5_EST_VAL2,
          DAY6_EST_VAL2,
          DAY7_EST_VAL2,
          DAY1_EST_UNIT2,
          DAY2_EST_UNIT2,
          DAY3_EST_UNIT2,
          DAY4_EST_UNIT2,
          DAY5_EST_UNIT2,
          DAY6_EST_UNIT2,
          DAY7_EST_UNIT2,
          NUM_UNITS_PER_TRAY2,
          STORE_MODEL_STOCK,
          DAY1_DELIV_PAT1,
          DAY2_DELIV_PAT1,
          DAY3_DELIV_PAT1,
          DAY4_DELIV_PAT1,
          DAY5_DELIV_PAT1,
          DAY6_DELIV_PAT1,
          DAY7_DELIV_PAT1,
          LAST_UPDATED_DATE,
          SCANNED_MODEL_STOCK_QTY)
  values
(         mer_lidso.sk1_location_no,
          mer_lidso.sk1_item_no,
          mer_lidso.post_date,
          mer_lidso.SK2_LOCATION_NO,
          mer_lidso.SK2_ITEM_NO,
          mer_lidso.DEPT_TYPE,
          mer_lidso.DIRECT_DELIVERY_IND,
          mer_lidso.NUM_STORE_LEADTIME_DAYS,
          mer_lidso.BOH_1_QTY,
          mer_lidso.BOH_1_IND,
          mer_lidso.BOH_2_QTY,
          mer_lidso.BOH_3_QTY,
          mer_lidso.SDN_1_QTY,
          mer_lidso.SDN1_IND,
          mer_lidso.SDN2_QTY,
          mer_lidso.SDN2_IND,
          mer_lidso.SHORT_QTY,
          mer_lidso.DAY1_ESTIMATE,
          mer_lidso.DAY2_ESTIMATE,
          mer_lidso.DAY3_ESTIMATE,
          mer_lidso.SAFETY_QTY,
          mer_lidso.MODEL_STOCK,
          mer_lidso.STORE_ORDER1,
          mer_lidso.STORE_ORDER2,
          mer_lidso.STORE_ORDER3,
          mer_lidso.DELIVERY_PATTERN,
          mer_lidso.NUM_UNITS_PER_TRAY,
          mer_lidso.WEEKLY_ESTIMATE1,
          mer_lidso.WEEKLY_ESTIMATE2,
          mer_lidso.SHELF_LIFE,
          mer_lidso.TRADING_DATE,
          mer_lidso.PROD_STATUS_1,
          mer_lidso.PROD_STATUS_2,
          mer_lidso.DIRECT_MU_QTY1,
          mer_lidso.DIRECT_MU_QTY2,
          mer_lidso.DIRECT_MU_QTY3,
          mer_lidso.DIRECT_MU_QTY4,
          mer_lidso.DIRECT_MU_QTY5,
          mer_lidso.DIRECT_MU_QTY6,
          mer_lidso.DIRECT_MU_QTY7,
          mer_lidso.DIRECT_MU_IND1,
          mer_lidso.DIRECT_MU_IND2,
          mer_lidso.DIRECT_MU_IND3,
          mer_lidso.DIRECT_MU_IND4,
          mer_lidso.DIRECT_MU_IND5,
          mer_lidso.DIRECT_MU_IND6,
          mer_lidso.DIRECT_MU_IND7,
          mer_lidso.DAY4_ESTIMATE,
          mer_lidso.DAY5_ESTIMATE,
          mer_lidso.DAY6_ESTIMATE,
          mer_lidso.DAY7_ESTIMATE,
          mer_lidso.DAY1_EST_VAL2,
          mer_lidso.DAY2_EST_VAL2,
          mer_lidso.DAY3_EST_VAL2,
          mer_lidso.DAY4_EST_VAL2,
          mer_lidso.DAY5_EST_VAL2,
          mer_lidso.DAY6_EST_VAL2,
          mer_lidso.DAY7_EST_VAL2,
          mer_lidso.DAY1_EST_UNIT2,
          mer_lidso.DAY2_EST_UNIT2,
          mer_lidso.DAY3_EST_UNIT2,
          mer_lidso.DAY4_EST_UNIT2,
          mer_lidso.DAY5_EST_UNIT2,
          mer_lidso.DAY6_EST_UNIT2,
          mer_lidso.DAY7_EST_UNIT2,
          mer_lidso.NUM_UNITS_PER_TRAY2,
          mer_lidso.STORE_MODEL_STOCK,
          mer_lidso.DAY1_DELIV_PAT1,
          mer_lidso.DAY2_DELIV_PAT1,
          mer_lidso.DAY3_DELIV_PAT1,
          mer_lidso.DAY4_DELIV_PAT1,
          mer_lidso.DAY5_DELIV_PAT1,
          mer_lidso.DAY6_DELIV_PAT1,
          mer_lidso.DAY7_DELIV_PAT1,
          mer_lidso.LAST_UPDATED_DATE,
          mer_lidso.SCANNED_MODEL_STOCK_QTY
);

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
end wh_prf_corp_749U_merge;
