--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_263U_BKP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_263U_BKP" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2014
--  Author:      Quentin Smit
--  Purpose:     Load Supplier Master Data History mart
--
--  Tables:      Input  - rtl_zone_item_supp_hist
--                        rtl_loc_item_dy_st_ord
--                        rtl_location_item
--                        rtl_zone_item_om
--                        rtl_depot_item_wk
--               Output - MART_SUPPLIER_MASTER_DATA_HIST
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_263U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE SUPPLIER MASTER DATA HISTORY MART';
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

    l_text := 'LOAD OF MART_SUPPLIER_MASTER_DATA_HIST STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--/*+ APPEND USE_HASH(rtl_mart ,mer_mart)*/
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

MERGE  INTO DWH_PERFORMANCE.MART_SUPPLIER_MASTER_DATA_HIST rtl_mart USING
(
with cal as (
      --select today_fin_year_no year_no, today_fin_week_no week_no, this_wk_start_date, this_wk_end_date, today_date
      --  from dim_control ),  -- select * from cal;
      select fin_year_no year_no, fin_week_no week_no, this_week_start_date, this_week_end_date, calendar_date today_date from dim_calendar where calendar_date = g_date),

     item_list as (select a.* 
                     from dim_item a, dim_department b, fnd_jdaff_dept_rollout c 
                    where a.business_unit_no = 50 
                      and a.department_no = b.department_no 
                      and b.department_no = c.department_no
                      and c.department_live_ind = 'Y'),

     loc_list as (select * from dim_location ),   -- select * from loc_list;

     supp_list as (select ds.* from dim_supplier ds),  --  select * from supp_list where sk1_supplier_no = 2208;

     zone_item_supp_hist as (select /*+ PARALLEL(rz,4) full(rz) */ unique rz.sk1_item_no, rz.sk1_supplier_no, rz.sk1_zone_group_zone_no,
                             supp_list.supplier_no, supp_list.supplier_long_desc,
                             rz.THIS_WK_DAY_1_DAILY_PERC , rz.THIS_WK_DAY_2_DAILY_PERC ,
                             rz.THIS_WK_DAY_3_DAILY_PERC , rz.THIS_WK_DAY_4_DAILY_PERC ,
                             rz.THIS_WK_DAY_5_DAILY_PERC , rz.THIS_WK_DAY_6_DAILY_PERC ,
                             rz.THIS_WK_DAY_7_DAILY_PERC , rz.num_lead_time_days,
                             rz.to_loc_no, rz.calendar_date,
                             dz.zone_no, dz.zone_description
                             from rtl_zone_item_supp_hist rz, loc_list, item_list, supp_list, dim_zone dz
                              where rz.sk1_zone_group_zone_no   = loc_list.sk1_fd_zone_group_zone_no
                                and rz.sk1_item_no              = item_list.sk1_item_no
                                and rz.sk1_supplier_no          = supp_list.sk1_supplier_no
                                and rz.sk1_zone_group_zone_no   = dz.sk1_zone_group_zone_no
                                and rz.calendar_date = g_date
                                )
     ,
     zone_list as (select dz.* from dim_zone dz, zone_item_supp_hist where dz.sk1_zone_group_zone_no = zone_item_supp_hist.sk1_zone_group_zone_no),

     st_ord_first_date as (select /*+ PARALLEL(a,4) FULL(a) */ a.sk1_item_no, a.sk1_location_no,
                             max(a.post_date) post_date
                           from RTL_LOC_ITEM_DY_ST_ORD a, item_list, loc_list, cal
                            where a.sk1_item_no     = item_list.sk1_item_no
                              and a.sk1_location_no = loc_list.sk1_location_no
                              and a.post_date = g_date
                              group by a.sk1_item_no, a.sk1_location_no)

                ,
    st_ord as (select /*+ PARALLEL(b,4) */ b.sk1_item_no  , b.sk1_location_no,
                                           b.post_date, sum(b.direct_mu_qty1 + b.direct_mu_qty2 + b.direct_mu_qty3 +
                                           b.direct_mu_qty4 + b.direct_mu_qty5 + b.direct_mu_qty6 + b.direct_mu_qty7) vendor_pack
                 from rtl_loc_item_dy_st_ord b, st_ord_first_date st
                  where b.sk1_location_no = st.sk1_location_no
                   and b.sk1_item_no = st.sk1_item_no
                   and b.post_date = st.post_date
                   group by b.sk1_item_no, b.sk1_location_no, b.post_date)
,

    loc_item as (select /*+ PARALLEL(rli,4) */ rli.sk1_location_no,
                                               rli.sk1_item_no, --nvl(rli.min_order_qty,0) moq, 
                                               item_list.item_no, item_list.item_desc,
                                               item_list.department_no, item_list.subclass_no, loc_list.location_no, loc_list.location_name, loc_list.region_name,
                                               item_list.fd_discipline_type, item_list.fd_product_no, item_list.package_size
                   from rtl_location_item rli, loc_list, item_list
                   where rli.sk1_location_no = loc_list.sk1_location_no
                     and rli.sk1_item_no     = item_list.sk1_item_no)
       ,
   zone_item_om as (select /*+ PARALLEL(rzom,4)  */  unique rzom.sk1_zone_group_zone_no, rzom.sk1_item_no,  rzom.from_loc_no, supp_list.supplier_no,
                            rzom.num_shelf_life_days, --rzom.reg_delivery_pattern_code, fzis.REG_DELIVER_PATTERN_CODE,
                            rzom.NUM_UNITS_PER_TRAY, rzom.product_status_code,
                            cast(rzom.num_extra_leadtime_days as varchar(30)) num_extra_leadtime_days,
                            dps.product_status_short_desc,
                            dps.sk1_product_status_no,
                            rzom.tray_size_code,
                            rzom.case_mass,
                            rzom.ship_hi,
                            rzom.ship_ti,
                            rzom.min_order_qty

                      from rtl_zone_item_om rzom,
                           loc_list,
                           item_list,
                           supp_list,
                           dim_product_status dps
                      where rzom.sk1_zone_group_zone_no = loc_list.sk1_fd_zone_group_zone_no
                        and rzom.sk1_item_no            = item_list.sk1_item_no
                        and rzom.supplier_no            = supp_list.supplier_no
                        and cast(rzom.product_status_code as varchar2(2)) = dps.product_status_code
                        and dps.sk1_product_status_no <> 0
                        )  --select * from zone_item_om;
  ,                      
    deliv_pat as (select /*+ PARALLEL(fzis,4)  */   unique loc_list.sk1_fd_zone_group_zone_no, item_list.sk1_item_no,  fzis.to_loc_no, supp_list.supplier_no,
                            fzis.REG_DELIVER_PATTERN_CODE

                      from fnd_zone_item_supp fzis,
                           loc_list,
                           item_list,
                           supp_list
                      where fzis.zone_no      = loc_list.wh_fd_zone_no
                        and fzis.item_no      = item_list.item_no
                        and fzis.supplier_no  = supp_list.supplier_no
                        and fzis.zone_no      = loc_list.wh_fd_zone_no
                        and fzis.to_loc_no    = loc_list.location_no 
                   )

select  zone_item_supp_hist.sk1_zone_group_zone_no,
        zone_item_supp_hist.sk1_supplier_no,
        loc_item.sk1_item_no,
        nvl(zone_item_om.sk1_product_status_no, 3302) sk1_product_status_no,
        loc_item.item_no,
        supp_list.supplier_no,
        zone_item_supp_hist.calendar_date ,
        loc_item.item_desc,
        loc_item.fd_discipline_type,
        loc_item.fd_product_no,
        nvl(zone_item_om.product_status_code, 0) product_status_code,
        loc_item.department_no,
        loc_item.subclass_no,
        nvl(ll_2.region_name, ' ') region_name,
        zone_item_supp_hist.zone_no,
        zone_item_supp_hist.zone_description,
        zone_item_supp_hist.supplier_long_desc,
        zone_item_supp_hist.THIS_WK_DAY_1_DAILY_PERC Daily_Perc_Day_1,
        zone_item_supp_hist.THIS_WK_DAY_2_DAILY_PERC Daily_Perc_Day_2,
        zone_item_supp_hist.THIS_WK_DAY_3_DAILY_PERC Daily_Perc_Day_3,
        zone_item_supp_hist.THIS_WK_DAY_4_DAILY_PERC Daily_Perc_Day_4,
        zone_item_supp_hist.THIS_WK_DAY_5_DAILY_PERC Daily_Perc_Day_5,
        zone_item_supp_hist.THIS_WK_DAY_6_DAILY_PERC Daily_Perc_Day_6,
        zone_item_supp_hist.THIS_WK_DAY_7_DAILY_PERC Daily_Perc_Day_7,
        nvl(zone_item_om.num_shelf_life_days, 0) num_shelf_life_days,
        zone_item_supp_hist.NUM_LEAD_TIME_DAYS num_lead_time_days,
        --nvl(zone_item_om.reg_delivery_pattern_code, 0) reg_delivery_pattern_code,
        nvl(deliv_pat.REG_DELIVER_PATTERN_CODE, 0) reg_delivery_pattern_code,
        
        nvl(zone_item_om.product_status_short_desc, ' ') product_status_short_desc,
        loc_item.package_size,
        nvl(zone_item_om.ship_ti, 0) ship_ti,
        nvl(zone_item_om.ship_hi, 0) ship_hi,
        nvl(zone_item_om.num_extra_leadtime_days, ' ') num_extra_leadtime_days,
        nvl(zone_item_om.num_units_per_tray, 0) num_units_per_tray,
        nvl(zone_item_om.tray_size_code, ' ') tray_size_code,
        nvl(zone_item_om.case_mass, 0) case_mass,

        nvl(zone_item_om.from_loc_no, 0) from_loc_no,
        zone_item_supp_hist.to_loc_no   ,

        nvl(ll3.location_no || ' - ' || ll3.location_name, ' ')  to_location_name,
        nvl(ll_2.location_no || ' - ' || ll_2.location_name, ' ') from_location_name,
        
        st_ord.post_date ,
        st_ord.vendor_pack,
        --loc_item.moq,
        nvl(zone_item_om.min_order_qty,0) min_order_qty,
        0 as subcase,
        case when uda.MIN_SBD_TOLERANCE_5104 = 'No Value' then '' else uda.MIN_SBD_TOLERANCE_5104 end as min_sbd,
        case when uda.MAX_SBD_TOLERENCE_5105 = 'No Value' then '' else uda.MAX_SBD_TOLERENCE_5105 end as max_sbd,
        --'' as min_sbd,
        --'' as max_sbd,
        g_date as last_updated_date

from loc_item
     join loc_list ll_1 on ll_1.sk1_location_no            = loc_item.sk1_location_no

     join zone_item_supp_hist on ll_1.sk1_fd_zone_group_zone_no     = zone_item_supp_hist.sk1_zone_group_zone_no
                             and loc_item.sk1_item_no               = zone_item_supp_hist.sk1_item_no
                             and ll_1.location_no                   = zone_item_supp_hist.to_loc_no

     join loc_list ll3 on zone_item_supp_hist.to_loc_no = ll3.location_no

     left outer join st_ord on loc_item.sk1_item_no       = st_ord.sk1_item_no
                and loc_item.sk1_location_no              = st_ord.sk1_location_no

     join supp_list on supp_list.sk1_supplier_no      = zone_item_supp_hist.sk1_supplier_no

     left outer join zone_item_om  on loc_item.sk1_item_no                = zone_item_om.sk1_item_no
                                  and ll_1.SK1_FD_ZONE_GROUP_ZONE_NO      = zone_item_om.sk1_zone_group_zone_no

     left outer join loc_list ll_2 on zone_item_om.from_loc_no = ll_2.location_no

     left outer join deliv_pat on ll_1.sk1_fd_zone_group_zone_no    = deliv_pat.sk1_fd_zone_group_zone_no
                              and loc_item.sk1_item_no               = deliv_pat.sk1_item_no
                              and zone_item_supp_hist.supplier_no    = deliv_pat.supplier_no

     left outer join dim_item_uda uda on loc_item.sk1_item_no = uda.sk1_item_no 

) mer_mart
ON  (mer_mart.sk1_zone_group_zone_no  = rtl_mart.sk1_zone_group_zone_no
and mer_mart.sk1_supplier_no          = rtl_mart.sk1_supplier_no
and mer_mart.sk1_item_no              = rtl_mart.sk1_item_no
and mer_mart.sk1_product_status_no    = rtl_mart.sk1_product_status_no
and mer_mart.calendar_date            = rtl_mart.calendar_date)
WHEN MATCHED THEN
UPDATE
SET       item_no                     = mer_mart.item_no,
          supplier_no                 = mer_mart.supplier_no,
          item_desc                   = mer_mart.item_desc,
          fd_discipline_type          = mer_mart.fd_discipline_type,
          fd_product_no               = mer_mart.fd_product_no,
          product_status_code         = mer_mart.product_status_code,
          department_no               = mer_mart.department_no,
          subclass_no                 = mer_mart.subclass_no,
          region_name                 = mer_mart.region_name,
          zone_no                     = mer_mart.zone_no,
          zone_description            = mer_mart.zone_description,
          supplier_long_desc           = mer_mart.supplier_long_desc,
          this_wk_day_1_daily_perc    = mer_mart.Daily_Perc_Day_1,
          this_wk_day_2_daily_perc    = mer_mart.Daily_Perc_Day_2,
          this_wk_day_3_daily_perc    = mer_mart.Daily_Perc_Day_3,
          this_wk_day_4_daily_perc    = mer_mart.Daily_Perc_Day_4,
          this_wk_day_5_daily_perc    = mer_mart.Daily_Perc_Day_5,
          this_wk_day_6_daily_perc    = mer_mart.Daily_Perc_Day_6,
          this_wk_day_7_daily_perc    = mer_mart.Daily_Perc_Day_7,
          num_shelf_life_days         = mer_mart.num_shelf_life_days,
          num_lead_time_days          = mer_mart.num_lead_time_days,
          REG_DELIVERY_PATTERN_CODE   = mer_mart.REG_DELIVERY_PATTERN_CODE,
          PRODUCT_STATUS_SHORT_DESC   = mer_mart.PRODUCT_STATUS_SHORT_DESC,
          PACKAGE_SIZE                = mer_mart.PACKAGE_SIZE,
          SHIP_TI                     = mer_mart.SHIP_TI,
          SHIP_HI                     = mer_mart.SHIP_HI,
          NUM_EXTRA_LEAD_TIME_DAYS    = mer_mart.NUM_EXTRA_LEADTIME_DAYS,
          NUM_UNITS_PER_TRAY          = mer_mart.NUM_UNITS_PER_TRAY,
          TRAY_SIZE_CODE              = mer_mart.TRAY_SIZE_CODE,
          CASE_MASS                   = mer_mart.CASE_MASS,
          FROM_LOC_NO                 = mer_mart.FROM_LOC_NO,
          TO_LOC_NO                   = mer_mart.TO_LOC_NO,
          TO_LOCATION_NAME             = mer_mart.TO_LOCATION_NAME,
          FROM_LOCATION_NAME          = mer_mart.FROM_LOCATION_NAME,
          POST_DATE                   = mer_mart.POST_DATE,
          VENDOR_PACK                 = mer_mart.VENDOR_PACK,
          MIN_ORDER_QTY               = mer_mart.MIN_ORDER_QTY,
          SUBCASE                     = mer_mart.SUBCASE,
          MIN_SBD                     = mer_mart.MIN_SBD,
          MAX_SBD                     = mer_mart.MAX_SBD,
          last_updated_date           = g_date
WHEN NOT MATCHED THEN
INSERT
(         sk1_zone_group_zone_no,
          sk1_supplier_no,
          sk1_item_no,
          sk1_product_status_no,
          item_no,
          supplier_no,
          calendar_date,
          item_desc,
          fd_discipline_type,
          fd_product_no,
          product_status_code,
          department_no,
          subclass_no,
          region_name,
          zone_no,
          zone_description,
          supplier_long_desc,
          this_wk_day_1_daily_perc,
          this_wk_day_2_daily_perc,
          this_wk_day_3_daily_perc,
          this_wk_day_4_daily_perc,
          this_wk_day_5_daily_perc,
          this_wk_day_6_daily_perc,
          this_wk_day_7_daily_perc,
          num_shelf_life_days,
          num_lead_time_days,
          REG_DELIVERY_PATTERN_CODE,
          PRODUCT_STATUS_SHORT_DESC,
          PACKAGE_SIZE,
          SHIP_TI,
          SHIP_HI,
          NUM_EXTRA_LEAD_TIME_DAYS,
          NUM_UNITS_PER_TRAY,
          TRAY_SIZE_CODE,
          CASE_MASS,
          FROM_LOC_NO,
          TO_LOC_NO,
          TO_LOCATION_NAME,
          FROM_LOCATION_NAME,
          POST_DATE,
          VENDOR_PACK,
          MIN_ORDER_QTY,
          SUBCASE,
          MIN_SBD,
          MAX_SBD ,
          last_updated_date
          )
  values
(         mer_mart.sk1_zone_group_zone_no,
          mer_mart.sk1_supplier_no,
          mer_mart.sk1_item_no,
          mer_mart.sk1_product_status_no,
          mer_mart.item_no,
          mer_mart.supplier_no,
          mer_mart.calendar_date,
          mer_mart.item_desc,
          mer_mart.fd_discipline_type,
          mer_mart.fd_product_no,
          mer_mart.product_status_code,
          mer_mart.department_no,
          mer_mart.subclass_no,
          mer_mart.region_name,
          mer_mart.zone_no,
          mer_mart.zone_description,
          mer_mart.supplier_long_desc,
          mer_mart.Daily_Perc_Day_1,
          mer_mart.Daily_Perc_Day_2,
          mer_mart.Daily_Perc_Day_3,
          mer_mart.Daily_Perc_Day_4,
          mer_mart.Daily_Perc_Day_5,
          mer_mart.Daily_Perc_Day_6,
          mer_mart.Daily_Perc_Day_7,
          mer_mart.num_shelf_life_days,
          mer_mart.num_lead_time_days,
          mer_mart.REG_DELIVERY_PATTERN_CODE,
          mer_mart.PRODUCT_STATUS_SHORT_DESC,
          mer_mart.PACKAGE_SIZE,
          mer_mart.SHIP_TI,
          mer_mart.SHIP_HI,
          mer_mart.NUM_EXTRA_LEADTIME_DAYS,
          mer_mart.NUM_UNITS_PER_TRAY,
          mer_mart.TRAY_SIZE_CODE,
          mer_mart.CASE_MASS,
          mer_mart.FROM_LOC_NO,
          mer_mart.TO_LOC_NO,
          mer_mart.TO_LOCATION_NAME,
          mer_mart.FROM_LOCATION_NAME,
          mer_mart.POST_DATE,
          mer_mart.VENDOR_PACK,
          mer_mart.MIN_ORDER_QTY,
          mer_mart.SUBCASE,
          mer_mart.MIN_SBD,
          mer_mart.MAX_SBD,
          g_date);

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
end wh_prf_corp_263U_BKP;
