--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_614U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_614U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2013
--  Author:      Quentin Smit
--  Purpose:     Create the store orders foundation record from various sources
--  Tables:      Input  - fnd_location
--                        fnd_item
--                        fnd_calendar
--                        fnd_location_item
--                        fnd_zone_item

--
--               Output - dim_item_loc_so
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  07 Jul 2015 - Remapping of product_status_1_code = prod_status_1 = product status this week and
--                             product_status_code = prod_status_2 = product status this week
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
g_recs_deleted       integer       :=  0;

g_date               date          := trunc(sysdate);    --'18/NOV/12';  --
g_last_week          date          := trunc(sysdate) - 7;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_614U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Store Order Loc Item Dimension';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

l_fin_year_no         number;
l_fin_week_no         number;
l_last_fin_year       number;
l_last_week_no        number;
l_next_wk_fin_week_no number;
l_next_wk_fin_year_no number;
l_last_wk_fin_year_no number;
l_start_6wks          number;
l_end_6wks            number;
l_start_6wks_date     date;
l_end_6wks_date       date;
l_ytd_start_date      date;
l_ytd_end_date        date;
l_last_wk_start_date  date;
l_last_wk_end_date    date;
l_last_yr_wk_start_date  date;
l_last_yr_wk_end_date    date;
l_day7_last_yr_wk_date date;
l_max_6wk_last_year   number;
l_6wks_string         char(200);
l_start_8wks_date     date;
l_day_no              number;
l_less_days           number;
l_this_wk_start_date  date;
l_date_day1_last_yr   date;
l_6wks_wk1_yr         number;
l_6wks_wk2_yr         number;
l_6wks_wk3_yr         number;
l_6wks_wk4_yr         number;
l_6wks_wk5_yr         number;
l_6wks_wk6_yr         number;
l_6wks_wk1            number;
l_6wks_wk2            number;
l_6wks_wk3            number;
l_6wks_wk4            number;
l_6wks_wk5            number;
l_6wks_wk6            number;
l_today_date_last_year date;
l_max_fin_week_last_year number;
l_item_price_date     date;
l_dc_no_stock_date  date;
l_depot_ly_date       date;
l_cover_cases_date    date;
l_today_date          date;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Create Mart entry for Store Orders STARTED AT '||
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

--    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    EXECUTE IMMEDIATE('truncate table dim_loc_item_so');
--    l_text := 'Truncate dim_loc_item_so Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

MERGE /*+ APPEND PARALLEL (dim_st_loc_item,4) */ INTO dim_loc_item_so dim_st_loc_item
using (
with item_list as (select sk1_item_no, item_no, vat_rate_perc from dim_item where business_unit_no = 50),

loc_item_so as (
select  /*+ PARALLEL(f,a)  */
d.sk1_location_no, --b.item_no,
c.sk1_item_no,
a.num_store_leadtime_days, b.model_stock, b.this_wk_deliv_pattern_code as delivery_pattern, b.num_shelf_life_days as shelf_life,

e.product_status_1_code as prod_status_1,
e.product_status_code as prod_status_2,
e.reg_rsp_incl_vat as day1_est_val2,   --excl
e.reg_rsp_incl_vat as day2_est_val2,
e.reg_rsp_incl_vat as day3_est_val2,
e.reg_rsp_incl_vat as day4_est_val2,
e.reg_rsp_incl_vat as day5_est_val2,
e.reg_rsp_incl_vat as day6_est_val2,
e.reg_rsp_incl_vat as day7_est_val2,

case when b.num_units_per_tray > 0 then
  b.model_stock/b.num_units_per_tray
else 0 end as store_model_stock,

SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,1,1) as day1_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,2,1) as day2_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,3,1) as day3_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,4,1) as day4_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,5,1) as day5_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,6,1) as day6_deliv_pat1,
SUBSTR(b.THIS_WK_DELIV_PATTERN_CODE,7,1) as day7_deliv_pat1,

ff_ord.num_units_per_tray,
ff_ord.num_units_per_tray2,

g_date as last_updated_date,
E.REG_RSP_INCL_VAT_LOCAL  -- change to B.REG_RSP_INCL_VAT_LOCAL ex loc_item if required in Africa Currency (A de Wet June 2018)

from fnd_location a,
     fnd_location_item b,
     item_list c,
     dim_location d,
     fnd_zone_item e,
     dwh_foundation.fnd_loc_item_dy_ff_ord ff_ord

where a.location_no   = b.location_no
  and b.location_no   = d.location_no
  and b.item_no       = c.item_no
  and c.item_no       = e.item_no
  and e.zone_no       = a.wh_zone_no
  and e.zone_group_no = a.wh_zone_group_no
  and a.location_no   = ff_ord.location_no
  and c.item_no       = ff_ord.item_no
  --and ff_ord.post_date >= g_date - 1
  and ff_ord.post_date >= g_date
  --and ff_ord.last_updated_date = g_date
order by a.location_no, c.item_no )

select * from loc_item_so
) mer_st_loc_item

ON (mer_st_loc_item.sk1_item_no     = dim_st_loc_item.sk1_item_no
and mer_st_loc_item.sk1_location_no = dim_st_loc_item.sk1_location_no)
WHEN MATCHED THEN
UPDATE
SET
    NUM_STORE_LEADTIME_DAYS   = mer_st_loc_item.NUM_STORE_LEADTIME_DAYS,
    MODEL_STOCK               = mer_st_loc_item.MODEL_STOCK,
    DELIVERY_PATTERN          = mer_st_loc_item.DELIVERY_PATTERN,
    SHELF_LIFE                = mer_st_loc_item.SHELF_LIFE,
    PROD_STATUS_1             = mer_st_loc_item.PROD_STATUS_1,
    PROD_STATUS_2             = mer_st_loc_item.PROD_STATUS_2,
    DAY1_EST_VAL2             = mer_st_loc_item.DAY1_EST_VAL2,
    DAY2_EST_VAL2             = mer_st_loc_item.DAY2_EST_VAL2,
    DAY3_EST_VAL2             = mer_st_loc_item.DAY3_EST_VAL2,
    DAY4_EST_VAL2             = mer_st_loc_item.DAY4_EST_VAL2,
    DAY5_EST_VAL2             = mer_st_loc_item.DAY5_EST_VAL2,
    DAY6_EST_VAL2             = mer_st_loc_item.DAY6_EST_VAL2,
    DAY7_EST_VAL2             = mer_st_loc_item.DAY7_EST_VAL2,
    STORE_MODEL_STOCK         = mer_st_loc_item.STORE_MODEL_STOCK,
    DAY1_DELIV_PAT1           = mer_st_loc_item.DAY1_DELIV_PAT1,
    DAY2_DELIV_PAT1           = mer_st_loc_item.DAY2_DELIV_PAT1,
    DAY3_DELIV_PAT1           = mer_st_loc_item.DAY3_DELIV_PAT1,
    DAY4_DELIV_PAT1           = mer_st_loc_item.DAY4_DELIV_PAT1,
    DAY5_DELIV_PAT1           = mer_st_loc_item.DAY5_DELIV_PAT1,
    DAY6_DELIV_PAT1           = mer_st_loc_item.DAY6_DELIV_PAT1,
    DAY7_DELIV_PAT1           = mer_st_loc_item.DAY7_DELIV_PAT1,
    NUM_UNITS_PER_TRAY        = mer_st_loc_item.NUM_UNITS_PER_TRAY,
    NUM_UNITS_PER_TRAY2       = mer_st_loc_item.NUM_UNITS_PER_TRAY2,
    LAST_UPDATED_DATE         = mer_st_loc_item.LAST_UPDATED_DATE,
    REG_RSP_INCL_VAT_LOCAL    = mer_st_loc_item.REG_RSP_INCL_VAT_LOCAL

WHEN NOT MATCHED THEN
INSERT
(   SK1_LOCATION_NO,
    SK1_ITEM_NO,
    NUM_STORE_LEADTIME_DAYS,
    MODEL_STOCK,
    DELIVERY_PATTERN,
    SHELF_LIFE,
    PROD_STATUS_1,
    PROD_STATUS_2,
    DAY1_EST_VAL2,
    DAY2_EST_VAL2,
    DAY3_EST_VAL2,
    DAY4_EST_VAL2,
    DAY5_EST_VAL2,
    DAY6_EST_VAL2,
    DAY7_EST_VAL2,
    STORE_MODEL_STOCK,
    DAY1_DELIV_PAT1,
    DAY2_DELIV_PAT1,
    DAY3_DELIV_PAT1,
    DAY4_DELIV_PAT1,
    DAY5_DELIV_PAT1,
    DAY6_DELIV_PAT1,
    DAY7_DELIV_PAT1,
    NUM_UNITS_PER_TRAY,
    NUM_UNITS_PER_TRAY2,
    LAST_UPDATED_DATE,
    REG_RSP_INCL_VAT_LOCAL)
VALUES
(   --CASE dwh_log.merge_counter(dwh_log.c_inserting)
    --      WHEN 0 THEN mer_st_loc_item.sk1_location_no
    --END,
    mer_st_loc_item.sk1_location_no,
    mer_st_loc_item.sk1_item_no,
    mer_st_loc_item.NUM_STORE_LEADTIME_DAYS,
    mer_st_loc_item.MODEL_STOCK,
    mer_st_loc_item.DELIVERY_PATTERN,
    mer_st_loc_item.SHELF_LIFE,
    mer_st_loc_item.PROD_STATUS_1,
    mer_st_loc_item.PROD_STATUS_2,
    mer_st_loc_item.DAY1_EST_VAL2,
    mer_st_loc_item.DAY2_EST_VAL2,
    mer_st_loc_item.DAY3_EST_VAL2,
    mer_st_loc_item.DAY4_EST_VAL2,
    mer_st_loc_item.DAY5_EST_VAL2,
    mer_st_loc_item.DAY6_EST_VAL2,
    mer_st_loc_item.DAY7_EST_VAL2,
    mer_st_loc_item.STORE_MODEL_STOCK,
    mer_st_loc_item.DAY1_DELIV_PAT1,
    mer_st_loc_item.DAY2_DELIV_PAT1,
    mer_st_loc_item.DAY3_DELIV_PAT1,
    mer_st_loc_item.DAY4_DELIV_PAT1,
    mer_st_loc_item.DAY5_DELIV_PAT1,
    mer_st_loc_item.DAY6_DELIV_PAT1,
    mer_st_loc_item.DAY7_DELIV_PAT1,
    mer_st_loc_item.NUM_UNITS_PER_TRAY,
    mer_st_loc_item.NUM_UNITS_PER_TRAY2,
    mer_st_loc_item.LAST_UPDATED_DATE,
    mer_st_loc_item.REG_RSP_INCL_VAT_LOCAL
 )
;

  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;


    commit;

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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


end wh_prf_corp_614u;
