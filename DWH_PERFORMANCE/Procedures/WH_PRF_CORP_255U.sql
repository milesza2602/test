--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_255U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_255U" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract_2_2
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_rec_out            DWH_PERFORMANCE.FOODS_RENEWAL_EXTRACT2_2%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_255U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 2 PART 2 - STORE ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.FOODS_RENEWAL_EXTRACT2_2%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.FOODS_RENEWAL_EXTRACT2_2%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) ;
l_catalog_date      date := trunc(sysdate) - 1;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF foods_renewal_extract_2_2 started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table DWH_PERFORMANCE.FOODS_RENEWAL_EXTRACT2_2');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

  l_text := 'Date being processed B4 lookup: ' || l_today_date ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- select today_date
--   into l_today_date
--   from dim_control;

 l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO DWH_PERFORMANCE.FOODS_RENEWAL_EXTRACT2_2 mart
WITH item_list AS
  (  select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name
            from dim_item di, dim_department dd
               --where dd.department_no in (44, 88, 41, 47, 68, 75, 85, 46, 51)  --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
               --where dd.department_no in (97,88,83,59,73,91,13,53,95,93,66,89,90,80,58,16,34,42,52,62,85)   --(88, 96, 73, 58, 47,55, 13, 44, 68, 81, 85, 75, 41, 40)
               --where dd.department_no in (15,20,41,43,48,64,71,78,99)
               --where dd.department_no in (49,65,87,88)
--               where dd.department_no in (13,44,68,73,83,10,11,45,55,58,64,82,96)
--               where dd.department_no in (45,64,22,26,27,32,33,67,69,70,97,58,96)
--               where dd.department_no in (12,28,29,36,37,53,57,59,66,76,77,79,86,89,91,95,98)
               where dd.department_no in (14,17,18,19,21,23,24,30,31,35,39,50,56,60,63,72,80,84,90,93)
                 and di.department_no = dd.department_no
  ) ,

  loc_list AS
  (SELECT location_no,
    location_name    ,
    sk1_location_no  ,
    wh_fd_zone_no    ,
    SK1_FD_ZONE_GROUP_ZONE_NO
    --num_store_leadtime_days
     FROM dim_location
  ) ,

   zone_item_om AS
  (SELECT UNIQUE /*+ PARALLEL(zom,4) FULL(zom) */
    il.item_no       ,
    ll.location_no,
    ll.wh_fd_zone_no              ,
    zom.num_extra_leadtime_days   ,
    zom.num_supplier_leadtime_days,
    zom.reg_rsp_excl_vat          ,
    zom.reg_delivery_pattern_code
    FROM rtl_zone_item_om zom,
    item_list il              ,
    loc_list ll
    WHERE zom.sk1_item_no        = il.sk1_item_no
  AND zom.sk1_zone_group_zone_no = ll.SK1_FD_ZONE_GROUP_ZONE_NO
   ) ,

  -- This is to get the Catalog data for yesterday
  aa1 AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                              ,
    d.fin_week_no                                    ,
    d.fin_day_no                                     ,
    b.wh_fd_zone_no                                  ,
    c.calendar_date                                  ,
    a.item_no                                        ,
    b.location_no,
    SUM(NVL(num_units_per_tray,0)) num_units_per_tray,
    SUM(NVL(num_shelf_life_days,0)) num_shelf_life_days,
    sum(nvl(boh_adj_qty_flt,0)) boh_adj_qty_flt,
    sum(nvl(wk_delivery_pattern,0)) delivery_pattern
     FROM item_list a        ,
    loc_list b               ,
    rtl_loc_item_dy_catalog c,
    dim_calendar d
    WHERE c.calendar_date = l_catalog_date --and '21/JUL/13'
  AND c.calendar_date     = d.calendar_date
  AND a.sk1_item_no       = c.sk1_item_no
  AND b.sk1_location_no   = c.sk1_location_no
 group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.calendar_date, a.item_no, b.location_no
  ) ,

  --This presents the catalog data from yesterday with today's date in order to keep it in line with
  --the rest of the data from other sources.  Clever, huh ?
  aa as (
  select
    b.fin_year_no                              ,
    b.fin_week_no                                    ,
    b.fin_day_no                                     ,
    a.wh_fd_zone_no                                  ,
    b.calendar_date                                  ,
    a.item_no                                        ,
    a.location_no,
    a.num_units_per_tray,
    a.num_shelf_life_days,
    a.boh_adj_qty_flt,
    a.delivery_pattern
    FROM aa1 a,
    dim_calendar b
    WHERE b.calendar_date = trunc(a.calendar_date) + 1
   ),

  ee AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no,
    d.fin_week_no      ,
    d.fin_day_no       ,
    b.wh_fd_zone_no    ,
    c.post_date        ,
    a.item_no          ,
    b.location_no,
    --MAX(delivery_pattern) delivery_pattern,
    sum(nvl(safety_qty,0)) safety_qty,
    sum(num_store_leadtime_days) num_store_leadtime_days
   FROM item_list a       ,
        loc_list b              ,
        rtl_loc_item_dy_st_ord c,
        dim_calendar d
  WHERE c.post_date   = l_today_date --and '21/JUL/13'
    AND c.post_date       = d.calendar_date
    AND a.sk1_item_no     = c.sk1_item_no
    AND b.sk1_location_no = c.sk1_location_no
 group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, a.item_no, b.location_no
  ) ,

  prom as (
    select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       max(dp.prom_type) prom_type
from item_list a, loc_list b, rtl_prom_loc_item_dy c, dim_calendar d, dim_prom dp
where c.post_date = l_today_date --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
and c.sk1_prom_no = dp.sk1_prom_no
and c.sk1_prom_period_no = 9749661
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
) ,


cust as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       sum(nvl(c.cust_order_qty,0)) cust_order_qty
from item_list a, loc_list b, rtl_loc_item_dy_om_ord c, dim_calendar d
--from dim_item a, dim_location b, rtl_loc_item_dy_om_ord c
where c.post_date = l_today_date    --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
),

RDF as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       sum(nvl(sales_dly_app_fcst_qty,0)) sales_dly_app_fcst_qty,
       sum(nvl(sales_dly_app_fcst_qty_flt_av,0)) sales_dly_app_fcst_qty_ll_av,
       sum(nvl(sales_dly_app_fcst,0)) app_fcst_selling
--from item_list a, loc_list b, rtl_loc_item_dy_rdf_fcst c, dim_calendar d      --RDF L1/L2 remapping change
from item_list a, loc_list b, RTL_LOC_ITEM_RDF_DYFCST_L2 c, dim_calendar d
where c.post_date = l_today_date --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
)  ,

TRICEPS as (
  select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.into_store_date,
       b.location_no,
       a.item_no,
       sum(nvl(special_cases,0)) special_cases,
       sum(nvl(forecast_cases,0)) forecast_cases,
       sum(nvl(safety_cases,0)) safety_cases,
       sum(nvl(over_cases,0)) over_cases
from item_list a, loc_list b, fnd_rtl_loc_item_dy_trcps_pick c, dim_calendar d
where c.into_store_date = l_today_date --and '21/JUL/13'
and c.into_store_date = d.calendar_date
and a.item_no = c.item_no
and b.location_no = c.location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.into_store_date, b.location_no, a.item_no
),

  xx AS (
   SELECT
    NVL(NVL(NVL(NVL(NVL(a.fin_year_no, e.fin_year_no), p.fin_year_no), cust.fin_year_no), rdf.fin_year_no), trcp.fin_year_no) fin_year_no,
    NVL(NVL(NVL(NVL(NVL(a.fin_week_no, e.fin_week_no), p.fin_week_no), cust.fin_week_no), rdf.fin_week_no), trcp.fin_week_no) fin_week_no,
    NVL(NVL(NVL(NVL(NVL(a.fin_day_no, e.fin_day_no), p.fin_day_no), cust.fin_day_no), rdf.fin_day_no), trcp.fin_day_no) fin_day_no,
    NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, e.wh_fd_zone_no), p.wh_fd_zone_no), cust.wh_fd_zone_no), rdf.wh_fd_zone_no), trcp.wh_fd_zone_no) wh_fd_zone_no,
    NVL(NVL(NVL(NVL(NVL(a.item_no, e.item_no), p.item_no), cust.item_no), rdf.item_no), trcp.item_no)   item_no,
    NVL(NVL(NVL(NVL(NVL(a.location_no, e.location_no), p.location_no), cust.location_no), rdf.location_no), trcp.location_no) location_no,


    nvl(rdf.app_fcst_selling, 0)            as app_fcst_selling,
    nvl(p.prom_type,'')                     as prom_type,
    NVL(a.num_units_per_tray,0)             as num_units_per_tray,
    NVL(a.num_shelf_life_days,0)            as num_shelf_life_days,
    nvl(e.safety_qty,'')                    as safety_qty,
    case when nvl(rdf.sales_dly_app_fcst_qty_ll_av,0) > 0 then
       (nvl(a.boh_adj_qty_flt,0) / (nvl(rdf.sales_dly_app_fcst_qty_ll_av,0) * 7))
    else
       364
    end as store_cover,
    nvl(rdf.sales_dly_app_fcst_qty_ll_av,0) as sales_dly_app_fcst_qty_ll_av,
    nvl(a.boh_adj_qty_flt,0)                as boh_ll_adj_qty,
    nvl(cust.cust_order_qty,0)              as cust_order_qty,
    --NVL(e.delivery_pattern,0)               as delivery_pattern,
    NVL(a.delivery_pattern,0)               as delivery_pattern,
    nvl(rdf.sales_dly_app_fcst_qty,0)       AS sales_dly_app_fcst_qty,
    nvl(e.num_store_leadtime_days,0)        AS num_store_leadtime_days,
    nvl(trcp.special_cases,0)               as special_cases,
    nvl(trcp.forecast_cases,0)              as forecast_cases,
    nvl(trcp.safety_cases,0)                as safety_cases,
    nvl(trcp.over_cases,0)                  as over_cases

  FROM aa a
  FULL OUTER JOIN ee e
       ON a.item_no       = e.item_no
      AND a.wh_fd_zone_no = e.wh_fd_zone_no
      and a.calendar_date = e.post_date
      and a.location_no   = e.location_no

  FULL OUTER JOIN prom p
       ON NVL(a.item_no, e.item_no)             = p.item_no
      AND NVL(a.wh_fd_zone_no, e.wh_fd_zone_no) = p.wh_fd_zone_no
      AND nvl(a.calendar_date, e.post_date)     = p.post_date
      and nvl(a.location_no, e.location_no)     = p.location_no

  FULL OUTER JOIN cust cust
       ON NVL(NVL(a.item_no, e.item_no), p.item_no)                   = cust.item_no
      AND NVL(NVL(a.wh_fd_zone_no, e.wh_fd_zone_no), p.wh_fd_zone_no) = cust.wh_fd_zone_no
      AND NVL(NVL(a.calendar_date, e.post_date), p.post_date)         = cust.post_date
      and nvl(nvl(a.location_no, e.location_no), p.location_no)       = cust.location_no

  FULL OUTER JOIN rdf rdf
       ON NVL(NVL(NVL(a.item_no, e.item_no), p.item_no), cust.item_no)                         = rdf.item_no
      AND NVL(NVL(NVL(a.wh_fd_zone_no, e.wh_fd_zone_no), p.wh_fd_zone_no), cust.wh_fd_zone_no) = rdf.wh_fd_zone_no
      AND NVL(NVL(NVL(a.calendar_date, e.post_date), p.post_date), cust.post_date)             = rdf.post_date
      and NVL(nvl(nvl(a.location_no, e.location_no), p.location_no), cust.location_no)         = rdf.location_no

  FULL OUTER JOIN triceps trcp
       ON NVL(NVL(NVL(NVL(a.item_no, e.item_no), p.item_no), cust.item_no), rdf.item_no)                                = trcp.item_no
      AND NVL(NVL(NVL(NVL(a.wh_fd_zone_no, e.wh_fd_zone_no), p.wh_fd_zone_no), cust.wh_fd_zone_no), rdf.wh_fd_zone_no)  = trcp.wh_fd_zone_no
      AND NVL(NVL(NVL(NVL(a.calendar_date, e.post_date), p.post_date), cust.post_date), rdf.post_date)                  = trcp.into_store_date
      and NVL(NVL(nvl(nvl(a.location_no, e.location_no), p.location_no), cust.location_no), rdf.location_no)            = trcp.location_no

  )

  SELECT xx.fin_year_no                             ,
        xx.fin_week_no                              ,
        xx.fin_day_no                               ,
        xx.wh_fd_zone_no as dc_region               ,
        xx.item_no                                  ,
        item_list.item_desc                         ,
        xx.location_no                              ,
        loc_list.location_name                      ,
        item_list.department_no                     ,
        item_list.department_name                   ,
        item_list.subclass_no                       ,
        item_list.subclass_name                     ,
        xx.app_fcst_selling                         ,
        xx.prom_type                                ,
        xx.num_units_per_tray                       ,
        xx.num_shelf_life_days                      ,
        --loc_list.num_store_leadtime_days,
        xx.num_store_leadtime_days                  ,
        xx.safety_qty                               ,
        xx.store_cover                              ,
        xx.sales_dly_app_fcst_qty_ll_av             ,
        xx.boh_ll_adj_qty                           ,
        xx.cust_order_qty                           ,
        zom.reg_rsp_excl_vat as reg_rsp_excl_vat_om ,
        xx.delivery_pattern                         ,
        zom.reg_delivery_pattern_code               ,
        xx.sales_dly_app_fcst_qty                   ,
        xx.special_cases                            ,
        xx.forecast_cases                           ,
        xx.safety_cases                             ,
        xx.over_cases

   FROM xx ,
        item_list,
        loc_list,
        zone_item_om zom
  WHERE xx.item_no            = item_list.item_no
    and xx.location_no        = loc_list.location_no
    and xx.item_no            = zom.item_no
    and xx.location_no        = zom.location_no
    and xx.wh_fd_zone_no      = zom.wh_fd_zone_no
    ORDER BY xx.item_no, xx.location_no
;

g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

--**************************************************************************************************
-- Write final log data
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

end wh_prf_corp_255u;
