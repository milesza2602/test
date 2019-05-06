--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_254T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_254T" (p_forall_limit in integer, p_success out boolean) as 

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract2_1_t
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
g_rec_out            w6005682.foods_renewal_extract2_1_t%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_254T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 2 PART 1 - DC ITEM SUPPLIER';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of w6005682.foods_renewal_extract2_1_t%rowtype index by binary_integer;
type tbl_array_u is table of w6005682.foods_renewal_extract2_1_t%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;


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
    
    l_text := 'LOAD OF foods_renewal_extract2_1_t started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table w6005682.foods_renewal_extract2_1_t');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    
    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

--  l_text := 'Date being processed B4 lookup: ' || l_today_date ;
l_text := 'Date being processed B4 lookup: 25/AUG/13';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 select today_date
   into l_today_date
   from dim_control;
     
   
-- l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
-- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
     
     
 --l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO w6005682.foods_renewal_extract2_1_t mart
WITH item_list AS
  (  select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name, di.fd_discipline_type
            from dim_item di, dim_department dd
               where dd.department_no in (41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
                 and di.department_no = dd.department_no
  ) ,  
  
  loc_list AS
  (SELECT location_no,
    --location_name    ,
    sk1_location_no  ,
    wh_fd_zone_no    ,
    SK1_FD_ZONE_GROUP_ZONE_NO
  FROM dim_location
  )  ,  
  
  supp_list AS
  (SELECT sk1_supplier_no, supplier_no, supplier_name FROM dim_supplier
  ),
  
  loc_item_list AS
  (SELECT /*+ PARALLEL(a,4) FULL(a) */
          b.item_no,
          --c.location_no,
          c.wh_fd_zone_no    ,
          SUM(NVL(a.min_order_qty,0)) min_order_qty
     FROM rtl_location_item a,
          item_list b              ,
          loc_list c
    WHERE a.sk1_item_no = b.sk1_item_no
      AND a.sk1_location_no = c.sk1_location_no
    GROUP BY b.item_no, c.wh_fd_zone_no
  ) ,   --  select * from loc_item_list;   -- where sk1_item_no = 265397;
  
  zone_item_om AS
  (SELECT UNIQUE /*+ PARALLEL(zom,4) FULL(zom) */
    il.item_no       ,
    zom.supplier_no               ,
    ll.wh_fd_zone_no              ,
    --zom.num_extra_leadtime_days   ,
    substr(zom.num_extra_leadtime_days, 1,1) num_extra_leadtime_dy1,
    substr(zom.num_extra_leadtime_days, 2,1) num_extra_leadtime_dy2,
    substr(zom.num_extra_leadtime_days, 3,1) num_extra_leadtime_dy3,
    substr(zom.num_extra_leadtime_days, 4,1) num_extra_leadtime_dy4,
    substr(zom.num_extra_leadtime_days, 5,1) num_extra_leadtime_dy5,
    substr(zom.num_extra_leadtime_days, 6,1) num_extra_leadtime_dy6,
    substr(zom.num_extra_leadtime_days, 7,1) num_extra_leadtime_dy7,
    zom.num_supplier_leadtime_days,
    zom.reg_rsp_excl_vat          ,
    zom.reg_delivery_pattern_code,
    zom.num_units_per_tray,
    zom.num_shelf_life_days
    FROM rtl_zone_item_om zom,
    item_list il              ,
    loc_list ll               ,
    supp_list sl
    WHERE zom.sk1_item_no        = il.sk1_item_no
  AND zom.sk1_zone_group_zone_no = ll.SK1_FD_ZONE_GROUP_ZONE_NO
  AND zom.supplier_no            = sl.supplier_no
  AND il.sk1_supplier_no         = sl.sk1_supplier_no
  ) ,  
  
--  aa AS
--  (SELECT /*+ PARALLEL(c,4) FULL(c) */
/*    d.fin_year_no                              ,
    d.fin_week_no                                    ,
    d.fin_day_no                                     ,
    b.wh_fd_zone_no                                  ,
    c.calendar_date                                  ,
    a.item_no                                        ,
    e.supplier_no                                    ,
    SUM(NVL(num_units_per_tray,0)) num_units_per_tray,
    SUM(NVL(num_shelf_life_days,0)) num_shelf_life_days
    --sum(nvl(boh_adj_qty_flt,0)) boh_adj_qty_flt
     FROM item_list a        ,
    loc_list b               ,
    rtl_loc_item_dy_catalog c,
    dim_calendar d           ,
    supp_list e
    WHERE c.calendar_date = l_today_date --and '21/JUL/13'
  AND c.calendar_date     = d.calendar_date
  AND a.sk1_supplier_no   = e.sk1_supplier_no
  AND a.sk1_item_no       = c.sk1_item_no
  AND b.sk1_location_no   = c.sk1_location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.calendar_date, a.item_no, e.supplier_no
  ),   
 */
 
  bb AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */ 
    d.fin_year_no    ,
    d.fin_week_no          ,
    d.fin_day_no fin_day_no, --d.fin_day_no,
    b.wh_fd_zone_no        ,
    d.calendar_date,
    a.item_no              ,
    e.supplier_no          ,
    MAX(c.ship_hi) ship_hi ,
    MAX(c.ship_ti) ship_ti
     FROM item_list a  ,
    loc_list b         ,
    rtl_depot_item_wk c,
    dim_calendar d     ,
    supp_list e
    WHERE d.calendar_date = l_today_date
  AND c.fin_year_no       = d.fin_year_no
  AND c.fin_week_no       = d.fin_week_no
  AND a.sk1_supplier_no   = e.sk1_supplier_no
  AND a.sk1_item_no       = c.sk1_item_no
  AND c.supplier_no       = e.supplier_no
  AND b.sk1_location_no   = c.sk1_location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, d.calendar_date, a.item_no, e.supplier_no
  ),  
  
  cc AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                                         ,
    d.fin_week_no                                               ,
    d.fin_day_no                                                ,
    b.wh_fd_zone_no                                             ,
    c.post_date                                                 ,
    a.item_no                                                   ,
    e.supplier_no                                               ,
    SUM(NVL(c.STOCK_DC_COVER_CASES,0)) DC_COVER_STOCK_CASES     ,
    SUM(NVL(c.est_ll_dc_cover_cases_av,0)) EST_LL_SHORT_CASES_AV,
    SUM(NVL(c.STOCK_CASES,0)) STOCK_CASES,
    
    sum(case when nvl(c.est_ll_dc_cover_cases_av,0) > 0 then
       (c.stock_cases / (c.est_ll_dc_cover_cases_av * 7))
    else
       364
    end)  as dc_cover
     FROM item_list a  ,
    loc_list b         ,
    rtl_depot_item_dy c,
    dim_calendar d     ,
    supp_list e
   WHERE c.post_date       = l_today_date --and '21/JUL/13'
     AND c.post_date       = d.calendar_date
     AND a.sk1_supplier_no = e.sk1_supplier_no
     AND a.sk1_item_no     = c.sk1_item_no
     AND b.sk1_location_no = c.sk1_location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, a.item_no, e.supplier_no
  ),  
  
  dd AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                                              ,
    d.fin_week_no                                                    ,
    d.fin_day_no                                                     ,
    b.wh_fd_zone_no                                                  ,
    c.into_store_date                                                ,
    a.item_no,        
    e.supplier_no                                                    ,
    MAX(c.REJECTED_BARCODE)          AS num_rejected_barcode         ,
    MAX(c.REJECTED_QTY_PER_LUG)      AS num_rejected_qty_per_lug     ,
    MAX(c.REJECTED_NUM_SELL_BY_DAYS) AS num_rejected_num_sell_by_days,
    MAX(c.REJECTED_SELL_PRICE)       AS num_rejected_sell_price      ,
    MAX(c.REJECTED_IDEAL_TEMP)       AS num_rejected_ideal_temp      ,
    MAX(c.REJECTED_MAX_TEMP_RANGE)   AS num_rejected_max_temp_range  ,
    MAX(c.REJECTED_ALT_SUPP)         AS num_rejected_alt_supp        ,
    MAX(c.REJECTED_OVER_DELIVERY)    AS num_rejected_over_delivery   ,
    MAX(c.REJECTED_TOLERANCE_MASS)   AS num_rejected_tolerance_mass  ,
    MAX(c.REJECTED_OUT_CASE)         AS num_rejected_out_case        ,
    MAX(c.MAX_SELL_BY_DATE)          AS max_sell_by_date             ,
    MAX(c.MIN_SELL_BY_DATE)          AS min_sell_by_date
  FROM item_list a       ,
    loc_list b              ,
    fnd_rtl_purchase_order c,
    dim_calendar d          ,
    supp_list e
 WHERE c.into_store_date     = l_today_date --and '21/JUL/13'
   AND c.into_store_date     = d.calendar_date
   AND c.supplier_no         = e.supplier_no
   AND a.item_no             = c.item_no
   AND b.location_no         = c.location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.into_store_date, a.item_no, e.supplier_no
  ) ,   
  
  ee AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no,
    d.fin_week_no      ,
    d.fin_day_no       ,
    b.wh_fd_zone_no    ,
    c.post_date        ,
    a.item_no          ,
    e.supplier_no      ,
    MAX(delivery_pattern) delivery_pattern
  FROM item_list a       ,
        loc_list b              ,
        rtl_loc_item_dy_st_ord c,
        dim_calendar d          ,
        supp_list e
  WHERE c.post_date       = l_today_date --and '21/JUL/13'
    AND c.post_date       = d.calendar_date
    AND a.sk1_supplier_no = e.sk1_supplier_no
    AND a.sk1_item_no     = c.sk1_item_no
    AND b.sk1_location_no = c.sk1_location_no
 GROUP BY d.fin_year_no,
          d.fin_week_no      ,
          d.fin_day_no       ,
          b.wh_fd_zone_no    ,
          c.post_date        ,
          a.item_no          ,
          e.supplier_no
  ) ,   --   select * from ee ;   --where item_no = 6001009027702 and location_no = 602 and supplier_no = 57480;    --804931
  
  
  xx AS (
   SELECT 
    NVL(NVL(NVL(b.fin_year_no, c.fin_year_no), d.fin_year_no), e.fin_year_no) fin_year_no,                           --p.fin_year_no),cust.fin_year_no), rdf.fin_year_no) fin_year_no,
    NVL(NVL(NVL(b.fin_week_no, c.fin_week_no), d.fin_week_no), e.fin_week_no) fin_week_no,                --p.fin_week_no), cust.fin_week_no), rdf.fin_week_no) fin_week_no,
    NVL(NVL(NVL(b.fin_day_no, c.fin_day_no), d.fin_day_no), e.fin_day_no) fin_day_no,                      --p.fin_day_no), cust.fin_day_no), rdf.fin_day_no) fin_day_no,
    NVL(NVL(NVL(b.wh_fd_zone_no, c.wh_fd_zone_no), d.wh_fd_zone_no), e.wh_fd_zone_no) wh_fd_zone_no,    --p.fin_year_no), cust.wh_fd_zone_no), rdf.wh_fd_zone_no) wh_fd_zone_no,
    NVL(NVL(NVL(b.item_no, c.item_no), d.item_no), e.item_no) item_no,                                        --p.item_no), cust.item_no), rdf.item_no)   item_no,
    NVL(NVL(NVL(b.supplier_no, c.supplier_no), d.supplier_no), e.supplier_no) supplier_no,                --p.supplier_no), cust.supplier_no), rdf.supplier_no) supplier_no,
    
    NVL(b.ship_hi,'') ship_hi,
    NVL(b.ship_ti,'') ship_ti,
    NVL(c.DC_COVER_STOCK_CASES,0) DC_COVER_STOCK_CASES,
    NVL(c.EST_LL_SHORT_CASES_AV,0) EST_LL_SHORT_CASES_AV,
    NVL(c.STOCK_CASES,0) STOCK_CASES,
    NVL(d.NUM_REJECTED_BARCODE,'')          AS num_rejected_barcode,
    NVL(d.NUM_REJECTED_QTY_PER_LUG,'')      AS num_rejected_qty_per_lug,
    NVL(d.NUM_REJECTED_NUM_SELL_BY_DAYS,'') AS num_rejected_num_sell_by_days,
    NVL(d.NUM_REJECTED_SELL_PRICE,'')       AS num_rejected_sell_price,
    NVL(d.NUM_REJECTED_IDEAL_TEMP,'')       AS num_rejected_ideal_temp,
    NVL(d.NUM_REJECTED_MAX_TEMP_RANGE,'')   AS num_rejected_max_temp_range,
    NVL(d.NUM_REJECTED_ALT_SUPP,'')         AS num_rejected_alt_supp,
    NVL(d.NUM_REJECTED_OVER_DELIVERY,'')    AS num_rejected_over_delivery,
    NVL(d.NUM_REJECTED_TOLERANCE_MASS,'')   AS num_rejected_tolerance_mass,
    NVL(d.NUM_REJECTED_OUT_CASE,'')         AS num_rejected_out_case,
    NVL(d.MAX_SELL_BY_DATE,'')              AS max_sell_by_date,
    NVL(d.MIN_SELL_BY_DATE,'')              AS min_sell_by_date,
    NVL(e.delivery_pattern,0)               as delivery_pattern,
    nvl(c.dc_cover,364)                     as dc_cover
   
  FROM bb b
  FULL OUTER JOIN cc c
       ON b.item_no       = c.item_no
      AND b.wh_fd_zone_no = c.wh_fd_zone_no
      and b.calendar_date = c.post_date
     and b.supplier_no    = c.supplier_no
     
  FULL OUTER JOIN dd d
       ON NVL(b.item_no, c.item_no)             = d.item_no
      AND NVL(b.wh_fd_zone_no, c.wh_fd_zone_no) = d.wh_fd_zone_no
      AND nvl(b.calendar_date, c.post_date)     = d.into_store_date
      and nvl(b.supplier_no, c.supplier_no)     = d.supplier_no
 
  FULL OUTER JOIN ee e
       ON NVL(NVL(b.item_no, c.item_no), d.item_no)                   = e.item_no
      AND NVL(NVL(b.wh_fd_zone_no, c.wh_fd_zone_no), d.wh_fd_zone_no) = e.wh_fd_zone_no
      AND NVL(NVL(b.calendar_date, c.post_date), d.into_store_date)   = e.post_date
      and nvl(nvl(b.supplier_no, c.supplier_no), d.supplier_no)       = e.supplier_no
  
  ) 
  SELECT xx.fin_year_no                  ,        
        xx.fin_week_no                  ,
        xx.fin_day_no                   ,
        xx.wh_fd_zone_no as dc_region   ,
        xx.item_no                      ,
        item_list.item_desc             ,
        item_list.fd_discipline_type    ,
        item_list.department_no         ,
        item_list.department_name       ,
        item_list.subclass_no           ,
        item_list.subclass_name         ,
        supp_list.supplier_no           ,
        supp_list.supplier_name         ,
        zom.num_units_per_tray          ,
        zom.num_shelf_life_days         ,
        --zom.num_extra_leadtime_days     ,
        zom.num_extra_leadtime_dy1      ,
        zom.num_extra_leadtime_dy2      ,
        zom.num_extra_leadtime_dy3      ,
        zom.num_extra_leadtime_dy4      ,
        zom.num_extra_leadtime_dy5      ,
        zom.num_extra_leadtime_dy6      ,
        zom.num_extra_leadtime_dy7      ,
        zom.num_supplier_leadtime_days  ,
        xx.ship_hi                      ,
        xx.ship_ti                      ,
        loc_item_list.min_order_qty     ,
        zom.reg_rsp_excl_vat as reg_rsp_excl_vat_om       ,
        xx.dc_cover,
        xx.dc_cover_stock_cases         ,
        xx.est_ll_short_cases_av        ,
        xx.stock_cases                  ,
        xx.num_rejected_barcode         ,
        xx.num_rejected_qty_per_lug     ,
        xx.num_rejected_num_sell_by_days,
        xx.num_rejected_sell_price      ,
        xx.num_rejected_ideal_temp      ,
        xx.num_rejected_max_temp_range  ,
        xx.num_rejected_alt_supp        ,
        xx.num_rejected_over_delivery   ,
        xx.num_rejected_tolerance_mass  ,
        xx.num_rejected_out_case        ,
        xx.delivery_pattern             ,
        zom.reg_delivery_pattern_code   ,
        xx.max_sell_by_date             ,
        xx.min_sell_by_date             
        
   FROM xx ,
        item_list,
        supp_list,
        loc_item_list,
        zone_item_om zom
  WHERE xx.item_no            = item_list.item_no
    AND xx.supplier_no        = supp_list.supplier_no
    AND xx.item_no            = loc_item_list.item_no
    and xx.wh_fd_zone_no      = loc_item_list.wh_fd_zone_no
    and xx.item_no            = zom.item_no
    and xx.supplier_no        = zom.supplier_no
    and xx.wh_fd_zone_no      = zom.wh_fd_zone_no 
    ORDER BY xx.item_no

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
       
end wh_prf_corp_254t;
