--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_259A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_259A" (p_forall_limit in integer, p_success out boolean) as 

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract2_2wk_roq
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
g_rec_out            W6005682.FOODS_RENEW_EXTRACT2_2WK_RQ_A%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_259A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 2 - 2WKS FORWARD ORDERS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.FOODS_RENEW_EXTRACT2_2WK_RQ_A%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.FOODS_RENEW_EXTRACT2_2WK_RQ_A%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate);
l_days              integer;
l_end_date          date;
l_start_date        date;
l_fin_day_no        integer;

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
    
    l_text := 'LOAD OF foods_renewal_extract2_2wk_roq started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table W6005682.FOODS_RENEW_EXTRACT2_2WK_RQ_A');
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

 select fin_day_no into l_fin_day_no
   from dim_calendar
  where calendar_date = l_today_date;
   
   --------------------------------------------------------------------------------------
   -- This is to determine the number of days going back from current day that must be --
   -- use to determine the start date for the two weeks prior to the current week.     --
   --------------------------------------------------------------------------------------
   if l_fin_day_no = 1 then l_days := 14; end if;
   if l_fin_day_no = 2 then l_days := 15; end if;
   if l_fin_day_no = 3 then l_days := 16; end if;
   if l_fin_day_no = 4 then l_days := 17; end if;
   if l_fin_day_no = 5 then l_days := 18; end if;
   if l_fin_day_no = 6 then l_days := 19; end if;
   if l_fin_day_no = 7 then l_days := 20; end if;

 
 select calendar_date into l_end_date
   from dim_calendar 
   where calendar_date = l_today_date;
   
 select calendar_date 
   into l_start_date 
   from dim_calendar
  where calendar_date = trunc(sysdate) - l_days;
  
  l_text := 'days = ' || l_days;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'start date = ' || l_start_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'end date = ' || l_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
--l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO W6005682.FOODS_RENEW_EXTRACT2_2WK_RQ_A mart
with 
 item_list as (select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name
                From Dim_item Di, Dim_department Dd
               where di.department_no in (13,41,44,47,55,58,68,73,75,81,85,88,96)  --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
                 and di.department_no = dd.department_no), 
 loc_list as (select location_no, location_name, sk1_location_no, wh_fd_zone_no from dim_location),
 
 store_stuff as 
   (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no,
    d.fin_week_no      ,
    d.fin_day_no       ,
    b.wh_fd_zone_no    ,
    c.post_date        ,
    a.item_no          ,
    b.location_no,
    sum(num_store_leadtime_days) store_lead_time, 
    sum(nvl(num_units_per_tray,0)) num_units_per_tray,
    sum(nvl(store_order1,0))   store_order1,
    sum(nvl(store_order2,0))   store_order2,
    sum(nvl(store_order3,0))   store_order3,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty1,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty2,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty3,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty4,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty5,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty6,
    sum(nvl(direct_mu_qty1,0)) direct_mu_qty7
   FROM item_list a       ,
        loc_list b              ,
        rtl_loc_item_dy_st_ord c,
        dim_calendar d          
  WHERE c.post_date       between l_start_date and l_end_date
    AND c.post_date       = d.calendar_date
    AND a.sk1_item_no     = c.sk1_item_no
    AND b.sk1_location_no = c.sk1_location_no
    --and c.direct_delivery_ind = 1
 group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, a.item_no, b.location_no
  ) ,   --select * from store_stuff;
 
 roq as (
 select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no, 
       c.post_date,
       b.location_no,
       a.item_no, 
       sum(nvl(c.roq_qty,0)) roq_units
from item_list a, loc_list b, rtl_loc_item_dy_om_ord c, dim_calendar d
where c.post_date  between l_start_date and l_end_date
and c.post_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no 
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
),   -- select * from roq;

 po as (
    select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no, 
       c.into_store_date,
       b.location_no,
       a.item_no
--from item_list a, loc_list b, fnd_rtl_purchase_order c, dim_calendar d
from item_list a, loc_list b, rtl_loc_item_dy_pick c, dim_calendar d
where c.into_store_date  between l_start_date and l_end_date
and c.into_store_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no 
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.into_store_date, b.location_no, a.item_no
  ),  -- select * from po;
 
 xx AS (
   SELECT 
    NVL(NVL(a.fin_year_no, b.fin_year_no), c.fin_year_no) fin_year_no,                    
    NVL(NVL(a.fin_week_no, b.fin_week_no), c.fin_week_no) fin_week_no,               
    NVL(NVL(a.fin_day_no,  b.fin_day_no), c.fin_day_no) fin_day_no,                      
    NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no) wh_fd_zone_no,    
    NVL(NVL(a.item_no, b.item_no), c.item_no) item_no,         
    NVL(NVL(a.location_no, b.location_no), c.location_no) location_no,
   
    NVL(a.store_lead_time,0)         as store_lead_time,
    nvl(a.num_units_per_tray,0)      as num_units_per_tray,
    nvl(a.store_order1,0)            as store_order1,
    nvl(a.store_order2,0)            as store_order2,
    nvl(a.store_order3,0)            as store_order3,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty1,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty2,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty3,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty4,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty5,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty6,
    nvl(a.direct_mu_qty1,0)          as direct_mu_qty7,
    nvl(b.roq_units, 0)              as roq_units,
    nvl(c.into_store_date, '')       as into_store_date,
    nvl(a.post_date, '')            as st_ord_post_date
   
  FROM store_stuff a
  FULL OUTER JOIN roq b
       ON a.fin_year_no   = b.fin_year_no
      AND a.fin_week_no   = b.fin_week_no
      AND a.fin_day_no    = b.fin_day_no
      AND a.item_no       = b.item_no
      AND a.wh_fd_zone_no = b.wh_fd_zone_no
      --AND a.post_date     = b.post_date
      AND a.location_no   = b.location_no
     
  FULL OUTER JOIN po c
       ON NVL(a.fin_year_no, b.fin_year_no)     = c.fin_year_no
      AND NVL(a.fin_week_no, b.fin_week_no)     = c.fin_week_no
      AND NVL(a.fin_day_no, b.fin_day_no)       = c.fin_day_no
      AND NVL(a.item_no, b.item_no)             = c.item_no
      AND NVL(a.wh_fd_zone_no, b.wh_fd_zone_no) = c.wh_fd_zone_no
      --AND nvl(a.post_date, b.post_date)         = c.into_store_date
      AND NVL(a.location_no, b.location_no)     = c.location_no
      
 )
  
 
 SELECT xx.fin_year_no                  ,        
        xx.fin_week_no                  ,
        xx.fin_day_no                   ,
        xx.wh_fd_zone_no as dc_region   ,
        xx.item_no                      ,
        item_list.item_desc             ,
        loc_list.location_no            ,
        loc_list.location_name          ,
        item_list.department_no         ,
        item_list.department_name       ,
        item_list.subclass_no           ,
        item_list.subclass_name         ,
        xx.store_lead_time              ,
        xx.num_units_per_tray           ,
        xx.roq_units                    ,
        xx.store_order1                 ,
        xx.store_order2                 ,
        xx.store_order3                 ,
        xx.direct_mu_qty1               ,
        xx.direct_mu_qty2               ,
        xx.direct_mu_qty3               ,
        xx.direct_mu_qty4               ,
        xx.direct_mu_qty5               ,
        xx.direct_mu_qty6               ,
        xx.direct_mu_qty7               ,
        xx.into_store_date              ,
        xx.st_ord_post_date
        
   FROM xx ,
        item_list,
        loc_list
  WHERE xx.item_no            = item_list.item_no
    AND xx.location_no        = loc_list.location_no
    and xx.wh_fd_zone_no      = loc_list.wh_fd_zone_no
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
       
end wh_prf_corp_259A;
