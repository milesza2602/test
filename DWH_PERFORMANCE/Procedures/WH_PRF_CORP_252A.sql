--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_252A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_252A" (p_forall_limit in integer, p_success out boolean) as 

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 1 (file 1 and 2 on BRS document)
--               FOR ALL DEPARTMENTS FOR CHRISTMAS TRADING COMPARISONS
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   FOODS_RENEWAL_EXTRACT1_ALL FOR CURRENT WEEK TO DATE
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
g_rec_out            W6005682.FOODS_RENEWAL_EXTRACT1_ALL%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_252A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 1';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.FOODS_RENEWAL_EXTRACT1_ALL%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.FOODS_RENEWAL_EXTRACT1_ALL%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;
l_start_date        date ;   --:= '24/JUN/13';
l_end_date          date ;   --:= '07/AUG/13';
l_ly_start_date     date;
l_ly_end_date       date;
l_less_days         integer;
l_today_no          integer;

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
    
    l_text := 'LOAD OF FOODS_RENEWAL_EXTRACT1_ALL started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table W6005682.FOODS_RENEWAL_EXTRACT1_ALL');  
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

 select today_date
   into l_today_date
   from dim_control;
     
   
 l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  select today_date, today_fin_day_no
   into l_today_date, l_today_no
   from dim_control;
   
 l_text := 'Today Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 l_text := 'Today day no : ' || l_today_no ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
 
 if l_today_no = 1 then l_less_days := 42; end if;
 if l_today_no = 2 then l_less_days := 43; end if;
 if l_today_no = 3 then l_less_days := 44; end if;
 if l_today_no = 4 then l_less_days := 45; end if;
 if l_today_no = 5 then l_less_days := 46; end if;
 if l_today_no = 6 then l_less_days := 48; end if;
 if l_today_no = 7 then l_less_days := 49; end if;
     
 select calendar_date
   into l_start_date
   from dim_calendar
  where calendar_date = trunc(sysdate) - l_less_days;

 l_text := 'Start date : ' || l_start_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);       
 l_text := 'End date : ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);       
 
 select ly_calendar_date
   into l_ly_start_date
   from dim_calendar
  where calendar_date = l_start_date;

   select ly_calendar_date
   into  l_ly_end_date
   from dim_calendar
  where calendar_date = l_today_date;
  
   
l_text := 'This year start date: ' || l_start_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
l_text := 'This year end date: ' || l_today_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
l_text := 'Last year start date: ' || l_ly_start_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
l_text := 'Last year end date: ' || l_ly_end_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
    
  
--l_today_date := 'Moo';
 
INSERT /*+ APPEND PARALLEL (mart,4) */ INTO W6005682.FOODS_RENEWAL_EXTRACT1_ALL mart
WITH item_list AS
  (  select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name 
            From Dim_item Di, Dim_department Dd
               where di.department_no in (13,41,44,47,55,58,68,73,75,81,85,88,96)   --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
                 and di.department_no = dd.department_no
  ) ,  --select * from item_list;
  
  loc_list AS
  (SELECT location_no,
    location_name    ,
    sk1_location_no  ,
    wh_fd_zone_no    ,
    SK1_FD_ZONE_GROUP_ZONE_NO,
    region_no
     FROM dim_location
    where chain_no = 10
  )  ,  -- select * from loc_list;
  
  supp_list AS
  (SELECT sk1_supplier_no, supplier_no, supplier_name FROM dim_supplier
  ),  -- select * from supp_list;
  
 aa as (
  select /*+ PARALLEL(c,4) FULL(c) */
         d.fin_year_no,
         d.fin_week_no,
         d.fin_day_no,
         b.wh_fd_zone_no, 
         c.post_date,
         a.item_no,
         b.location_no,
         nvl(c.SALES_DLY_APP_FCST,0) as sales_app_fcst,
         nvl(c.SALES_DLY_APP_FCST_QTY,0) as sales_app_fcst_qty
  FROM item_list a,
       loc_list b ,
       --xtl_loc_item_dy_rdf_fcst c,        --RDF L1/L2 remapping change
       RTL_LOC_ITEM_RDF_DYFCST_L2 c,
       dim_calendar d            
  WHERE c.post_date between  l_start_date and l_today_date       --'05/AUG/13' and '07/AUG/13'  --
   AND c.post_date        = d.calendar_date
   AND a.sk1_item_no      = c.sk1_item_no
   AND b.sk1_location_no  = c.sk1_location_no
   and (c.SALES_DLY_APP_FCST is not null or c.SALES_DLY_APP_FCST_QTY is not null)

 )  ,   --select * from aa; 
  
   bb AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no,
    d.fin_week_no,
    d.fin_day_no,
    b.wh_fd_zone_no,
    c.post_date,
    a.item_no,
    b.location_no,
    SUM(NVL(sales,0)) sales,
    SUM(NVL(sales_qty,0)) sales_qty
  FROM item_list a        ,
    loc_list b               ,
    rtl_loc_item_dy_rms_dense c,
    dim_calendar d           
    WHERE c.post_date between  l_start_date and l_today_date       --'05/AUG/13' and '07/AUG/13'  ----and '21/JUL/13'
  AND c.post_date     = d.calendar_date
  AND a.sk1_item_no       = c.sk1_item_no
  AND b.sk1_location_no   = c.sk1_location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, a.item_no, b.location_no
  ) ,  --   select * from bb;  --939644
  
  cc AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */ 
    d.fin_year_no    ,
    d.fin_week_no          ,
    d.fin_day_no fin_day_no, --d.fin_day_no,
    b.wh_fd_zone_no        ,
    c.post_date,
    a.item_no              ,
    b.location_no,
    nvl(sum(waste_cost),0) waste_cost,
    nvl(sum(waste_qty),0) waste_qty
    FROM item_list a  ,
    loc_list b         ,
    rtl_loc_item_dy_rms_sparse c,
    dim_calendar d     
    WHERE c.post_date between  l_start_date and l_today_date       --'05/AUG/13' and '07/AUG/13'  --= '29/JUL/13'
  AND c.post_date       = d.calendar_date
  AND a.sk1_item_no       = c.sk1_item_no
  AND b.sk1_location_no   = c.sk1_location_no
 GROUP BY d.fin_year_no,
    d.fin_week_no,
    d.fin_day_no,
    b.wh_fd_zone_no,
    c.post_date,
    a.item_no,
    b.location_no
  ) ,  --   select *  from cc;   -- where item_no = 265397;  4790
  
  dd AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                                         ,
    d.fin_week_no                                               ,
    d.fin_day_no                                                ,
    b.wh_fd_zone_no                                             ,
    c.calendar_date                                                 ,
    a.item_no                                                   ,
    b.location_no,
    nvl(c.fd_num_avail_days,0) fd_num_avail_days,
    nvl(c.fd_num_catlg_days,0) fd_num_catlg_days,
    nvl(c.fd_num_avail_days_adj,0) fd_num_avail_days_adj,
    nvl(c.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj,
    nvl(c.boh_adj_qty,0) boh_units_adj,
    nvl(c.boh_adj_cost,0) boh_adj_cost
     FROM item_list a  ,
    loc_list b         ,
    rtl_loc_item_dy_catalog c,
    dim_calendar d     
   WHERE c.calendar_date   between  l_start_date and l_today_date       --'05/AUG/13' and '07/AUG/13'  ----and '21/JUL/13'
     AND c.calendar_date       = d.calendar_date
     AND a.sk1_item_no     = c.sk1_item_no
     AND b.sk1_location_no = c.sk1_location_no
 --GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.calendar_date, a.item_no, b.location_no
  ),  --     select * from dd;   -- where item_no = 6001009011404;    --32864
  
 
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
where c.post_date between  l_start_date and l_today_date       --'05/AUG/13' and '07/AUG/13'  ----and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no 
and c.sk1_prom_no = dp.sk1_prom_no
and c.sk1_prom_period_no = 9749661
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no--, e.supplier_no
)  ,   --select * from prom;

  sales_ly as ( /*+ PARALLEL(a,4) FULL(a) */
    select c.fin_year_no, 
           c.fin_week_no, 
           c.fin_day_no, 
           loc_list.wh_fd_zone_no, 
           c.calendar_date post_date, 
           loc_list.location_no, 
           item_list.item_no, 
           a.sales sales_ly, 
           a.sales_qty sales_qty_ly
      from rtl_loc_item_dy_rms_dense a,  dim_calendar c, item_list, loc_list
      where c.ly_calendar_date between l_ly_start_date and l_ly_end_date   --l_start_date and l_today_date       --'25/JUN/12' and '08/AUG/12' 
        and a.post_date = c.ly_calendar_date
        and a.sk1_item_no = item_list.sk1_item_no
        and a.sk1_location_no = loc_list.sk1_location_no
   ),  -- select * from sales_ly WHERE item_no = 6005000846713 and location_no = 495;


fcst_ly as (
 select /*+ PARALLEL(c,4) FULL(c) */
         d.fin_year_no,
         d.fin_week_no,
         d.fin_day_no,
         b.wh_fd_zone_no, 
         d.calendar_date post_date,
         a.item_no,
         b.location_no,
         nvl(c.SALES_DLY_APP_FCST,0) as sales_app_fcst_ly,
         nvl(c.SALES_DLY_APP_FCST_QTY,0) as sales_app_fcst_qty_ly
  FROM item_list a,
       loc_list b ,
       --xtl_loc_item_dy_rdf_fcst c,            --RDF L1/L2 remapping change
       RTL_LOC_ITEM_RDF_DYFCST_L2 c,
       dim_calendar d            
  WHERE d.ly_calendar_date between  l_ly_start_date and l_ly_end_date   --l_start_date and l_today_date       --'25/JUN/12' and '08/AUG/12' 
   AND c.post_date        = d.ly_calendar_date
   AND a.sk1_item_no      = c.sk1_item_no
   AND b.sk1_location_no  = c.sk1_location_no
   and (c.SALES_DLY_APP_FCST is not null or c.SALES_DLY_APP_FCST_QTY is not null)
 ) ,   --  select * from fcst_ly WHERE item_no = 6005000846713 and location_no = 495; 

waste_ly as 
(SELECT /*+ PARALLEL(c,4) FULL(c) */ 
    d.fin_year_no    ,
    d.fin_week_no          ,
    d.fin_day_no fin_day_no, --d.fin_day_no,
    b.wh_fd_zone_no        ,
    d.calendar_date post_date,
    a.item_no              ,
    b.location_no,
    nvl(sum(waste_cost),0) waste_cost_ly,
    nvl(sum(waste_qty),0) waste_qty_ly
    FROM item_list a  ,
    loc_list b         ,
    rtl_loc_item_dy_rms_sparse c,
    dim_calendar d     
    WHERE d.ly_calendar_date between  l_ly_start_date and l_ly_end_date   --l_start_date and l_today_date       --'25/JUN/12' and '08/AUG/12'   --= '29/JUL/13'
  AND c.post_date       = d.ly_calendar_date
  AND a.sk1_item_no       = c.sk1_item_no
  AND b.sk1_location_no   = c.sk1_location_no
 GROUP BY d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, d.calendar_date, a.item_no, b.location_no
  ) ,   --   select *  from waste_ly;   -- where item_no = 265397;  4790

avail_ly as 
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                                         ,
    d.fin_week_no                                               ,
    d.fin_day_no                                                ,
    b.wh_fd_zone_no                                             ,
    d.calendar_date  calendar_date                                  ,
    a.item_no                                                   ,
    b.location_no,
    nvl(c.fd_num_avail_days,0) fd_num_avail_days_ly,
    nvl(c.fd_num_catlg_days,0) fd_num_catlg_days_ly,
    nvl(c.fd_num_avail_days_adj,0) fd_num_avail_days_adj_ly,
    nvl(c.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj_ly,
    nvl(c.boh_adj_qty,0) boh_units_adj_ly,
    nvl(c.boh_adj_cost,0) boh_adj_cost_ly
     FROM item_list a  ,
    loc_list b         ,
    rtl_loc_item_dy_catalog c,
    dim_calendar d     
   WHERE d.ly_calendar_date   between  l_ly_start_date and l_ly_end_date   --l_start_date and l_today_date       --'25/JUN/12' and '08/AUG/12'
     AND c.calendar_date   = d.ly_calendar_date
     AND a.sk1_item_no     = c.sk1_item_no
     AND b.sk1_location_no = c.sk1_location_no
 
  ) ,   --    select * from avail_ly;   -- where item_no = 6001009011404;    --32864

  xx AS (
   SELECT 
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_year_no, b.fin_year_no), c.fin_year_no), d.fin_year_no), sly.fin_year_no), fly.fin_year_no), wly.fin_year_no), aly.fin_year_no) fin_year_no,
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_week_no, b.fin_week_no), c.fin_week_no), d.fin_week_no), sly.fin_week_no), fly.fin_week_no), wly.fin_week_no), aly.fin_week_no) fin_week_no,
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_day_no, b.fin_day_no), c.fin_day_no), d.fin_day_no), sly.fin_day_no), fly.fin_day_no), wly.fin_day_no), aly.fin_day_no) fin_day_no,
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), sly.wh_fd_zone_no), fly.wh_fd_zone_no), wly.wh_fd_zone_no), aly.wh_fd_zone_no) wh_fd_zone_no,
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), sly.item_no), fly.item_no), wly.item_no), aly.item_no) item_no,
    --NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.location_no, b.location_no), c.location_no), d.location_no), sly.location_no), fly.location_no), wly.location_no), aly.location_no) location_no,    
    
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_year_no, b.fin_year_no), c.fin_year_no), d.fin_year_no), p.fin_year_no), sly.fin_year_no), fly.fin_year_no), wly.fin_year_no), aly.fin_year_no) fin_year_no,
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_week_no, b.fin_week_no), c.fin_week_no), d.fin_week_no), p.fin_week_no),sly.fin_week_no), fly.fin_week_no), wly.fin_week_no), aly.fin_week_no) fin_week_no,
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.fin_day_no, b.fin_day_no), c.fin_day_no), d.fin_day_no), p.fin_day_no),sly.fin_day_no), fly.fin_day_no), wly.fin_day_no), aly.fin_day_no) fin_day_no,
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), p.wh_fd_zone_no),sly.wh_fd_zone_no), fly.wh_fd_zone_no), wly.wh_fd_zone_no), aly.wh_fd_zone_no) wh_fd_zone_no,
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), p.item_no),sly.item_no), fly.item_no), wly.item_no), aly.item_no) item_no,
    NVL(NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.location_no, b.location_no), c.location_no), d.location_no), p.location_no),sly.location_no), fly.location_no), wly.location_no), aly.location_no) location_no,
    
    --NVL(e.po_no, 0)                         AS po_no,
    NVL(a.sales_app_fcst,0)                   AS sales_app_fcst,
    NVL(a.sales_app_fcst_qty,0)                   AS sales_app_fcst_qty,
                                                                       
    NVL(b.sales,'')                           AS sales,
    NVL(b.sales_qty,'')                       AS sales_qty,
                     
    NVL(c.waste_cost,0)                       AS waste_cost, 
    NVL(c.waste_qty,0)                        AS waste_qty,
    case when b.sales > 0 then 
       (NVL(c.waste_cost,0)/NVL(b.sales,0)) * 100 
    else 0 end                                AS waste_cost_perc,
    
    NVL(d.fd_num_avail_days,0)                AS fd_num_avail_days,
    nvl(d.fd_num_catlg_days,0)                AS fd_num_catlg_days,
    case when d.fd_num_catlg_days > 0 then
        (NVL(d.fd_num_avail_days,0) / d.fd_num_catlg_days) * 100
    else 0 end                                AS availability_perc,
    
    nvl(d.fd_num_avail_days_adj,0)            AS fd_num_avail_days_adj,
    nvl(d.fd_num_catlg_days_adj,0)            AS fd_num_catlg_days_adj,
    case when fd_num_catlg_days_adj > 0 then
        (nvl(d.fd_num_avail_days_adj,0)  / fd_num_catlg_days_adj) * 100
    else 0 end                                AS bus_avail_perc,
    nvl(d.boh_adj_cost,0)                     AS boh_cost_adj,
    nvl(d.boh_units_adj,0)                    AS boh_units_adj,
    
    nvl(p.prom_type,'')                       AS prom_type,
    
    --LY Measures
    NVL(fly.sales_app_fcst_ly,0)              AS sales_app_fcst_ly,
    NVL(fly.sales_app_fcst_qty_ly,0)          AS sales_app_fcst_qty_ly,
    NVL(sly.sales_ly,'')                      AS sales_ly,
    NVL(sly.sales_qty_ly,'')                  AS sales_qty_ly,
    NVL(wly.waste_cost_ly,0)                  AS waste_cost_ly, 
    NVL(wly.waste_qty_ly,0)                   AS waste_qty_ly,
    case when sly.sales_ly > 0 then 
       (NVL(wly.waste_cost_ly,0)/NVL(sly.sales_ly,0)) * 100 
    else 0 end                                AS waste_cost_perc_ly,
    NVL(aly.fd_num_avail_days_ly,0)           AS fd_num_avail_days_ly,
    nvl(aly.fd_num_catlg_days_ly,0)           AS fd_num_catlg_days_ly,
    case when aly.fd_num_catlg_days_ly > 0 then
        (NVL(aly.fd_num_avail_days_ly,0) / aly.fd_num_catlg_days_ly) * 100
    else 0 end                                AS availability_perc_ly,
    nvl(aly.fd_num_avail_days_adj_ly,0)       AS fd_num_avail_days_adj_ly,
    nvl(aly.fd_num_catlg_days_adj_ly,0)       AS fd_num_catlg_days_adj_ly,
    
    case when fd_num_catlg_days_adj_ly > 0 then
        (nvl(aly.fd_num_avail_days_adj_ly,0)  / aly.fd_num_catlg_days_adj_ly) * 100
    else 0 end                                AS bus_avail_perc_ly,
    nvl(aly.boh_units_adj_ly,0)               AS boh_units_adj_ly,
    nvl(aly.boh_adj_cost_ly,0)                AS boh_cost_adj_ly
    
    
  FROM aa a
  FULL OUTER JOIN bb b
       ON a.item_no       = b.item_no
      AND a.wh_fd_zone_no = b.wh_fd_zone_no
      and a.location_no   = b.location_no
      and a.post_date     = b.post_date
      
  FULL OUTER JOIN cc c
       ON NVL(a.item_no, b.item_no)             = c.item_no
      AND NVL(a.wh_fd_zone_no, b.wh_fd_zone_no) = c.wh_fd_zone_no
      AND nvl(a.post_date, b.post_date)         = c.post_date
      and nvl(a.location_no, b.location_no)     = c.location_no
  
  FULL OUTER JOIN dd d
       ON NVL(NVL(a.item_no, b.item_no), c.item_no)                   = d.item_no
      AND NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no) = d.wh_fd_zone_no
      AND NVL(NVL(a.post_date, b.post_date), c.post_date)             = d.calendar_date
      and NVL(NVL(a.location_no, b.location_no), c.location_no)       = d.location_no

  FULL OUTER JOIN prom p
       ON NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no)                         = p.item_no
      AND NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no) = p.wh_fd_zone_no
      AND NVL(NVL(NVL(a.post_date, b.post_date), c.post_date), d.calendar_date)             = p.post_date
      AND NVL(NVL(NVL(a.location_no,  b.location_no), c.location_no), d.location_no)        = p.location_no
      
   FULL OUTER JOIN sales_ly sly
       ON NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), p.item_no)                                = sly.item_no
      AND NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), p.wh_fd_zone_no)  = sly.wh_fd_zone_no
      AND NVL(NVL(NVL(NVL(a.post_date, b.post_date), c.post_date), d.calendar_date), p.post_date)                  = sly.post_date
      AND NVL(NVL(NVL(NVL(a.location_no,  b.location_no), c.location_no), d.location_no),  p.location_no)          = sly.location_no

   FULL OUTER JOIN fcst_ly fly
       ON NVL(NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), p.item_no), sly.item_no)                                     = fly.item_no
      AND NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), p.wh_fd_zone_no), sly.wh_fd_zone_no) = fly.wh_fd_zone_no
      AND NVL(NVL(NVL(NVL(NVL(a.post_date, b.post_date), c.post_date), d.calendar_date), p.post_date), sly.post_date)                     = fly.post_date
      AND NVL(NVL(NVL(NVL(NVL(a.location_no,  b.location_no), c.location_no), d.location_no),  p.location_no), sly.location_no)           = fly.location_no

   FULL OUTER JOIN waste_ly wly
       ON NVL(NVL(NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), p.item_no), sly.item_no), fly.item_no)                                           = wly.item_no
      AND NVL(NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), p.wh_fd_zone_no), sly.wh_fd_zone_no), fly.wh_fd_zone_no) = wly.wh_fd_zone_no
      AND NVL(NVL(NVL(NVL(NVL(NVL(a.post_date, b.post_date), c.post_date), d.calendar_date), p.post_date), sly.post_date), fly.post_date)                         = wly.post_date
      AND NVL(NVL(NVL(NVL(NVL(NVL(a.location_no,  b.location_no), c.location_no), d.location_no),  p.location_no), sly.location_no), fly.location_no)             = wly.location_no

   FULL OUTER JOIN avail_ly aly
       ON NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.item_no, b.item_no), c.item_no), d.item_no), p.item_no), sly.item_no), fly.item_no), wly.item_no)                                                 = aly.item_no
      AND NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), p.wh_fd_zone_no), sly.wh_fd_zone_no), fly.wh_fd_zone_no), wly.wh_fd_zone_no) = aly.wh_fd_zone_no
      AND NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.post_date, b.post_date), c.post_date), d.calendar_date), p.post_date), sly.post_date), fly.post_date), wly.post_date)                             = aly.calendar_date
      AND NVL(NVL(NVL(NVL(NVL(NVL(NVL(a.location_no,  b.location_no), c.location_no), d.location_no),  p.location_no), sly.location_no), fly.location_no), wly.location_no)               = aly.location_no      
  
  )  
  
  SELECT xx.fin_year_no                  ,        
        xx.fin_week_no                  ,
        xx.fin_day_no                   ,
        loc_list.region_no,
        xx.wh_fd_zone_no as dc_region   ,
        xx.item_no                      ,
        item_list.item_desc             ,
        xx.location_no                  ,
        loc_list.location_name,
        item_list.department_no         ,
        item_list.department_name       ,
        item_list.subclass_no           ,
        item_list.subclass_name         ,
        xx.sales,            
        xx.sales_qty,
        xx.sales_app_fcst,
        xx.sales_app_fcst_qty,
        xx.waste_cost,
        xx.waste_qty,
        xx.waste_cost_perc,
        xx.fd_num_avail_days,
        xx.fd_num_catlg_days,
        xx.availability_perc,
        xx.fd_num_avail_days_adj,
        xx.fd_num_catlg_days_adj,
        xx.bus_avail_perc,
        xx.boh_cost_adj,
        xx.boh_units_adj,
        xx.prom_type,
        xx.sales_app_fcst_ly,
        xx.sales_app_fcst_qty_ly,
        xx.sales_ly,
        xx.sales_qty_ly,
        xx.waste_cost_ly,
        xx.waste_qty_ly,
        xx.waste_cost_perc_ly,
        xx.fd_num_avail_days_ly,
        xx.fd_num_catlg_days_ly,
        xx.availability_perc_ly,
        xx.fd_num_avail_days_adj_ly,
        xx.fd_num_catlg_days_adj_ly,
        xx.bus_avail_perc_ly,
        xx.boh_units_adj_ly,
        xx.boh_cost_adj_ly
   FROM xx ,
        item_list,
        loc_list
  WHERE xx.item_no            = item_list.item_no
    and xx.location_no        = loc_list.location_no
    
  order by xx.item_no;

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
       
end wh_prf_corp_252A;
