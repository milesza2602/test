--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_227U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_227U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2012
--  Author:      Quentin Smit
--  Purpose:     Extract RDF and Depot values on a Sunday for the Monday morning run of
--               of the weekly foods mart snapshot (mart_foods_weekly_snapshot)
--  Tables:      Input  - rtl_loc_item_wk_rdf_sale
--                        rtl_loc_item_wk_rdf_fcst
--                        rtl_depot_item_dy
--                        rtl_depot_item_wk
--               Output - tmp_mart_rdf_depot
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  17 oct 2013 -  hint changed from /* +  to /*+
--  09 Nov 2016 - A Joshua - Level1 rename (chg-2218)
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
g_recs_deleted       integer       :=  0;

g_date               date          := trunc(sysdate);
g_last_week          date          := trunc(sysdate) - 7;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_227U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Table for RDF and Depot measures for weekly snapshot';
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
--l_last_yr_wk_start_date  date;
--l_last_yr_wk_end_date    date;
--l_day7_last_yr_wk_date date;
--l_date_day1_last_yr   date;
l_max_6wk_last_year   number;
l_6wks_string         char(200);
l_start_8wks_date     date;
l_day_no              number;
l_less_days           number;
l_this_wk_start_date  date;
l_this_wk_end_date    date;

--***************************************
-- New Variable to cater for Week 53
l_53_ly_fin_year_no       NUMBER;
l_53_ly_fin_week_no       NUMBER;
l_53_ly_day1_date         date;
l_53_ly_day7_date         date;

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

    l_text := 'Create RDF/Depot data table for Foods weekly snapshot STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table dwh_performance.tmp_mart_rdf_depot');
    l_text := 'Truncate Temp RDF / Depot measures table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'Week being processed:- '||g_last_week ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--***************************************************
-- set up some variables to be used in the program
--***************************************************
     select  today_fin_year_no,       -- current year
        today_fin_year_no -1 ,        -- last year
        last_wk_fin_week_no,          -- last_completed_week
        today_fin_week_no,            -- current_week
        next_wk_fin_week_no,          -- next_week
        next_wk_fin_year_no,          -- next week's fin year
        last_wk_fin_year_no,           -- last week's fin year
        today_fin_day_no,
        this_wk_start_date,            -- start date of current week
        this_wk_end_date               -- last day of current week ; day 7 of current week
     into l_fin_year_no,
             l_last_fin_year,
             l_last_week_no,
             l_fin_week_no,
             l_next_wk_fin_week_no,
             l_next_wk_fin_year_no,
             l_last_wk_fin_year_no,
             l_day_no,
             l_this_wk_start_date,
             l_this_wk_end_date
FROM dim_control_report;

--*********************************************************************
-- Get the week, year and date of corresponding last year values
-- catering for any week 53 scenario comparison.
-- Put these in new variable so no change to existing code required
--*********************************************************************

SELECT ly_fin_year_no,ly_fin_week_no
INTO   l_53_ly_fin_year_no,l_53_ly_fin_week_no
FROM   dim_calendar
WHERE  calendar_date = g_date;


SELECT MIN(calendar_date) start_date,
       MAX(calendar_date) end_date
INTO   l_53_ly_day1_date , l_53_ly_day7_date
FROM   dim_calendar
WHERE  fin_year_no = l_53_ly_fin_year_no
  AND  fin_week_no = l_53_ly_fin_week_no
GROUP BY fin_year_no, fin_week_no;

--*********************************************************************
-- Defunct since wk53 code
/*
--Get date of day 1 of corresponding current week last year
if l_fin_week_no = 1 then
   select calendar_date
    into l_date_day1_last_yr
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no
      and fin_week_no = l_fin_week_no
      and fin_day_no = 1;
else
   select calendar_date
    into l_date_day1_last_yr
     from dim_calendar
    where fin_year_no = l_last_fin_year
      and fin_week_no = l_fin_week_no
      and fin_day_no = 1;
end if;
*/

---Start and end of 6wk period for 6 week average calculations
--with six_wks as (
--select unique fin_week_no
--from dim_calendar
--where fin_year_no = l_fin_year_no
--  and fin_week_no > l_last_week_no - 6
--  and fin_week_no  < l_fin_week_no
--order by fin_week_no
--)
--select min(fin_week_no) start_6wks, max(fin_week_no) end_6wks
--into l_start_6wks, l_end_6wks
--from six_wks;

--need to check if 6wk period spans more than on fin year
--if l_end_6wks < 6 then
--   select max(fin_week_no)
 --    into l_max_6wk_last_year
 --    from dim_calendar_wk
 --   where fin_year_no = l_last_fin_year;

 --  select calendar_date end_date
 --    into l_end_6wks_date
 --    from dim_calendar
 --   where fin_year_no = l_last_fin_year
  --    and fin_week_no = l_end_6wks
  --    and fin_day_no = 7;

 --  l_6wks_string := '((fin_year_no=l_last_fin_year and fin_week_no between l_start_6wks and l_max_6wk_last_year) or (fin_year_no = l_fin_year_no and fin_week_no <= l_end_6wk))';
--else
--   select calendar_date end_date
--     into l_end_6wks_date
--     from dim_calendar
 --   where fin_year_no = l_fin_year_no
 --     and fin_week_no = l_end_6wks
 --     and fin_day_no = 7;

--    l_6wks_string := '( fin_year_no = l_fin_year_no and fin_week between l_start_6wks and l_end_6wk)';
--end if
--;

--Calendar dates for last 6 weeks
--select unique calendar_date start_date
--into l_start_6wks_date
--from dim_calendar
--where fin_year_no = l_fin_year_no
--and fin_week_no = l_start_6wks
--and fin_day_no = 1;

-- Get start date of 8 weeks ago ; program should only run on a Monday or Tuesday but cater for the whole week
-- To go back 8 weeks subtract the amount of days depending on which day the program is run on
if l_day_no = 1 then l_less_days := '49'; end if;  --57
if l_day_no = 2 then l_less_days := '50'; end if;
if l_day_no = 3 then l_less_days := '51'; end if;
if l_day_no = 4 then l_less_days := '52'; end if;
if l_day_no = 5 then l_less_days := '53'; end if;
if l_day_no = 6 then l_less_days := '54'; end if;
if l_day_no = 7 then l_less_days := '55'; end if;  --63

select calendar_date
  into l_start_8wks_date
  from dim_calendar
 where calendar_date = trunc(sysdate) - l_less_days;
   --and fin_day_no = 1;

--with week_list as (
--select fin_year_no,
--       fin_week_no,
 --      this_week_start_date,
 --      this_week_end_date
--from   dim_calendar_wk
--where fin_year_no   = l_fin_year_no
--  and fin_week_no  < l_fin_week_no
--)
--select min(this_week_start_date) start_date,
--       max(this_week_end_date) end_date
--into l_ytd_start_date, l_ytd_end_date
--from week_list;

-- Get start and end dates of CURRENT week
--select min(calendar_date) start_date,
--       max(calendar_date) end_date
--into l_last_wk_start_date, l_last_wk_end_date
--from dim_calendar
--where fin_year_no = l_last_wk_fin_year_no
--  and fin_week_no = l_last_week_no
--  group by fin_year_no, fin_week_no;

/*
-- Get start and end dates of corresponding last completed week last year
Defunct with wk53 code
select min(calendar_date) start_date,
       max(calendar_date) end_date
into l_last_yr_wk_start_date, l_last_yr_wk_end_date
from dim_calendar
where fin_year_no = l_last_fin_year
  and fin_week_no = l_last_week_no
  group by fin_year_no, fin_week_no;

--day7_last_yr_wk
select calendar_date
into l_day7_last_yr_wk_date
from dim_calendar
where fin_year_no = l_last_fin_year
and fin_week_no = l_last_week_no
and fin_day_no = 7;

*/

l_text := 'current date = '|| g_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current year = '|| l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last year = '|| l_last_fin_year;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last completed week = '|| l_last_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current week = '|| l_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'next week = '|| l_next_wk_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'next year = '|| l_next_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start 8wks date = '|| l_start_8wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_day_no = '|| l_day_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_less_days = '|| l_less_days;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 8wks date = '|| l_this_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'last week start date = '|| l_last_wk_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'last week end date = '|| l_last_wk_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'day7 last week last year date = '|| l_day7_last_yr_wk_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'this week start date  = '|| l_this_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'this week end date  = '|| l_this_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'day 1 of current week last year  = '|| l_date_day1_last_yr;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := '53 Week LY Year =  = '||l_53_ly_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY week  = '|| l_53_ly_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY Day 1  = '||l_53_ly_day1_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY Day7  = '|| l_53_ly_day7_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --l_53_ly_fin_year_no := 'MOO';
----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------




-------------------------------------------------------------

INSERT /*+ APPEND */ INTO dwh_performance.tmp_mart_rdf_depot mart
with item_list as (
select sk1_item_no,
       item_no,
       item_no || ' ' || item_desc,
       fd_product_no,
       dd.department_no,
       dd.department_no || ' ' || dd.department_name,
       subclass_no,
       subclass_no || ' ' || subclass_name,
       dd.sk1_department_no,
       tran_ind
from   dim_item di,
       dim_department dd
where  di.business_unit_no = 50
   and di.sk1_department_no = dd.sk1_department_no
),

store_list as (
select sk1_location_no,
       location_no,
       location_no || ' ' || location_name location_no_and_name,
       region_no,
       region_no || ' ' || region_name region_no_and_name,
       area_no,
       area_no || ' ' || area_name area_no_and_name,
       wh_fd_zone_no,
       chain_no,
       loc_type
from   dim_location
where  chain_no = 10
),

store_list_area as (
select sk1_location_no,
       location_no,
       location_no || ' ' || location_name location_no_and_name,
       region_no,
       region_no || ' ' || region_name region_no_and_name,
       area_no,
       area_no || ' ' || area_name area_no_and_name,
       wh_fd_zone_no,
       chain_no,
       loc_type
from   dim_location
where  area_no = 9951
),

--list of year to date weeks for last year
--week_list as (
--select dcw.fin_year_no,
--       dcw.fin_week_no,
--       this_week_start_date,
--       this_week_end_date
--from   dim_calendar_wk dcw
--where dcw.fin_year_no =  l_last_wk_fin_year_no
--  and dcw.fin_week_no  < l_fin_week_no
--),

--list of weeks in last year (Not used in code below - ADW 06/13)
last_year_weeks as (
select dcw.fin_week_no
from   dim_calendar_wk dcw
where dcw.fin_year_no =  l_last_fin_year
),

--Date of last day of last 8 weeks
eight_weeks_end_date as (
select calendar_date
  from dim_calendar
  where calendar_date between l_start_8wks_date and l_this_wk_start_date
   and fin_day_no = 1
  order by calendar_date
),

-- Get RDF weekly forecast measures from last completed week    YES  DONE
rdf_fcst_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.sales_dly_app_fcst,0)) sales_wk_app_fcst,
       sum(nvl(f.sales_dly_app_fcst_qty,0)) sales_wk_app_fcst_qty
--from rtl_loc_item_dy_rdf_fcst f, item_list il, store_list sl      --RDF L1/L2 remapping chg-2218
from RTL_LOC_ITEM_RDF_DYFCST_L2 f, item_list il, store_list sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.post_date between l_this_wk_start_date and l_this_wk_end_date  -- CURRENT week !!
and (f.sales_dly_app_fcst is not null
or  f.sales_dly_app_fcst_qty is not null)
group by f.sk1_location_no, f.sk1_item_no
),

rdf_fcst_measures1 as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.SALES_DLY_APP_FCST_QTY_FLT_AV,0)) SALES_WK_APP_FCST_QTY_FLT_AV
--from rtl_loc_item_dy_rdf_fcst f, item_list il, store_list_area sl     --RDF L1/L2 remapping chg_2218
from RTL_LOC_ITEM_RDF_DYFCST_L2 f, item_list il, store_list_area sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
--and f.post_date between l_this_wk_start_date and l_this_wk_end_date -- CURRENT week !!
and f.post_date = l_this_wk_end_date
and f.SALES_DLY_APP_FCST_QTY_FLT_AV is not null
group by f.sk1_location_no, f.sk1_item_no
),

--Available Days, Catalogue Days and DC availability for last completed week
catalog_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.this_wk_catalog_ind
from   rtl_loc_item_wk_catalog f, item_list il, dim_item_uda iu, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_item_no     = iu.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_fin_year_no        -- fin year for CURRENT week
and f.fin_week_no    = l_fin_week_no        --CURRENT week !!
and (f.fd_num_avail_days is not null or f.fd_num_catlg_days is not null)
),

--Corrected sales for last completed week  - YES
--corrected_sales as (
--select /*+ PARALLEL(f,4) FULL(f) */
--       f.sk1_location_no,
--       f.sk1_item_no,
--       sum(nvl(f.corr_sales,0)) corrected_sales
--from RTL_LOC_ITEM_DY_RDF_SALE f, item_list il, store_list sl
--where f.sk1_item_no   = il.sk1_item_no
--and f.sk1_location_no = sl.sk1_location_no
--and f.post_date between l_this_wk_start_date and l_this_wk_end_date  -- CURRENT week !!
--and f.corr_sales is not null
--group by f.sk1_location_no,   f.sk1_item_no
--) ,


--Expired stock for the last completed week   - YES  DONE
expired_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(shelf_life_expird) expired_stock,
       sum(SHELF_LIFE_01_07 + SHELF_LIFE_08_14 + SHELF_LIFE_15_21) expiring_stock_wk1_3
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_this_wk_end_date  --'09/APR/12' and '15/APR/12'
  and (f.shelf_life_expird is not null
        or f.shelf_life_01_07 is not null
        or f.shelf_life_08_14 is not null
        or f.shelf_life_15_21 is not null)
 group by f.sk1_location_no, f.sk1_item_no
),

-- Depot values from day 7 of corresponding current week from last year  YES  DONE
depot_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days_ly,
       sum(nvl(f.fd_num_dc_avail_days,0)) fd_num_dc_avail_days_ly,
       sum(nvl(f.fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days_ly,
       sum(nvl(f.fd_num_dc_catlg_days,0)) fd_num_dc_catlg_days_ly
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  AND F.SK1_ITEM_NO = IL.SK1_ITEM_NO
--  and f.post_date = l_date_day1_last_yr
  and f.post_date = l_53_ly_day1_date         -- Cater for week 53 --
  and (f.fd_num_dc_avail_adj_days is not null
    or f.fd_num_dc_avail_days is not null
    or f.fd_num_dc_catlg_adj_days is not null
    or f.fd_num_dc_catlg_days is not null)
 group by f.sk1_location_no, f.sk1_item_no
),


-- Get depot day measures for day 7 of last completed week   YES  DONE
dc_no_stock_day1 as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.stock_cases) stock_cases,
       sum(f.ll_dc_out_of_stock) DC_OUT_OF_STOCK,
       sum(f.stock_dc_cover_cases) stock_dc_cover_cases,
       sum(nvl(f.fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days,
       sum(nvl(f.fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days,
       sum(nvl(f.fd_num_dc_avail_days,0)) fd_num_dc_avail_days,
       sum(nvl(f.fd_num_dc_catlg_days,0)) FD_NUM_DC_CATLG_DAYS
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_this_wk_end_date  --'30/APR/12'
  and (f.ll_dc_out_of_stock is not null
    or f.stock_dc_cover_cases is not null
    or f.fd_num_dc_avail_adj_days is not null
    or f.fd_num_dc_catlg_adj_days is not null
    or f.fd_num_dc_catlg_days is not null
    or f.stock_cases is not null
    or f.fd_num_dc_avail_days is not null)
group by f.sk1_location_no, f.sk1_item_no
),

-- Get cover cases for current week  YES DONE
cover_cases as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.est_ll_dc_cover_cases_av est_ll_short_cases_av
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_this_wk_end_date
  and il.tran_ind = 1
),

-- Get DC no stock values for last 8 weeks   YES
weeks_no_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.ll_dc_out_of_stock) weeks_out_of_stock
from rtl_depot_item_dy f, item_list il, store_list sl, eight_weeks_end_date ew
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = ew.calendar_date
  and f.ll_dc_out_of_stock is not null
group by f.sk1_location_no, f.sk1_item_no
),

all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,2) PARALLEL(f12,2) */

nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                           es.sk1_location_no),
                                           cc.sk1_location_no),
                                           ws.sk1_location_no),
                                           dl.sk1_location_no),
                                           f9.sk1_location_no),
                                           f10.sk1_location_no)

sk1_location_no,
nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                           es.sk1_item_no),
                                           cc.sk1_item_no),
                                           ws.sk1_item_no),
                                           dl.sk1_item_no),
                                           f9.sk1_item_no),
                                           f10.sk1_item_no)
sk1_item_no,
f0.this_wk_catalog_ind catalog_ind,
--cs.corrected_sales,
f1.sales_wk_app_fcst,
f1.sales_wk_app_fcst_qty,
f10.sales_wk_app_fcst_qty_flt_av,
f9.fd_num_dc_avail_adj_days,
f9.fd_num_dc_avail_days,
f9.fd_num_dc_catlg_adj_days,
f9.fd_num_dc_catlg_days,
dl.fd_num_dc_avail_adj_days_ly,
dl.fd_num_dc_avail_days_ly,
dl.fd_num_dc_catlg_adj_days_ly,
dl.fd_num_dc_catlg_days_ly,
f9.stock_cases,
es.expired_stock,
es.expiring_stock_wk1_3,
cc.est_ll_short_cases_av,
f9.stock_dc_cover_cases,
f9.DC_OUT_OF_STOCK,
ws.weeks_out_of_stock

from catalog_measures f0
full outer join rdf_fcst_measures  f1 on f0.sk1_location_no = f1.sk1_location_no
                                     and f0.sk1_item_no     = f1.sk1_item_no

full outer join expired_stock es                    on nvl(f0.sk1_location_no, f1.sk1_location_no) = es.sk1_location_no
                                                       and nvl(f0.sk1_item_no, f1.sk1_item_no)= es.sk1_item_no

full outer join cover_cases cc                  on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                   es.sk1_location_no)= cc.sk1_location_no
                                                   and nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                                   es.sk1_item_no)  =cc.sk1_item_no

full outer join weeks_no_stock ws           on nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                   es.sk1_location_no),
                                                                                   cc.sk1_location_no) = ws.sk1_location_no
                                               and nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                                   es.sk1_item_no),
                                                                                   cc.sk1_item_no) = ws.sk1_item_no

full outer join depot_ly dl             on nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                   es.sk1_location_no),
                                                                                   cc.sk1_location_no),
                                                                                   ws.sk1_location_no) = dl.sk1_location_no
                                           and nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                                   es.sk1_item_no),
                                                                                   cc.sk1_item_no),
                                                                                   ws.sk1_item_no) = dl.sk1_item_no

full outer join dc_no_stock_day1 f9 on nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                   es.sk1_location_no),
                                                                                   cc.sk1_location_no),
                                                                                   ws.sk1_location_no),
                                                                                   dl.sk1_location_no) = f9.sk1_location_no
                                       and nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                                   es.sk1_item_no),
                                                                                   cc.sk1_item_no),
                                                                                   ws.sk1_item_no),
                                                                                   dl.sk1_item_no)     = f9.sk1_item_no

full outer join rdf_fcst_measures1 f10 on nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                   es.sk1_location_no),
                                                                                   cc.sk1_location_no),
                                                                                   ws.sk1_location_no),
                                                                                   dl.sk1_location_no),
                                                                                   f9.sk1_location_no) = f10.sk1_location_no
                                       and nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                                   es.sk1_item_no),
                                                                                   cc.sk1_item_no),
                                                                                   ws.sk1_item_no),
                                                                                   dl.sk1_item_no),
                                                                                   f9.sk1_item_no) = f10.sk1_item_no

)

select /*+ PARALLEL(atg,4) */
atg.sk1_location_no,
atg.sk1_item_no,
atg.catalog_ind,
atg.sales_wk_app_fcst,
atg.sales_wk_app_fcst_qty,
atg.sales_wk_app_fcst_qty_flt_av,

atg.fd_num_dc_avail_adj_days,
atg.fd_num_dc_avail_days,
atg.fd_num_dc_catlg_adj_days,
atg.fd_num_dc_catlg_days,
atg.fd_num_dc_avail_adj_days_ly,
atg.fd_num_dc_avail_days_ly,
atg.fd_num_dc_catlg_adj_days_ly,
atg.fd_num_dc_catlg_days_ly,

atg.stock_cases,
atg.expired_stock,
atg.expiring_stock_wk1_3,
atg.est_ll_short_cases_av,
atg.stock_dc_cover_cases,
atg.DC_OUT_OF_STOCK,
atg.weeks_out_of_stock,
g_date

from all_together atg,
     store_list sl,
     item_list il

where atg.sk1_location_no = sl.sk1_location_no
  and atg.sk1_item_no     = il.sk1_item_no


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

END wh_prf_corp_227u;
