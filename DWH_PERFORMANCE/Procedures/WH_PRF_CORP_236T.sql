--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_236T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_236T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        October 2012
--  Author:      Quentin Smit
--  Purpose:     Create Food Location Item Week Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               For Foods weekly snapshot 1.2 for CURRENT WEEK TO DATE
--               On a Monday this program will load data for the last completed week as the program
--               that loads data for the last completed week will have to load data for 2 weeks back (wh_prf_corp_235u)
--  Tables:      Input  - rtl_loc_item_wk_rms_dense ..
--               Output - TMP_MART_FD_LOC_DEPT_WK_T
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_236T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Foods Loc Item Week Dept TEST Mart - CWD';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

l_fin_year_no           number;
l_fin_week_no           number;
l_last_fin_year         number;
l_last_week_no          number;
l_next_wk_fin_week_no   number;
l_next_wk_fin_year_no   number;
l_last_wk_fin_year_no   number;
l_start_6wks            number;
l_end_6wks              number;
l_start_6wks_date       date;
l_end_6wks_date         date;
l_ytd_start_date        date;
l_ytd_end_date          date;
l_last_wk_start_date    date;
l_last_wk_end_date      date;
l_last_yr_wk_start_date date;
l_last_yr_wk_end_date   date;
l_day7_last_yr_wk_date  date;
l_max_6wk_last_year     number;
l_6wks_string           char(200);
l_start_8wks_date       date;
l_day_no                number;
l_less_days             number;
l_this_wk_start_date    date;
l_last_week_start_date  date;
l_last_week_end_date    date;
l_max_fin_week_last_year number;
l_cra_start_date        date;
l_cra_end_date          date;
l_today_date_last_year  date;
l_ly_current_date       date;
l_this_wk_end_date      date;
l_fin_week_code       varchar2(7 byte);

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

    l_text := 'Create Foods Loc Item Week Depot Mart for LAST COMPLETED WEEK STARTED AT '||
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

--***************************************************
-- set up some variables to be used in the program
--***************************************************
     select  today_fin_year_no,         -- current year
        last_wk_fin_year_no -1 ,        -- last year
        last_wk_fin_week_no,            -- last_completed_week
        today_fin_week_no,              -- current_week
        next_wk_fin_week_no,            -- next_week
        next_wk_fin_year_no,            -- next week's fin year
        last_wk_fin_year_no,            -- last week's fin year
        today_fin_day_no,
        this_wk_start_date,             -- start date of current week
        last_wk_fin_week_no,             -- end date of 6 week period
        last_wk_start_date,
        last_wk_end_date
        into l_fin_year_no,
             l_last_fin_year,
             l_last_week_no,
             l_fin_week_no,
             l_next_wk_fin_week_no,
             l_next_wk_fin_year_no,
             l_last_wk_fin_year_no,
             l_day_no,
             l_this_wk_start_date,
             l_end_6wks,
             l_last_week_start_date,
             l_last_week_end_date
from dim_control_report;

l_day_no := 1;

select max(fin_week_no)
  into l_max_fin_week_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year;

-- Must cater for when this program runs on a Monday as then it must get data for last completed week
if l_day_no = 1 then
   if l_fin_week_no = 1 then
      l_fin_week_no := l_max_fin_week_last_year;            -- First day of new year, get last week of last fin year
      l_last_wk_fin_year_no := l_last_wk_fin_year_no - 1;   -- First day of new year, get fin year - 1
      l_fin_year_no := l_last_fin_year;
      l_last_fin_year := l_last_fin_year - 2;               -- first day of new year, previous fin year will be - 2
   else
      l_fin_week_no := l_last_week_no;
      --l_last_week_no := l_last_week_no -1;
   end if;
   l_cra_start_date := l_last_week_start_date;
   l_cra_end_date   := l_last_week_end_date;

else
   l_cra_start_date := l_this_wk_start_date;
   l_cra_end_date   := l_this_wk_end_date;

end if;

l_text := 'Week being processed:- '||l_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--Get date of corresponding current week last year
select calendar_date
  into l_today_date_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year
   and fin_week_no = l_fin_week_no
   and fin_day_no = l_day_no;

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

with week_list as (
select fin_year_no,
       fin_week_no,
       this_week_start_date,
       this_week_end_date
from   dim_calendar_wk
where fin_year_no   = l_last_wk_fin_year_no   --l_fin_year_no
  and fin_week_no  <= l_fin_week_no
)
select min(this_week_start_date) start_date,
       max(this_week_end_date) end_date
into l_ytd_start_date, l_ytd_end_date
from week_list;

-- Get start and end dates of last completed week
select min(calendar_date) start_date,
       max(calendar_date) end_date,
       max(fin_week_code) fin_week_code
into l_last_wk_start_date, l_last_wk_end_date, l_fin_week_code
from dim_calendar
where fin_year_no = l_last_wk_fin_year_no
  and fin_week_no = l_last_week_no 
  ;

-- Get start and end dates of corresponding last completed week last year
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

if l_day_no = 1 then
   l_ly_current_date := l_last_yr_wk_end_date;
else
    l_ly_current_date :=l_today_date_last_year;
end if;

l_text := 'current date = '|| g_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current year = '|| l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last year = '|| l_last_fin_year;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last completed week = '|| l_last_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

if l_day_no = 1 then
   l_text := 'current week = '|| (l_fin_week_no + 1);
 else
   l_text := 'current week = '|| l_fin_week_no;
end if;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'next week = '|| l_next_wk_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'next week year = '|| l_next_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'last completed weeks year = '|| l_last_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'start 6wks : year = '|| l_6wks_wk6_yr || ' week = ' ||  l_6wks_wk6;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'end 6wks : year = '|| l_6wks_wk1_yr || ' week = ' || l_6wks_wk1;    --l_end_6wks;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'start 6wks date = '|| l_start_6wks_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'start 8wks date = '|| l_start_8wks_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_day_no = '|| l_day_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_less_days = '|| l_less_days;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 6wks date = '|| l_end_6wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'YTD start date = '|| l_ytd_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'YTD end date = '|| l_ytd_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start date of week being processed = '|| l_last_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end date of week being processed  = '|| l_last_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'day7 last week last year date = '|| l_day7_last_yr_wk_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'this week start date  = '|| l_this_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start date of last completed week last year  = '|| l_last_yr_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end date of last completed week last year  = '|| l_last_yr_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

INSERT /*+ APPEND */ INTO w6005682.TMP_MART_FD_LOC_DEPT_WK_T mart
with item_list as (
select di.sk1_item_no,
       item_no,
       item_no || ' ' || item_desc,
       fd_product_no,
       dd.department_no,
       dd.department_no || ' ' || dd.department_name,
       subclass_no,
       subclass_no || ' ' || subclass_name,
       dd.sk1_department_no,
       dd.department_name,
       tran_ind,
       di.group_no,
       di.group_name,
       di.subgroup_no,
       di.subgroup_name
from   dim_item di,
       dim_department dd
where  di.business_unit_no = 50
and    di.sk1_department_no = dd.sk1_department_no
),

store_list as (
select sk1_location_no,
       location_no,
       location_no || ' ' || location_name location_no_and_name,
       location_name,
       region_no,
       region_no || ' ' || region_name region_no_and_name,
       region_name,
       area_no,
       area_no || ' ' || area_name area_no_and_name,
       area_name,
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

department_info as (
select unique dd.sk1_department_no,
       dd.department_no,
       dd.department_name
 from  dim_department dd, 
       item_list il
 where dd.sk1_department_no = il.sk1_department_no
 ),

--list of year to date weeks for last year
week_list as (
select dcw .fin_year_no,
       dcw.fin_week_no,
       this_week_start_date,
       this_week_end_date
from   dim_calendar_wk dcw
where dcw.fin_year_no =  l_fin_year_no
  and dcw.fin_week_no  <= l_fin_week_no
),

--list of weeks in last year
last_year_weeks as (
select dcw.fin_week_no
from   dim_calendar_wk dcw
where dcw.fin_year_no =  l_last_fin_year
),

--Driver for department list
catalog_measures as (
select /*+ PARALLEL(f,4)  */ unique
       f.sk1_location_no,
       il.sk1_department_no
from   rtl_loc_item_wk_catalog f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_fin_year_no
and f.fin_week_no    = l_fin_week_no
),

--Sales at loc item week level for last completed week
rms_dense_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       il.sk1_department_no,
       sum(nvl(f.sales,0)) sales_actual
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_fin_year_no
and f.fin_week_no         = l_fin_week_no
and (f.sales is not null
  or f.sales_qty is not null
  or f.sales_margin is not null)
  group by f.sk1_location_no, il.sk1_department_no
),

--Sales YTD at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       il.sk1_department_no,
       sum(nvl(f.sales,0)) sales_actual_ytd
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.fin_year_no
  and f.fin_week_no     = wl.fin_week_no
and f.sales is not null
group by f.sk1_location_no, il.sk1_department_no
),

sales_budget_values as (
select f.sk1_location_no, f.sk1_department_no,
sum(sales_budget) sales_budget_dept,
sum(waste_cost_budget) waste_cost_budget,
sum(sales_margin_budget) sales_margin_budget
from rtl_loc_dept_wk f,  store_list sl, department_info di
where f.fin_year_no     = l_fin_year_no
and f.fin_week_no       = l_fin_week_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
and (sales_budget <> 0 or waste_cost_budget <>0) 
group by f.sk1_location_no, f.sk1_department_no
order by f.sk1_location_no, f.sk1_department_no
),

sales_budget_ytd_values as (
select f.sk1_location_no,
f.sk1_department_no,
sum(sales_budget) sales_budget_dept_ytd,
sum(waste_cost_budget) waste_cost_budget_ytd,
sum(sales_margin_budget) sales_margin_budget_ytd
from rtl_loc_dept_wk f, store_list sl, week_list wl, department_info di
where f.fin_year_no = wl.fin_year_no
and f.fin_week_no = wl.fin_week_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
and f.sales_budget <> 0
group by f.sk1_location_no, f.sk1_department_no
),

cra_dept_sales as (
select f.sk1_location_no, f.sk1_department_no,
sum(cra_recon_2_sales) cra_sales
from rtl_loc_dept_dy f,  store_list sl, department_info di  --item_list il
where post_date between l_cra_start_date and l_cra_end_date
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
group by f.sk1_location_no, f.sk1_department_no
),

cra_dept_sales_ly as (
select f.sk1_location_no, f.sk1_department_no,
sum(cra_recon_2_sales) cra_sales_ly
from rtl_loc_dept_dy f,  store_list sl, department_info di  --item_list il
where post_date between l_last_yr_wk_start_date and l_ly_current_date
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
group by f.sk1_location_no, f.sk1_department_no
),

sales_budget_values_ly as (
select f.sk1_location_no, f.sk1_department_no,
sum(sales_budget) sales_budget_dept_ly
--sum(waste_cost_budget) waste_cost_budget,
--sum(sales_margin_budget) sales_margin_budget
from rtl_loc_dept_wk f,  store_list sl, department_info di
where f.fin_year_no     = l_last_fin_year
and f.fin_week_no       = l_last_week_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
and sales_budget <> 0 
group by f.sk1_location_no, f.sk1_department_no
--order by f.sk1_location_no, f.sk1_department_no
),

--Sales YTD LY
rms_dense_ytd_measures_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       il.sk1_department_no,
       sum(nvl(f.sales,0)) sales_actual_ly_ytd
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.fin_year_no-1
  and f.fin_week_no     = wl.fin_week_no
and f.sales is not null
group by f.sk1_location_no, il.sk1_department_no
),

sales_budget_year_values as (
select f.sk1_location_no,
f.sk1_department_no,
sum(sales_budget) total_sales_budget,
sum(waste_cost_budget) total_waste_cost_budget,
sum(sales_margin_budget) total_sales_margin_budget
from rtl_loc_dept_wk f, store_list sl, week_list wl, department_info di
where f.fin_year_no = l_last_wk_fin_year_no    --wl.fin_year_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = di.sk1_department_no
and f.sales_budget <> 0
group by f.sk1_location_no, f.sk1_department_no
),

all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,2) PARALLEL(f2,2) */

nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                            f2.sk1_location_no),
                                            f3.sk1_location_no),
                                            f4.sk1_location_no),
                                            f5.sk1_location_no),
                                            f6.sk1_location_no),
                                            f7.sk1_location_no),
                                            f8.sk1_location_no),
                                            f9.sk1_location_no)

sk1_location_no,
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_department_no, f1.sk1_department_no),
                                              f2.sk1_department_no),
                                              f3.sk1_department_no),
                                              f4.sk1_department_no),
                                              f5.sk1_department_no),
                                              f6.sk1_department_no),
                                              f7.sk1_department_no),
                                              f8.sk1_department_no),
                                              f9.sk1_department_no)
sk1_department_no,
f3.sales_budget_dept,
f1.sales_actual,
f2.sales_actual_ytd,
f4.sales_budget_dept_ytd,
f3.sales_margin_budget,
f4.sales_margin_budget_ytd,
f5.cra_sales,
f6.cra_sales_ly,
f3.waste_cost_budget,
f4.waste_cost_budget_ytd,
f7.sales_budget_dept_ly,
f8.sales_actual_ly_ytd,
f9.total_sales_budget,
f9.total_waste_cost_budget,
f9.total_sales_margin_budget

from catalog_measures f0
full outer join rms_dense_measures      f1 on f0.sk1_location_no    = f1.sk1_location_no
                                          and f0.sk1_department_no  = f1.sk1_department_no

full outer join rms_dense_ytd_measures  f2 on nvl(f0.sk1_location_no, f1.sk1_location_no)    = f2.sk1_location_no
                                          and nvl(f0.sk1_department_no,f1.sk1_department_no) = f2.sk1_department_no


full outer join sales_budget_values     f3 on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                          f2.sk1_location_no)    = f3.sk1_location_no
                                          and nvl(nvl(f0.sk1_department_no, f1.sk1_department_no),
                                                                            f2.sk1_department_no) = f3.sk1_department_no

full outer join sales_budget_ytd_values f4 on nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                              f2.sk1_location_no),
                                                                              f3.sk1_location_no)   = f4.sk1_location_no
                                        and nvl(nvl(nvl(f0.sk1_department_no, f1.sk1_department_no),
                                                                              f2.sk1_department_no),
                                                                              f3.sk1_department_no) = f4.sk1_department_no

full outer join cra_dept_sales          f5 on nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                                  f2.sk1_location_no),
                                                                                  f3.sk1_location_no),
                                                                                  f4.sk1_location_no)    = f5.sk1_location_no
                                         and nvl(nvl(nvl(nvl(f0.sk1_department_no,f1.sk1_department_no),
                                                                                  f2.sk1_department_no),
                                                                                  f3.sk1_department_no),
                                                                                  f4.sk1_department_no)  = f5.sk1_department_no

full outer join cra_dept_sales_ly       f6 on nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                     f2.sk1_location_no),
                                                                                     f3.sk1_location_no),
                                                                                     f4.sk1_location_no),
                                                                                     f5.sk1_location_no)  = f6.sk1_location_no
                                        and nvl(nvl(nvl(nvl(nvl(f0.sk1_department_no,f1.sk1_department_no),
                                                                                     f2.sk1_department_no),
                                                                                     f3.sk1_department_no),
                                                                                     f4.sk1_department_no),
                                                                                     f5.sk1_department_no)   = f6.sk1_department_no

full outer join sales_budget_values_ly       f7 on nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                     f2.sk1_location_no),
                                                                                     f3.sk1_location_no),
                                                                                     f4.sk1_location_no),
                                                                                     f5.sk1_location_no),
                                                                                     f6.sk1_location_no) = f7.sk1_location_no
                                        and nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_department_no,f1.sk1_department_no),
                                                                                     f2.sk1_department_no),
                                                                                     f3.sk1_department_no),
                                                                                     f4.sk1_department_no),
                                                                                     f5.sk1_department_no),
                                                                                     f6.sk1_department_no) = f7.sk1_department_no

full outer join rms_dense_ytd_measures_ly    f8 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                     f2.sk1_location_no),
                                                                                     f3.sk1_location_no),
                                                                                     f4.sk1_location_no),
                                                                                     f5.sk1_location_no),
                                                                                     f6.sk1_location_no),
                                                                                     f7.sk1_location_no) = f8.sk1_location_no
                                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_department_no,f1.sk1_department_no),
                                                                                     f2.sk1_department_no),
                                                                                     f3.sk1_department_no),
                                                                                     f4.sk1_department_no),
                                                                                     f5.sk1_department_no),
                                                                                     f6.sk1_department_no),
                                                                                     f7.sk1_department_no) = f8.sk1_department_no

full outer join sales_budget_year_values    f9 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                     f2.sk1_location_no),
                                                                                     f3.sk1_location_no),
                                                                                     f4.sk1_location_no),
                                                                                     f5.sk1_location_no),
                                                                                     f6.sk1_location_no),
                                                                                     f7.sk1_location_no),
                                                                                     f8.sk1_location_no) = f9.sk1_location_no
                                                                                     
                                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_department_no,f1.sk1_department_no),
                                                                                     f2.sk1_department_no),
                                                                                     f3.sk1_department_no),
                                                                                     f4.sk1_department_no),
                                                                                     f5.sk1_department_no),
                                                                                     f6.sk1_department_no),
                                                                                     f7.sk1_department_no),
                                                                                     f8.sk1_department_no) = f9.sk1_department_no

)

select /*+ PARALLEL(atg,4) */
atg.sk1_location_no,
atg.sk1_department_no,
l_fin_year_no,
l_fin_week_no,
dd.department_no,
trim(dd.department_name),
atg.sales_budget_dept,
atg.sales_actual,
atg.sales_actual_ytd,
(atg.sales_actual - atg.sales_budget_dept) sales_budget_variance,
(atg.sales_actual_ytd - atg.sales_budget_dept_ytd) sales_budget_variance_ytd,
atg.sales_budget_dept_ytd,
atg.sales_margin_budget,
atg.sales_margin_budget_ytd,
atg.cra_sales,
atg.cra_sales_ly,
atg.waste_cost_budget,
atg.waste_cost_budget_ytd,
g_date last_updated_date,
l_fin_week_code,
l_this_wk_start_date,
atg.SALES_BUDGET_DEPT_LY,
atg.TOTAL_SALES_BUDGET,
atg.SALES_ACTUAL_LY_YTD,
atg.TOTAL_SALES_MARGIN_BUDGET,     --atg.TOTAL_MARGIN_BUDGET_RANDS
atg.TOTAL_WASTE_COST_BUDGET,
(atg.total_waste_cost_budget - atg.waste_cost_budget_ytd)  total_waste_cost_budget_adj,
(atg.total_sales_budget - atg.sales_actual_ytd) total_sales_budget_adj,
0   sales_actual_ly_ytg

from all_together atg,
     department_info dd
    
where atg.sk1_department_no   = dd.sk1_department_no

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


end wh_prf_corp_236t;
