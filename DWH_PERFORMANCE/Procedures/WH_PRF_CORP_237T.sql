--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_237T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_237T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        October 2012
--  Author:      Quentin Smit
--  Purpose:     Create location_item promotions Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               For Foods weekly snapshot 1.2 for LAST COMPLETED WEEK
--               When a Monday, this must run for two weeks back as the program that runs
--               for the current week on a Monday will then do the last completed week (wh_prf_corp_238u).
--               DOES THE TRUNCATE SO MUST RUN FIRST !!
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - tmp_mart_fd_loc_itwm_wk_prom
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_237T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Mart table for Loc Item Wk Prom - LCW';
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

l_max_fin_week_last_year number;
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

    l_text := 'Create TEST Foods Loc Item Week Prom Mart for LAST COMPLETED WEEK STARTED AT  '||
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


     select  today_fin_year_no,         -- current year
        last_wk_fin_year_no -1 ,        -- last year
        last_wk_fin_week_no,            -- last_completed_week
        today_fin_week_no,              -- current_week
        next_wk_fin_week_no,            -- next_week
        next_wk_fin_year_no,            -- next week's fin year
        last_wk_fin_year_no,            -- last week's fin year
        today_fin_day_no,
        this_wk_start_date,             -- start date of current week
        last_wk_fin_week_no             -- end date of 6 week period
        into l_fin_year_no,
             l_last_fin_year,
             l_last_week_no,
             l_fin_week_no,
             l_next_wk_fin_week_no,
             l_next_wk_fin_year_no,
             l_last_wk_fin_year_no,
             l_day_no,
             l_this_wk_start_date,
             l_end_6wks
from dim_control_report;

select max(fin_week_no)
  into l_max_fin_week_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year;
 
l_day_no := 1;    --QST

-- Must cater for when this program runs on a Monday as then it must get data for 2 weeks back
if l_day_no = 1 then
   if l_fin_week_no = 1 then
      l_fin_week_no := l_max_fin_week_last_year;            -- First day of new year, get last week of last fin year
      l_last_wk_fin_year_no := l_last_wk_fin_year_no - 1;   -- First day of new year, get fin year - 1
      l_last_fin_year := l_last_fin_year - 2;               -- first day of new year, previous fin year will be - 2
   else
      -- It's a Monday, process 2 weeks ago data
      l_fin_week_no := l_fin_week_no -1;
      l_last_week_no := l_last_week_no -1;
   end if;
end if;

l_text := 'Week being processed:- '||l_last_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--Get date of day 1 of corresponding current week last year
--if l_fin_week_no = 1 then
--   select calendar_date
--    into l_date_day1_last_yr
--     from dim_calendar
--    where fin_year_no = l_last_wk_fin_year_no
--      and fin_week_no = l_fin_week_no
--      and fin_day_no = 1;
--else
--   select calendar_date
--    into l_date_day1_last_yr
--     from dim_calendar
--    where fin_year_no = l_last_fin_year
--      and fin_week_no = l_fin_week_no
--      and fin_day_no = 1;
--end if;

-- Get start date of 8 weeks ago ; program should only run on a Monday or Tuesday but cater for the whole week
-- To go back 8 weeks subtract the amount of days depending on which day the program is run on
if l_day_no = 1 then l_less_days := '49'; end if;  --57
if l_day_no = 2 then l_less_days := '50'; end if;
if l_day_no = 3 then l_less_days := '51'; end if;
if l_day_no = 4 then l_less_days := '52'; end if;
if l_day_no = 5 then l_less_days := '53'; end if;
if l_day_no = 6 then l_less_days := '54'; end if;
if l_day_no = 7 then l_less_days := '55'; end if;  --63

-- Get start and end dates of last completed week
select min(calendar_date) start_date,
       max(calendar_date) end_date
into l_last_wk_start_date, l_last_wk_end_date
from dim_calendar
where fin_year_no = l_last_wk_fin_year_no
  and fin_week_no = l_last_week_no
  group by fin_year_no, fin_week_no;

-- Get start and end dates of corresponding last completed week last year
select min(calendar_date) start_date,
       max(calendar_date) end_date,
       max(fin_week_code) fin_week_code
into l_last_yr_wk_start_date, l_last_yr_wk_end_date, l_fin_week_code
from dim_calendar
where fin_year_no = l_last_fin_year
  and fin_week_no = l_last_week_no
  ;

--day7_last_yr_wk
--select calendar_date
--into l_day7_last_yr_wk_date
--from dim_calendar
--where fin_year_no = l_last_fin_year
--and fin_week_no = l_last_week_no
--and fin_day_no = 7;

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
l_text := 'next week year = '|| l_next_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'last completed weeks year = '|| l_last_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'l_day_no = '|| l_day_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_less_days = '|| l_less_days;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 6wks date = '|| l_end_6wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start date of week being processed = '|| l_last_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end date of week being processed = '|| l_last_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'day7 last week last year date = '|| l_day7_last_yr_wk_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'this week start date  = '|| l_this_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'day 1 of current week last year  = '|| l_date_day1_last_yr;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start date of last completed week last year  = '|| l_last_yr_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end date of last completed week last year  = '|| l_last_yr_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table w6005682.tmp_mart_fd_loc_item_wk_prom_t');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

INSERT /*+ APPEND */ INTO w6005682.tmp_mart_fd_loc_item_wk_prom_t mart
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
       tran_ind,
       rit.no_of_weeks,
       di.group_no,
       di.group_name,
       di.subgroup_no,
       di.subgroup_name
from   dim_item di
       join dim_department dd on di.sk1_department_no = dd.sk1_department_no
       left join rtl_item_trading rit on di.sk1_item_no = rit.sk1_item_no
where  di.business_unit_no = 50
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

item_pclass as (
select di.sk1_item_no,
       di.item_no,
       SUBSTR(di.item_desc,1,40) AS item_desc,
       di.department_no,
       di.department_name,
       di.subclass_no,
       di.subclass_name,
       uda.product_class_desc_507,
       uda.merchandise_category_desc_100,
       uda.commercial_manager_desc_562
from   dim_item di, dim_item_uda uda
where di.sk1_item_no=uda.sk1_item_no
  and di.business_unit_no = 50
),

prom_values as (
select /*+ PARALLEL(prom,4)  */
 prom.sk1_location_no, prom.sk1_item_no,
 prom.sk1_prom_no,
--  max(case
--    when dp.Prom_type = 'MU' THEN 'MU'
--  
--    when dp.Prom_type = 'SK' THEN 'SK'
--  
--    when dp.Prom_type = 'TH' THEN 'TH'
--  
--    when dp.Prom_type = 'MM' THEN 'MM'
--    else null
-- END) as promo_type,
  max(prom_type) as prom_type,
  
  max(case
    when dp.prom_level_type = 'S' then 'STORE'
  
    when dp.prom_level_type = 'Z' then 'ZONE'
    else null
  end) as prom_level_type,
  
  sum(prom_sales) prom_sales,
  sum(prom_sales_qty) prom_sales_qty
 from rtl_prom_loc_item_dy prom, store_list dl, item_list di, dim_prom dp
 where prom.Post_Date between l_last_wk_start_date and l_last_wk_end_date
 and prom.sk1_location_no = dl.sk1_location_no
 and prom.sk1_item_no = di.sk1_item_no
 and prom.sk1_prom_no = dp.sk1_prom_no
 and prom.sk1_prom_period_no = 9749661
 group by prom.sk1_location_no, prom.sk1_item_no, prom.sk1_prom_no
  ),

prom_values_ly as (
select prom.sk1_location_no, prom.sk1_item_no,
 prom.sk1_prom_no,
  sum(prom_sales) prom_sales_ly
 from rtl_prom_loc_item_dy prom, store_list dl, item_list di, dim_prom dp
 where prom.Post_Date between l_last_yr_wk_start_date and l_last_yr_wk_end_date
 and prom.sk1_location_no = dl.sk1_location_no
 and prom.sk1_item_no = di.sk1_item_no
 and prom.sk1_prom_no = dp.sk1_prom_no
 and prom.sk1_prom_period_no = 9749661
 group by prom.sk1_location_no, prom.sk1_item_no, prom.sk1_prom_no
),

--prom_values_sparse as (
--select /*+ PARALLEL(f,4)  */
-- f.sk1_location_no, f.sk1_item_no,
-- dp.sk1_prom_no,
-- sum(prom_sales) prom_sales, 
--  sum(prom_sales_qty) prom_sales_qty
-- from rtl_loc_item_wk_rms_sparse f, store_list dl, item_list di, dim_prom dp
-- where f.fin_year_no = l_fin_year_no
-- and f.fin_week_no = l_fin_week_no
--and f.sk1_location_no = dl.sk1_location_no
--   and f.sk1_item_no = di.sk1_item_no
--   and f.prom_discount_no = dp.prom_no
-- group by f.sk1_location_no, f.sk1_item_no, dp.sk1_prom_no
--),

--prom_values_ly as (
--select /*+ PARALLEL(f,4)  */
-- f.sk1_location_no, f.sk1_item_no,
-- dp.sk1_prom_no,
--  sum(prom_sales) prom_sales_ly
--  from rtl_loc_item_wk_rms_sparse f, store_list dl, item_list di, dim_prom dp
-- where f.fin_year_no = l_last_fin_year
-- and f.fin_week_no = l_fin_week_no
--and f.sk1_location_no = dl.sk1_location_no
--   and f.sk1_item_no = di.sk1_item_no
--   and f.prom_discount_no = dp.prom_no
-- group by f.sk1_location_no, f.sk1_item_no, dp.sk1_prom_no
--),

all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,2) PARALLEL(f1,2) */

nvl(f0.sk1_location_no,f1.sk1_location_no)   sk1_location_no,
nvl(f0.sk1_item_no, f1.sk1_item_no)          sk1_item_no,
nvl(f0.sk1_prom_no, f1.sk1_prom_no)          sk1_prom_no,
f0.PROM_LEVEL_TYPE,
f0.PROM_SALES,
f1.PROM_SALES_LY,
f0.PROM_SALES_QTY,
f0.PROM_TYPE

from prom_values f0
full outer join prom_values_ly        f1 on f0.sk1_location_no  = f1.sk1_location_no
                                        and f0.sk1_item_no      = f1.sk1_item_no
                                        and f0.sk1_prom_no      = f1.sk1_prom_no
                                        
--full outer join prom_values_sparse    f2 on nvl(f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
--                                        and nvl(f0.sk1_item_no,     f1.sk1_item_no)     = f2.sk1_item_no
--                                        and nvl(f0.sk1_prom_no,     f1.sk1_prom_no)     = f2.sk1_prom_no
--
)

select /*+ PARALLEL(atg,4) */
atg.sk1_location_no,
atg.sk1_item_no,
l_fin_year_no,
l_last_week_no,
atg.sk1_prom_no,

atg.PROM_LEVEL_TYPE,
atg.PROM_SALES,
atg.PROM_SALES_LY,
atg.PROM_SALES_QTY,
atg.PROM_TYPE,
sl.WH_FD_ZONE_NO,
g_date last_updated_date,
l_fin_week_code,
l_this_wk_start_date,
0,    --prom_sales_6wk_avg  --> LOGIC STILL TO BE ADDED!!
0     --prom_sales_qty_ly   --> LOGIC STILL TO BE ADDED!!

from all_together atg,
     store_list sl

where atg.sk1_location_no     = sl.sk1_location_no
  --and atg.sk1_location_no = itm.sk1_location_no
  --and atg.sk1_item_no     = il.sk1_item_no

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

end wh_prf_corp_237t;
