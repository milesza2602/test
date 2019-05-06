--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_223A_1B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_223A_1B" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2012
--  Author:      Quentin Smit
--  Purpose:     Create location_item Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               For Foods weekly snapshot
--               Monday Run
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - tmp_mart_foods_wkly_snpsht_t
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_223A_1B';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Mart table for Foods weekly snapshot';
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

    l_text := 'Create Mart table for Foods weekly snapshot STARTED AT '||
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


    l_text := 'Week being processed:- '||g_last_week ;
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
-- Defunct after wk 53 changes
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
--need to check if 6wk period spans more than on fin year
if l_end_6wks < 6 then
   select max(fin_week_no)
     into l_max_6wk_last_year
     from dim_calendar_wk
    where fin_year_no = l_last_fin_year;

   select calendar_date end_date
     into l_end_6wks_date
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no
      and fin_week_no = l_end_6wks
      and fin_day_no = 7;

   --############################################################################################
   -- Below is a bit long but it breaks down the year and week grouping for the 6wk calculation.
   -- This needs to be done as the string can't be used in the insert / append statement as it
   -- uses a big 'with ..'  clause.
   -- There is probably a nicer and neater way of doing by using a loop but for now this will work..
   --############################################################################################
   if l_end_6wks = 5 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk5_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_end_6wks-4;
      l_6wks_wk6    := l_max_6wk_last_year;
   end if;
   if l_end_6wks = 4 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_max_6wk_last_year;
      l_6wks_wk6    := l_max_6wk_last_year-1;
   end if;
   if l_end_6wks = 3 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_max_6wk_last_year;
      l_6wks_wk5    := l_max_6wk_last_year-1;
      l_6wks_wk6    := l_max_6wk_last_year-2;
   end if;
   if l_end_6wks = 2 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_fin_year;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_max_6wk_last_year;
      l_6wks_wk4    := l_max_6wk_last_year-1;
      l_6wks_wk5    := l_max_6wk_last_year-2;
      l_6wks_wk6    := l_max_6wk_last_year-3;
   end if;
   if l_end_6wks = 1 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_fin_year;
      l_6wks_wk3_yr := l_last_fin_year;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_max_6wk_last_year;
      l_6wks_wk3    := l_max_6wk_last_year-1;
      l_6wks_wk4    := l_max_6wk_last_year-2;
      l_6wks_wk5    := l_max_6wk_last_year-3;
      l_6wks_wk6    := l_max_6wk_last_year-4;
   end if;

--Calendar dates for last 6 weeks
select unique calendar_date start_date
into l_start_6wks_date
from dim_calendar
where fin_year_no = l_last_fin_year
 and fin_week_no = l_6wks_wk6
 and fin_day_no = 1;

else
   select calendar_date end_date
     into l_end_6wks_date
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no  --l_fin_year_no
      and fin_week_no = l_end_6wks
      and fin_day_no = 7;

     --l_6wks_string := '( fin_year_no = l_fin_year_no and fin_week between l_start_6wks and l_end_6wk)';
     l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk5_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk6_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk1    := l_end_6wks;
     l_6wks_wk2    := l_end_6wks-1;
     l_6wks_wk3    := l_end_6wks-2;
     l_6wks_wk4    := l_end_6wks-3;
     l_6wks_wk5    := l_end_6wks-4;
     l_6wks_wk6    := l_end_6wks-5;

     --Calendar dates for last 6 weeks
     select unique calendar_date start_date
       into l_start_6wks_date
       from dim_calendar
      where fin_year_no = l_last_wk_fin_year_no  --l_fin_year_no
        and fin_week_no = l_6wks_wk6  --l_start_6wks
        and fin_day_no = 1;
end if
;


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
  and fin_week_no  <= l_last_week_no   --l_fin_week_no
)
select min(this_week_start_date) start_date,
       max(this_week_end_date) end_date
into l_ytd_start_date, l_ytd_end_date
from week_list;

-- Get start and end dates of last completed week
select min(calendar_date) start_date,
       max(calendar_date) end_date
into l_last_wk_start_date, l_last_wk_end_date
from dim_calendar
where fin_year_no = l_last_wk_fin_year_no
  and fin_week_no = l_last_week_no
  group by fin_year_no, fin_week_no;

/* Defunct after wk53 change
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

l_text := 'last completed weeks year = '|| l_last_wk_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start 6wks : year = '|| l_6wks_wk6_yr || ' week = ' ||  l_6wks_wk6;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 6wks : year = '|| l_6wks_wk1_yr || ' week = ' || l_6wks_wk1;    --l_end_6wks;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start 6wks date = '|| l_start_6wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'start 8wks date = '|| l_start_8wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

l_text := 'last week start date = '|| l_last_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last week end date = '|| l_last_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'day7 last week last year date = '|| l_day7_last_yr_wk_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'this week start date  = '|| l_this_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'day 1 of current week last year  = '|| l_date_day1_last_yr;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'start date of last completed week last year  = '|| l_last_yr_wk_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'end date of last completed week last year  = '|| l_last_yr_wk_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text:= '6wks week 1 year = ' || l_6wks_wk1_yr || ' week = ' || l_6wks_wk1;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text:= '6wks week 2 year = ' || l_6wks_wk2_yr || ' week = ' || l_6wks_wk2;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text:= '6wks week 3 year = ' || l_6wks_wk3_yr || ' week = ' || l_6wks_wk3;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text:= '6wks week 4 year = ' || l_6wks_wk4_yr || ' week = ' || l_6wks_wk4;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text:= '6wks week 5 year = ' || l_6wks_wk5_yr || ' week = ' || l_6wks_wk5;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text:= '6wks week 6 year = ' || l_6wks_wk6_yr || ' week = ' || l_6wks_wk6;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := '53 Week LY Year =  = '||l_53_ly_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY week  = '|| l_53_ly_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY Day 1  = '||l_53_ly_day1_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '53 Week LY Day7  = '|| l_53_ly_day7_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_53_ly_fin_week_no := 'MOO';
----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table dwh_performance.tmp_mart_foods_wkly_snpsht_t');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

INSERT /*+ APPEND */ INTO dwh_performance.tmp_mart_foods_wkly_snpsht_t mart
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
       rit.no_of_weeks,
       di.group_no,
       di.group_name,
       di.subgroup_no,
       di.subgroup_name,
       di.PACK_ITEM_IND pack_item_ind
from   dim_item di
       join dim_department dd on di.sk1_department_no = dd.sk1_department_no
       left join rtl_item_trading rit on di.sk1_item_no = rit.sk1_item_no
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

calendar_week as (
select  today_fin_year_no fin_year_no,      -- current year
        today_fin_year_no -1 last_fin_year, -- last year
        last_wk_fin_week_no,                -- last_completed_week
        today_fin_week_no fin_week_no,      -- current_week
        next_wk_fin_week_no,                -- next_week
        last_wk_fin_year_no
from dim_control_report
),

--list of year to date weeks for last year
week_list as (
select dcw .fin_year_no,
       dcw.fin_week_no,
       this_week_start_date,
       this_week_end_date,
       ly_fin_year_no,
       ly_fin_week_no
from   dim_calendar_wk dcw
where dcw.fin_year_no =  l_last_wk_fin_year_no
  and dcw.fin_week_no  <= l_last_week_no  -- l_fin_week_no
),

--list of weeks in last year
last_year_weeks as (
select dcw.fin_week_no
from   dim_calendar_wk dcw
where dcw.fin_year_no =  l_last_fin_year
),

---Start and end of 6wk period for 6 week average calculations
six_wks as (
select unique wl.fin_week_no
from week_list wl, calendar_week cw
where wl.fin_week_no > cw.last_wk_fin_week_no - 6
order by fin_week_no
),

--Date of last day of last 8 weeks
eight_weeks_end_date as (
select calendar_date
  from dim_calendar
  where calendar_date between l_start_8wks_date and l_this_wk_start_date
   and fin_day_no = 1
  order by calendar_date
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

like_for_like_ly as (
  select sk1_location_no,
         max(like_for_like_adj_ind) like_for_like_ind_ly
    from rtl_loc_dy
    where post_date = l_53_ly_day1_date    --WEEK 53 CODE--
--   where post_date = l_last_yr_wk_start_date   --between l_last_yr_wk_start_date and l_last_yr_wk_end_date
   group by sk1_location_no
),

--like_for_like_loc_ly as (
--   select unique sk1_location_no, sk1_item_no, like_for_like_ind like_for_like_ind_ly
--   from like_for_like_ly, item_list
--),

like_for_like_ty as (
  select sk1_location_no,
         max(like_for_like_adj_ind) like_for_like_ind_ty
    from rtl_loc_dy
   where post_date = l_last_wk_start_date  --between l_last_wk_start_date and l_last_wk_start_date
   group by sk1_location_no
),

-- Items that were out of stock on the last day of last completed week
items_out_of_stock as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_last_wk_end_date
  and f.ll_dc_out_of_stock > 0
),

--Sales at loc item week level for last completed week
rms_dense_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales,0) sales,
       nvl(f.sales_qty,0) sales_qty,
       nvl(f.sales_margin,0) sales_margin
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no     =    l_last_wk_fin_year_no
and f.fin_week_no     =  l_last_week_no
and (f.sales is not null
  or f.sales_qty is not null
  or f.sales_margin is not null)
),

--Sales LY at loc item week level. Used to calculate S79012ales LY %. Sales LY not shown in report.
rms_dense_ly_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales sales_ly,
       f.sales_qty sales_qty_ly,
       f.sales_margin sales_margin_ly
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no     =  l_53_ly_fin_year_no  --WK 53 CODE--
and f.fin_week_no     =  l_53_ly_fin_week_no  --WK 53 CODE--
--and f.fin_year_no   =  l_last_fin_year
--and f.fin_week_no   =  l_last_week_no
and (f.sales is not null or f.sales_qty is not null or f.sales_margin is not null)
),

-- Average sales for last 6 weeks
rms_dense_6wk_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
 nvl((sum(f.sales)/6),0) sales_6w_avg
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and ((f.fin_year_no =  l_6wks_wk1_yr and f.fin_week_no = l_6wks_wk1)
   or (f.fin_year_no = l_6wks_wk2_yr and f.fin_week_no = l_6wks_wk2)
   or (f.fin_year_no = l_6wks_wk3_yr and f.fin_week_no = l_6wks_wk3)
   or (f.fin_year_no = l_6wks_wk4_yr and f.fin_week_no = l_6wks_wk4)
   or (f.fin_year_no = l_6wks_wk5_yr and f.fin_week_no = l_6wks_wk5)
   or (f.fin_year_no = l_6wks_wk6_yr and f.fin_week_no = l_6wks_wk6))
and f.sales is not null
and f.sales<>0
group by f.sk1_location_no, f.sk1_item_no
),

--Sales YTD at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.sales,0)) sales_ytd,
       sum(nvl(f.sales_qty,0)) sales_qty_ytd,
       sum(nvl(f.sales_margin,0)) sales_margin_ytd
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.fin_year_no
  and f.fin_week_no     = wl.fin_week_no
and (f.sales is not null or f.sales_qty is not null or f.sales_margin is not null)
group by f.sk1_location_no, f.sk1_item_no
),

--Sales YTD LY at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures_ly as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(f.sales),0) sales_ly_ytd
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.ly_fin_year_no       --wl.fin_year_no-1
  and f.fin_week_no     = wl.ly_fin_week_no       --wl.fin_week_no
  and f.sales is not null
  and f.sales <>0
group by f.sk1_location_no, f.sk1_item_no
),

-- Item Price for day 7 of last completed week
item_price_day7 as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
reg_rsp rsp_day7
from rtl_loc_item_dy_rms_price pr, item_list il, store_list sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date = l_last_wk_end_date
and reg_rsp is not null
),

-- Item price for corresponding day 7 of same week LY
item_price_day7_ly as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
reg_rsp rsp_day7_ly
from rtl_loc_item_dy_rms_price pr,
item_list il, store_list sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date = l_53_ly_day7_date  --WK 53 CODE--
--and pr.calendar_date = l_day7_last_yr_wk_date  --'11/APR/11'
and reg_rsp is not null
),

--Shorts values for last completed week
po_supchain_week as (
select /*+ PARALLEL(po,6) FULL(po) */
       po.sk1_location_no,
       po.sk1_item_no,
 sum(nvl(po.shorts_selling,0)) shorts_rands ,
       sum(nvl(po.shorts_cases,0)) shorts_cases,
       sum(nvl(po.shorts_qty,0)) shorts_units,
       sum(nvl(po.fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty,
       sum(nvl(po.fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty
from rtl_supchain_loc_item_dy po, item_list il, store_list sl
where po.sk1_item_no     = il.sk1_item_no
  and po.sk1_location_no = sl.sk1_location_no
  and po.tran_date between l_last_wk_start_date and l_last_wk_end_date
  and (po.shorts_selling is not null
   or po.shorts_cases is not null
   or po.shorts_qty is not null
   or po.fillrate_fd_po_grn_qty is not null
   or po.fillrate_fd_latest_po_qty is not null)
group by po.sk1_location_no, po.sk1_item_no
),

--PO values for YTD
po_supchain_ytd as (
select /*+ PARALLEL(po,6) FULL(po) */
       po.sk1_location_no,
       po.sk1_item_no,
       sum(nvl(fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty_ytd,
       sum(nvl(fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty_ytd,
       sum(nvl(shorts_qty,0)) shorts_units_ytd
from rtl_supchain_loc_item_dy po, item_list il, store_list sl --, calendar_week cw
where po.sk1_item_no   = il.sk1_item_no
and po.sk1_location_no = sl.sk1_location_no
and po.tran_date between l_ytd_start_date and l_ytd_end_date  --'27/JUN/11' and '15/APR/12'
and (fillrate_fd_po_grn_qty is not null or fillrate_fd_latest_po_qty is not null)
group by po.sk1_location_no, po.sk1_item_no--, cw.fin_week_no
),

--PO 6wk average value
po_6wk_avg as (
select /*+ PARALLEL(f,6) FULL(f) */
       po.sk1_location_no,
       po.sk1_item_no,
       (sum(nvl(fillrate_fd_po_grn_qty,0)) / 6) fillrate_fd_po_grn_qty_6wk_avg,
       (sum(nvl(fillrate_fd_latest_po_qty,0)) / 6) fillrate_fd_ltst_p_qty_6wk_avg,
       (sum(nvl(shorts_qty,0)) / 6) shorts_units_6wk_avg
from   rtl_supchain_loc_item_dy po, item_list il, store_list sl  --,  calendar_week cw
where po.sk1_item_no     = il.sk1_item_no
and po.sk1_location_no = sl.sk1_location_no
and po.tran_date >= l_start_6wks_date  --'05/MAR/12'
and po.tran_date <= l_end_6wks_date  --15/APR/12'
and (fillrate_fd_po_grn_qty is not null or fillrate_fd_latest_po_qty is not null)
group by po.sk1_location_no, po.sk1_item_no--, cw.fin_year_no, cw.fin_week_no
),

--Promotion data
prom_values as (
select /*+ PARALLEL(p,6) FULL(p) */
       p.sk1_location_no,
       p.sk1_item_no,
       max(dp.prom_type) prom_type,
       max(wreward_ind) wreward_ind
from rtl_prom_loc_item_dy p, dim_prom dp, item_list il, store_list sl
where p.sk1_prom_no = dp.sk1_prom_no
and p.sk1_item_no = il.sk1_item_no
and p.sk1_location_no = sl.sk1_location_no
and p.post_date between l_last_wk_start_date and l_last_wk_end_date
group by p.sk1_location_no, p.sk1_item_no--, lwd.fin_week_no
),

--Available Days, Catalogue Days and DC availability for last completed week
catalog_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.fd_num_avail_days_adj,0) fd_num_avail_days_adj,
       nvl(f.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj,
       nvl(f.fd_num_avail_days,0) fd_num_avail_days,
       nvl(f.fd_num_catlg_days,0) fd_num_catlg_days,
       nvl(f.fd_num_catlg_wk,0) FD_NUM_CATLG_WK,
       case when iu.merchandise_category_desc_100 ='L' then f.boh_adj_qty else 0 end as boh_ll_adj_qty,
       nvl(f.boh_adj_selling,0) boh_selling_adj,
       f.this_wk_catalog_ind,
       nvl(ds.product_status_short_desc,0) product_status_this_week,
       nvl(ds1.product_status_short_desc,0) product_status_next_week,
       case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
          f.soh_adj_selling else 0
       end as soh_selling_adj_dc,
       nvl(f.soh_adj_selling,0) soh_selling_adj
from   rtl_loc_item_wk_catalog f, item_list il, dim_item_uda iu, store_list sl, dim_product_status ds, dim_product_status ds1
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_item_no     = iu.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_last_wk_fin_year_no
and f.fin_week_no    = l_last_week_no
and to_char(f.product_status_1_code) = ds.product_status_code
and to_char(f.product_status_code) = ds1.product_status_code
and (f.fd_num_avail_days_adj is not null or f.fd_num_catlg_days_adj is not null
  or f.fd_num_avail_days is not null     or f.fd_num_catlg_days is not null
  or f.boh_adj_qty is not null           or f.boh_adj_qty is not null)  -- or f.boh_adj_qty_flt is not null)
),

catalog_measures1 as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.BOH_ADJ_QTY_FLT,0) BOH_ADJ_QTY_FLT
from   rtl_loc_item_wk_catalog f, item_list il, dim_item_uda iu, store_list_area sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_item_no     = iu.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_last_wk_fin_year_no
and f.fin_week_no    = l_last_week_no
and f.boh_adj_qty_flt is not null
),

-- Availability values for LY
catalog_measures_ly as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.fd_num_avail_days_adj,0) fd_num_avail_days_adj_ly,
       nvl(f.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj_ly,
       nvl(f.fd_num_avail_days,0) fd_num_avail_days_ly,
       nvl(f.fd_num_catlg_days,0) fd_num_catlg_days_ly,
       nvl(f.fd_num_catlg_wk,0) fd_num_catlg_wk_ly,
       nvl(f.boh_adj_selling,0) boh_selling_adj_ly,
       case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
          nvl(f.soh_adj_selling,0) else 0
       end as soh_selling_adj_ly_dc,
       nvl(f.soh_adj_selling,0) soh_selling_adj_ly
from   rtl_loc_item_wk_catalog f, item_list il, store_list sl  --, calendar_week cw  --, last_year_weeks lyw
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no   =  l_53_ly_fin_year_no  --WK 53 CODE--
and f.fin_week_no   =  l_53_ly_fin_week_no  --WK 53 CODE--
--and f.fin_year_no   =  l_last_fin_year
--and f.fin_week_no   =  l_last_week_no
and (f.fd_num_avail_days_adj is not null
      or f.fd_num_catlg_days_adj is not null
      or f.fd_num_avail_days is not null
      or f.fd_num_catlg_days is not null)
),

--Catalog measures YTD
catalog_measures_ytd as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.fd_num_avail_days_adj,0)) fd_num_avail_days_adj_ytd,
       sum(nvl(f.fd_num_catlg_days_adj,0)) fd_num_catlg_days_adj_ytd,
       sum(nvl(f.fd_num_avail_days,0)) fd_num_avail_days_ytd,
       sum(nvl(f.fd_num_catlg_days,0)) fd_num_catlg_days_ytd
from   rtl_loc_item_wk_catalog f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no =  wl.fin_year_no
and f.fin_week_no = wl.fin_week_no
and (f.fd_num_avail_days_adj is not null
   or fd_num_catlg_days_adj is not null
   or f.fd_num_avail_days is not null
   or f.fd_num_catlg_days is not null)
group by f.sk1_location_no, f.sk1_item_no
),

-- RDF / Depot data
rdf_depot as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales_wk_app_fcst,0) sales_wk_app_fcst,
       nvl(f.sales_wk_app_fcst_qty,0) sales_wk_app_fcst_qty,
       nvl(f.SALES_WK_APP_FCST_QTY_FLT_AV,0) SALES_WK_APP_FCST_QTY_FLT_AV,
       nvl(f.fd_num_dc_avail_adj_days,0) fd_num_dc_avail_adj_days,
       nvl(f.fd_num_dc_avail_days,0) fd_num_dc_avail_days,
       nvl(f.fd_num_dc_catlg_adj_days,0) fd_num_dc_catlg_adj_days,
       nvl(f.fd_num_dc_catlg_days,0) FD_NUM_DC_CATLG_DAYS,
       nvl(f.fd_num_dc_avail_adj_days,0) fd_num_dc_avail_adj_days_ly,
       nvl(f.fd_num_dc_avail_days,0) fd_num_dc_avail_days_ly,
       nvl(f.fd_num_dc_catlg_adj_days,0) fd_num_dc_catlg_adj_days_ly,
       nvl(f.fd_num_dc_catlg_days,0) fd_num_dc_catlg_days_ly,
       nvl(f.stock_cases,0) stock_cases,
       nvl(expired_stock,0) expired_stock,
       nvl(expiring_stock_wk1_3,0) expiring_stock_wk1_3,
       nvl(f.est_ll_short_cases_av,0) est_ll_short_cases_av,
       nvl(f.stock_dc_cover_cases,0) stock_dc_cover_cases,
       nvl(f.dc_out_of_stock,0) DC_OUT_OF_STOCK
       --nvl(f.weeks_out_of_stock,0) weeks_out_of_stock   TESTING !! SEE TABLE weeks_no_stock
from tmp_mart_rdf_depot f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
),

--Corrected sales for last completed week
corrected_sales as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.corr_sales,0) corrected_sales
from rtl_loc_item_wk_rdf_sale f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no     = l_last_wk_fin_year_no
and f.fin_week_no     = l_last_week_no
and f.corr_sales is not null
) ,

--Waste Cost
rms_sparse_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(waste_cost),0) waste_cost,
       nvl(sum(f.prom_sales),0) prom_sales,
       nvl(sum(f.prom_sales_qty),0) prom_sales_qty,
       nvl(sum(f.shrinkage_selling),0) shrinkage_selling
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no  =   l_last_wk_fin_year_no
and f.fin_week_no   =  l_last_week_no
--and ((f.waste_cost is not null and f.waste_cost > 0)
-- or (f.prom_sales is not null and f.prom_sales > 0)
-- or (f.prom_sales_qty is not null and f.prom_sales_qty >0))
group by f.sk1_location_no, f.sk1_item_no
),

-- Waste cost LY
rms_sparse_measures_ly as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.waste_cost waste_cost_ly
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no  =  l_53_ly_fin_year_no   --WK 53 CODE--
and f.fin_week_no  =  l_53_ly_fin_week_no   --WK 53 CODE--
--and f.fin_year_no  =  l_last_fin_year
--and f.fin_week_no  =  l_last_week_no
--and f.waste_cost is not null
),

rms_sparse_ytd_measures as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(f.waste_cost),0) waste_cost_ytd,
       nvl(sum(f.shrinkage_selling),0) shrinkage_selling_ytd
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.fin_year_no
  and f.fin_week_no     = wl.fin_week_no
  --and f.waste_cost is not null
group by f.sk1_location_no, f.sk1_item_no
),

rms_stock as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sit_selling
from rtl_loc_item_wk_rms_stock f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no = il.sk1_item_no
and f.fin_year_no = l_last_wk_fin_year_no
and f.fin_week_no = l_last_week_no
),

rms_stock_ly as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sit_selling sit_selling_ly
from rtl_loc_item_wk_rms_stock f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no  = il.sk1_item_no
and f.fin_year_no  =  l_53_ly_fin_year_no   --WK 53 CODE--
and f.fin_week_no  =  l_53_ly_fin_week_no   --WK 53 CODE--
--and f.fin_year_no = l_last_fin_year
--and f.fin_week_no = l_last_week_no
),

weeks_no_stock as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.ll_dc_out_of_stock) weeks_out_of_stock
from rtl_depot_item_dy f, item_list il, store_list sl, eight_weeks_end_date ew, items_out_of_stock iofs
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and iofs.sk1_location_no = f.sk1_location_no
  and iofs.sk1_item_no = f.sk1_item_no
  and f.post_date = ew.calendar_date
  and f.ll_dc_out_of_stock is not null
  and f.fd_num_dc_catlg_days > 0
group by f.sk1_location_no, f.sk1_item_no
),

prom_type_set as (
select /*+ PARALLEL(prom,6) FULL(prom) */
 prom.sk1_location_no, prom.sk1_item_no,
  max(case
    when dp.Prom_type = 'MU' THEN 'MU'
  END) as promo_type_MU,

  max(case
    when dp.Prom_type = 'SK' THEN 'SK'
  END) as promo_type_SK,

  max(case
    when dp.Prom_type = 'TH' THEN 'TH'
  END) as promo_type_TH,

  max(case
    when dp.Prom_type = 'MM' THEN 'MM'
  else null
  END) as promo_type_MM,

  max(case
    when dp.prom_level_type = 'S' then 'S'
  end) as prom_level_type_s,

  max(case
    when dp.prom_level_type = 'Z' then 'Z'
  end) as prom_level_type_z
 from rtl_prom_loc_item_dy prom, store_list dl, item_list di, dim_prom dp
 where prom.Post_Date between l_last_wk_start_date and l_last_wk_end_date
 and prom.sk1_location_no = dl.sk1_location_no
 and prom.sk1_item_no = di.sk1_item_no
 and prom.sk1_prom_no = dp.sk1_prom_no
 group by prom.sk1_location_no, prom.sk1_item_no
 ),

rate_of_sale as (
--select ros.sk1_location_no, ros.sk1_item_no, ros.rate_of_sale_avg_wk, ros.rate_of_sale_sum_wk
select ros.sk1_location_no, ros.sk1_item_no, ros.avg_units_per_day rate_of_sale_avg_wk, ros.units_per_week rate_of_sale_sum_wk
  from dwh_performance.rtl_loc_item_wk_rate_of_sale ros,
       store_list dl,
       item_list di
where ros.fin_year_no = l_last_wk_fin_year_no
 and  ros.fin_week_no = l_last_week_no
 and  ros.sk1_location_no = dl.sk1_location_no
 and  ros.sk1_item_no = di.sk1_item_no
),

rate_of_sale_6wk as (
select ros.sk1_location_no, ros.sk1_item_no,
--sum(ros.rate_of_sale_sum_wk)/6 rate_of_sale_6wk_sum
sum(ros.units_per_week)/6 rate_of_sale_6wk_sum
from dwh_performance.rtl_loc_item_wk_rate_of_sale ros,
     store_list dl,
     item_list di
where ros.fin_year_no =  l_6wks_wk1_yr and ros.fin_week_no = l_6wks_wk1
--where ((ros.fin_year_no =  l_6wks_wk1_yr and ros.fin_week_no = l_6wks_wk1)
   --or (ros.fin_year_no = l_6wks_wk2_yr and ros.fin_week_no = l_6wks_wk2)
   --or (ros.fin_year_no = l_6wks_wk3_yr and ros.fin_week_no = l_6wks_wk3)
   --or (ros.fin_year_no = l_6wks_wk4_yr and ros.fin_week_no = l_6wks_wk4)
   --or (ros.fin_year_no = l_6wks_wk5_yr and ros.fin_week_no = l_6wks_wk5)
   --or (ros.fin_year_no = l_6wks_wk6_yr and ros.fin_week_no = l_6wks_wk6))
--where calendar_date between l_start_6wks_date and l_end_6wks_date
 and ros.sk1_location_no = dl.sk1_location_no
 and  ros.sk1_item_no = di.sk1_item_no
group by ros.sk1_location_no, ros.sk1_item_no
),

/*
ros_item_6_weeks as (
--selecT ros.fin_week_no, ros.sk1_item_no, max(rate_of_sale_sum_wk)  max_rate
--selecT ros.fin_week_no, ros.sk1_item_no, max(rate_of_sale_avg_wk)  max_rate
selecT ros.fin_week_no, ros.sk1_item_no, max(avg_units_per_day)  max_rate
from dwh_performance.rtl_loc_item_wk_rate_of_sale ros, item_list il, store_list sl
where ((ros.fin_year_no =  l_6wks_wk1_yr and ros.fin_week_no = l_6wks_wk1)
   or (ros.fin_year_no = l_6wks_wk2_yr and ros.fin_week_no = l_6wks_wk2)
   or (ros.fin_year_no = l_6wks_wk3_yr and ros.fin_week_no = l_6wks_wk3)
   or (ros.fin_year_no = l_6wks_wk4_yr and ros.fin_week_no = l_6wks_wk4)
   or (ros.fin_year_no = l_6wks_wk5_yr and ros.fin_week_no = l_6wks_wk5)
   or (ros.fin_year_no = l_6wks_wk6_yr and ros.fin_week_no = l_6wks_wk6))
   and ros.sk1_location_no = sl.sk1_location_no
   and ros.sk1_item_no = il.sk1_item_no
group by ros.fin_week_no, ros.sk1_item_no
order by fin_week_no) ,

ros_6wk_avg as (
select sk1_item_no, (sum(max_rate)/6) rate_of_sale_6wk_avg
from ros_item_6_weeks
group by sk1_item_no
order by sk1_item_no
),

--ros_6wk_avg_loc as (
--select sl.sk1_location_no, sk1_item_no, rate_of_sale_6wk_avg
--from store_list sl, ros_6wk_avg ros
--order by  sk1_item_no, sk1_location_no
--),
*/

sales_budget_values as (
select f.sk1_location_no, il.sk1_item_no, f.sk1_department_no,
sales_budget sales_budget_dept,
waste_cost_budget waste_cost_budget,
sales_margin_budget sales_margin_budget
from rtl_loc_dept_wk f,  store_list sl, item_list il
where f.fin_year_no     = l_last_wk_fin_year_no
and f.fin_week_no       = l_last_week_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = il.sk1_department_no
and (sales_budget <> 0 or waste_cost_budget <>0 or sales_margin_budget <>0)
order by f.sk1_location_no, il.department_no
),

sales_budget_ytd_values as (
select f.sk1_location_no, il.sk1_item_no, f.sk1_department_no,
sum(sales_budget) sales_budget_dept_ytd
from rtl_loc_dept_wk f, store_list sl, week_list wl, item_list il
where f.fin_year_no = wl.fin_year_no
and f.fin_week_no = wl.fin_week_no
and f.sk1_location_no = sl.sk1_location_no
and f.sk1_department_no = il.sk1_department_no
and f.sales_budget <> 0
group by f.sk1_location_no, il.sk1_item_no, f.sk1_department_no
),


all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,4) PARALLEL(f2,4) */

nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                           f3.sk1_location_no),
                                           f4.sk1_location_no),
                                           f6.sk1_location_no),
                                           fy0.sk1_location_no),
                                           fy1.sk1_location_no),
                                           fy2.sk1_location_no),
                                           fy3.sk1_location_no),
                                           ip1.sk1_location_no),
                                           ip2.sk1_location_no),
                                           po2.sk1_location_no),
                                           po3.sk1_location_no),
                                           ps.sk1_location_no),
                                           dly.sk1_location_no),
                                           f7.sk1_location_no),
                                           f8.sk1_location_no),
                                           stk.sk1_location_no),
                                           stk_ly.sk1_location_no),
                                           rdf.sk1_location_no),
                                           cs.sk1_location_no),
                                           f9.sk1_location_no),
                                           f16.sk1_location_no),
                                           f17.sk1_location_no),      --TEST
                                           f18.sk1_location_no),
                                           f19.sk1_location_no),
                                           f22.sk1_location_no),
                                           f23.sk1_location_no)
                                           --f24.sk1_location_no)

sk1_location_no,
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                          f3.sk1_item_no),
                                          f4.sk1_item_no),
                                          f6.sk1_item_no),
                                          fy0.sk1_item_no),
                                          fy1.sk1_item_no),
                                          fy2.sk1_item_no),
                                          fy3.sk1_item_no),
                                          ip1.sk1_item_no),
                                          ip2.sk1_item_no),
                                          po2.sk1_item_no),
                                          po3.sk1_item_no),
                                          ps.sk1_item_no),
                                          dly.sk1_item_no),
                                          f7.sk1_item_no),
                                          f8.sk1_item_no),
                                          stk.sk1_item_no),
                                          stk_ly.sk1_item_no),
                                          rdf.sk1_item_no),
                                          cs.sk1_item_no),
                                          f9.sk1_item_no),
                                          f16.sk1_item_no),
                                          f17.sk1_item_no),
                                          f18.sk1_item_no),
                                          f19.sk1_item_no),
                                          f22.sk1_item_no),
                                          f23.sk1_item_no)
                                          --f24.sk1_item_no)


sk1_item_no,
f0.this_wk_catalog_ind catalog_ind,
f0.product_status_this_week,
f0.product_status_next_week,
f2.sales,
f3.sales_ly,
f4.sales_6w_avg,
fy0.sales_ytd,
f2.sales_qty,
f3.sales_qty_ly,
dly.sales_ly_ytd,
fy0.sales_qty_ytd,
f2.sales_margin,
f3.sales_margin_ly,
fy0.sales_margin_ytd,
cs.corrected_sales,
rdf.sales_wk_app_fcst,
rdf.sales_wk_app_fcst_qty,
fy3.shorts_rands,
fy3.shorts_cases,
fy3.shorts_units,
po2.shorts_units_ytd,
po3.shorts_units_6wk_avg,
ps.prom_type,
f6.prom_sales,
f6.prom_sales_qty,
fy3.fillrate_fd_po_grn_qty,
fy3.fillrate_fd_latest_po_qty,
po2.fillrate_fd_po_grn_qty_ytd,
po2.fillrate_fd_latest_po_qty_ytd,
po3.fillrate_fd_po_grn_qty_6wk_avg,
po3.fillrate_fd_ltst_p_qty_6wk_avg,
ip1.rsp_day7,
ip2.rsp_day7_ly,

f0.fd_num_avail_days_adj,
f0.fd_num_catlg_days_adj,
fy2.fd_num_avail_days_adj_ly,
fy2.fd_num_catlg_days_adj_ly,
fy1.fd_num_avail_days_adj_ytd,
fy1.fd_num_catlg_days_adj_ytd,
f0.fd_num_avail_days,
f0.fd_num_catlg_days,
fy2.fd_num_avail_days_ly,
fy2.fd_num_catlg_days_ly,
rdf.fd_num_dc_avail_adj_days,
rdf.fd_num_dc_avail_days,
rdf.fd_num_dc_catlg_adj_days,
rdf.fd_num_dc_catlg_days,
fy1.fd_num_avail_days_ytd,
fy1.fd_num_catlg_days_ytd,
rdf.fd_num_dc_avail_adj_days_ly,
rdf.fd_num_dc_avail_days_ly,
rdf.fd_num_dc_catlg_adj_days_ly,
rdf.fd_num_dc_catlg_days_ly,

f0.boh_selling_adj,
f9.BOH_ADJ_QTY_FLT,

stk.sit_selling,
f0.soh_selling_adj_dc,
stk_ly.sit_selling_ly,
fy2.soh_selling_adj_ly_dc,

f0.fd_num_catlg_wk,
fy2.fd_num_catlg_wk_ly,
fy2.BOH_SELLING_ADJ_LY,
f0.boh_ll_adj_qty,

f6.waste_cost,
f7.waste_cost_ly,
f8.waste_cost_ytd,
rdf.SALES_WK_APP_FCST_QTY_FLT_AV,
rdf.stock_cases,
rdf.expired_stock,
rdf.expiring_stock_wk1_3,
rdf.est_ll_short_cases_av,
rdf.stock_dc_cover_cases,
rdf.DC_OUT_OF_STOCK,
f17.weeks_out_of_stock,
ps.wreward_ind,

f18.rate_of_sale_avg_wk,
f18.rate_of_sale_sum_wk,
f19.rate_of_sale_6wk_sum,
f16.promo_type_MU,
f16.promo_type_SK,
f16.promo_type_TH,
f16.promo_type_MM,
f16.prom_level_type_s,
f16.prom_level_type_z,
f0.soh_selling_adj,
fy2.soh_selling_adj_ly,
f6.shrinkage_selling,
f8.shrinkage_selling_ytd,
f22.waste_cost_budget,
f22.sales_budget_dept,
f22.sales_margin_budget,
f23.sales_budget_dept_ytd
--f24.rate_of_sale_6wk_avg

from catalog_measures f0
full outer join rms_dense_measures        f2 on f0.sk1_location_no = f2.sk1_location_no
                                           and f0.sk1_item_no     = f2.sk1_item_no

full outer join rms_dense_ly_measures     f3 on nvl(f0.sk1_location_no,f2.sk1_location_no) = f3.sk1_location_no
                                           and nvl(f0.sk1_item_no,f2.sk1_item_no)         = f3.sk1_item_no


full outer join rms_dense_6wk_measures    f4 on nvl(nvl(f0.sk1_location_no, f2.sk1_location_no),
                                                                            f3.sk1_location_no)    = f4.sk1_location_no
                                           and nvl(nvl(f0.sk1_item_no, f2.sk1_item_no),
                                                                       f3.sk1_item_no)        = f4.sk1_item_no

full outer join rms_sparse_measures f6 on nvl(nvl(nvl(f0.sk1_location_no, f2.sk1_location_no),
                                                                          f3.sk1_location_no),
                                                                          f4.sk1_location_no)    = f6.sk1_location_no
                                         and nvl(nvl(nvl(f0.sk1_item_no, f2.sk1_item_no),
                                                                         f3.sk1_item_no),
                                                                         f4.sk1_item_no)        = f6.sk1_item_no

full outer join rms_dense_ytd_measures      fy0 on nvl(nvl(nvl(nvl(f0.sk1_location_no, f2.sk1_location_no),
                                                                                       f3.sk1_location_no),
                                                                                       f4.sk1_location_no),
                                                                                       f6.sk1_location_no)          = fy0.sk1_location_no
                                           and nvl(nvl(nvl(nvl(f0.sk1_item_no,     f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no) = fy0.sk1_item_no

full outer join catalog_measures_ytd  fy1 on nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no)   = fy1.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no)      = fy1.sk1_item_no

full outer join catalog_measures_ly fy2 on nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no)         = fy2.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no)     = fy2.sk1_item_no


full outer join po_supchain_week fy3 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no)          = fy3.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no)      = fy3.sk1_item_no

 full outer join item_price_day7 ip1 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no) = ip1.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no) = ip1.sk1_item_no

 full outer join item_price_day7_ly ip2 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no) = ip2.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no) = ip2.sk1_item_no

 full outer join po_supchain_ytd po2 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no) = po2.sk1_location_no
                                      and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no) = po2.sk1_item_no

 full outer join po_6wk_avg po3 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no) = po3.sk1_location_no
                                      and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no)  = po3.sk1_item_no

 full outer join prom_values ps on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no) = ps.sk1_location_no
                                      and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no) = ps.sk1_item_no

full outer join rms_dense_ytd_measures_ly dly on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no) = dly.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no) = dly.sk1_item_no

full outer join rms_sparse_measures_ly f7 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no) = f7.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no) = f7.sk1_item_no

full outer join rms_sparse_ytd_measures f8 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no) = f8.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no) = f8.sk1_item_no

full outer join rms_stock stk on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no) = stk.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no) = stk.sk1_item_no

full outer join rms_stock_ly stk_ly on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no) = stk_ly.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no) = stk_ly.sk1_item_no

full outer join rdf_depot rdf on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no) = rdf.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no) = rdf.sk1_item_no

full outer join corrected_sales cs on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no) = cs.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no) = cs.sk1_item_no

full outer join catalog_measures1 f9 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no) = f9.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no) = f9.sk1_item_no

full outer join prom_type_set f16 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   --f9.sk1_location_no),
                                                                                   --f12.sk1_location_no),
                                                                                   --f13.sk1_location_no),
                                                                                   --f14.sk1_location_no),
                                                                                   --f15.sk1_location_no) = f16.sk1_location_no
                                                                                   f9.sk1_location_no) = f16.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   --f9.sk1_item_no),
                                                                                   --f12.sk1_item_no),
                                                                                   --f13.sk1_item_no),
                                                                                   --f14.sk1_item_no),
                                                                                   --f15.sk1_item_no) = f16.sk1_item_no
                                                                                   f0.sk1_item_no) = f16.sk1_item_no
--- TEST CODE
full outer join weeks_no_stock f17 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   --f9.sk1_location_no),
                                                                                   --f12.sk1_location_no),
                                                                                   --f13.sk1_location_no),
                                                                                   --f14.sk1_location_no),
                                                                                   --f15.sk1_location_no) = f16.sk1_location_no
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no) = f17.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   --f9.sk1_item_no),
                                                                                   --f12.sk1_item_no),
                                                                                   --f13.sk1_item_no),
                                                                                   --f14.sk1_item_no),
                                                                                   --f15.sk1_item_no) = f16.sk1_item_no
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no) = f17.sk1_item_no

full outer join rate_of_sale f18 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no) = f18.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no) = f18.sk1_item_no

full outer join rate_of_sale_6wk f19 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no) = f19.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no) =  f19.sk1_item_no

full outer join sales_budget_values f22 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no) = f22.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no) = f22.sk1_item_no

full outer join sales_budget_ytd_values f23 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f22.sk1_location_no) = f23.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f22.sk1_item_no) = f23.sk1_item_no
/*
full outer join ros_6wk_avg_loc f24 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   po2.sk1_location_no),
                                                                                   po3.sk1_location_no),
                                                                                   ps.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f7.sk1_location_no),
                                                                                   f8.sk1_location_no),
                                                                                   stk.sk1_location_no),
                                                                                   stk_ly.sk1_location_no),
                                                                                   rdf.sk1_location_no),
                                                                                   cs.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no) = f24.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   po2.sk1_item_no),
                                                                                   po3.sk1_item_no),
                                                                                   ps.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f7.sk1_item_no),
                                                                                   f8.sk1_item_no),
                                                                                   stk.sk1_item_no),
                                                                                   stk_ly.sk1_item_no),
                                                                                   rdf.sk1_item_no),
                                                                                   cs.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no) = f24.sk1_item_no
*/
)

select /*+ PARALLEL(atg,6) */
atg.sk1_location_no,
atg.sk1_item_no,
l_fin_year_no,
l_fin_week_no,
sl.location_no,
sl.location_no_and_name,
sl.loc_type,
sl.region_no,
sl.region_no_and_name,
sl.area_no,
sl.area_no_and_name,
sl.wh_fd_zone_no,
itm.item_no,
trim(itm.item_desc),
il.fd_product_no,
itm.department_no,
itm.department_no||' - '||itm.department_name department_no_and_name,
itm.subclass_no,
itm.subclass_no||' - '||itm.subclass_name subclass_no_and_name,
itm.product_class_desc_507,
itm.merchandise_category_desc_100,
atg.catalog_ind,
l_last_fin_year,
l_last_week_no,
l_next_wk_fin_week_no,
atg.product_status_this_week,
atg.product_status_next_week,
atg.sales,
atg.sales_ly,
atg.sales_ly_ytd,
atg.sales_6w_avg,
atg.sales_ytd,
atg.sales_qty,
atg.sales_qty_ly,
atg.SALES_QTY_YTD,
atg.sales_margin,
atg.sales_margin_ly,
atg.sales_margin_ytd,
atg.sales_budget_dept,                                                    --atg.sales_budget_dept,
atg.sales_budget_dept_ytd,                                                    --atg.sales_budget_dept_ytd,
0 sales_actual_dept,                                                    --atg.sales_actual_detp
0 sales_actual_dept_ytd,                                                    --atg.sales_actual_dept_ytd
atg.corrected_sales,
atg.sales_wk_app_fcst,
atg.sales_wk_app_fcst_qty,
atg.SALES_WK_APP_FCST_QTY_FLT_AV,
atg.shorts_rands,                                                    --atg.shorts_rands,
atg.shorts_cases,                                                    --atg.shorts_cases,
atg.shorts_units,
atg.shorts_units_ytd,
atg.shorts_units_6wk_avg,
atg.prom_type,
atg.prom_sales,
atg.prom_sales_qty,
atg.fillrate_fd_po_grn_qty,
atg.fillrate_fd_latest_po_qty,
atg.fillrate_fd_po_grn_qty_ytd,
atg.fillrate_fd_latest_po_qty_ytd,
atg.fillrate_fd_po_grn_qty_6wk_avg,
atg.fillrate_fd_ltst_p_qty_6wk_avg,
atg.rsp_day7,
atg.rsp_day7_ly,
atg.FD_NUM_AVAIL_DAYS_ADJ,
atg.FD_NUM_CATLG_DAYS_ADJ,
atg.FD_NUM_AVAIL_DAYS_ADJ_LY,
atg.FD_NUM_CATLG_DAYS_ADJ_LY,
atg.FD_NUM_AVAIL_DAYS_ADJ_YTD,
atg.FD_NUM_CATLG_DAYS_ADJ_YTD,
atg.FD_NUM_AVAIL_DAYS,
atg.FD_NUM_CATLG_DAYS,
atg.FD_NUM_AVAIL_DAYS_LY,
atg.FD_NUM_CATLG_DAYS_LY,
atg.FD_NUM_AVAIL_DAYS_YTD,
atg.FD_NUM_CATLG_DAYS_YTD,
atg.FD_NUM_DC_AVAIL_ADJ_DAYS,
atg.FD_NUM_DC_AVAIL_DAYS,
atg.FD_NUM_DC_CATLG_ADJ_DAYS,
atg.FD_NUM_DC_CATLG_DAYS,
atg.FD_NUM_DC_AVAIL_ADJ_DAYS_LY,
atg.FD_NUM_DC_AVAIL_DAYS_LY,
atg.FD_NUM_DC_CATLG_ADJ_DAYS_LY,
atg.FD_NUM_DC_CATLG_DAYS_LY,
atg.BOH_SELLING_ADJ ,
atg.BOH_SELLING_ADJ_LY,
atg.stock_cases,
atg.stock_dc_cover_cases,
atg.est_ll_short_cases_av ,
atg.FD_NUM_CATLG_WK,
atg.FD_NUM_CATLG_WK_LY,
atg.BOH_LL_ADJ_QTY,
atg.BOH_ADJ_QTY_FLT,

nvl(atg.SIT_SELLING,0),
nvl(atg.SOH_SELLING_ADJ_DC,0),
nvl(atg.SIT_SELLING_LY,0),
nvl(atg.SOH_SELLING_ADJ_LY_DC,0),

nvl((atg.soh_selling_adj_dc + atg.sit_selling),0),
nvl((atg.soh_selling_adj_ly_dc + atg.sit_selling_ly),0),

0 NO_OF_LINES_CATALOGUED,
0 NO_OF_LINES_CATALOGUED_LY,
0 NO_OF_STORES_CATALOGUED,
0 NO_OF_STORES_CATALOGUED_LY,
atg.WASTE_COST,
atg.WASTE_COST_LY,
atg.WASTE_COST_YTD,
atg.EXPIRED_STOCK,
atg.EXPIRING_STOCK_WK1_3,
atg.DC_OUT_OF_STOCK,
atg.WEEKS_OUT_OF_STOCK,
0,   --atg.rate_of_sale_avg_wk,
0,   --atg.rate_of_sale_sum_wk,
0,   --ros6.rate_of_sale_6wk_avg,
0,   --atg.rate_of_sale_6wk_sum,
il.no_of_weeks,
g_date last_updated_date,
atg.wreward_ind,
atg.promo_type_MU,
atg.promo_type_SK,
atg.promo_type_TH,
atg.promo_type_MM,
atg.SOH_SELLING_ADJ ,
atg.SOH_SELLING_ADJ_LY,
il.group_no,
il.group_name,
il.subgroup_no,
il.subgroup_name,
atg.prom_level_type_s,
atg.prom_level_type_z,
itm.commercial_manager_desc_562,
lfl_ly.like_for_like_ind_ly,
lfl_ty.like_for_like_ind_ty,
nvl(atg.shrinkage_selling_ytd,0),
nvl(atg.shrinkage_selling,0),
atg.waste_cost_budget,
atg.sales_margin_budget,
case when atg.rate_of_sale_sum_wk >= 0 then 1 else 0 end case,
il.PACK_ITEM_IND


from all_together atg,
     item_pclass itm,
     store_list sl,
     item_list il,
     --ros_6wk_avg ros6,
     like_for_like_ly lfl_ly,
     like_for_like_ty lfl_ty
     --sales_budget_values sb
     --sales_budget_ytd_values sbytd

where atg.sk1_item_no     = itm.sk1_item_no
  and atg.sk1_location_no = sl.sk1_location_no
  and atg.sk1_item_no     = il.sk1_item_no
  --and atg.sk1_item_no     = ros6.sk1_item_no (+)
  and atg.sk1_location_no = lfl_ly.sk1_location_no (+)
  and atg.sk1_location_no = lfl_ty.sk1_location_no (+)
  --and atg.sk1_location_no = sb.sk1_location_no
  --and itm.department_no   = sb.department_no
  --and atg.sk1_location_no = sbytd.sk1_location_no
  --and itm.department_no   = sbytd.department_no

--from all_together atg
-- join item_pclass itm on itm.sk1_item_no = atg.sk1_item_no
-- join store_list sl on sl.sk1_location_no = atg.sk1_location_no
-- join item_list il on il.sk1_item_no = atg.sk1_item_no
-- left outer join rtl_item_trading rit on atg.sk1_item_no = rit.sk1_item_no

--where atg.sk1_item_no     = itm.sk1_item_no
--  and atg.sk1_location_no = sl.sk1_location_no
--  and atg.sk1_item_no     = il.sk1_item_no
--  and atg.sk1_item_no     = rit.sk1_item_no


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

END WH_PRF_CORP_223A_1B;
