--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_233T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_233T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2012
--  Author:      Quentin Smit
--  Purpose:     Create Food Location Item Week Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               For Foods weekly snapshot 1.2 for CURRENT WEEK TO DATE - runs daily
--               On a Monday this program will load data for the last completed week as the program
--               that loads data for the last completed week will have to load data for 2 weeks back (wh_prf_corp_234u)
--               DOES THE TRUNCATE SO MUST RUN FIRST!!
--  Tables:      Input  - rtl_loc_item_wk_rms_dense ..
--               Output - TMP_MART_FD_LOC_ITEM_WK_T
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

g_date               date          := trunc(sysdate);    --'18/NOV/12';  --
g_last_week          date          := trunc(sysdate) - 7;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_233T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Foods Loc Item Week Mart';
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
l_dc_no_stock_date    date;
l_depot_ly_date       date;
l_cover_cases_date    date;
l_today_date          date;
l_fin_week_code       varchar2(7 byte);

l_ly_fin_week_no      number;
l_ly_lc_wk              number;

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

 --***************************************************
-- set up some variables to be used in the program
--***************************************************
     select  today_fin_year_no,         -- current year
        --last_wk_fin_year_no -1 ,        -- last year
        last_yr_fin_year_no,
        last_wk_fin_week_no,            -- last_completed_week
        today_fin_week_no,              -- current_week
        next_wk_fin_week_no,            -- next_week
        next_wk_fin_year_no,            -- next week's fin year
        last_wk_fin_year_no,            -- last week's fin year
        today_fin_day_no,
        this_wk_start_date,             -- start date of current week
        last_wk_fin_week_no,             -- end date of 6 week period
        today_date
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
             l_today_date
from dim_control_report;

select max(fin_week_no)
  into l_max_fin_week_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year;

l_text := 'l_max_fin_week_last_year :- '||l_max_fin_week_last_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_last_fin_year :- '||l_last_fin_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_day_no :- '||l_day_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_fin_week_no :- '||l_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_last_wk_fin_year_no :- '||l_last_wk_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- l_day_no := 1;    ---XXXXXX

-- Must cater for when this program runs on a Monday as then it must get data for last completed week
if l_day_no = 1 then
   if l_fin_week_no = 1 then
      l_fin_week_no := l_max_fin_week_last_year;            -- First day of new year, get last week of last fin year
      if l_max_fin_week_last_year = 53 then                  ---- ### STARTED HERE ####
         l_ly_fin_week_no := 1;                               --QST wk53
      else
         --l_last_wk_fin_year_no := l_last_wk_fin_year_no - 1;   -- First day of new year, get fin year - 1
         --l_fin_year_no := l_last_fin_year;    --XX
         l_last_fin_year := l_last_fin_year - 1;          --2     -- first day of new year, previous fin year will be - 2
      end if;
      l_fin_year_no := l_last_fin_year;   --XXD
      --l_last_fin_year := l_last_fin_year - 2;               -- first day of new year, previous fin year will be - 2

      --l_text := 'l_last_wk_fin_year_no X:- '||l_last_wk_fin_year_no ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --l_text := 'l_last_fin_year X:- '||l_last_fin_year ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --l_text := 'l_fin_year_no X:- '||l_fin_year_no ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --l_text := 'l_fin_week_no X :- '||l_fin_week_no ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       select unique ly_fin_year_no, ly_fin_week_no                 --QST wk53
        into l_last_fin_year, l_last_week_no                        --QST wk53
        from dim_calendar                                           --QST wk53
       where fin_year_no = l_last_wk_fin_year_no   --l_fin_year_no                            --QST wk53
         and fin_week_no = l_fin_week_no;                           --QST wk53

      --l_text := 'l_last_week_no X :- '||l_last_week_no ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --l_text := 'l_last_fin_year X :- '||l_last_fin_year ;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      l_ly_fin_week_no :=  l_last_week_no;                    --QST wk53

      l_text := 'l_ly_fin_week_no X :- '||l_ly_fin_week_no ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   else
      l_fin_week_no := l_fin_week_no -1;

      select unique ly_fin_year_no, ly_fin_week_no                  --QST wk53
        into l_last_fin_year, l_last_week_no                        --QST wk53
        from dim_calendar                                           --QST wk53
       where fin_year_no = l_fin_year_no                            --QST wk53
         and fin_week_no = l_fin_week_no;                           --QST wk53

      l_ly_fin_week_no :=  l_last_week_no;                    --QST wk53
   end if;

else
   l_item_price_date := g_date;

   select unique ly_fin_year_no, ly_fin_week_no                   --QST wk53
     into l_last_fin_year, l_last_week_no                         --QST wk53
     from dim_calendar                                            --QST wk53
    where fin_year_no = l_fin_year_no                             --QST wk53
      and fin_week_no = l_fin_week_no;                            --QST wk53

   l_ly_fin_week_no :=  l_last_week_no;                           --QST wk53

   l_end_6wks := l_fin_week_no;

end if;

  -- Get start and end dates of corresponding last completed week last year
  -- Get start and end dates of corresponding last completed week last year
  -- For CWD it must be the dates of the LAST COMPLETED WEEK LAST YEAR and not CORRESPONDING WEEK LAST YEAR
  -- So when this program is run on Tuesday onwards, the dates must not be for the correpsonding week last year
  -- but the dates of the last completed week prior to the corresponding week last year.
  -- Eg : being run on 18 June 2013 : weeking being processed is 2013 wk 52, corresponding week last year
  --      is 2012 52 HOWEVER, the last completed week last year would actually be 2012 51.
  -- this is why all the 'if' statements that follow ..
  
if l_day_no = 1 then --or l_ly_fin_week_no = 1 then   --or l_fin_week_no = 1 then
    select min(calendar_date) start_date,
          max(calendar_date) end_date
   into l_last_yr_wk_start_date, l_last_yr_wk_end_date
   from dim_calendar
   where fin_year_no = l_last_fin_year
     and fin_week_no = l_ly_fin_week_no   --l_last_week_no
   ;
else
   -- Now, if it's the first week of a fin year, the dates of the last completed week will
   -- be the last week of previous previous fin year.
   -- Eg: run on 26 June 2013, week being processed is week 1 of 2013 which means last completed week of
   -- last year will be week 52 of 2011.
   if l_ly_fin_week_no = 1 then
      select max(fin_week_no)
        into l_ly_lc_wk
        from dim_calendar
       where fin_year_no = l_last_fin_year -1;
       
      select min(calendar_date) start_date,
         max(calendar_date) end_date
        into l_last_yr_wk_start_date, l_last_yr_wk_end_date
        from dim_calendar
       where fin_year_no = l_last_fin_year -1
         and fin_week_no = l_ly_lc_wk ;    --l_ly_fin_week_no -1   --l_last_week_no

   else
      l_ly_lc_wk := l_ly_fin_week_no - 1;
      select min(calendar_date) start_date,
         max(calendar_date) end_date
        into l_last_yr_wk_start_date, l_last_yr_wk_end_date
        from dim_calendar
       where fin_year_no = l_last_fin_year 
         and fin_week_no = l_ly_lc_wk;    --l_ly_fin_week_no -1   --l_last_week_no
   end if;
    
    
--  select min(calendar_date) start_date,
--          max(calendar_date) end_date
--     into l_last_yr_wk_start_date, l_last_yr_wk_end_date
--     from dim_calendar
--    where fin_year_no = l_last_fin_year
--      and fin_week_no = l_ly_fin_week_no -1   --l_last_week_no
--   group by fin_year_no, fin_week_no;
end if;
/*
if l_day_no = 1 or l_ly_fin_week_no = 1 then
   select min(calendar_date) start_date,
          max(calendar_date) end_date
   into l_last_yr_wk_start_date, l_last_yr_wk_end_date
   from dim_calendar
   where fin_year_no = l_last_fin_year
     and fin_week_no = l_ly_fin_week_no   --l_last_week_no
   ;
else
   --l_text := 'XX l_last_fin_year = '||l_last_fin_year ;
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --l_text := 'XX l_ly_fin_week_no = '||l_ly_fin_week_no ;
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   select min(calendar_date) start_date,
          max(calendar_date) end_date
     into l_last_yr_wk_start_date, l_last_yr_wk_end_date
     from dim_calendar
    where fin_year_no = l_last_fin_year
      and fin_week_no = l_ly_fin_week_no -1   --l_last_week_no
   group by fin_year_no, fin_week_no;
end if;

   select min(calendar_date) start_date,
          max(calendar_date) end_date
     into l_last_yr_wk_start_date, l_last_yr_wk_end_date
     from dim_calendar
    where fin_year_no = l_last_fin_year
      and fin_week_no = l_ly_fin_week_no;   --l_fin_week_no;   --QST wk53
*/
--l_text := 'here Y ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'l_fin_year_no Y '||l_fin_year_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'l_fin_week_no Y '||l_fin_week_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select calendar_date
     into l_this_wk_start_date
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no    --l_fin_year_no   XXXX
     and fin_week_no = l_fin_week_no  -- + 1
     and fin_day_no = 1;

--l_text := 'last year :- '||l_last_fin_year ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'last week :- '||l_last_week_no ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'last year week no:- '||l_ly_fin_week_no ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'Week being processed:- '||l_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--Get date of day 1 of corresponding current week last year and current day last year
if l_fin_week_no = 1 then
--   select calendar_date                         REMOVED THIS, NOT USED
--    into l_date_day1_last_yr
--     from dim_calendar
--    where fin_year_no = l_last_wk_fin_year_no
--      and fin_week_no = l_fin_week_no
--      and fin_day_no = 1;

   select calendar_date
     into l_today_date_last_year
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no
      and fin_week_no = l_ly_fin_week_no   --l_fin_week_no   XXX
      and fin_day_no = l_day_no;
else
--   select calendar_date
--    into l_date_day1_last_yr
--     from dim_calendar
--    where fin_year_no = l_last_fin_year
--      and fin_week_no = l_fin_week_no + 1
--      and fin_day_no = 1;

  select calendar_date
     into l_today_date_last_year
     from dim_calendar
    where fin_year_no = l_last_fin_year
      and fin_week_no = l_ly_fin_week_no   --l_fin_week_no  XXX
      and fin_day_no = l_day_no;
end if;

--l_text := 'l_end_6wks='||l_end_6wks ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

   --l_6wks_string := '((fin_year_no=l_last_fin_year and fin_week_no between l_start_6wks and l_max_6wk_last_year) or (fin_year_no = l_fin_year_no and fin_week_no <= l_end_6wk))';
   --############################################################################################
   -- Below is a bit long but it breaks down the year and week grouping for the 6wk calculation.
   -- This needs to be done as the string can't be used in the insert / append statement as it
   -- uses a big 'with ..'  clause.
   -- There is probably a nicer and neater way of doing by using a loop but for now this will work..
   --############################################################################################
--l_text := '6WKS - l_last_wk_fin_year_no='||l_last_wk_fin_year_no ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := '6WKS - l_fin_year_no='||l_fin_year_no ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := '6WKS - l_last_fin_year='||l_last_fin_year ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if l_end_6wks = 5 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk5_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_end_6wks-4;
      l_6wks_wk6    := l_max_6wk_last_year;
   end if;
   if l_end_6wks = 4 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
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
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
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
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;   --l_last_wk_fin_year_no
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
      l_6wks_wk1_yr := l_fin_year_no;   --l_last_wk_fin_year_no
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
where fin_year_no   = l_fin_year_no
  and fin_week_no  <= l_fin_week_no
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
  and fin_week_no = l_fin_week_no
  group by fin_year_no, fin_week_no;


l_text := 'l_last_fin_year='||l_last_fin_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_ly_fin_week_no='||l_ly_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--day7_last_yr_wk
select calendar_date
into l_day7_last_yr_wk_date
from dim_calendar
where fin_year_no = l_last_fin_year
and fin_week_no = l_ly_fin_week_no   --l_fin_week_no    --l_last_week_no     --- 3RD CHANGE QST
and fin_day_no = 7;

-- If Monday, the RSP check must use day 7 of last completed week
if l_day_no = 1 then
  l_item_price_date := l_last_wk_end_date;
  l_today_date_last_year:= l_day7_last_yr_wk_date;
  l_dc_no_stock_date :=  l_ytd_end_date;
  l_depot_ly_date    := l_day7_last_yr_wk_date;
  l_cover_cases_date := l_last_wk_end_date;
else
  l_dc_no_stock_date := g_date;   --l_this_wk_start_date;
  l_depot_ly_date    := g_date;
  l_cover_cases_date := l_today_date;
end if;


--l_dc_no_stock_date := '03/MAR/13';    --QST

l_text := 'current date = '|| g_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current year = '|| l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'last year = '|| l_last_fin_year;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

if l_day_no = 1 then
   l_text := 'last completed week = '|| l_fin_week_no;
else
   l_text := 'last completed week = ' || l_last_week_no;
end if;

--l_text := 'last year :- '||l_last_fin_year ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'same week LY :- '||l_ly_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current week = '|| l_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'next week = '|| l_next_wk_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'next week year = '|| l_next_wk_fin_year_no;
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

l_text := 'day7 last week last year date = '|| l_day7_last_yr_wk_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'date of same day last year (Monday will = dy7 LW last year) = ' || l_today_date_last_year;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'this week start date  = '|| l_this_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'day 1 of current week last year [NOT USED]  = '|| l_date_day1_last_yr;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start date of last completed week last year  = '|| l_last_yr_wk_start_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end date of last completed week last year  = '|| l_last_yr_wk_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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

l_text:= 'Date to be used for RSP = ' || l_item_price_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text:= 'DC availability date = ' || l_dc_no_stock_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text:= 'Depot LY Date = ' || l_depot_ly_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text:= 'Cover Cases Date = ' || l_cover_cases_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_fin_year_no := 'Mooo';

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table W6005682.TMP_MART_FD_LOC_ITEM_WK_T');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

INSERT /*+ APPEND */ INTO w6005682.TMP_MART_FD_LOC_ITEM_WK_T mart
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
--or area_no in (8700, 9953, 8800, 9952)
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
  and dcw.fin_week_no  <= l_fin_week_no
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
       uda.commercial_manager_desc_562,
       uda.shorts_longlife_desc_542,
       uda.new_line_indicator_desc_3502
from   dim_item di, dim_item_uda uda
where di.sk1_item_no=uda.sk1_item_no
  and di.business_unit_no = 50
),

like_for_like_ly as (
  select sk1_location_no,
         max(like_for_like_adj_ind) like_for_like_ind_ly
    from rtl_loc_dy
   where post_date = l_last_yr_wk_start_date
   group by sk1_location_no
),

like_for_like_ty as (
  select sk1_location_no,
         max(like_for_like_adj_ind) like_for_like_ind_ty
    from rtl_loc_dy
   where post_date = l_last_wk_start_date
   group by sk1_location_no
),

-- Items that were out of stock on the last day of last completed week
items_out_of_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.ll_dc_out_of_stock) DC_OUT_OF_STOCK,
       sum(f.stock_cases) stock_cases,
       sum(f.shelf_life_01_07) expired_stock_cases_wk1,
       sum(f.on_order_cases) on_order_cases,
       sum(f.on_order_selling) on_order_selling

from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_dc_no_stock_date    --l_last_wk_end_date
  and (f.ll_dc_out_of_stock > 0 or f.stock_cases is not null)
  group by f.sk1_location_no, f.sk1_item_no
),

rdf_fcst_measures as (
  select /*+ PARALLEL(f,4) FULL(f) */
         f.sk1_location_no,
         f.sk1_item_no,
         nvl(f.sales_wk_app_fcst,0) sales_wk_app_fcst,
         nvl(f.sales_wk_app_fcst_qty,0) sales_wk_app_fcst_qty
  from rtl_loc_item_wk_rdf_fcst f, item_list il, store_list sl
 where f.sk1_item_no     = il.sk1_item_no
   and f.sk1_location_no = sl.sk1_location_no
   and f.fin_year_no   =  l_fin_year_no  -- current week fin year
   and f.fin_week_no     = l_fin_week_no  -- current week to date
   and (f.sales_wk_app_fcst is not null
   or  f.sales_wk_app_fcst_qty is not null)
),

rdf_fcst_measures_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales_wk_app_fcst,0) sales_wk_app_fcst_ly,
       nvl(f.sales_wk_app_fcst_qty,0) sales_wk_app_fcst_qty_ly,
       nvl(f.SALES_WK_APP_FCST_QTY_FLT_AV,0) SALES_WK_APP_FCST_QTY_FT_AV_LY
from rtl_loc_item_wk_rdf_fcst f, item_list il, store_list_area sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no   =  l_last_fin_year  -- last fin year
and f.fin_week_no     = l_ly_fin_week_no    --l_last_week_no  -- last completed week    --QST wk53
and (f.SALES_WK_APP_FCST_QTY_FLT_AV is not null
     or f.sales_wk_app_fcst is not null
     or f.sales_wk_app_fcst_qty is not null)
),

rdf_fcst_measures1 as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.SALES_DLY_APP_FCST_QTY_FLT_AV,0) SALES_WK_APP_FCST_QTY_FLT_AV
from rtl_loc_item_dy_rdf_fcst f, item_list il, store_list_area sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.post_date = l_last_wk_end_date
and f.SALES_DLY_APP_FCST_QTY_FLT_AV is not null
),

--Sales at loc item week level for last completed week
rms_dense_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales,0) sales,
       nvl(f.sales_qty,0) sales_qty,
       nvl(f.sales_margin,0) sales_margin,
       nvl(f.sdn_in_cost,0) sdn_in_cost,
       nvl(f.sdn_in_qty,0) sdn_in_qty,
       nvl(f.sdn_in_selling,0) sdn_in_selling
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_fin_year_no  -- current week fin year
and f.fin_week_no         = l_fin_week_no  -- current week to date
and (f.sales is not null
  or f.sales_qty is not null
  or f.sales_margin is not null
  or f.sdn_in_cost is not null
  or f.sdn_in_qty is not null
  or f.sdn_in_selling is not null)
),

--Sales LY at loc item week level. Used to calculate S79012ales LY %. Sales LY not shown in report.
rms_dense_ly_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales sales_ly,
       f.sales_qty sales_qty_ly,
       f.sales_margin sales_margin_ly
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no   =  l_last_fin_year
and f.fin_week_no   =  l_ly_fin_week_no   --l_fin_week_no           --QST wk53
and (f.sales is not null or f.sales_qty is not null or f.sales_margin is not null)
),

-- Average sales for last 6 weeks
rms_dense_6wk_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       avg(sales_qty) sales_6w_qty_avg,
       avg(sales) sales_6w_avg
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and ((f.fin_year_no =  l_6wks_wk1_yr and f.fin_week_no = l_6wks_wk1)
   or (f.fin_year_no = l_6wks_wk2_yr and f.fin_week_no = l_6wks_wk2)
   or (f.fin_year_no = l_6wks_wk3_yr and f.fin_week_no = l_6wks_wk3)
   or (f.fin_year_no = l_6wks_wk4_yr and f.fin_week_no = l_6wks_wk4)
   or (f.fin_year_no = l_6wks_wk5_yr and f.fin_week_no = l_6wks_wk5)
   or (f.fin_year_no = l_6wks_wk6_yr and f.fin_week_no = l_6wks_wk6))
--and f.sales is not null
--and f.sales<>0
group by f.sk1_location_no, f.sk1_item_no
),

--Sales YTD at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures as (   --***
select /*+ PARALLEL(f,4) FULL(f) */
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
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.sales_qty,0)) sales_qty_ly_ytd,
       sum(nvl(f.sales,0)) sales_ly_ytd
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.ly_fin_year_no     --wl.fin_year_no-1
  and f.fin_week_no     = wl.ly_fin_week_no     --wl.fin_week_no
  and f.sales is not null
  and f.sales <>0 
  group by f.sk1_location_no, f.sk1_item_no
),

-- Item Price for day 7 of last completed week
item_price_day7 as (
select /*+ PARALLEL(pr,4) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       pr.reg_rsp rsp_day7,
       pr.prom_rsp prom_rsp_ty
from rtl_loc_item_dy_rms_price pr, item_list il, store_list sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date = l_item_price_date
--and pr.calendar_date between l_this_wk_start_date and g_date      --XX
and (reg_rsp is not null or prom_rsp is not null)
--group by pr.sk1_location_no, pr.sk1_item_no, pr.reg_rsp, pr.prom_rsp
),

-- Item price for corresponding day 7 of same week LY
item_price_day7_ly as (
select /*+ PARALLEL(pr,4) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       pr.reg_rsp rsp_day7_ly,
       pr.prom_rsp prom_rsp_ly
from rtl_loc_item_dy_rms_price pr,
item_list il, store_list sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date = l_today_date_last_year
--and pr.calendar_date between l_date_day1_last_yr and l_today_date_last_year   --XX
and (reg_rsp is not null or prom_rsp is not null)
--group by pr.sk1_location_no, pr.sk1_item_no, pr.reg_rsp, pr.prom_rsp
),

--Shorts values for last completed week
po_supchain_week as (     --XXXXX
select /*+ PARALLEL(po,4) FULL(po) */
       po.sk1_location_no,
       po.sk1_item_no,
 sum(nvl(po.shorts_selling,0)) shorts_selling ,
       sum(nvl(po.shorts_cases,0)) shorts_cases,
       sum(nvl(po.shorts_qty,0)) shorts_units,
       sum(nvl(po.fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty,
       sum(nvl(po.fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty
from rtl_supchain_loc_item_dy po, item_list il, store_list sl
where po.sk1_item_no     = il.sk1_item_no
  and po.sk1_location_no = sl.sk1_location_no
  and po.tran_date between l_this_wk_start_date and g_date
  and (po.shorts_selling is not null
   or po.shorts_cases is not null
   or po.shorts_qty is not null
   or po.fillrate_fd_po_grn_qty is not null
   or po.fillrate_fd_latest_po_qty is not null)
group by po.sk1_location_no, po.sk1_item_no
),

--Available Days, Catalogue Days and DC availability for last completed week
catalog_measures as (
select /*+ PARALLEL(f,4)  */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.fd_num_avail_days_adj,0) fd_num_avail_days_adj,
       nvl(f.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj,
       nvl(f.fd_num_avail_days,0) fd_num_avail_days,
       nvl(f.fd_num_catlg_days,0) fd_num_catlg_days,
       nvl(f.fd_num_catlg_wk,0) FD_NUM_CATLG_WK,
       case when iu.merchandise_category_desc_100 ='L' then f.boh_adj_qty else 0 end as boh_ll_adj_qty,
       nvl(f.boh_adj_selling,0) boh_adj_selling,
       f.this_wk_catalog_ind,
       nvl(ds.product_status_short_desc,0) product_status_this_week,
       nvl(ds1.product_status_short_desc,0) product_status_next_week,
       
       --case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
       case when sl.area_no = 9965 then
          f.soh_adj_selling else 0
       end as soh_selling_adj_dc,
       --case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
       case when sl.area_no = 9965 then
          f.soh_adj_qty else 0
       end as soh_adj_qty_dc,
       
       --case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
       --case when sl.area_no = 9965 then
       --   f.boh_adj_selling else 0
       --end as boh_adj_selling_dc,
       --case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
       --case when sl.area_no = 9965 then
       --   f.boh_adj_qty else 0
       --end as boh_adj_qty_dc,
       
       case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
          f.soh_adj_selling else 0
       end as soh_adj_selling_xdoc_dc,
       
       case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
          f.soh_adj_qty else 0
       end as soh_adj_qty_xdoc_dc,
       
       nvl(f.soh_adj_selling,0) soh_adj_selling,
       nvl(f.soh_adj_qty,0) soh_adj_units,
       nvl(f.boh_adj_qty,0) boh_adj_units,
       nvl(f.num_units_per_tray,0) num_units_per_tray
from   rtl_loc_item_wk_catalog f, item_list il, dim_item_uda iu, store_list sl, dim_product_status ds, dim_product_status ds1
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_item_no     = iu.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_fin_year_no
and f.fin_week_no    = l_fin_week_no
and to_char(f.product_status_1_code) = ds.product_status_code
and to_char(f.product_status_code) = ds1.product_status_code
and (f.fd_num_avail_days_adj is not null or f.fd_num_catlg_days_adj is not null
  or f.fd_num_avail_days is not null     or f.fd_num_catlg_days is not null
  or f.soh_adj_qty is not null           or f.soh_adj_qty is not null
  or f.soh_adj_selling is not null       or f.soh_adj_qty is not null)
),

catalog_measures1 as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.BOH_ADJ_QTY_FLT,0) BOH_ADJ_QTY_FLT
from   rtl_loc_item_wk_catalog f, item_list il, dim_item_uda iu, store_list_area sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_item_no     = iu.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no    = l_fin_year_no
and f.fin_week_no    = l_fin_week_no
and f.boh_adj_qty_flt is not null
),

--Catalog measures YTD
catalog_measures_ytd as (
select /*+ PARALLEL(f,4) FULL(f) */
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

catalog_dc_measures_ytd as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days_ytd,
       sum(nvl(f.fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days_ytd,
       sum(nvl(f.fd_num_dc_avail_days,0)) fd_num_dc_avail_days_ytd,
       sum(nvl(f.fd_num_dc_catlg_days,0)) fd_num_dc_catlg_days_ytd
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date between l_ytd_start_date and l_ytd_end_date
  and (f.fd_num_dc_avail_adj_days is not null
    or f.fd_num_dc_catlg_adj_days is not null)
group by f.sk1_location_no, f.sk1_item_no
),

--Waste Cost
rms_sparse_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(waste_cost),0) waste_cost,
       nvl(sum(f.prom_sales),0) prom_sales,
       nvl(sum(f.prom_sales_qty),0) prom_sales_qty,
       nvl(sum(f.shrinkage_selling),0) shrinkage_selling,
       nvl(sum(f.rtv_cost),0) rtv_cost,
       nvl(sum(f.rtv_qty),0) rtv_qty,
       nvl(sum(f.rtv_selling),0) rtv_selling,
       nvl(sum(f.claim_cost),0) claim_cost,
       nvl(sum(f.claim_qty),0) claim_qty,
       nvl(sum(f.claim_selling),0) claim_selling,
       nvl(sum(f.gain_cost),0) gain_cost,
       nvl(sum(f.gain_qty),0) gain_qty,
       nvl(sum(f.gain_selling),0) gain_selling,
       nvl(sum(f.self_supply_cost),0) self_supply_cost,
       nvl(sum(f.self_supply_qty),0) self_supply_qty,
       nvl(sum(f.self_supply_selling),0) self_supply_selling,
       nvl(sum(f.shrink_cost),0) shrink_cost,
       nvl(sum(f.shrink_qty),0) shrink_qty,
       nvl(sum(f.shrinkage_cost),0) shrinkage_cost,
       nvl(sum(f.shrinkage_qty),0) shrinkage_qty,
       nvl(sum(waste_qty),0) waste_qty,
       nvl(sum(waste_selling),0) waste_selling
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no  =   l_fin_year_no
and f.fin_week_no   =  l_fin_week_no
group by f.sk1_location_no, f.sk1_item_no
),

--Waste Cost LY
rms_sparse_measures_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(waste_cost),0) waste_cost_ly,
       nvl(sum(f.claim_cost),0) claim_cost_ly,
       nvl(sum(waste_qty),0) waste_qty_ly
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl
where f.sk1_item_no     = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no  =   l_last_fin_year
and f.fin_week_no   =  l_fin_week_no
group by f.sk1_location_no, f.sk1_item_no
),

rms_sparse_ytd_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(sum(f.waste_cost),0) waste_cost_ytd,
       nvl(sum(f.shrinkage_selling),0) shrinkage_selling_ytd,
       nvl(sum(waste_qty),0) waste_qty_ytd
from   rtl_loc_item_wk_rms_sparse f, item_list il, store_list sl, week_list wl
where f.sk1_item_no     = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     = wl.fin_year_no
  and f.fin_week_no     = wl.fin_week_no
group by f.sk1_location_no, f.sk1_item_no
),

dc_no_stock_day1 as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       --sum(f.stock_cases) stock_cases,
       --sum(f.ll_dc_out_of_stock) DC_OUT_OF_STOCK,
       sum(f.stock_dc_cover_cases) stock_dc_cover_cases,
       sum(nvl(f.fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days,
       sum(nvl(f.fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days,
       sum(nvl(f.fd_num_dc_avail_days,0)) fd_num_dc_avail_days,
       sum(nvl(f.fd_num_dc_catlg_days,0)) FD_NUM_DC_CATLG_DAYS,
       sum(shelf_life_expird * case_cost) expired_stock,
       sum(SHELF_LIFE_01_07 + SHELF_LIFE_08_14 + SHELF_LIFE_15_21) expired_stock_cases_wk1_3,
       sum((SHELF_LIFE_EXPIRD + SHELF_LIFE_01_07) * case_cost) expired_stock_wk1,
       sum(SHELF_LIFE_08_14 * case_cost) expired_stock_wk2,
       sum(SHELF_LIFE_15_21 * case_cost) expired_stock_wk3,
       sum(f.case_cost) case_cost,
       sum(SHELF_LIFE_22_28 + SHELF_LIFE_29_35 + SHELF_LIFE_36_49) expired_stock_cases_wk4_7,
       sum((SHELF_LIFE_22_28 + SHELF_LIFE_29_35 + SHELF_LIFE_36_49) * case_cost) expired_stock_wk4_7,
       sum((SHELF_LIFE_01_07 + SHELF_LIFE_08_14 + SHELF_LIFE_15_21) * case_cost) expired_stock_wk1_3

       
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_dc_no_stock_date
  --and (f.ll_dc_out_of_stock is not null
  and (f.stock_dc_cover_cases is not null
    or f.fd_num_dc_avail_adj_days is not null
    or f.fd_num_dc_catlg_adj_days is not null
    or f.fd_num_dc_catlg_days is not null
    --or f.stock_cases is not null
    or f.shelf_life_expird is not null
    or f.shelf_life_01_07 is not null
    or f.shelf_life_08_14 is not null
    or f.shelf_life_15_21 is not null
    or f.fd_num_dc_avail_days is not null)
group by f.sk1_location_no, f.sk1_item_no
),

cover_cases as (
  select /*+ PARALLEL(f,4) FULL(f) */
         f.sk1_location_no,
         f.sk1_item_no,
         f.est_ll_dc_cover_cases_av est_ll_short_cases_av
  from rtl_depot_item_dy f, item_list il, store_list sl
  where f.sk1_location_no = sl.sk1_location_no
    and f.sk1_item_no = il.sk1_item_no
    and f.post_date = l_cover_cases_date      --l_this_wk_start_date
    and il.tran_ind = 1
),

--Corrected sales for last completed week
corrected_sales as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.corr_sales,0) corrected_sales,
       nvl(f.lost_sales,0) lost_sales
from rtl_loc_item_wk_rdf_sale f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no  =   l_fin_year_no
and f.fin_week_no   =  l_fin_week_no
and f.corr_sales is not null
) ,

rms_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sit_selling,
       f.sit_qty
from rtl_loc_item_wk_rms_stock f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no = il.sk1_item_no
and f.fin_year_no = l_fin_year_no
and f.fin_week_no = l_fin_week_no
),

rms_stock_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sit_selling sit_selling_ly
from rtl_loc_item_wk_rms_stock f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no = il.sk1_item_no
and f.fin_year_no = l_last_fin_year
and f.fin_week_no = l_fin_week_no
),

-- Availability values for LY
catalog_measures_ly as (
select /*+ PARALLEL(f,4)  */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.fd_num_avail_days_adj,0) fd_num_avail_days_adj_ly,
       nvl(f.fd_num_catlg_days_adj,0) fd_num_catlg_days_adj_ly,
       nvl(f.fd_num_avail_days,0) fd_num_avail_days_ly,
       nvl(f.fd_num_catlg_days,0) fd_num_catlg_days_ly,
       nvl(f.fd_num_catlg_wk,0) fd_num_catlg_wk_ly,
       nvl(f.boh_adj_selling,0) boh_adj_selling_ly,
       case when sl.location_no in (2070,2200,3050,4000,6010,6060,6110) then
          nvl(f.soh_adj_selling,0) else 0
       end as soh_selling_adj_ly_dc,
       nvl(f.soh_adj_selling,0) soh_adj_selling_ly,
       nvl(f.boh_adj_qty,0) boh_adj_units_ly,
       nvl(f.soh_adj_qty,0) soh_adj_units_ly
from   rtl_loc_item_wk_catalog f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and f.fin_year_no   =  l_last_fin_year
and f.fin_week_no   =  l_ly_fin_week_no            -- l_fin_week_no   --QST wk53
and (f.fd_num_avail_days_adj is not null
      or f.fd_num_catlg_days_adj is not null
      or f.fd_num_avail_days is not null
      or f.fd_num_catlg_days is not null)
),

-- Depot values from day 1 of corresponding current week from last year
depot_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(nvl(f.fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days_ly,    --fd_num_dc_avail_adj_days_ly
       sum(nvl(f.fd_num_dc_avail_days,0)) fd_num_dc_avail_days_ly,
       sum(nvl(f.fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days_ly,
       sum(nvl(f.fd_num_dc_catlg_days,0)) fd_num_dc_catlg_days_ly
from rtl_depot_item_dy f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
  and f.sk1_item_no = il.sk1_item_no
  and f.post_date = l_depot_ly_date
  and (f.fd_num_dc_avail_adj_days is not null
    or f.fd_num_dc_avail_days is not null
    or f.fd_num_dc_catlg_adj_days is not null
    or f.fd_num_dc_catlg_days is not null)
 group by f.sk1_location_no, f.sk1_item_no
),

--PO values for YTD
po_supchain_ytd as (                          --f24
select /*+ PARALLEL(po,4) FULL(po) */
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

--PO values for LY
po_supchain_ly as (                           --F25
select /*+ PARALLEL(po,4) FULL(po) */
       po.sk1_location_no,
       po.sk1_item_no,
       sum(nvl(fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty_ly,
       sum(nvl(fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty_ly,
       sum(nvl(shorts_qty,0)) shorts_units_ly
from rtl_supchain_loc_item_dy po, item_list il, store_list sl --, calendar_week cw
where po.sk1_item_no   = il.sk1_item_no
and po.sk1_location_no = sl.sk1_location_no
and po.tran_date between l_last_yr_wk_start_date and l_last_yr_wk_end_date  --'27/JUN/11' and '15/APR/12'
and (fillrate_fd_po_grn_qty is not null or fillrate_fd_latest_po_qty is not null)
group by po.sk1_location_no, po.sk1_item_no--, cw.fin_week_no
),

--PO 6wk average value
po_6wk_avg as (                           --f26
select /*+ PARALLEL(f,4) FULL(f) */
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

--sales_index as (                  --f27
--select dense.sk1_location_no,
--       dense.sk1_item_no,
--       sum(dense.sales) * (1 + 
--       (case 
--       when max(ip.prom_rsp_ty) Is Null And (max(ip_ly.prom_rsp_ly) Is Null Or max(ip_ly.prom_rsp_ly) = 0) Then nvl(max(ip.rsp_day7) / max(ip_ly.rsp_day7_ly) - 1,0)
--      when max(ip.prom_rsp_ty) Is Not Null Or max(ip.prom_rsp_ty) != 0 Then nvl(max(ip.prom_rsp_ty) / max(ip_ly.rsp_day7_ly) - 1,0)
--       when max(ip.prom_rsp_ty) Is Not Null Or max(ip_ly.prom_rsp_ly) != 0 Then nvl(max(ip.rsp_day7) / max(ip_ly.prom_rsp_ly) - 1,0)
--       when max(ip.prom_rsp_ty) Is Not Null And (max(ip_ly.prom_rsp_ly) Is Not Null Or max(ip_ly.prom_rsp_ly) != 0) Then nvl(max(ip.prom_rsp_ty) / max(ip_ly.rsp_day7_ly) - 1,0)
--       else null
--       end
--       )) as Sales_Index
--  from rms_dense_measures dense, item_price_day7 ip, item_price_day7_ly ip_ly
-- where dense.sk1_item_no = ip.sk1_item_no
--   and dense.sk1_item_no = ip_ly.sk1_item_no
--   and dense.sk1_location_no = ip.sk1_location_no
--   and dense.sk1_location_no = ip_ly.sk1_location_no 
--   group by dense.sk1_location_no, dense.sk1_item_no
--),

sales_index as (
select /*+ PARALLEL(f,4) FULL(f) */ 
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales_index,
       f.PROM_RSP_TY,
       f.PROM_RSP_LY,
       f.RSP_DAY7,
       f.RSP_DAY7_LY
 from w6005682.rtl_loc_item_wk_sales_index f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
  and f.sk1_location_no = sl.sk1_location_no
  and f.fin_year_no     =  l_fin_year_no
  and f.fin_week_no     =  l_fin_week_no
  
),

weeks_no_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
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

sales_l4l as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales,0) sales_l4l
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, like_for_like_ty l4l
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_fin_year_no  -- current week fin year
and f.fin_week_no         = l_fin_week_no  -- current week to date
and f.sk1_location_no    = l4l.sk1_location_no
and l4l.like_for_like_ind_ty = 1
and f.sales is not null
),

sales_l4l_ly as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       nvl(f.sales,0) sales_l4l_ly
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl, like_for_like_ly l4l
where f.sk1_item_no           = il.sk1_item_no
and f.sk1_location_no         = sl.sk1_location_no
and f.fin_year_no             = l_last_fin_year  -- current week fin year
and f.fin_week_no             = l_fin_week_no  -- current week to date
and f.sk1_location_no         = l4l.sk1_location_no
and l4l.like_for_like_ind_ly  = 1
and f.sales is not null
),

all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,2) PARALLEL(f2,2) */

nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                          f2.sk1_location_no),
                                          f3.sk1_location_no),
                                          f6.sk1_location_no),
                                          fy0.sk1_location_no),
                                          fy3.sk1_location_no),
                                          ip1.sk1_location_no),
                                          ip2.sk1_location_no),
                                          dly.sk1_location_no),
                                          f9.sk1_location_no),
                                          f10.sk1_location_no),
                                          f11.sk1_location_no),
                                          f12.sk1_location_no),
                                          f13.sk1_location_no),
                                          f14.sk1_location_no),
                                          f15.sk1_location_no),
                                          f16.sk1_location_no),
                                          f17.sk1_location_no),
                                          f18.sk1_location_no),
                                          f19.sk1_location_no),
                                          f20.sk1_location_no),
                                          f22.sk1_location_no),
                                          f23.sk1_location_no),
                                          f24.sk1_location_no),
                                          f25.sk1_location_no),
                                          f26.sk1_location_no),
                                          f27.sk1_location_no),
                                          f28.sk1_location_no),
                                          f29.sk1_location_no),
                                          f30.sk1_location_no),
                                          f31.sk1_location_no),
                                          f32.sk1_location_no)

sk1_location_no,
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                          f2.sk1_item_no),
                                          f3.sk1_item_no),
                                          f6.sk1_item_no),
                                          fy0.sk1_item_no),
                                          fy3.sk1_item_no),
                                          ip1.sk1_item_no),
                                          ip2.sk1_item_no),
                                          dly.sk1_item_no),
                                          f9.sk1_item_no),
                                          f10.sk1_item_no),
                                          f11.sk1_item_no),
                                          f12.sk1_item_no),
                                          f13.sk1_item_no),
                                          f14.sk1_item_no),
                                          f15.sk1_item_no),
                                          f16.sk1_item_no),
                                          f17.sk1_item_no),
                                          f18.sk1_item_no),
                                          f19.sk1_item_no),
                                          f20.sk1_item_no), 
                                          f22.sk1_item_no),
                                          f23.sk1_item_no),
                                          f24.sk1_item_no),
                                          f25.sk1_item_no),
                                          f26.sk1_item_no),
                                          f27.sk1_item_no),
                                          f28.sk1_item_no),
                                          f29.sk1_item_no),
                                          f30.sk1_item_no),
                                          f31.sk1_item_no),
                                          f32.sk1_item_no)
                                    

sk1_item_no,
f0.this_wk_catalog_ind catalog_ind,
f0.product_status_this_week,
f0.product_status_next_week,
f0.boh_adj_selling,
f0.boh_adj_units,
--f0.boh_adj_selling_dc,
f0.soh_adj_qty_dc,
f0.soh_selling_adj_dc,
--f0.boh_adj_qty_dc,
f0.soh_adj_selling_xdoc_dc,
f0.soh_adj_qty_xdoc_dc,
f0.fd_num_catlg_wk,
f0.num_units_per_tray,
f2.sales,
f2.sales_qty,
f2.sales_margin,
f2.sdn_in_cost,
f2.sdn_in_qty,
f2.sdn_in_selling,
f3.sales_ly,
f3.sales_qty_ly,
f3.sales_margin_ly,
fy0.sales_ytd,
fy0.sales_qty_ytd,
fy0.sales_margin_ytd,
dly.sales_ly_ytd,
dly.sales_qty_ly_ytd,
f1.sales_wk_app_fcst,
f1.sales_wk_app_fcst_qty,
fy3.shorts_units,
fy3.shorts_selling,
fy3.shorts_cases,
fy3.fillrate_fd_po_grn_qty,
fy3.fillrate_fd_latest_po_qty,
f27.rsp_day7,         --ip1
f27.prom_rsp_ty,      --ip1
f27.rsp_day7_ly,      --ip2
f27.prom_rsp_ly,      --ip2
f6.rtv_cost,
f6.rtv_qty,
f6.rtv_selling,
f6.claim_cost,
f6.claim_qty,
f6.claim_selling,
f6.gain_cost,
f6.gain_qty,
f6.gain_selling,
f6.self_supply_cost,
f6.self_supply_qty,
f6.self_supply_selling,         --- NEW FIELD
f6.shrink_cost,
f6.shrink_qty,
f6.shrinkage_qty,
f6.shrinkage_cost,
f6.shrinkage_selling,
f6.waste_qty,
f6.waste_selling,
--f6.waste_cost_qty,
f0.fd_num_avail_days_adj,
f0.fd_num_catlg_days_adj,
f0.fd_num_avail_days,
f0.fd_num_catlg_days,
f9.fd_num_dc_avail_adj_days,
f9.fd_num_dc_catlg_adj_days,
f9.expired_stock,
f9.case_cost,
f9.expired_stock_cases_wk4_7,
f9.expired_stock_wk4_7,
f9.expired_stock_wk1_3,
f10.BOH_ADJ_QTY_FLT,
f6.waste_cost,
f11.SALES_WK_APP_FCST_QTY_FLT_AV,
f9.expired_stock_cases_wk1_3,    --expiring_stock_wk1_3,
f9.expired_stock_wk1,
f9.expired_stock_wk2,
f9.expired_stock_wk3,
f9.stock_dc_cover_cases,
f9.fd_num_dc_avail_days,
f9.fd_num_dc_catlg_days,
f0.soh_adj_selling,
f0.soh_adj_units,

f12.waste_cost_ytd,
f12.shrinkage_selling_ytd,
f12.waste_qty_ytd,
f13.est_ll_short_cases_av,
f14.fd_num_avail_days_ytd,
f14.fd_num_catlg_days_ytd,
f14.fd_num_avail_days_adj_ytd,
f14.fd_num_catlg_days_adj_ytd,
f15.fd_num_dc_avail_adj_days_ytd,
f15.fd_num_dc_catlg_adj_days_ytd,
f15.fd_num_dc_avail_days_ytd,
f15.fd_num_dc_catlg_days_ytd,
f16.DC_OUT_OF_STOCK,
f16.stock_cases,
f16.expired_stock_cases_wk1,
f16.on_order_cases,
f16.on_order_selling,

f17.corrected_sales,
f17.lost_sales,
f18.sales_6w_qty_avg,
f18.sales_6w_avg,
f19.sales_wk_app_fcst_ly,
f19.sales_wk_app_fcst_qty_ly,
f19.sales_wk_app_fcst_qty_ft_av_ly,
f20.sit_selling,
f20.sit_qty,
f22.fd_num_avail_days_adj_ly,
f22.fd_num_catlg_days_adj_ly,
f22.fd_num_avail_days_ly,
f22.fd_num_catlg_days_ly,
f22.fd_num_catlg_wk_ly,
f22.boh_adj_selling_ly,
f22.soh_selling_adj_ly_dc,
f22.soh_adj_selling_ly,
f22.boh_adj_units_ly,
f22.soh_adj_units_ly,
f23.fd_num_dc_avail_adj_days_ly,
f23.fd_num_dc_avail_days_ly,
f23.fd_num_dc_catlg_adj_days_ly,
f23.fd_num_dc_catlg_days_ly,
f24.fillrate_fd_po_grn_qty_ytd,
f24.fillrate_fd_latest_po_qty_ytd,
f24.shorts_units_ytd,
f25.fillrate_fd_po_grn_qty_ly,
f25.fillrate_fd_latest_po_qty_ly,
f26.fillrate_fd_po_grn_qty_6wk_avg,
f26.fillrate_fd_ltst_p_qty_6wk_avg,
f26.shorts_units_6wk_avg,
f27.sales_index,
f28.sit_selling_ly,
f29.claim_cost_ly,
f29.waste_cost_ly,
f29.waste_qty_ly,
f30.weeks_out_of_stock,
f31.sales_l4l,
f32.sales_l4l_ly


from catalog_measures f0
full outer join rdf_fcst_measures        f1 on f0.sk1_location_no = f1.sk1_location_no
                                           and f0.sk1_item_no     = f1.sk1_item_no

full outer join rms_dense_measures       f2 on nvl(f0.sk1_location_no,f1.sk1_location_no) = f2.sk1_location_no
                                           and nvl(f0.sk1_item_no,f1.sk1_item_no)         = f2.sk1_item_no


full outer join rms_dense_ly_measures    f3 on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                           f2.sk1_location_no)    = f3.sk1_location_no
                                           and nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                       f2.sk1_item_no)        = f3.sk1_item_no

full outer join rms_sparse_measures      f6 on nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no),
                                                                               f2.sk1_location_no),
                                                                               f3.sk1_location_no)          = f6.sk1_location_no
                                           and nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no),
                                                                           f2.sk1_item_no),
                                                                           f3.sk1_item_no) = f6.sk1_item_no

full outer join rms_dense_ytd_measures  fy0 on nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                  f2.sk1_location_no),
                                                                                  f3.sk1_location_no),
                                                                                  f6.sk1_location_no)    = fy0.sk1_location_no
                                           and nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                              f2.sk1_item_no),
                                                                              f3.sk1_item_no),
                                                                              f6.sk1_item_no)  = fy0.sk1_item_no

full outer join po_supchain_week fy3 on nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                               f2.sk1_location_no),
                                                                               f3.sk1_location_no),
                                                                               f6.sk1_location_no),
                                                                               fy0.sk1_location_no)  = fy3.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                  f2.sk1_item_no),
                                                                                  f3.sk1_item_no),
                                                                                  f6.sk1_item_no),
                                                                                  fy0.sk1_item_no)   = fy3.sk1_item_no

 full outer join item_price_day7 ip1 on nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no) = ip1.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no) = ip1.sk1_item_no

 full outer join item_price_day7_ly ip2 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no) = ip2.sk1_location_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no) = ip2.sk1_item_no

full outer join rms_dense_ytd_measures_ly dly on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no) = dly.sk1_location_no
                                 and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no) = dly.sk1_item_no

full outer join dc_no_stock_day1 f9 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no) = f9.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no) = f9.sk1_item_no

full outer join catalog_measures1 f10 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no) = f10.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no) = f10.sk1_item_no

full outer join rdf_fcst_measures1 f11 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no) = f11.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no) = f11.sk1_item_no

full outer join rms_sparse_ytd_measures f12 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no) = f12.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no) = f12.sk1_item_no

full outer join cover_cases f13 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no) = f13.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no) = f13.sk1_item_no

full outer join catalog_measures_ytd f14 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no) = f14.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no) = f14.sk1_item_no

full outer join catalog_dc_measures_ytd f15 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no) = f15.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no) = f15.sk1_item_no

full outer join items_out_of_stock f16 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no) = f16.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no) = f16.sk1_item_no

full outer join corrected_sales f17 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no) = f17.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no) = f17.sk1_item_no

full outer join rms_dense_6wk_measures f18 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no) = f18.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no) = f18.sk1_item_no

full outer join rdf_fcst_measures_ly f19 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no) = f19.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no) = f19.sk1_item_no                                                                                   
 
 full outer join rms_stock f20 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no) = f20.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no) = f20.sk1_item_no
             
full outer join catalog_measures_ly f22 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no) = f22.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no) = f22.sk1_item_no


full outer join depot_ly f23 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no) = f23.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no) = f23.sk1_item_no                                                                                   
                                                       
full outer join po_supchain_ytd f24 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no) = f24.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no) = f24.sk1_item_no 

full outer join po_supchain_ly f25 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no) = f25.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no) = f25.sk1_item_no                                                                                                                                            
          
               
full outer join po_6wk_avg f26 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no) = f26.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no) = f26.sk1_item_no

full outer join sales_index f27 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no) = f27.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no) = f27.sk1_item_no

full outer join rms_stock_ly f28 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no),
                                                                                   f27.sk1_location_no) = f28.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no),
                                                                                   f27.sk1_item_no) = f28.sk1_item_no
          
full outer join rms_sparse_measures_ly f29 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no),
                                                                                   f27.sk1_location_no),
                                                                                   f28.sk1_location_no) = f29.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no),
                                                                                   f27.sk1_item_no),
                                                                                   f28.sk1_item_no) = f29.sk1_item_no

full outer join weeks_no_stock f30 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no),
                                                                                   f27.sk1_location_no),
                                                                                   f28.sk1_location_no),
                                                                                   f29.sk1_location_no) = f30.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no),
                                                                                   f27.sk1_item_no),
                                                                                   f28.sk1_item_no),
                                                                                   f29.sk1_item_no) = f30.sk1_item_no                    

full outer join sales_l4l f31 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no),
                                                                                   f27.sk1_location_no),
                                                                                   f28.sk1_location_no),
                                                                                   f29.sk1_location_no),
                                                                                   f30.sk1_location_no) = f31.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no),
                                                                                   f27.sk1_item_no),
                                                                                   f28.sk1_item_no),
                                                                                   f29.sk1_item_no),
                                                                                   f30.sk1_item_no) = f31.sk1_item_no                    
          
full outer join sales_l4l_ly f32 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy3.sk1_location_no),
                                                                                   ip1.sk1_location_no),
                                                                                   ip2.sk1_location_no),
                                                                                   dly.sk1_location_no),
                                                                                   f9.sk1_location_no),
                                                                                   f10.sk1_location_no),
                                                                                   f11.sk1_location_no),
                                                                                   f12.sk1_location_no),
                                                                                   f13.sk1_location_no),
                                                                                   f14.sk1_location_no),
                                                                                   f15.sk1_location_no),
                                                                                   f16.sk1_location_no),
                                                                                   f17.sk1_location_no),
                                                                                   f18.sk1_location_no),
                                                                                   f19.sk1_location_no),
                                                                                   f20.sk1_location_no),
                                                                                   f22.sk1_location_no),
                                                                                   f23.sk1_location_no),
                                                                                   f24.sk1_location_no),
                                                                                   f25.sk1_location_no),
                                                                                   f26.sk1_location_no),
                                                                                   f27.sk1_location_no),
                                                                                   f28.sk1_location_no),
                                                                                   f29.sk1_location_no),
                                                                                   f30.sk1_location_no),
                                                                                   f31.sk1_location_no) = f32.sk1_location_no
                        and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy3.sk1_item_no),
                                                                                   ip1.sk1_item_no),
                                                                                   ip2.sk1_item_no),
                                                                                   dly.sk1_item_no),
                                                                                   f9.sk1_item_no),
                                                                                   f10.sk1_item_no),
                                                                                   f11.sk1_item_no),
                                                                                   f12.sk1_item_no),
                                                                                   f13.sk1_item_no),
                                                                                   f14.sk1_item_no),
                                                                                   f15.sk1_item_no),
                                                                                   f16.sk1_item_no),
                                                                                   f17.sk1_item_no),
                                                                                   f18.sk1_item_no),
                                                                                   f19.sk1_item_no),
                                                                                   f20.sk1_item_no),
                                                                                   f22.sk1_item_no),
                                                                                   f23.sk1_item_no),
                                                                                   f24.sk1_item_no),
                                                                                   f25.sk1_item_no),
                                                                                   f26.sk1_item_no),
                                                                                   f27.sk1_item_no),
                                                                                   f28.sk1_item_no),
                                                                                   f29.sk1_item_no),
                                                                                   f30.sk1_item_no),
                                                                                   f31.sk1_item_no) = f32.sk1_item_no          
          
)

select /*+ PARALLEL(atg,4) */
atg.sk1_location_no,
atg.sk1_item_no,
l_fin_year_no,
l_fin_week_no,
sl.loc_type,
sl.location_no,
sl.location_name,
itm.item_no,
trim(itm.item_desc),
itm.department_no,
trim(itm.department_name),
atg.catalog_ind,
sl.area_no,
sl.area_name,
il.fd_product_no,
il.group_no,
il.group_name,
il.subgroup_no,
il.subgroup_name,
sl.region_no,
trim(sl.region_name),
itm.merchandise_category_desc_100,
itm.product_class_desc_507,
atg.product_status_this_week,
atg.product_status_next_week,
itm.shorts_longlife_desc_542,
itm.subclass_no,
trim(itm.subclass_name),
atg.rsp_day7,
atg.rsp_day7_ly,
sl.wh_fd_zone_no,
itm.commercial_manager_desc_562,

atg.BOH_ADJ_QTY_FLT,
atg.BOH_ADJ_SELLING,
atg.BOH_ADJ_SELLING_LY,             --PA
atg.BOH_ADJ_UNITS,
atg.BOH_ADJ_UNITS_LY,               --PA
atg.CLAIM_COST,                     --PA
atg.CLAIM_COST_LY,                  --PA
atg.CLAIM_SELLING,                  --PA
atg.FD_NUM_AVAIL_DAYS,
atg.FD_NUM_AVAIL_DAYS_ADJ,
atg.FD_NUM_AVAIL_DAYS_ADJ_LY,       --PA
atg.FD_NUM_AVAIL_DAYS_ADJ_YTD,
atg.FD_NUM_AVAIL_DAYS_LY,           --PA
atg.FD_NUM_AVAIL_DAYS_YTD,
atg.FD_NUM_CATLG_DAYS,
atg.FD_NUM_CATLG_DAYS_ADJ,
atg.FD_NUM_CATLG_DAYS_ADJ_LY,       --PA
atg.FD_NUM_CATLG_DAYS_ADJ_YTD,
atg.FD_NUM_CATLG_DAYS_LY,           --PA
atg.FD_NUM_CATLG_DAYS_YTD,
atg.FD_NUM_CATLG_WK,                --PA
atg.FD_NUM_CATLG_WK_LY,             --PA
atg.FILLRATE_FD_LATEST_PO_QTY,
atg.FILLRATE_FD_LATEST_PO_QTY_LY,   --PA
atg.FILLRATE_FD_LATEST_PO_QTY_YTD,  --PA
atg.FILLRATE_FD_LTST_P_QTY_6WK_AVG, --PA
atg.FILLRATE_FD_PO_GRN_QTY,
atg.FILLRATE_FD_PO_GRN_QTY_6WK_AVG, --PA
atg.FILLRATE_FD_PO_GRN_QTY_LY,      --PA
atg.FILLRATE_FD_PO_GRN_QTY_YTD,     --PA
atg.GAIN_COST,                      --PA
atg.GAIN_QTY,                       --PA
atg.GAIN_SELLING,                   --PA
il.no_of_weeks,   
atg.NUM_UNITS_PER_TRAY,             --PA
0,   --atg.RATE_OF_SALE_SUM_WK
0,   --atg.RATE_OF_SALE_UNITS_6WK_SUM
atg.RTV_COST,
atg.RTV_QTY,
atg.RTV_SELLING,
atg.SALES,
atg.SALES_6W_QTY_AVG,
atg.SALES_6W_AVG,
atg.SALES_LY,
atg.SALES_LY_YTD,
atg.SALES_MARGIN,
atg.SALES_MARGIN_LY,
atg.SALES_MARGIN_YTD,
atg.SALES_QTY,
atg.SALES_QTY_LY,
atg.SALES_QTY_YTD,
atg.SALES_QTY_LY_YTD,
atg.SALES_YTD,
atg.SDN_IN_COST,
atg.SDN_IN_QTY,
atg.SDN_IN_SELLING,
atg.SELF_SUPPLY_COST,
atg.SELF_SUPPLY_QTY,
atg.SHORTS_CASES,
atg.SHORTS_SELLING,
atg.SHORTS_UNITS,
atg.SHORTS_UNITS_6WK_AVG,
atg.SHORTS_UNITS_YTD,
atg.SHRINKAGE_COST,
atg.SHRINKAGE_QTY,
atg.SHRINKAGE_SELLING,
atg.SHRINKAGE_SELLING_YTD,
atg.SIT_SELLING,
atg.SIT_QTY,
atg.SIT_SELLING_LY,
(atg.SIT_SELLING + atg.SOH_ADJ_SELLING_XDOC_DC) AS SIT_SOH_SELLING_ADJ,   --atg.SIT_SOH_SELLING_ADJ
(atg.SIT_SELLING + atg.SOH_ADJ_SELLING) AS SIT_SOH_SELLING_ADJ_LY,             
(atg.SIT_QTY + atg.SOH_ADJ_QTY_XDOC_DC) as SIT_SOH_QTY_ADJ,
atg.SOH_ADJ_SELLING,
atg.SOH_ADJ_SELLING_LY,                 --PA
atg.SOH_ADJ_UNITS,
atg.SOH_ADJ_UNITS_LY,                   --PA
atg.SOH_SELLING_ADJ_DC,
atg.SOH_ADJ_QTY_DC,
atg.SOH_SELLING_ADJ_LY_DC,              --PA

atg.SOH_ADJ_SELLING_XDOC_DC,
atg.SOH_ADJ_QTY_XDOC_DC,

atg.WASTE_COST,
atg.WASTE_COST_LY,                      --PA
atg.WASTE_QTY_LY,      --> waste_cost_qty               --PA
atg.WASTE_QTY_YTD,    --> waste_cost_qty_ly
atg.claim_qty,   --atg.WASTE_COST_QTY_YTD --PA
atg.WASTE_COST_YTD,
atg.WASTE_QTY,                          --PA
atg.WASTE_SELLING,                      --PA
atg.CASE_COST,                          --PA
atg.CORRECTED_SALES,
atg.DC_OUT_OF_STOCK,
atg.EST_LL_SHORT_CASES_AV,
atg.EXPIRED_STOCK,
atg.EXPIRED_STOCK_WK1,
atg.EXPIRED_STOCK_WK4_7,                --PA
atg.EXPIRED_STOCK_CASES_WK4_7,          --PA
atg.EXPIRED_STOCK_CASES_WK1_3,
atg.EXPIRED_STOCK_CASES_WK1,
atg.EXPIRED_STOCK_WK2,
atg.EXPIRED_STOCK_WK3,
atg.FD_NUM_DC_AVAIL_ADJ_DAYS,
atg.FD_NUM_DC_AVAIL_ADJ_DAYS_LY,         --PA
atg.FD_NUM_DC_AVAIL_ADJ_DAYS_YTD,
atg.FD_NUM_DC_AVAIL_DAYS,               --PA
atg.FD_NUM_DC_AVAIL_DAYS_LY,            --PA
atg.FD_NUM_DC_AVAIL_DAYS_YTD,           --PA
atg.FD_NUM_DC_CATLG_ADJ_DAYS,
atg.FD_NUM_DC_CATLG_ADJ_DAYS_LY,        --PA
atg.FD_NUM_DC_CATLG_ADJ_DAYS_YTD,
atg.FD_NUM_DC_CATLG_DAYS,               --PA
atg.FD_NUM_DC_CATLG_DAYS_LY,            --PA
atg.FD_NUM_DC_CATLG_DAYS_YTD,           --PA
atg.ON_ORDER_CASES,                     --PA
atg.ON_ORDER_SELLING,                   --PA
atg.SALES_WK_APP_FCST,
atg.SALES_WK_APP_FCST_LY,
atg.SALES_WK_APP_FCST_QTY_FLT_AV,
atg.SALES_WK_APP_FCST_QTY_FT_AV_LY,
atg.SALES_WK_APP_FCST_QTY_LY,
atg.STOCK_CASES,
atg.STOCK_DC_COVER_CASES,
atg.WEEKS_OUT_OF_STOCK,
atg.PROM_RSP_TY,
atg.PROM_RSP_LY,
atg.SHRINK_COST,
atg.SHRINK_QTY,
itm.new_line_indicator_desc_3502,
lfl_ly.like_for_like_ind_ly,
lfl_ty.like_for_like_ind_ty,
atg.lost_sales,
g_date last_updated_date,
atg.sales_index,
expired_stock_wk1_3,
0,    --atg.rate_of_sale_6wk_avg,
0,    --atg.rate_of_sale_6wk_sum,
l_fin_week_code,
l_this_wk_start_date,
atg.sales_wk_app_fcst_qty,
0,     --atg.sales_index_ly,
(atg.soh_adj_selling + atg.SIT_SELLING + atg.SOH_ADJ_SELLING_XDOC_DC) as soh_and_sit_selling_adj,   -- atg.SIT_SELLING + atg.SOH_ADJ_SELLING_XDOC_DC => sit_soh_selling_adj
(atg.soh_adj_units + atg.SIT_QTY + atg.SOH_ADJ_QTY_XDOC_DC) as soh_and_sit_qty_adj,                 -- atg.SIT_QTY + atg.SOH_ADJ_QTY_XDOC_DC => atg.sit_soh_qty_adj 
atg.sales,        --sales_l4l    using atg.sales as dummy data
atg.sales_ly,     --sales_l4l_ly using atg_sales_ly as dummy data
0,    --atg.sales_lw,
0,     --atg.sales_qty_lw => a
0,      --dc_availabilty_6wk_avg
0,      --FD_NUM_AVAIL_DAYS_LW
0,      --FD_NUM_AVAIL_DAYS_ADJ_LW
0,      --FD_NUM_CATLG_DAYS_LW
0,      --FD_NUM_CATLG_DAYS_ADJ_LW
0,      --WASTE_COST_LW
0      --WASTE_QTY_LW


from all_together atg,
     item_pclass itm,
     store_list sl,
     item_list il,
     like_for_like_ly lfl_ly,
     like_for_like_ty lfl_ty 

where atg.sk1_item_no     = itm.sk1_item_no
  and atg.sk1_location_no = sl.sk1_location_no
  and atg.sk1_item_no     = il.sk1_item_no
  and atg.sk1_location_no = lfl_ly.sk1_location_no (+)
  and atg.sk1_location_no = lfl_ty.sk1_location_no (+)
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


end wh_prf_corp_233t;
