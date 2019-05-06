--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_260U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_260U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        October 2012
--  Author:      Quentin Smit
--  Purpose:     Load the C&H Parameter Location Item Week Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               On Tuesday to Sunday it loads data for CURRENT WEEK TO DATE and on a Monday this 
--               program will load data for the last completed week.
--  Tables:      Input  - rtl_loc_item_wk_rms_dense ..
--               Output - TMP_MART_CH_PARAM_LOC_ITEM_WK
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_260U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Foods Loc Item Week Dept Mart - CWD';
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

l_ly_fin_week_no        number;
l_ly_lc_wk              number;

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
        --last_wk_fin_year_no -1 ,        -- last year
        last_yr_fin_year_no,
        last_wk_fin_week_no,            -- last_completed_week
        today_fin_week_no,              -- current_week
        next_wk_fin_week_no,            -- next_week
        next_wk_fin_year_no,            -- next week's fin year
        last_wk_fin_year_no,            -- last week's fin year
        today_fin_day_no,
        this_wk_start_date,             -- start date of current week
        today_fin_week_no,      --last_wk_fin_week_no,             -- end date of 6 week period
        last_wk_start_date,
        last_wk_end_date,
        this_wk_end_date
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
             l_last_week_end_date,
             l_this_wk_end_date
from dim_control_report;

if l_last_fin_year = l_fin_year_no then
   l_last_fin_year := l_fin_year_no - 1;
end if;

select max(fin_week_no)
  into l_max_fin_week_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year;

l_text := 'l_fin_year_no = '||l_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
l_text := 'l_last_fin_year = '||l_last_fin_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_fin_week_no = '||l_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_end_6wks (1) = '||l_end_6wks ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_max_fin_week_last_year = '||l_max_fin_week_last_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_last_wk_fin_year_no = '||l_last_wk_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- Must cater for when this program runs on a Monday as then it must get data for last completed week
if l_day_no = 1 then
   if l_fin_week_no = 1 then
      l_fin_week_no := l_max_fin_week_last_year;            -- First day of new year, get last week of last fin year
      if l_max_fin_week_last_year = 53 then                  ---- ### STARTED HERE ####
         l_ly_fin_week_no := 1;                               --QST wk53
         
      else
         l_last_fin_year := l_last_fin_year - 2;               -- first day of new year, previous fin year will be - 2

         l_text := 'l_last_fin_year 1 = '||l_last_fin_year ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      end if;
      l_fin_year_no := l_last_fin_year;
      l_end_6wks :=  l_max_fin_week_last_year;
      
   else
      l_fin_week_no := l_fin_week_no -1;

      l_end_6wks := l_fin_week_no;
      l_text := 'l_end_6wks (2) = '||l_end_6wks ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'l_last_fin_year = '||l_last_fin_year ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'l_fin_year_no = '||l_fin_year_no ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
   end if;
else
   l_end_6wks := l_fin_week_no;
   l_text := 'l_end_6wks (3) = '||l_end_6wks ;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end if;

l_text := 'Week being processed:- '||l_fin_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'l_last_fin_year = '||l_last_fin_year ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'L_END_6WKS = '||l_end_6wks ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_last_week_no = '||l_last_week_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--Get date of corresponding current week last year
select calendar_date
  into l_today_date_last_year
  from dim_calendar
 where fin_year_no = l_last_fin_year
   and fin_week_no = l_fin_week_no
   and fin_day_no = l_day_no;
   

l_text := '6WKS - l_last_fin_year='||l_last_fin_year ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '6WKS - l_last_wk_fin_year_no='||l_last_wk_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '6WKS - l_fin_year_no='||l_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   
if l_end_6wks < 6 then
   select max(fin_week_no)
     into l_max_6wk_last_year
     from dim_calendar_wk
    where fin_year_no = l_last_fin_year;

   select calendar_date end_date
     into l_end_6wks_date
     from dim_calendar
    where fin_year_no = l_fin_year_no   --l_last_wk_fin_year_no
      and fin_week_no = l_end_6wks
      and fin_day_no = 7;

   --l_6wks_string := '((fin_year_no=l_last_fin_year and fin_week_no between l_start_6wks and l_max_6wk_last_year) or (fin_year_no = l_fin_year_no and fin_week_no <= l_end_6wk))';
   --############################################################################################
   -- Below is a bit long but it breaks down the year and week grouping for the 6wk calculation.
   -- This needs to be done as the string can't be used in the insert / append statement as it
   -- uses a big 'with ..'  clause.
   -- There is probably a nicer and neater way of doing by using a loop but for now this will work..
   --############################################################################################
l_text := '6WKS - l_last_wk_fin_year_no='||l_last_wk_fin_year_no ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := '6WKS - l_end_6wks_date='||l_end_6wks_date ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := '6WKS - l_last_fin_year='||l_last_fin_year ;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if l_end_6wks = 5 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  
      l_6wks_wk5_yr := l_last_wk_fin_year_no;  
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_end_6wks-4;
      l_6wks_wk6    := l_max_6wk_last_year;
   end if;
   if l_end_6wks = 4 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  
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
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  
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
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  
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
      l_6wks_wk1_yr := l_fin_year_no;     --l_last_wk_fin_year_no;   
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
       max(calendar_date) end_date
into l_last_wk_start_date, l_last_wk_end_date
from dim_calendar
where fin_year_no = l_last_wk_fin_year_no
  and fin_week_no = l_fin_week_no   --l_last_week_no
  group by fin_year_no, fin_week_no;

-- Get start and end dates of corresponding last completed week last year
  -- For CWD it must be the dates of the LAST COMPLETED WEEK LAST YEAR and not CORRESPONDING WEEK LAST YEAR
  -- So when this program is run on Tuesday onwards, the dates must not be for the correpsonding week last year
  -- but the dates of the last completed week prior to the corresponding week last year.
  -- Eg : being run on 18 June 2013 : weeking being processed is 2013 wk 52, corresponding week last year
  --      is 2012 52 HOWEVER, the last completed week last year would actually be 2012 51.
  -- this is why all the 'if' statements that follow ..

/*
if l_day_no = 1 then 
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
         and fin_week_no = l_ly_lc_wk ;    

   else
      l_ly_lc_wk := l_ly_fin_week_no - 1;
      select min(calendar_date) start_date,
         max(calendar_date) end_date
        into l_last_yr_wk_start_date, l_last_yr_wk_end_date
        from dim_calendar
       where fin_year_no = l_last_fin_year 
         and fin_week_no = l_ly_lc_wk;    
   end if;
end if;
*/

--day7_last_yr_wk
--select calendar_date
--into l_day7_last_yr_wk_date
--from dim_calendar
--where fin_year_no = l_last_fin_year
--and fin_week_no = l_ly_fin_week_no   
--and fin_day_no = 7;

--if l_day_no = 1 then
--   l_ly_current_date := l_last_yr_wk_end_date;
--else
--    l_ly_current_date :=l_today_date_last_year;
--end if;


l_text := 'current date = '|| g_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'current year = '|| l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'last year = '|| l_last_fin_year;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'last completed week = '|| l_last_week_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--if l_day_no = 1 then
--   l_text := 'current week = '|| (l_fin_week_no + 1);
-- else
   l_text := 'current week = '|| l_fin_week_no;
--end if;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'next week = '|| l_next_wk_fin_week_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'next week year = '|| l_next_wk_fin_year_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'last completed weeks year = '|| l_last_wk_fin_year_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start 6wks : year = '|| l_6wks_wk6_yr || ' week = ' ||  l_6wks_wk6;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 6wks : year = '|| l_6wks_wk1_yr || ' week = ' || l_6wks_wk1;    --l_end_6wks;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'start 6wks date = '|| l_start_6wks_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'l_day_no = '|| l_day_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_less_days = '|| l_less_days;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'end 6wks date = '|| l_end_6wks_date;
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

--l_text := 'YTD start date = '|| l_ytd_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'YTD end date = '|| l_ytd_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'start date of week being processed [NOT USED] = '|| l_last_wk_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'end date of week being processed [NOT USED] = '|| l_last_wk_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'day7 last week last year date [NOT USED] = '|| l_day7_last_yr_wk_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'this week start date  = '|| l_this_wk_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'start date of last completed week last year  = '|| l_last_yr_wk_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'end date of last completed week last year  = '|| l_last_yr_wk_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'CRA start date  = '|| l_cra_start_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--l_text := 'CRA end date  = '|| l_cra_end_date;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'YEAR USED FOR MART INSERT = '|| l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'WEEK USED FOR MART INSERT = '|| l_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_day_no := 'Moo';

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --EXECUTE IMMEDIATE('truncate table W6005682.TMP_MART_CH_PARAM_LOC_ITEM_WK');
    EXECUTE IMMEDIATE('truncate table MART_CH_PARAM_LOC_ITEM_WK');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

--INSERT /*+ APPEND */ INTO W6005682.TMP_MART_CH_PARAM_LOC_ITEM_WK mart
INSERT /*+ APPEND */ INTO MART_CH_PARAM_LOC_ITEM_WK mart
with item_list as (
select di.sk1_item_no,
       item_no,
       item_level1_no,
       item_short_desc,
       fd_product_no,
       dd.department_no,
       subclass_no,
       subclass_name,
       class_no,
       class_name,
       dd.sk1_department_no,
       dd.department_name,
       tran_ind,
       di.group_no,
       di.group_name,
       di.subgroup_no,
       di.subgroup_name,
       di.diff_1_code,
       di.sk1_merch_season_phase_no,
       dmp.merch_season_desc,
       dmp.merch_season_code,
       ds.sk1_supplier_no,
       ds.supplier_no,
       ds.supplier_long_desc
from   dim_item di,
       dim_department dd,
       dim_merch_season_phase dmp,
       dim_supplier ds
where  di.business_unit_no <> 50
and    di.sk1_department_no = dd.sk1_department_no
and    di.sk1_merch_season_phase_no = dmp.sk1_merch_season_phase_no
and    di.sk1_supplier_no = ds.sk1_supplier_no
and    di.subgroup_no = 233
and    di.rpl_ind = 1
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
and    area_no = 9951
),

item_uda as (
  select uda.sk1_item_no,
         uda.fragrance_brand_desc_1002,
         uda.fragrance_house_desc_1003,
         uda.beauty_brnd_prd_dtl_desc_2501,
         uda.event_buy_desc_331
   from dim_item_uda uda, item_list il
 where uda.sk1_item_no = il.sk1_item_no
 
),

price as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.reg_rsp
from rtl_location_item f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no = il.sk1_item_no
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

--Sales at loc item week level for last completed week
rms_dense_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       il.sk1_item_no,
       sum(nvl(f.sales_qty,0)) sales_qty
from   rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_last_wk_fin_year_no   --l_fin_year_no
and f.fin_week_no         = l_last_week_no          --l_fin_week_no
and (f.sales is not null
  or f.sales_qty is not null
  or f.sales_margin is not null)
  group by f.sk1_location_no, il.sk1_item_no
),

-- Average sales for last 6 weeks
rms_dense_6wk_measures as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       SUM(sales_qty) as sales_6wk_qty
 from  rtl_loc_item_wk_rms_dense f, item_list il, store_list sl
where f.sk1_item_no   = il.sk1_item_no
and f.sk1_location_no = sl.sk1_location_no
and ((f.fin_year_no =  l_6wks_wk1_yr and f.fin_week_no = l_6wks_wk1)
   or (f.fin_year_no = l_6wks_wk2_yr and f.fin_week_no = l_6wks_wk2)
   or (f.fin_year_no = l_6wks_wk3_yr and f.fin_week_no = l_6wks_wk3)
   or (f.fin_year_no = l_6wks_wk4_yr and f.fin_week_no = l_6wks_wk4)
   or (f.fin_year_no = l_6wks_wk5_yr and f.fin_week_no = l_6wks_wk5)
   or (f.fin_year_no = l_6wks_wk6_yr and f.fin_week_no = l_6wks_wk6))
group by f.sk1_location_no, f.sk1_item_no
),

rms_stock as (
select /*+ PARALLEL(f,4) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.soh_qty,
       f.soh_selling,                               ---QC4970
       f.inbound_incl_cust_ord_qty
from rtl_loc_item_wk_rms_stock f, item_list il, store_list sl
where f.sk1_location_no = sl.sk1_location_no
and f.sk1_item_no = il.sk1_item_no
and f.fin_year_no = l_fin_year_no
and f.fin_week_no = l_fin_week_no
),

all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,2) PARALLEL(f2,2) */

nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no)  sk1_location_no,
nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no) sk1_item_no,
f0.sales_qty,
f1.sales_6wk_qty/6 sales_6wk_avg_qty,                 ---QC4970
f2.soh_qty,
f2.soh_selling,                                       ---QC4970
f2.inbound_incl_cust_ord_qty

from rms_dense_measures                 f0
full outer join rms_dense_6wk_measures  f1 on f0.sk1_location_no  = f1.sk1_location_no
                                          and f0.sk1_item_no      = f1.sk1_item_no

full outer join rms_stock               f2 on nvl(f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
                                          and nvl(f0.sk1_item_no,f1.sk1_item_no)          = f2.sk1_item_no

--full outer join rms_stock               f3 on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
--                                          and nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no)             = f3.sk1_item_no

)

select /*+ PARALLEL(atg,4) */
atg.sk1_location_no,
atg.sk1_item_no,
l_fin_year_no,
l_fin_week_no,
sl.AREA_NO,
sl.LOCATION_NO,
sl.LOCATION_NAME,
il.DEPARTMENT_NO,
il.DEPARTMENT_NAME,
il.CLASS_NO,
il.CLASS_NAME,
il.SUBCLASS_NO,
il.SUBCLASS_NAME,
il.ITEM_LEVEL1_NO,
il.ITEM_NO,
il.ITEM_SHORT_DESC,
il.DIFF_1_CODE,
il.SK1_MERCH_SEASON_PHASE_NO,
--il.MERCH_SEASON_CODE,
il.merch_season_desc,
il.SK1_SUPPLIER_NO,
il.SUPPLIER_NO,
il.SUPPLIER_LONG_DESC,
uda.FRAGRANCE_BRAND_DESC_1002,
uda.FRAGRANCE_HOUSE_DESC_1003,
uda.BEAUTY_BRND_PRD_DTL_DESC_2501,
uda.EVENT_BUY_DESC_331,
atg.sales_qty,
atg.sales_6wk_avg_qty,                     ---QC4970
atg.SOH_QTY,
atg.INBOUND_INCL_CUST_ORD_QTY,
pr.reg_rsp,
case when atg.sales_6wk_avg_qty > 0 then   ---QC4970
  (atg.soh_qty / atg.sales_6wk_avg_qty)    ---QC4970 
 else 0 
end as soh_avg_cover,
g_date last_updated_date,
atg.soh_selling                            ---QC4970

from all_together atg,
     item_list il,
     store_list sl,
     item_uda uda,
     price pr
where atg.sk1_item_no     = il.sk1_item_no
  and atg.sk1_item_no     = uda.sk1_item_no
  and atg.sk1_location_no = sl.sk1_location_no
  and atg.sk1_location_no = pr.sk1_location_no
  and atg.sk1_item_no     = pr.sk1_item_no

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


end wh_prf_corp_260u;
