--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_739U_SHPDT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_739U_SHPDT" 
                                                                                
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  ROLLUP FOR shpd DATAFIX - wENDY - 13 SEP 2016
--**************************************************************************************************
--  Date:        May 2013
--  Author:      Quentin Smit
--  Purpose:     Create the store orders foundation record from various sources
--  Tables:      Input  - rtl_loc_item_dy_st_dir_ord
--                      - rtl_loc_item_so_sdn
--                      - rtl_loc_item_dy_so_fcst
--                      - dim_loc_item_so

----               Output - RTL_LOC_ITEM_DY_ST_ORD_FF
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_739U_SHPDT';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Foods Loc Item Week Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

G_START_DATE         DATE;
G_END_DATE         DATE;

R_FIN_DAY_NO        number;
R_TODAY_DATE       DATE;
R_TODAY_FIN_YEAR_NO        number;
R_TODAY_FIN_MONTH_NO        number;
R_TODAY_FIN_WEEK_NO        number;
R_TODAY_FIN_DAY_NO        number;
R_THIS_WK_START_DATE       DATE;
R_THIS_WK_END_DATE       DATE;
R_THIS_MN_START_DATE       DATE;
R_THIS_MN_END_DATE       DATE;
R_THIS_SEASON_NO        number;
R_THIS_SEASON_NAME  VARCHAR2(40);
R_LAST_WK_FIN_YEAR_NO        number;
R_LAST_WK_FIN_WEEK_NO        number;
R_LAST_WK_START_DATE       DATE;
R_LAST_WK_END_DATE       DATE;
R_LAST_WK_SEASON_NO        number;
R_LAST_MN_FIN_MONTH_NO        number;
R_LAST_YR_FIN_YEAR_NO        number;
R_NEXT_WK_FIN_YEAR_NO        number;
R_NEXT_WK_FIN_WEEK_NO        number;
R_YESTERDAY_DATE       DATE;
R_YESTERDAY_FIN_YEAR_NO        number;
R_YESTERDAY_FIN_WEEK_NO        number;
R_YESTERDAY_FIN_DAY_NO        number;
R_EERGISTER_DATE       DATE;
R_EERGISTER_FIN_YEAR_NO        number;
R_EERGISTER_FIN_WEEK_NO        number;
R_EERGISTER_FIN_DAY_NO        number;
R_LAST_UPDATED_DATE       DATE;
              
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
l_dc_no_stock_date  date;
l_depot_ly_date       date;
l_cover_cases_date    date;
l_today_date          date := trunc(sysdate)-1;   -- MAY NEED TO CHANGE

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

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    G_DATE := '26 SEPTEMBER 2016';
    g_date := g_date + 1;
    l_text := 'Derived ----->>>>BATCH DATE BEING PROCESSED  - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

        EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD ');
          l_text := 'TRUNCATE TABLE DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_DATAFIX.WL_TESTRTL_ST_ORD   ');
          l_text := 'TRUNCATE TABLE DWH_DATAFIX.WL_TESTRTL_ST_ORD  ';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --         l_text := 'g_start='||g_start_date||' - '||g_end_date;
 --         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- START LOOP
--**************************************************************************************************


for g_sub in 0..1 loop
--for g_sub in 0..137 loop
--for g_sub in 0..68 loop
--for g_sub in 0..2 loop
      --    g_start_date := g_start_date - 7;
      --   g_end_date := g_end_date - 7;
       --  
--**************************************************************************************************
-- SETUP DIM_CONTROL_REPORT
--**************************************************************************************************

         R_today_date               := g_date;
      
         select  fin_week_no,
         fin_year_no,
         calendar_date + (7 - fin_day_no) ,
         (calendar_date + (7 - fin_day_no)) - 6,
         season_no
         into R_last_wk_fin_week_no  ,   
         R_last_wk_fin_year_no  ,  
         R_last_wk_end_date   ,     
         R_last_wk_start_date ,    
         R_last_wk_season_no       
         from   dim_calendar
         where  calendar_date = g_date - 7;

      
         select calendar_date + (7 - fin_day_no),
         (calendar_date + (7 - fin_day_no) ) - 6
         into  R_this_wk_end_date   ,          R_this_wk_start_date   
         from   dim_calendar
         where  calendar_date = g_date;

      
         select  fin_week_no,
              fin_year_no
         into    R_next_wk_fin_week_no,
                R_next_wk_fin_year_no 
         from   dim_calendar
         where  calendar_date = g_date + 7;
      
         select fin_year_no,fin_week_no,fin_day_no
                , fin_month_no,season_no,season_name
                , CASE WHEN  fin_month_no - 1 = 0
                   THEN  12 ELSE fin_month_no - 1
                   END last_mn_fin_month_no 
               , CASE WHEN  fin_month_no - 1 = 0
                   THEN fin_year_no - 1
                   ELSE  fin_YEAR_no 
                   END last_yr_fin_year_no 
         into   R_today_fin_year_no,
                R_today_fin_week_no,
                R_today_fin_day_no,
                R_today_fin_month_no,
                R_this_season_no,
                R_this_season_name,
                R_last_mn_fin_month_no ,
                R_last_yr_fin_year_no 
         from   dim_calendar
         where  calendar_date = g_date;
      
         select calendar_date,fin_year_no,fin_week_no,fin_day_no
         into   R_yesterday_date,
                R_yesterday_fin_year_no,
                R_yesterday_fin_week_no,
                R_yesterday_fin_day_no
         from   dim_calendar
         where  calendar_date = g_date - 1;
      
         select calendar_date,fin_year_no,fin_week_no,fin_day_no
         into   R_eergister_date,
                R_eergister_fin_year_no,
                R_eergister_fin_week_no,
                R_eergister_fin_day_no
         from   dim_calendar
         where  calendar_date = g_date - 2;
      

      
         select max(calendar_date), min(calendar_date)
         into   R_this_mn_end_date,R_this_mn_start_date
         from   dim_calendar
         where  fin_month_no = R_today_fin_month_no and
                fin_year_no  = R_today_fin_year_no;
      
         R_last_updated_date := g_date;

INSERT INTO DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD
           (
            TODAY_DATE
            ,TODAY_FIN_YEAR_NO
            ,TODAY_FIN_MONTH_NO
            ,TODAY_FIN_WEEK_NO
            ,TODAY_FIN_DAY_NO
            ,THIS_WK_START_DATE
            ,THIS_WK_END_DATE
            ,THIS_MN_START_DATE
            ,THIS_MN_END_DATE
            ,THIS_SEASON_NO
            ,THIS_SEASON_NAME
            ,LAST_WK_FIN_YEAR_NO
            ,LAST_WK_FIN_WEEK_NO
            ,LAST_WK_START_DATE
            ,LAST_WK_END_DATE
            ,LAST_WK_SEASON_NO
            ,LAST_MN_FIN_MONTH_NO
            ,LAST_YR_FIN_YEAR_NO
            ,NEXT_WK_FIN_YEAR_NO
            ,NEXT_WK_FIN_WEEK_NO
            ,YESTERDAY_DATE
            ,YESTERDAY_FIN_YEAR_NO
            ,YESTERDAY_FIN_WEEK_NO
            ,YESTERDAY_FIN_DAY_NO
            ,EERGISTER_DATE
            ,EERGISTER_FIN_YEAR_NO
            ,EERGISTER_FIN_WEEK_NO
            ,EERGISTER_FIN_DAY_NO
            ,LAST_UPDATED_DATE)
      (SELECT 
              R_TODAY_DATE
              , R_TODAY_FIN_YEAR_NO
              , R_TODAY_FIN_MONTH_NO
              , R_TODAY_FIN_WEEK_NO
              , R_TODAY_FIN_DAY_NO
              , R_THIS_WK_START_DATE
              , R_THIS_WK_END_DATE
              , R_THIS_MN_START_DATE
              , R_THIS_MN_END_DATE
              , R_THIS_SEASON_NO
              , R_THIS_SEASON_NAME
              , R_LAST_WK_FIN_YEAR_NO
              , R_LAST_WK_FIN_WEEK_NO
              , R_LAST_WK_START_DATE
              , R_LAST_WK_END_DATE
              , R_LAST_WK_SEASON_NO
              , R_LAST_MN_FIN_MONTH_NO
              , R_LAST_YR_FIN_YEAR_NO
              , R_NEXT_WK_FIN_YEAR_NO
              , R_NEXT_WK_FIN_WEEK_NO
              , R_YESTERDAY_DATE
              , R_YESTERDAY_FIN_YEAR_NO
              , R_YESTERDAY_FIN_WEEK_NO
              , R_YESTERDAY_FIN_DAY_NO
              , R_EERGISTER_DATE
              , R_EERGISTER_FIN_YEAR_NO
              , R_EERGISTER_FIN_WEEK_NO
              , R_EERGISTER_FIN_DAY_NO
              , R_LAST_UPDATED_DATE
              FROM DUAL );
        g_recs_read := 0;
        g_recs_inserted :=  0;    
        g_recs_read := g_recs_read + SQL%ROWCOUNT;
        g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

          l_text := 'DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD RECS='||g_recs_inserted;
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
                  last_wk_fin_week_no--,             -- end date of 6 week period
                  --today_date
                  into l_fin_year_no,
                       l_last_fin_year,
                       l_last_week_no,
                       l_fin_week_no,
                       l_next_wk_fin_week_no,
                       l_next_wk_fin_year_no,
                       l_last_wk_fin_year_no,
                       l_day_no,
                       l_this_wk_start_date,
                       l_end_6wks--,
                       --l_today_date
          from DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD
          WHERE TODAY_FIN_DAY_NO = R_TODAY_FIN_DAY_NO;

          
          select max(fin_week_no)
            into l_max_fin_week_last_year
            from dim_calendar
           where fin_year_no = l_last_fin_year;
          
          -- l_day_no := 1;    ---XXXXXX
          
          -- Must cater for when this program runs on a Monday as then it must get data for last completed week
          if l_day_no = 1 then
             if l_fin_week_no = 1 then
                l_fin_week_no := l_max_fin_week_last_year;            -- First day of new year, get last week of last fin year
                l_last_wk_fin_year_no := l_last_wk_fin_year_no - 1;   -- First day of new year, get fin year - 1
                l_fin_year_no := l_last_fin_year;
                l_last_fin_year := l_last_fin_year - 2;               -- first day of new year, previous fin year will be - 2
             else
                l_fin_week_no := l_fin_week_no -1;
                l_last_week_no := l_last_week_no -1;
                --l_item_price_date := g_date;
             end if;
          
             -- Get start and end dates of corresponding last completed week last year
             select min(calendar_date) start_date,
                    max(calendar_date) end_date
               into l_last_yr_wk_start_date, l_last_yr_wk_end_date
               from dim_calendar
              where fin_year_no = l_last_fin_year
                and fin_week_no = l_fin_week_no;
          
             select calendar_date
               into l_this_wk_start_date
               from dim_calendar
              where fin_year_no = l_fin_year_no
               and fin_week_no = l_fin_week_no  -- + 1
               and fin_day_no = 1;
          
          else
          
             -- Get start and end dates of corresponding last completed week last year
             select min(calendar_date) start_date,
                    max(calendar_date) end_date
               into l_last_yr_wk_start_date, l_last_yr_wk_end_date
               from dim_calendar
              where fin_year_no = l_last_fin_year
                and fin_week_no = l_last_week_no;
          
             select calendar_date
               into l_this_wk_start_date
               from dim_calendar
              where fin_year_no = l_fin_year_no
               and fin_week_no = l_fin_week_no
               and fin_day_no = 1;
          
             l_end_6wks := l_fin_week_no;
          
          
          end if;
          l_text := 'Date being processed:- '||g_date ;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          
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
          
          l_text := 'current date = '|| g_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'current year = '|| l_fin_year_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'last year = '|| l_last_fin_year;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          
          l_text := 'current week = '|| l_fin_week_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          l_text := 'l_day_no = '|| l_day_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          l_text := 'this week start date  = '|| l_this_wk_start_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          
          l_text := 'today date  = '|| l_today_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  G_DATE := G_DATE - 1;        
 end loop;   
 
   

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

end wh_prf_corp_739U_SHPDT;
