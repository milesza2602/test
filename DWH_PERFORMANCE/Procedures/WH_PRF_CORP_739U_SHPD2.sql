--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_739U_SHPD2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_739U_SHPD2" 
                                                                                                                                                                                                                                                                                
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

----               Output - rtl_loc_item_dy_st_ord_ff
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_739U_SHPD2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Foods Loc Item Week Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

G_START_DATE         DATE;
G_END_DATE         DATE;

r_g_date               date    ;
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
    G_DATE := '27 SEPTEMBER 2016';

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


for g_sub in 0..4 loop
--for g_sub in 0..137 loop
--for g_sub in 0..68 loop
--for g_sub in 0..2 loop
      --    g_start_date := g_start_date - 7;
      --   g_end_date := g_end_date - 7;
       --  
--**************************************************************************************************
-- SETUP DIM_CONTROL_REPORT
--**************************************************************************************************

         r_g_date := g_date + 1;
         R_today_date               := r_g_date;
      
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
         where  calendar_date = r_g_date - 7;

      
         select calendar_date + (7 - fin_day_no),
         (calendar_date + (7 - fin_day_no) ) - 6
         into  R_this_wk_end_date   ,          R_this_wk_start_date   
         from   dim_calendar
         where  calendar_date = r_g_date;

      
         select  fin_week_no,
              fin_year_no
         into    R_next_wk_fin_week_no,
                R_next_wk_fin_year_no 
         from   dim_calendar
         where  calendar_date = r_g_date + 7;
      
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
         where  calendar_date = r_g_date;
      
         select calendar_date,fin_year_no,fin_week_no,fin_day_no
         into   R_yesterday_date,
                R_yesterday_fin_year_no,
                R_yesterday_fin_week_no,
                R_yesterday_fin_day_no
         from   dim_calendar
         where  calendar_date = r_g_date - 1;
      
         select calendar_date,fin_year_no,fin_week_no,fin_day_no
         into   R_eergister_date,
                R_eergister_fin_year_no,
                R_eergister_fin_week_no,
                R_eergister_fin_day_no
         from   dim_calendar
         where  calendar_date = r_g_date - 2;
      

      
         select max(calendar_date), min(calendar_date)
         into   R_this_mn_end_date,R_this_mn_start_date
         from   dim_calendar
         where  fin_month_no = R_today_fin_month_no and
                fin_year_no  = R_today_fin_year_no;
      
         R_last_updated_date := r_g_date;

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
      (SELECT distinct 
              trunc(R_TODAY_DATE)
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

          l_text := 'DWH_DATAFIX.DIM_CONTROL_REPORT_SHPD recs='||g_recs_inserted||' for '||r_g_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


 --***************************************************
-- set up some variables to be used in the program
--***************************************************
 l_today_date     := g_date;
 
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
          
          l_text := 'DATES= '|| g_date||'*'||l_today_date||'*'||l_fin_year_no||'*'||l_last_fin_year||'*'||l_fin_week_no||'*'||l_day_no||'*'||l_this_wk_start_date||'*'||l_today_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                   
          ----------------------------------------------------------------------------------------------------
          
              execute immediate 'alter session enable parallel dml';
          
          ----------------------------------------------------------------------------------------------------
 --         MERGE /*+ parallel(rtl_mart,4) */  INTO dwh_performance.rtl_loc_item_dy_st_ord_ff rtl_mart using (         
-- USE TEST TABLE

          MERGE /*+ parallel(rtl_mart,4) */  INTO DWH_DATAFIX.WL_TESTRTL_ST_ORD           rtl_mart 
          using (
          with item_list as (
          select /*+ FULL(di) */ 
                 di.sk1_item_no,
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
                  and   di.sk1_item_no = 15995248
          ),
          
          store_list as (
          select a.sk1_location_no,
                 a.location_no,
                 a.location_no || ' ' || a.location_name location_no_and_name,
                 a.location_name,
                 a.region_no,
                 a.region_no || ' ' || a.region_name region_no_and_name,
                 a.region_name,
                 a.area_no,
                 a.area_no || ' ' || a.area_name area_no_and_name,
                 a.area_name,
                 a.wh_fd_zone_no,
                 a.chain_no,
                 a.loc_type,
                 b.NUM_STORE_LEADTIME_DAYS
          from   dim_location a, fnd_location b
          where  a.location_no = b.location_no
                  and     a.sk1_location_no = 10722	
          --where  chain_no = 10
          --or area_no in (8700, 9953, 8800, 9952)
          ),
          
          ff_st_dir_order as (
          select /*+ full(f) parallel(f,4) */ 
                 f.SK1_LOCATION_NO,
                 f.SK1_ITEM_NO,
                 f.POST_DATE,
                 f.BOH_1_QTY,
                 f.BOH_1_IND,
                 f.BOH_2_QTY,
                 f.BOH_3_QTY,
                 f.STORE_ORDER1,
                 f.STORE_ORDER2,
                 f.STORE_ORDER3,
                 f.SAFETY_QTY,
                 f.SPECIAL_CASES,
                 f.FORECAST_CASES,
                 f.SAFETY_CASES,
                 f.OVER_CASES,
                 f.TRADING_DATE,
                 f.DIRECT_MU_QTY1,
                 f.DIRECT_MU_QTY2,
                 f.DIRECT_MU_QTY3,
                 f.DIRECT_MU_QTY4,
                 f.DIRECT_MU_QTY5,
                 f.DIRECT_MU_QTY6,
                 f.DIRECT_MU_QTY7
           from dwh_performance.rtl_loc_item_dy_st_dir_ord f,
                item_list il,
                store_list sl
           where f.post_date >= l_today_date         ---QST was = l_today_date
            and f.sk1_location_no = sl.sk1_location_no
            and f.sk1_item_no = il.sk1_item_no
        and     (f.sk1_location_no = 10722	 and f.sk1_item_no = 15995248	and f.post_date = '27/SEP/16')
          ),   -- select * from ff_st_dir_order;
          
          so_sdn_shipments as (
            select /*+ full(f) parallel(f,4) */ f.sk1_location_no,
                   f.sk1_item_no,
                   --f.post_date,
                   f.RECEIVE_DATE,
                   f.TO_LOC_NO,
                   f.RECEIVED_QTY,
                   f.REG_RSP,
                   f.SDN_QTY
              from rtl_loc_item_so_sdn f, store_list sl,
                   item_list il
             where f.receive_date = l_today_date
               and f.sk1_item_no  = il.sk1_item_no
               and f.sk1_location_no    = sl.sk1_location_no
                       and     (f.sk1_location_no = 10722	 and f.sk1_item_no = 15995248	and f.receive_date = '27/SEP/16')
          ),  -- select * from so_sdn_shipments;
          
          rdf_fcst_measures as (
            select /*+ FULL(f) PARALLEL(f,4) */
                   f.sk1_location_no,
                   f.sk1_item_no,
                   f.post_date,
                   f.DAY1_ESTIMATE,
                   f.DAY2_ESTIMATE,
                   f.DAY3_ESTIMATE,
                   f.WEEKLY_ESTIMATE1,
                   f.WEEKLY_ESTIMATE2,
                   f.DAY4_ESTIMATE,
                   f.DAY5_ESTIMATE,
                   f.DAY6_ESTIMATE,
                   f.DAY7_ESTIMATE,
                   f.DAY1_EST_UNIT2,
                   f.DAY2_EST_UNIT2,
                   f.DAY3_EST_UNIT2,
                   f.DAY4_EST_UNIT2,
                   f.DAY5_EST_UNIT2,
                   f.DAY6_EST_UNIT2,
                   f.DAY7_EST_UNIT2
            from dwh_performance.rtl_loc_item_dy_so_fcst f, item_list il, store_list sl
            --from dwh_performance.rtl_loc_item_dy_so_fcst
           where f.sk1_item_no     = il.sk1_item_no
             and f.sk1_location_no = sl.sk1_location_no
             and f.post_date   between l_today_date and l_today_date + 1   --  l_today_date  
                     and     (f.sk1_location_no = 10722	 and f.sk1_item_no = 15995248	and f.post_date = '27/SEP/16')
             --and f.post_date   =  l_today_date
          ),  -- select * from rdf_fcst_measures;
          
          
          all_together as (
          --Joining all temp data sets into final result set
          select /*+ PARALLEL(f0,4) PARALLEL(f2,4) */
          
          nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),f2.sk1_location_no)
          sk1_location_no,
          nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),f2.sk1_item_no)
          sk1_item_no,
          nvl(nvl(f0.post_date,f1.receive_date),f2.post_date)
          post_date,
          
          f0.BOH_1_QTY,
          f0.BOH_1_IND,
          f0.BOH_2_QTY,
          f0.BOH_3_QTY,
          f0.STORE_ORDER1,
          f0.STORE_ORDER2,
          f0.STORE_ORDER3,
          f0.SAFETY_QTY,
          f0.SPECIAL_CASES,
          f0.FORECAST_CASES,
          f0.SAFETY_CASES,
          f0.OVER_CASES,
          f0.TRADING_DATE,
          f0.DIRECT_MU_QTY1,
          f0.DIRECT_MU_QTY2,
          f0.DIRECT_MU_QTY3,
          f0.DIRECT_MU_QTY4,
          f0.DIRECT_MU_QTY5,
          f0.DIRECT_MU_QTY6,
          f0.DIRECT_MU_QTY7,
          
          f1.RECEIVE_DATE,
          f1.TO_LOC_NO,
          f1.RECEIVED_QTY,
          f1.REG_RSP,
          f1.SDN_QTY,
          
          f2.DAY1_ESTIMATE,
          f2.DAY2_ESTIMATE,
          f2.DAY3_ESTIMATE,
          f2.WEEKLY_ESTIMATE1,
          f2.WEEKLY_ESTIMATE2,
          f2.DAY4_ESTIMATE,
          f2.DAY5_ESTIMATE,
          f2.DAY6_ESTIMATE,
          f2.DAY7_ESTIMATE,
          f2.DAY1_EST_UNIT2,
          f2.DAY2_EST_UNIT2,
          f2.DAY3_EST_UNIT2,
          f2.DAY4_EST_UNIT2,
          f2.DAY5_EST_UNIT2,
          f2.DAY6_EST_UNIT2,
          f2.DAY7_EST_UNIT2
          
          from ff_st_dir_order f0
          full outer join so_sdn_shipments     f1 on f0.post_date                             = f1.receive_date
                                                 and f0.sk1_location_no                       = f1.sk1_location_no
                                                 and f0.sk1_item_no                           = f1.sk1_item_no
          
          full outer join rdf_fcst_measures   f2 on nvl(f0.post_date, f1.receive_date)            = f2.post_date
                                                and nvl(f0.sk1_location_no,f1.sk1_location_no) = f2.sk1_location_no
                                                and nvl(f0.sk1_item_no,f1.sk1_item_no)         = f2.sk1_item_no
          
          
          )
          
          select /*+ PARALLEL(atg,4) full(its) full(dih) full(dlh) */
          atg.sk1_location_no,
          atg.sk1_item_no,
          atg.post_date,
          dlh.sk2_location_no,
          dih.sk2_item_no,
          
          ' ' as DEPT_TYPE,
          case when DIRECT_MU_QTY1 > 0 or DIRECT_MU_QTY2 > 0 or DIRECT_MU_QTY3 > 0 or DIRECT_MU_QTY4 > 0  --if source is direct order then = 1 else 0
                 or DIRECT_MU_QTY5 > 0 or DIRECT_MU_QTY6 > 0 or DIRECT_MU_QTY7 > 0 then 1 else 0
           end as DIRECT_DELIVERY_IND,
          
          sl.NUM_STORE_LEADTIME_DAYS,
          atg.BOH_1_QTY,
          atg.BOH_1_IND,
          atg.BOH_2_QTY,
          atg.BOH_3_QTY,
          atg.received_qty as SDN_1_QTY,
          CASE when atg.received_qty > 0 then 1 else 0 end as SDN_1_IND,
          '0' as SDN2_QTY,
          '0' as SDN2_IND,
          '0' as SHORT_QTY,                    -- HERE
          atg.DAY1_ESTIMATE,
          atg.DAY2_ESTIMATE,
          atg.DAY3_ESTIMATE,
          atg.SAFETY_QTY,
          its.MODEL_STOCK,
          atg.STORE_ORDER1,
          atg.STORE_ORDER2,
          atg.STORE_ORDER3,
          --its.DELIVERY_PATTERN,
          to_number(translate(decode(nvl(its.DELIVERY_PATTERN,'0'),' ','0',nvl(its.DELIVERY_PATTERN,'0')), 'YNO0', '1322')) as    delivery_pattern,
          its.NUM_UNITS_PER_TRAY,
          atg.WEEKLY_ESTIMATE1,
          atg.WEEKLY_ESTIMATE2,
          its.SHELF_LIFE,
          atg.TRADING_DATE,
          --its.PROD_STATUS_1,
          --its.PROD_STATUS_2,
          ( Case
                    when its.PROD_STATUS_1 = 'A' then 1
                    when its.PROD_STATUS_1 = 'D' then 4
                    when its.PROD_STATUS_1 = 'N' then 14
                    when its.PROD_STATUS_1 = 'O' then 15
                    when its.PROD_STATUS_1 = 'U' then 21
                    when its.PROD_STATUS_1 = 'X' then 24
                    when its.PROD_STATUS_1 = 'Z' then 26
                    when its.PROD_STATUS_1 IS NULL then 0
                    else 0
                    end )
                         as   prod_status_1,
                   ( Case
                    when its.PROD_STATUS_2 = 'A' then 1
                    when its.PROD_STATUS_2 = 'D' then 4
                    when its.PROD_STATUS_2 = 'N' then 14
                    when its.PROD_STATUS_2 = 'O' then 15
                    when its.PROD_STATUS_2 = 'U' then 21
                    when its.PROD_STATUS_2 = 'X' then 24
                    when its.PROD_STATUS_2 = 'Z' then 26
                    when its.PROD_STATUS_2 IS NULL then 0
                    else 0
                    end )
                         as   prod_status_2,
          atg.DIRECT_MU_QTY1,
          atg.DIRECT_MU_QTY2,
          atg.DIRECT_MU_QTY3,
          atg.DIRECT_MU_QTY4,
          atg.DIRECT_MU_QTY5,
          atg.DIRECT_MU_QTY6,
          atg.DIRECT_MU_QTY7,
          case when atg.DIRECT_MU_QTY1 > 0 then 1 else 0 end as DIRECT_MU_IND1,
          case when atg.DIRECT_MU_QTY2 > 0 then 1 else 0 end as DIRECT_MU_IND2,
          case when atg.DIRECT_MU_QTY3 > 0 then 1 else 0 end as DIRECT_MU_IND3,
          case when atg.DIRECT_MU_QTY4 > 0 then 1 else 0 end as DIRECT_MU_IND4,
          case when atg.DIRECT_MU_QTY5 > 0 then 1 else 0 end as DIRECT_MU_IND5,
          case when atg.DIRECT_MU_QTY6 > 0 then 1 else 0 end as DIRECT_MU_IND6,
          case when atg.DIRECT_MU_QTY7 > 0 then 1 else 0 end as DIRECT_MU_IND7,
          atg.DAY4_ESTIMATE,
          atg.DAY5_ESTIMATE,
          atg.DAY6_ESTIMATE,
          atg.DAY7_ESTIMATE,
          its.DAY1_EST_VAL2,
          its.DAY2_EST_VAL2,
          its.DAY3_EST_VAL2,
          its.DAY4_EST_VAL2,
          its.DAY5_EST_VAL2,
          its.DAY6_EST_VAL2,
          its.DAY7_EST_VAL2,
          atg.DAY1_EST_UNIT2,
          atg.DAY2_EST_UNIT2,
          atg.DAY3_EST_UNIT2,
          atg.DAY4_EST_UNIT2,
          atg.DAY5_EST_UNIT2,
          atg.DAY6_EST_UNIT2,
          atg.DAY7_EST_UNIT2,
          its.NUM_UNITS_PER_TRAY2,
          --(its.MODEL_STOCK/ its.num_units_per_tray) as STORE_MODEL_STOCK,
          case when its.num_units_per_tray > 0 then (its.MODEL_STOCK/ its.num_units_per_tray) else 0 end as STORE_MODEL_STOCK,
          --its.DAY1_DELIV_PAT1,
          --its.DAY2_DELIV_PAT1,
          --its.DAY3_DELIV_PAT1,
          --its.DAY4_DELIV_PAT1,
          --its.DAY5_DELIV_PAT1,
          --its.DAY6_DELIV_PAT1,
          --its.DAY7_DELIV_PAT1,
          to_number(translate(decode(nvl(its.DAY1_DELIV_PAT1,'0'),' ','0',nvl(its.DAY1_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day1_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY2_DELIV_PAT1,'0'),' ','0',nvl(its.DAY2_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day2_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY3_DELIV_PAT1,'0'),' ','0',nvl(its.DAY3_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day3_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY4_DELIV_PAT1,'0'),' ','0',nvl(its.DAY4_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day4_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY5_DELIV_PAT1,'0'),' ','0',nvl(its.DAY5_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day5_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY6_DELIV_PAT1,'0'),' ','0',nvl(its.DAY6_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day6_deliv_pat1,
          to_number(translate(decode(nvl(its.DAY7_DELIV_PAT1,'0'),' ','0',nvl(its.DAY7_DELIV_PAT1,'0')), 'YNO0', '1322'))
                      as day7_deliv_pat1,
          
          g_date as LAST_UPDATED_DATE,
          nvl((atg.store_order2 + atg.safety_qty),0) as SCANNED_MODEL_STOCK_QTY,
          il.item_no,
          sl.location_no
          
          
          from all_together atg,
               store_list sl,
               item_list il,
               dim_loc_item_so its,
               dim_item_hist dih,
               dim_location_hist dlh
          
          where atg.sk1_location_no = sl.sk1_location_no
            and atg.sk1_item_no     = il.sk1_item_no
            and atg.sk1_location_no = its.sk1_location_no    -- (+)
            and atg.sk1_item_no     = its.sk1_item_no        -- (+)
            and il.item_no          = dih.item_no
            and atg.post_date   between dih.sk2_active_from_date and dih.sk2_active_to_date
            and sl.location_no      = dlh.location_no
            and atg.post_date   between dlh.sk2_active_from_date and dlh.sk2_active_to_date
            --AND atg.sk1_location_no = 9342 --and atg.sk1_item_no = 19533 and atg.post_date = '27/DEC/13'
          
           )   mer_mart
           ON  (mer_mart.sk1_item_no      = rtl_mart.sk1_item_no
          and   mer_mart.sk1_location_no  = rtl_mart.sk1_location_no
          and   mer_mart.post_date        = rtl_mart.post_date
          
          )
          
          WHEN MATCHED THEN
            --l_text := 'MATCH FOUND - UPDATE !!'|| l_today_date;
            --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          UPDATE
          SET   SK2_LOCATION_NO	          = mer_mart.SK2_LOCATION_NO,
                SK2_ITEM_NO	              = mer_mart.SK2_ITEM_NO,
                DEPT_TYPE	                = mer_mart.DEPT_TYPE,
                DIRECT_DELIVERY_IND	      = mer_mart.DIRECT_DELIVERY_IND,
                NUM_STORE_LEADTIME_DAYS	  = mer_mart.NUM_STORE_LEADTIME_DAYS,
                BOH_1_QTY	                = mer_mart.BOH_1_QTY,
                BOH_1_IND	                = mer_mart.BOH_1_IND,
                BOH_2_QTY	                = mer_mart.BOH_2_QTY,
                BOH_3_QTY	                = mer_mart.BOH_3_QTY,
                SDN_1_QTY	                = mer_mart.SDN_1_QTY,
                SDN1_IND	                = mer_mart.SDN_1_IND,
                SDN2_QTY	                = mer_mart.SDN2_QTY,
                SDN2_IND	                = mer_mart.SDN2_IND,
                SHORT_QTY	                = mer_mart.SHORT_QTY,
                DAY1_ESTIMATE	            = mer_mart.DAY1_ESTIMATE,
                DAY2_ESTIMATE	            = mer_mart.DAY2_ESTIMATE,
                DAY3_ESTIMATE	            = mer_mart.DAY3_ESTIMATE,
                SAFETY_QTY	              = mer_mart.SAFETY_QTY,
                MODEL_STOCK	              = mer_mart.MODEL_STOCK,
                STORE_ORDER1	            = mer_mart.STORE_ORDER1,
                STORE_ORDER2	            = mer_mart.STORE_ORDER2,
                STORE_ORDER3	            = mer_mart.STORE_ORDER3,
                DELIVERY_PATTERN	        = mer_mart.DELIVERY_PATTERN,
                NUM_UNITS_PER_TRAY	      = mer_mart.NUM_UNITS_PER_TRAY,
                WEEKLY_ESTIMATE1	        = mer_mart.WEEKLY_ESTIMATE1,
                WEEKLY_ESTIMATE2	        = mer_mart.WEEKLY_ESTIMATE2,
                SHELF_LIFE	              = mer_mart.SHELF_LIFE,
                TRADING_DATE	            = mer_mart.TRADING_DATE,
                PROD_STATUS_1	            = mer_mart.PROD_STATUS_1,
                PROD_STATUS_2	            = mer_mart.PROD_STATUS_2,
                DIRECT_MU_QTY1	          = mer_mart.DIRECT_MU_QTY1,
                DIRECT_MU_QTY2	          = mer_mart.DIRECT_MU_QTY2,
                DIRECT_MU_QTY3	          = mer_mart.DIRECT_MU_QTY3,
                DIRECT_MU_QTY4	          = mer_mart.DIRECT_MU_QTY4,
                DIRECT_MU_QTY5	          = mer_mart.DIRECT_MU_QTY5,
                DIRECT_MU_QTY6	          = mer_mart.DIRECT_MU_QTY6,
                DIRECT_MU_QTY7	          = mer_mart.DIRECT_MU_QTY7,
                DIRECT_MU_IND1	          = mer_mart.DIRECT_MU_IND1,
                DIRECT_MU_IND2	          = mer_mart.DIRECT_MU_IND2,
                DIRECT_MU_IND3	          = mer_mart.DIRECT_MU_IND3,
                DIRECT_MU_IND4	          = mer_mart.DIRECT_MU_IND4,
                DIRECT_MU_IND5	          = mer_mart.DIRECT_MU_IND5,
                DIRECT_MU_IND6	          = mer_mart.DIRECT_MU_IND6,
                DIRECT_MU_IND7	          = mer_mart.DIRECT_MU_IND7,
                DAY4_ESTIMATE	            = mer_mart.DAY4_ESTIMATE,
                DAY5_ESTIMATE	            = mer_mart.DAY5_ESTIMATE,
                DAY6_ESTIMATE	            = mer_mart.DAY6_ESTIMATE,
                DAY7_ESTIMATE	            = mer_mart.DAY7_ESTIMATE,
                DAY1_EST_VAL2	            = mer_mart.DAY1_EST_VAL2,
                DAY2_EST_VAL2	            = mer_mart.DAY2_EST_VAL2,
                DAY3_EST_VAL2	            = mer_mart.DAY3_EST_VAL2,
                DAY4_EST_VAL2	            = mer_mart.DAY4_EST_VAL2,
                DAY5_EST_VAL2	            = mer_mart.DAY5_EST_VAL2,
                DAY6_EST_VAL2	            = mer_mart.DAY6_EST_VAL2,
                DAY7_EST_VAL2	            = mer_mart.DAY7_EST_VAL2,
                DAY1_EST_UNIT2	          = mer_mart.DAY1_EST_UNIT2,
                DAY2_EST_UNIT2	          = mer_mart.DAY2_EST_UNIT2,
                DAY3_EST_UNIT2	          = mer_mart.DAY3_EST_UNIT2,
                DAY4_EST_UNIT2	          = mer_mart.DAY4_EST_UNIT2,
                DAY5_EST_UNIT2	          = mer_mart.DAY5_EST_UNIT2,
                DAY6_EST_UNIT2	          = mer_mart.DAY6_EST_UNIT2,
                DAY7_EST_UNIT2	          = mer_mart.DAY7_EST_UNIT2,
                NUM_UNITS_PER_TRAY2	      = mer_mart.NUM_UNITS_PER_TRAY2,
                STORE_MODEL_STOCK	        = mer_mart.STORE_MODEL_STOCK,
                DAY1_DELIV_PAT1	          = mer_mart.DAY1_DELIV_PAT1,
                DAY2_DELIV_PAT1	          = mer_mart.DAY2_DELIV_PAT1,
                DAY3_DELIV_PAT1	          = mer_mart.DAY3_DELIV_PAT1,
                DAY4_DELIV_PAT1	          = mer_mart.DAY4_DELIV_PAT1,
                DAY5_DELIV_PAT1	          = mer_mart.DAY5_DELIV_PAT1,
                DAY6_DELIV_PAT1	          = mer_mart.DAY6_DELIV_PAT1,
                DAY7_DELIV_PAT1	          = mer_mart.DAY7_DELIV_PAT1,
                LAST_UPDATED_DATE	        = mer_mart.LAST_UPDATED_DATE,
                SCANNED_MODEL_STOCK_QTY	  = mer_mart.SCANNED_MODEL_STOCK_QTY,
                item_no                   = mer_mart.item_no,
                location_no               = mer_mart.location_no
          
          WHEN NOT MATCHED THEN
             --l_text := 'no MATCH FOUND - UPDATE !!'|| l_today_date;
          --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          INSERT
          (         sk1_location_no,
                    sk1_item_no,
                    post_date,
                    SK2_LOCATION_NO,
                    SK2_ITEM_NO,
                    DEPT_TYPE,
                    DIRECT_DELIVERY_IND,
                    NUM_STORE_LEADTIME_DAYS,
                    BOH_1_QTY,
                    BOH_1_IND,
                    BOH_2_QTY,
                    BOH_3_QTY,
                    SDN_1_QTY,
                    SDN1_IND,
                    SDN2_QTY,
                    SDN2_IND,
                    SHORT_QTY,
                    DAY1_ESTIMATE,
                    DAY2_ESTIMATE,
                    DAY3_ESTIMATE,
                    SAFETY_QTY,
                    MODEL_STOCK,
                    STORE_ORDER1,
                    STORE_ORDER2,
                    STORE_ORDER3,
                    DELIVERY_PATTERN,
                    NUM_UNITS_PER_TRAY,
                    WEEKLY_ESTIMATE1,
                    WEEKLY_ESTIMATE2,
                    SHELF_LIFE,
                    TRADING_DATE,
                    PROD_STATUS_1,
                    PROD_STATUS_2,
                    DIRECT_MU_QTY1,
                    DIRECT_MU_QTY2,
                    DIRECT_MU_QTY3,
                    DIRECT_MU_QTY4,
                    DIRECT_MU_QTY5,
                    DIRECT_MU_QTY6,
                    DIRECT_MU_QTY7,
                    DIRECT_MU_IND1,
                    DIRECT_MU_IND2,
                    DIRECT_MU_IND3,
                    DIRECT_MU_IND4,
                    DIRECT_MU_IND5,
                    DIRECT_MU_IND6,
                    DIRECT_MU_IND7,
                    DAY4_ESTIMATE,
                    DAY5_ESTIMATE,
                    DAY6_ESTIMATE,
                    DAY7_ESTIMATE,
                    DAY1_EST_VAL2,
                    DAY2_EST_VAL2,
                    DAY3_EST_VAL2,
                    DAY4_EST_VAL2,
                    DAY5_EST_VAL2,
                    DAY6_EST_VAL2,
                    DAY7_EST_VAL2,
                    DAY1_EST_UNIT2,
                    DAY2_EST_UNIT2,
                    DAY3_EST_UNIT2,
                    DAY4_EST_UNIT2,
                    DAY5_EST_UNIT2,
                    DAY6_EST_UNIT2,
                    DAY7_EST_UNIT2,
                    NUM_UNITS_PER_TRAY2,
                    STORE_MODEL_STOCK,
                    DAY1_DELIV_PAT1,
                    DAY2_DELIV_PAT1,
                    DAY3_DELIV_PAT1,
                    DAY4_DELIV_PAT1,
                    DAY5_DELIV_PAT1,
                    DAY6_DELIV_PAT1,
                    DAY7_DELIV_PAT1,
                    LAST_UPDATED_DATE,
                    SCANNED_MODEL_STOCK_QTY,
                    item_no,
                    location_no)
          
          values
          (         --CASE dwh_log.merge_counter(dwh_log.c_inserting)
                    --  WHEN 0 THEN mer_mart.sk1_location_no
                    --END,
                    mer_mart.sk1_location_no,
                    mer_mart.sk1_item_no,
                    mer_mart.post_date,
                    mer_mart.SK2_LOCATION_NO,
                    mer_mart.SK2_ITEM_NO,
                    mer_mart.DEPT_TYPE,
                    mer_mart.DIRECT_DELIVERY_IND,
                    mer_mart.NUM_STORE_LEADTIME_DAYS,
                    mer_mart.BOH_1_QTY,
                    mer_mart.BOH_1_IND,
                    mer_mart.BOH_2_QTY,
                    mer_mart.BOH_3_QTY,
                    mer_mart.SDN_1_QTY,
                    mer_mart.SDN_1_IND,
                    mer_mart.SDN2_QTY,
                    mer_mart.SDN2_IND,
                    mer_mart.SHORT_QTY,
                    mer_mart.DAY1_ESTIMATE,
                    mer_mart.DAY2_ESTIMATE,
                    mer_mart.DAY3_ESTIMATE,
                    mer_mart.SAFETY_QTY,
                    mer_mart.MODEL_STOCK,
                    mer_mart.STORE_ORDER1,
                    mer_mart.STORE_ORDER2,
                    mer_mart.STORE_ORDER3,
                    mer_mart.DELIVERY_PATTERN,
                    mer_mart.NUM_UNITS_PER_TRAY,
                    mer_mart.WEEKLY_ESTIMATE1,
                    mer_mart.WEEKLY_ESTIMATE2,
                    mer_mart.SHELF_LIFE,
                    mer_mart.TRADING_DATE,
                    mer_mart.PROD_STATUS_1,
                    mer_mart.PROD_STATUS_2,
                    mer_mart.DIRECT_MU_QTY1,
                    mer_mart.DIRECT_MU_QTY2,
                    mer_mart.DIRECT_MU_QTY3,
                    mer_mart.DIRECT_MU_QTY4,
                    mer_mart.DIRECT_MU_QTY5,
                    mer_mart.DIRECT_MU_QTY6,
                    mer_mart.DIRECT_MU_QTY7,
                    mer_mart.DIRECT_MU_IND1,
                    mer_mart.DIRECT_MU_IND2,
                    mer_mart.DIRECT_MU_IND3,
                    mer_mart.DIRECT_MU_IND4,
                    mer_mart.DIRECT_MU_IND5,
                    mer_mart.DIRECT_MU_IND6,
                    mer_mart.DIRECT_MU_IND7,
                    mer_mart.DAY4_ESTIMATE,
                    mer_mart.DAY5_ESTIMATE,
                    mer_mart.DAY6_ESTIMATE,
                    mer_mart.DAY7_ESTIMATE,
                    mer_mart.DAY1_EST_VAL2,
                    mer_mart.DAY2_EST_VAL2,
                    mer_mart.DAY3_EST_VAL2,
                    mer_mart.DAY4_EST_VAL2,
                    mer_mart.DAY5_EST_VAL2,
                    mer_mart.DAY6_EST_VAL2,
                    mer_mart.DAY7_EST_VAL2,
                    mer_mart.DAY1_EST_UNIT2,
                    mer_mart.DAY2_EST_UNIT2,
                    mer_mart.DAY3_EST_UNIT2,
                    mer_mart.DAY4_EST_UNIT2,
                    mer_mart.DAY5_EST_UNIT2,
                    mer_mart.DAY6_EST_UNIT2,
                    mer_mart.DAY7_EST_UNIT2,
                    mer_mart.NUM_UNITS_PER_TRAY2,
                    mer_mart.STORE_MODEL_STOCK,
                    mer_mart.DAY1_DELIV_PAT1,
                    mer_mart.DAY2_DELIV_PAT1,
                    mer_mart.DAY3_DELIV_PAT1,
                    mer_mart.DAY4_DELIV_PAT1,
                    mer_mart.DAY5_DELIV_PAT1,
                    mer_mart.DAY6_DELIV_PAT1,
                    mer_mart.DAY7_DELIV_PAT1,
                    mer_mart.LAST_UPDATED_DATE,
                    mer_mart.SCANNED_MODEL_STOCK_QTY,
                    mer_mart.item_no,
                    mer_mart.location_no
             );
        g_recs_read := 0;
        g_recs_inserted :=  0;    
        g_recs_read :=  SQL%ROWCOUNT;
        g_recs_inserted :=  SQL%ROWCOUNT;

    commit;

          l_text := 'Period='||g_start_date||' - '||g_end_date||' Recs MERGED = '||g_recs_inserted;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := g_date - 1;
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

end wh_prf_corp_739U_SHPD2;
