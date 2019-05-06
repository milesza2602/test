--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_860U_TMP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_860U_TMP" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        November 2015
--  Author:      W Lyttle
--  Purpose:     Foods Extract for Tableau
--               2 years extract first then last 6 weeks
--               write each extract to temp table 
--  Tables:      Input  - various
--               Output - 
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--    15 Jan 2016 - Added procedure (extract_Ruling) & table (dwh_performance.temp_foods_tab_2yrs_ruling) for Rulings extract   - B Kirschner
-- wendy lyttle 26 aug 2016 - added to export_cust
--                            and wpd.reward_type = 'LR'
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
g_recs_inserted      integer       :=  0;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_ly_fin_week_no     dim_calendar.fin_week_no%type;
g_ly_fin_year_no     dim_calendar.fin_year_no%type;
g_lcw_fin_week_no    dim_calendar.fin_week_no%type;
g_lcw_fin_year_no    dim_calendar.fin_year_no%type;
g_date               date;
g_start_date         date;
g_end_date           date;
g_ly_start_date      date;
g_ly_end_date        date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_860U_TMP';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Load Foods Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_RMS_DENSE
--**************************************************************************************************
PROCEDURE extract_dense
AS
BEGIN 
   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_dense';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_dense');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
          insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_dense
          with selitm as (select  sk1_item_no
                            from dim_item where business_unit_no = 50),
          selloc as (select location_no,  sk1_location_no
                            from dim_location where area_no = 9951),
          selcal as (select distinct this_week_start_date,fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                            from dim_calendar where calendar_date between trunc(sysdate) - 84 and trunc(sysdate) - 7)
          select /*+ parallel(SRC,8) */ SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no, sum(nvl(sales,0)) sales, sum(nvl(sales_qty,0)) sales_qty, sum(nvl(sales_cost,0)) sales_cost ,sum(nvl(sales_margin,0)) sales_margin 
          from RTL_LOC_ITEM_WK_RMS_DENSE src, selitm si, selloc sl, selcal sc
          where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no 
          and src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
          and src.sales > 0
          group by SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);



EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_dense - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_dense;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_RMS_SPARSE
--**************************************************************************************************
PROCEDURE extract_sparse
AS
BEGIN

   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_sparse';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_sparse');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_sparse
    with selitm as (select sk1_item_no
                      from dim_item where business_unit_no = 50),
    SELLOC as (select LOCATION_NO, SK1_LOCATION_NO
                      from dim_location where area_no = 9951),
    selcal as (select distinct this_week_start_date,fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                      from dim_calendar where calendar_date between trunc(sysdate) - 84 and trunc(sysdate) - 7)
    select /*+ parallel(SRC,8) */ 
           src.fin_year_no, 
           src.fin_week_no, 
           src.sk1_item_no, 
           src.sk1_location_no, 
           sum(nvl(prom_sales,0)) prom_sales,   
           sum(nvl(prom_sales_qty,0)) prom_sales_qty,
           sum(nvl(prom_sales_margin,0)) prom_sales_margin,
           sum(nvl(waste_cost,0)) waste_cost,
           sum(nvl(waste_qty,0)) waste_qty            
    from RTL_LOC_ITEM_WK_RMS_SPARSE src, selitm si, selloc sl, selcal sc
    where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no 
    and src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
    and (prom_sales <> 0 or waste_cost <> 0)
    group by SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_sparse - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_sparse;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_WWO_Sale
--**************************************************************************************************
PROCEDURE extract_WWO_Sale
AS
BEGIN


   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_wwo_sale';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_WWO_SALE');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_WWO_SALE
    with SELITM as 
              (select sk1_item_no
               from dim_item where business_unit_no = 50),
    selloc as (select location_no, SK1_LOCATION_NO
               from dim_location where area_no = 9951),
    SELCAL as (select distinct THIS_WEEK_START_DATE,THIS_WEEK_END_DATE,FIN_YEAR_NO, FIN_QUARTER_NO, FIN_MONTH_NO, FIN_WEEK_NO, (FIN_YEAR_NO*100+FIN_WEEK_NO) FIN_YRWK_NO
               from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
    SELPER as (select min(THIS_WEEK_START_DATE) PERIOD_START_DATE, max(THIS_WEEK_END_DATE) PERIOD_END_DATE
               from SELCAL),
    seldat as (select a.calendar_date, a.fin_year_no, a.fin_week_no
                from dim_calendar a, SELPER b
                where  a.CALENDAR_DATE between B.PERIOD_START_DATE and B.PERIOD_END_DATE
                order by a.CALENDAR_DATE)
                     
    select /*+ parallel(SRC,8) */ cal.fin_year_no, cal.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no
            , SUM(NVL(ONLINE_SALES,0)) ONLINE_SALES
            , sum(nvl(online_sales_qty,0)) online_sales_qty
            , SUM(NVL(ONLINE_SALES_COST,0)) ONLINE_SALES_COST
            , SUM(NVL(ONLINE_SALES_MARGIN,0)) ONLINE_SALES_MARGIN  
    from RTL_LOC_ITEM_DY_WWO_SALE SRC, SELITM SI, SELLOC SL, seldat cal
    where SRC.SK1_ITEM_NO = SI.SK1_ITEM_NO 
    and SRC.SK1_LOCATION_NO = SL.SK1_LOCATION_NO 
    and  POST_DATE = cal.calendar_date
    group by cal.fin_year_no, cal.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;                     

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);



EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_WWO_Sale - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_WWO_Sale;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_Catalog
--**************************************************************************************************
PROCEDURE extract_Catalog
AS
BEGIN

   l_text := 'truncate table  dwh_performance.TEMP_FOODS_TAB_2YRS_CATALOG';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table dwh_performance.TEMP_FOODS_TAB_2YRS_CATALOG');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
   
   insert /*+ append */ into TEMP_FOODS_TAB_2YRS_CATALOG
   with SELITM as (select SK1_ITEM_NO
                   from DIM_ITEM where BUSINESS_UNIT_NO = 50),
        SELLOC as (select LOCATION_NO, SK1_LOCATION_NO
                   from DIM_LOCATION where area_no = 9951),
        SELCAL as (select distinct THIS_WEEK_START_DATE, FIN_YEAR_NO, FIN_QUARTER_NO, FIN_MONTH_NO, FIN_WEEK_NO, (FIN_YEAR_NO*100+FIN_WEEK_NO) FIN_YRWK_NO
                   from DIM_CALENDAR 
                   where CALENDAR_DATE between TRUNC(sysdate) - 42 and TRUNC(sysdate) - 7)
                   
    select /*+ parallel(SRC,8) */ SRC.FIN_YEAR_NO, SRC.FIN_WEEK_NO, SRC.SK1_ITEM_NO, SRC.SK1_LOCATION_NO
           , SUM(NVL(FD_NUM_CATLG_DAYS,0)) FD_NUM_CATLG_DAYS 
           , SUM(NVL(FD_NUM_AVAIL_DAYS,0))  FD_NUM_AVAIL_DAYS 
           , SUM(NVL(FD_NUM_CATLG_DAYS_ADJ,0))  FD_NUM_CATLG_DAYS_ADJ 
           , SUM(NVL(FD_NUM_AVAIL_DAYS_ADJ,0))  FD_NUM_AVAIL_DAYS_ADJ
           , SUM(NVL(THIS_WK_CATALOG_IND,0))  THIS_WK_CATALOG_IND
           , SUM(NVL(FD_NUM_CUST_AVAIL_ADJ,0))  FD_NUM_CUST_AVAIL_ADJ
    from  RTL_LOC_ITEM_WK_CATALOG SRC, SELITM SI, SELLOC SL, SELCAL SC
    where src.sk1_item_no = si.sk1_item_no 
    and   src.sk1_location_no = sl.sk1_location_no 
    and   src.fin_year_no = sc.fin_year_no 
    and   SRC.FIN_WEEK_NO = SC.FIN_WEEK_NO
    and   FD_NUM_CATLG_DAYS > 0
    group by SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;       

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_Catalog - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_Catalog;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_POS_JV
--**************************************************************************************************
PROCEDURE extract_POS_JV
AS
BEGIN


   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_pos_jv';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_pos_jv');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_POS_JV
    with selitm as 
              (select  sk1_item_no
               from dim_item where business_unit_no = 50),
    selloc as (select location_no, SK1_LOCATION_NO
               from dim_location where area_no = 9951),
    selcal as (select distinct this_week_start_date,this_week_end_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
               from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
    select /*+ parallel(SRC,8) */ 
             src.fin_year_no
            ,src.fin_week_no
            ,src.sk1_item_no
            ,SRC.sk1_location_no
            ,sum(nvl(spec_dept_revenue,0)) spec_dept_revenue 
            ,sum(nvl(spec_dept_qty,0)) spec_dept_qty
    from  RTL_LOC_ITEM_wk_POS_JV src, selitm si, selloc sl, selcal sc
    where src.sk1_item_no = si.sk1_item_no 
     and  src.sk1_location_no = sl.sk1_location_no
     and  src.fin_year_no = sc.fin_year_no 
     and  src.fin_week_no = sc.fin_week_no
    group by SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;    

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_pos_jv - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_pos_jv;

--**************************************************************************************************
-- CUST_BASKET_AUX
--**************************************************************************************************
PROCEDURE extract_cust
AS
BEGIN


   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_cust';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_cust');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_cust
    with selitm as 
              (select item_no, sk1_item_no
               from DIM_ITEM where BUSINESS_UNIT_NO = 50),
    selloc as (select location_no, sk1_location_no
               from DIM_LOCATION where area_no = 9951),
    SELCAL as (select distinct THIS_WEEK_START_DATE, THIS_WEEK_END_DATE, FIN_YEAR_NO, FIN_QUARTER_NO, FIN_MONTH_NO, FIN_WEEK_NO, (FIN_YEAR_NO*100+FIN_WEEK_NO) FIN_YRWK_NO
               from DIM_CALENDAR where CALENDAR_DATE between TRUNC(sysdate) - 42 and TRUNC(sysdate) - 7),
    SELDAT as (select a.CALENDAR_DATE, a.FIN_YEAR_NO, a.FIN_WEEK_NO
               from dim_calendar a, SELCAL b
               where  a.CALENDAR_DATE between B.THIS_WEEK_START_DATE and B.THIS_WEEK_END_DATE
               order by a.CALENDAR_DATE),
    selprom as (select /*+ materialize parallel(SRC,6) parallel(WPD,6) full SC */ 
                sc.fin_year_no, 
                sc.fin_week_no,  
                item_no, 
                location_no 
              , sum(nvl(wreward_sales_value,0)) wreward_sales_excl_vat
              , sum(case when loyalty_partner_id <> 'LOYLPPV' then 1 else 0 end) wrewards_sales_unit1
              , sum(case when loyalty_partner_id = 'LOYLPPV' then 1 else 0 end)  wrewards_sales_unit2
              , SUM(NVL(PROMOTION_DISCOUNT_AMOUNT,0)) TOTAL_WREWARDS_DISCOUNT
                from DWH_CUST_PERFORMANCE.CUST_BASKET_AUX SRC,   DWH_CUST_FOUNDATION.FND_WOD_PROM_DISCOUNT WPD,  
                SELDAT SC
                where TRAN_DATE = sc.calendar_date
                 AND  '666'||wpd.prom_no = src.promotion_no
                 and wpd.reward_type = 'LR'   -- added 26 aug 2016
                group by fin_year_no, fin_week_no, item_no, location_no )
                
    select    fin_year_no, 
              fin_week_no, 
              sk1_item_no, 
              sk1_location_no ,
              wreward_sales_excl_vat,
              wrewards_sales_unit1,
              wrewards_sales_unit2,
              total_wrewards_discount 
    from SELPROM SRC, SELITM SI, SELLOC SL
    where src.item_no = si.item_no and src.location_no = sl.location_no ;

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


exception
when others then
  l_message := 'extract_cust - error';
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end extract_cust;

--**************************************************************************************************
-- RTL_LOC_ITEM_WK_RATE_OF_SALE
--**************************************************************************************************
PROCEDURE extract_rate_of_sale
AS
BEGIN

   l_text := 'truncate table dwh_performance.TEMP_FOODS_TAB_RATE_OF_SALE';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table dwh_performance.TEMP_FOODS_TAB_RATE_OF_SALE');
   commit;

   l_text := 'insert starting ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   insert /*+ append */ into TEMP_FOODS_TAB_RATE_OF_SALE
   with 
    selitm as (select sk1_item_no
               from dim_item 
               where business_unit_no = 50),
    selloc as (select location_no, sk1_location_no
               from dim_location where area_no = 9951),
    selcal as (select distinct this_week_start_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
               from dim_calendar 
               where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
                   
    select /*+ parallel(ros,8) */ ros.fin_year_no, ros.fin_week_no, ros.sk1_item_no , ros.sk1_location_no
           , sum(avg_units_per_day) avg_units_per_day           
    from  rtl_loc_item_wk_rate_of_sale ros, selitm si, selcal sc, selloc sl
    where ros.sk1_item_no     = si.sk1_item_no
     and  ros.sk1_location_no = sl.sk1_location_no
     and  ros.fin_year_no     = sc.fin_year_no 
     and  ros.fin_week_no     = sc.fin_week_no
    group by ros.fin_year_no, ros.fin_week_no, ros.sk1_item_no, ros.sk1_location_no;      

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_rate_of_sale - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
end extract_rate_of_sale;

--**************************************************************************************************
-- RTL_LOC_DY
--**************************************************************************************************
PROCEDURE extract_like_4_like
AS
BEGIN

   l_text := 'truncate table  dwh_performance.TEMP_FOODS_TAB_LIKE_FOR_LIKE';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table dwh_performance.TEMP_FOODS_TAB_LIKE_FOR_LIKE');
   commit;

   l_text := 'insert starting ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   insert /*+ append */ into dwh_performance.TEMP_FOODS_TAB_LIKE_FOR_LIKE
   with 
   selloc as (select location_no, sk1_location_no
              from dim_location where area_no = 9951),
   selcal as (select distinct this_week_start_date,this_week_end_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
              from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
   selsel as (select min(this_week_start_date) this_week_start_date, max(this_week_end_date) this_week_end_date 
              from selcal),
   seldat as (select a.calendar_date, a.fin_year_no, a.fin_week_no
              from dim_calendar a, selsel b
              where  a.calendar_date between b.this_week_start_date and b.this_week_end_date
              order by a.calendar_date)
                   
   select /*+ parallel(ld,6) full (sl) full (sc) parallel(ds,6) full (ds) */ sc.fin_year_no, sc.fin_week_no, 
           ds.sk1_item_no, sl.sk1_location_no, max(like_for_like_ind) like_for_like_ind 
   from  rtl_loc_dy ld, selloc sl, seldat sc, TEMP_FOODS_TAB_2YRS_CATALOG ds
   where ld.sk1_location_no = sl.sk1_location_no 
    and  ld.post_date       = sc.calendar_date
    and  sc.fin_year_no     = ds.fin_year_no
    and  sc.fin_week_no     = ds.fin_week_no
    and  sl.sk1_location_no = ds.sk1_location_no
   group by sc.fin_year_no, sc.fin_week_no, sl.sk1_location_no, ds.sk1_item_no;  

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   l_text := '----------------------------------------';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_like_4_like - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  raise;
end extract_like_4_like;

--new1
--**************************************************************************************************
-- RTL_LOC_ITEM_WK_RMS_PRICE
--**************************************************************************************************
PROCEDURE extract_price
AS
BEGIN


   l_text := 'truncate table dwh_performance.TEMP_FOODS_TAB_PRICE';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table dwh_performance.TEMP_FOODS_TAB_PRICE');
   commit;

   l_text := 'insert starting ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    insert /*+ append */ into dwh_performance.TEMP_FOODS_TAB_PRICE  
    with selitm as 
              (select sk1_item_no
               from dim_item where business_unit_no = 50),
    selloc as (select location_no, sk1_location_no
               from dim_location where area_no = 9951),
    selcal as (select distinct this_week_start_date,this_week_end_date,fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
               from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
    selper as (select min(this_week_start_date) period_start_date, max(this_week_end_date) period_end_date
               from selcal),
    seldat as (select a.calendar_date, a.fin_year_no, a.fin_week_no
               from dim_calendar a, selper b
               where  a.calendar_date between b.period_start_date and b.period_end_date
               order by a.calendar_date),
                     
    selprice as (                 
    select /*+ parallel(SRC,8) */ 
              cal.fin_year_no
            , cal.fin_week_no
            , src.sk1_item_no
            , src.sk1_location_no
            , max(nvl(reg_rsp,0)) reg_rsp
            , max(nvl(prom_rsp,0)) prom_rsp
            , max(nvl(ruling_rsp,0)) ruling_rsp          
    from rtl_loc_item_dy_rms_price src, selitm si, selloc sl, seldat cal
    where src.sk1_item_no     = si.sk1_item_no 
    and   src.sk1_location_no = sl.sk1_location_no 
    and   src.calendar_date   = cal.calendar_date
    group by cal.fin_year_no, cal.fin_week_no, src.sk1_item_no, src.sk1_location_no)
    
    select /*+ full (a) parallel (a,4) full (c) parallel (c,4) */ a.*
    from  dwh_performance.temp_foods_tab_2yrs_sparse c,
          selprice a
    where c.fin_year_no     = a.fin_year_no
      and c.fin_week_no     = a.fin_week_no
      and c.sk1_item_no     = a.sk1_item_no
      and c.sk1_location_no = a.sk1_location_no
      and (c.prom_sales <> 0 or c.waste_cost <> 0) 
    union 
    select /*+ full (a) parallel (a,4) full (c) parallel (c,4) */ a.*
    from  dwh_performance.temp_foods_tab_2yrs_dense c,
          selprice a
    where c.fin_year_no     = a.fin_year_no
      and c.fin_week_no     = a.fin_week_no
      and c.sk1_item_no     = a.sk1_item_no
      and c.sk1_location_no = a.sk1_location_no
      and sales <> 0 
    union 
    select /*+ full (a) parallel (a,4) full (c) parallel (c,4) */ a.*
    from  TEMP_FOODS_TAB_2YRS_CATALOG c,
          selprice a
    where c.fin_year_no     = a.fin_year_no
      and c.fin_week_no     = a.fin_week_no
      and c.sk1_item_no     = a.sk1_item_no
      and c.sk1_location_no = a.sk1_location_no;                 

      g_recs_read     := 0;
      g_recs_inserted := 0;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'records inserted='||g_recs_inserted   ;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := '----------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



exception
when others then
  l_message := 'extract_price - error';
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end extract_price;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit := p_forall_limit;
   end if;
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOAD OF FOODS 2YRS STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   execute immediate 'alter session enable parallel dml';

   extract_dense;
   extract_sparse;
   extract_wwo_sale;   
   extract_catalog;
   extract_pos_jv;
   extract_cust;
   extract_rate_of_sale;
   extract_like_4_like;
   extract_price;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end WH_PRF_CORP_860U_TMP;
