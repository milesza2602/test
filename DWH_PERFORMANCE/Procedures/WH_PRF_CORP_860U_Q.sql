--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_860U_Q
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_860U_Q" 
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
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD 2 years foods';
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
          selloc as (select location_no,  SK1_LOCATION_NO
                            from dim_location where area_no=9951),
          selcal as (select distinct this_week_start_date,fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                            from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
          select /*+ parallel(SRC,8) */ SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no, sum(nvl(sales,0)) sales, sum(nvl(sales_qty,0)) sales_qty, sum(nvl(sales_cost,0)) sales_cost ,sum(nvl(sales_margin,0)) sales_margin 
          from RTL_LOC_ITEM_WK_RMS_DENSE src, selitm si, selloc sl, selcal sc
          where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no and src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
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
    selloc as (select location_no, SK1_LOCATION_NO
                      from dim_location where area_no=9951),
    selcal as (select distinct this_week_start_date,fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                      from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
    select /*+ parallel(SRC,8) */ SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no
            , sum(nvl(prom_sales,0)) prom_sales,   sum(nvl(prom_sales_qty,0)) prom_sales_qty,sum(nvl(prom_sales_margin,0)) prom_sales_margin
             ,sum(nvl(waste_cost,0)) waste_cost,sum(nvl(waste_qty,0)) waste_qty            
    from RTL_LOC_ITEM_WK_RMS_SPARSE src, selitm si, selloc sl, selcal sc
    where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no and src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
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
with selitm as (select sk1_item_no
                  from dim_item where business_unit_no = 50),
selloc as (select location_no, SK1_LOCATION_NO
                  from dim_location where area_no=9951),
SELCAL as (select distinct THIS_WEEK_START_DATE,THIS_WEEK_END_DATE,FIN_YEAR_NO, FIN_QUARTER_NO, FIN_MONTH_NO, FIN_WEEK_NO, (FIN_YEAR_NO*100+FIN_WEEK_NO) FIN_YRWK_NO
                  from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
selPER as (select MIN(this_week_start_date) PERIOD_START_DATE, MAX(THIS_WEEK_END_DATE) PERIOD_END_DATE
                  from SELCAL)
select /*+ parallel(SRC,8) */ cal.fin_year_no, cal.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no
        , sum(nvl(online_sales,0)) online_sales,sum(nvl(online_sales_qty,0)) online_sales_qty
        , SUM(NVL(ONLINE_SALES_COST,0)) ONLINE_SALES_COST, SUM(NVL(ONLINE_SALES_MARGIN,0)) ONLINE_SALES_MARGIN  
from RTL_LOC_ITEM_DY_WWO_Sale src, selitm si, selloc sl, selcal sc, selper, dim_calendar cal
where SRC.SK1_ITEM_NO = SI.SK1_ITEM_NO 
and SRC.SK1_LOCATION_NO = SL.SK1_LOCATION_NO 
and  POST_DATE between PERIOD_START_DATE and PERIOD_END_DATE
and  POST_DATE = CAL.CALENDAR_DATE
and SC.THIS_WEEK_START_DATE = CAL.THIS_WEEK_START_DATE
--and sc.this_week_start_date between PERIOD_START_DATE AND PERIOD_END_DATE
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

   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_catalog';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_catalog');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_CATALOG
with selitm as (select sk1_item_no
                  from dim_item where business_unit_no = 50),
selloc as (select location_no, SK1_LOCATION_NO
                  from dim_location where area_no=9951),
selcal as (select distinct this_week_start_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                  from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
select /*+ parallel(SRC,8) */ SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no
        , sum(nvl(FD_Num_Catlg_Days,0)) FD_Num_Catlg_Days , sum(nvl(FD_Num_Avail_Days,0))  FD_Num_Avail_Days 
        ,sum(nvl(FD_Num_Catlg_Days_Adj,0))  FD_Num_Catlg_Days_Adj , sum(nvl(FD_Num_Avail_Days_Adj,0))  FD_Num_Avail_Days_Adj, sum(nvl(This_wk_catalog_Ind,0))  This_wk_catalog_Ind
from RTL_LOC_ITEM_WK_CATALOG src, selitm si, selloc sl, selcal sc
where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no and src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
   AND fd_num_catlg_days > 0
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
with selitm as (select  sk1_item_no
                  from dim_item where business_unit_no = 50),
selloc as (select location_no, SK1_LOCATION_NO
                  from dim_location where area_no=9951),
selcal as (select distinct this_week_start_date,this_week_end_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                  from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
select /*+ parallel(SRC,8) */ SRC.fin_year_no, SRC.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no
        ,sum(nvl(spec_dept_revenue,0)) spec_dept_revenue ,sum(nvl(spec_dept_qty,0)) spec_dept_qty
from RTL_LOC_ITEM_wk_POS_JV src, selitm si, selloc sl, selcal sc
where src.sk1_item_no = si.sk1_item_no and src.sk1_location_no = sl.sk1_location_no and  src.fin_year_no = sc.fin_year_no and src.fin_week_no = sc.fin_week_no
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
-- MART_FD_ZONE_LOC_ITEM_6WKAVG
--**************************************************************************************************
PROCEDURE extract_mart
AS
BEGIN


   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_mart';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_mart');
   commit;

   l_text := 'insert starting ';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

insert /*+ append */ into dwh_performance.temp_foods_tab_2yrs_MART
with selitm as (select item_no, sk1_item_no
                  from dim_item where business_unit_no = 50),
selloc as (select location_no, SK1_LOCATION_NO
                  from dim_location where area_no=9951),
selcal as (select distinct fin_year_no, fin_week_no, this_week_end_date
                  from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7)
select /*+ parallel(SRC,8) */ src.fin_year_no, src.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no 
          , sum(nvl(SALES_6WK_QTY,0)) SALES_6WK_QTY , sum(nvl(SALES_6WKAVG_EXCL_PROMO_QTY,0)) SALES_6WKAVG_EXCL_PROMO_QTY
          , sum(nvl(SALES_6WK,0)) SALES_6WK ,sum(nvl(SALES_6WKAVG_EXCL_PROMO,0)) SALES_6WKAVG_EXCL_PROMO 
            ,sum(nvl(sales_6wk_margin,0))  sales_6wk_margin ,sum(nvl(waste_6wk_promo_cost,0))  waste_6wk_promo_cost        
--from dwh_performance.rtl_loc_item_wk_sales_6wkavg src, selitm si, selloc sl, selcal sc
from dwh_performance.mart_fd_zone_loc_item_6wkavg src, selitm si, selloc sl, selcal sc
where src.sk1_item_no = si.sk1_item_no 
and src.sk1_location_no = sl.sk1_location_no 
and src.fin_year_no = sc.fin_year_no
and src.fin_week_no = sc.fin_week_no
group by src.fin_year_no, src.fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no;   

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
  L_MESSAGE := 'extract_mart - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_mart;

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
with selitm as (select item_no, sk1_item_no
                  from dim_item where business_unit_no = 50),
selloc as (select location_no, sk1_location_no
                  from dim_location where area_no=9951),
selcal as (select distinct this_week_start_date, this_week_end_date, fin_year_no, fin_quarter_no, fin_month_no, fin_week_no, (fin_year_no*100+fin_week_no) fin_yrwk_no
                  from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
selprom as (select /*+ materialize parallel(SRC,6) */ c.fin_year_no, c.fin_week_no,  item_no, location_no 
                            ,sum(nvl(wreward_sales_value,0)) wreward_sales_excl_vat
                            , sum(case when loyalty_partner_id <> 'LOYLPPV' then 1 else 0 end) wrewards_sales_unit1
                            , sum(case when loyalty_partner_id = 'LOYLPPV' then 1 else 0 end)  wrewards_sales_unit2
                            , SUM(NVL(PROMOTION_DISCOUNT_AMOUNT,0)) TOTAL_WREWARDS_DISCOUNT
                              from DWH_CUST_PERFORMANCE.CUST_BASKET_AUX SRC,   DWH_CUST_FOUNDATION.FND_WOD_PROM_DISCOUNT WPD,  dim_calendar c
                              --selcal sc
                              where TRAN_DATE between THIS_WEEK_START_DATE and THIS_WEEK_END_DATE
                               and TRAN_DATE = c.calendar_date
                               AND  '666'||wpd.prom_no = src.promotion_no
                               group by FIN_YEAR_NO, FIN_WEEK_NO, ITEM_NO, LOCATION_NO )
select    fin_year_no, fin_week_no, sk1_item_no, sk1_location_no ,
          wreward_sales_excl_vat,
          wrewards_sales_unit1,
          wrewards_sales_unit2,
          total_wrewards_discount 
From selprom src, selitm si, selloc sl
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


EXCEPTION
WHEN OTHERS THEN
  L_MESSAGE := 'extract_cust - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_cust;


--**************************************************************************************************
-- rtl_loc_item_dy_rms_price
--**************************************************************************************************
PROCEDURE extract_Ruling
AS
BEGIN 
   l_text := 'truncate table  dwh_performance.temp_foods_tab_2yrs_ruling';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   execute immediate('truncate table  dwh_performance.temp_foods_tab_2yrs_ruling');
--   commit;

   l_text := 'insert starting ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          insert /*+ append parallel (t,4) */  into dwh_performance.temp_foods_tab_2yrs_ruling t
          
          with selitm as (select  /*+ full(dim_item) */ sk1_item_no
                          from    dim_item 
                          where   business_unit_no = 50),
               selloc as (select  /*+ full(dim_location) */ location_no, SK1_LOCATION_NO
                          from    dim_location 
                          where   area_no=9951),
               selcal as (select  /*+ full(dim_calendar) */
                                  distinct this_week_start_date,this_week_end_date,Fin_Year_no, Fin_Quarter_no, 
                                  Fin_Month_no, Fin_week_no, (Fin_Year_no*100+Fin_week_no) fin_yrwk_no
                          from    dim_calendar 
                          where   calendar_date between trunc(sysdate) - 42 and trunc(sysdate) - 7),
               selPER as (select  MIN(this_week_start_date) PERIOD_START_DATE, MAX(THIS_WEEK_END_DATE) PERIOD_END_DATE
                          from    SELCAL)
                          
          select  /*+ parallel(SRC,4) full(src) */
                  fin_year_no, fin_week_no, src.sk1_item_no, src.sk1_location_no
                  , min(nvl(ruling_rsp,0)) ruling_rsp
          from    rtl_loc_item_dy_rms_price src, 
                  selitm si, selloc sl, selcal sc, selper
          where   src.sk1_item_no     = si.sk1_item_no 
          and     src.sk1_location_no = sl.sk1_location_no 
          and     calendar_date between period_start_date AND PERIOD_END_DATE
          and     sc.this_week_start_date between period_start_date and period_end_date
          group by 
                  fin_year_no, fin_week_no, SRC.sk1_item_no, SRC.sk1_location_no; 


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
  L_MESSAGE := 'extract_ruling - error';
  DWH_LOG.RECORD_ERROR(L_MODULE_NAME,SQLCODE,L_MESSAGE);
  RAISE;
END extract_Ruling;


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
   EXTRACT_WWO_SALE;
   extract_catalog;
   extract_pos_jv;
   extract_mart;
   extract_cust;
--   extract_ruling;

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

end WH_PRF_CORP_860U_Q;
