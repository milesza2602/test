--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_861U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_861U_OLD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2015
--  Author:      W Lyttle
--  Purpose:     Foods Extract for Tableau
--               2 years extract first then last 6 weeks
--               combine all extracts and write to target table 
--               Does union at end
--  Tables:      Input  - various
--               Output - 
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--    15 Jan 2016:  a. Include data from 'Rulings' extract (dwh_performance.temp_foods_tab_2yrs_ruling) 
--                  b. Include extra join for additional table accesses required for column ‘REG_RSP’ (ln 269-271)
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_month_no       number        :=  0;
g_fin_quarter_no     number        :=  0;
g_loop_fin_year_no   number        :=  0;
g_loop_fin_week_no   number        :=  0;
g_this_week_start_date date;
g_sub                integer       :=  0;
g_loop_cnt           integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_date_adhoc         date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_loop_start_date    date;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_861U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS DENSE PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

    begin
       for g_sub in 1 .. 5 --> 5 week drop of data
         loop         
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dim_calendar
           where  calendar_date = (g_date) - (g_sub * 7);
           
         execute immediate 'alter table '|| 'dwh_performance' || '.'|| 'MART_FDS_LOC_ITEM_WK_TABLEAU' ||' truncate subpartition for ('||G_LOOP_FIN_YEAR_NO||','||G_LOOP_FIN_WEEK_NO||')';
--         execute immediate 'alter table '|| 'dwh_performance' || '.'|| 'temp_foods_tab_roll' ||' truncate partition for (to_date(''' || g_loop_start_date || ''', ''dd-mm-yyyy hh24:mi:ss''))';
         l_text := 'Truncate Partition = '||g_loop_fin_year_no||' - '||g_loop_fin_week_no;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         commit;
         end loop;
    end;

    select distinct count(distinct this_week_start_date) into g_loop_cnt -->--> 6 week build of data
    from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate -7);

    
--g_loop_cnt := g_loop_cnt - 1;
--g_date := '07 MAR 2016';
-- for testing
--g_loop_cnt := 5; --> remove

    for g_sub in 1..g_loop_cnt loop
       select distinct fin_year_no, fin_week_no, fin_month_no, fin_quarter_no, this_week_start_date
       into   g_fin_year_no, g_fin_week_no, g_fin_month_no, g_fin_quarter_no, g_this_week_start_date
       from   dim_calendar
       where  calendar_date = trunc(sysdate) - (g_sub * 7);
--      dbms_output.put_line('Display '||g_fin_year_no||' '||g_fin_week_no||' '||g_fin_month_no||g_fin_quarter_no||' '||g_this_week_start_date);
       l_text := 'Rollup = '||g_fin_year_no||' - '||g_fin_week_no;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /*+ APPEND parallel (t,6) */ into dwh_performance.MART_FDS_LOC_ITEM_WK_TABLEAU t
    --dwh_datafix.AJ_FOODS_TABLEAU_ROLL_TMPX
    --dwh_performance.MART_FDS_LOC_ITEM_WK_TABLEAU t
    --dwh_datafix.AJ_FOODS_TABLEAU_ROLL_TMPX t
    --dwh_performance.MART_FDS_LOC_ITEM_WK_TABLEAU t
    --dwh_datafix.aj_foods_tableau_roll_tmp1 t
    --aj_foods_tableau_roll_tmp --dwh_performance.temp_foods_tab_roll t
     with selunion as 
        (  select  sk1_location_no
                    ,sk1_item_no
                    ,Fin_Year_no, Fin_week_no 
                    ,sum(nvl(sales,0)) sales,sum(nvl(sales_qty,0)) sales_qty, sum(nvl(sales_cost,0)) sales_cost, sum(nvl(sales_margin,0)) sales_margin 
                    ,sum(nvl(spec_dept_revenue,0)) spec_dept_revenue ,sum(nvl(spec_dept_qty,0)) spec_dept_qty 
                    ,sum(nvl(prom_sales,0)) prom_sales, sum(nvl(prom_sales_qty,0)) prom_sales_qty,sum(nvl(prom_sales_margin,0)) prom_sales_margin,sum(nvl(waste_cost,0)) waste_cost, sum(nvl(waste_qty,0)) waste_qty
                    ,sum(nvl(FD_Num_Catlg_Days,0)) FD_num_Catlg_Days, sum(nvl(FD_Num_Avail_Days,0)) FD_Num_Avail_Days, sum(nvl(FD_Num_Catlg_Days_Adj,0)) FD_Num_Catlg_Days_Adj,sum(nvl(FD_Num_Avail_Days_Adj,0)) FD_Num_Avail_Days_Adj
                    ,sum(nvl(online_sales,0)) online_sales,sum(nvl(online_sales_qty,0))online_sales_qty, sum(nvl(online_sales_cost,0)) online_sales_cost, sum(nvl(online_sales_margin,0))online_sales_margin  
                    ,sum(nvl(SALES_6WK_QTY,0))SALES_6WK_QTY , sum(nvl(SALES_6WKAVG_EXCL_PROMO_QTY,0)) SALES_6WKAVG_EXCL_PROMO_QTY, sum(nvl(SALES_6WK,0)) SALES_6WK ,sum(nvl(SALES_6WKAVG_EXCL_PROMO,0)) SALES_6WKAVG_EXCL_PROMO ,sum(nvl(SALES_6WK_MARGIN,0)) SALES_6WK_MARGIN ,sum(nvl(WASTE_6WK_PROMO_COST,0)) WASTE_6WK_PROMO_COST
                    ,sum(nvl(wreward_sales_excl_vat,0)) wreward_sales_excl_vat,sum(nvl(wrewards_sales_unit1,0) - nvl(wrewards_sales_unit2,0)) wrewards_sales_unit ,sum(nvl(total_wrewards_discount,0)) total_wrewards_discount
                    ,sum(nvl(ruling_rsp,0)) ruling_rsp
                    ,sum(nvl(prom_rsp,0)) prom_rsp
               from (
                         --dwh_performance.temp_foods_tab_2yrs_dense
                            select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            ,   sales,  sales_qty,   sales_cost,   sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from DWH_PERFORMANCE.TEMP_FOODS_TAB_2YRS_DENSE a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
/*                                 where fin_year_no = 2016
                                  and fin_week_no = 45
                                  and sk1_item_no = 322424
                                  and sk1_location_no = 566 */
 
                          union all
                         -- dwh_performance.temp_foods_tab_2yrs_sparse
                                   select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            ,   prom_sales,   prom_sales_qty,   prom_sales_margin,   waste_cost,    waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from dwh_performance.temp_foods_tab_2yrs_sparse a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and SK1_ITEM_NO = 322424
--                                  and sk1_location_no = 566 
                                  
                          union all
                        ---dwh_performance.temp_foods_tab_2yrs_WWO_SALE';
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            ,   online_sales,  online_sales_qty,    online_sales_cost,  online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from dwh_performance.temp_foods_tab_2yrs_wwo_sale a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and sk1_item_no = 322424
--                                  and sk1_location_no = 566 
                                  
                          union all
                         -- dwh_performance.temp_foods_tab_2yrs_catalog
                                   select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            ,   FD_num_Catlg_Days,    FD_Num_Avail_Days,   FD_Num_Catlg_Days_Adj,   FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from dwh_performance.temp_foods_tab_2yrs_catalog a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and sk1_item_no = 322424
--                                  and sk1_location_no = 566 
                       
                          union all
                        ---dwh_performance.temp_foods_tab_2yrs_pos_jv';
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            ,  spec_dept_revenue ,   spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from dwh_performance.temp_foods_tab_2yrs_pos_jv a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and sk1_item_no = 322424
--                                  and sk1_location_no = 566 
                                  
                          union all
                         --dwh_performance.temp_foods_tab_2yrs_mart';
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            ,  SALES_6WK_QTY ,    SALES_6WKAVG_EXCL_PROMO_QTY,    SALES_6WK ,   SALES_6WKAVG_EXCL_PROMO ,   SALES_6WK_MARGIN ,   WASTE_6WK_PROMO_COST
                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
                                            , null ruling_rsp, null prom_rsp
--                                 from dwh_performance.temp_foods_tab_2yrs_mart a
                                 from dwh_performance.rtl_loc_item_wk_sales_6wkavg a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and sk1_item_no = 322424
--                                  and sk1_location_no = 566 
                                 
                          union all
                        --dwh_performance.temp_foods_tab_2yrs_cust';
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
                                            , null spec_dept_revenue , null  spec_dept_qty 
                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
                                            ,   wreward_sales_excl_vat,   wrewards_sales_unit1, wrewards_sales_unit2,   total_wrewards_discount
                                            , null RULING_RSP, null prom_rsp
                                  from dwh_performance.temp_foods_tab_2yrs_cust a
                                  where fin_year_no = g_fin_year_no
                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and SK1_ITEM_NO = 322424
--                                  and sk1_location_no = 566 
                        )       
--                          union all
--                        --dwh_performance.temp_foods_tab_2yrs_ruling';
--                                  select /*+ parallel (a,6) */
--                                            sk1_location_no,sk1_item_no,Fin_Year_no,  Fin_week_no
--                                            , null  sales, null  sales_qty,  null sales_cost, null  sales_margin 
--                                            , null spec_dept_revenue , null  spec_dept_qty 
--                                            , null  prom_sales,  null  prom_sales_qty, null  prom_sales_margin, null  waste_cost,  null  waste_qty
--                                            , null  FD_num_Catlg_Days,  null  FD_Num_Avail_Days,  null  FD_Num_Catlg_Days_Adj, null  FD_Num_Avail_Days_Adj
--                                            , null  online_sales, null online_sales_qty,  null  online_sales_cost,  null online_sales_margin  
--                                            , null SALES_6WK_QTY ,  null  SALES_6WKAVG_EXCL_PROMO_QTY,  null  SALES_6WK , null  SALES_6WKAVG_EXCL_PROMO , null  SALES_6WK_MARGIN , null  WASTE_6WK_PROMO_COST
--                                            , null  wreward_sales_excl_vat, null  wrewards_sales_unit1, null  wrewards_sales_unit2, null  total_wrewards_discount
--                                            ,  RULING_RSP
--                                  from dwh_performance.temp_foods_tab_2yrs_ruling a
--                                  where fin_year_no = g_fin_year_no
--                                  and FIN_WEEK_NO = G_FIN_WEEK_NO
--                                  and sk1_item_no = 322424
--                                  and sk1_location_no = 566 
--                                
--                         )
          group by   sk1_location_no
                    ,sk1_item_no
                    ,fin_year_no, fin_week_no )
                    
    select  /*+ parallel(rl,4), parallel(se,4), full (rl) full (di) full (dl) */
        location_no
        ,di.item_no
        ,Fin_Year_no
        ,Fin_week_no
        ,g_this_week_start_date
        ,g_Fin_Quarter_no
        ,g_Fin_Month_no 
        ,(Fin_Year_no*100+Fin_week_no) fin_yrwk_no
        ,FD_PRODUCT_NO
        ,sales
        ,sales_qty
        ,sales_cost
        ,sales_margin 
        ,spec_dept_revenue
        ,spec_dept_qty 
        ,prom_sales
        ,prom_sales_qty
        ,prom_sales_margin
        ,waste_cost
        ,waste_qty
        ,FD_num_Catlg_Days
        ,FD_Num_Avail_Days
        ,FD_Num_Catlg_Days_Adj
        ,FD_Num_Avail_Days_Adj
        ,online_sales
        ,online_sales_qty
        ,online_sales_cost
        ,online_sales_margin  
        ,SALES_6WK_QTY
        ,SALES_6WKAVG_EXCL_PROMO_QTY
        ,SALES_6WK 
        ,SALES_6WKAVG_EXCL_PROMO 
        ,SALES_6WK_MARGIN 
        ,WASTE_6WK_PROMO_COST
        ,wreward_sales_excl_vat
        ,wrewards_sales_unit
        ,total_wrewards_discount 
        ,rl.REG_RSP as RULING_RSP        
        ,dl.WH_FD_ZONE_NO
        ,di.FD_DISCIPLINE_TYPE
        ,rl.REG_RSP        
        ,rl.prom_rsp
    from  selunion se, dim_location dl, dim_item di, rtl_location_item rl
--        , dim_item_uda uda
    where se.sk1_location_no = dl.sk1_location_no
    and   se.sk1_item_no = di.sk1_item_no
    and   se.sk1_location_no = rl.sk1_location_no(+)
    and   se.sk1_item_no = rl.sk1_item_no(+);
--    and   se.sk1_item_no = uda.sk1_item_no(+)
--    order by Fin_Year_no,  Fin_week_no,location_no,item_no;

    g_recs_read := g_recs_read + sql%rowcount;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;
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

end wh_prf_corp_861u_old;
