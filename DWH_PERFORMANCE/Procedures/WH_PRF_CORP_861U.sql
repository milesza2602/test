--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_861U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_861U" (p_forall_limit in integer,p_success out boolean) as

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
--    23 Sep 2016:  Additional column WREWARD_SALES_INCL_VAT required
--                  Modify functionality. The field WREWARD_SALES_EXCL_VAT does not currently exclude VAT. This
--                  needs to be swapped with the new column.
--    30 Sep 2016:  Code to remove & reload previous 6 week load periods depandant on current g_date & in line with extract module 860U:
--                  if G_DATE is a Monday (day 1) then remove & reload previous 6 weeks (current week is not yet available)
--                  if G_DATE is Tues to Sun (day 2 to 7) then remove & reload previous 5 weeks and remove & reload current week
--                  B Kirschner - REF: BK30Sep2016
--    14 Nov 2017   Add additional measures as well as source pricing info from PRICE table and not LOCATION ITEM. Also
--                  changed log file from WH_PRF_CORP_861U_TMP to WH_PRF_CORP_861U
--                  A Joshua Chg-10153
--    10 Apr 2018   Add 2 additional customer availability measures from the CATALOG table.
--                  A Joshua Chg-13604
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
--g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_date_adhoc         date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_loop_start_date    date;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dwh_performance.dim_calendar.fin_day_no%type;
g_wkday              number       :=  0;                                        -- BK30Sep2016

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
    sys.dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
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
    
    -- set exptract period depandant on day number of week.
    select today_fin_day_no into g_wkday from dwh_performance.dim_control_report;               -- BK30Sep2016        


    if g_wkday = 1 then
       g_loop_cnt := 5;
    else 
       g_loop_cnt := 6;
    end if;
    
   --A. Remove previous load periods for reload ...
    begin
       for g_sub in 1 .. g_loop_cnt                                                      -- BK30Sep2016                                                
         loop         
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (g_date) - (g_sub * 7);  
        
         execute immediate 'alter table '|| 'dwh_performance' || '.'|| 'MART_FDS_LOC_ITEM_WK_TABLEAU' ||' truncate subpartition for ('||G_LOOP_FIN_YEAR_NO||','||G_LOOP_FIN_WEEK_NO||')';
         l_text := 'Truncate Partition = '||g_loop_fin_year_no||' - '||g_loop_fin_week_no;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         commit;
         end loop;
    end;

    g_loop_cnt := 0;
    -- B. Re-load periods as per extract (860U) ...
    select distinct count(distinct this_week_start_date) into g_loop_cnt -->--> 6 week build of data
    from dwh_performance.dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate -7);
    
    for g_sub in 1..g_loop_cnt loop
           -- load previous weeks (1 to 6) except if day 2 to 7 then load previous weeks 1 to 5 only ...
           select distinct fin_year_no, fin_week_no, fin_month_no, fin_quarter_no, this_week_start_date
           into   g_fin_year_no, g_fin_week_no, g_fin_month_no, g_fin_quarter_no, g_this_week_start_date
           from   dwh_performance.dim_calendar
           where  calendar_date = trunc(sysdate) - (g_sub * 7);

        l_text := 'Rollup = '||g_fin_year_no||' - '||g_fin_week_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /*+ APPEND parallel (t,6) */ into MART_FDS_LOC_ITEM_WK_TABLEAU t
   
     with selunion as 
        (  select    sk1_location_no
                    ,sk1_item_no
                    ,fin_year_no
                    ,fin_week_no 
                    ,sum(nvl(sales,0)) sales
                    ,sum(nvl(sales_qty,0)) sales_qty
                    ,sum(nvl(sales_cost,0)) sales_cost
                    ,sum(nvl(sales_margin,0)) sales_margin 
                    ,sum(nvl(spec_dept_revenue,0)) spec_dept_revenue 
                    ,sum(nvl(spec_dept_qty,0)) spec_dept_qty 
                    ,sum(nvl(prom_sales,0)) prom_sales
                    ,sum(nvl(prom_sales_qty,0)) prom_sales_qty
                    ,sum(nvl(prom_sales_margin,0)) prom_sales_margin
                    ,sum(nvl(waste_cost,0)) waste_cost
                    ,sum(nvl(waste_qty,0)) waste_qty
                    ,sum(nvl(fd_num_catlg_days,0)) fd_num_catlg_days
                    ,sum(nvl(fd_num_avail_days,0)) fd_num_avail_days
                    ,sum(nvl(fd_num_catlg_days_adj,0)) fd_num_catlg_days_adj
                    ,sum(nvl(fd_num_avail_days_adj,0)) fd_num_avail_days_adj
                    ,sum(nvl(online_sales,0)) online_sales
                    ,sum(nvl(online_sales_qty,0))online_sales_qty
                    ,sum(nvl(online_sales_cost,0)) online_sales_cost
                    ,sum(nvl(online_sales_margin,0))online_sales_margin
                    ,sum(nvl(sales_6wk_qty,0))sales_6wk_qty
                    ,sum(nvl(sales_6wkavg_excl_promo_qty,0)) sales_6wkavg_excl_promo_qty
                    ,sum(nvl(sales_6wk,0)) sales_6wk
                    ,sum(nvl(sales_6wkavg_excl_promo,0)) sales_6wkavg_excl_promo 
                    ,sum(nvl(sales_6wk_margin,0)) sales_6wk_margin
                    ,sum(nvl(waste_6wk_cost,0)) waste_6wk_cost
                    ,sum(nvl(wreward_sales_excl_vat,0)) wreward_sales_excl_vat
                    ,sum(nvl(wrewards_sales_unit1,0) - nvl(wrewards_sales_unit2,0)) wrewards_sales_unit
                    ,sum(nvl(total_wrewards_discount,0)) total_wrewards_discount
                    ,max(nvl(ruling_rsp,0)) ruling_rsp
                    ,max(nvl(prom_rsp,0)) prom_rsp
                    ,sum(nvl(wreward_sales_excl_vat,0)) wreward_sales_incl_vat
                    ,sum(nvl(sales_6wkavg_qty,0)) sales_6wkavg_qty
                    ,sum(nvl(sales_6wkavg,0)) sales_6wkavg
                    ,sum(nvl(sales_6wkavg_margin,0)) sales_6wkavg_margin
                    ,sum(nvl(waste_6wkavg_cost,0)) waste_6wkavg_cost                    
                    ,sum(nvl(this_wk_catalog_ind,0)) this_wk_catalog_ind
                    ,sum(nvl(fd_num_cust_avail_adj,0)) fd_num_cust_avail_adj
                    ,sum(nvl(fd_num_cust_avail_perc,0)) fd_num_cust_avail_perc
                    ,sum(nvl(rate_of_sale,0)) rate_of_sale
                    ,max(nvl(like_for_like_ind,0)) like_for_like_ind
                    ,max(nvl(reg_rsp,0)) reg_rsp
                    ,sum(nvl(sales_6wkavg_margin_excl_promo,0)) sales_6wkavg_margin_excl_promo
                    ,sum(nvl(fd_num_cust_catlg_adj,0))  fd_num_cust_catlg_adj -- chg-13604
                    ,SUM(NVL(fd_cust_avail,0))          fd_cust_avail -- chg-13604
           from (
--Sales Information
                          select /*+ parallel (a,6) full(a) */
                                  sk1_location_no
                                 ,sk1_item_no
                                 ,fin_year_no
                                 ,fin_week_no
                                 ,sales
                                 ,sales_qty
                                 ,sales_cost
                                 ,sales_margin 
                                 ,null spec_dept_revenue
                                 ,null spec_dept_qty 
                                 ,null prom_sales
                                 ,null prom_sales_qty
                                 ,null prom_sales_margin
                                 ,null waste_cost
                                 ,null waste_qty
                                 ,null fd_num_catlg_days
                                 ,null fd_num_avail_days
                                 ,null fd_num_catlg_days_adj
                                 ,null fd_num_avail_days_adj
                                 ,null online_sales
                                 ,null online_sales_qty
                                 ,null online_sales_cost
                                 ,null online_sales_margin  
                                 ,null sales_6wk_qty
                                 ,null sales_6wkavg_excl_promo_qty
                                 ,null sales_6wk
                                 ,null sales_6wkavg_excl_promo
                                 ,null sales_6wk_margin
                                 ,null waste_6wk_cost
                                 ,null wreward_sales_excl_vat
                                 ,null wrewards_sales_unit1
                                 ,null wrewards_sales_unit2
                                 ,null total_wrewards_discount
                                 ,null ruling_rsp
                                 ,null prom_rsp
                                 ,null sales_6wkavg_qty
                                 ,null sales_6wkavg
                                 ,null sales_6wkavg_margin
                                 ,null waste_6wkavg_cost
                                 ,null this_wk_catalog_ind
                                 ,null fd_num_cust_avail_adj
                                 ,null fd_num_cust_avail_perc
                                 ,null rate_of_sale
                                 ,null like_for_like_ind
                                 ,null reg_rsp
                                 ,null sales_6wkavg_margin_excl_promo
                                 ,null fd_num_cust_catlg_adj -- chg-13604
                                 ,null fd_cust_avail -- chg-13604    
                          from   dwh_performance.temp_foods_tab_2yrs_dense a
                          where  fin_year_no = g_fin_year_no
                           and   fin_week_no = g_fin_week_no
 
                          union all 
-- Sparse information
                          select /*+ parallel (a,6) full(a) */
                                  sk1_location_no
                                 ,sk1_item_no
                                 ,fin_year_no
                                 ,fin_week_no
                                 ,null sales
                                 ,null sales_qty
                                 ,null sales_cost
                                 ,null sales_margin 
                                 ,null spec_dept_revenue 
                                 ,null spec_dept_qty 
                                 ,prom_sales
                                 ,prom_sales_qty
                                 ,prom_sales_margin
                                 ,waste_cost
                                 ,waste_qty
                                 ,null fd_num_catlg_days
                                 ,null fd_num_avail_days
                                 ,null fd_num_catlg_days_adj
                                 ,null fd_num_avail_days_adj
                                 ,null online_sales
                                 ,null online_sales_qty
                                 ,null online_sales_cost
                                 ,null online_sales_margin  
                                 ,null sales_6wk_qty 
                                 ,null sales_6wkavg_excl_promo_qty
                                 ,null sales_6wk 
                                 ,null sales_6wkavg_excl_promo 
                                 ,null sales_6wk_margin 
                                 ,null waste_6wk_cost
                                 ,null wreward_sales_excl_vat
                                 ,null wrewards_sales_unit1
                                 ,null wrewards_sales_unit2
                                 ,null total_wrewards_discount
                                 ,null ruling_rsp
                                 ,null prom_rsp
                                 ,null sales_6wkavg_qty
                                 ,null sales_6wkavg
                                 ,null sales_6wkavg_margin
                                 ,null waste_6wkavg_cost
                                 ,null this_wk_catalog_ind
                                 ,null fd_num_cust_avail_adj
                                 ,null fd_num_cust_avail_perc
                                 ,null rate_of_sale
                                 ,null like_for_like_ind
                                 ,null reg_rsp
                                 ,null sales_6wkavg_margin_excl_promo
                                 ,null fd_num_cust_catlg_adj -- chg-13604
                                 ,null fd_cust_avail -- chg-13604    
                          from   dwh_performance.temp_foods_tab_2yrs_sparse a
                          where  fin_year_no = g_fin_year_no
                           and   fin_week_no = g_fin_week_no
                                  
                          union all 
-- Online Sales
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue 
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            ,   online_sales
                                            ,  online_sales_qty
                                            ,    online_sales_cost
                                            ,  online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null  wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            ,  null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604    
                                  from dwh_performance.temp_foods_tab_2yrs_wwo_sale a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no                                  
                                  
                          union all 
-- Catalogue Information
                                   select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            ,   fd_num_catlg_days
                                            ,    fd_num_avail_days
                                            ,   fd_num_catlg_days_adj
                                            ,   fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null  wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            , null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , this_wk_catalog_ind
                                            , fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , fd_num_cust_catlg_adj -- chg-13604
                                            , fd_cust_avail -- chg-13604
                                  from TEMP_FOODS_TAB_2YRS_CATALOG a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
                       
                          union all 
-- POS JV information
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            ,  spec_dept_revenue
                                            ,   spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null  wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            ,  null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
                                  from dwh_performance.temp_foods_tab_2yrs_pos_jv a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
                                  
                          union all 
-- FPP information
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue 
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            ,  sales_6wk_qty 
                                            ,    sales_6wkavg_excl_promo_qty
                                            ,    sales_6wk 
                                            ,   sales_6wkavg_excl_promo 
                                            ,   sales_6wk_margin 
                                            ,   waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null  wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , sales_6wkavg_qty
                                            , sales_6wkavg
                                            , sales_6wkavg_margin
                                            , waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
--                                 from dwh_performance.temp_foods_tab_2yrs_mart a
                                 from dwh_performance.rtl_loc_item_wk_sales_6wkavg a
--                                 from dwh_datafix.bk_loc_item_wk_sales_6wkavg a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
                                
                          union all 
-- Wreward information
                                  select /*+ parallel (a,6) full(a) */
                                              sk1_location_no
                                            , sk1_item_no
                                            , fin_year_no
                                            , fin_week_no
                                            , null sales
                                            , null sales_qty
                                            , null sales_cost
                                            , null sales_margin 
                                            , null spec_dept_revenue 
                                            , null spec_dept_qty 
                                            , null prom_sales
                                            , null prom_sales_qty
                                            , null prom_sales_margin
                                            , null waste_cost
                                            , null waste_qty
                                            , null fd_num_catlg_days
                                            , null fd_num_avail_days
                                            , null fd_num_catlg_days_adj
                                            , null fd_num_avail_days_adj
                                            , null online_sales
                                            , null online_sales_qty
                                            , null online_sales_cost
                                            , null online_sales_margin  
                                            , null sales_6wk_qty 
                                            , null sales_6wkavg_excl_promo_qty
                                            , null sales_6wk 
                                            , null sales_6wkavg_excl_promo 
                                            , null sales_6wk_margin 
                                            , null waste_6wk_cost
                                            , wreward_sales_excl_vat
                                            , wrewards_sales_unit1
                                            , wrewards_sales_unit2
                                            , total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            , null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
                                  from dwh_performance.temp_foods_tab_2yrs_cust a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
          
-- Rate of Sales Information
                        union all 
   
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue 
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            , null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , avg_units_per_day as rate_of_sale
                                            , null like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
                                  from TEMP_FOODS_TAB_RATE_OF_SALE a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
-- Like4Like Information
                        union all 
                       
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue 
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , null ruling_rsp
                                            , null prom_rsp
                                            , null sales_6wkavg_qty
                                            , null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , like_for_like_ind
                                            , null reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
                                  from TEMP_FOODS_TAB_LIKE_FOR_LIKE a                                                   
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no
-- Pricing Information
                        union all 
                        
                                  select /*+ parallel (a,6) full(a) */
                                            sk1_location_no
                                            ,sk1_item_no
                                            ,fin_year_no
                                            ,  fin_week_no
                                            , null  sales
                                            , null  sales_qty
                                            ,  null sales_cost
                                            , null  sales_margin 
                                            , null spec_dept_revenue 
                                            , null  spec_dept_qty 
                                            , null  prom_sales
                                            ,  null  prom_sales_qty
                                            , null  prom_sales_margin
                                            , null  waste_cost
                                            ,  null  waste_qty
                                            , null  fd_num_catlg_days
                                            ,  null  fd_num_avail_days
                                            ,  null  fd_num_catlg_days_adj
                                            , null  fd_num_avail_days_adj
                                            , null  online_sales
                                            , null online_sales_qty
                                            ,  null  online_sales_cost
                                            ,  null online_sales_margin  
                                            , null sales_6wk_qty 
                                            ,  null  sales_6wkavg_excl_promo_qty
                                            ,  null  sales_6wk 
                                            , null  sales_6wkavg_excl_promo 
                                            , null  sales_6wk_margin 
                                            , null  waste_6wk_cost
                                            , null  wreward_sales_excl_vat
                                            , null  wrewards_sales_unit1
                                            , null wrewards_sales_unit2
                                            , null  total_wrewards_discount
                                            , ruling_rsp                                            
                                            , prom_rsp
                                            , null sales_6wkavg_qty
                                            , null sales_6wkavg
                                            , null sales_6wkavg_margin
                                            , null waste_6wkavg_cost
                                            , null this_wk_catalog_ind
                                            , null fd_num_cust_avail_adj
                                            , null fd_num_cust_avail_perc
                                            , null rate_of_sale
                                            , null like_for_like_ind
                                            , reg_rsp
                                            , null sales_6wkavg_margin_excl_promo
                                            , null fd_num_cust_catlg_adj -- chg-13604
                                            , null fd_cust_avail -- chg-13604  
                                  from TEMP_FOODS_TAB_PRICE a
                                  where fin_year_no = g_fin_year_no
                                  and fin_week_no = g_fin_week_no 
                        )       

          group by   sk1_location_no
                    ,sk1_item_no
                    ,fin_year_no
                    ,fin_week_no )
                    
    select  /*+ parallel(rl,4), parallel(se,4), full (rl) full (di) full (dl) */
         location_no
        ,di.item_no
        ,fin_year_no
        ,fin_week_no
        ,g_this_week_start_date
        ,g_fin_quarter_no
        ,g_fin_month_no 
        ,(fin_year_no*100+fin_week_no) fin_yrwk_no
        ,fd_product_no
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
        ,fd_num_catlg_days
        ,fd_num_avail_days
        ,fd_num_catlg_days_adj
        ,fd_num_avail_days_adj
        ,online_sales
        ,online_sales_qty
        ,online_sales_cost
        ,online_sales_margin  
        ,sales_6wk_qty
        ,sales_6wkavg_excl_promo_qty
        ,sales_6wk 
        ,sales_6wkavg_excl_promo 
        ,sales_6wk_margin 
        ,waste_6wk_cost
        ,case when di.vat_rate_perc > 0 then (wreward_sales_excl_vat/cast('1.'||di.vat_rate_perc as number)) else wreward_sales_excl_vat end wreward_sales_excl_vat
        ,wrewards_sales_unit
        ,total_wrewards_discount 
        ,se.ruling_rsp --rl.reg_rsp as ruling_rsp        
        ,dl.wh_fd_zone_no
        ,di.fd_discipline_type
        ,se.reg_rsp --rl.reg_rsp        
        ,se.prom_rsp --rl.prom_rsp
        ,wreward_sales_incl_vat
        ,sales_6wkavg_qty
        ,sales_6wkavg
        ,sales_6wkavg_margin
        ,waste_6wkavg_cost
        ,se.this_wk_catalog_ind -- new
        ,fd_num_cust_avail_adj
        ,case when fd_num_cust_avail_adj > 0 then (fd_num_catlg_days_adj / fd_num_cust_avail_adj) end fd_num_cust_avail_perc
        ,rate_of_sale
        ,like_for_like_ind
        ,sales_6wkavg_margin_excl_promo
        ,fd_num_cust_catlg_adj -- chg-13604
        ,fd_cust_avail -- chg-13604 
    from  selunion se, dwh_performance.dim_location dl, dwh_performance.dim_item di
    where se.sk1_location_no = dl.sk1_location_no
    and   se.sk1_item_no = di.sk1_item_no;

    g_recs_read := g_recs_read + sql%rowcount;
    g_recs_inserted :=  g_recs_inserted + sql%rowcount;

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

end wh_prf_corp_861u;
