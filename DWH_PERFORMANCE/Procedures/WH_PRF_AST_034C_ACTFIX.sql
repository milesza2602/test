--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034C_ACTFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034C_ACTFIX" 
(P_FORALL_LIMIT in integer,P_SUCCESS OUT BOOLEAN)
as

--**************************************************************************************************
--
-- Fix version of WH_PRF_AST_034C - fix actuals values
--
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Wendy Lyttle
--
--  Purpose:     Create Style-colour datamart for C&GM
--
--
--  Tables:      Input  - dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act (ACTUAL AND PRE-ACTUAL)
--                        dwh_performance.mart_ch_ast_chn_grd_sc_wk (Current Mart)
--                        dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act (GEO SEASON SALES AND STOCK)
--               Output - mart_ch_ast_chn_grd_sc_wk_dtfx
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  May 2013 - wendy - new version of wh_prf_ast_034u
--                   - write to mart_ch_ast_chn_grd_sc_wk
--  june 2013 - wendy - fin year_end rollover
--            - VERSION 18 JUNE 2013
--  21 June 2013 - release to prd
--  November 11 2013 - 4878 -Enhancement:Style Card - Filter on Chain 10 for the Availability measure
--                           where area_no not in (9965, 8800, 9978, 9979, 9953) AND CHAIN_NO = 10
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************


g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;

g_date                    date;
g_6wk_bck_start_date      date;
g_curr_season_start_date  date;
g_curr_season_end_date  date;
g_curr_half_wk_start_date date;
g_date_minus_84           date;

g_fin_year_no             number;
g_fin_half_no             number;

g_fin_year_no_new            number;
g_fin_half_no_new            number;

g_start_fin_week_no       number;
g_end_fin_week_no         number;
g_curr_season_fin_week_no number;
g_start_fin_week_date     date;
g_this_week_start_date    date;

g_new_season_start_date date;
g_new_season_fin_week_no number;
g_new_half_wk_start_date date;
g_new_season_end_date date;

g_sub                 number;
g_sub_end                   number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034C_ACTFIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DATAMART - DATAFIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
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
    L_TEXT := 'LOAD OF mart_ch_ast_chn_grd_sc_wk_dtfx STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DWH_LOG.INSERT_LOG_SUMMARY(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_DESCRIPTION,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
--
-- S T E P 1 : setup dates required for processing
--
--**************************************************************************************************

    execute immediate 'truncate table dwh_datafix.mart_ch_ast_chn_grd_sc_wk_dtfx';
    L_TEXT := 'truncate table dwh_datafix.mart_ch_ast_chn_grd_sc_wk_dtfx';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- For this datafix, we need to set the G_DATE = what would have been the batch_date
-- eg.
--29 DEC 2013
--5 JAN 2014
--12 JAN 2014
--19 JAN 2014
--26 JAN 2014
-- 02 feb 2014 --> 27 to 32
-- 16 mar 2014 --> 33 to 38
-- 27 apr 2014 --> 39 to 44
-- 25 may 2014 --> 45 to 48

G_DATE := '27 Apr 2014';
    l_text := 'BATCH RUN DATE  - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



-- The loop will start at the g_date but roll forward
-- we need to set the no. of loops.
-- eg.
-- If we want it to run for six weeks then G_SUB_END = 6 -1

g_sub_end := 1;
g_date := g_date + 1;

for g_sub in 0..G_SUB_END loop
    
    l_text := '---------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --- the reason for the 42*g_sub is that we will process 6 weeks at a time
    g_date := g_date + (7 * g_sub);
    
    
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_date_minus_84 := g_date - (7 * 12);

    select this_week_start_date,    this_week_start_date - 42,     fin_week_no
     into   g_this_week_start_date,  g_6wk_bck_start_date,          g_end_fin_week_no
    from   dim_calendar
    where  calendar_date = g_date;

    select fin_half_no,       fin_year_no
    into   g_fin_half_no,     g_fin_year_no
    from   DIM_CALENDAR
    where  calendar_date = g_6wk_bck_start_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_curr_season_start_date,             g_curr_season_fin_week_no,    g_curr_half_wk_start_date, g_curr_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no
    and   fin_year_no = g_fin_year_no;

--
-- This part caters for the situatrion at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    select fin_half_no,       fin_year_no
    into   g_fin_half_no_new,     g_fin_year_no_new
    from   DIM_CALENDAR
    where  calendar_date = g_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_new_season_start_date,             g_new_season_fin_week_no,    g_new_half_wk_start_date, g_new_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no_new
    and   fin_year_no = g_fin_year_no_new;


    SELECT THIS_WEEK_START_DATE,         FIN_WEEK_NO
     into  g_start_fin_week_date,        g_start_fin_week_no
    from   DIM_CALENDAR
    where  CALENDAR_DATE = case when g_6wk_bck_start_date < G_CURR_SEASON_START_DATE
                                          then G_CURR_SEASON_START_DATE
                                else
                                          g_6wk_bck_start_date
                                end;


    L_TEXT := 'g_this_week_start_date = '||G_THIS_WEEK_START_DATE||' g_6wk_bck_start_date = '||g_6wk_bck_start_date;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_end_fin_week_no = '||G_END_FIN_WEEK_NO||' g_fin_half_no = '||g_fin_half_no||' g_fin_year_no = '||g_fin_year_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_curr_season_start_date = '||g_curr_season_start_date||' g_curr_season_fin_week_no = '||g_curr_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   L_TEXT := 'g_curr_half_wk_start_date = '||g_curr_half_wk_start_date||' g_start_fin_week_date = '||G_START_FIN_WEEK_DATE;
       DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_start_fin_week_no = '||G_START_FIN_WEEK_NO;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_new_season_start_date = '||g_new_season_start_date||' g_new_season_fin_week_no = '||g_new_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--**************************************************************************************************
--
-- S T E P 2 : create MART table
--
--**************************************************************************************************


    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    insert /*+append */ into  dwh_datafix.mart_ch_ast_chn_grd_sc_wk_dtfx m
      with
    --------------------------------------------------------------------------------
    -- last completed week
    --------------------------------------------------------------------------------
      cal as (select distinct fin_year_no, fin_week_no, fin_week_code, this_week_start_date, this_week_end_date
              from   dim_calendar
              where  this_week_start_date >= g_start_fin_week_date
              and    this_week_start_date < g_this_week_start_date) ,

      selmart as (select sk1_chain_no,
                         sk1_grade_no,
                         sk1_style_colour_no,
                         fin_year_no,
                         fin_week_no,
                         this_week_start_date,
                          chain_intake_margin_act                                  chain_intake_margin_act,
                          sales_qty_6wk_act                                        sales_qty_6wk_act,
                          sales_6wk_act                                            sales_6wk_act,
                          target_stock_selling_6wk_act                             target_stock_selling_6wk_act,
                          chain_intake_selling_act                                 chain_intake_selling_act,
                          chain_intake_selling_std_act                             chain_intake_selling_std_act,
                          chain_intake_margin_std_act                              chain_intake_margin_std_act,
                          NUM_AVAIL_DAYS_STD_ACT                                   NUM_AVAIL_DAYS_STD_ACT,
                          NUM_CATLG_DAYS_STD_ACT                                   NUM_CATLG_DAYS_STD_ACT
              from   dwh_performance.mart_ch_ast_chn_grd_sc_wk
              where  this_week_start_date >= g_start_fin_week_date
              and    this_week_start_date < g_this_week_start_date) ,
    --------------------------------------------------------------------------------
    -- actual and pre-act -- comment AJ must not go 6 wks back past start of new season
    --------------------------------------------------------------------------------
      act as (select /*+parallel (a,2) */
                     a.sk1_chain_no,
                     a.sk1_grade_no,
                     a.sk1_style_colour_no,
                     a.fin_year_no,
                     a.fin_week_no,
                     cal.this_week_start_date,
                     max(case when a.sk1_plan_type_no = 63 then nvl(a.cost_price,0) else 0 end)     cost_price_act, -- AJ 18/1/13
                     max(case when a.sk1_plan_type_no = 63 then nvl(a.selling_price,0) else 0 end)  selling_price,  -- AJ 18/1/13
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales_qty,0) else 0 end)      sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 64 then nvl(a.sales_qty,0) else 0 end)      sales_planned_qty_pre_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_qty,0) else 0 end) target_stock_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_selling,0) else 0 end) target_stock_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales,0) else 0 end)          sales_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.aps_sales_qty,0) else 0 end)  aps_sales_qty,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(store_count,0) else 0 end)      store_count_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_selling,0) else 0 end) prom_sales_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_qty,0) else 0 end)   prom_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_qty,0) else 0 end)  clear_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_selling,0) else 0 end) clear_sales_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_qty,0) else 0 end)    reg_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_selling,0) else 0 end) reg_sales_selling_act
              from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act a
              join
                  cal
                  on a.fin_year_no = cal.fin_year_no
                 and a.fin_week_no = cal.fin_week_no
              where  a.sk1_plan_type_no in (63, 64)
  --     and   sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)

              group by a.sk1_chain_no,
                       a.sk1_grade_no,
                       a.sk1_style_colour_no,
                       a.fin_year_no,
                       a.fin_week_no,
                       cal.this_week_start_date),
    
    --------------------------------------------------------------------------------
    --- season geo to date
    --
-- The UNION caters for the situation at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    --------------------------------------------------------------------------------
 selgdt as
              (
              select distinct dc1.fin_week_no ,
                                dc1.fin_year_no,
                                dc1.this_week_start_date,
                                dc1.fin_half_end_date
              from dim_calendar dc1
              where dc1.this_week_start_date >=
            g_curr_season_start_date
         --   '24/DEC/12'
              and dc1.this_week_start_date <   g_this_week_start_date
        --      '8/jul/13'
              )
              ,
      selgeowk as
               (
                select /*+parallel(geowk,2)*/
                          distinct
                          sk1_chain_no,
                          sk1_geography_no,
                                    sk1_grade_no,
                                    sk1_style_colour_no,
                                    geowk.fin_year_no,
                                    geowk.fin_week_no,
                                    SK1_PLAN_TYPE_NO ,
                                    this_week_start_date,
                                    fin_half_end_date,
                                    sales_qty,
                                    sales,
                                    store_intake_selling,
                                    selling_price,
                                    sales_cost,
                                    target_stock_qty
                from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act geowk, dim_calendar dcgeowk
                where sk1_plan_type_no    = 63
                  and dcgeowk.this_week_start_date >=   g_curr_season_start_date
            --'24/DEC/12'
                  and dcgeowk.this_week_start_date   < g_this_week_start_date
             -- '08/JUL/13'
                  and geowk.fin_year_no   = dcgeowk.fin_year_no
                  and geowk.fin_week_no   = dcgeowk.fin_week_no

--                  and geowk.fin_year_no   = g_fin_year_no -- start of season year
--                 and geowk.fin_week_no   >= g_curr_season_fin_week_no   -- start of season week
--                  and geowk.fin_week_no   < g_end_fin_week_no
--                  and geowk.fin_year_no   = dcgeowk.fin_year_no
--                  and geowk.fin_week_no   = dcgeowk.fin_week_no
                       --    and sk1_style_colour_no in(727553,727553,5502589,16420002)
     --                           and sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)
                ),
      ssng as
                (select sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no                                                   fin_year_no ,
                        gdt.fin_week_no                                                   fin_week_no ,
                        gdt.this_week_start_date                                          this_week_start_date,
                        sum(nvl(sales_qty,0))                                             sales_qty_std_act,
                        sum(nvl(sales,0))                                                 sales_std_act,
                        sum(nvl(store_intake_selling,0))                                  store_intake_selling_std_act,
                        max(case when selling_price = 0 then null else selling_price end) selling_price_std,
                        sum(nvl(sales_cost,0))                                            sales_cost_std_act,
                        sum(nvl(target_stock_qty,0))                                      target_stock_qty_std_act
                from  selgdt gdt ,
                      selgeowk gwk
                where gdt.this_week_start_date >= gwk.this_week_start_date
                and gdt.fin_half_end_date = gwk.fin_half_end_date
                and gdt.this_week_start_date between  g_curr_season_start_date and g_curr_season_end_date
                group by sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no ,
                        gdt.fin_week_no,
                        gdt.this_week_start_date
                union
                select sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no                                                   fin_year_no ,
                        gdt.fin_week_no                                                   fin_week_no ,
                        gdt.this_week_start_date                                          this_week_start_date,
                        sum(nvl(sales_qty,0))                                             sales_qty_std_act,
                        sum(nvl(sales,0))                                                 sales_std_act,
                        sum(nvl(store_intake_selling,0))                                  store_intake_selling_std_act,
                        max(case when selling_price = 0 then null else selling_price end) selling_price_std,
                        sum(nvl(sales_cost,0))                                            sales_cost_std_act,
                        sum(nvl(target_stock_qty,0))                                      target_stock_qty_std_act
                from  selgdt gdt ,
                      selgeowk gwk
                where gdt.this_week_start_date >= gwk.this_week_start_date
                and gdt.fin_half_end_date = gwk.fin_half_end_date
                and gdt.this_week_start_date between  g_new_season_start_date and g_new_season_end_date
                group by sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no ,
                        gdt.fin_week_no,
                        gdt.this_week_start_date
                 ) ,
     ssngval as
                (            select sg.sk1_chain_no                                                  sk1_chain_no,
                        sg.sk1_grade_no                                                  sk1_grade_no ,
                        sg.sk1_style_colour_no                                           sk1_style_colour_no ,
                        sg.fin_year_no                                                   fin_year_no,
                        sg.fin_week_no                                                   fin_week_no ,
                        sg.this_week_start_date                                          this_week_start_date,
                        sg.sales_qty_std_act                                             sales_qty_std_act  ,
                        sg.sales_std_act                                                 sales_std_act,
                        sg.store_intake_selling_std_act                                  store_intake_selling_std_act,
                        sg.selling_price_std                                             selling_price_std ,
                        sg.sales_cost_std_act                                            sales_cost_std_act ,
                        sg.target_stock_qty_std_act                                      target_stock_qty_std_act
                from  ssng sg
                where sg.this_week_start_date >= g_6wk_bck_start_date
              --  '3/june/13'
    --           and sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)
                )
                

     ---************************************************************************************---
     --***************
    ---         MAIN JOIN OF ALL DATA EXTRACTS ABOVE
     --***************
     ---************************************************************************************---
     select 
               nvl(ac.sk1_chain_no,nvl(sm.sk1_chain_no, sg.sk1_chain_no))                 sk1_chain_no,
               nvl(ac.sk1_grade_no,nvl(sm.sk1_grade_no,  sg.sk1_grade_no))                                        sk1_grade_no,
               nvl(ac.sk1_style_colour_no,nvl(sm.sk1_style_colour_no,   sg.sk1_style_colour_no))     sk1_style_colour_no,
               nvl(ac.fin_year_no,nvl(sm.fin_year_no,  sg.fin_year_no))                                              fin_year_no,
               nvl(ac.fin_week_no,nvl(sm.fin_week_no,  sg.fin_week_no))                                              fin_week_no,
               nvl(ac.this_week_start_date,nvl(sm.this_week_start_date,  sg.this_week_start_date)) this_week_start_date,
               round((max(selling_price) / 1.14),2)                                               reg_rsp_excl_vat,
               round(sum(NVL(sales_act,0)),2)                                                     sales_act,
               round(max(cost_price_act),2)                                                       cost_price_act,
               round(max(selling_price),2)                                                        selling_rsp_act,
            chain_intake_margin_act                                  chain_intake_margin_act,
               round(sum(NVL(sales_qty_act,0)),2)                                                 sales_qty_act,
               round(sum(NVL(sales_planned_qty_pre_act,0)),2)                                     sales_planned_qty_pre_act,
               round(sum(NVL(target_stock_qty_act,0)),2)                                          target_stock_qty_act,
               round(sum(NVL(aps_sales_qty,0)) / greatest(sum(NVL(store_count_act,0)),1),2)       aps_sales_qty_act,
               round(sum(NVL(sales_std_act,0)),2)                                                 sales_std_act,
               round(sum(NVL(sales_std_act,0)) - sum(NVL(sales_cost_std_act,0)),2)                sales_margin_std_act,
               round(sum(NVL(store_intake_selling_std_act,0)),2)                                  store_intake_selling_std_act,
               round(sum(NVL(sales_qty_std_act,0)),2)                                             sales_qty_std_act,
               round(sum(NVL(target_stock_qty_act,0)),2)                                          target_stock_qty_std_act,
            sales_qty_6wk_act                                        sales_qty_6wk_act,
            sales_6wk_act                                            sales_6wk_act,
            target_stock_selling_6wk_act                             target_stock_selling_6wk_act,
               sum(NVL(store_count_act,0))                                                        Store_count_act,
               round(sum(NVL(prom_sales_qty_act,0)),2)                                            prom_sales_qty_act,
               round(sum(NVL(prom_sales_selling_act,0)),2)                                        prom_sales_selling_act,
               round(sum(NVL(clear_sales_qty_act,0)),2)                                           clear_sales_qty_act,
               round(sum(NVL(clear_sales_selling_act,0)),2)                                       clear_sales_selling_act,
               round(sum(NVL(target_stock_selling_act,0)),2)                                      target_stock_selling_act,
               round(sum(NVL(reg_sales_qty_act,0)),2)                                             reg_sales_qty_act,
               round(sum(NVL(reg_sales_selling_act,0)),2)                                         reg_sales_selling_act,
               g_date                                                                    last_updated_date,
            NUM_AVAIL_DAYS_STD_ACT                                   NUM_AVAIL_DAYS_STD_ACT,
            NUM_CATLG_DAYS_STD_ACT                                   NUM_CATLG_DAYS_STD_ACT,
            chain_intake_selling_act                                 chain_intake_selling_act,
            chain_intake_selling_std_act                             chain_intake_selling_std_act,
            chain_intake_margin_std_act                              chain_intake_margin_std_act
     from act ac
     full outer join ssngval sg
                on sg.sk1_chain_no = ac.sk1_chain_no
                   and sg.sk1_grade_no = ac.sk1_grade_no
                   and sg.sk1_style_colour_no = ac.sk1_style_colour_no
                   and sg.fin_year_no = ac.fin_year_no
                   and sg.fin_week_no = ac.fin_week_no
     full outer join selmart sm
                on sm.sk1_chain_no = sg.sk1_chain_no
                   and sm.sk1_grade_no = sg.sk1_grade_no
                   and sm.sk1_style_colour_no = sg.sk1_style_colour_no
                   and sm.fin_year_no = sg.fin_year_no
                   and sm.fin_week_no = sg.fin_week_no
      group by
               nvl(ac.sk1_chain_no,nvl(sm.sk1_chain_no, sg.sk1_chain_no))              ,
               nvl(ac.sk1_grade_no,nvl(sm.sk1_grade_no,  sg.sk1_grade_no))                                     ,
               nvl(ac.sk1_style_colour_no,nvl(sm.sk1_style_colour_no,   sg.sk1_style_colour_no))     ,
               nvl(ac.fin_year_no,nvl(sm.fin_year_no,  sg.fin_year_no))                                            ,
               nvl(ac.fin_week_no,nvl(sm.fin_week_no,  sg.fin_week_no))                                          ,
               nvl(ac.this_week_start_date,nvl(sm.this_week_start_date,  sg.this_week_start_date)) ,
            chain_intake_margin_act              ,
            sales_qty_6wk_act                                     ,
            sales_6wk_act                                       ,
            target_stock_selling_6wk_act                         ,
            g_date,
            NUM_AVAIL_DAYS_STD_ACT                              ,
            NUM_CATLG_DAYS_STD_ACT                                ,
            chain_intake_selling_act                              ,
            chain_intake_selling_std_act                          ,
            chain_intake_margin_std_act                           
               ;

    g_recs_inserted := sql%rowcount;
    L_TEXT := 'Recs inserted into mart_ch_ast_chn_grd_sc_wk_dtfx = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 -- g_recs_read := g_recs_read + SQL%ROWCOUNT;
 -- g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
 
 
 
 
    commit;




end loop;
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


END WH_PRF_AST_034C_ACTFIX;
