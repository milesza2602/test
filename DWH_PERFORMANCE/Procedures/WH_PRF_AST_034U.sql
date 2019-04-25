--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      Jacqui Pember
--  Purpose:
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_act and rtl_chain_sc_wk_ast_pln
--               Output - mart_ch_ast_chn_grd_sc_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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

g_date                   date;
g_this_week_start_date   date;
g_curr_season_start_date date;

g_fin_year_no          dim_calendar.fin_year_no%type;

g_start_fin_week_no       number;
g_end_fin_week_no         number;
g_curr_season_fin_week_no number;
g_start_fin_week_code     number;
g_end_fin_week_code       number;

g_fin_half_no             number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DATAMART - CHN LEVEL';
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
    l_text := 'LOAD OF mart_ch_ast_chn_grd_sc_wk STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := g_date + 1;
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select to_number(fin_year_no||lpad(fin_week_no,2,'0')), this_week_start_date - 42, fin_week_no
    into   g_end_fin_week_code, g_this_week_start_date, g_end_fin_week_no
    from   dim_calendar
    where  calendar_date = g_date;

    select fin_half_no, fin_year_no
    into   g_fin_half_no, g_fin_year_no
    from   dim_calendar
    where  calendar_date = g_this_week_start_date;

    select distinct min(season_start_date), min(fin_week_no)
    into  g_curr_season_start_date, g_curr_season_fin_week_no
    from  dim_calendar
    where fin_half_no = g_fin_half_no
    and   fin_year_no = g_fin_year_no;

    select to_number(fin_year_no||lpad(fin_week_no,2,'0')), fin_week_no
    into   g_start_fin_week_code, g_start_fin_week_no
    from   dim_calendar
    where  calendar_date = case when g_this_week_start_date < g_curr_season_start_date then g_curr_season_start_date else g_this_week_start_date end;

    l_text := 'Data extract for weeks '||g_curr_season_fin_week_no||' thru '||g_end_fin_week_no||' of year '||g_fin_year_no;

    execute immediate 'truncate table dwh_performance.rtl_sc_continuity_wk';

    execute immediate 'alter session enable parallel dml';

    -- the following provides the date ranges for the calculation of the accumulated 6 week measures
    -- populate global temp table
    insert /*+ parallel(a,6)*/ into rtl_sc_continuity_wk a
          (sk1_style_colour_no,
           continuity_ind,
           season_first_trade_date,
           cont_prev_week_code,
           cont_start_week_code,
           cont_end_week_code,
           fash_start_week_code,
           fash_end_week_code,
           last_updated_date
           )
    select /*+ parallel(d,6) */ sk1_style_colour_no,
           continuity_ind,
           season_first_trade_date,
           cont_prev_week_code,
           cont_start_week_code,
           cont_end_week_code,
           fash_start_week_code,
           fash_end_week_code,
           g_date
    from
      (select /*+ parallel(t,6) */ distinct
               sk1_style_colour_no,
               continuity_ind,
               season_first_trade_date,
               case when continuity_ind = 1 then
                    c3.fin_year_no||lpad(c3.fin_week_no,2,'0')
               else c1.fin_year_no||lpad(c1.fin_week_no,2,'0')
               end  cont_prev_week_code,
               case when continuity_ind=1 then
                         -- set first week to start of season if it's less
                         case when c1.fin_year_no||lpad(c1.fin_week_no,2,'0') < ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0') then--season start week code
                              ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0')
                         else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end
                    else '999999' end cont_start_week_code,
               case when continuity_ind=1 then
                         c2.fin_year_no||lpad(c2.fin_week_no,2,'0')
                    else '999999' end cont_end_week_code,
               min(case when c1.calendar_date < season_first_trade_date then '999999' else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end)
                   over (partition by sk1_style_colour_no) fash_start_week_code,
               few.fin_year_no||lpad(few.fin_week_no,2,'0') fash_end_week_code
       from    rtl_sc_trading t
         join  dim_calendar c1
           on  c1.calendar_date between
                 case when continuity_ind=0 then t.season_first_trade_date else g_date - (7 * 12) end
                 and  g_date
         -- get the rpl end week code (6 weeks from calendar date)
         join  dim_calendar c2
           on  c2.calendar_date = c1.calendar_date + (7*6)
         -- get the previous week's code of the rpl end week code
         join  dim_calendar c3
           on  c3.calendar_date = c2.calendar_date - 7
         -- get the week code for the season start date
         join  dim_calendar ssw
           on  ssw.calendar_date = g_curr_season_start_date
         -- get the fashion end week code
         join  dim_calendar few
           on  few.calendar_date = (case when c1.calendar_date <= season_first_trade_date then season_first_trade_date
                                         when c1.calendar_date > season_first_trade_date + (5*7) then season_first_trade_date + (5*7)
                                         else c1.calendar_date end) + 7
       where   t.season_start_date = g_curr_season_start_date) d

    where  cont_prev_week_code >= g_start_fin_week_code
    and    cont_prev_week_code <  g_end_fin_week_code;

    commit;

    -- create a table with the 6 week measures, which is to be used in the main select statement below.Fastest method of extract the data.
    begin
      execute immediate ('drop table rtl_ch_ast_chn_grd_sc_wk_6w purge');

      exception when others then null; -- ignore if error because the table doesn't exist
    end;

    execute immediate ('create table rtl_ch_ast_chn_grd_sc_wk_6w  parallel 6 as
      select /*+ parallel (a,8) */ distinct
           sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           a.fin_year_no,
           a.fin_week_no,
           a.fin_week_code,
           case when continuity_ind = 0 then sales_qty_6wk_act_f
                else sales_qty_6wk_act end                                                  SALES_QTY_6WK_ACT,
           case when continuity_ind = 0 then sales_6wk_act_f
                else sales_6wk_act end                                                      SALES_6WK_ACT,
           case when continuity_ind = 0 then target_stock_selling_6wk_act_f
                else target_stock_selling_6wk_act end                                       TARGET_STOCK_SELLING_6WK_ACT
      from
          (select /*+  parallel (a,8) */ distinct
                  a.sk1_chain_no,
                  a.sk1_grade_no,
                  a.sk1_style_colour_no,
                  substr(ic.cont_prev_week_code,1,4) fin_year_no,
                  substr(ic.cont_prev_week_code,5,2) fin_week_no,
                  ''W''||ic.cont_prev_week_code fin_week_code,
                  ic.continuity_ind,
                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) < ic.cont_end_week_code then nvl(a.sales_qty,0) else 0 end) over
                     (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) SALES_QTY_6WK_ACT,
                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) < ic.cont_end_week_code then nvl(a.sales,0) else 0 end) over
                     (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) SALES_6WK_ACT,
                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) = ic.cont_prev_week_code then nvl(a.target_stock_selling,0) else 0 end) over
                       (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) TARGET_STOCK_SELLING_6WK_ACT,

                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) < ic.fash_end_week_code then nvl(a.sales_qty,0) else 0 end) over
                     (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) SALES_QTY_6WK_ACT_f,
                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) < ic.fash_end_week_code then nvl(a.sales,0) else 0 end) over
                     (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) SALES_6WK_ACT_f,
                  sum(case when to_number(a.fin_year_no||lpad(a.fin_week_no,2,''0'')) = lw.last_week_code then nvl(a.target_stock_selling,0) else 0 end) over
                       (partition by a.sk1_chain_no, a.sk1_grade_no, a.sk1_style_colour_no, ic.cont_prev_week_code order by a.fin_year_no||lpad(a.fin_week_no,''0'',2)) TARGET_STOCK_SELLING_6WK_ACT_f
             from rtl_chn_geo_grd_sc_wk_ast_act a
             join
                rtl_sc_continuity_wk ic
                  on a.sk1_style_colour_no = ic.sk1_style_colour_no
                  and (a.fin_year_no >= case when continuity_ind=1 then substr(ic.cont_start_week_code,1,4) else substr(ic.fash_start_week_code,1,4) end
                       and a.fin_week_no >= case when continuity_ind=1 then substr(ic.cont_start_week_code,5,2) else substr(ic.fash_start_week_code,5,2) end)
                  and  (a.fin_year_no <= substr(ic.cont_end_week_code,1,4)
                       and a.fin_week_no <= substr(ic.cont_end_week_code,5,2))
             join (select distinct cw.fin_year_no||lpad(cw.fin_week_no,2,''0'') this_week_code, lw.fin_year_no||lpad(lw.fin_week_no,2,''0'') last_week_code
                   from   dim_calendar cw
                     join dim_calendar lw
                       on cw.this_week_start_date - 7 = lw.this_week_start_date) lw
                  on  lw.this_week_code = case when ic.continuity_ind=0 then ic.fash_end_week_code else ic.cont_end_week_code end
              where a.sk1_plan_type_no = 63
              ) a
              join (select distinct fin_week_no, fin_year_no, this_week_start_date,
                           fin_week_code
                     from  dim_calendar c) cal
         on  a.fin_year_no = cal.fin_year_no
         and a.fin_week_no = cal.fin_week_no
    ');

    execute immediate ('alter table rtl_ch_ast_chn_grd_sc_wk_6w noparallel');

    execute immediate (
    'insert /*+ append parallel(m,6) */ into MART_CH_AST_CHN_GRD_SC_WK m
          (sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           fin_year_no,
           fin_week_no,
           fin_week_code,
           reg_rsp_excl_vat,
           sales_act,
           cost_price_act,
           selling_rsp_act,
           chain_intake_margin_act,
           sales_qty_act,
           sales_planned_qty_pre_act,
           target_stock_qty_act,
           target_stock_selling_act,
           aps_sales_qty_act,
           sales_std_act,
           sales_margin_std_act,
           store_intake_selling_std_act,
           sales_qty_std_act,
           target_stock_qty_std_act,
           sales_qty_6wk_act,
           sales_6wk_act,
           target_stock_selling_6wk_act,
           store_count_act,
           prom_sales_qty_act,
           prom_sales_selling_act,
           clear_sales_qty_act,
           clear_sales_selling_act,
           reg_sales_qty_act,
           reg_sales_selling_act,
           last_updated_date,
           num_avail_days_6wk_act,
           num_catlg_days_6wk_act,
           chain_intake_selling_act)
    select /*+ parallel (o,6) */
           sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           fin_year_no,
           fin_week_no,
           fin_week_code,
           round((max(selling_price) / 1.14),2)                                        REG_RSP_EXCL_VAT,
           round(sum(sales_act),2)                                                     SALES_ACT,
           round(max(cost_price_act),2)                                                COST_PRICE_ACT,
           round(max(selling_price),2)                                                 SELLING_RSP_ACT,
           round(sum(nvl(chain_intake_margin_act,0)) ,2)                               CHAIN_INTAKE_MARGIN_ACT,
           round(sum(sales_qty_act),2)                                                 SALES_QTY_ACT,
           round(sum(sales_planned_qty_pre_act),2)                                     SALES_PLANNED_QTY_PRE_ACT,
           round(sum(target_stock_qty_act),2)                                          TARGET_STOCK_QTY_ACT,
           round(sum(target_stock_selling_act),2)                                      TARGET_STOCK_SELLING_ACT,
           round(sum(aps_sales_qty) / greatest(sum(store_count_act),1),2)              APS_SALES_QTY_ACT,
           round(Sum(Sales_Std_Act),2)                                                 SALES_STD_ACT,
           round(sum(Sales_Std_Act) - sum(sales_cost_std_act),2)                       SALES_MARGIN_STD_ACT,
           round(SUM(STORE_INTAKE_SELLING_STD_ACT),2)                                  STORE_INTAKE_SELLING_STD_ACT,
           round(SUM(SALES_QTY_STD_ACT),2)                                             SALES_QTY_STD_ACT,
           round(sum(target_stock_qty_act),2)                                          TARGET_STOCK_QTY_STD_ACT,
           round(sum(sales_qty_6wk_act),2)                                             SALES_QTY_6WK_ACT,
           round(sum(sales_6wk_act),2)                                                 SALES_6WK_ACT,
           round(sum(target_stock_selling_6wk_act),2)                                  TARGET_STOCK_SELLING_6WK_ACT,
           sum(store_count_act)                                                        STORE_COUNT_ACT,
           round(sum(prom_sales_qty_act),2)                                            PROM_SALES_QTY_ACT,
           round(sum(prom_sales_selling_act),2)                                        PROM_SALES_SELLING_ACT,
           round(sum(clear_sales_qty_act),2)                                           CLEAR_SALES_QTY_ACT,
           round(sum(clear_sales_selling_act),2)                                       CLEAR_SALES_SELLING_ACT,
           round(sum(reg_sales_qty_act),2)                                             REG_SALES_QTY_ACT,
           round(sum(reg_sales_selling_act),2)                                         REG_SALES_SELLING_ACT,
           sysdate,
           round(sum(num_avail_days_6wk_act),0)                                        NUM_AVAIL_DAYS_6WK_ACT,
           round(sum(num_catlg_days_6wk_act),0)                                        NUM_CATLG_DAYS_6WK_ACT,
           round(sum(chain_intake_selling_act),2)                                      CHAIN_INTAKE_SELLING_ACT
    from
    ( -- actual and pre-act -- comment AJ must not go 6 wks back past start of new season
      select /*+ parallel (a,6) */
           a.sk1_chain_no,
           a.sk1_grade_no,
           a.sk1_style_colour_no,
           a.fin_year_no,
           a.fin_week_no,
           cal.fin_week_code,
           max(case when a.sk1_plan_type_no = 63 then nvl(a.cost_price,0) else 0 end)     COST_PRICE_ACT, -- AJ 18/1/13
           max(case when a.sk1_plan_type_no = 63 then nvl(a.selling_price,0) else 0 end)  SELLING_PRICE,  -- AJ 18/1/13
           sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales_qty,0) else 0 end)      SALES_QTY_ACT,
           sum(case when a.sk1_plan_type_no = 64 then nvl(a.sales_qty,0) else 0 end)      SALES_PLANNED_QTY_PRE_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_qty,0) else 0 end) TARGET_STOCK_QTY_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_selling,0) else 0 end) TARGET_STOCK_SELLING_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales,0) else 0 end)          SALES_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(a.aps_sales_qty,0) else 0 end)  APS_SALES_QTY,
           sum(case when a.sk1_plan_type_no = 63 then nvl(store_count,0) else 0 end)      STORE_COUNT_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_selling,0) else 0 end) PROM_SALES_SELLING_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_qty,0) else 0 end)   PROM_SALES_QTY_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_qty,0) else 0 end)  CLEAR_SALES_QTY_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_selling,0) else 0 end) CLEAR_SALES_SELLING_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_qty,0) else 0 end)    REG_SALES_QTY_ACT,
           sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_selling,0) else 0 end) REG_SALES_SELLING_ACT,

           0                                                                              CHAIN_INTAKE_MARGIN_ACT,
           0                                                                              SALES_QTY_6WK_ACT,
           0                                                                              SALES_6WK_ACT,
           0                                                                              TARGET_STOCK_SELLING_6WK_ACT,
           0                                                                              SALES_QTY_STD_ACT,
           0                                                                              SALES_STD_ACT,
           0                                                                              STORE_INTAKE_SELLING_STD_ACT,
           0                                                                              SELLING_PRICE_STD,
           0                                                                              SALES_COST_STD_ACT,
           0                                                                              TARGET_STOCK_QTY_STD_ACT,
           0                                                                              NUM_AVAIL_DAYS_6WK_ACT,
           0                                                                              NUM_CATLG_DAYS_6WK_ACT,
           0                                                                              CHAIN_INTAKE_SELLING_ACT
      from rtl_chn_geo_grd_sc_wk_ast_act a
      join
          (select distinct fin_year_no, fin_week_no, fin_week_code
           from   dim_calendar
           where  to_number(fin_year_no||lpad(fin_week_no,2,''0'')) >= '||g_start_fin_week_code||'    -- 6wks back
           and    to_number(fin_year_no||lpad(fin_week_no,2,''0'')) < '||g_end_fin_week_code||') cal  -- last completed week
          on a.fin_year_no = cal.fin_year_no
         and a.fin_week_no = cal.fin_week_no
      where  a.sk1_plan_type_no in (63, 64)
      group by a.
           sk1_chain_no,
           a.sk1_grade_no,
           a.sk1_style_colour_no,
           a.fin_year_no,
           a.fin_week_no,
           cal.fin_week_code

    union all
    -- chain intake margin -- comment include CHAIN_INTAKE_SELLING
    select /*+ parallel (a,6) */
           a.sk1_chain_no,
           999,
           a.sk1_style_colour_no,
           a.fin_year_no,
           a.fin_week_no,
           cal.fin_week_code,
           0                                                                              COST_PRICE_ACT,
           0                                                                              SELLING_PRICE,
           0                                                                              SALES_QTY_ACT,
           0                                                                              SALES_PLANNED_QTY_PRE_ACT,
           0                                                                              TARGET_STOCK_QTY_ACT,
           0                                                                              TARGET_STOCK_SELLING,
           0                                                                              SALES_ACT,
           0                                                                              APS_SALES_QTY,
           0                                                                              STORE_COUNT_ACT,
           0                                                                              PROM_SALES_SELLING_ACT,
           0                                                                              PROM_SALES_QTY_ACT,
           0                                                                              CLEAR_SALES_QTY_ACT,
           0                                                                              clear_sales_selling_act,
           0                                                                              reg_sales_qty_act,
           0                                                                              reg_sales_selling_act,
           (sum(nvl(a.chain_intake_selling,0))- sum(nvl(a.chain_intake_cost,0)))          chain_intake_margin,
           0                                                                              SALES_QTY_6WK_ACT,
           0                                                                              SALES_6WK_ACT,
           0                                                                              TARGET_STOCK_SELLING_6WK_ACT,
           0                                                                              SALES_QTY_STD_ACT,
           0                                                                              SALES_STD_ACT,
           0                                                                              STORE_INTAKE_SELLING_STD_ACT,
           0                                                                              SELLING_PRICE_STD,
           0                                                                              sales_cost_std_act,
           0                                                                              TARGET_STOCK_QTY_STD_ACT,
           0                                                                              NUM_AVAIL_DAYS_6WK_ACT,
           0                                                                              NUM_CATLG_DAYS_6WK_ACT,
           sum(nvl(a.chain_intake_selling,0))                                             CHAIN_INTAKE_SELLING_ACT
    from   rtl_chain_sc_wk_ast_pln a
    join
          (select distinct fin_year_no, fin_week_no, fin_week_code
           from   dim_calendar
           where  to_number(fin_year_no||lpad(fin_week_no,2,''0'')) >= '||g_start_fin_week_code||'    -- 6wks back
           and    to_number(fin_year_no||lpad(fin_week_no,2,''0'')) < '||g_end_fin_week_code||') cal
        on a.fin_year_no      = cal.fin_year_no
       and a.fin_week_no      = cal.fin_week_no
    where  a.sk1_plan_type_no = 63
    group by a.sk1_chain_no,
           999,
           a.sk1_style_colour_no,
           a.fin_year_no,
           a.fin_week_no,
           cal.fin_week_code

    union all

    -- 6week measures
    select /*+ parallel(w,6) */
           sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           to_number(fin_year_no),
           to_number(fin_week_no),
           fin_week_code,
           0                                                                              COST_PRICE_ACT,
           0                                                                              SELLING_PRICE,
           0                                                                              SALES_QTY_ACT,
           0                                                                              SALES_PLANNED_QTY_PRE_ACT,
           0                                                                              TARGET_STOCK_QTY_ACT,
           0                                                                              TARGET_STOCK_SELLING,
           0                                                                              SALES_ACT,
           0                                                                              APS_SALES_QTY,
           0                                                                              STORE_COUNT_ACT,
           0                                                                              PROM_SALES_SELLING_ACT,
           0                                                                              PROM_SALES_QTY_ACT,
           0                                                                              CLEAR_SALES_QTY_ACT,
           0                                                                              CLEAR_SALES_SELLING_ACT,
           0                                                                              REG_SALES_QTY_ACT,
           0                                                                              REG_SALES_SELLING_ACT,
           0                                                                              CHAIN_INTAKE_MARGIN,
           SALES_QTY_6WK_ACT,
           SALES_6WK_ACT,
           TARGET_STOCK_SELLING_6WK_ACT,
           0                                                                              SALES_QTY_STD_ACT,
           0                                                                              SALES_STD_ACT,
           0                                                                              STORE_INTAKE_SELLING_STD_ACT,
           0                                                                              SELLING_PRICE_STD,
           0                                                                              sales_cost_std_act,
           0                                                                              TARGET_STOCK_QTY_STD_ACT,
           0                                                                              NUM_AVAIL_DAYS_6WK_ACT,
           0                                                                              NUM_CATLG_DAYS_6WK_ACT,
           0                                                                              CHAIN_INTAKE_SELLING_ACT
    from rtl_ch_ast_chn_grd_sc_wk_6w w

    union all

    -- season to date
    select /*+ parallel (b,6)*/
           sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           fin_year_no,
           fin_week_no,
           fin_week_code,
           0                                                                              COST_PRICE_ACT,
           0                                                                              SELLING_PRICE,
           0                                                                              SALES_QTY_ACT,
           0                                                                              SALES_PLANNED_QTY_PRE_ACT,
           0                                                                              TARGET_STOCK_QTY_ACT,
           0                                                                              TARGET_STOCK_SELLING,
           0                                                                              SALES_ACT,
           0                                                                              APS_SALES_QTY,
           0                                                                              STORE_COUNT_ACT,
           0                                                                              PROM_SALES_SELLING_ACT,
           0                                                                              PROM_SALES_QTY_ACT,
           0                                                                              CLEAR_SALES_QTY_ACT,
           0                                                                              CLEAR_SALES_SELLING_ACT,
           0                                                                              REG_SALES_QTY_ACT,
           0                                                                              REG_SALES_SELLING_ACT,
           0                                                                              CHAIN_INTAKE,
           0                                                                              SALES_QTY_6WK_ACT,
           0                                                                              SALES_6WK_ACT,
           0                                                                              TARGET_STOCK_SELLING_6WK_ACT,

           SALES_QTY_STD_ACT,
           SALES_STD_ACT,
           STORE_INTAKE_SELLING_STD_ACT,
           SELLING_PRICE_STD,
           SALES_COST_STD_ACT,
           TARGET_STOCK_QTY_STD_ACT,
           0                                                                              NUM_AVAIL_DAYS_6WK_ACT,
           0                                                                              NUM_CATLG_DAYS_6WK_ACT,
           0                                                                              CHAIN_INTAKE_SELLING_ACT
    from
       (select /*+ parallel (a,6) */ distinct
               cal.sk1_chain_no,
               cal.sk1_grade_no,
               cal.sk1_style_colour_no,
               cal.fin_year_no,
               cal.fin_week_no,
               cal.fin_week_code,
               cal.num_fwc,
               sum(nvl(a.sales_qty,0)) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no
                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) SALES_QTY_STD_ACT,
               sum(nvl(a.sales,0)) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no
                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) SALES_STD_ACT,
               sum(nvl(a.store_intake_selling,0)) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no
                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) STORE_INTAKE_SELLING_STD_ACT,
               max(case when a.selling_price = 0 then null else a.selling_price end) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no

                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) SELLING_PRICE_STD,
               sum(nvl(a.sales_cost,0)) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no
                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) SALES_COST_STD_ACT,
               sum(nvl(target_stock_qty,0)) over (partition by  cal.sk1_chain_no,cal.sk1_grade_no,cal.sk1_style_colour_no
                                         order by      cal.sk1_chain_no, cal.sk1_grade_no, cal.sk1_style_colour_no, cal.num_fwc) TARGET_STOCK_QTY_STD_ACT
        from
                    (select distinct sk1_chain_no,
                      sk1_grade_no,
                      sk1_style_colour_no,
                      fin_week_no,
                      fin_year_no,
                      num_fwc,
                      fin_week_code
              from
                  (select /*+ parallel(a,6) */ distinct
                         sk1_chain_no,
                         sk1_grade_no,
                         sk1_style_colour_no
                   from  rtl_chn_geo_grd_sc_wk_ast_act a
                   where a.sk1_plan_type_no = 63
                   and   fin_year_no =  '||g_fin_year_no||'               -- start of season year
                   and   fin_week_no >= '||g_curr_season_fin_week_no||'   -- start of season week
                   and   fin_week_no <  '||g_end_fin_week_no||')   ,         -- end of curr wk
                 (select distinct c1.fin_week_no, c1.fin_year_no, c1.this_week_start_date,
                         to_number(c1.fin_year_no||lpad(c1.fin_week_no,2,''0'')) num_fwc,
                         c1.fin_week_code                      
                   from  dim_calendar c1
                  where c1.fin_half_no = '||g_fin_half_no||'
                   and c1.fin_year_no = '||g_fin_year_no||'
                   and to_number(c1.fin_year_no||lpad(c1.fin_week_no,2,''0'')) < '||g_end_fin_week_code||'))  cal -- last completed week
              left join
                  rtl_chn_geo_grd_sc_wk_ast_act a
                      on  a.fin_year_no = cal.fin_year_no
                      and a.fin_week_no = cal.fin_week_no
                      and a.sk1_chain_no = cal.sk1_chain_no
                      and a.sk1_grade_no = cal.sk1_grade_no
                      and a.sk1_style_colour_no = cal.sk1_style_colour_no
                      and a.sk1_plan_type_no = 63                   ) b
    where num_fwc >= '||g_start_fin_week_code||') o
    group by
           sk1_chain_no,
           sk1_grade_no,
           sk1_style_colour_no,
           fin_year_no,
           fin_week_no,
           fin_week_code');

    g_recs_read     := g_recs_read     + sql%rowcount;
    g_recs_inserted := g_recs_inserted + sql%rowcount;

    commit;
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

end WH_PRF_AST_034U;
