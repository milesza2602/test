--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_032U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_032U" 
								(p_forall_limit in integer,p_success out boolean) as
-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-03-18 
-- Author: Paul Wakefield
-- Added Materialize hint to insert query
-- **************************************************************************************************

--**************************************************************************************************
--  Date:        June 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Attribute/Product View (Approved, Original, Actualised and Pre-Actualised Plan) from performance level.
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_pln, rtl_chn_geo_grd_sc_wk_ast_act and rtl_chain_sc_wk_ast_pln
--               Output - mart_ch_ast_chn_sc_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit          integer       :=  dwh_constants.vc_forall_limit;
g_recs_read             integer       :=  0;
g_recs_inserted         integer       :=  0;
g_date                  date;
g_season_start_date     date;
g_season_end_date       date;
g_ly_season_start_date  date;
g_ly_season_end_date    date;
g_calendar_end_date_ly  date;
/*
g_6wk_bck_start_date date             := '20 feb 12';
g_74wk_fwd_end_date   date            := '17 mar 13';
g_prev_week_end_date   date;
g_6wk_bck_start_date_ly date          := '27 jun 11';
g_max_end_date_ly   date              := '18 mar 12';
*/
g_6wk_bck_start_date    date;
g_74wk_fwd_end_date     date;
g_prev_week_end_date    date;
g_6wk_bck_start_date_ly date;
g_max_end_date_ly       date;

g_fin_week_no           dim_calendar.fin_week_no%type;
g_fin_year_no           dim_calendar.fin_year_no%type;
g_fin_half_no           dim_calendar.fin_half_no%type;
g_fin_year_no_ly        dim_calendar.fin_year_no%type;
g_fin_week_no_ly        dim_calendar.fin_week_no%type;

l_message               sys_dwh_errlog.log_text%type;
l_module_name           sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_032U';
l_name                  sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name           sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name           sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name        sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                  sys_dwh_log.log_text%type ;
l_description           sys_dwh_log_summary.log_description%type  := 'LOAD THE ATRRIBUTE/PRODUCT VIEW DATAMART - CHN LEVEL';
l_process_type          sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
   l_text := 'LOAD OF mart_ch_ast_chn_sc_wk STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select (this_week_end_date - 6) - (7*6), this_week_end_date + (7*74), this_week_end_date - 7
   into  g_6wk_bck_start_date , g_74wk_fwd_end_date, g_prev_week_end_date
   from  dim_calendar
   where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);

   select ly_fin_week_no, ly_fin_year_no, ly_calendar_date
   into   g_fin_week_no_ly, g_fin_year_no_ly, g_6wk_bck_start_date_ly
   from   dim_calendar
   where  calendar_date = g_6wk_bck_start_date;
/*
   select this_week_start_date
   into   g_6wk_bck_start_date_ly
   from   dim_calendar
   where  fin_year_no = g_fin_year_no_ly
    and   fin_week_no = g_fin_week_no_ly
    and   fin_day_no  = 3;
*/
   select ly_fin_week_no, ly_fin_year_no, ly_calendar_date
   into   g_fin_week_no_ly, g_fin_year_no_ly, g_calendar_end_date_ly
   from   dim_calendar
   where  calendar_date = g_prev_week_end_date;
/*   
   select this_week_end_date
   into   g_calendar_end_date_ly
   from   dim_calendar
   where  fin_year_no = g_fin_year_no_ly
    and   fin_week_no = g_fin_week_no_ly
    and   fin_day_no  = 3;

   select max(this_week_end_date)
   into   g_max_end_date_ly
   from   dim_calendar
   where  fin_year_no = g_fin_year_no_ly;
*/
   l_text := 'Data extract for extended period '||g_6wk_bck_start_date|| ' to '||g_74wk_fwd_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'Data extract for last year '||g_6wk_bck_start_date_ly|| ' to '||g_calendar_end_date_ly;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'Data extract for last week '||g_prev_week_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   execute immediate 'truncate table dwh_performance.mart_ch_ast_chn_sc_wk';
   execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (mart,4) */ into dwh_performance.mart_ch_ast_chn_sc_wk mart
-- Approved Plan - extract1
      with aplan_data as (
      select /*+ full (apln) parallel (apln,4) */
             apln.sk1_chain_no,
             apln.sk1_style_colour_no,
             apln.fin_year_no,
             apln.fin_week_no,
             cal.fin_week_code,
             sum(apln.sales_qty) sales_qty,
             sum(apln.sales) sales,
             sum(apln.store_intake_qty) store_intake_qty,
             sum(apln.store_intake_selling) store_intake_selling,
             sum(apln.target_stock_selling) target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_pln apln,
             dim_calendar cal
      where  apln.sk1_plan_type_no = 60
       and   apln.fin_year_no      = cal.fin_year_no
       and   apln.fin_week_no      = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date
      group by apln.sk1_chain_no, apln.sk1_style_colour_no, apln.fin_year_no, apln.fin_week_no, cal.fin_week_code),
-- Original Plan - extract2
      orig_data as (
      select /*+ full (orig) parallel (orig,4) */
             orig.sk1_chain_no,
             orig.sk1_style_colour_no,
             orig.fin_year_no,
             orig.fin_week_no,
             cal.fin_week_code,
             sum(orig.sales_qty) sales_qty,
             sum(orig.sales) sales,
             sum(orig.store_intake_qty) store_intake_qty,
             sum(orig.store_intake_selling) store_intake_selling,
             sum(orig.target_stock_selling)target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_pln orig,
             dim_calendar cal
      where  orig.sk1_plan_type_no = 62
       and   orig.fin_year_no      = cal.fin_year_no
       and   orig.fin_week_no      = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date
      group by orig.sk1_chain_no, orig.sk1_style_colour_no, orig.fin_year_no, orig.fin_week_no, cal.fin_week_code),
-- Actualised Plan - extract3  -- must change to include prom_reg (qty and sell)
      act_data as (
      select /*+ full (act) parallel (act,4) */
             act.sk1_chain_no,
             act.sk1_style_colour_no,
             act.fin_year_no,
             act.fin_week_no,
             cal.fin_week_code,
             sum(act.sales_qty) sales_qty,
             SUM(ACT.SALES) SALES,
--             sum(act.sit_qty) sit_qty,
--             sum(act.prom_sales_qty) prom_sales_qty,
--             sum(act.prom_sales_selling) prom_sales_selling,
             sum(act.prom_reg_qty) prom_reg_qty,
             SUM(ACT.PROM_REG_SELLING) PROM_REG_SELLING,
--             sum(act.selling_price) * sum(sit_qty) sit_selling,
             sum(act.target_stock_qty) target_stock_qty,
             sum(act.store_intake_qty) store_intake_qty, --chg
             sum(act.store_intake_selling) store_intake_selling, --chg
             sum(act.target_stock_selling)target_stock_selling -- new 4/5/12
      from   rtl_chn_geo_grd_sc_wk_ast_act act,
             dim_calendar cal
      where  act.sk1_plan_type_no = 63
       and   act.fin_year_no      = cal.fin_year_no
       and   act.fin_week_no      = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date
      GROUP BY act.sk1_chain_no, act.sk1_style_colour_no, act.fin_year_no, act.fin_week_no, cal.fin_week_code),
-- Pre-Actualised Plan - extract4
      preact_data as (
      select /*+ full (preact) parallel (preact,4) */
             preact.sk1_chain_no,
             preact.sk1_style_colour_no,
             preact.fin_year_no,
             preact.fin_week_no,
             cal.fin_week_code,
             sum(preact.sales_qty) sales_qty,
             sum(preact.sales) sales,
             sum(preact.store_intake_qty) store_intake_qty,
             sum(preact.store_intake_selling) store_intake_selling,
             sum(preact.target_stock_selling) target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_act preact,
             dim_calendar cal
      where  preact.sk1_plan_type_no  = 64
       and   preact.fin_year_no       = cal.fin_year_no
       and   preact.fin_week_no       = cal.fin_week_no
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date
       and   cal.fin_day_no = 3
      group by preact.sk1_chain_no, preact.sk1_style_colour_no, preact.fin_year_no, preact.fin_week_no, cal.fin_week_code),
-- Approved Chain StyleColour - extract5
      aplnchain_data as (
      select /*+ full (aplnch) parallel (aplnch,4) */
             aplnch.sk1_chain_no,
             aplnch.sk1_style_colour_no,
             aplnch.fin_year_no,
             aplnch.fin_week_no,
             aplnch.fin_week_code,
             aplnch.chain_intake_qty,
             aplnch.chain_intake_selling
--             aplnch.chain_closing_stock_qty,   -- 15/02/13 - incorrect plan type
--             aplnch.chain_closing_stock_selling -- -- 15/02/13 - incorrect plan type
      from   rtl_chain_sc_wk_ast_pln aplnch,
             dim_calendar cal
      where  aplnch.sk1_plan_type_no = 60
       and   aplnch.fin_year_no      = cal.fin_year_no
       and   aplnch.fin_week_no      = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date),
-- Original Chain StyleColour- - extract6
      origchain_data as (
      select /*+ full (oplnch) parallel (oplnch,4) */
             oplnch.sk1_chain_no,
             oplnch.sk1_style_colour_no,
             oplnch.fin_year_no,
             oplnch.fin_week_no,
             oplnch.fin_week_code,
             oplnch.chain_intake_qty,
             oplnch.chain_intake_selling
      from   rtl_chain_sc_wk_ast_pln oplnch,
             dim_calendar cal
      where  oplnch.sk1_plan_type_no  = 62
       and   oplnch.fin_year_no       = cal.fin_year_no
       and   oplnch.fin_week_no       = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date),
-- Actualised Chain StyleColour - extract7
      actchain_data as (
      select /*+ full (actch) parallel (actch,4) */
             actch.sk1_chain_no,
             actch.sk1_style_colour_no,
             actch.fin_year_no,
             actch.fin_week_no,
             actch.fin_week_code,
             actch.chain_intake_qty,
             actch.chain_intake_selling,
             actch.chain_closing_stock_qty,    -- 15/02/13 -- needed against actual plan type
             actch.chain_closing_stock_selling -- 15/02/13 -- needed against actual plan type
      from   rtl_chain_sc_wk_ast_pln actch,
             dim_calendar cal
      where  actch.sk1_plan_type_no   = 63
       and   actch.fin_year_no        = cal.fin_year_no
       and   actch.fin_week_no        = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date),
-- Pre-Actualised Chain StyleColour - extract8
      preactchain_data as (
      select /*+ full (preactch) parallel (preactch,4) */
             preactch.sk1_chain_no,
             preactch.sk1_style_colour_no,
             preactch.fin_year_no,
             preactch.fin_week_no,
             preactch.fin_week_code,
             preactch.chain_intake_qty,
             preactch.chain_intake_selling
      from   rtl_chain_sc_wk_ast_pln preactch,
             dim_calendar cal
      where  preactch.sk1_plan_type_no   = 64
       and   preactch.fin_year_no        = cal.fin_year_no
       and   preactch.fin_week_no        = cal.fin_week_no
       and   cal.fin_day_no = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date),
-- LY Original Plan - extract9
      selcal as (
      select distinct fin_year_no, fin_week_no, this_week_start_date
      from dim_calendar dc
      where dc.this_week_start_date between g_6wk_bck_start_date_ly and g_calendar_end_date_ly),
      ly_orig_data as (
      select /*+ full (lyorig) parallel (lyorig,4) */
             lyorig.sk1_chain_no,
             lyorig.sk1_style_colour_no,
             cal.fin_year_no,
             cal.fin_week_no,
             'W'||(cal.fin_year_no)||cal.fin_week_no as fin_week_code,
--             lyorig.fin_year_no + 1 as fin_year_no,
--             lyorig.fin_week_no,
--             'W'||(lyorig.fin_year_no + 1)||lyorig.fin_week_no as fin_week_code,
             sum(lyorig.sales_qty) sales_qty,
             sum(lyorig.sales) sales
      from   rtl_chn_geo_grd_sc_wk_ast_pln lyorig,
             selcal sc,
             dim_calendar cal
      where  lyorig.sk1_plan_type_no  = 62
       and   lyorig.fin_year_no    = sc.fin_year_no
       and   lyorig.fin_week_no    = sc.fin_week_no
       and   cal.ly_fin_year_no     = lyorig.fin_year_no
       and   cal.ly_fin_week_no     = lyorig.fin_week_no
       and   cal.fin_day_no         = 3
     group by lyorig.sk1_chain_no, lyorig.sk1_style_colour_no, cal.fin_year_no, cal.fin_week_no, 'W'||(cal.fin_year_no)||cal.fin_week_no, 'W'||(lyorig.fin_year_no + 1)||lyorig.fin_week_no),
      ly_act_data as (
      select /*+ full (lyact) parallel (lyact,4) */
             lyact.sk1_chain_no,
             lyact.sk1_style_colour_no,
             cal.fin_year_no,
             cal.fin_week_no,
             'W'||(cal.fin_year_no)||cal.fin_week_no as fin_week_code,
             sum(lyact.sales_qty) sales_qty,
             sum(lyact.sales) sales,
             sum(lyact.store_intake_qty) store_intake_qty,
             sum(lyact.store_intake_selling) store_intake_selling
      from   rtl_chn_geo_grd_sc_wk_ast_act lyact,
             selcal sc,
             dim_calendar cal
      where  lyact.sk1_plan_type_no  = 63
       and   lyact.fin_year_no  = sc.fin_year_no
       and   lyact.fin_week_no  = sc.fin_week_no
       and   cal.ly_fin_year_no = lyact.fin_year_no
       and   cal.ly_fin_week_no = lyact.fin_week_no
       and   cal.fin_day_no     = 3
     group by lyact.sk1_chain_no, lyact.sk1_style_colour_no, cal.fin_year_no, cal.fin_week_no, 'W'||(cal.fin_year_no)||cal.fin_week_no),
-- Actual RMS Bought - extract11
      rms_act_data as (
         select /*+ full (rmsact) parallel (rmsact,4) */
             rmsact.sk1_chain_no,
             rmsact.sk1_style_colour_no,
             rmsact.fin_year_no,
             rmsact.fin_week_no,
             'W'||rmsact.fin_year_no||rmsact.fin_week_no      as fin_week_code,
             sum(nvl(latest_po_qty,0)+nvl(boc_qty,0))         as bought_qty,
             sum(nvl(latest_po_selling,0)+nvl(boc_selling,0)) as bought_selling,
             sum(nvl(latest_po_cost,0)+nvl(boc_cost,0))       as bought_cost,
             sum(nvl(latest_po_qty,0))                        as latest_po_qty,
             sum(nvl(latest_po_selling,0))                    as latest_po_selling,
             sum(nvl(latest_po_cost,0))                       as latest_po_cost
      from   rtl_contract_chain_sc_wk rmsact,
             dim_ast_lev1_diff1 sc,
             dim_calendar cal
      where  rmsact.sk1_style_colour_no = sc.sk1_style_colour_no
       and   rmsact.fin_year_no         = cal.fin_year_no
       and   rmsact.fin_week_no         = cal.fin_week_no
       and   cal.fin_day_no             = 3
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_74wk_fwd_end_date
      group by rmsact.sk1_chain_no, rmsact.sk1_style_colour_no, rmsact.fin_year_no, rmsact.fin_week_no, 'W'||rmsact.fin_year_no||rmsact.fin_week_no),

      -- Actual JDA Data for Stock - extract12
      actual_data_stk as (
      select /*+ materialize full (a) parallel (a,4) */ distinct
             a.sk1_chain_no,
             a.sk1_style_colour_no,
             a.fin_year_no,
             a.fin_week_no,
             a.fin_week_code
      from rtl_chn_geo_grd_sc_wk_ast_act a, dim_calendar b
      where  a.fin_year_no = b.fin_year_no
       and   a.fin_week_no = b.fin_week_no
       and   b.fin_day_no  = 3
       and   b.this_week_start_date between g_6wk_bck_start_date and g_prev_week_end_date),

      rms_stock_data as (
      select /*+ index (stk PK_P_RTL_LC_SC_WK_RMS_STCK) parallel (stk, 4) */
             act.sk1_chain_no,
             stk.sk1_style_colour_no,
             stk.fin_year_no,
             stk.fin_week_no,
             act.fin_week_code,
             sum(stk.inbound_incl_cust_ord_qty)     inbound_incl_cust_ord_qty,
             sum(stk.inbound_incl_cust_ord_selling) inbound_incl_cust_ord_selling
      from   rtl_loc_sc_wk_rms_stock stk,
             dim_location loc,
             actual_data_stk act
      where  stk.fin_year_no         = act.fin_year_no         and
             stk.fin_week_no         = act.fin_week_no         and
             stk.sk1_style_colour_no = act.sk1_style_colour_no and
             stk.sk1_location_no     = loc.sk1_location_no     and
             loc.sk1_chain_no        = act.sk1_chain_no
      group by act.sk1_chain_no, stk.sk1_style_colour_no, stk.fin_year_no, stk.fin_week_no, act.fin_week_code
      union all
      select /*+  parallel (stk,6) */
             loc.sk1_chain_no,
             stk.sk1_style_colour_no,
             stk.fin_year_no,
             stk.fin_week_no,
             stk.fin_week_code,
             sum(stk.inbound_incl_cust_ord_qty)     inbound_incl_cust_ord_qty,
             sum(stk.inbound_incl_cust_ord_selling) inbound_incl_cust_ord_selling
      from   rtl_loc_sc_wk_rms_stock stk,
             dim_location loc
      where  stk.this_week_start_date between g_6wk_bck_start_date and g_prev_week_end_date and
             stk.sk1_location_no     = loc.sk1_location_no     and
             loc.chain_no            = 30
      group by loc.sk1_chain_no, stk.sk1_style_colour_no, stk.fin_year_no, stk.fin_week_no, stk.fin_week_code)

-- collapsing of data from respective tables
      select
             sk1_chain_no,
             sk1_style_colour_no,
             fin_year_no,
             fin_week_no,
             fin_week_code,
             sum(SALES_QTY_APLN)		                  SALES_QTY_APLN	,
             sum(SALES_QTY_OPLN)		                  SALES_QTY_OPLN	,
             sum(SALES_QTY_ACT)		                    SALES_QTY_ACT	,
             sum(SALES_QTY_PREACT) 		                SALES_QTY_PREACT	,
             sum(SALES_QTY_OPLN_LY)    	              SALES_QTY_OPLN_LY	,
             sum(SALES_QTY_ACT_LY) 		                SALES_QTY_ACT_LY	,
             sum(SALES_APLN) 		                      SALES_APLN	,
             sum(SALES_OPLN) 		                      SALES_OPLN	,
             sum(SALES_ACT) 		                      SALES_ACT	,
             sum(SALES_PREACT) 		                    SALES_PREACT	,
             sum(SALES_OPLN_LY)   		                SALES_OPLN_LY	,
             sum(SALES_ACT_LY) 		                    SALES_ACT_LY	,
             sum(STORE_INTAKE_QTY_APLN)  		          STORE_INTAKE_QTY_APLN	,
             sum(STORE_INTAKE_QTY_ACT)  		          STORE_INTAKE_QTY_ACT	,
             sum(STORE_INTAKE_QTY_PREACT) 		        STORE_INTAKE_QTY_PREACT	,
             sum(STORE_INTAKE_QTY_ACT_LY)  		        STORE_INTAKE_QTY_ACT_LY	,
             sum(STORE_INTAKE_SELLING_APLN)  		      STORE_INTAKE_SELLING_APLN	,
             sum(STORE_INTAKE_SELLING_ACT)  		      STORE_INTAKE_SELLING_ACT	,
             sum(STORE_INTAKE_SELLING_PREACT)  		    STORE_INTAKE_SELLING_PREACT	,
             sum(STORE_INTAKE_SELLING_ACT_LY)  		    STORE_INTAKE_SELLING_ACT_LY	,
             sum(TARGET_STOCK_SELLING_APLN)  		      TARGET_STOCK_SELLING_APLN	,
             sum(TARGET_STOCK_SELLING_ACT)  		      TARGET_STOCK_SELLING_ACT	,
             sum(TARGET_STOCK_SELLING_PREACT)  		    TARGET_STOCK_SELLING_PREACT	,
--             sum(PROM_SALES_QTY_ACT)  		            PROM_SALES_QTY_ACT	,
--             sum(PROM_SALES_SELLING_ACT)  		        PROM_SALES_SELLING_ACT	,
             sum(PROM_REG_QTY_ACT)  		              PROM_REG_QTY_ACT	,
             sum(PROM_REG_SELLING_ACT)  		          PROM_REG_SELLING_ACT	,
             sum(CHAIN_INTAKE_QTY_APLN)  		          CHAIN_INTAKE_QTY_APLN	,
             sum(CHAIN_INTAKE_QTY_ACT)  		          CHAIN_INTAKE_QTY_ACT	,
             sum(CHAIN_INTAKE_QTY_PREACT)  		        CHAIN_INTAKE_QTY_PREACT	,
             sum(CHAIN_INTAKE_QTY_OPLN)   		        CHAIN_INTAKE_QTY_OPLN	,
             sum(CHAIN_INTAKE_SELLING_APLN)  		      CHAIN_INTAKE_SELLING_APLN	,
             sum(CHAIN_INTAKE_SELLING_ACT)  		      CHAIN_INTAKE_SELLING_ACT	,
             sum(CHAIN_INTAKE_SELLING_PREACT)  		    CHAIN_INTAKE_SELLING_PREACT	,
             sum(CHAIN_INTAKE_SELLING_OPLN)  		      CHAIN_INTAKE_SELLING_OPLN	,
             sum(CHAIN_CLOSING_STK_QTY_ACT) 	        CHAIN_CLOSING_STK_QTY_ACT	,
             sum(chain_closing_stk_sell_act) 		      CHAIN_CLOSING_STK_SELL_ACT	,
--             sum(SIT_QTY_ACT) 		                    SIT_QTY_ACT	,
--             sum(SIT_SELLING_ACT) 		                SIT_SELLING_ACT	,
             sum(inbound_incl_cust_ord_qty) 		      INBOUND_INCL_CUST_ORD_QTY	,
             sum(inbound_incl_cust_ord_selling) 		  INBOUND_INCL_CUST_ORD_SELLING	,
             sum(bought_qty)  		                    BOUGHT_QTY	,
             sum(bought_selling)  		                BOUGHT_SELLING	,
             sum(bought_cost)  		                    BOUGHT_COST	,
             sum(latest_po_qty)  		                  LATEST_PO_QTY	,
             sum(latest_po_selling)  		              LATEST_PO_SELLING	,
             sum(latest_po_cost)  		                LATEST_PO_COST	,
             sum(TARGET_STOCK_QTY_ACT)                TARGET_STOCK_QTY_ACT,
             g_date
      from   (

-- Combined
      select /*+ full (x1) parallel (x1,4) */
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no,x2.sk1_chain_no),x3.sk1_chain_no),x4.sk1_chain_no),
                 x5.sk1_chain_no),x6.sk1_chain_no),x7.sk1_chain_no),x8.sk1_chain_no),
                 x9.sk1_chain_no),x10.sk1_chain_no),x11.sk1_chain_no),x12.sk1_chain_no) as sk1_chain_no,
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no,x2.sk1_style_colour_no),x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                 x5.sk1_style_colour_no),x6.sk1_style_colour_no),x7.sk1_style_colour_no),x8.sk1_style_colour_no),
                 x9.sk1_style_colour_no),x10.sk1_style_colour_no),x11.sk1_style_colour_no),x12.sk1_style_colour_no) as sk1_style_colour_no,
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no,x2.fin_year_no),x3.fin_year_no),x4.fin_year_no),
                 x5.fin_year_no),x6.fin_year_no),x7.fin_year_no),x8.fin_year_no),
                 x9.fin_year_no),x10.fin_year_no),x11.fin_year_no), x12.fin_year_no) as fin_year_no,
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no,x2.fin_week_no),x3.fin_week_no),x4.fin_week_no),
                 x5.fin_week_no),x6.fin_week_no),x7.fin_week_no),x8.fin_week_no),
                 x9.fin_week_no),x10.fin_week_no),x11.fin_week_no),x12.fin_week_no) as fin_week_no,
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_code,x2.fin_week_code),x3.fin_week_code),x4.fin_week_code),
                 x5.fin_week_code),x6.fin_week_code),x7.fin_week_code),x8.fin_week_code),
                 x9.fin_week_code),x10.fin_week_code),x11.fin_week_code),x12.fin_week_code) as fin_week_code,
             x1.sales_qty		                  SALES_QTY_APLN	,
             x2.sales_qty		                  SALES_QTY_OPLN	,
             x3.sales_qty		                  SALES_QTY_ACT	,
             x4.sales_qty		                  SALES_QTY_PREACT	,
             x9.sales_qty		                  SALES_QTY_OPLN_LY	,
             x10.sales_qty		                SALES_QTY_ACT_LY	,
             x1.sales		                      SALES_APLN	,
             x2.sales		                      SALES_OPLN	,
             x3.sales		                      SALES_ACT	,
             x4.sales		                      SALES_PREACT	,
             x9.sales		                      SALES_OPLN_LY	,
             x10.sales		                    SALES_ACT_LY	,
             x1.store_intake_qty		          STORE_INTAKE_QTY_APLN	,
             x3.store_intake_qty		          STORE_INTAKE_QTY_ACT	, --chg
             x4.store_intake_qty		          STORE_INTAKE_QTY_PREACT	,
             x10.store_intake_qty		          STORE_INTAKE_QTY_ACT_LY	,
             x1.store_intake_selling		      STORE_INTAKE_SELLING_APLN	,
             x3.store_intake_selling		      STORE_INTAKE_SELLING_ACT	, --chg
             x4.store_intake_selling		      STORE_INTAKE_SELLING_PREACT	,
             x10.store_intake_selling		      STORE_INTAKE_SELLING_ACT_LY	,
             x1.target_stock_selling		      TARGET_STOCK_SELLING_APLN	,
             x3.target_stock_selling		      TARGET_STOCK_SELLING_ACT	,
             x4.target_stock_selling		      TARGET_STOCK_SELLING_PREACT	,
--             x3.prom_sales_qty		            PROM_SALES_QTY_ACT	,
--             x3.prom_sales_selling		        PROM_SALES_SELLING_ACT	,
             x3.prom_reg_qty		              PROM_REG_QTY_ACT	,
             x3.prom_reg_selling		          PROM_REG_SELLING_ACT	,
             x5.chain_intake_qty		          CHAIN_INTAKE_QTY_APLN	,
             x7.chain_intake_qty		          CHAIN_INTAKE_QTY_ACT	,
             x8.chain_intake_qty		          CHAIN_INTAKE_QTY_PREACT	,
             x6.chain_intake_qty		          CHAIN_INTAKE_QTY_OPLN	,
             x5.chain_intake_selling		      CHAIN_INTAKE_SELLING_APLN	,
             x7.chain_intake_selling		      CHAIN_INTAKE_SELLING_ACT	,
             x8.chain_intake_selling		      CHAIN_INTAKE_SELLING_PREACT	,
             x6.chain_intake_selling		      CHAIN_INTAKE_SELLING_OPLN	,
             x7.chain_closing_stock_qty	      CHAIN_CLOSING_STK_QTY_ACT	,
             x7.chain_closing_stock_selling		chain_closing_stk_sell_act	,
--             x12.sit_qty		                  SIT_QTY_ACT	,
--             x12.sit_selling		              sit_selling_act	,
             x12.inbound_incl_cust_ord_qty		 INBOUND_INCL_CUST_ORD_QTY,
             x12.inbound_incl_cust_ord_selling INBOUND_INCL_CUST_ORD_SELLING,
             x11.bought_qty		                BOUGHT_QTY	,
             x11.bought_selling		            BOUGHT_SELLING	,
             x11.bought_cost		              BOUGHT_COST	,
             x11.latest_po_qty		            latest_po_qty	,
             x11.latest_po_selling		        latest_po_selling	,
             x11.latest_po_cost		            latest_po_cost	,
             x3.target_stock_qty              TARGET_STOCK_QTY_ACT,
             g_date
      from   aplan_data x1
--
      full outer join orig_data x2 on
             x1.sk1_chain_no        = x2.sk1_chain_no
       and   x1.sk1_style_colour_no = x2.sk1_style_colour_no
       and   x1.fin_year_no         = x2.fin_year_no
       and   x1.fin_week_no         = x2.fin_week_no
--
      full outer join act_data x3 on
             nvl(x1.sk1_chain_no, x2.sk1_chain_no)               = x3.sk1_chain_no
       and   nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no) = x3.sk1_style_colour_no
       and   nvl(x1.fin_year_no, x2.fin_year_no)                 = x3.fin_year_no
       and   nvl(x1.fin_week_no, x2.fin_week_no)                 = x3.fin_week_no
--
      full outer join preact_data x4 on
             nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                     x3.sk1_chain_no)                                 = x4.sk1_chain_no
       and   nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                     x3.sk1_style_colour_no)                          = x4.sk1_style_colour_no
       and   nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                     x3.fin_year_no)                                  = x4.fin_year_no
       and   nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                     x3.fin_week_no)                                  = x4.fin_week_no
--
      full outer join aplnchain_data x5 on
             nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                         x3.sk1_chain_no),x4.sk1_chain_no)               = x5.sk1_chain_no
       and   nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                         x3.sk1_style_colour_no),x4.sk1_style_colour_no) = x5.sk1_style_colour_no
       and   nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                         x3.fin_year_no),x4.fin_year_no)                 = x5.fin_year_no
       and   nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                         x3.fin_week_no),x4.fin_week_no)                 = x5.fin_week_no
--
      full outer join origchain_data x6 on
             nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                             x3.sk1_chain_no),x4.sk1_chain_no),
                             x5.sk1_chain_no)                            = x6.sk1_chain_no
       and   nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                             x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                             x5.sk1_style_colour_no)                     = x6.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                             x3.fin_year_no),x4.fin_year_no),
                             x5.fin_year_no)                             = x6.fin_year_no
       and   nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                             x3.fin_week_no),x4.fin_week_no),
                             x5.fin_week_no)                             = x6.fin_week_no
--
      full outer join actchain_data x7 on
             nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                 x3.sk1_chain_no),x4.sk1_chain_no),
                                 x5.sk1_chain_no),x6.sk1_chain_no)       = x7.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                 x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                 x5.sk1_style_colour_no),x6.sk1_style_colour_no)
                                                                         = x7.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                 x3.fin_year_no),x4.fin_year_no),
                                 x5.fin_year_no),x6.fin_year_no)         = x7.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                 x3.fin_week_no),x4.fin_week_no),
                                 x5.fin_week_no),x6.fin_week_no)         = x7.fin_week_no
--
      full outer join preactchain_data x8 on
             nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                     x3.sk1_chain_no),x4.sk1_chain_no),
                                     x5.sk1_chain_no),x6.sk1_chain_no),
                                     x7.sk1_chain_no)                   = x8.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                     x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                     x5.sk1_style_colour_no),x6.sk1_style_colour_no),
                                     x7.sk1_style_colour_no)            = x8.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                     x3.fin_year_no),x4.fin_year_no),
                                     x5.fin_year_no),x6.fin_year_no),
                                     x7.fin_year_no)                    = x8.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                     x3.fin_week_no),x4.fin_week_no),
                                     x5.fin_week_no),x6.fin_week_no),
                                     x7.fin_week_no)                    = x8.fin_week_no
--
      full outer join ly_orig_data x9 on
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                     x3.sk1_chain_no),x4.sk1_chain_no),
                                     x5.sk1_chain_no),x6.sk1_chain_no),
                                     x7.sk1_chain_no),x8.sk1_chain_no)                = x9.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                     x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                     x5.sk1_style_colour_no),x6.sk1_style_colour_no),
                                     x7.sk1_style_colour_no),x8.sk1_style_colour_no)  = x9.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                     x3.fin_year_no),x4.fin_year_no),
                                     x5.fin_year_no),x6.fin_year_no),
                                     x7.fin_year_no),x8.fin_year_no)                  = x9.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                     x3.fin_week_no),x4.fin_week_no),
                                     x5.fin_week_no),x6.fin_week_no),
                                     x7.fin_week_no),x8.fin_week_no)                  = x9.fin_week_no
--
      full outer join ly_act_data x10 on
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                     x3.sk1_chain_no),x4.sk1_chain_no),
                                     x5.sk1_chain_no),x6.sk1_chain_no),
                                     x7.sk1_chain_no),x8.sk1_chain_no),
                                     x9.sk1_chain_no)                                 = x10.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                     x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                     x5.sk1_style_colour_no),x6.sk1_style_colour_no),
                                     x7.sk1_style_colour_no),x8.sk1_style_colour_no),
                                     x9.sk1_style_colour_no)                          = x10.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                     x3.fin_year_no),x4.fin_year_no),
                                     x5.fin_year_no),x6.fin_year_no),
                                     x7.fin_year_no),x8.fin_year_no),
                                     x9.fin_year_no)                                  = x10.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                     x3.fin_week_no),x4.fin_week_no),
                                     x5.fin_week_no),x6.fin_week_no),
                                     x7.fin_week_no),x8.fin_week_no),
                                     x9.fin_week_no)                                  = x10.fin_week_no
--
      full outer join rms_act_data x11 on
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                     x3.sk1_chain_no),x4.sk1_chain_no),
                                     x5.sk1_chain_no),x6.sk1_chain_no),
                                     x7.sk1_chain_no),x8.sk1_chain_no),
                                     x9.sk1_chain_no),x10.sk1_chain_no)                                 = x11.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                     x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                     x5.sk1_style_colour_no),x6.sk1_style_colour_no),
                                     x7.sk1_style_colour_no),x8.sk1_style_colour_no),
                                     x9.sk1_style_colour_no),x10.sk1_style_colour_no)                          = x11.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                     x3.fin_year_no),x4.fin_year_no),
                                     x5.fin_year_no),x6.fin_year_no),
                                     x7.fin_year_no),x8.fin_year_no),
                                     x9.fin_year_no),x10.fin_year_no)                                  = x11.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                     x3.fin_week_no),x4.fin_week_no),
                                     x5.fin_week_no),x6.fin_week_no),
                                     x7.fin_week_no),x8.fin_week_no),
                                     x9.fin_week_no),x10.fin_week_no)                                  = x11.fin_week_no
      --
      full outer join rms_stock_data x12 on
             nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                                     x3.sk1_chain_no),x4.sk1_chain_no),
                                     x5.sk1_chain_no),x6.sk1_chain_no),
                                     x7.sk1_chain_no),x8.sk1_chain_no),
                                     x9.sk1_chain_no),x10.sk1_chain_no),
                                     x11.sk1_chain_no)                                                 = x12.sk1_chain_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                                     x3.sk1_style_colour_no),x4.sk1_style_colour_no),
                                     x5.sk1_style_colour_no),x6.sk1_style_colour_no),
                                     x7.sk1_style_colour_no),x8.sk1_style_colour_no),
                                     x9.sk1_style_colour_no),x10.sk1_style_colour_no),
                                     x11.sk1_style_colour_no)                                          = x12.sk1_style_colour_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                                     x3.fin_year_no),x4.fin_year_no),
                                     x5.fin_year_no),x6.fin_year_no),
                                     x7.fin_year_no),x8.fin_year_no),
                                     x9.fin_year_no),x10.fin_year_no),
                                     x11.fin_year_no)                                                  = x12.fin_year_no
       and   nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                                     x3.fin_week_no),x4.fin_week_no),
                                     x5.fin_week_no),x6.fin_week_no),
                                     x7.fin_week_no),x8.fin_week_no),
                                     x9.fin_week_no),x10.fin_week_no),
                                     x11.fin_week_no)                                                  = x12.fin_week_no)
      group by sk1_chain_no,
               sk1_style_colour_no,
               fin_year_no,
               fin_week_no,
               fin_week_code,
               g_date;

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

end wh_prf_ast_032u;
