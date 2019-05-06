--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_036U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_036U_FIX" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        August 2013
--  Author:      A Joshua
--  Purpose:     Load Assort Mart for Chain Product and Procurement cubes
--  Tables:      Input  - rtl_chain_sc_wk_ast_pln, rtl_chn_geo_grd_sc_wk_ast_pln, rtl_contract_chain_sc_wk, rtl_loc_sc_wk_aps
--               Output - rtl_chain_sc_supchn_wk_ast_pln
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
--g_start_date         date;
--g_end_date           date;
--g_ly_start_date      date;
--g_ly_end_date        date;
g_6wk_bck_start_date date;
g_74wk_fwd_end_date  date;
g_prev_week_end_date date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_036U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE SPCHN PLANS ex AST';
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
   l_text := 'LOAD OF RTL_CHAIN_SC_SUPCHN_WK_AST_PLN STARTED AT '||
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

   select (this_week_end_date - 6) - (7*6), this_week_end_date - 7, this_week_end_date + (7*74)
   into  g_6wk_bck_start_date , g_prev_week_end_date, g_74wk_fwd_end_date
   from  dim_calendar
   where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);
/*
   select min(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_start_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_start_date = g_6wk_bck_start_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;

   select max(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_end_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_end_date = g_prev_week_end_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;
*/
   l_text := 'Current Data extract from '||g_6wk_bck_start_date|| ' to '||g_74wk_fwd_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (spchn,4) */ into rtl_chain_sc_supchn_wk_ast_pln spchn

      with chain_plan_data as (
      select /*+ parallel (pln,6) */
             pln.sk1_chain_no,
             sc.sk1_style_colour_no,
             sct.sk1_supply_chain_no,
             pln.fin_year_no,
             pln.fin_week_no,
             pln.sk1_plan_type_no,
             'W'||(pln.fin_year_no)||pln.fin_week_no                    fin_week_code,
             sum(pln.chain_intake_qty)                                  chain_intake_qty,
             sum(pln.chain_intake_cost)                                 chain_intake_cost,
             sum(pln.chain_intake_selling)                              chain_intake_selling,
             sum(pln.chain_intake_selling) - sum(pln.chain_intake_cost) chain_intake_margin,
             '25 jun 12' as last_updated_date
      from   rtl_chain_sc_wk_ast_pln pln,
             dim_ast_lev1_diff1 sc,
             dim_supply_chain_type sct
--             dim_calendar cal
      where  pln.sk1_style_colour_no  = sc.sk1_style_colour_no
       and   sct.supply_chain_code    = sc.supply_chain_code
       and   pln.fin_year_no          = 2014
--       and   pln.fin_week_no          between 1 and 34
       and   pln.sk1_plan_type_no in (60,61,62)
      group by pln.sk1_chain_no, sc.sk1_style_colour_no, sct.sk1_supply_chain_no, pln.fin_year_no, pln.fin_week_no, pln.sk1_plan_type_no, 'W'||(pln.fin_year_no)||pln.fin_week_no, '25 jun 12') ,

      geo_grade_plan_data as (
      select /*+ parallel (cgg,6) */
             cgg.sk1_chain_no,
             sc.sk1_style_colour_no,
             sct.sk1_supply_chain_no,
             cgg.fin_year_no,
             cgg.fin_week_no,
             cgg.sk1_plan_type_no,
             'W'||(cgg.fin_year_no)||cgg.fin_week_no                    fin_week_code,
             sum(cgg.sales_qty)             sales_qty,
             sum(cgg.sales_cost)            sales_cost,
             sum(cgg.sales)                 sales,
             sum(cgg.sales) - sum(cgg.sales_cost)  sales_margin,
             sum(cgg.target_stock_qty)      target_stock_qty,
             sum(cgg.target_stock_selling)  target_stock_selling,
             sum(cgg.store_intake_qty)      store_intake_qty,
             sum(cgg.store_intake_selling)  store_intake_selling,
             sum(cgg.aps_sales_qty)         aps_sales_qty,
             sum(cgg.store_count)           store_count,
             '25 jun 12' as last_updated_date
      from   rtl_chn_geo_grd_sc_wk_ast_pln cgg,
             dim_ast_lev1_diff1 sc,
             dim_supply_chain_type sct
--             dim_calendar cal
      where  cgg.sk1_style_colour_no  = sc.sk1_style_colour_no
       and   sct.supply_chain_code    = sc.supply_chain_code
       and   cgg.fin_year_no          = 2014
--       and   cgg.fin_week_no          between 1 and 34
       and   cgg.sk1_plan_type_no in (60,61,62)
      group by cgg.sk1_chain_no, sc.sk1_style_colour_no, sct.sk1_supply_chain_no, cgg.fin_year_no, cgg.fin_week_no, cgg.sk1_plan_type_no, 'W'||(cgg.fin_year_no)||cgg.fin_week_no, '25 jun 12'),

      contract_data as (
      select /*+ parallel (con,6) */
             con.sk1_chain_no,
             sc.sk1_style_colour_no,
             sct.sk1_supply_chain_no,
             con.fin_year_no,
             con.fin_week_no,
             1                                                    sk1_plan_type_no,
             'W'||(con.fin_year_no)||con.fin_week_no              fin_week_code,
             sum(con.boc_selling) - sum(con.boc_cost)             boc_margin,
             sum(con.latest_po_selling) - sum(con.latest_po_cost) latest_po_margin,
             '25 jun 12' as last_updated_date
      from   rtl_contract_chain_sc_wk con,
             dim_ast_lev1_diff1 sc,
             dim_supply_chain_type sct
--             dim_calendar cal
      where  con.sk1_style_colour_no  = sc.sk1_style_colour_no
       and   sct.supply_chain_code    = sc.supply_chain_code
       and   con.fin_year_no          = 2014
--       and   con.fin_week_no          between 1 and 34
      group by con.sk1_chain_no, sc.sk1_style_colour_no, sct.sk1_supply_chain_no, con.fin_year_no, con.fin_week_no, 1, 'W'||(con.fin_year_no)||con.fin_week_no, '25 jun 12'),

      aps_data as (
      select /*+ parallel (aps,6) */
             loc.sk1_chain_no,
             sc.sk1_style_colour_no,
             sct.sk1_supply_chain_no,
             aps.fin_year_no,
             aps.fin_week_no,
             1                               sk1_plan_type_no,
             'W'||(aps.fin_year_no)||aps.fin_week_no              fin_week_code,
             sum(aps.aps_sales_qty_cleansed) aps_sales_qty_cleansed,
             sum(aps.aps_sales_qty)          aps_sales_qty,
             sum(aps.store_count_cleansed)   store_count_cleansed,
             sum(aps.store_count)            store_count,
             '25 jun 12' as last_updated_date
      from   rtl_loc_sc_wk_aps aps,
             dim_ast_lev1_diff1 sc,
             dim_supply_chain_type sct,
--             dim_calendar cal,
             dim_location loc
      where  aps.sk1_style_colour_no  = sc.sk1_style_colour_no
       and   sct.supply_chain_code    = sc.supply_chain_code
       and   aps.sk1_location_no      = loc.sk1_location_no
       and   aps.fin_year_no          = 2014
--       and   aps.fin_week_no          between 1 and 34
      group by loc.sk1_chain_no, sc.sk1_style_colour_no, sct.sk1_supply_chain_no, aps.fin_year_no, aps.fin_week_no, 1, 'W'||(aps.fin_year_no)||aps.fin_week_no, '25 jun 12')

      select sk1_chain_no,
             sk1_style_colour_no,
             sk1_supply_chain_no,
             fin_year_no,
             fin_week_no,
             sk1_plan_type_no,
             fin_week_code,
             sum(sales_qty)                           sales_qty,
             sum(sales_cost)                          sales_cost,
             sum(sales)                               sales,
             sum(sales_margin)                        sales_margin,
             sum(chain_intake_qty)                    chain_intake_qty,
             sum(chain_intake_cost)                   chain_intake_cost,
             sum(chain_intake_selling)                chain_intake_selling,
             sum(chain_intake_margin)                 chain_intake_margin,
             sum(target_stock_qty)                    target_stock_qty,
             sum(target_stock_selling)                target_stock_selling,
             sum(store_intake_qty)                    store_intake_qty,
             sum(store_intake_selling)                store_intake_selling,
             sum(boc_margin)                          boc_margin,
             sum(latest_po_margin)                    latest_po_margin,
             sum(nvl(latest_po_margin,0)) + sum(nvl(boc_margin,0))  bought_margin,
             sum(aps_sales_qty_cleansed)              aps_sales_qty_cleansed,
             sum(aps_sales_qty)                       aps_sales_qty,
             sum(store_count)                         store_count,
             sum(store_count_cleansed)                store_count_cleansed,
             g_date                                   last_updated_date
      from   (
         select /*+ parallel (x1,3) parallel (x2,3) parallel(x3,3) parallel (x4,3) */
             nvl(nvl(nvl(x1.sk1_chain_no,x2.sk1_chain_no),
             x3.sk1_chain_no),x4.sk1_chain_no)                        as sk1_chain_no,
             nvl(nvl(nvl(x1.sk1_style_colour_no,x2.sk1_style_colour_no),
             x3.sk1_style_colour_no),x4.sk1_style_colour_no)          as sk1_style_colour_no,
             nvl(nvl(nvl(x1.sk1_supply_chain_no,x2.sk1_supply_chain_no),
             x3.sk1_supply_chain_no),x4.sk1_supply_chain_no)          as sk1_supply_chain_no,
             nvl(nvl(nvl(x1.fin_year_no,x2.fin_year_no),
             x3.fin_year_no),x4.fin_year_no)                          as fin_year_no,
             nvl(nvl(nvl(x1.fin_week_no,x2.fin_week_no),
             x3.fin_week_no),x4.fin_week_no)                          as fin_week_no,
             nvl(nvl(nvl(x1.sk1_plan_type_no,x2.sk1_plan_type_no),
             x3.sk1_plan_type_no),x4.sk1_plan_type_no)                as sk1_plan_type_no,
             nvl(nvl(nvl(x1.fin_week_code,x2.fin_week_code),
             x3.fin_week_code),x4.fin_week_code)                      as fin_week_code,
             x2.sales_qty                                             as sales_qty,
             x2.sales_cost                                            as sales_cost,
             x2.sales                                                 as sales,
             x2.sales_margin                                          as sales_margin,
             x1.chain_intake_qty                                      as chain_intake_qty,
             x1.chain_intake_cost                                     as chain_intake_cost,
             x1.chain_intake_selling                                  as chain_intake_selling,
             x1.chain_intake_margin                                   as chain_intake_margin,
             x2.target_stock_qty                                      as target_stock_qty,
             x2.target_stock_selling                                  as target_stock_selling,
             x2.store_intake_qty                                      as store_intake_qty,
             x2.store_intake_selling                                  as store_intake_selling,
             x3.boc_margin                                            as boc_margin,
             x3.latest_po_margin                                      as latest_po_margin,
             x3.latest_po_margin + x3.boc_margin                      as bought_margin,
             x4.aps_sales_qty_cleansed                                as aps_sales_qty_cleansed,
             (case x4.sk1_plan_type_no
                when 1 then  x4.aps_sales_qty
                else         x2.aps_sales_qty
              end)                                                    as aps_sales_qty,
             (case x4.sk1_plan_type_no
                when 1 then  x4.store_count
                else         x2.store_count
              end)                                                    as store_count,
             x4.store_count_cleansed                                  as store_count_cleansed,
             g_date                                                   as last_updated_date
         from   chain_plan_data x1

      full outer join geo_grade_plan_data x2 on
             x1.sk1_chain_no        = x2.sk1_chain_no
         and x1.sk1_style_colour_no = x2.sk1_style_colour_no
         and x1.sk1_supply_chain_no = x2.sk1_supply_chain_no
         and x1.fin_year_no         = x2.fin_year_no
         and x1.fin_week_no         = x2.fin_week_no
         and x1.sk1_plan_type_no    = x2.sk1_plan_type_no

      full outer join contract_data x3 on
             nvl(x1.sk1_chain_no, x2.sk1_chain_no)               = x3.sk1_chain_no
       and   nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no) = x3.sk1_style_colour_no
       and   nvl(x1.sk1_supply_chain_no, x2.sk1_supply_chain_no) = x3.sk1_supply_chain_no
       and   nvl(x1.fin_year_no, x2.fin_year_no)                 = x3.fin_year_no
       and   nvl(x1.fin_week_no, x2.fin_week_no)                 = x3.fin_week_no
       and   nvl(x1.sk1_plan_type_no, x2.sk1_plan_type_no)       = x3.sk1_plan_type_no
--
      full outer join aps_data x4 on
             nvl(nvl(x1.sk1_chain_no, x2.sk1_chain_no),
                     x3.sk1_chain_no)                                 = x4.sk1_chain_no
       and   nvl(nvl(x1.sk1_style_colour_no, x2.sk1_style_colour_no),
                     x3.sk1_style_colour_no)                          = x4.sk1_style_colour_no
       and   nvl(nvl(x1.sk1_supply_chain_no, x2.sk1_supply_chain_no),
                     x3.sk1_supply_chain_no)                          = x4.sk1_supply_chain_no
       and   nvl(nvl(x1.fin_year_no, x2.fin_year_no),
                     x3.fin_year_no)                                  = x4.fin_year_no
       and   nvl(nvl(x1.fin_week_no, x2.fin_week_no),
                     x3.fin_week_no)                                  = x4.fin_week_no
       and   nvl(nvl(x1.sk1_plan_type_no, x2.sk1_plan_type_no),
                     x3.sk1_plan_type_no)                             = x4.sk1_plan_type_no)

      group by sk1_chain_no, sk1_style_colour_no, sk1_supply_chain_no, fin_year_no, fin_week_no, sk1_plan_type_no, fin_week_code, g_date;

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

end wh_prf_ast_036u_fix;
