--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_862U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_862U_OLD" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2016
--  Author:      A Joshua
--  Purpose:     Load 6 week sales data ex temp tables populated by WH_PRF_CORP_860U (prededessor)
--               Needed as input into TABLEAU mart (successor WH_PRF_CORP_861U)
--  Tables:      Input  - temp_foods_tab_2yrs_dense and temp_foods_tab_2yrs_sparse
--               Output - rtl_loc_item_wk_sales_6wkavg
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
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_862U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WEEKLY SALES AVERAGE DATA';
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
   l_text := 'LOAD OF RTL_LOC_ITEM_WK_SALES_6WKAVG STARTED AT '||
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
   
   select fin_year_no, fin_week_no
   into g_fin_year_no, g_fin_week_no
   from dim_calendar where calendar_date = trunc(sysdate) - 7;

   execute immediate 'alter session enable parallel dml';

   insert /*+ APPEND parallel (a,4) */ into rtl_loc_item_wk_sales_6wkavg a
   --dwh_datafix.aj_rtl_loc_item_wk_sales_6wk a
         
   with sales_xtr as (
   select /*+ parallel (a,4) full (a) */ 
   a.sk1_item_no, 
   a.sk1_location_no,
   sum(sales) sales_6wk, 
   sum(sales_qty) sales_6wk_qty, 
   sum(sales_margin) sales_6wk_margin,
   sum(case when nvl(sales,0) > 0 then
       round((sales /6),2) end) sales_6wk_avg,
   sum(case when nvl(sales_qty,0) > 0 then
       round((sales_qty /6),2) end) sales_qty_6wk_avg,
   sum(case when nvl(sales_margin,0) > 0 then
       round((sales_margin /6),2) end) sales_margin_6wk_avg
   from temp_foods_tab_2yrs_dense a
   group by a.sk1_item_no, a.sk1_location_no
  ),
  
   sparse_xtr as (
   select /*+ parallel (a,6) full (a) */
   a.sk1_item_no, 
   a.sk1_location_no,
   sum(a.waste_cost) as waste_cost,
   sum(case when nvl(a.waste_cost,0) <> 0 then
       round((waste_cost /6),2) end) waste_6wkavg_cost_perc
   from temp_foods_tab_2yrs_sparse a
   group by a.sk1_item_no, a.sk1_location_no
  )
   select sk1_location_no,
          sk1_item_no,
          g_fin_year_no,
          g_fin_week_no,
          sum(sales_6wk_qty)               as sales_6wk_qty,
          sum(sales_6wkavg_excl_promo_qty) as sales_6wkavg_excl_promo_qty,
          sum(sales_6wk)                   as sales_6wk,
          sum(sales_6wkavg_excl_promo)     as sales_6wkavg_excl_promo,
          sum(sales_6wk_margin)            as sales_6wk_margin,
          avg(sales_6wkavg_margin_perc)    as sales_6wkavg_margin_perc,
          avg(waste_6wk_promo_cost)        as waste_6wk_promo_cost,
          avg(waste_6wkavg_cost_perc)      as waste_6wkavg_cost_perc,
          null,
          g_date
   from (
   select nvl(r1.sk1_location_no,  r2.sk1_location_no)  as sk1_location_no,
          nvl(r1.sk1_item_no,  r2.sk1_item_no)          as sk1_item_no,
          r1.sales_6wk_qty                              as sales_6wk_qty,
          r1.sales_qty_6wk_avg                          as sales_6wkavg_excl_promo_qty,
          r1.sales_6wk                                  as sales_6wk,
          r1.sales_6wk_avg                              as sales_6wkavg_excl_promo,
          r1.sales_6wk_margin                           as sales_6wk_margin,
          r1.sales_margin_6wk_avg                       as sales_6wkavg_margin_perc,
          r2.waste_cost                                 as waste_6wk_promo_cost,
          r2.waste_6wkavg_cost_perc                     as waste_6wkavg_cost_perc,
          null,
          g_date
   from sales_xtr r1
   full outer join sparse_xtr r2 on r1.sk1_item_no = r2.sk1_item_no
                             and r1.sk1_location_no = r2.sk1_location_no
                             )
   group by sk1_location_no, sk1_item_no ;
        
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

end wh_prf_corp_862u_old;
