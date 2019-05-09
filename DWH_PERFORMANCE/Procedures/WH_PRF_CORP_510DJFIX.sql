--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_510DJFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_510DJFIX" 
                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      M Munnik
--  Purpose:     Rollup from rtl_loc_item_wk_rms_dense to rtl_loc_sc_wk_rms_dense.
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - rtl_loc_sc_wk_rms_dense
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
g_recs_inserted      integer       :=  0;
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_sub                integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_510U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS DENSE FROM WEEK TO STYLE_COLOUR';
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
   l_text := 'ROLLUP OF rtl_loc_sc_wk_rms_dense EX WEEK LEVEL STARTED AT '||
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

   for g_sub in 6..17 loop
   
      select fin_week_no,   fin_year_no
      into   g_fin_week_no, g_fin_year_no
      from   dim_calendar
      where  calendar_date = g_date - (g_sub * 7);
 
      insert /*+ APPEND */  into dwh_performance.rtl_loc_sc_wk_rms_dense rtl_lswrd 
      select   liw.sk1_location_no as sk1_location_no,
               di.sk1_style_colour_no as sk1_style_colour_no,
               liw.fin_year_no as fin_year_no,
               liw.fin_week_no as fin_week_no,
               max(liw.fin_week_code)  fin_week_code,
               max(liw.this_week_start_date) as this_week_start_date,
               max(liw.sk2_location_no) sk2_location_no,
               sum(liw.sales_qty) sales_qty,
               sum(liw.sales_cases) sales_cases,
               sum(liw.sales) sales,
               sum(liw.sales_incl_vat) sales_incl_vat,
               sum(liw.sales_cost) sales_cost,
               sum(liw.sales_fr_cost) sales_fr_cost,
               sum(liw.sales_margin) sales_margin,
               sum(liw.franchise_sales) franchise_sales,
               sum(liw.franchise_sales_margin) franchise_sales_margin,
               sum(liw.reg_sales_qty) reg_sales_qty,
               sum(liw.reg_sales) reg_sales,
               sum(liw.reg_sales_cost) reg_sales_cost,
               sum(liw.reg_sales_fr_cost) reg_sales_fr_cost,
               sum(liw.reg_sales_margin) reg_sales_margin,
               sum(liw.franchise_reg_sales_margin) franchise_reg_sales_margin,
               sum(liw.gross_sales_qty) gross_sales_qty,
               sum(liw.gross_sales) gross_sales,
               sum(liw.gross_sales_cost) gross_sales_cost,
               sum(liw.gross_sales_fr_cost) gross_sales_fr_cost,
               sum(liw.gross_reg_sales_qty) gross_reg_sales_qty,
               sum(liw.gross_reg_sales) gross_reg_sales,
               sum(liw.gross_reg_sales_cost) gross_reg_sales_cost,
               sum(liw.gross_reg_sales_fr_cost) gross_reg_sales_fr_cost,
               sum(liw.sdn_in_qty) sdn_in_qty,
               sum(liw.sdn_in_selling) sdn_in_selling,
               sum(liw.sdn_in_cost) sdn_in_cost,
               sum(liw.sdn_in_fr_cost) sdn_in_fr_cost,
               sum(liw.sdn_in_cases) sdn_in_cases,
               sum(liw.actl_store_rcpt_qty) actl_store_rcpt_qty,
               sum(liw.actl_store_rcpt_selling) actl_store_rcpt_selling,
               sum(liw.actl_store_rcpt_cost) actl_store_rcpt_cost,
               sum(liw.actl_store_rcpt_fr_cost) actl_store_rcpt_fr_cost,
               sum(liw.store_deliv_selling) store_deliv_selling,
               sum(liw.store_deliv_cost) store_deliv_cost,
               sum(liw.store_deliv_fr_cost) store_deliv_fr_cost,
               sum(liw.store_intake_qty) store_intake_qty,
               sum(liw.store_intake_selling) store_intake_selling,
               sum(liw.store_intake_cost) store_intake_cost,
               sum(liw.store_intake_fr_cost) store_intake_fr_cost,
               sum(liw.store_intake_margin) store_intake_margin,
               sum(liw.sales_returns_qty) sales_returns_qty,
               sum(liw.sales_returns_selling) sales_returns_selling,
               sum(liw.sales_returns_cost) sales_returns_cost,
               sum(liw.sales_returns_fr_cost) sales_returns_fr_cost,
               sum(liw.reg_sales_returns_qty) reg_sales_returns_qty,
               sum(liw.reg_sales_returns_selling) reg_sales_returns_selling,
               sum(liw.reg_sales_returns_cost) reg_sales_returns_cost,
               sum(liw.reg_sales_returns_fr_cost) reg_sales_returns_fr_cost,
               sum(liw.clear_sales_returns_selling) clear_sales_returns_selling,
               sum(liw.clear_sales_returns_cost) clear_sales_returns_cost,
               sum(liw.clear_sales_returns_fr_cost) clear_sales_returns_fr_cost,
               sum(liw.clear_sales_returns_qty) clear_sales_returns_qty,
               g_date last_updated_date
      from     rtl_loc_item_wk_rms_dense liw 
      join     dim_item di
      on       liw.sk1_item_no  = di.sk1_item_no
      where    liw.fin_year_no  = g_fin_year_no
      and      liw.fin_week_no  = g_fin_week_no  
      group by liw.fin_year_no,
               liw.fin_week_no,
               di.sk1_style_colour_no,
               liw.sk1_location_no;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      l_text := 'ROLLED UP YEAR AND WEEK - '||g_fin_year_no||' '|| g_fin_week_no||' at '||
                to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount||' total '||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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

end wh_prf_corp_510djfix;