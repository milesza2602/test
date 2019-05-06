--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_510U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_510U_FIX" 
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_510U_FIX';
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

     MERGE /*+ PARALLEL(rtl_lswrd,4) */  into dwh_performance.rtl_loc_sc_wk_rms_dense rtl_lswrd using 
     (
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
               '28/DEC/15' last_updated_date
      from     rtl_loc_item_wk_rms_dense liw 
      join     dim_item di
      on       liw.sk1_item_no  = di.sk1_item_no
      and    liw.fin_year_no  = 2016
      and      liw.fin_week_no  in (26, 27)
      
      join     dim_calendar dc on dc.fin_week_no = liw.fin_week_no and dc.fin_year_no = dc.fin_year_no
      join     W6005682.dense_flowers_insert lid
        on     lid.sk1_item_no = liw.sk1_item_no
       and     lid.sk1_location_no = liw.sk1_location_no
       and     lid.post_date = dc.calendar_date
       
      group by liw.fin_year_no,
               liw.fin_week_no,
               di.sk1_style_colour_no,
               liw.sk1_location_no
         
  ) mer_dense
  
  on (rtl_lswrd.sk1_location_no         = mer_dense.sk1_location_no
  and rtl_lswrd.sk1_style_colour_no     = mer_dense.sk1_style_colour_no
  and rtl_lswrd.fin_year_no             = mer_dense.fin_year_no
  and rtl_lswrd.fin_week_no             = mer_dense.fin_week_no
  )
  
  when matched then 
    update
      set 
      
          fin_week_code               =		              mer_dense.fin_week_code,
          this_week_start_date        =				          mer_dense.this_week_start_date,
          sk2_location_no             =				          mer_dense.sk2_location_no,
          sales_qty                   =				          mer_dense.sales_qty,
          sales_cases                 =				          mer_dense.sales_cases,
          sales                       =				          mer_dense.sales,
          sales_incl_vat              =				          mer_dense.sales_incl_vat,
          sales_cost                  =				          mer_dense.sales_cost,
          sales_fr_cost               =				          mer_dense.sales_fr_cost,
          sales_margin                =				          mer_dense.sales_margin,
          franchise_sales             =				          mer_dense.franchise_sales,
          franchise_sales_margin      =				          mer_dense.franchise_sales_margin,
          reg_sales_qty               =				          mer_dense.reg_sales_qty,
          reg_sales                   =				          mer_dense.reg_sales,
          reg_sales_cost              =				          mer_dense.reg_sales_cost,
          reg_sales_fr_cost           =				          mer_dense.reg_sales_fr_cost,
          reg_sales_margin            =				          mer_dense.reg_sales_margin,
          franchise_reg_sales_margin  =				          mer_dense.franchise_reg_sales_margin,
          gross_sales_qty             =				          mer_dense.gross_sales_qty,
          gross_sales                 =				          mer_dense.gross_sales,
          gross_sales_cost            =				          mer_dense.gross_sales_cost,
          gross_sales_fr_cost         =				          mer_dense.gross_sales_fr_cost,
          gross_reg_sales_qty         =				          mer_dense.gross_reg_sales_qty,
          gross_reg_sales             =				          mer_dense.gross_reg_sales,
          gross_reg_sales_cost        =				          mer_dense.gross_reg_sales_cost,
          gross_reg_sales_fr_cost     =				          mer_dense.gross_reg_sales_fr_cost,
          sdn_in_qty                  =				          mer_dense.sdn_in_qty,
          sdn_in_selling              =				          mer_dense.sdn_in_selling,
          sdn_in_cost                 =				          mer_dense.sdn_in_cost,
          sdn_in_fr_cost              =				          mer_dense.sdn_in_fr_cost,
          sdn_in_cases                =				          mer_dense.sdn_in_cases,
          actl_store_rcpt_qty         =				          mer_dense.actl_store_rcpt_qty,
          actl_store_rcpt_selling     =				          mer_dense.actl_store_rcpt_selling,
          actl_store_rcpt_cost        =				          mer_dense.actl_store_rcpt_cost,
          actl_store_rcpt_fr_cost     =				          mer_dense.actl_store_rcpt_fr_cost,
          store_deliv_selling         =				          mer_dense.store_deliv_selling,
          store_deliv_cost            =				          mer_dense.store_deliv_cost,
          store_deliv_fr_cost         =				          mer_dense.store_deliv_fr_cost,
          store_intake_qty            =				          mer_dense.store_intake_qty,
          store_intake_selling        =				          mer_dense.store_intake_selling,
          store_intake_cost           =				          mer_dense.store_intake_cost,
          store_intake_fr_cost        =				          mer_dense.store_intake_fr_cost,
          store_intake_margin         =				          mer_dense.store_intake_margin,
          sales_returns_qty           =		              mer_dense.sales_returns_qty,
          sales_returns_selling       =				          mer_dense.sales_returns_selling,
          sales_returns_cost          =				          mer_dense.sales_returns_cost,
          sales_returns_fr_cost       =				          mer_dense.sales_returns_fr_cost,
          reg_sales_returns_qty       =				          mer_dense.reg_sales_returns_qty,
          reg_sales_returns_selling   =				          mer_dense.reg_sales_returns_selling,
          reg_sales_returns_cost      =				          mer_dense.reg_sales_returns_cost,
          reg_sales_returns_fr_cost   =				          mer_dense.reg_sales_returns_fr_cost,
          clear_sales_returns_selling =				          mer_dense.clear_sales_returns_selling,
          clear_sales_returns_cost    =				          mer_dense.clear_sales_returns_cost,
          clear_sales_returns_fr_cost =				          mer_dense.clear_sales_returns_fr_cost,
          clear_sales_returns_qty     =				          mer_dense.clear_sales_returns_qty,
          last_updated_date            =				        mer_dense.last_updated_date

      
  when not matched then 
    insert 
    (
          sk1_location_no,
          sk1_style_colour_no,
          fin_year_no,
          fin_week_no,
          fin_week_code,
          this_week_start_date,
          sk2_location_no,
          sales_qty,
          sales_cases,
          sales,
          sales_incl_vat,
          sales_cost,
          sales_fr_cost,
          sales_margin,
          franchise_sales,
          franchise_sales_margin,
          reg_sales_qty,
          reg_sales,
          reg_sales_cost,
          reg_sales_fr_cost,
          reg_sales_margin,
          franchise_reg_sales_margin,
          gross_sales_qty,
          gross_sales,
          gross_sales_cost ,
          gross_sales_fr_cost,
          gross_reg_sales_qty,
          gross_reg_sales ,
          gross_reg_sales_cost,
          gross_reg_sales_fr_cost,
          sdn_in_qty,
          sdn_in_selling,
          sdn_in_cost,
          sdn_in_fr_cost ,
          sdn_in_cases   ,
          actl_store_rcpt_qty     ,
          actl_store_rcpt_selling ,
          actl_store_rcpt_cost   ,
          actl_store_rcpt_fr_cost  ,
          store_deliv_selling   ,
          store_deliv_cost    ,
          store_deliv_fr_cost,
          store_intake_qty   ,
          store_intake_selling    ,
          store_intake_cost     ,
          store_intake_fr_cost    ,
          store_intake_margin    ,
          sales_returns_qty      ,
          sales_returns_selling   ,
          sales_returns_cost      ,
          sales_returns_fr_cost   ,
          reg_sales_returns_qty    ,
          reg_sales_returns_selling  ,
          reg_sales_returns_cost    ,
          reg_sales_returns_fr_cost  ,
          clear_sales_returns_selling ,
          clear_sales_returns_cost    ,
          clear_sales_returns_fr_cost,
          clear_sales_returns_qty    ,
          last_updated_date           
    )
    values
    (
          mer_dense.sk1_location_no,
          mer_dense.sk1_style_colour_no,
          mer_dense.fin_year_no,
          mer_dense.fin_week_no,
          mer_dense.fin_week_code,
          mer_dense.this_week_start_date,
          mer_dense.sk2_location_no,
          mer_dense.sales_qty,
          mer_dense.sales_cases,
          mer_dense.sales,
          mer_dense.sales_incl_vat,
          mer_dense.sales_cost,
          mer_dense.sales_fr_cost,
          mer_dense.sales_margin,
          mer_dense.franchise_sales,
          mer_dense.franchise_sales_margin,
          mer_dense.reg_sales_qty,
          mer_dense.reg_sales,
          mer_dense.reg_sales_cost,
          mer_dense.reg_sales_fr_cost,
          mer_dense.reg_sales_margin,
          mer_dense.franchise_reg_sales_margin,
          mer_dense.gross_sales_qty,
          mer_dense.gross_sales,
          mer_dense.gross_sales_cost,
          mer_dense.gross_sales_fr_cost,
          mer_dense.gross_reg_sales_qty,
          mer_dense.gross_reg_sales,
          mer_dense.gross_reg_sales_cost,
          mer_dense.gross_reg_sales_fr_cost,
          mer_dense.sdn_in_qty,
          mer_dense.sdn_in_selling,
          mer_dense.sdn_in_cost,
          mer_dense.sdn_in_fr_cost,
          mer_dense.sdn_in_cases,
          mer_dense.actl_store_rcpt_qty,
          mer_dense.actl_store_rcpt_selling,
          mer_dense.actl_store_rcpt_cost,
          mer_dense.actl_store_rcpt_fr_cost,
          mer_dense.store_deliv_selling,
          mer_dense.store_deliv_cost,
          mer_dense.store_deliv_fr_cost,
          mer_dense.store_intake_qty,
          mer_dense.store_intake_selling,
          mer_dense.store_intake_cost,
          mer_dense.store_intake_fr_cost,
          mer_dense.store_intake_margin,
          mer_dense.sales_returns_qty,
          mer_dense.sales_returns_selling,
          mer_dense.sales_returns_cost,
          mer_dense.sales_returns_fr_cost,
          mer_dense.reg_sales_returns_qty,
          mer_dense.reg_sales_returns_selling,
          mer_dense.reg_sales_returns_cost,
          mer_dense.reg_sales_returns_fr_cost,
          mer_dense.clear_sales_returns_selling,
          mer_dense.clear_sales_returns_cost,
          mer_dense.clear_sales_returns_fr_cost,
          mer_dense.clear_sales_returns_qty,
          mer_dense.last_updated_date
    )
    ;


      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      l_text := 'ROLLED UP YEAR AND WEEKS - 2016 (26, 27) at '||
                to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount||' total '||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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

end wh_prf_corp_510u_fix;
