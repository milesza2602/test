--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_581U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_581U" 
                                                                                               (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  date:        22 may 2018
--  author:      Karna Nallamothu
--  purpose:     update sales forecast
--               with input trading Group Sales forecast table from foundation layer.
--
--  tables:      input  - fnd_area_wk_trdgrp_fcst_ratio
--               output - rtl_area_wk_trdgrp_fcst_ratio
--  packages:    constants, dwh_log, dwh_valid
--
--  maintenance:

--
-- note: this version attempts to do a bulk insert / update / hospital. downside is that hospital message is generic!!
--       this would be appropriate for large loads where most of the data is for insert like with sales transactions.

--  naming conventions
--  g_  -  global variable
--  l_  -  log table variable
--  a_  -  array variable
--  v_  -  local variable as found in packages
--  p_  -  parameter
--  c_  -  prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;

g_start_date         date;
g_end_date           date;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_581U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'load wh plan fact data from jda';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_stg_fcst_fcst_plan is
     select /*+ full(fnd)  parallel (fnd,2) */
              dl.sk1_area_no,
              dc.fin_year_no,
              Dc.Fin_Week_No,
              dc.fin_day_no,
              dc.this_week_start_date,
              fnd.forecast_sales,
              fnd.forecast_ratio_perc,
              fnd.financial_forecast,
              fnd.fresh_forecast,
              fnd.fresh_forecast_ratio_perc,
              fnd.fresh_finance_forecast,
              fnd.ll_forecast,
              Fnd.Ll_Forecast_Ratio_Perc,
              fnd.ll_finance_forecast
     from     fnd_area_wk_trdgrp_fcst_ratio fnd,
              rtl_area_wk_trdgrp_fcst_ratio prf,
              dim_calendar dc,
              dim_area dl
     where    dl.sk1_area_no        = prf.sk1_area_no
     and      dc.fin_year_no        = prf.fin_year_no
     and      dc.fin_week_no        = prf.fin_week_no
     and      fnd.area_no           = dl.area_no
     and      fnd.fin_year_no       = dc.fin_year_no
     And      Fnd.Fin_Week_No       = Dc.Fin_Week_No
     and      fnd.fin_day_no        = dc.fin_day_no
     and      fnd.last_updated_date = g_date
     group by dl.sk1_area_no,
              dc.fin_year_no,
              Dc.Fin_Week_No,
              dc.fin_day_no,
              Dc.This_Week_Start_Date,
              fnd.fin_day_no,
              fnd.forecast_sales,
              fnd.forecast_ratio_perc,
              Fnd.Financial_Forecast,
              fnd.fresh_forecast,
              fnd.fresh_forecast_ratio_perc,
              fnd.fresh_finance_forecast,
              fnd.ll_forecast,
              fnd.ll_forecast_ratio_perc,
              fnd.ll_finance_forecast
     order by dl.sk1_area_no,
              dc.fin_year_no,
              Dc.Fin_Week_No,
              fnd.fin_day_no,
              dc.this_week_start_date;

--**************************************************************************************************
-- insert all record flaged as 'i' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
Begin
  --     g_rec_out.last_updated_date         := g_date;

      insert /*+ append parallel (prf,2) */ into rtl_area_wk_trdgrp_fcst_ratio prf
      Select /*+ full(fnd)  */
              dl.sk1_area_no,
              dc.fin_year_no,
              Dc.Fin_Week_No,
              dc.fin_day_no,
              dc.this_week_start_date,
              fnd.forecast_sales,
              fnd.forecast_ratio_perc,
              fnd.financial_forecast,
              fnd.fresh_forecast,
              fnd.fresh_forecast_ratio_perc,
              fnd.fresh_finance_forecast,
              fnd.ll_forecast,
              Fnd.Ll_Forecast_Ratio_Perc,
              fnd.ll_finance_forecast,
              g_date as last_updated_date

     from     fnd_area_wk_trdgrp_fcst_ratio fnd,
              dim_calendar dc,
              Dim_Area Dl
     where    fnd.area_no         = dl.area_no
     and      fnd.fin_year_no     = dc.fin_year_no
     And      Fnd.Fin_Week_No     = Dc.Fin_Week_No
     And      Fnd.fin_day_no     = Dc.fin_day_no
    and       fnd.last_updated_date = g_date
     and      not exists
              (Select /*+ full (r) */  * From rtl_area_wk_trdgrp_fcst_ratio r
               where   sk1_area_no      = dl.sk1_area_no and
                       Fin_Year_No      = Dc.Fin_Year_No And
                       Fin_Week_No      = Dc.Fin_Week_No and
                       fin_day_no       = dc.fin_day_no)
                       group by   dl.sk1_area_no,
                                  dc.fin_year_no,
                                  Dc.Fin_Week_No,
                                  dc.fin_day_no,
                                  dc.this_week_start_date,
                                  fnd.forecast_sales,
                                  fnd.forecast_ratio_perc,
                                  fnd.financial_forecast,
                                  FND.FRESH_FORECAST,
                                  fnd.fresh_forecast_ratio_perc,
                                  fnd.fresh_finance_forecast,
                                  fnd.ll_forecast,
                                  fnd.ll_forecast_ratio_perc,
                                  fnd.ll_finance_forecast ;

      g_recs_inserted := g_recs_inserted + sql%rowcount;
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'flag insert - insert error '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'flag insert - other error '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
End Flagged_Records_Insert;

--**************************************************************************************************
-- insert all record flaged as 'i' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin

for upd_rec in c_stg_fcst_fcst_plan
   Loop
     Update rtl_area_wk_trdgrp_fcst_ratio Prf
     Set  Prf.Forecast_Sales      	   = Upd_Rec.Forecast_Sales,
          prf.forecast_ratio_perc			 = upd_rec.forecast_ratio_perc,
          prf.financial_forecast				= upd_rec.financial_forecast,
          prf.fresh_forecast					  = upd_rec.fresh_forecast,
          prf.fresh_forecast_ratio_perc	= upd_rec.fresh_forecast_ratio_perc,
          prf.fresh_finance_forecast		= upd_rec.fresh_finance_forecast,
          prf.ll_forecast						    = upd_rec.ll_forecast,
          prf.ll_forecast_ratio_perc		= upd_rec.ll_forecast_ratio_perc,
          Prf.Ll_Finance_Forecast				= Upd_Rec.Ll_Finance_Forecast,
	        Prf.Last_Updated_Date         = g_date
     Where  Prf.Sk1_Area_No             = Upd_Rec.Sk1_Area_No And
            prf.fin_year_no             = upd_rec.fin_year_no  and
            Prf.Fin_Week_No             = Upd_Rec.Fin_Week_No  And
            Prf.Fin_Day_No              = Upd_Rec.Fin_Day_No ;


      g_recs_updated := g_recs_updated + 1;
   End Loop;
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'flag update - insert error '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'flag update - other error '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;



--**************************************************************************************************
-- main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'batch date being processed is:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- call the bulk routines
--**************************************************************************************************

    select count(*)
    into   g_recs_read
    from   dwh_foundation.fnd_area_wk_trdgrp_fcst_ratio
    where  last_updated_date = g_date;

    l_text := 'bulk update started at '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'bulk insert started at '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated then
      l_text :=  'record counts do not balance - check your code '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'error - record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'record count error - see log files');
   end if;


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
       Rollback;
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

end "WH_PRF_CORP_581U";
