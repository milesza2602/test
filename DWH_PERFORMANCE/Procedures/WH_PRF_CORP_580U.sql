--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_580U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_580U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2013
--  Author:      Wendy Lyttle
--  Purpose:     Update Sales Forecast
--               with input ex Sales Forecast  table from foundation layer.
--
--  Tables:      Input  - fnd_excel_fd_forecast_ratio
--               Output - rtl_excel_fd_forecast_ratio
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--               13 Apr 2016 BarryK - Add required code for 3 additional columns added to source FND & target RTL tables 
--               and drop down to DAY granularity                                                 (Ref: BK13/04/16)
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     INTEGER       :=  0;

g_start_date         date;
g_end_date           date;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_580U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_stg_om_wh_plan is
     select /*+ FULL(stg)  parallel (stg,2) */
              dl.sk1_area_no,
              dc.fin_year_no,
              dc.fin_week_no,
--              dc.FIN_DAY_NO,
              dc.THIS_WEEK_START_date,
              fnd.forecast_sales,
              fnd.source_data_status_code,
              fnd.FIN_DAY_NO,                                                   -- Ref: BK13/04/16
              fnd.FORECAST_RATIO_PERC,                                          -- Ref: BK13/04/16
              fnd.FINANCIAL_FORECAST                                            -- Ref: BK13/04/16
     from     dwh_foundation.fnd_excel_fd_forecast_ratio fnd,
              rtl_excel_fd_forecast_ratio prf,
              dim_calendar dc,
              dim_area dl
     where    dl.sk1_area_no        = prf.sk1_area_no
     and      dc.fin_year_no        = prf.fin_year_no
     and      dc.fin_week_no        = prf.fin_week_no
     and      dc.fin_day_no         = prf.fin_day_no
     and      fnd.area_no           = dl.area_no
     and      fnd.fin_year_no       = dc.fin_year_no
     and      fnd.fin_week_no       = dc.fin_week_no
     and      fnd.fin_day_no        = dc.fin_day_no
--     and      dc.calendar_date  between g_start_date and g_end_date
     and      fnd.last_updated_date = g_date
     GROUP BY dl.sk1_area_no,
              dc.fin_year_no,
              dc.fin_week_no,
--              dc.FIN_DAY_NO,
              dc.THIS_WEEK_START_date,
              fnd.forecast_sales,
              fnd.source_data_status_code,
              fnd.FIN_DAY_NO,                                                   -- Ref: BK13/04/16
              fnd.FORECAST_RATIO_PERC,                                          -- Ref: BK13/04/16
              fnd.FINANCIAL_FORECAST                                            -- Ref: BK13/04/16
     order by dl.sk1_area_no,
              dc.fin_year_no,
              dc.fin_week_no,
              fnd.FIN_DAY_NO,
              dc.THIS_WEEK_START_date;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into rtl_excel_fd_forecast_ratio prf
      select /*+ FULL(cpy)  parallel (cpy,2) */
              dl.sk1_area_no,
              dc.fin_year_no,
              dc.fin_week_no,
              dc.THIS_WEEK_START_date,
              fnd.forecast_sales,
              g_date as last_updated_date,
              fnd.FIN_DAY_NO,                                                   -- Ref: BK13/04/16
              fnd.FORECAST_RATIO_PERC,                                          -- Ref: BK13/04/16
              fnd.FINANCIAL_FORECAST                                            -- Ref: BK13/04/16
              
              
     from     dwh_foundation.fnd_excel_fd_forecast_ratio fnd,
              dim_calendar dc,
              dim_area dl
     where    fnd.area_no       = dl.area_no
     and      fnd.fin_year_no   = dc.fin_year_no
     and      fnd.fin_week_no   = dc.fin_week_no
     and      fnd.fin_day_no    = dc.fin_day_no
     and      fnd.last_updated_date = g_date
--     and      dc.calendar_date  between g_start_date and g_end_date
     and      not exists
              (select /*+ nl_aj */  * from rtl_excel_fd_forecast_ratio
               where   sk1_area_no   = dl.sk1_area_no and
                       fin_year_no   = dc.fin_year_no and
                       fin_week_no   = dc.fin_week_no and
                       fin_day_no    = dc.fin_day_no)
               GROUP BY 
                       dl.sk1_area_no,
                       dc.fin_year_no,
                       dc.fin_week_no,
                       dc.THIS_WEEK_START_date,
                       fnd.forecast_sales,
                       fnd.FIN_DAY_NO,                                          -- Ref: BK13/04/16
                       fnd.FORECAST_RATIO_PERC,                                 -- Ref: BK13/04/16
                       fnd.FINANCIAL_FORECAST;                                  -- Ref: BK13/04/16

      g_recs_inserted := g_recs_inserted + sql%rowcount;
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin

for upd_rec in c_stg_om_wh_plan
   loop
     update rtl_excel_fd_forecast_ratio prf
     set    prf.forecast_sales      = upd_rec.forecast_sales,
            prf.last_updated_date   = g_date,
--            prf.FIN_DAY_NO          = upd_rec.FIN_DAY_NO,                       -- Ref: BK13/04/16
            prf.FINANCIAL_FORECAST  = upd_rec.FINANCIAL_FORECAST,               -- Ref: BK13/04/16;
            prf.FORECAST_RATIO_PERC = upd_rec.FORECAST_RATIO_PERC               -- Ref: BK13/04/16
     where  prf.sk1_area_no         = upd_rec.sk1_area_no  and
            prf.fin_year_no         = upd_rec.fin_year_no  and
            prf.fin_week_no         = upd_rec.fin_week_no  and
            prf.fin_day_no          = upd_rec.fin_day_no  ;

      g_recs_updated := g_recs_updated + 1;
   end loop;


      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************

    select count(*)
    into   g_recs_read
    from   DWH_FOUNDATION.fnd_excel_fd_forecast_ratio
    where  last_updated_date = g_date;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- Write final log data
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
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
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

END WH_PRF_CORP_580U;
