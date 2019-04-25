--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_002R
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_002R" (p_success out boolean) AS
--**************************************************************************************************
--  Date:        July 2008
--  Author:      Alastair de Wet
--  Purpose:     Generate the fnd control table in the foundation layer.
--               1 record representing today's time /date values for reporting
--  Tables:      Input  - None
--               Output - dim_control_report
--  Packages:    constants, dwh_log,
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
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
g_recs_read         integer       :=  0;
g_recs_updated      integer       :=  0;
g_recs_inserted     integer       :=  0;
g_select_error      varchar2(40);


g_rec_out           dim_control_report%rowtype;
g_rec_in            dim_calendar%rowtype;
g_date              date          :=  trunc(sysdate);

l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_002U';
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'GENERATE THE CONTROL RECORD';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Process data
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.today_date               := g_date;

   g_select_error := 'Err 1 - calendar_date = g_date - 7';
   select *
   into   g_rec_in
   from   dim_calendar
   where  calendar_date = g_date - 7;
   g_rec_out.last_wk_fin_week_no      := g_rec_in.fin_week_no;
   g_rec_out.last_wk_fin_year_no      := g_rec_in.fin_year_no;
   g_rec_out.last_wk_end_date         := g_rec_in.calendar_date + (7 - g_rec_in.fin_day_no) ;
   g_rec_out.last_wk_start_date       := g_rec_out.last_wk_end_date - 6;
   g_rec_out.last_wk_season_no        := g_rec_in.season_no;

   g_select_error := 'Err 2 - calendar_date = g_date';
   select *
   into   g_rec_in
   from   dim_calendar
   where  calendar_date = g_date;
   g_rec_out.this_wk_end_date         := g_rec_in.calendar_date + (7 - g_rec_in.fin_day_no) ;
   g_rec_out.this_wk_start_date       := g_rec_out.this_wk_end_date - 6;

   g_select_error := 'Err 3 - calendar_date = g_date + 7';
   select *
   into   g_rec_in
   from   dim_calendar
   where  calendar_date = g_date + 7;
   g_rec_out.next_wk_fin_week_no      := g_rec_in.fin_week_no;
   g_rec_out.next_wk_fin_year_no      := g_rec_in.fin_year_no;

   g_select_error := 'Err 4 - calendar_date = g_date';
   select fin_year_no,fin_week_no,fin_day_no,
          fin_month_no,season_no,season_name
   into   g_rec_out.today_fin_year_no,
          g_rec_out.today_fin_week_no,
          g_rec_out.today_fin_day_no,
          g_rec_out.today_fin_month_no,
          g_rec_out.this_season_no,
          g_rec_out.this_season_name
   from   dim_calendar
   where  calendar_date = g_date;

   g_select_error := 'Err 5 - calendar_date = g_date - 1';
   select calendar_date,fin_year_no,fin_week_no,fin_day_no
   into   g_rec_out.yesterday_date,
          g_rec_out.yesterday_fin_year_no,
          g_rec_out.yesterday_fin_week_no,
          g_rec_out.yesterday_fin_day_no
   from   dim_calendar
   where  calendar_date = g_date - 1;

   g_select_error := 'Err 6 - calendar_date = g_date - 2';
   select calendar_date,fin_year_no,fin_week_no,fin_day_no
   into   g_rec_out.eergister_date,
          g_rec_out.eergister_fin_year_no,
          g_rec_out.eergister_fin_week_no,
          g_rec_out.eergister_fin_day_no
   from   dim_calendar
   where  calendar_date = g_date - 2;

   g_rec_out.last_yr_fin_year_no  := g_rec_out.today_fin_year_no;
   g_rec_out.last_mn_fin_month_no := g_rec_out.today_fin_month_no - 1;
   if g_rec_out.last_mn_fin_month_no = 0 then
      g_rec_out.last_mn_fin_month_no := 12;
      g_rec_out.last_yr_fin_year_no  := g_rec_out.last_yr_fin_year_no - 1;
   end if;

   g_select_error := 'Err 7 - fin_month_no & fin_year_no';
   select max(calendar_date), min(calendar_date)
   into   g_rec_out.this_mn_end_date,g_rec_out.this_mn_start_date
   from   dim_calendar
   where  fin_month_no = g_rec_out.today_fin_month_no and
          fin_year_no  = g_rec_out.today_fin_year_no;

   g_rec_out.last_updated_date := g_date;

   exception

      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||g_select_error|| ' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   insert into dim_control_report values g_rec_out;
   g_recs_inserted         := g_recs_inserted + sql%rowcount;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'GENERATE dim_control_report TABLE STARTED AT '||
     to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************

       delete from dim_control_report;
       local_address_variable;
       local_write_output;



--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');

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
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
END WH_PRF_CORP_002R;
