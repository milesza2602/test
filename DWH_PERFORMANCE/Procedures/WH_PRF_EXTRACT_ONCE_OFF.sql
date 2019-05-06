--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXTRACT_ONCE_OFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXTRACT_ONCE_OFF" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        November 2009
--  Author:      M Munnik
--  Purpose:     Anybody may change this procedure to do a once-off extract to flat file.
--               Change the sql statement and the file name.
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_count              number        :=  0;
g_date               date;
g_start_date         date;
g_end_date           date;
g_end_date1           date;
g_sql                varchar2(2000);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXTRACT_ONCE_OFF';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT DATA TO FLAT FILE';
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

   l_text := 'EXTRACT OF DATA STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from vw_extr_nielsens','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************

   g_start_date := '30 mar 2010';
   g_end_date   := '06 apr 2010';
   g_end_date1  := '31 mar 2010';

--   g_sql := 'select * '||
--            'from rtl_depot_item_wk ';
            
  g_sql := 
  'with dnse as '||
'(select r.post_date, c.fin_year_no, c.fin_week_no, c.fin_day_no, l.location_no, i.item_no,  '||
'round(r.sales,0) sales, round(r.store_intake_selling,0) intake '||
'from rtl_loc_item_dy_rms_dense r join dim_calendar c on r.post_date = c.calendar_date '||
'join dim_item i on r.sk1_item_no = i.sk1_item_no '||
'join dim_location l on r.sk1_location_no = l.sk1_location_no '||
'where r.post_date between ''11 Jan 2010'' and ''17 Jan 2010'' '||
'and i.business_unit_no = 50 '||
'and ((r.sales is not null) or (r.store_intake_selling is not null))), '||
'sprse as '||
'(select r.post_date, c.fin_year_no, c.fin_week_no, c.fin_day_no, l.location_no, i.item_no, round(r.waste_selling,0) waste '||
'from rtl_loc_item_dy_rms_sparse r join dim_calendar c on r.post_date = c.calendar_date '||
'join dim_item i on r.sk1_item_no = i.sk1_item_no '||
'join dim_location l on r.sk1_location_no = l.sk1_location_no '||
'where r.post_date between ''11 Jan 2010'' and ''17 Jan 2010'' '||
'and i.business_unit_no = 50 '||
'and (r.waste_selling is not null)) '||
'select nvl(d.fin_year_no, s.fin_year_no) year, nvl(d.fin_week_no, s.fin_week_no) week, nvl(d.fin_day_no, s.fin_day_no) day,  '||
'nvl(d.location_no, s.location_no) store, nvl(d.item_no, s.item_no) sku,  '||
'nvl(d.sales,0) sales, nvl(d.intake,0) intake, nvl(s.waste,0) waste '||
'from dnse d full outer join sprse s '||
'on d.post_date = s.post_date '||
'and d.location_no = s.location_no '||
'and d.item_no = s.item_no';
  
   g_count := dwh_generic_file_extract(g_sql,'|','DWH_FILES_OUT','xmas_wk29.txt');
   l_text  := 'Records extracted for Sales Week 29 = '||g_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,'','','','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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

end wh_prf_extract_once_off;
