--------------------------------------------------------
--  DDL for Procedure RDF_INSERT_WK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."RDF_INSERT_WK" 
(p_forall_limit in integer,p_success out boolean
) 
as
--**************************************************************************************************
--  Date:        April 2015
--  Author:      Quentin Smit
--  Purpose:     Copy data for the RDF L1 / L2 migration
--               
--  Tables:      Input  - RTL_LOC_ITEM_RDF_WKFCST_L1
--               Output - RTL_LOC_ITEM_WK_RDF_FCST
--  Packages:    constants, dwh_log, dwh_valid
--

--**************************************************************************************************


  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_recs_hospital integer := 0;
  g_recs_deleted  integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_sub           integer := 0;
  g_part_name     varchar2(30);
  g_fin_month_code dim_calendar.fin_month_code%type;

  
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  g_end_date date ;
  g_yesterday date := trunc(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'RDF_INSERT_WK';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'INSERT INTO RTL_LOC_ITEM_WK_RDF_FCST FROM RTL_LOC_ITEM_WK_RDF_FCST';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  g_start_fin_year_no dim_calendar.fin_year_no%type;
  g_start_fin_week_no dim_calendar.fin_week_no%type;

  -- For Output Arrays Into Bulk Load Forall Statements --
  --**************************************************************************************************
  -- Main Process
  --**************************************************************************************************

begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  dbms_output.put_line('BULK WRITE LIMIT '||p_forall_limit||' '||g_forall_limit);

  p_success := false;

  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'INSERT INTO RTL_LOC_ITEM_RDF_WKFCST_L1 FROM RTL_LOC_ITEM_WK_RDF_FCST started at '|| to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Look Up Batch Date From Dim_Control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  --g_date := '11/MAY/13';
  l_text := 'Batch date being processed is :- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  execute immediate 'alter session set workarea_size_policy=manual';
  execute immediate 'alter session set sort_area_size=100000000';
  execute immediate 'alter session enable parallel dml';
  
  g_start_fin_year_no := 2014;
  g_start_fin_week_no := 30;

  --**************************************************************************************************
  -- Insert Into RTL_LOC_ITEM_DY_RDF_FCST_L1
  --**************************************************************************************************
  g_recs_inserted := 0;
  
  while g_start_fin_week_no > 0 loop

       l_text := 'year and week being copied is :- '||g_start_fin_year_no || ' ' || g_start_fin_week_no;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       insert /*+ parallel(a,8) */ into DWH_PERFORMANCE.RTL_LOC_ITEM_RDF_WKFCST_L1 a
       select /*+ parallel(b,8) */ b.* from DWH_PERFORMANCE.RTL_LOC_ITEM_WK_RDF_FCST b 
        where b.fin_year_no = g_start_fin_year_no and b.fin_week_no = g_start_fin_week_no;
     
       commit;
       g_start_fin_week_no := g_start_fin_week_no - 1;
       g_recs_inserted := g_recs_inserted + sql%rowcount;
   end loop;
   
  l_text := 'Insert completed NEW:- RECS =  '||g_recs_inserted||' '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   commit;


  --**************************************************************************************************
  -- Write Final Log Data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_deleted||g_recs_deleted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;

  p_success := true;

exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
when others then
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;


END RDF_INSERT_WK;
