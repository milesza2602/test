--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_715X
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_715X" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2015
--  Author:      Alastair de Wet
--  Purpose:     Create SPACERACE extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - vw_extr_space_race
--               Output - flat file extracts
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_715X';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT SPACERACE DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_last_week_no      dim_calendar.fin_week_no%type;
g_fin_year_no       dim_calendar.fin_year_no%type;
g_spacerace_file    varchar2(20 byte);

g_string            varchar2(5000 byte);


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT SPACERACE DATA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    select last_wk_fin_year_no, last_wk_fin_week_no
      into g_fin_year_no, g_last_week_no
      from dim_control;
      
    --g_fin_year_no  := 2017;   
    --g_last_week_no := 5;
    
 --   while g_last_week_no < 17 loop
      
        g_spacerace_file := 'spacerace_' || g_fin_year_no ||'_'|| g_last_week_no;
        l_text := ' filename = ' || g_spacerace_file;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
        DBMS_OUTPUT.PUT_LINE('spacerace extract');
        
        ----  AND A.Waste_Qty        IS NOT NULL   removed from Selsprs
     
        G_COUNT := DWH_GENERIC_FILE_EXTRACT(
       'select * from DWH_PERFORMANCE.RTL_LOC_ITEM_WK_SPACERACE',
    
       -- '|','DWH_FILES_OUT','space_race1.txt'); 
        
        '|','DWH_FILES_OUT', g_spacerace_file );
      
        
      --  g_count := dwh_generic_file_extract('select * from vw_extr_space_race','|','DWH_FILES_OUT','spacerace_'''|| g_fin_year_no ||'''2425’');
     --   g_count := dwh_generic_file_extract('select * from vw_extr_space_race','|','DWH_FILES_OUT','spacerace_'''|| g_fin_year_no ||'''2425');
    
      l_text :=  'Records extracted to space_race1.txt'||g_count;
     --   l_text :=  'Records extracted to ' g_spacerace_file '|| ' g_count;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
--    g_last_week_no := g_last_week_no + 1;
  
--  end loop;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

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
end wh_prf_corp_715x;
