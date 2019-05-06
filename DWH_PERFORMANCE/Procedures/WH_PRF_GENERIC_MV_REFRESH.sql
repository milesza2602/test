--------------------------------------------------------
--  DDL for Procedure WH_PRF_GENERIC_MV_REFRESH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_GENERIC_MV_REFRESH" (p_table in varchar2,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jan 2011
--  Author:      Alastair de Wet
--  Purpose:     Refresh mv test
--  Tables:
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions:
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
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_command_string     varchar2(100)  :=  '';
g_table              varchar2(100)  :=  '';

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_GENERIC_MV_REFRESH';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Refresh a Materialized View';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Start Refresh'||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_command_string := 'truncate table dwh_performance.'||p_table;
    execute immediate (g_command_string);
    l_text := g_command_string;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
  
    g_command_string := 'alter MATERIALIZED VIEW DWH_PERFORMANCE.'||p_table||' parallel 4';
    execute immediate (g_command_string);
    l_text := g_command_string;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    
    g_table := 'DWH_PERFORMANCE.'||p_table;

    DBMS_MVIEW.REFRESH
      (g_table,'C');

    g_command_string := 'alter MATERIALIZED VIEW DWH_PERFORMANCE.'||p_table||' noparallel';
    execute immediate (g_command_string);
    l_text := g_command_string;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

--DBMS_MVIEW.REFRESH
--   ('mv_dim_location','f');

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************


    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end wh_prf_generic_mv_refresh ;
