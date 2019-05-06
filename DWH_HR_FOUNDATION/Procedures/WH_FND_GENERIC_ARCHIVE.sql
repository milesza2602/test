--------------------------------------------------------
--  DDL for Procedure WH_FND_GENERIC_ARCHIVE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_FND_GENERIC_ARCHIVE" (p_table_name VARCHAR2,
                                      p_log_script_name  sys_dwh_log.log_script_name%type,
                                      p_log_procedure_name sys_dwh_log.log_procedure_name%type,
                                      p_description       sys_dwh_log_summary.log_description%type) AS
--**************************************************************************************************
--  Date:        June 2008
--  Author:      Janakies Louca
--  Purpose:     Archive and Drop the CPY table passed as input parameter
--
--  Parameters:  p_table_name
--               p_log_script_name
--               p_log_procedure_name
--               p_description
--
--  Tables:      Input  - Copy table to archive/drop
--               Output - Respective Archive table
--  Packages:    dwh_log,
--
--  Maintenance:
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
g_stmt              varchar2(500);
g_arc_table_name    varchar2(31);
g_hsp_table_name    varchar2(31);
g_cpy_table_name    varchar2(31);

l_message           sys_dwh_errlog.log_text%type;
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_stage;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_archive;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := upper(p_log_procedure_name);
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type
                    := upper(p_description);

--******************************************************************************
begin
---    g_arc_table_name := upper(substr(p_table_name, 1, instr(upper(p_table_name), 'CPY',1)-1)||'arc');

    g_arc_table_name := upper(p_table_name||'_ARC');
    g_hsp_table_name := upper(p_table_name||'_HSP');
    g_cpy_table_name := upper(p_table_name||'_CPY');

    l_text :=
    '===================================================================================';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ARCHIVE OF '||g_cpy_table_name||' STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************

    g_stmt := 'insert into '||g_arc_table_name||' select * from '||g_cpy_table_name;

    execute immediate g_stmt;
    g_recs_inserted := sql%rowcount;

    l_text := 'ARCHIVE OF '||g_cpy_table_name||' ENDED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'NOW DROPPING '||g_cpy_table_name||' - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_stmt := 'drop table '||g_cpy_table_name;

    execute immediate g_stmt;

    l_text := 'DROP COMPLETED '||g_cpy_table_name||' - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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


EXCEPTION
   when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);

       RAISE;

END WH_FND_GENERIC_ARCHIVE;
