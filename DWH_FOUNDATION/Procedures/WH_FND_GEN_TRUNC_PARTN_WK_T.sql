--------------------------------------------------------
--  DDL for Procedure WH_FND_GEN_TRUNC_PARTN_WK_T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_GEN_TRUNC_PARTN_WK_T" 
 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
-- PARAMETERS OF LIVE JOB (p_success out boolean, p_no_of_weeks in integer, p_table_name in varchar2)
--**************************************************************************************************
--  Date:        February 2013
--  Author:      A Joshua
--  Purpose:     Truncate weekly subpartitions on table prior to rollup or load
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_no_of_weeks        integer;
g_table_name         varchar2(50);
g_partition_name     varchar2(100);
g_table_short_name   varchar2(30);

g_sub                integer       :=  0;
g_month_code         varchar2(7);
g_week               number(2);
g_date               date;
g_partn_date         date;
g_stmt               varchar2(1500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_GEN_TRUNC_PARTN_WK_T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TRUNCATE WEEKLY PARTITIONS ON SPECIFIED TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
begin

    p_success := false;

    g_no_of_weeks := 3 - 1;
    g_table_name  := upper('FND_AST_LOC_SC_WK_ACT');

    select table_short_name
    into   g_table_short_name
    from   DWH_META_DATA.DWH_PART_META_DATA
    where  table_name  = g_table_name;

    g_table_name := 'dwh_foundation.'||g_table_name;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'TRUNCATE WEEKLY PARTIONS ON '||trim(g_table_name)||' STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



for g_sub in 0 .. g_no_of_weeks
  loop

    g_partn_date := g_date - (g_sub * 7);
    select fin_month_code,fin_week_no
    into   g_month_code,g_week
    from   dim_calendar
    where  calendar_date = g_partn_date;




    g_partition_name := g_table_short_name||'_'||g_month_code||'_'|| g_week;
    g_stmt           := 'alter table '||g_table_name||' truncate SUBPARTITION '||g_partition_name;

    l_text := 'PARTITION TO BE TRUNCATED :- '||g_partition_name ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'DDL Statement :- '||g_stmt ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    DBMS_LOCK.sleep(0.25);      -- added 25 Oct 2016 to prevent shared pool issues
end loop;
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,'','','','','');
    l_text := 'TRUNCATE WEEKLY PARTIONS ON '||trim(g_table_name)||' ENDED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END WH_FND_GEN_TRUNC_PARTN_WK_T;
