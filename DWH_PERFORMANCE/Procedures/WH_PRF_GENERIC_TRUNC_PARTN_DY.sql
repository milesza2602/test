--------------------------------------------------------
--  DDL for Procedure WH_PRF_GENERIC_TRUNC_PARTN_DY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_GENERIC_TRUNC_PARTN_DY" 
                                                                                (p_success out boolean, p_start_date in date, p_no_of_days in integer, p_table_name in varchar2)
as
--**************************************************************************************************
--  Date:        June 2009
--  Author:      A de Wet
--  Purpose:     Truncate daily subpartitions on table prior to rollup or load.
--               PARAMS ARE: THE DATE ON WHICH YOU WANT TO START, 
--                           THE NO OF DAYS BACK YOU WANT TO TRUNCATE AND
--                           THE TABLE NAME YOU WISH TO TRUNCATE DAILY SUBPARTITIONS ON
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
 
g_no_of_days         integer;
g_table_name         varchar2(50);
g_partition_name     varchar2(100);
g_table_short_name   varchar2(30);

g_sub                integer       :=  0;

g_date               date;
g_partn_date         date;
g_stmt               varchar2(1500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_GENERIC_TRUNC_PARTN_DY';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TRUNCATE DAILY PARTITIONS ON SPECIFIED TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
begin
    
    p_success := false;
    
    g_no_of_days  := p_no_of_days - 1;
    g_table_name  := upper(p_table_name);
    
    select table_short_name
    into   g_table_short_name
    from   DWH_META_DATA.DWH_PART_META_DATA
    where  table_name  = g_table_name;
    
    g_table_name := 'dwh_performance.'||g_table_name;
    
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'TRUNCATE DAILY PARTIONS ON '||trim(g_table_name)||' STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    if p_start_date is not null then 
       g_date := p_start_date;
    end if;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'START DATE AND NO OF DAYS BACK TO TRUNCATE:- '||g_date||'  '|| g_no_of_days;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--------    execute immediate g_stmt;      
    
for g_sub in 0 .. g_no_of_days
  loop
    
    g_partn_date := g_date -  g_sub  ;
 
     
    g_partition_name := g_table_short_name||'_'||TO_CHAR(g_partn_date, 'DDMMYY');
    g_stmt           := 'alter table '||g_table_name||' truncate SUBPARTITION '||g_partition_name;
    
    l_text := 'PARTITION TO BE TRUNCATED :- '||g_partition_name ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'DDL Statement :- '||g_stmt ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    execute immediate g_stmt;    
    commit;
    DBMS_LOCK.sleep(0.25);      -- added 25 Oct 2016 to prevent shared pool issues
end loop;
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,'','','','','');
    l_text := 'TRUNCATE DAILY PARTIONS ON '||trim(g_table_name)||' ENDED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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

end wh_prf_generic_trunc_partn_dy;