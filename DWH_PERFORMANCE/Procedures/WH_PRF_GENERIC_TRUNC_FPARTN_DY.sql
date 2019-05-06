--------------------------------------------------------
--  DDL for Procedure WH_PRF_GENERIC_TRUNC_FPARTN_DY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_GENERIC_TRUNC_FPARTN_DY" 
--                                                                                (p_success out boolean, p_start_date in date, p_no_of_days in integer, p_table_name in varchar2)
                                                                                (p_success out boolean, p_no_of_days in integer, p_table_name in varchar2)
as
--**************************************************************************************************
--  Date:        April 2012
--  Author:      A Joshua
--  Purpose:     Truncate daily future subpartitions on table prior to rollup or load.
--               PARAMS ARE: THE DATE ON WHICH YOU WANT TO START,
--                           THE NO OF DAYS FORWARD YOU WANT TO TRUNCATE AND
--                           THE TABLE NAME YOU WISH TO TRUNCATE DAILY FUTURE SUBPARTITIONS ON
--                           For RDF daily forecast user 21 day partition drop
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

g_sub                  integer       :=  0;
g_no_of_days           number(3)     :=  0;
g_fin_day_no           number(1)     :=  0;
g_date                 date;
g_partn_date           date;
g_this_week_start_date date;
g_this_week_end_date   date;
g_table_name           varchar2(50);
g_partition_name       varchar2(100);
g_table_short_name     varchar2(30);
g_stmt                 varchar2(1500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_GENERIC_TRUNC_FPARTN_DY';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TRUNCATE DAILY FUTURE PARTITIONS ON SPECIFIED TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_index_cnt          integer;
g_index_name	       sys.dba_indexes.index_name%type;
g_table_name_only    sys.dba_indexes.index_name%type;
begin

    p_success := false;

    g_table_name  := upper(p_table_name);
    g_table_name_only := g_table_name;

    select table_short_name
    into   g_table_short_name
    from   DWH_META_DATA.DWH_PART_META_DATA
    where  table_name  = g_table_name;

    g_table_name_only := trim(g_table_name);
    g_table_name := 'DWH_PERFORMANCE.'||trim(g_table_name);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'TRUNCATE DAILY PARTIONS ON '||trim(g_table_name)||' STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;

    select fin_day_no, calendar_date + 2
    into   g_fin_day_no, g_this_week_start_date
    from   dim_calendar
    where  calendar_date = g_date;

--    if p_start_date is null then
       g_date                 := g_this_week_start_date;
--    else
--       g_date                 := p_start_date;
--    end if;

    if p_no_of_days is null then
       g_no_of_days           := 0;
    else
       g_no_of_days           := p_no_of_days;
    end if;

    select calendar_date + g_no_of_days
    into   g_this_week_end_date
    from   dim_calendar
    where  calendar_date = g_date;

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'START DATE, NO OF DAYS FORWARD TO TRUNCATE AND END DATE:- '||g_this_week_start_date||' '||g_no_of_days||' '||g_this_week_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------    execute immediate g_stmt;

   for g_sub in 0 .. g_no_of_days
     loop

    g_partn_date := g_date +  g_sub  ;

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
   
      -- Check if the indexes are left in an UNUSABLE state due to partition truncate.
-- This is caused by an index being a Global one on a partitioned table
-- If such indexes are found, they need to be rebuilt to prevent subsequent jobs from aborting
/*    l_text := 'CHECKING INDEXES ON '||G_TABLE_NAME;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   SELECT COUNT(*) 
      INTO G_INDEX_CNT
      from dba_indexes
     WHERE TABLE_OWNER = 'DWH_PERFORMANCE'
       AND TABLE_NAME = g_table_name_only
       AND status NOT IN ('N/A', 'USABLE','VALID');
       
    IF G_INDEX_CNT > 0 THEN
       SELECT INDEX_NAME 
         INTO G_INDEX_NAME
         FROM SYS.DBA_INDEXES 
        WHERE TABLE_OWNER = 'DWH_PERFORMANCE'
          AND TABLE_NAME = g_table_name_only
          AND status NOT IN ('N/A', 'USABLE','VALID');
          
       g_stmt := 'alter index DWH_PERFORMANCE.'||g_index_name||' rebuild parallel 1';
       execute immediate g_stmt;
       
       l_text := 'INDEX ' || G_INDEX_NAME || ' was UNUSABLE and has been rebuilt';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    END IF;
*/

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

end wh_prf_generic_trunc_fpartn_dy;
