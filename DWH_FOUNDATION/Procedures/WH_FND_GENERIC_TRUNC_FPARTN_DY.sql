--------------------------------------------------------
--  DDL for Procedure WH_FND_GENERIC_TRUNC_FPARTN_DY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_GENERIC_TRUNC_FPARTN_DY" 
 (p_success out boolean, p_no_of_days in integer, p_table_name in varchar2)
as
--**************************************************************************************************
--  Date:        April 2010
--  Author:      W Lyttle
--  Purpose:     Truncate DAILY subpartitions on table prior to rollup or load
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

g_no_of_days        integer;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_GENERIC_TRUNC_FPARTN_DY';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TRUNCATE DAILY PARTITIONS ON SPECIFIED TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;  
g_index_cnt          integer;
g_index_name	       VARCHAR2(30);
--g_index_name	       sys.dba_indexes.index_name%type;
begin

    p_success := false;
     dwh_lookup.dim_control(g_date);

     --g_date := '11/JAN/15';

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    g_no_of_days := p_no_of_days + 1;
    g_table_name  := upper(p_table_name);
    
    --if upper(p_table_name) = 'FND_JDAFF_ST_PLAN_ANALYSIS_WK' then
    --   g_no_of_days := 133;
    --end if;

     l_text := 'PARAMETERS:- no of days='||g_no_of_days||' table='||g_table_name;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select table_short_name
    into   g_table_short_name
    from   DWH_META_DATA.DWH_PART_META_DATA
    where  table_name  = g_table_name;

    --g_table_short_name := 'FND_STPLN';

    g_table_name := 'DWH_FOUNDATION.'||g_table_name;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'TRUNCATE DAILY PARTITIONS ON '||trim(g_table_name)||' STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


for g_sub in 2 .. g_no_of_days
  loop

    g_partn_date := g_date + g_sub;

    g_partition_name := g_table_short_name||'_'||to_char(g_partn_date,'ddmmyy');

    g_stmt           := 'alter table '||g_table_name||' truncate SUBPARTITION '||g_partition_name;
    --dbms_output.put_line(g_stmt);

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
/* 
  SELECT COUNT(*) 
    INTO G_INDEX_CNT
    from dba_indexes
   WHERE TABLE_OWNER = 'DWH_FOUNDATION'
     AND TABLE_NAME = G_TABLE_NAME
     AND status NOT IN ('N/A', 'USABLE','VALID');
     
  IF G_INDEX_CNT > 0 THEN
     SELECT INDEX_NAME 
       INTO G_INDEX_NAME
       FROM SYS.DBA_INDEXES 
      WHERE TABLE_OWNER = 'DWH_FOUNDATION'
        AND TABLE_NAME = G_TABLE_NAME
        AND status NOT IN ('N/A', 'USABLE','VALID');
        
     g_stmt := 'alter index DWH_FOUNDATION.'||g_index_name||' rebuild parallel 1';
     execute immediate g_stmt;
     
     l_text := 'INDEX ' || G_INDEX_NAME || ' was UNUSABLE and has been rebuilt';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  END IF;

 /*
     if g_table_name = 'DWH_FOUNDATION.FND_JDAFF_ST_PLAN_ANALYSIS_DY' then
       l_text := 'Rebuilding index';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
       
       g_stmt := 'alter index DWH_FOUNDATION.PK_JDAFF_ST_PLAN_ANALYSIS_DY rebuild parallel 1';
       execute immediate g_stmt;
       
       l_text := 'INDEX DWH_FOUNDATION.PK_JDAFF_ST_PLAN_ANALYSIS_DY was UNUSABLE and has been rebuilt';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
    
    if g_table_name = 'DWH_FOUNDATION.FND_JDAFF_ST_PLAN_ANALYSIS_WK' then
       l_text := 'Rebuilding index';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
       
       g_stmt := 'alter index DWH_FOUNDATION.PK_JDAFF_ST_PLAN_ANALYSIS_WK rebuild parallel 1';
       execute immediate g_stmt;
       
       l_text := 'INDEX DWH_FOUNDATION.PK_JDAFF_ST_PLAN_ANALYSIS_WK was UNUSABLE and has been rebuilt';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
 
 */
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,'','','','','');
    l_text := 'TRUNCATE DAILY PARTITIONS ON '||trim(g_table_name)||' ENDED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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

END WH_FND_GENERIC_TRUNC_FPARTN_DY;
