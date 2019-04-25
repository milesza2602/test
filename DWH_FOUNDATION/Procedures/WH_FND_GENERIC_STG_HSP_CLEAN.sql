--------------------------------------------------------
--  DDL for Procedure WH_FND_GENERIC_STG_HSP_CLEAN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_GENERIC_STG_HSP_CLEAN" 
                                                                (p_success out boolean) as
--**************************************************************************************************
--  Date:        OCT 2018
--  Author:      Alastair de Wet
--  Purpose:     CLEAN UP HSP TABLES STAGING - Was initially run on 19 Oct 2018 and works with no issues.
--               A limit in the code is set to not worry about tables < 100k records. This can be changed!!
--  Tables:      Input  - _HSP
--               Output - _HSP
--  Packages:    constants, dwh_log,
--
--  Maintenance:
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
g_recs_hospital     integer       :=  0;

g_count             integer       :=  0;
g_stmt              varchar(200);
g_date              date          := trunc(sysdate);
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type  := 'WH_FND_GENERIC_STG_HSP_CLEAN';
l_name              sys_dwh_log.log_name%type               := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type        := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type        := dwh_constants.vc_log_script_rtl_prf_md ;
l_procedure_name    sys_dwh_log.log_procedure_name%type     := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'STAGING HISTORY CLEAN';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_hsp is
SELECT TAB.OWNER,TAB.TABLE_NAME,TAB.NUM_ROWS
FROM   ALL_TABLES TAB,
       ALL_TAB_COLS COL
WHERE  TAB.NUM_ROWS > 100000
AND    TAB.OWNER    LIKE 'DWH%'
AND    TAB.TABLE_NAME LIKE '%_HSP' 
AND    TAB.TABLE_NAME = COL.TABLE_NAME 
AND    COL.COLUMN_NAME = 'SYS_LOAD_DATE'
AND    COL.DATA_TYPE = 'DATE'
ORDER BY TAB.NUM_ROWS  DESC;

g_rec_in            c_stg_hsp%rowtype;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

    g_stmt := 'create table DWH_FOUNDATION.STG_HSP_BU as '||
              'select * from '||g_rec_in.owner||'.'||g_rec_in.table_name||
              ' where 1=2';

    DBMS_OUTPUT.PUT_LINE(g_stmt);
    execute immediate g_stmt;
 
    l_text := g_stmt||' BACKUP TABLE CREATE COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--------------------------------------------------------------------------------------------    
    g_stmt := 'insert into DWH_FOUNDATION.STG_HSP_BU '||
              ' select * from '||g_rec_in.owner||'.'||g_rec_in.table_name|| 
              ' where sys_load_date > SYSDATE - 90 ';

    DBMS_OUTPUT.PUT_LINE(g_stmt);
    execute immediate g_stmt;
    
    l_text :=  sql%rowcount||' COPY DATA FOR PAST 3 MONTHS '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--------------------------------------------------------------------------------------------    
    
    g_stmt := 'truncate table '||g_rec_in.owner||'.'||g_rec_in.table_name ;
 
    DBMS_OUTPUT.PUT_LINE(g_stmt);
    execute immediate g_stmt;
    
    l_text := g_stmt||' TRUNCATE HOSPITAL STAGING TABLE '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--------------------------------------------------------------------------------------------
    
    g_stmt := 'insert into '||g_rec_in.owner||'.'||g_rec_in.table_name|| 
              ' select * from  DWH_FOUNDATION.STG_HSP_BU ' ;

    DBMS_OUTPUT.PUT_LINE(g_stmt);
    execute immediate g_stmt;
    
    l_text :=  sql%rowcount||' PUT DATA BACK IN HOSPITAL '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--------------------------------------------------------------------------------------------
    g_stmt := 'drop table  DWH_FOUNDATION.STG_HSP_BU' ;

    DBMS_OUTPUT.PUT_LINE(g_stmt);
    execute immediate g_stmt;

    l_text := 'DROP COMPLETED DWH_FOUNDATION.STG_HSP_BU'||' - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
commit;

--  CREATE TABLE   "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSB"  AS
--  (SELECT * FROM "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSP"
--  WHERE 1 = 2)  ;
  
--  INSERT /*+ APPEND parallel(8) */  INTO "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSB" 
--  SELECT /*+ parallel(8) */ * FROM "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSP"
--  where sys_load_date > SYSDATE - 90  ;
  
--  Truncate table "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSP";
  
--  INSERT /*+ APPEND parallel(8) */  INTO "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSP" 
--  SELECT /*+ parallel(8) */ * FROM "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSB"  ;
 
-- SELECT COUNT(*) 
-- INTO G_COUNT
-- FROM "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSP" ; 
  
-- DROP TABLE  "DWH_FOUNDATION"."STG_CAM_LOCATION_ITEM_HSB";

      g_recs_updated              := g_recs_updated + 1;



  exception


      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_in.table_name;
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

    l_text := 'LOAD OF ITEM MASTER SK2 VERSION STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
    for v_stg_hsp in c_stg_hsp
    loop
      g_recs_read := g_recs_read + 1;
      g_rec_in := v_stg_hsp;
 
      l_text := g_rec_in.table_name||'  '||
      to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      local_write_output;

    end loop;

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_in.table_name;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end WH_FND_GENERIC_STG_HSP_CLEAN;
