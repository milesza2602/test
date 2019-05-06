--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_CGMR_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_CGMR_BCK" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jul 2017
--  Author:      Mariska Matthee
--  Purpose:     This procedure will backup all the FND and PRF dimension tables for the CGM Re-class 
--               10 Jul 2017
--
--  Tables:      Input  -  
--               Output - 
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

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;
g_start_date         date;
g_end_date           date;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_CGMR_BCK';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'BACKUP FND AND PRF DIMENSION TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'RUN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Backup Foundation tables
--**************************************************************************************************
    
    l_text := 'BACKUP FOUNDATION DIMENSION TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_FOUNDATION.FND_AST_LEV1_DIFF1_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_AST_LEV1_DIFF1';
  
    execute immediate 'create table DWH_FOUNDATION.FND_CLASS_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_CLASS';
  
    execute immediate 'create table DWH_FOUNDATION.FND_DEPARTMENT_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_DEPARTMENT';
  
    execute immediate 'create table DWH_FOUNDATION.FND_GROUP_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_GROUP';

    execute immediate 'create table DWH_FOUNDATION.FND_ITEM_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_ITEM';
  
    execute immediate 'create table DWH_FOUNDATION.FND_SUBCLASS_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_SUBCLASS';
  
    execute immediate 'create table DWH_FOUNDATION.FND_SUBGROUP_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_SUBGROUP';
                         
    execute immediate 'create table DWH_FOUNDATION.FND_LOCATION_BCK20170710
                       tablespace "FND_MASTER" as
                       select *
                         from DWH_FOUNDATION.FND_LOCATION';
                                               
--**************************************************************************************************
-- Backup Performance tables
--**************************************************************************************************

    l_text := 'BACKUP PERFORMANCE DIMENSION TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'create table DWH_PERFORMANCE.DM_AST_LEV1_DIFF1_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_AST_LEV1_DIFF1';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_CLASS_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_CLASS';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_DEPARTMENT_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_DEPARTMENT';

    execute immediate 'create table DWH_PERFORMANCE.DM_GROUP_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_GROUP';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_ITEM_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_ITEM';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_LEV1_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_LEV1';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_LEV1_DIFF1_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_LEV1_DIFF1';
  
    execute immediate 'create table DWH_PERFORMANCE.DM_SUBCLASS_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_SUBCLASS';
                        
    execute immediate 'create table DWH_PERFORMANCE.DM_SUBGROUP_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_SUBGROUP';
                         
    execute immediate 'create table DWH_PERFORMANCE.DM_LOCATION_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.DIM_LOCATION';
                        
    execute immediate 'create table DWH_PERFORMANCE.RTL_ITEM_SUPPLIER_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_PERFORMANCE.RTL_ITEM_SUPPLIER';
                         
    execute immediate 'create table DWH_META_DATA.META_TABLE_BCK20170710
                       tablespace "PRF_MASTER" as
                       select *
                         from DWH_META_DATA.META_TABLE';

--**************************************************************************************************
-- Set Meta Table
--**************************************************************************************************
    
    l_text := 'META TABLE DATA UPDATES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -- For MP staging tables
    update meta_table
       set skip_alert_ind = 'Y',
           release_time = to_date(to_char(trunc(sysdate), 'yyyy/mm/dd') || ' 23:30:00','yyyy/mm/dd hh24:mi:ss')
     where table_name like 'STG_MP_%';
         
    /*
      -- reset code
      update meta_table mt
         set mt.skip_alert_ind = (select mtb.skip_alert_ind
                                    from DWH_META_DATA.META_TABLE_BCK20170710 mtb
                                   where mt.table_name = mtb.table_name
                                     and mtb.table_name like 'STG_MP_%'),
             mt.release_time = (select mtb.release_time
                                  from DWH_META_DATA.META_TABLE_BCK20170710 mtb
                                 where mt.table_name = mtb.table_name
                                   and mtb.table_name like 'STG_MP_%')
       where exists (select *
                       from DWH_META_DATA.META_TABLE_BCK20170710 mtb
                      where mt.table_name = mtb.table_name
                        and mtb.table_name like 'STG_MP_%');
      commit;
    */

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'RUN COMPLETED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end WH_PRF_CORP_CGMR_BCK;
