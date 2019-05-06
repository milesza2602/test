--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_CGMR_DEL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_CGMR_DEL" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jul 2017
--  Author:      Mariska Matthee
--  Purpose:     This procedure will backup and delete data from the PRF tables for the CGM 
--               Re-class 10 Jul 2017
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_CGMR_DEL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'DELETE FROM PRF TABLES';
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

--**************************************************************************************************
-- Backup and delete from Performance tables
--**************************************************************************************************

    l_text := 'BACKUP AND TRUNCATION/DELETIONS ON PERFORMANCE TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --------------------------------------------------------------------------------
    -- RTL_AREA_DEPT_WK_MP_PLAN
    
    l_text := 'BACKUP OF RTL_AREA_DEPT_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_A_DPT_W_MP_PLN_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from RTL_AREA_DEPT_WK_MP_PLAN tmp';
    
    l_text := 'TRUNCATE RTL_AREA_DEPT_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_AREA_DEPT_WK_MP_PLAN';
    
    --------------------------------------------------------------------------------
    -- RTL_AREA_SUBCLASS_WK_MP_PLAN
    
    l_text := 'BACKUP OF RTL_AREA_SUBCLASS_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_A_SCS_W_MP_PLN_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from RTL_AREA_SUBCLASS_WK_MP_PLAN tmp';
    
    l_text := 'TRUNCATE RTL_AREA_SUBCLASS_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_AREA_SUBCLASS_WK_MP_PLAN';
    
    --------------------------------------------------------------------------------
    -- RTL_CHAIN_DEPT_WK_MP_PLAN
    
    l_text := 'BACKUP OF RTL_CHAIN_DEPT_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_C_DPT_W_MP_PLN_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from RTL_CHAIN_DEPT_WK_MP_PLAN tmp';
    
    l_text := 'TRUNCATE RTL_CHAIN_DEPT_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_CHAIN_DEPT_WK_MP_PLAN';
    
    --------------------------------------------------------------------------------
    -- RTL_CHAIN_SUBCLASS_WK_MP_PLAN
    
    l_text := 'BACKUP OF RTL_CHAIN_SUBCLASS_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_C_SCS_W_MP_PLN_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from RTL_CHAIN_SUBCLASS_WK_MP_PLAN tmp';
    
    l_text := 'TRUNCATE RTL_CHAIN_SUBCLASS_WK_MP_PLAN STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_CHAIN_SUBCLASS_WK_MP_PLAN';
    
    --------------------------------------------------------------------------------
    -- RTL_LOC_DEPT_MTH_PLAN_MP
    
    l_text := 'BACKUP OF RTL_LOC_DEPT_MTH_PLAN_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_L_DPT_M_PLN_MP_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from dwh_performance.RTL_LOC_DEPT_MTH_PLAN_MP tmp';
    
    l_text := 'TRUNCATE RTL_LOC_DEPT_MTH_PLAN_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_LOC_DEPT_MTH_PLAN_MP';
    
    --------------------------------------------------------------------------------
    -- RTL_LOC_SUBC_MTH_PLAN_MP
    
    l_text := 'BACKUP OF RTL_LOC_SUBC_MTH_PLAN_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_L_SCS_M_PLN_MP_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from dwh_performance.RTL_LOC_SUBC_MTH_PLAN_MP tmp';
    
    l_text := 'TRUNCATE RTL_LOC_SUBC_MTH_PLAN_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_LOC_SUBC_MTH_PLAN_MP';
    
    --------------------------------------------------------------------------------
    -- RTL_LOC_SUBC_WK_APS
    -- not to delete from this table, re rollup data from Y2014 W1 to Y2017 Wlatest
    
    l_text := 'BACKUP OF RTL_LOC_SUBC_WK_APS STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_LOC_SUBC_W_APS_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from dwh_performance.RTL_LOC_SUBC_WK_APS tmp';
    
    l_text := 'TRUNCATE RTL_LOC_SUBC_WK_APS STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.RTL_LOC_SUBC_WK_APS';
    
    --------------------------------------------------------------------------------
    -- MART_RTL_LOC_SUBC_PLN_WK_RMS
    -- not to delete from this table, re rollup data from Y2014 W1 to Y2017

    l_text := 'BACKUP OF MART_RTL_LOC_SUBC_PLN_WK_RMS STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.MRTL_LSC_PL_W_RMS_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from dwh_performance.MART_RTL_LOC_SUBC_PLN_WK_RMS tmp';
    
    l_text := 'TRUNCATE MART_RTL_LOC_SUBC_PLN_WK_RMS STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'truncate table dwh_performance.MART_RTL_LOC_SUBC_PLN_WK_RMS';
    
    --------------------------------------------------------------------------------
    -- RTL_LOC_SUBC_WK_MP
    
    l_text := 'BACKUP OF RTL_LOC_SUBC_WK_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_PERFORMANCE.RTL_LOC_SUBC_WK_MP_BCK20170710
                       tablespace "PRF_MASTER" as
                       select /*+ parallel(tmp,6) full(tmp) */ *
                         from dwh_performance.RTL_LOC_SUBC_WK_MP tmp
                        where sk1_subclass_no in (select sk1_subclass_nol
                                                    from DWH_PERFORMANCE.TEMP_CGM_RECLASS_SCLASS_DEL
                                                   where july_ind = 1)';
    
    l_text := 'DELETION FROM RTL_LOC_SUBC_WK_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    delete /*+ parallel(tmp,6) full(tmp) */
      from dwh_performance.RTL_LOC_SUBC_WK_MP tmp
     where sk1_subclass_no in (select sk1_subclass_no
                                 from DWH_PERFORMANCE.TEMP_CGM_RECLASS_SCLASS_DEL
                                where july_ind = 1);
    commit;
    
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'TRUNCATION/DELETIONS COMPLETED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       commit;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       commit;
       p_success := false;
       raise;
end WH_PRF_CORP_CGMR_DEL;
