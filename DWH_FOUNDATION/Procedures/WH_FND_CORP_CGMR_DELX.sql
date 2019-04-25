--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_CGMR_DELX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_CGMR_DELX" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2017
--  Author:      Mariska Matthee
--  Purpose:     This procedure will backup and delete data from the FND tables for the CGM 
--               Re-class 8 May 2017
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_CGMR_DELX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'DELETE FROM FND TABLES';
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
-- Backup and delete from Foundation tables
--**************************************************************************************************

    l_text := 'BACKUPS AND TRUNCATION/DELETIONS ON FOUNDATION TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
  
    --------------------------------------------------------------------------------
    -- FND_RTL_LOC_SUBC_WK_MP
    
    l_text := 'BACKUP OF FND_RTL_LOC_SUBC_WK_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'create table DWH_FOUNDATION.FND_RTL_LC_SC_W_MP_BCK20170508
                       tablespace "FND_MASTER" as
                       select /*+ parallel(tmp,4) full(tmp) */ *
                         from dwh_foundation.FND_RTL_LOC_SUBC_WK_MP tmp';
               --         where department_no in (select department_no
               --                                   from DWH_PERFORMANCE.TEMP_CGM_RECLASS_DEPT_DEL
               --                                  where july_ind = 0)
               --            or subclass_no in (select subclass_no
               --                                 from DWH_PERFORMANCE.TEMP_CGM_RECLASS_SCLASS_DEL
               --                                where july_ind = 0)';
    
    l_text := 'DELETION FROM FND_RTL_LOC_SUBC_WK_MP STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    delete /*+ parallel(tmp,4) full(tmp) */
      from dwh_foundation.FND_RTL_LOC_SUBC_WK_MP tmp
     where department_no in (select department_no
                               from DWH_PERFORMANCE.TEMP_CGM_RECLASS_DEPT_DEL
                              where july_ind = 0);
    commit;
    
    delete /*+ parallel(tmp,4) full(tmp) */
      from dwh_foundation.FND_RTL_LOC_SUBC_WK_MP tmp
     where subclass_no in (select subclass_no
                               from DWH_PERFORMANCE.TEMP_CGM_RECLASS_SCLASS_DEL
                              where july_ind = 0);
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
end WH_FND_CORP_CGMR_DELX;
