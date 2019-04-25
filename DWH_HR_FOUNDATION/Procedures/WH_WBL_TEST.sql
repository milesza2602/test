--------------------------------------------------------
--  DDL for Procedure WH_WBL_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_WBL_TEST" (p_forall_limit in integer,p_success out boolean) AS
g_recs_read         integer       :=  0;
g_recs_updated      integer       :=  0;
g_recs_inserted     integer       :=  0;
g_stmt              varchar2(1500);
g_table_name        varchar2(31);
g_arc_table_name    varchar2(31);
g_hsp_table_name    varchar2(31);
g_cpy_table_name    varchar2(31);
g_index_name        varchar2(31);
g_cpy_index_name    varchar2(31);
g_pk_name           varchar2(31);
g_cpy_pk_name       varchar2(31);
g_pk_stmt           varchar2(1500);
g_tablespace        varchar2(31);
--
l_message           sys_dwh_errlog.log_text%type;
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_stage;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_rename;
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_text              sys_dwh_log.log_text%type ;

BEGIN
p_success := false;
-- awx_job_control.complete_job_status(stg_excel_ed_benefit_cntr);
--  EXECUTE IMMEDIATE('create table stg_excel_ed_benefit_cntr tablespace STG_STAGING as select * from stg_excel_ed_benefit_cntr_cpy where 1=2 ');
--  COMMIT;
    g_stmt := 'GRANT SELECT,UPDATE,INSERT ON stg_excel_ed_benefit_cntr TO DWH_AIT';
--

        execute immediate g_stmt;
        commit;
        p_success := true;
END WH_WBL_TEST;
