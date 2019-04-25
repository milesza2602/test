--------------------------------------------------------
--  DDL for Procedure WH_FND_RENAME_WENDY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_RENAME_WENDY" (p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
--  Date:        June 2008
--  Author:      Janakies Louca
--  Purpose:     Rename the staging table to equivalent cpy and then
--               create a blank version of the original for AIT to stage into.
--
--  Parameters:  p_table_name
--               p_log_script_name
--               p_log_procedure_name
--               p_description
--
--  Tables:      Input  - Table to rename and recreate
--               Output - Renamed and recreated tables
--  Packages:    dwh_log,
--
--  Maintenance: 2/6/09 - Lance Hamel - Added awx_job_control.complete_job_status call at step 18.
--   
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
p_log_script_name sys_dwh_log.log_script_name%type;
p_log_procedure_name sys_dwh_log.log_procedure_name%type;
p_description       sys_dwh_log_summary.log_description%type;
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
p_table_name VARCHAR2(21);
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_RENAME_WENDY';
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_stage;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_rename;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := 'WH_FND_CORP_112U';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type
                    := upper(p_description);
--
/*cursor c_index_names is
select index_name
from user_indexes
where table_name = upper(g_cpy_table_name);
*/
--
--**************************************************************************************************
begin
      DBMS_OUTPUT.PUT_LINE('1=');
	 execute immediate('alter table DWH_FOUNDATION.STG_RMS_PROM_LOCATION 
   ADD CONSTRAINT PK_S_STG_RMS_PRM_LC PRIMARY KEY (SYS_SOURCE_BATCH_ID, SYS_SOURCE_SEQUENCE_NO)
  USING INDEX 
  TABLESPACE STG_STAGING  ENABLE
   ');
      DBMS_OUTPUT.PUT_LINE('2=');
  execute immediate('CREATE INDEX DWH_FOUNDATION.BS_RMS_PROM_LOCATION ON DWH_FOUNDATION.STG_RMS_PROM_LOCATION (SYS_PROCESS_CODE) 
    TABLESPACE FND_MASTER ');
    commit;
      DBMS_OUTPUT.PUT_LINE('3='); 


EXCEPTION
    when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
--
        RAISE;



END WH_FND_RENAME_WENDY;
