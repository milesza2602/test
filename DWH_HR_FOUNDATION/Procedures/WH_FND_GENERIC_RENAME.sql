--------------------------------------------------------
--  DDL for Procedure WH_FND_GENERIC_RENAME
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_FND_GENERIC_RENAME" ( p_table_name VARCHAR2,
                                   p_log_script_name sys_dwh_log.log_script_name%type,
                                   p_log_procedure_name sys_dwh_log.log_procedure_name%type,
                                   p_description       sys_dwh_log_summary.log_description%type) AS
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
l_procedure_name    sys_dwh_log.log_procedure_name%type       := upper(p_log_procedure_name);
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type
                    := upper(p_description);
--
cursor c_index_names is
select index_name
from user_indexes
where table_name = upper(g_cpy_table_name);
--
--**************************************************************************************************
begin
    /* Derive Archive, Hospital and Copy table names */
    g_table_name := upper(p_table_name);
    g_arc_table_name := upper(p_table_name||'_ARC');
    g_hsp_table_name := upper(p_table_name||'_HSP');
    g_cpy_table_name := upper(p_table_name||'_CPY');

    /* Start Logging */
    l_text :=
    '===================================================================================';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_OUTPUT.PUT_LINE('1=');
    l_text := 'RENAME '|| p_table_name||' STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_OUTPUT.PUT_LINE('2=');
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
      DBMS_OUTPUT.PUT_LINE('3=');
--
--**************************************************************************************************
--
    /* Get tablespace name from current table */
    select tablespace_name
    into g_tablespace
    from user_tables
    where table_name = upper(p_table_name);
      DBMS_OUTPUT.PUT_LINE('4=');
    /* Get creation statement of PK of input table */
    BEGIN
       select constraint_name
       into   g_pk_name
       from   user_constraints
       where  table_name = upper(p_table_name)
              and constraint_type = 'P';
                    DBMS_OUTPUT.PUT_LINE('5=');
    EXCEPTION
    when NO_DATA_FOUND then
          DBMS_OUTPUT.PUT_LINE('6=');
          l_text := p_table_name||' DOES NOT CONTAIN ANY CONSTRAINTS!'||
          to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_message := 'CONSTRAINT MODULE - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_procedure_name,sqlcode,l_message);
          RAISE;
    End;
          DBMS_OUTPUT.PUT_LINE('7=');
    g_pk_stmt := dbms_metadata.get_ddl( 'CONSTRAINT', g_pk_name);
    g_pk_stmt := substr(g_pk_stmt,1,instr(g_pk_stmt, 'USING', 1)-1);
    g_pk_stmt := g_pk_stmt ||' USING INDEX TABLESPACE '||g_tablespace||' enable';
         DBMS_OUTPUT.PUT_LINE('8=');
    /* Copy *Fixed* records from Hospital to input table */
    g_stmt := 'insert into '||p_table_name||' select * from '|| g_hsp_table_name||
              ' where sys_process_code = ''N''';
          DBMS_OUTPUT.PUT_LINE('9=');
    execute immediate g_stmt;
        DBMS_OUTPUT.PUT_LINE('10=');
    l_text := g_stmt||' INSERT EX _HSP COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        DBMS_OUTPUT.PUT_LINE('11=');
      /* Copy *Fixed* records from Hospital to input table */
    g_stmt := 'delete from '|| g_hsp_table_name||
              ' where sys_process_code = ''N''';
          DBMS_OUTPUT.PUT_LINE('12=');
    execute immediate g_stmt;
        DBMS_OUTPUT.PUT_LINE('13=');
    l_text := g_stmt||' DELETE FROM _HSP COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          DBMS_OUTPUT.PUT_LINE('14=');
    /* Rename input table to Copy equivalent */
    g_stmt :=  'alter table '||p_table_name||' rename to '|| g_cpy_table_name;
      DBMS_OUTPUT.PUT_LINE('15=');
    execute immediate g_stmt;
--
    l_text := g_stmt||' RENAME COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    /* Rename indexes on table to Copy equivalent */
  DBMS_OUTPUT.PUT_LINE('16=');
    BEGIN

    for v_index_name in c_index_names
    loop
       g_index_name := v_index_name.index_name;
       g_cpy_index_name := substr(g_index_name, 1, 26)||'_CPY';
       g_stmt := 'alter index '||g_index_name ||' rename to '||g_cpy_index_name;
--
       execute immediate g_stmt;
       l_text := g_stmt||' INDEX RENAME COMPLETED '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end loop;
    Exception
       when NO_DATA_FOUND then
          l_text := p_table_name||' DOES NOT CONTAIN ANY INDEXES!'||
          to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_message := 'RENAME INDEX MODULE - OTHER ERROR '||sqlcode||' '||sqlerrm;
          dwh_log.record_error(l_procedure_name,sqlcode,l_message);
          raise;
    End;
  DBMS_OUTPUT.PUT_LINE('17=');
    /* Rename the Primary key to Copy equivalent */
    select constraint_name
    into   g_pk_name
    from   user_constraints
    where  table_name = upper(g_cpy_table_name)
           and constraint_type = 'P';
--
    g_cpy_pk_name := substr(g_pk_name, 1, 26)||'_CPY';
--
    g_stmt := 'alter table '||g_cpy_table_name||' rename constraint '||
              g_pk_name||' to '||g_cpy_pk_name;
    DBMS_OUTPUT.PUT_LINE('18=');
    execute immediate g_stmt;
    l_text := g_stmt||' COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- 
    /* Update awx_job_control trigger - status = Y for table */
    DBMS_OUTPUT.PUT_LINE('18B=');
    awx_job_control.complete_job_status(g_table_name);
    l_text := 'Set AWX_JOB_STATUS = Y on '||  p_table_name;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
    /* Recreate the input table from the Copy table */
    g_stmt := 'create table '|| p_table_name ||' tablespace '|| g_tablespace||' as '||
             'select * from '|| g_cpy_table_name ||
             ' where 1=2';

    DBMS_OUTPUT.PUT_LINE('19=');
    execute immediate g_stmt;
    l_text := g_stmt||' TABLE RE-CREATE COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    /* Create Primary Key on new table */
      DBMS_OUTPUT.PUT_LINE('20=');
    execute immediate g_pk_stmt;

    l_text := g_stmt||' PRIMARY KEY CREATE COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
    /* Create bitmap index on SYS_PROCESS_CODE on new table */
    g_stmt := 'CREATE INDEX BS_'|| substr(p_table_name, 5, 22)||
              ' on '|| p_table_name || ' (SYS_PROCESS_CODE)';
--
    l_text := g_stmt||' BITMAP KEY CREATE COMPLETED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_OUTPUT.PUT_LINE('21=');
    execute immediate g_stmt;

      DBMS_OUTPUT.PUT_LINE('22=');
        /* Grant privileges to new table */
    g_stmt := 'GRANT SELECT,UPDATE,INSERT ON '|| p_table_name||
              ' TO DWH_AIT';
--
    l_text := g_stmt||' PERMISSIONS GRANTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate g_stmt;
  DBMS_OUTPUT.PUT_LINE('23=');
--
    l_text := 'GATHER TABLE STATS on '||  g_cpy_table_name;
--    dbms_stats.gather_table_stats('DWH_HR_FOUNDATION',g_cpy_table_name);
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_OUTPUT.PUT_LINE('24=');
--**************************************************************************************************
--
    /* Complete Logging */
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
      DBMS_OUTPUT.PUT_LINE('25=');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_OUTPUT.PUT_LINE('26=');
    l_text :=  dwh_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_OUTPUT.PUT_LINE('27=');
EXCEPTION
    when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
--
        RAISE;

END WH_FND_GENERIC_RENAME;
