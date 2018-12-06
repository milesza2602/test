-- ****** Object: Procedure W7104429.TBL_COMP Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "TBL_COMP"  (l_owner in varchar2, l_table_name in varchar2, L_CLIST in varchar2) authid  current_user    --definer as  --
as

   l_sql                VARCHAR2 (8192);
   l_start              DATE;
   l_runtm              DATE;
   l_crt_count          NUMBER;
   l_scn_count          NUMBER;
   l_seconds            NUMBER;
   l_rowcnt             NUMBER;
   l_name               sys_dwh_log.log_name%type                 := 'Table Compare';
   l_system_name        sys_dwh_log.log_system_name%type          := 'ORA12C TESTING';
   l_script_name        sys_dwh_log.log_script_name%type          := 'TBL_COMP';
   l_procedure_name     sys_dwh_log.log_procedure_name%type       := 'ALL';
   l_text               sys_dwh_log.log_text%type ;
   g_recs_updated       integer       :=  0;
   g_recs_dropped       integer       :=  0;

   c                    integer;
   l_tab                varchar2(36);
   l_frdte              DATE;
   g_date               date          := trunc(sysdate);

   l_dblink             varchar(64) := 'DWHUAT.DWHDBDEV.WOOLWORTHS.CO.ZA';

   too_many_attempts EXCEPTION;
   PRAGMA EXCEPTION_INIT(too_many_attempts, -20300);

BEGIN
--insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, '001');
-- This comment was added to see if merge statement picks up differences

--    execute immediate 'alter session set events = ''6512 trace name ERRORSTACK level 3''';

    l_tab := 'W7104429.'||l_table_name;
    SELECT today_date INTO l_frdte FROM DWH_PERFORMANCE.DIM_CONTROL;
--    l_frdte := '11 Sep 2018';

    -- Remove if already present ...
    select /* + parallel (x,4) */ count(*) into c from all_tab_cols x where owner = 'W7104429' and table_name = l_table_name; 
    if  c > 0 then   
        execute immediate 'drop table w7104429.' ||l_table_name;
    end if;

--insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, '002');

   -- Create rows in SIM but not in PROD (SIM minus PROD)...
   l_sql := 
        'create table W7104429.' ||l_table_name || 
        ' as select ' || '/*+ parallel (a,6) */ ' ||l_clist || ', ''SIM '' as SRC' ||
        ' from ' ||l_owner||'.'||l_table_name ||  
        ' a where last_updated_date = ''' || l_frdte ||'''' ||
        ' minus select ' || '/*+ parallel (a,6) */ ' ||l_clist || ', ''SIM ''' ||
        ' from ' ||l_owner||'.'||l_table_name || '@' ||L_DBLINK || 
        ' a where last_updated_date = ''' || l_frdte||'''';

--        l_sql :=
--            'insert into W7104429.' ||l_table_name || ' select ' ||l_clist || ' from ' ||l_owner||'.'||l_table_name || ' where rownum < 11';
   EXECUTE IMMEDIATE l_sql; commit;
   
--insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, '003');
--        DBMS_OUTPUT.put_line ('ln ' || '---> ' || l_sql);

   -- Create rows in PROD but not in SIM (PROD minus SIM)...
   l_sql := 
        'insert into W7104429.' ||l_table_name || 
        ' select ' || '/*+ parallel (a,6) */ ' ||l_clist || ', ''PROD''' ||
        ' from ' ||l_owner||'.'||l_table_name || '@' ||L_DBLINK || 
        ' a where last_updated_date = ''' || l_frdte ||'''' ||
        ' minus select ' || '/*+ parallel (a,6) */ ' ||l_clist || ', ''PROD''' ||
        ' from ' ||l_owner||'.'||l_table_name || 
        ' a where last_updated_date = ''' || l_frdte||'''' ;

--        l_sql :=
--            'insert into W7104429.' ||l_table_name || ' select ' ||l_clist || ' from ' ||l_owner||'.'||l_table_name || ' where rownum < 11';
    EXECUTE IMMEDIATE l_sql; commit;

--insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, '004');

    -- Only keep tables with discrepencies ...
    l_sql := 'select /* + parallel (x,4) */ count(*) from ' || l_tab; 
    EXECUTE IMMEDIATE l_sql into c; 

    if c = 0 then   
       execute immediate 'drop table w7104429.' ||l_table_name;
    end if;
    commit;

    l_procedure_name := l_table_name;
    l_text := case when c > 0 then 'Discrpencies - created' else 'No discrepencies - dropped' end;
    insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, 'CMP');
    commit;
    
    EXCEPTION
       WHEN too_many_attempts THEN 
        dbms_output.put_line('User Message');

       WHEN OTHERS                                    -- then logit and exit
       THEN                                                -- logit and exit
       l_text := sqlcode|| ' - '||SQLERRM;
       insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, 0, 'ERR'); commit;
       
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          ROLLBACK;
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          raise_application_error (-20300, 'INS failed');

end "TBL_COMP";
/