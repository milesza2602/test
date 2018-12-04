-- ****** Object: Procedure W7104429.INS_STG_DATA Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "INS_STG_DATA"  (l_owner in varchar2, l_table_name in varchar2, l_pfx in varchar2, l_pfx2 in varchar2) authid  current_user    --definer as  --
as

   l_sql_i              VARCHAR2 (512);
   l_sql_t              VARCHAR2 (512);
   l_sql_c              VARCHAR2 (512);
   l_gn                 VARCHAR2 (512);
   l_start              DATE;
   l_end                DATE;
   l_ins_count          NUMBER;
   l_upd_count          NUMBER;
   l_seconds            NUMBER;
   l_count              NUMBER;
   g_recs_inserted1     integer       :=  0;
   g_recs_inserted2     integer       :=  0;
   g_recs_updated       integer       :=  0;
   g_recs_dropped       integer       :=  0;

   l_message            sys_dwh_errlog.log_text%type;
   l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'LOAD_CPY_TABLES';
   l_name               sys_dwh_log.log_name%type                 := 'Revitilization';
   l_system_name        sys_dwh_log.log_system_name%type          := 'FLASHDISK TESTING';
   l_script_name        sys_dwh_log.log_script_name%type          := 'Bulk STG loads';
   l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
   l_text               sys_dwh_log.log_text%type ;
   l_description        sys_dwh_log_summary.log_description%type  := 'Copy data for STG table loads';
   l_process_type       sys_dwh_log_summary.log_process_type%type := 'NORMAL';
   c                    integer;

   too_many_attempts EXCEPTION;
   PRAGMA EXCEPTION_INIT(too_many_attempts, -20300);

BEGIN
   l_name := l_table_name;

   -- DB check
   SELECT GLOBAL_NAME INTO l_gn FROM global_name;
   l_count := 0;

   -- for all tables to be done, trunc and insert
   DBMS_OUTPUT.put_line ('=========================================================');
   DBMS_APPLICATION_INFO.SET_MODULE ('CNT ' || l_count, l_table_name);

   BEGIN
    --   The following for creation of tables in local schema ...
      select count(*) into c from sys.dba_tab_cols where owner = l_owner and table_name = l_table_name||'_CPY';

      if c > 0 then
         l_sql_i :=
             'create table W7104429.'
             || l_pfx
             || l_table_name
             || ' nologging as select /* + append parallel (t,4)  */ * from ' 
             || l_owner
             || '.'
             || l_table_name || '_CPY t';
         EXECUTE IMMEDIATE l_sql_i; 
         g_recs_inserted1 := SQL%ROWCOUNT;
         commit;

         l_sql_i := 'update W7104429.' || l_pfx || l_table_name || ' set SYS_PROCESS_CODE = ''N'' ';      
         EXECUTE IMMEDIATE l_sql_i; commit; 
      else       
         select count(*) into c from all_tab_cols where owner = l_owner and table_name = l_table_name;

         if c > 0 then
            l_sql_i :=
                 'create table W7104429.'
                 || l_pfx
                 || l_table_name
                 || ' nologging as select /* + append parallel (t,4)  */ * from ' 
                 || l_owner
                 || '.'
                 || l_table_name || ' t';
            EXECUTE IMMEDIATE l_sql_i; 
            g_recs_inserted2 := SQL%ROWCOUNT;
            commit;

            l_sql_i := 'update W7104429.'|| l_pfx || l_table_name || ' set SYS_PROCESS_CODE = ''N'' ';      
            EXECUTE IMMEDIATE l_sql_i; commit;
         end if; 
      end if; 

      if g_recs_inserted1 > 0 then 
         l_text :=  '>> ' || 'Records Inserted: '||g_recs_inserted1;
         insert into w7104429.sys_dwh_log values
        (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, g_recs_inserted1, 'INc');
      end if;
      commit;

      if g_recs_inserted2 > 0 then 
         l_text :=  '>> ' || 'Records Inserted: '||g_recs_inserted2;
         insert into w7104429.sys_dwh_log values
        (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, g_recs_inserted2, 'INs');
      end if; 
      commit;
      
      -- remove the oldest version of the table after 14 days ...
      select count(*) into c from sys.dba_tab_cols where owner = 'W7104429' and table_name = l_pfx2||l_table_name;
--      l_text := 'in if';
--      insert into w7104429.sys_dwh_log values (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, c, '002');
      
      if c > 0 then
         l_sql_i := 'drop table W7104429.'|| l_pfx2 || l_table_name;
         EXECUTE IMMEDIATE l_sql_i; commit;
      end if;
      commit;
--     DBMS_OUTPUT.put_line ('DEL   ' || SUBSTR (l_sql_i, 1, 180));

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
   END;
   commit;


--   DBMS_OUTPUT.put_line ('UPD  ' || SUBSTR (l_sql_i, 1, 180));

END  "INS_STG_DATA";
/