-- ****** Object: Procedure W7104429.LOAD_CPY_TABLES_STU Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "LOAD_CPY_TABLES_STU"  (l_owner in varchar2, l_table_name in varchar2, l_pfx in varchar2) authid  current_user    --definer as  --
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
   g_recs_inserted      integer       :=  0;
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
   execute immediate 'alter session enable parallel dml';
   l_name := l_table_name;
--   l_system_name := '';
--   l_script_name := '';

   -- DB check
   SELECT GLOBAL_NAME INTO l_gn FROM global_name;

--   IF l_gn <> 'DWHPRD.DWHDBPRD.WOOLWORTHS.CO.ZA'
----   IF l_gn <> 'DWHUAT.DWHDBUAT.WOOLWORTHS.CO.ZA'
--   THEN
--      raise_application_error (-20100, 'WRONG DB failed, can only be run from PROD');
--   END IF;

   DBMS_OUTPUT.put_line ('Correct database');
   l_count := 0;

   -- for all tables to be done, trunc and insert
   DBMS_OUTPUT.put_line ('=========================================================');
   DBMS_APPLICATION_INFO.SET_MODULE ('CNT ' || l_count, l_table_name);

   BEGIN
      -- in SIM - check if the STG table exists and if so then truncate it and load from the local schema in PROD ...
      --        - if not present - create and load from the local schema ...
      select count(*) into c from all_tab_cols where owner = l_owner and table_name = l_table_name; 
      
      if  c > 0 then   
          l_sql_i := 'truncate table ' || l_owner || '.'|| l_table_name || ' ';         -- DWH_FOUNDATION
          EXECUTE IMMEDIATE l_sql_i;
          commit;
    
          l_sql_i :=
                   'insert /*+ append parallel (t,4)  */ into '
                || l_owner
                || '.'
                || l_table_name
                || ' t select /* + parallel (x,4)  */ * from W7104429.'
                || l_table_name
                || ' x';
    
    --       DBMS_OUTPUT.put_line ('INS   ' || SUBSTR (l_sql_i, 1, 180));
    
           EXECUTE IMMEDIATE l_sql_i;
           g_recs_inserted := SQL%ROWCOUNT;
    
           l_text :=  '>> ' || 'Records Inserted: '||g_recs_inserted;
           insert into w7104429.sys_dwh_log values
          (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, g_recs_inserted, 'INS');
      else
          l_sql_i :=
                   'create table '
                || l_owner
                || '.'
                || l_table_name
                || ' nologging as select /* + append parallel (x,4)  */ * from W7104429.' 
                || l_table_name
                || ' x';
    
    --       DBMS_OUTPUT.put_line ('INS   ' || SUBSTR (l_sql_i, 1, 180));
    
           EXECUTE IMMEDIATE l_sql_i;
           g_recs_inserted := SQL%ROWCOUNT;
    
           l_text :=  '>> ' || 'Records Created: '||g_recs_inserted;
           insert into w7104429.sys_dwh_log values
          (localtimestamp,l_name,l_system_name,l_script_name,l_procedure_name,l_text, g_recs_inserted, 'CRT');   
      end if;
      
      commit;

   EXCEPTION
       WHEN too_many_attempts THEN 
        dbms_output.put_line('User Message');

       WHEN OTHERS                                    -- then logit and exit
       THEN                                                -- logit and exit
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          ROLLBACK;
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          raise_application_error (-20300, 'INS failed');
   END;
   commit;


--   DBMS_OUTPUT.put_line ('UPD  ' || SUBSTR (l_sql_i, 1, 180));

END  "LOAD_CPY_TABLES_STU";
/