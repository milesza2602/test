-- ****** Object: Procedure W7104429.OLAP_EMULATE_IDX Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "OLAP_EMULATE_IDX" (AWX_P1 in varchar2, AWX_P2 in varchar2) authid  current_user    
as

   l_sql_i              VARCHAR2 (512);
   l_sql_t              VARCHAR2 (512);
   l_sql_c              VARCHAR2 (512);
   l_gn                 VARCHAR2 (512);
   l_start              date;
   l_start_time         date;
   l_end_time           date;
   l_ins_count          NUMBER;
   l_upd_count          NUMBER;
   l_seconds            NUMBER;
   l_count              NUMBER;
   g_recs_inserted      integer       :=  0;
   g_recs_updated       integer       :=  0;
   g_recs_dropped       integer       :=  0;

   l_message            sys_dwh_errlog.log_text%type;
   l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'OLAP_EMULATE_IDX';
   l_name               sys_dwh_log.log_name%type                 := 'Revitilization';
   l_system_name        sys_dwh_log.log_system_name%type          := 'OLAP Simulation';
   l_script_name        sys_dwh_log.log_script_name%type          := 'AWX Testing';
   l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
   l_text               sys_dwh_log.log_text%type ;
   l_description        sys_dwh_log_summary.log_description%type  := 'Log OLAP job + sleep (run) duration';
   l_process_type       sys_dwh_log_summary.log_process_type%type := 'NORMAL';
   c                    integer;
   l_sleep_time         number(4);
   l_JOB_NM             VARCHAR2(128);

   too_many_attempts EXCEPTION;
   PRAGMA EXCEPTION_INIT(too_many_attempts, -20300);

BEGIN
   execute immediate 'alter session enable parallel dml';
--   l_name := l_table_name;
   l_system_name := '';
   l_script_name := '';

   -- DB check
   SELECT GLOBAL_NAME INTO l_gn FROM global_name;

   IF l_gn <> 'DWHPRD.DWHDBPRD.WOOLWORTHS.CO.ZA'
--   IF l_gn <> 'DWHUAT.DWHDBUAT.WOOLWORTHS.CO.ZA'
   THEN
      raise_application_error (-20100, 'WRONG DB failed, can only be run from UAT');
   END IF;

   DBMS_OUTPUT.put_line ('Correct database');
   l_count := 0;

   -- for all tables to be done, trunc and insert
   DBMS_OUTPUT.put_line ('=========================================================');
   DBMS_APPLICATION_INFO.SET_MODULE ('CNT ' || l_count, l_name);

   l_start := sysdate; commit;

   BEGIN
    -- Cubes (all parms passed)...
    if AWX_P1 is not null and AWX_P2 is not null then     
       select   AVG_SECS 
       into     l_sleep_time 
       from     w7104429.AWX_UPGRD_OLAP_LOOKUP t
       where    t.OLAP_TYP = 'SSAS'
       and      t.FLOW_NM = upper(AWX_P1)
       and      t.JOB_NM  = 'IDX';

       l_JOB_NM := 'IDX';
    end if;

    l_start_time    := sysdate; commit;
    dbms_lock.sleep(4); 
--    dbms_lock.sleep(l_sleep_time);   
    l_end_time      := sysdate; commit;

    -- log it
    insert into w7104429.AWX_UPGRD_OLAP_LOG values (l_start, 'SSAS', AWX_P1, l_JOB_NM, 'Success', l_sleep_time, AWX_P2, null, null, l_start_time, l_end_time, 'OLAP_EMULATE_IDX');
    commit;

   EXCEPTION
        WHEN too_many_attempts THEN 
        dbms_output.put_line('User Message');

       WHEN OTHERS                                    -- then logit and exit
       THEN                                                -- logit and exit
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          ROLLBACK;
          DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
          raise_application_error (-20300, 'LOG failed');

          l_end_time := sysdate;
          insert into w7104429.AWX_UPGRD_OLAP_LOG values (l_start, 'SSAS', AWX_P1, l_JOB_NM, 'Failure', l_sleep_time, AWX_P2, null, null, l_start_time, l_end_time, 'LOG Failed');
   END;
   commit;

END  "OLAP_EMULATE_IDX";
/