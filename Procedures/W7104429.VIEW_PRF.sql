-- ****** Object: Procedure W7104429.VIEW_PRF Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "VIEW_PRF"  (l_owner in varchar2, l_view_name in varchar2, l_OLAP_TP in varchar2, l_bgn in date, l_gn in varchar2, l_clause in varchar2) authid  current_user    --definer as  - as

    l_name        sys_dwh_log.log_name%type;
    l_text        sys_dwh_log.log_text%type;

    l_start       timestamp;
    l_end         timestamp;

    RUN_HR        NUMBER;
    RUN_MIN       NUMBER;
    RUN_SEC       NUMBER;
    l_count       NUMBER;
    l_sql 	      varchar2(3072);
    l_VWNM        varchar2(68);

    g_recs_inserted      integer       :=  0;
    g_recs_updated       integer       :=  0;

    too_many_attempts EXCEPTION;
    PRAGMA EXCEPTION_INIT(too_many_attempts, -20300);

BEGIN
--   execute immediate 'alter session enable parallel dml';

-- Comment - BK ...

   BEGIN
        l_start := localtimestamp;

        l_count:= 0;
        W7104429.view_prf2(l_clause, l_count);
        dbms_output.put_line ('Rows processed l ' || l_count);

--        if l_OLAP_TP = 'SSAS' then
--          l_name := 'OLAP - CUBE EVAL';
- --          if l_owner <> 'JOIN QUERY' then
--             l_sql :=
--                'select count(*) from (select /*+ materialize */ * from ' || l_owner || '.' || l_view_name || ' ' || l_clause || ')';
--          else
--             l_sql :=
--                'select count(*) from (' ||l_clause || ')';
--          end if;
--        end if;
- --        if l_OLAP_TP = 'QV' then
--           l_name := 'OLAP - QV EVAL';
- --           l_sql :=
--                'select count(*) from (' ||l_clause || ')';
--        end if;
- --        execute immediate l_sql into l_count;
        l_end  := localtimestamp;

        RUN_SEC := SUBSTR((l_end-l_start), INSTR((l_end-l_start),' ')+7,2);
        RUN_MIN := SUBSTR((l_end-l_start), INSTR((l_end-l_start),' ')+4,2);
        RUN_HR  := SUBSTR((l_end-l_start), INSTR((l_end-l_start),' ')+1,2);

        insert into w7104429.OLAP_PRF_LOG values
          (l_bgn, l_name, l_owner, l_view_name, l_gn, l_count, l_start, l_end, RUN_HR, RUN_MIN, RUN_SEC);
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

END  "VIEW_PRF";
/