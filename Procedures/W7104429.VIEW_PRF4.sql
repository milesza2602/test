-- ****** Object: Procedure W7104429.VIEW_PRF4 Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "VIEW_PRF4"  (l_owner in varchar2, l_view_name in varchar2, l_OLAP_TP in varchar2, l_bgn in date, l_gn in varchar2, l_clause in varchar2, l_USING_CLS in varchar2) authid  current_user    --definer as  --
as

       lsql varchar2(3048);
       
    begin
        execute immediate 
--  lsql :=
        '
         DECLARE
            l_name        sys_dwh_log.log_name%type;
            l_text        sys_dwh_log.log_text%type;
        
            l_start       timestamp;
            l_end         timestamp;
        
            RUN_HR        NUMBER;
            RUN_MIN       NUMBER;
            RUN_SEC       NUMBER;
            l_count       NUMBER;
            cntr          number := 0; 
            l_sql 	      varchar2(3072);
            l_VWNM        varchar2(68);
        
            g_recs_inserted      integer       :=  0;
            g_recs_updated       integer       :=  0;
        
         BEGIN
            l_name := ''OLAP - SSRS EVAL'';
            l_start := localtimestamp;
           
            for r in (' || l_clause || ' using ' || l_USING_CLS || ' ) 
            loop 
                cntr := cntr + 1; 
            end loop;  
            
            l_count := cntr;
            dbms_output.put_line (''Rows processed'' || l_count);
            
            l_end  := localtimestamp;
    
            RUN_SEC := SUBSTR((l_end-l_start), INSTR((l_end-l_start),'' '')+7,2);
            RUN_MIN := SUBSTR((l_end-l_start), INSTR((l_end-l_start),'' '')+4,2);
            RUN_HR  := SUBSTR((l_end-l_start), INSTR((l_end-l_start),'' '')+1,2);
    
            insert into w7104429.OLAP_PRF_LOG values
              ('''|| l_bgn || ''', l_name, ''' || l_owner || ''', ''' || l_view_name || ''', ''' || l_gn || ''', l_count, l_start, l_end, RUN_HR, RUN_MIN, RUN_SEC);
            commit;
    END;
   ';
--   dbms_output.put_line ('Query' || lsql);
   commit;

END  "VIEW_PRF4";
/