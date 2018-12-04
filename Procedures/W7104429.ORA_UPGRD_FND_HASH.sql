-- ****** Object: Procedure W7104429.ORA_UPGRD_FND_HASH Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "ORA_UPGRD_FND_HASH" as
begin
DECLARE
   l_sql_u       VARCHAR2 (4000);
   l_gn          VARCHAR2 (512);
   
   l_start       DATE;
   l_end         DATE;
   l_frdte       date;
   l_rowcnt      int;
   l_crt_count   NUMBER := 0;
   l_scn_count   NUMBER := 0;
   l_seconds     NUMBER;
   l_count       int;
   l_runtm       date;
   
   -- list the FND tables for compare:
   CURSOR csr1
   IS
      with 
      PRTA as 
      (
        select  owner||JOBNM skey
        from    w7104429.ORA_BTCH_TBLS 
        where   REF_TBL in (select TABLE_NAME||'_CPY' from w7104429.ORA_AWX_BTCH_STG_TBLS) --where TABLE_NAME in ('STG_RMS_GROUP', 'STG_RMS_RTL_ALLOCATION'))
      ),
  
      PrtB as
      (
        select  owner,
                JOBNM,
                REF_TBL
        from    w7104429.ORA_BTCH_TBLS 
        where   owner||JOBNM in (select skey from PRTA)
      ),
    
      Constr as
      (
        SELECT /*+ parallel (uc,6) parallel (ucc1,6) */
               UC.OWNER,
               UC.TABLE_NAME,
               UC.CONSTRAINT_NAME,
    --                     replace(WMSYS.WM_CONCAT(ucc1.column_name), ',', '||')
               to_char( RTRIM(XMLAGG(XMLELEMENT(E,ucc1.column_name,'||').EXTRACT('//text()') ORDER BY ucc1.position).GetClobVal(),'||')) AS Conkey  -- V11.1 (PROD)
--               listagg(ucc1.column_name, '||' ) within group (order by position) Conkey                                                             -- V11.2 onwards (UAT)
    
        FROM  DBA_CONSTRAINTS uc,
              DBA_CONS_COLUMNS ucc1
        
        WHERE UC.CONSTRAINT_NAME  = UCC1.CONSTRAINT_NAME
        and   UC.OWNER            = UCC1.OWNER
        and   UC.TABLE_NAME       = UCC1.TABLE_NAME
        AND   UC.CONSTRAINT_TYPE  = 'P'
        
        and   UC.OWNER||UC.TABLE_NAME in (select owner||REF_TBL from Prtb where substr(REF_TBL,1,3) = 'FND')
        
        group by
              UC.OWNER,
              UC.TABLE_NAME,
              UC.CONSTRAINT_NAME
      )
  
    SELECT    /*+ parallel (a,4) */ 	
              a.owner, 
              a.table_name,
              b.Conkey,
              to_char(RTRIM(XMLAGG(XMLELEMENT(E,a.column_name,'||').EXTRACT('//text()') ORDER BY a.column_id).GetClobVal(),'||')) AS clist        -- V11.1 (PROD)
--              listagg(a.column_name, '||' ) within group (order by column_id) clist                                                               -- V11.2 onwards (UAT)
    FROM      all_tab_columns a
    join      Constr          b on (a.OWNER = b.OWNER and a.TABLE_NAME = b.TABLE_NAME)
    where     a.column_name not in ('LAST_UPDATED_DATE')
    group by  a.owner, 
              a.table_name, 
              b.ConKey 
    ORDER BY  2;  

BEGIN

  select current_date into l_runtm from dual;
--get the last batch period ...
  select today_date into l_frdte from dim_control;
--  l_frdte := '23/Jan/18';       

  FOR i IN csr1 LOOP
  BEGIN
          -- Add information to the v$session and v$session_longops - to allow tracking of execution times of long-running batch jobs ...
          DBMS_APPLICATION_INFO.SET_MODULE ('Counter ' || l_count, i.table_name);
  
          -- A1. Build the hash query ...
            l_sql_u :=
               'insert into ORA_UPGRD_HASH_TABLES 
                select  /*+ parallel (a,8) */ ' || ''''||i.owner||'''' || ',' || ''''||i.table_name||'''' || ', LAST_UPDATED_DATE, ' || i.Conkey || ',
                         ora_hash('|| i.clist || ')  HashVal 
                from ' || i.owner || '.' || i.TABLE_NAME || ' a where LAST_UPDATED_DATE = ' || ''''||l_frdte||'''' || '';        
                            
          DBMS_OUTPUT.put_line ('Create Hash total - Table: ' || i.TABLE_NAME); 
--          DBMS_OUTPUT.put_line ('Ln:   ' || l_sql_u); 
  
          -- A2. apply the hash ...
          EXECUTE IMMEDIATE l_sql_u;
          l_crt_count := l_crt_count + SQL%ROWCOUNT;
          commit;
          
        EXCEPTION
           WHEN OTHERS                                    
           THEN                                                
              DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
              ROLLBACK;
              DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_backtrace);
              raise_application_error (-20300, 'Hash Create failed');
        END;
  END LOOP;
   
  DBMS_OUTPUT.put_line ('Strt time: ' || to_char(l_runtm, 'DD/MM/YYYY HH24:MI:SS')); 
  DBMS_OUTPUT.put_line ('End  time: ' || to_char(current_date, 'DD/MM/YYYY HH24:MI:SS'));
--  DBMS_OUTPUT.put_line ('Pass count: ' || l_count);
  DBMS_OUTPUT.put_line ('Hash rows created: ' || l_crt_count);
end;
end "ORA_UPGRD_FND_HASH";
/