-- ****** Object: Procedure W7104429.DBMS_COMP Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "DBMS_COMP"  (l_owner in varchar2, l_table_name in varchar2, L_CLIST in varchar2) authid  current_user    --definer as  --
as

   l_sql_u              VARCHAR2 (4096);
   l_sql_t              VARCHAR2 (512);
   l_sql_c              VARCHAR2 (512);
   l_gn                 VARCHAR2 (512);
   l_start              DATE;
   l_runtm              DATE;
   l_crt_count          NUMBER;
   l_scn_count          NUMBER;
   l_seconds            NUMBER;
   l_rowcnt             NUMBER;
   g_recs_inserted1     integer       :=  0;
   g_recs_inserted2     integer       :=  0;
   g_recs_updated       integer       :=  0;
   g_recs_dropped       integer       :=  0;

   c                    integer;

   consistent    BOOLEAN;
   scan_info     DBMS_COMPARISON.COMPARISON_TYPE;

   too_many_attempts EXCEPTION;
   PRAGMA EXCEPTION_INIT(too_many_attempts, -20300);

BEGIN

        execute immediate 'alter session set events = ''6512 trace name ERRORSTACK level 3''';
          -- A1. create the comparison ...
--            , schema_name     => ''' || i.owner || '''
          l_sql_u :=
               'BEGIN
                   DBMS_COMPARISON.CREATE_COMPARISON
                  ( comparison_name => ''ODWH_TABLE_COMPARE''
                  , schema_name     => ''DWH_PERFORMANCE''
                  , object_name     => ''' || l_table_name || '''
                  , dblink_name     => ''DWH_POSTSNAP.DWHDBPRD.WOOLWORTHS.CO.ZA''
                  , remote_schema_name=>''DWH_PERFORMANCE''
                  , remote_object_name=>''' || l_table_name || '''
                  , column_list     => ''' || L_CLIST || '''
                  );
                END;'; 

          -- A2. apply the create ...
          EXECUTE IMMEDIATE l_sql_u;
          l_crt_count := l_crt_count + SQL%ROWCOUNT;

-- B. Perform a compare scan operation ...
        BEGIN
            consistent := DBMS_COMPARISON.COMPARE
                          ( comparison_name => 'ODWH_TABLE_COMPARE'
                          , scan_info       => scan_info
                          , perform_row_dif => TRUE
                          );
        END;
        commit;

        -- C. Save the results ...
        insert into W7104429.ORA_TBL_COMPARE 
        select  a.OBJECT_NAME,	
                a.SCHEMA_NAME,
                a.REMOTE_SCHEMA_NAME,
                a.DBLINK_NAME,
                b.SCAN_ID,
                b.PARENT_SCAN_ID,
                b.STATUS,
                b.CURRENT_DIF_COUNT,  
                b.INITIAL_DIF_COUNT,
                b.COUNT_ROWS,
                b.SCAN_NULLS,
                a.LAST_UPDATE_TIME
        from    USER_COMPARISON               a
        join    USER_COMPARISON_SCAN_SUMMARY  b on (a.COMPARISON_NAME = b.COMPARISON_NAME);
        commit;

        insert into W7104429.ORA_TBL_COMPARE_DTL 
        select  a.OBJECT_NAME,	
                a.SCHEMA_NAME,
                a.REMOTE_SCHEMA_NAME,
                a.DBLINK_NAME,

                r.SCAN_ID,
                c.COLUMN_NAME,
                r.INDEX_VALUE, 
                case when r.LOCAL_ROWID  is null then 'No' else 'Yes' end  LOCAL_ROWID,
                case when r.REMOTE_ROWID is null then 'No' else 'Yes' end  REMOTE_ROWID,
                a.LAST_UPDATE_TIME
        FROM    USER_COMPARISON         a
,               USER_COMPARISON_COLUMNS c
,               USER_COMPARISON_ROW_DIF r
,               USER_COMPARISON_SCAN    s
        WHERE   c.COMPARISON_NAME = upper('ODWH_TABLE_COMPARE') 
        AND     r.SCAN_ID         = s.SCAN_ID 
        --AND    s.last_update_time > systimestamp - 1/24/15  
        AND     r.STATUS          = 'DIF' 
        AND     c.INDEX_COLUMN    = 'Y' 
        AND     c.COMPARISON_NAME = r.COMPARISON_NAME 
        AND     c.COMPARISON_NAME = a.COMPARISON_NAME 
        ;
        commit;

        -- Purge the data ...
        begin DBMS_COMPARISON.DROP_COMPARISON('ODWH_TABLE_COMPARE'); end;
end "DBMS_COMP";
/