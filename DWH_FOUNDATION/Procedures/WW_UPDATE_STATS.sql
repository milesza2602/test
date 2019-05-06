--------------------------------------------------------
--  DDL for Procedure WW_UPDATE_STATS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WW_UPDATE_STATS" (p_table_name varchar2
)
AS
   l_msg   VARCHAR2 (255);
BEGIN
  DBMS_STATS.gather_table_stats 
  (ownname => 'DWH_FOUNDATION',
                                  tabname => p_table_name,
                                  estimate_percent => DBMS_STATS.auto_sample_size,
                                  method_opt => 'FOR ALL COLUMNS SIZE AUTO',
                                  granularity => 'ALL',
                                  degree => 6,
                                  cascade => TRUE
   );
   DBMS_OUTPUT.put_line(   'Gather table stats on '
                        || p_table_name
                        || ' has been completed.');
END;
