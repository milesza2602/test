--------------------------------------------------------
--  DDL for Procedure MV_REFRESH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."MV_REFRESH" (p_table_name VARCHAR2, p_degree NUMBER)
AS
BEGIN
 
  DBMS_SNAPSHOT.REFRESH(
    LIST                 => 'DWH_PERFORMANCE.'||p_table_name
   ,PUSH_DEFERRED_RPC    => TRUE
   ,REFRESH_AFTER_ERRORS => FALSE
   ,PURGE_OPTION         => 0
   ,PARALLELISM          => p_degree
   ,HEAP_SIZE            => 1
   ,ATOMIC_REFRESH       => TRUE
   ,NESTED               => FALSE);
END MV_REFRESH;
