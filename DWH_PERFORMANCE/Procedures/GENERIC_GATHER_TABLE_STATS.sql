--------------------------------------------------------
--  DDL for Procedure GENERIC_GATHER_TABLE_STATS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."GENERIC_GATHER_TABLE_STATS" (procedure_name IN VARCHAR2, table_name IN VARCHAR2) AS

   filter_lst  DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB();

BEGIN
   IF procedure_name LIKE '%DJ' THEN
      RETURN;
   END IF;

   filter_lst.extend(1);
   filter_lst(1).objname := table_name;
   DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>NULL,obj_filter_list=>filter_lst,options=>'gather auto');
END;
