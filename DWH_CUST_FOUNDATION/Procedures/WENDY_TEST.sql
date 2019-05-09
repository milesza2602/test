--------------------------------------------------------
--  DDL for Procedure WENDY_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WENDY_TEST" AS 
g_sql            varchar2(8000);
BEGIN
--g_sql := ('drop TABLE DWH_FOUNDATION.FND_WOD_PROM_DISCOUNT');
--make a change to test release
--20190509
execute immediate g_sql;
commit;
END WENDY_TEST;
