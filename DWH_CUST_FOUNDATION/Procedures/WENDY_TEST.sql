--------------------------------------------------------
--  DDL for Procedure WENDY_TEST
--------------------------------------------------------
SET DEFINE OFF;

CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WENDY_TEST" AS
    g_sql   VARCHAR2(8000);
BEGIN
-- added a comment - mdm
--g_sql := ('drop TABLE DWH_FOUNDATION.FND_WOD_PROM_DISCOUNT');
--make a change to test release
--20190509
--add a comment - 20190509 
    EXECUTE IMMEDIATE g_sql;
    COMMIT;
END wendy_test;
/

SHOW ERRORS