--------------------------------------------------------
--  DDL for Procedure WW_DO_TRUNC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WW_DO_TRUNC" (
   p_table_name VARCHAR2)
AS
BEGIN
   EXECUTE IMMEDIATE 'truncate table ' || p_table_name;
END;
