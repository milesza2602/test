--------------------------------------------------------
--  DDL for Procedure MV_TRUNC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."MV_TRUNC" (p_table_name VARCHAR2)
AS
BEGIN
    execute immediate 'truncate table DWH_PERFORMANCE.'||p_table_name;
END MV_TRUNC;
