-- ****** Object: Procedure W7131037.WW_DO_TRUNC Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WW_DO_TRUNC" (
   p_table_name VARCHAR2)
AS
BEGIN
   EXECUTE IMMEDIATE 'truncate table ' || p_table_name;
END;
