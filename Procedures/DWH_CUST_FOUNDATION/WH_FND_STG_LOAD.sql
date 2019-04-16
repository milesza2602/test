--------------------------------------------------------
--  DDL for Procedure WH_FND_STG_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_STG_LOAD" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alastair de Wet
--  Purpose:     LOAD STAGING
--  Tables:      Input  - NONE
--               Output - STG_C2_CUSTOMER_PORTFOLIO_CPY
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_count              number        :=  0;
g_date               date          := trunc(sysdate);



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

for g_count in 1 .. 250000
loop

INSERT INTO DWH_CUST_FOUNDATION.STG_C2_CUSTOMER_PORTFOLIO_CPY VALUES (
1,g_count,'01/jan/13','N','DWH',1,'Message',g_count,9433609,1,'ACTIVE','01/JAN/13','01/FEB/13','01/MAR/13','6007850107706739','PRIMARY','BS','ABC123456CDE');
INSERT INTO DWH_CUST_FOUNDATION.STG_C2_CUSTOMER_PORTFOLIO_CPY VALUES (
1,G_COUNT + 250000,'01/jan/13','N','DWH',1,'Message',G_COUNT+249000,9433609,19,'ACTIVE','01/JAN/13','01/FEB/13','01/MAR/13','6007850107706739','PRIMARY','BS','ABC123456CDE');


end loop;
END WH_FND_STG_LOAD;
