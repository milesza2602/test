-- ****** Object: Procedure W7131037.WH_PRF_CUST_167U_HIST Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_167U_HIST" (p_forall_limit in integer,p_success out boolean,p_date in date) AS

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    CREATE TIER MONTH DETAIL EX FOUNDATION TABLE WITH UPPIVOT
--  Tables:      Input  - fnd_wod_tier_mth_detail
--               Output - cust_wod_tier_mth_detail
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance: Theo Filander 23/03/2017
--  Request No:  BCB-60
--  Change No:   Chg-6111
--  Remarks    : Update the current month only into a temporary table TEMP_WOD_TIER_MTH_DETAIL.
--               On the last day of the month after load of temp table all data is moved to
--               the permanent table.
--               This fix doesnt allow for retro fixes.
--               Only data where LAST_UPDATED_DATE = batch date (g_date) is processed during the month.
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_last_read     integer       :=  0;
g_recs_last_inserted integer       :=  0;
g_roll_customers     integer       :=  0;
g_recs_hospital      integer       :=  0;

g_forall_limit       integer       :=  10000;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate)-1;
g_this_mn_end_date   date          := trunc(sysdate)-1;
g_prev_mn_end_date   date          := trunc(sysdate)-1;
g_year_no            number        := 0;
g_month_no           number        := 0;
g_prev_year_no       number        := 0;
g_prev_month_no      number        := 0;
g_2_year_no          number        := 0;
g_2_month_no         number        := 0;
g_next_year_no       number        := 0;
g_next_month_no      number        := 0;

g_stmt                varchar(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_167U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_WOD_TIER_MTH_DETAIL EX TEMP EX FND TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF CUST_WOD_TIER_MTH_DETAIL EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd/MON/yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    if p_date is not null then
       g_date := p_date;
    else
      dwh_lookup.dim_control(g_date);
    end if;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text      := 'START OF MERGE TO LOAD TEMP_WOD_TIER_MTH_DETAIL';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session force parallel dml';

--**************************************************************************************************
-- Lookup this and last months year and month number
-- Amend to identify prev month as well
--**************************************************************************************************
--select distinct fin_year_no,fin_month_no this_mn_end_date
--  into g_year_no, g_month_no,g_this_mn_end_date
--  from dim_calendar
-- where calendar_date = (select distinct this_mn_start_date -1
--                          from dim_calendar
--                         where calendar_date = g_date);

select ac.fin_year_no,ac.fin_month_no,ac.this_mn_end_date,bc.this_mn_end_date,bc.fin_year_no,bc.fin_month_no,cc.fin_year_no,cc.fin_month_no, nc.fin_year_no,nc.fin_month_no
  into g_year_no, g_month_no,g_this_mn_end_date,g_prev_mn_end_date, g_prev_year_no, g_prev_month_no,g_2_year_no, g_2_month_no, g_next_year_no, g_next_month_no
  from dim_calendar ac
  join dim_calendar bc on ac.this_mn_start_date-1 = bc.calendar_date
  join dim_calendar cc on bc.this_mn_start_date-1 = cc.calendar_date
  join dim_calendar nc on ac.this_mn_end_date+1 = nc.calendar_date
 where ac.calendar_date = g_date;

l_text := 'FOR CURRENT PERIOD '|| g_year_no||g_month_no||' AND PREVIOUS '||g_prev_year_no||g_prev_month_no||'. MONTH END DATE IS '||g_this_mn_end_date;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  TFilander  Additional Code
--**************************************************************************************************
-- During the course of the financial month, all current month data is inserted into the temp table.
--
--**************************************************************************************************
MERGE  INTO W7131037.temp_wod_tier_mth_detail TWOD USING
(
   with unpvt as (
                  select * from (
                  SELECT
                         FIN_YEAR_NO,
                         PRIMARY_CUSTOMER_IDENTIFIER,
                         START_TIER,
                         OVERRIDE_TIER,
                         YTD_SPEND,
                         YTD_GREEN_VALUE,
                         YTD_TBC_VALUE,
                         YTD_TIER_VALUE,
                         YTD_DISCOUNT,
                         MONTH_01_SPEND,
                         MONTH_01_GREEN_VALUE,
                         MONTH_01_TBC_VALUE,
                         MONTH_01_TIER_VALUE,
                         MONTH_01_DISCOUNT,
                         MONTH_01_TIER,
                         MONTH_02_SPEND,
                         MONTH_02_GREEN_VALUE,
                         MONTH_02_TBC_VALUE,
                         MONTH_02_TIER_VALUE,
                         MONTH_02_DISCOUNT,
                         MONTH_02_TIER,
                         MONTH_03_SPEND,
                         MONTH_03_GREEN_VALUE,
                         MONTH_03_TBC_VALUE,
                         MONTH_03_TIER_VALUE,
                         MONTH_03_DISCOUNT,
                         MONTH_03_TIER,
                         MONTH_04_SPEND,
                         MONTH_04_GREEN_VALUE,
                         MONTH_04_TBC_VALUE,
                         MONTH_04_TIER_VALUE,
                         MONTH_04_DISCOUNT,
                         MONTH_04_TIER,
                         MONTH_05_SPEND,
                         MONTH_05_GREEN_VALUE,
                         MONTH_05_TBC_VALUE,
                         MONTH_05_TIER_VALUE,
                         MONTH_05_DISCOUNT,
                         MONTH_05_TIER,
                         MONTH_06_SPEND,
                         MONTH_06_GREEN_VALUE,
                         MONTH_06_TBC_VALUE,
                         MONTH_06_TIER_VALUE,
                         MONTH_06_DISCOUNT,
                         MONTH_06_TIER,
                         MONTH_07_SPEND,
                         MONTH_07_GREEN_VALUE,
                         MONTH_07_TBC_VALUE,
                         MONTH_07_TIER_VALUE,
                         MONTH_07_DISCOUNT,
                         MONTH_07_TIER,
                         MONTH_08_SPEND,
                         MONTH_08_GREEN_VALUE,
                         MONTH_08_TBC_VALUE,
                         MONTH_08_TIER_VALUE,
                         MONTH_08_DISCOUNT,
                         MONTH_08_TIER,
                         MONTH_09_SPEND,
                         MONTH_09_GREEN_VALUE,
                         MONTH_09_TBC_VALUE,
                         MONTH_09_TIER_VALUE,
                         MONTH_09_DISCOUNT,
                         MONTH_09_TIER,
                         MONTH_10_SPEND,
                         MONTH_10_GREEN_VALUE,
                         MONTH_10_TBC_VALUE,
                         MONTH_10_TIER_VALUE,
                         MONTH_10_DISCOUNT,
                         MONTH_10_TIER,
                         MONTH_11_SPEND,
                         MONTH_11_GREEN_VALUE,
                         MONTH_11_TBC_VALUE,
                         MONTH_11_TIER_VALUE,
                         MONTH_11_DISCOUNT,
                         MONTH_11_TIER,
                         MONTH_12_SPEND,
                         MONTH_12_GREEN_VALUE,
                         MONTH_12_TBC_VALUE,
                         MONTH_12_TIER_VALUE,
                         MONTH_12_DISCOUNT,
                         MONTH_12_TIER,
                         LAST_UPDATED_DATE,
        ----- Existing Columns ^ ---    New Columns v  ------
                         MONTH_01_SPEND YTD_01_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND YTD_02_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND YTD_03_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND YTD_04_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND YTD_05_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND YTD_06_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND YTD_07_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND+
                         MONTH_08_SPEND YTD_08_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND+
                         MONTH_08_SPEND+
                         MONTH_09_SPEND YTD_09_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND+
                         MONTH_08_SPEND+
                         MONTH_09_SPEND+
                         MONTH_10_SPEND YTD_10_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND+
                         MONTH_08_SPEND+
                         MONTH_09_SPEND+
                         MONTH_10_SPEND+
                         MONTH_11_SPEND YTD_11_SPEND,
                         MONTH_01_SPEND+
                         MONTH_02_SPEND+
                         MONTH_03_SPEND+
                         MONTH_04_SPEND+
                         MONTH_05_SPEND+
                         MONTH_06_SPEND+
                         MONTH_07_SPEND+
                         MONTH_08_SPEND+
                         MONTH_09_SPEND+
                         MONTH_10_SPEND+
                         MONTH_11_SPEND+
                         MONTH_12_SPEND YTD_12_SPEND,
                         MONTH_01_GREEN_VALUE YTD_01_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE YTD_02_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE YTD_03_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE YTD_04_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE YTD_05_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE YTD_06_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE YTD_07_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE+
                         MONTH_08_GREEN_VALUE YTD_08_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE+
                         MONTH_08_GREEN_VALUE+
                         MONTH_09_GREEN_VALUE YTD_09_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE+
                         MONTH_08_GREEN_VALUE+
                         MONTH_09_GREEN_VALUE+
                         MONTH_10_GREEN_VALUE YTD_10_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE+
                         MONTH_08_GREEN_VALUE+
                         MONTH_09_GREEN_VALUE+
                         MONTH_10_GREEN_VALUE+
                         MONTH_11_GREEN_VALUE YTD_11_GREEN_VALUE,
                         MONTH_01_GREEN_VALUE+
                         MONTH_02_GREEN_VALUE+
                         MONTH_03_GREEN_VALUE+
                         MONTH_04_GREEN_VALUE+
                         MONTH_05_GREEN_VALUE+
                         MONTH_06_GREEN_VALUE+
                         MONTH_07_GREEN_VALUE+
                         MONTH_08_GREEN_VALUE+
                         MONTH_09_GREEN_VALUE+
                         MONTH_10_GREEN_VALUE+
                         MONTH_11_GREEN_VALUE+
                         MONTH_12_GREEN_VALUE YTD_12_GREEN_VALUE,
                         MONTH_01_DISCOUNT YTD_01_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT YTD_02_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT YTD_03_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT YTD_04_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT YTD_05_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT YTD_06_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT YTD_07_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT+
                         MONTH_08_DISCOUNT YTD_08_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT+
                         MONTH_08_DISCOUNT+
                         MONTH_09_DISCOUNT YTD_09_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT+
                         MONTH_08_DISCOUNT+
                         MONTH_09_DISCOUNT+
                         MONTH_10_DISCOUNT YTD_10_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT+
                         MONTH_08_DISCOUNT+
                         MONTH_09_DISCOUNT+
                         MONTH_10_DISCOUNT+
                         MONTH_11_DISCOUNT YTD_11_DISCOUNT,
                         MONTH_01_DISCOUNT+
                         MONTH_02_DISCOUNT+
                         MONTH_03_DISCOUNT+
                         MONTH_04_DISCOUNT+
                         MONTH_05_DISCOUNT+
                         MONTH_06_DISCOUNT+
                         MONTH_07_DISCOUNT+
                         MONTH_08_DISCOUNT+
                         MONTH_09_DISCOUNT+
                         MONTH_10_DISCOUNT+
                         MONTH_11_DISCOUNT+
                         MONTH_12_DISCOUNT YTD_12_DISCOUNT
                  FROM   W7131037.FND_WOD_TIER_MTH_DETAIL@DWHPRD)

                  UNPIVOT EXCLUDE NULLS
                    (
                    (YTD_REV_SPEND,YTD_REV_GREEN_VALUE,YTD_REV_DISCOUNT,MONTH_SPEND,MONTH_GREEN_VALUE,MONTH_TBC_VALUE,MONTH_TIER_VALUE,MONTH_DISCOUNT,MONTH_TIER)
                     FOR FIN_MONTH_NO IN
                       (
                       (YTD_01_SPEND,YTD_01_GREEN_VALUE,YTD_01_DISCOUNT,MONTH_01_SPEND,MONTH_01_GREEN_VALUE,MONTH_01_TBC_VALUE,MONTH_01_TIER_VALUE,MONTH_01_DISCOUNT,MONTH_01_TIER) AS 01,
                       (YTD_02_SPEND,YTD_02_GREEN_VALUE,YTD_02_DISCOUNT,MONTH_02_SPEND,MONTH_02_GREEN_VALUE,MONTH_02_TBC_VALUE,MONTH_02_TIER_VALUE,MONTH_02_DISCOUNT,MONTH_02_TIER) AS 02,
                       (YTD_03_SPEND,YTD_03_GREEN_VALUE,YTD_03_DISCOUNT,MONTH_03_SPEND,MONTH_03_GREEN_VALUE,MONTH_03_TBC_VALUE,MONTH_03_TIER_VALUE,MONTH_03_DISCOUNT,MONTH_03_TIER) AS 03,
                       (YTD_04_SPEND,YTD_04_GREEN_VALUE,YTD_04_DISCOUNT,MONTH_04_SPEND,MONTH_04_GREEN_VALUE,MONTH_04_TBC_VALUE,MONTH_04_TIER_VALUE,MONTH_04_DISCOUNT,MONTH_04_TIER) AS 04,
                       (YTD_05_SPEND,YTD_05_GREEN_VALUE,YTD_05_DISCOUNT,MONTH_05_SPEND,MONTH_05_GREEN_VALUE,MONTH_05_TBC_VALUE,MONTH_05_TIER_VALUE,MONTH_05_DISCOUNT,MONTH_05_TIER) AS 05,
                       (YTD_06_SPEND,YTD_06_GREEN_VALUE,YTD_06_DISCOUNT,MONTH_06_SPEND,MONTH_06_GREEN_VALUE,MONTH_06_TBC_VALUE,MONTH_06_TIER_VALUE,MONTH_06_DISCOUNT,MONTH_06_TIER) AS 06,
                       (YTD_07_SPEND,YTD_07_GREEN_VALUE,YTD_07_DISCOUNT,MONTH_07_SPEND,MONTH_07_GREEN_VALUE,MONTH_07_TBC_VALUE,MONTH_07_TIER_VALUE,MONTH_07_DISCOUNT,MONTH_07_TIER) AS 07,
                       (YTD_08_SPEND,YTD_08_GREEN_VALUE,YTD_08_DISCOUNT,MONTH_08_SPEND,MONTH_08_GREEN_VALUE,MONTH_08_TBC_VALUE,MONTH_08_TIER_VALUE,MONTH_08_DISCOUNT,MONTH_08_TIER) AS 08,
                       (YTD_09_SPEND,YTD_09_GREEN_VALUE,YTD_09_DISCOUNT,MONTH_09_SPEND,MONTH_09_GREEN_VALUE,MONTH_09_TBC_VALUE,MONTH_09_TIER_VALUE,MONTH_09_DISCOUNT,MONTH_09_TIER) AS 09,
                       (YTD_10_SPEND,YTD_10_GREEN_VALUE,YTD_10_DISCOUNT,MONTH_10_SPEND,MONTH_10_GREEN_VALUE,MONTH_10_TBC_VALUE,MONTH_10_TIER_VALUE,MONTH_10_DISCOUNT,MONTH_10_TIER) AS 10,
                       (YTD_11_SPEND,YTD_11_GREEN_VALUE,YTD_11_DISCOUNT,MONTH_11_SPEND,MONTH_11_GREEN_VALUE,MONTH_11_TBC_VALUE,MONTH_11_TIER_VALUE,MONTH_11_DISCOUNT,MONTH_11_TIER) AS 11,
                       (YTD_12_SPEND,YTD_12_GREEN_VALUE,YTD_12_DISCOUNT,MONTH_12_SPEND,MONTH_12_GREEN_VALUE,MONTH_12_TBC_VALUE,MONTH_12_TIER_VALUE,MONTH_12_DISCOUNT,MONTH_12_TIER) AS 12
                       )
                     )
                  WHERE  LAST_UPDATED_DATE = G_DATE
                )
   SELECT FIN_YEAR_NO,
          FIN_MONTH_NO,
          PRIMARY_CUSTOMER_IDENTIFIER,
          START_TIER,
          -- YTD_SPEND,
          YTD_REV_SPEND YTD_SPEND,
          -- YTD_GREEN_VALUE,
          YTD_REV_GREEN_VALUE YTD_GREEN_VALUE,
          -- YTD_TIER_VALUE,
          YTD_REV_SPEND+YTD_REV_GREEN_VALUE YTD_TIER_VALUE,
          -- YTD_DISCOUNT,
          YTD_REV_DISCOUNT YTD_DISCOUNT,
          MONTH_SPEND,
          MONTH_GREEN_VALUE,
          MONTH_TIER_VALUE,
          MONTH_DISCOUNT,
          MONTH_TIER,
          G_DATE AS LAST_UPDATED_DATE
   FROM   UNPVT
  ) FWOD
    ON  (TWOD.FIN_YEAR_NO       = FWOD.FIN_YEAR_NO AND
         TWOD.FIN_MONTH_NO      = FWOD.FIN_MONTH_NO AND
         TWOD.CUSTOMER_NO       = FWOD.PRIMARY_CUSTOMER_IDENTIFIER )
    WHEN MATCHED THEN
    update set    TWOD.START_TIER               =   FWOD.START_TIER,
                  TWOD.YTD_SPEND                =   FWOD.YTD_SPEND,
                  TWOD.YTD_GREEN_VALUE          =   FWOD.YTD_GREEN_VALUE,
                  TWOD.YTD_TIER_VALUE           =   FWOD.YTD_TIER_VALUE,
                  TWOD.YTD_DISCOUNT             =   FWOD.YTD_DISCOUNT,
                  TWOD.MONTH_SPEND              =   FWOD.MONTH_SPEND,
                  TWOD.MONTH_GREEN_VALUE        =   FWOD.MONTH_GREEN_VALUE,
                  TWOD.MONTH_TIER_VALUE         =   FWOD.MONTH_TIER_VALUE,
                  TWOD.MONTH_DISCOUNT           =   FWOD.MONTH_DISCOUNT,
                  TWOD.MONTH_TIER               =   FWOD.MONTH_TIER,
                  TWOD.LAST_UPDATED_DATE        =   G_DATE
      where       NVL(TWOD.START_TIER,0)        <>  FWOD.START_TIER OR
                  NVL(TWOD.YTD_SPEND,0)         <>  FWOD.YTD_SPEND OR
                  NVL(TWOD.YTD_GREEN_VALUE,0)   <>  FWOD.YTD_GREEN_VALUE OR
                  NVL(TWOD.YTD_TIER_VALUE,0)    <>  FWOD.YTD_TIER_VALUE OR
                  NVL(TWOD.YTD_DISCOUNT,0)      <>  FWOD.YTD_DISCOUNT OR
                  NVL(TWOD.MONTH_SPEND,0)       <>  FWOD.MONTH_SPEND OR
                  NVL(TWOD.MONTH_GREEN_VALUE,0) <>  FWOD.MONTH_GREEN_VALUE OR
                  NVL(TWOD.MONTH_TIER_VALUE,0)  <>  FWOD.MONTH_TIER_VALUE OR
                  NVL(TWOD.MONTH_DISCOUNT,0)    <>  FWOD.MONTH_DISCOUNT OR
                  NVL(TWOD.MONTH_TIER,0)        <>  FWOD.MONTH_TIER
    WHEN NOT MATCHED THEN
      insert (
              TWOD.FIN_YEAR_NO,
              TWOD.FIN_MONTH_NO,
              TWOD.CUSTOMER_NO,
              TWOD.START_TIER,
              TWOD.YTD_SPEND,
              TWOD.YTD_GREEN_VALUE,
              TWOD.YTD_TIER_VALUE,
              TWOD.YTD_DISCOUNT,
              TWOD.MONTH_SPEND,
              TWOD.MONTH_GREEN_VALUE,
              TWOD.MONTH_TIER_VALUE,
              TWOD.MONTH_DISCOUNT,
              TWOD.MONTH_TIER,
              TWOD.LAST_UPDATED_DATE
             )
      values
             (
              FWOD.FIN_YEAR_NO,
              FWOD.FIN_MONTH_NO,
              FWOD.PRIMARY_CUSTOMER_IDENTIFIER,
              FWOD.START_TIER,
              FWOD.YTD_SPEND,
              FWOD.YTD_GREEN_VALUE,
              FWOD.YTD_TIER_VALUE,
              FWOD.YTD_DISCOUNT,
              FWOD.MONTH_SPEND,
              FWOD.MONTH_GREEN_VALUE,
              FWOD.MONTH_TIER_VALUE,
              FWOD.MONTH_DISCOUNT,
              FWOD.MONTH_TIER,
              G_DATE
              );
--  TFilander

g_recs_read:=g_recs_read+SQL%ROWCOUNT;
g_recs_inserted:=g_recs_inserted+SQL%ROWCOUNT;
l_text :=  'RECORDS WRITTEN TO TEMP '||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
commit;

--    l_text := 'UPDATE STATS ON TEMP_WOD_TIER_MTH_DETAIL';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('W7131037','TEMP_WOD_TIER_MTH_DETAIL',estimate_percent=>1, DEGREE => 32);

IF (g_date = g_this_mn_end_date       or
    g_date = g_prev_mn_end_date + 1   or
    g_date = g_prev_mn_end_date + 7   or
    g_date = g_prev_mn_end_date + 14  or
    g_date = g_prev_mn_end_date + 21) then
--**************************************************************************************************
-- If on the last day of the financial month, all records in the temp table needs to be copied
-- into the dimension table for the previous month.
-- This is repeated 7 days later to catch any late updates
--**************************************************************************************************
    IF g_date = g_this_mn_end_date then
        l_text := 'END OF FINANCIAL MONTH. LOADING DATA INTO CUST_WOD_TIER_MTH_DETAIL '||
        to_char(g_date,('dd/MON/yyyy'));
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    else
        l_text := 'REFRESHING DATA IN CUST. LOADING DATA INTO CUST_WOD_TIER_MTH_DETAIL '||
        to_char(g_date,('dd/MON/yyyy'));
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;

--**************************************************************************************************
-- If on the last day of the financial month, all records in the CUST table needs to be copied
-- into the temp table for the following month.
--
-- Load customer data that have not transacted in the current month.
--**************************************************************************************************
IF g_date = g_prev_mn_end_date+1 then

    l_text := 'IDENTIFY CUSTOMERS WHO HAVE NOT TRANSACTED IN THE CURRENT MONTH AND LOAD THEM INTO TEMP '||
    to_char(g_this_mn_end_date,('dd/MON/yyyy'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /*+ Parallel(tw,6) */ into temp_wod_tier_mth_detail tw
    select /*+ Parallel(prev,6) Full(prev)  */
           g_year_no,
           g_month_no,
           prev.customer_no,
           prev.start_tier,
           case when g_month_no = 1 then 0 else prev.ytd_spend end ytd_spend,
           case when g_month_no = 1 then 0 else prev.ytd_green_value end ytd_green_value,
           case when g_month_no = 1 then 0 else prev.ytd_tier_value end ytd_tier_value,
           case when g_month_no = 1 then 0 else prev.ytd_discount end ytd_discount,
           0,
           0,
           0,
           0,
           0,
           trunc(sysdate) last_updated_date
      from W7131037.cust_wod_tier_mth_detail prev
     where fin_year_no   = g_prev_year_no
        and fin_month_no = g_prev_month_no
     and NOT EXISTS (select /*+ Parallel(curr,6) Full(curr)  */
                            customer_no
                       from W7131037.temp_wod_tier_mth_detail curr
                      where fin_year_no  = g_year_no
                        and fin_month_no = g_month_no
                        and prev.customer_no = curr.customer_no
                       );
    g_roll_customers:=g_roll_customers+SQL%ROWCOUNT;
    l_text :=  'CUSTOMERS ROLLED FROM PREVIOUS MONTH '||g_roll_customers;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;

end if;

--**************************************************************************************************
-- Move tier data from TEMP_WOD_TIER_MTH_DETAIL TO CUST_WOD_TIER_MTH_DETAIL.
--**************************************************************************************************

    l_text := 'MOVE TIER DATA FROM TEMP_WOD_TIER_MTH_DETAIL TO CUST_WOD_TIER_MTH_DETAIL. '||
    to_char(g_this_mn_end_date,('dd/MON/yyyy'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE INTO cust_wod_tier_mth_detail CWOD USING
    (
       SELECT FIN_YEAR_NO,
              FIN_MONTH_NO,
              CUSTOMER_NO,
              START_TIER,
              YTD_SPEND,
              YTD_GREEN_VALUE,
              YTD_TIER_VALUE,
              YTD_DISCOUNT,
              MONTH_SPEND,
              MONTH_GREEN_VALUE,
              MONTH_TIER_VALUE,
              MONTH_DISCOUNT,
              MONTH_TIER,
              LAST_UPDATED_DATE

       FROM TEMP_WOD_TIER_MTH_DETAIL
      WHERE FIN_YEAR_NO  IN (CASE WHEN g_date = g_this_mn_end_date THEN (G_YEAR_NO) ELSE (G_PREV_YEAR_NO) END)
        AND FIN_MONTH_NO IN (CASE WHEN g_date = g_this_mn_end_date THEN (G_MONTH_NO) ELSE (G_PREV_MONTH_NO) END)
      ) FWOD
        ON  (CWOD.FIN_YEAR_NO       = FWOD.FIN_YEAR_NO AND
             CWOD.FIN_MONTH_NO      = FWOD.FIN_MONTH_NO AND
             CWOD.CUSTOMER_NO       = FWOD.CUSTOMER_NO )
        WHEN MATCHED THEN
        update set    CWOD.START_TIER               =   FWOD.START_TIER,
                      CWOD.YTD_SPEND                =   FWOD.YTD_SPEND,
                      CWOD.YTD_GREEN_VALUE          =   FWOD.YTD_GREEN_VALUE,
                      CWOD.YTD_TIER_VALUE           =   FWOD.YTD_TIER_VALUE,
                      CWOD.YTD_DISCOUNT             =   FWOD.YTD_DISCOUNT,
                      CWOD.MONTH_SPEND              =   FWOD.MONTH_SPEND,
                      CWOD.MONTH_GREEN_VALUE        =   FWOD.MONTH_GREEN_VALUE,
                      CWOD.MONTH_TIER_VALUE         =   FWOD.MONTH_TIER_VALUE,
                      CWOD.MONTH_DISCOUNT           =   FWOD.MONTH_DISCOUNT,
                      CWOD.MONTH_TIER               =   FWOD.MONTH_TIER,
                      CWOD.LAST_UPDATED_DATE        =   G_DATE
          where       NVL(CWOD.START_TIER,0)        <>  FWOD.START_TIER OR
                      NVL(CWOD.YTD_SPEND,0)         <>  FWOD.YTD_SPEND OR
                      NVL(CWOD.YTD_GREEN_VALUE,0)   <>  FWOD.YTD_GREEN_VALUE OR
                      NVL(CWOD.YTD_TIER_VALUE,0)    <>  FWOD.YTD_TIER_VALUE OR
                      NVL(CWOD.YTD_DISCOUNT,0)      <>  FWOD.YTD_DISCOUNT OR
                      NVL(CWOD.MONTH_SPEND,0)       <>  FWOD.MONTH_SPEND OR
                      NVL(CWOD.MONTH_GREEN_VALUE,0) <>  FWOD.MONTH_GREEN_VALUE OR
                      NVL(CWOD.MONTH_TIER_VALUE,0)  <>  FWOD.MONTH_TIER_VALUE OR
                      NVL(CWOD.MONTH_DISCOUNT,0)    <>  FWOD.MONTH_DISCOUNT OR
                      NVL(CWOD.MONTH_TIER,0)        <>  FWOD.MONTH_TIER
        WHEN NOT MATCHED THEN
          insert (
                  CWOD.FIN_YEAR_NO,
                  CWOD.FIN_MONTH_NO,
                  CWOD.CUSTOMER_NO,
                  CWOD.START_TIER,
                  CWOD.YTD_SPEND,
                  CWOD.YTD_GREEN_VALUE,
                  CWOD.YTD_TIER_VALUE,
                  CWOD.YTD_DISCOUNT,
                  CWOD.MONTH_SPEND,
                  CWOD.MONTH_GREEN_VALUE,
                  CWOD.MONTH_TIER_VALUE,
                  CWOD.MONTH_DISCOUNT,
                  CWOD.MONTH_TIER,
                  CWOD.LAST_UPDATED_DATE
                 )
          values
                 (
                  FWOD.FIN_YEAR_NO,
                  FWOD.FIN_MONTH_NO,
                  FWOD.CUSTOMER_NO,
                  FWOD.START_TIER,
                  FWOD.YTD_SPEND,
                  FWOD.YTD_GREEN_VALUE,
                  FWOD.YTD_TIER_VALUE,
                  FWOD.YTD_DISCOUNT,
                  FWOD.MONTH_SPEND,
                  FWOD.MONTH_GREEN_VALUE,
                  FWOD.MONTH_TIER_VALUE,
                  FWOD.MONTH_DISCOUNT,
                  FWOD.MONTH_TIER,
                  G_DATE
                  );

    g_recs_last_read:=g_recs_last_read+SQL%ROWCOUNT;
    g_recs_last_inserted:=g_recs_last_inserted+SQL%ROWCOUNT;
    commit;

--    l_text := 'UPDATE STATS ON CUST_WOD_TIER_MTH_DETAIL';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('W7131037','CUST_WOD_TIER_MTH_DETAIL',estimate_percent=>1, DEGREE => 32);
else
    l_text := 'BATCH DATE IS NOT SAME AS END OF FIN MONTH '||
    to_char(g_this_mn_end_date,('dd/MON/yyyy'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end if;

l_text :=  'RECORDS WRITTEN TO DIM '||g_recs_last_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_last_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END "WH_PRF_CUST_167U_HIST";
