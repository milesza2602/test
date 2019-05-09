--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_167U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_167U_OLD" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    CREATE TIER MONTH DETAIL EX FOUNDATION TABLE WITH UPPIVOT
--  Tables:      Input  - fnd_wod_tier_mth_detail  
--               Output - cust_wod_tier_mth_detail 
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance: Theo Filander 23/03/2017
--  Request No.  BCB-60
--  Remarks    : Update the current month only.
--               This fix doesnt allow for retro fixes. 
--               Only data where LAST_UPDATED_DATE = batch date (g_date) is processed
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
g_recs_hospital      integer       :=  0;

g_forall_limit       integer       :=  10000;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_year_no            number        := 0;
g_month_no           number        := 0;

g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_167U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD cust_wod_tier_mth_detail EX FND TABLE';
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
    
    l_text := 'LOAD OF cust_wod_tier_mth_detail EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

  


   l_text      := 'Start of Merge to LOAD cust_wod_tier_mth_detail';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
   
execute immediate 'alter session enable parallel dml';   

--************************************************************************************************** 
-- Lookup last months year and month number
--**************************************************************************************************
select distinct fin_year_no,fin_month_no 
  into g_year_no, g_month_no
  from dim_calendar 
 where calendar_date = (select distinct this_mn_start_date -1 
                          from dim_calendar 
                         where calendar_date = g_date);
                         
MERGE  /*+ parallel (rli,4) */ INTO cust_wod_tier_mth_detail CWOD USING
(
  with unpvt as (
                  SELECT *
                  FROM   DWH_CUST_FOUNDATION.FND_WOD_TIER_MTH_DETAIL
                  
                  UNPIVOT EXCLUDE NULLS 
                    (
                    (MONTH_SPEND,MONTH_GREEN_VALUE,MONTH_TBC_VALUE,MONTH_TIER_VALUE,MONTH_DISCOUNT,MONTH_TIER) 
                     FOR FIN_MONTH_NO IN 
                       (
                       (MONTH_01_SPEND,MONTH_01_GREEN_VALUE,MONTH_01_TBC_VALUE,MONTH_01_TIER_VALUE,MONTH_01_DISCOUNT,MONTH_01_TIER) AS 01, 
                       (MONTH_02_SPEND,MONTH_02_GREEN_VALUE,MONTH_02_TBC_VALUE,MONTH_02_TIER_VALUE,MONTH_02_DISCOUNT,MONTH_02_TIER) AS 02,
                       (MONTH_03_SPEND,MONTH_03_GREEN_VALUE,MONTH_03_TBC_VALUE,MONTH_03_TIER_VALUE,MONTH_03_DISCOUNT,MONTH_03_TIER) AS 03,
                       (MONTH_04_SPEND,MONTH_04_GREEN_VALUE,MONTH_04_TBC_VALUE,MONTH_04_TIER_VALUE,MONTH_04_DISCOUNT,MONTH_04_TIER) AS 04,
                       (MONTH_05_SPEND,MONTH_05_GREEN_VALUE,MONTH_05_TBC_VALUE,MONTH_05_TIER_VALUE,MONTH_05_DISCOUNT,MONTH_05_TIER) AS 05,
                       (MONTH_06_SPEND,MONTH_06_GREEN_VALUE,MONTH_06_TBC_VALUE,MONTH_06_TIER_VALUE,MONTH_06_DISCOUNT,MONTH_06_TIER) AS 06,
                       (MONTH_07_SPEND,MONTH_07_GREEN_VALUE,MONTH_07_TBC_VALUE,MONTH_07_TIER_VALUE,MONTH_07_DISCOUNT,MONTH_07_TIER) AS 07,
                       (MONTH_08_SPEND,MONTH_08_GREEN_VALUE,MONTH_08_TBC_VALUE,MONTH_08_TIER_VALUE,MONTH_08_DISCOUNT,MONTH_08_TIER) AS 08,
                       (MONTH_09_SPEND,MONTH_09_GREEN_VALUE,MONTH_09_TBC_VALUE,MONTH_09_TIER_VALUE,MONTH_09_DISCOUNT,MONTH_09_TIER) AS 09,
                       (MONTH_10_SPEND,MONTH_10_GREEN_VALUE,MONTH_10_TBC_VALUE,MONTH_10_TIER_VALUE,MONTH_10_DISCOUNT,MONTH_10_TIER) AS 10,
                       (MONTH_11_SPEND,MONTH_11_GREEN_VALUE,MONTH_11_TBC_VALUE,MONTH_11_TIER_VALUE,MONTH_11_DISCOUNT,MONTH_11_TIER) AS 11,
                       (MONTH_12_SPEND,MONTH_12_GREEN_VALUE,MONTH_12_TBC_VALUE,MONTH_12_TIER_VALUE,MONTH_12_DISCOUNT,MONTH_12_TIER) AS 12
                       )
                     )
                  WHERE  LAST_UPDATED_DATE = G_DATE
                  -- This ensures that only last months data is updated.
                    AND FIN_YEAR_NO  = G_YEAR_NO
                    AND FIN_MONTH_NO = G_MONTH_NO
                )
   SELECT FIN_YEAR_NO,
          FIN_MONTH_NO,
          PRIMARY_CUSTOMER_IDENTIFIER,
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
          G_DATE AS LAST_UPDATED_DATE
   FROM   UNPVT 
  ) FWOD
    ON  (CWOD.FIN_YEAR_NO       = FWOD.FIN_YEAR_NO AND
         CWOD.FIN_MONTH_NO      = FWOD.FIN_MONTH_NO AND
         CWOD.CUSTOMER_NO       = FWOD.PRIMARY_CUSTOMER_IDENTIFIER )
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
 
g_recs_read:=g_recs_read+SQL%ROWCOUNT;
g_recs_inserted:=g_recs_inserted+SQL%ROWCOUNT;

commit;
       


--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_inserted;
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


END WH_PRF_CUST_167U_OLD;
