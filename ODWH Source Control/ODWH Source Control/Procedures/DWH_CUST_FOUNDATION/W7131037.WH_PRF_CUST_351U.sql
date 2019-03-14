-- ****** Object: Procedure W7131037.WH_PRF_CUST_351U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_351U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        OCT 2017
--  Author:      Alastair de Wet
--  Purpose:     Create CUST_MART_VOUCHERS_WEEK fact table in the performance layer
--               with input ex cust_CSM_st_wk table from performance layer.
--               THIS JOB RUNS WEEKLY AFTER THE START OF A NEW WEEK
--  Tables:      Input  - cust_fv_voucher
--               Output - CUST_MART_VOUCHERS_WEEK
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            CUST_MART_VOUCHERS_WEEK%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number         ;
g_end_month          number         ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_351U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE CUST_FV_VOUCHER to CUST_MART_VOUCHERS_WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP OF CUST_MART_VOUCHERS_WEEK EX cust_fv_vouchers LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Determine if this is a day on which we process
--**************************************************************************************************


  if to_char(sysdate,'DY')  <> 'SAT' then
      l_text      := 'This job only runs on Saturday and today '||to_char(sysdate,'DAY')||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;
      l_text      := 'This job only runs on Saturday and today '||to_char(sysdate,'DAY')||' is that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Main loop
--**************************************************************************************************

execute immediate 'alter session enable parallel dml';

    l_text      := 'TRUNCATE tableS  W7131037.CUST_MART_VOUCHERS_WK, MN AND YR';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_stmt      := 'TRUNCATE table  W7131037.CUST_MART_VOUCHERS_WEEK';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  W7131037.CUST_MART_VOUCHERS_MONTH';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  W7131037.CUST_MART_VOUCHERS_YEAR';
    execute immediate g_stmt;


COMMIT;

--================================================================================

    l_text := 'ROLLUP WEEK RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 1' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


INSERT  /*+ append */ INTO CUST_MART_VOUCHERS_WEEK CSM
with      CAMPS AS (
SELECT   /*+ parallel(16) */
         CAMPAIGN_NO,
         MAX(CAMPAIGN_START_DATE) CAMPAIGN_START_DATE,
         MAX(CAMPAIGN_END_DATE) CAMPAIGN_END_DATE
FROM     FND_FV_CAMPAIGN_PROMOTION
GROUP BY CAMPAIGN_NO
               ),
      WEEKS AS (
SELECT  /*+ parallel(4) */
        FIN_YEAR_NO,
        FIN_WEEK_NO,
        CAMPAIGN_NO
FROM    CAMPS CMP,
        DIM_CALENDAR_WK
WHERE   THIS_WEEK_END_DATE   >=  CAMPAIGN_START_DATE
AND     THIS_WEEK_START_DATE <=  CAMPAIGN_END_DATE
                ),
      ISSUED AS (
SELECT  /*+ parallel(CNT,16) FULL(CNT) FULL(WEEKS) */
        FIN_YEAR_NO,
        FIN_WEEK_NO,
        PRIMARY_CUSTOMER_IDENTIFIER,
        VOUCHER_NO
FROM    CUST_FV_VOUCHER CNT ,
        WEEKS
WHERE   CNT.CAMPAIGN_NO = WEEKS.CAMPAIGN_NO
AND     FIN_YEAR_NO > 2012
AND     VOUCHER_STATUS_DESCRIPTION IN ( 'Active','Redeemed','Expired')
                 )
SELECT   /*+ parallel(issued,16)  FULL(issued)   */
         ISSUED.FIN_YEAR_NO,
         ISSUED.FIN_WEEK_NO,
         COUNT(DISTINCT VOUCHER_NO) VOUCHER_TOT,
         COUNT(DISTINCT PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
         '','','','',
         G_DATE,''
FROM     ISSUED
GROUP BY ISSUED.FIN_YEAR_NO,
         ISSUED.FIN_WEEK_NO
;


  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

COMMIT;

--================================================================================

    l_text := 'UPDATE STATS ON MART TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','CUST_MART_VOUCHERS_WEEK',estimate_percent=>1, DEGREE => 32);
COMMIT;

--================================================================================

    l_text := 'ROLLUP WEEK RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 2' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE  /*+ PARALLEL(4) */ INTO CUST_MART_VOUCHERS_WEEK CSM
    USING (

    SELECT   /*+ parallel(VC,16) parallel(CB,16) FULL(VC) FULL(CB) FULL(DC) */
             DC.FIN_YEAR_NO,
             DC.FIN_WEEK_NO,
             COUNT(DISTINCT VC.VOUCHER_NO) VOUCHER_TOT,
             COUNT(DISTINCT VC.PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
             COUNT(DISTINCT CB.TRAN_DATE||CB.TRAN_NO||CB.TILL_NO||CB.LOCATION_NO) TRAN_TOT,
             SUM(VC.REDEEMED_AMOUNT) REDEEMED_AMOUNT,
             SUM(CB.TRAN_SELLING) TRAN_SELLING
    FROM     CUST_FV_VOUCHER VC,
             CUST_BASKET CB,
             DIM_CALENDAR DC
    WHERE    VOUCHER_STATUS_DESCRIPTION = 'Redeemed'
    AND      REDEEMED_DATE    = DC.CALENDAR_DATE
    AND      REDEEMED_DATE    = CB.TRAN_DATE
    AND      REDEEMED_TRAN_NO = CB.TRAN_NO
    AND      REDEEMED_TILL_NO = CB.TILL_NO
    AND      REDEEMED_STORE   = CB.LOCATION_NO
    GROUP BY DC.FIN_YEAR_NO,
             DC.FIN_WEEK_NO
                   ) MER_REC
    ON    (  CSM.	FIN_YEAR_NO	          =	MER_REC.	FIN_YEAR_NO AND
             CSM.	FIN_WEEK_NO	          =	MER_REC.	FIN_WEEK_NO )
    WHEN MATCHED THEN
    UPDATE SET
             CSM.	REDEEMED_VOUCHERS	        =	MER_REC.	VOUCHER_TOT	,
             CSM.	REDEEMED_CUSTOMERS	      =	MER_REC.	CUSTOMER_TOT	,
             CSM.	REDEEMED_VALUE	          =	MER_REC.	REDEEMED_AMOUNT	,
             CSM.	REDEEMED_BASKET_VALUE	    =	MER_REC.	TRAN_SELLING	,
             CSM.	REDEEMED_TRANSACTIONS     =	MER_REC.	TRAN_TOT	,
             CSM. LAST_UPDATED_DATE         = G_DATE
;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;



--================================================================================

    l_text := 'ROLLUP MONTH RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 1' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


INSERT  /*+ append */ INTO CUST_MART_VOUCHERS_MONTH CSM
with      CAMPS AS (
SELECT   /*+ parallel(16) */
         CAMPAIGN_NO,
         MAX(CAMPAIGN_START_DATE) CAMPAIGN_START_DATE,
         MAX(CAMPAIGN_END_DATE) CAMPAIGN_END_DATE
FROM     FND_FV_CAMPAIGN_PROMOTION
GROUP BY CAMPAIGN_NO
               ),
      MONTHS AS (
SELECT   /*+ parallel(4) */
         FIN_YEAR_NO,
         FIN_MONTH_NO,
         CAMPAIGN_NO
FROM     CAMPS CMP,
         DIM_CALENDAR_WK
WHERE    THIS_MN_END_DATE   >=  CAMPAIGN_START_DATE
AND      THIS_MN_START_DATE <=  CAMPAIGN_END_DATE
GROUP BY FIN_YEAR_NO,
         FIN_MONTH_NO,
         CAMPAIGN_NO
                ),
      ISSUED AS (
SELECT  /*+ parallel(CNT,16) FULL(CNT) FULL(WEEKS) */
        FIN_YEAR_NO,
        FIN_MONTH_NO,
        PRIMARY_CUSTOMER_IDENTIFIER,
        VOUCHER_NO
FROM    CUST_FV_VOUCHER CNT ,
        MONTHS
WHERE   CNT.CAMPAIGN_NO = MONTHS.CAMPAIGN_NO
AND     FIN_YEAR_NO > 2012
AND     VOUCHER_STATUS_DESCRIPTION IN ( 'Active','Redeemed','Expired')
                 )
SELECT   /*+ parallel(issued,16)  FULL(issued)   */
         ISSUED.FIN_YEAR_NO,
         ISSUED.FIN_MONTH_NO,
         COUNT(DISTINCT VOUCHER_NO) VOUCHER_TOT,
         COUNT(DISTINCT PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
         '','','','',
         G_DATE,''
FROM     ISSUED
GROUP BY ISSUED.FIN_YEAR_NO,
         ISSUED.FIN_MONTH_NO
;


  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

COMMIT;

--================================================================================

    l_text := 'UPDATE STATS ON MART TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','CUST_MART_VOUCHERS_MONTH',estimate_percent=>1, DEGREE => 32);
COMMIT;

--================================================================================

    l_text := 'ROLLUP MONTH RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 2' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE  /*+ PARALLEL(4) */ INTO CUST_MART_VOUCHERS_MONTH CSM
    USING (

    SELECT   /*+ parallel(VC,16) parallel(CB,16) FULL(VC) FULL(CB) FULL(DC) */
             DC.FIN_YEAR_NO,
             DC.FIN_MONTH_NO,
             COUNT(DISTINCT VC.VOUCHER_NO) VOUCHER_TOT,
             COUNT(DISTINCT VC.PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
             COUNT(DISTINCT CB.TRAN_DATE||CB.TRAN_NO||CB.TILL_NO||CB.LOCATION_NO) TRAN_TOT,
             SUM(VC.REDEEMED_AMOUNT) REDEEMED_AMOUNT,
             SUM(CB.TRAN_SELLING) TRAN_SELLING
    FROM     CUST_FV_VOUCHER VC,
             CUST_BASKET CB,
             DIM_CALENDAR DC
    WHERE    VOUCHER_STATUS_DESCRIPTION = 'Redeemed'
    AND      REDEEMED_DATE    = DC.CALENDAR_DATE
    AND      REDEEMED_DATE    = CB.TRAN_DATE
    AND      REDEEMED_TRAN_NO = CB.TRAN_NO
    AND      REDEEMED_TILL_NO = CB.TILL_NO
    AND      REDEEMED_STORE   = CB.LOCATION_NO
    GROUP BY DC.FIN_YEAR_NO,
             DC.FIN_MONTH_NO
                   ) MER_REC
    ON    (  CSM.	FIN_YEAR_NO	          =	MER_REC.	FIN_YEAR_NO AND
             CSM.	FIN_MONTH_NO	        =	MER_REC.	FIN_MONTH_NO )
    WHEN MATCHED THEN
    UPDATE SET
             CSM.	REDEEMED_VOUCHERS	        =	MER_REC.	VOUCHER_TOT	,
             CSM.	REDEEMED_CUSTOMERS	      =	MER_REC.	CUSTOMER_TOT	,
             CSM.	REDEEMED_VALUE	          =	MER_REC.	REDEEMED_AMOUNT	,
             CSM.	REDEEMED_BASKET_VALUE	    =	MER_REC.	TRAN_SELLING	,
             CSM.	REDEEMED_TRANSACTIONS     =	MER_REC.	TRAN_TOT	,
             CSM. LAST_UPDATED_DATE         = G_DATE
;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;



--================================================================================

    l_text := 'ROLLUP YEAR RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 1' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


INSERT  /*+ append */ INTO CUST_MART_VOUCHERS_YEAR CSM
with      CAMPS AS (
SELECT   /*+ parallel(16) */
         CAMPAIGN_NO,
         MAX(CAMPAIGN_START_DATE) CAMPAIGN_START_DATE,
         MAX(CAMPAIGN_END_DATE) CAMPAIGN_END_DATE
FROM     FND_FV_CAMPAIGN_PROMOTION
GROUP BY CAMPAIGN_NO
               ),
      YEARS AS (
SELECT   /*+ parallel(4) */
         FIN_YEAR_NO,
         CAMPAIGN_NO
FROM     CAMPS CMP,
         DIM_CALENDAR_WK
WHERE    THIS_MN_END_DATE   >=  CAMPAIGN_START_DATE
AND      THIS_MN_START_DATE <=  CAMPAIGN_END_DATE
GROUP BY FIN_YEAR_NO,
         CAMPAIGN_NO
                ),
      ISSUED AS (
SELECT  /*+ parallel(CNT,16) FULL(CNT) FULL(WEEKS) */
        FIN_YEAR_NO,
        PRIMARY_CUSTOMER_IDENTIFIER,
        VOUCHER_NO
FROM    CUST_FV_VOUCHER CNT ,
        YEARS
WHERE   CNT.CAMPAIGN_NO = YEARS.CAMPAIGN_NO
AND     FIN_YEAR_NO > 2012
AND     VOUCHER_STATUS_DESCRIPTION IN ( 'Active','Redeemed','Expired')
                 )
SELECT   /*+ parallel(issued,16)  FULL(issued)   */
         ISSUED.FIN_YEAR_NO,
         COUNT(DISTINCT VOUCHER_NO) VOUCHER_TOT,
         COUNT(DISTINCT PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
         '','','','',
         G_DATE,''
FROM     ISSUED
GROUP BY ISSUED.FIN_YEAR_NO
;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

COMMIT;

--================================================================================

    l_text := 'UPDATE STATS ON MART TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','CUST_MART_VOUCHERS_YEAR',estimate_percent=>1, DEGREE => 32);
COMMIT;

--================================================================================

    l_text := 'ROLLUP YEAR RANGE IS:-SINCE THE BEGINNING OF HISTORY PHASE 2' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE  /*+ PARALLEL(4) */ INTO CUST_MART_VOUCHERS_YEAR CSM
    USING (

    SELECT   /*+ parallel(VC,16) parallel(CB,16) FULL(VC) FULL(CB) FULL(DC) */
             DC.FIN_YEAR_NO,
             COUNT(DISTINCT VC.VOUCHER_NO) VOUCHER_TOT,
             COUNT(DISTINCT VC.PRIMARY_CUSTOMER_IDENTIFIER) CUSTOMER_TOT,
             COUNT(DISTINCT CB.TRAN_DATE||CB.TRAN_NO||CB.TILL_NO||CB.LOCATION_NO) TRAN_TOT,
             SUM(VC.REDEEMED_AMOUNT) REDEEMED_AMOUNT,
             SUM(CB.TRAN_SELLING) TRAN_SELLING
    FROM     CUST_FV_VOUCHER VC,
             CUST_BASKET CB,
             DIM_CALENDAR DC
    WHERE    VOUCHER_STATUS_DESCRIPTION = 'Redeemed'
    AND      REDEEMED_DATE    = DC.CALENDAR_DATE
    AND      REDEEMED_DATE    = CB.TRAN_DATE
    AND      REDEEMED_TRAN_NO = CB.TRAN_NO
    AND      REDEEMED_TILL_NO = CB.TILL_NO
    AND      REDEEMED_STORE   = CB.LOCATION_NO
    GROUP BY DC.FIN_YEAR_NO
                   ) MER_REC
    ON    (  CSM.	FIN_YEAR_NO	          =	MER_REC.	FIN_YEAR_NO )
    WHEN MATCHED THEN
    UPDATE SET
             CSM.	REDEEMED_VOUCHERS	        =	MER_REC.	VOUCHER_TOT	,
             CSM.	REDEEMED_CUSTOMERS	      =	MER_REC.	CUSTOMER_TOT	,
             CSM.	REDEEMED_VALUE	          =	MER_REC.	REDEEMED_AMOUNT	,
             CSM.	REDEEMED_BASKET_VALUE	    =	MER_REC.	TRAN_SELLING	,
             CSM.	REDEEMED_TRANSACTIONS     =	MER_REC.	TRAN_TOT	,
             CSM. LAST_UPDATED_DATE         = G_DATE
;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    p_success := true;

  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

end wh_prf_cust_351u;
