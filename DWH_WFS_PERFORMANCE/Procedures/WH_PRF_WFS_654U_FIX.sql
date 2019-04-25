--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_654U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_654U_FIX" (p_forall_limit in integer,p_success out boolean, 
   p_date_start IN DATE, p_date_end in Date) as

--**************************************************************************************************
--  Description  WFS Sales Mart - Load Monthly Sales Mart 
--  Date:        2017-05-04
--  Author:      Naresh Chauhan
--  Purpose:     Load Monthly Sales Mart

--               
--                    
--               THIS JOB RUNS DAILY, but will process once a month 
--  Tables:      Input  - 
--                    wfs_mart_sales_basket_item
--                    wfs_mart_sales_cust_acc_mly
--
--               Output - wfs_mart_sales_mly

--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-05-04 N Chauhan - created
--  2017-05-05 N Chauhan - restructured using MERGE in case of re-run/fix
--  2017-05-05 N Chauhan - Merge removed - too slow.  - plain inserts are fine for this load, 
--                         as there are no duplicates in wfs_mart_sales_basket_item.
--                         A fiddle to g_date_start will be needed if resumed after a partial run.
--  2017-05-29 N Chauhan - Tidy up.
--  2017-05-30 N Chauhan - Check for source data availability before load. 
--  2017-06-08 N Chauhan - revise calc for SC_IND and CC_IN, for null cusstomer_no
--  2017-07-31 N Chauhan - fix for scheduled run when new cal month more than 8 days into new fin month. 
--  2018-01-12 N Chauhan - removed "full(t)" hint in max(tran_date) query, to use index.
--  2018-10-04 N Chauhan - create index parallel degree changed from 16 to 8 as per dba request.
--  2019-02-05 N Chauhan - Fix version to process adhoc a range of days specified as parameters.
--
--
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
g_date               date          := trunc(sysdate);

g_year_no            integer       :=  0;
g_month_no           integer       :=  0;



L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_654U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales Mart - Load Monthly Sales Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_job_desc varchar2(200):= 'Load Monthly Sales Mart';
g_success boolean:= TRUE;
g_date_start date;
g_date_end date;
g_date_to_do date;
g_date_done date;

g_run_date date;
g_2nd_of_month date;
g_today date;
l_sql_stmt varchar2(200);
g_source_rows_chk integer :=0;
g_recs_cnt_day   integer   :=  0;
g_yr_mth_cmplt number(6,0);


g_idx_drop_success boolean:= false;
g_idx_existed  boolean:= false;
g_analysed_count integer:=0;
g_analysed_success boolean:= false;

--l_tablespace varchar2(50):= 'STG_STAGING';    /* UAT testing */
l_tablespace varchar2(50):= 'WFS_PRF_MASTER_03';  /* production */



procedure  drop_index( p_index_name in varchar2) as

begin   

    DWH_DBA_WFS.drop_index_if_exists(
         'DWH_WFS_PERFORMANCE',
         p_index_name,
         g_idx_existed,
         g_idx_drop_success );

     if g_idx_drop_success = false then
        l_text :=p_index_name||'  index drop failed';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     else 
        if g_idx_existed = false then
           l_text :=p_index_name||'  index drop skipped as it does not exist';
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        else
        l_text :=p_index_name||'  index dropped';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        end if;
     end if;   

end drop_index;


procedure SALES_MART_LOAD_DAY(p_date_to_do in date, g_success out boolean) as
begin

		insert /*+ APPEND  parallel(sm,4) */ 
      into DWH_WFS_PERFORMANCE.wfs_mart_sales_mly sm (
			CUSTOMER_NO
		  , ID_NUMBER
		  , WFS_CUSTOMER_NO
		  , CUSTOMER_STATUS
		  , AGE
		  , GENDER
		  , WFS_PRODUCT
		  , WFS_ACCOUNT_NO
		  , SC_ACCOUNT_STATUS
		  , SC_CREDIT_LIMIT
		  , SC_CURRENT_BALANCE
		  , SC_OPEN_TO_BUY
		  , SC_DATE_OPENED
		  , SC_DATE_CLOSED
		  , SC_DATE_CHGOFF
		  , SC_DATE_LAST_STATEMENT
		  , SC_SHOPABLE_IND
		  , SC_MOB
		  , DELINQUENCY_CYCLE
		  , STMT_DATE_LAST_STATEMENT
		  , ISC_RISK_CAT_STMT
		  , CUSTOMER_KEY
		  , CC_ACCOUNT_NO
		  , CC_ACCOUNT_STATUS
		  , CC_DATE_OPENED
		  , CC_DATE_CLOSED
		  , CC_MOB
		  , CC_CURRENT_BALANCE
		  , CC_CREDIT_LIMIT
		  , CC_OPEN_TO_BUY
		  , CC_ACCOUNT_STATUS_CLASS
		  , CC_EXCLUDE_IND
		  , CC_SECONDARY_CARD_IND
		  , CC_PRE_CLASS
		  , CC_CLASS
		  , CC_SHOPABLE_IND
		  , SC_IND
		  , CC_IND
		  , FOODS_SEGMENT
		  , NON_FOODS_SEGMENT
		  , LOYALTY_TIERS
		  , LITTLEWORLD_IND
		  , MYSCHOOL_IND
		  , WWDIFFERENCE_IND
		  , WODIFFERENCE_IND
		  , DISCOVERY_IND
		  , TRAN_DATE
		  , TRAN_TIME
		  , LOCATION_NO
		  , TILL_NO
		  , TRAN_NO
		  , ITEM_NO
		  , ITEM_TRAN_QTY
		  , SUBGROUP_NO
		  , ITEM_TRAN_AMT
		  , DISCOUNT_SELLING
		  , TOTAL_BASKET
		  , TRAN_WFS_SC
		  , TRAN_WFS_CC
		  , TRAN_HYBRID_CARD
		  , TRAN_VISA
		  , TRAN_DEBIT_CARD
		  , TRAN_MASTER_CARD
		  , TRAN_DINERS_CARD
		  , TRAN_AMEX
		  , TRAN_CASH
		  , TRAN_WFS_CC_LOYALTY_VOUCHER
		  , TRAN_THRESHOLD_VOUCHER
		  , TRAN_GIFT_CARD
		  , TRAN_BUY_AID
		  , TRAN_UNKNOWN
		  , WW_FIN_YEAR_NO
		  , WW_FIN_MONTH_NO
		  , WW_FIN_WEEK_NO
		  , WW_FIN_DAY_NO
		  , WREWARDS_PROMOTION_NO
		  , WREWARDS_ITEMS_COUNT
		  , WREWARDS_DISCOUNT
		  , WREWARDS_SALES_VALUE
		  , WREWARDS_PERCENTAGE
		  , DIFF_REWARDS_PROMOTION_NO
		  , DIFF_REWARDS_ITEMS_COUNT
		  , DIFF_REWARDS_DISCOUNT
		  , DIFF_REWARDS_SALES_VALUE
		  , DIFF_REWARDS_PERCENTAGE
		  , ONLINE_PURCHASE_IND
		  , LAST_UPDATED_DATE
		  )

-- ***************  BASKET_ITEM ****************


  -- ######### main select ########################################## --

		  select  /*+ parallel(dly,4) full(dly) parallel(mnthly, 4) full(mnthly) parallel(CAL,4) full(CAL) */
			DLY.CUSTOMER_NO
		  , MNTHLY.ID_NUMBER
		  , MNTHLY.WFS_CUSTOMER_NO
		  , MNTHLY.CUSTOMER_STATUS
		  , MNTHLY.AGE
		  , MNTHLY.GENDER
		  , MNTHLY.WFS_PRODUCT
		  , MNTHLY.WFS_ACCOUNT_NO
		  , MNTHLY.SC_ACCOUNT_STATUS
		  , MNTHLY.SC_CREDIT_LIMIT
		  , MNTHLY.SC_CURRENT_BALANCE
		  , MNTHLY.SC_OPEN_TO_BUY
		  , MNTHLY.SC_DATE_OPENED
		  , MNTHLY.SC_DATE_CLOSED
		  , MNTHLY.SC_DATE_CHGOFF
		  , MNTHLY.SC_DATE_LAST_STATEMENT
		  , MNTHLY.SC_SHOPABLE_IND
		  , MNTHLY.SC_MOB
		  , MNTHLY.DELINQUENCY_CYCLE
		  , MNTHLY.STMT_DATE_LAST_STATEMENT
		  , MNTHLY.ISC_RISK_CAT_STMT
		  , MNTHLY.CUSTOMER_KEY
		  , MNTHLY.CC_ACCOUNT_NO
		  , MNTHLY.CC_ACCOUNT_STATUS
		  , MNTHLY.CC_DATE_OPENED
		  , MNTHLY.CC_DATE_CLOSED
		  , MNTHLY.CC_MOB
		  , MNTHLY.CC_CURRENT_BALANCE
		  , MNTHLY.CC_CREDIT_LIMIT
		  , MNTHLY.CC_OPEN_TO_BUY
		  , MNTHLY.CC_ACCOUNT_STATUS_CLASS
		  , MNTHLY.CC_EXCLUDE_IND
		  , MNTHLY.CC_SECONDARY_CARD_IND
		  , MNTHLY.CC_PRE_CLASS
		  , MNTHLY.CC_CLASS
		  , MNTHLY.CC_SHOPABLE_IND
--		  , CASE WHEN mnthly.sc_shopable_ind is not null or dly.tran_wfs_sc is not null then 1 else 0 end as SC_IND
--		  , CASE WHEN MNTHLY.CC_SHOPABLE_IND is not null or dly.tran_wfs_cc is not null or DLY.TRAN_WFS_VISA_LOYALTY_VOUCHER is not null then 1 else 0 end as CC_IND

--, CASE WHEN mnthly.sc_shopable_ind is not null or max(case when dly.tran_wfs_sc > 0 then 1 else 0 end) over (partition by dly.customer_no) = 1 then 1 else 0 end as SC_IND
      , CASE
        when dly.customer_no in (0, 99999999999999) then
         0
        WHEN mnthly.sc_shopable_ind is not null or max(case when dly.tran_wfs_sc > 0 then 1 else 0 end) over (partition by dly.customer_no) = 1 then 
         1 
        else 
         0 
        end as SC_IND

--, CASE WHEN MNTHLY.CC_SHOPABLE_IND is not null or max(case when dly.tran_wfs_cc > 0 or  dly.tran_wfs_visa_loyalty_voucher > 0 then 1 else 0 end) over (partition by dly.customer_no) = 1 then 1 else 0 end as CC_IND
      , CASE
        when dly.customer_no in (0, 99999999999999) then
         0
        WHEN MNTHLY.CC_SHOPABLE_IND is not null or max(case when dly.tran_wfs_cc > 0 or  dly.tran_wfs_visa_loyalty_voucher > 0 then 1 else 0 end) over (partition by dly.customer_no) = 1 then 
         1 
        else 
         0 
        end as CC_IND

		  , MNTHLY.FOODS_SEGMENT
		  , MNTHLY.NON_FOODS_SEGMENT
		  , MNTHLY.LOYALTY_TIERS
		  , MNTHLY.LITTLEWORLD_IND
		  , MNTHLY.MYSCHOOL_IND
		  , MNTHLY.WWDIFFERENCE_IND
		  , MNTHLY.WODIFFERENCE_IND
		  , MNTHLY.DISCOVERY_IND
		  , DLY.TRAN_DATE
		  , DLY.TRAN_TIME
		  , DLY.LOCATION_NO
		  , DLY.TILL_NO
		  , DLY.TRAN_NO
		  , DLY.ITEM_NO
		  , DLY.ITEM_TRAN_QTY
		  , DLY.SUBGROUP_NO
		  , DLY.ITEM_TRAN_AMT
		  , DLY.DISCOUNT_SELLING
		  , DLY.TOTAL_BASKET
		  , DLY.TRAN_WFS_SC
		  , DLY.TRAN_WFS_CC
		  , DLY.TRAN_HYBRID_CARD
		  , DLY.TRAN_VISA
		  , DLY.TRAN_DEBIT_CARD
		  , DLY.TRAN_MASTER_CARD
		  , DLY.TRAN_DINERS_CARD
		  , DLY.TRAN_AMEX
		  , DLY.TRAN_CASH
		  , DLY.TRAN_WFS_VISA_LOYALTY_VOUCHER as TRAN_WFS_CC_LOYALTY_VOUCHER
		  , DLY.TRAN_THRESHOLD_VOUCHER
		  , DLY.TRAN_GIFT_CARD
		  , DLY.TRAN_BUY_AID
		  , DLY.TRAN_UNKNOWN
		  , CAL.FIN_YEAR_NO AS WW_FIN_YEAR_NO
		  , CAL.FIN_MONTH_NO AS WW_FIN_MONTH_NO
		  , CAL.FIN_WEEK_NO AS WW_FIN_WEEK_NO
		  , CAL.FIN_DAY_NO AS WW_FIN_DAY_NO
		  , DLY.WREWARDS_PROMOTION_NO
		  , DLY.WREWARDS_ITEMS_COUNT
		  , DLY.WREWARDS_DISCOUNT
		  , DLY.WREWARDS_SALES_VALUE
		  , DLY.WREWARDS_PERCENTAGE
		  , DLY.DIFF_REWARDS_PROMOTION_NO
		  , DLY.DIFF_REWARDS_ITEMS_COUNT
		  , DLY.DIFF_REWARDS_DISCOUNT
		  , DLY.DIFF_REWARDS_SALES_VALUE
		  , DLY.DIFF_REWARDS_PERCENTAGE
		  , CASE DLY.TILL_NO WHEN 999 THEN 1 ELSE 0 END as ONLINE_IND
		  , TRUNC(SYSDATE)
		FROM
			DWH_WFS_PERFORMANCE.WFS_MART_SALES_BASKET_ITEM DLY
			inner join dim_calendar CAL
				on dly.tran_date = CAL.calendar_date
			LEFT JOIN DWH_WFS_PERFORMANCE.wfs_mart_sales_cust_acc_mly MNTHLY
			  ON DLY.CUSTOMER_NO = MNTHLY.CUSTOMER_NO and mnthly.CAL_YEAR_MONTH_NO=to_number(to_char(p_date_to_do,'YYYYMM'))
		WHERE
			DLY.TRAN_DATE = p_date_to_do
			and DLY.TOTAL_BASKET is NOT null;

   g_success := true;


exception

   when others then
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||
--            'SALES_MART_LOAD_DAY Error '||sqlcode||' '||sqlerrm );

      l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'sub proc SALES_MART_LOAD_DAY Error '||sqlcode||' '||sqlerrm;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    rollback;
    g_success := false;
    raise;

end SALES_MART_LOAD_DAY;




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

    l_text := 'WFS SALES Mart load STARTED AT '||
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
    l_text := 'LOAD TABLE: '||'wfs_mart_sales_mly' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';


--************************************************************************************************** 
-- DETERMINE WHEN JOB RUNS   
--**************************************************************************************************     
--   
--
--    g_today:= trunc(sysdate);
--
--    select last_yr_fin_year_no, last_mn_fin_month_no
--      into g_year_no, g_month_no
--    from dim_control;
--
--    select   max(fin_week_end_date),  -- last day of completed fin month
--             max(CAL_YEAR_MONTH_NO)
--      into     g_run_date, g_yr_mth_cmplt
--    from     dim_calendar
--    where    fin_year_no  =  g_year_no  and
--             fin_month_no =  g_month_no;
--             
--
--    g_run_date := g_run_date + 1;  -- 1st day of the new fin month
--    g_run_date := g_run_date + 7;  -- 8th day of the new fin month
--   -- to run  after the load of final monthly load of CUST_LSS_LIFESTYLE_SEGMENTS (WH_PRF_CUST_284U)
--   
--    g_2nd_of_month:= add_months(to_date(to_char(g_yr_mth_cmplt)||'02', 'YYYYMMDD'),1);
--   
--    if g_run_date < g_2nd_of_month then
--       g_run_date:= g_2nd_of_month;
--       -- the LATER of the 8th Financial day of the month, or the 2nd day 
--       -- we allow at least 2 days to ensure that all transactions from all stores are available
--    end if;
--
----/* temp override fiddle to force a load */   g_run_date:= g_today;
--   
--    if g_today <> g_run_date then
--       l_text      := 'This job only runs on '||to_char(g_run_date,'DD/Mon/YYYY')
--                      ||' and today '||to_char(g_today,'DD/Mon/YYYY')||' is not that day !';
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
----dbms_output.put_line( l_text );
--       p_success := true;
--       return;
--    end if;  
--    
--    l_text      := 'This job only runs on '||to_char(g_run_date,'DD/Mon/YYYY')
--                   ||' and today '||to_char(g_today,'DD/Mon/YYYY')||' is that day !  busy..';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
----dbms_output.put_line( l_text ); 
-- 
--    -- determine period
--
--    g_date_start:= to_date(to_char(g_yr_mth_cmplt)||'01', 'YYYYMMDD');
--    -- first day of last completed calendar month
--
----/***TEMP override to resume from some other day: */   g_date_start:= to_date(to_char(add_months(g_today,-1),'YYYYMM')||'06', 'YYYYMMDD' );
--
--
--    g_date_end:=add_months(to_date(to_char(g_yr_mth_cmplt)||'01', 'YYYYMMDD'),1) -1 ;
--     -- last day of last completed calendar month
-- 
--    l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Starting load for period '
--              ||to_char(g_date_start, 'YYYY-MM-DD')||' - '||to_char(g_date_end,'YYYY-MM-DD');
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
----dbms_output.put_line( l_text );
-- 
--
--    -- check if source data available
--    
--    select /*+ parallel(t,4) full(t) */
--     count(*) cnt into g_source_rows_chk
--    from  WFS_MART_SALES_CUST_ACC_MLY    t
--    where CAL_YEAR_MONTH_NO = to_number(to_char(g_date_start,'YYYYMM') );
-- 
--    if g_source_rows_chk < 1000000 then
--       l_text      := 'Incomplete data in WFS_MART_SALES_CUST_ACC_MLY. Load abandoned.'; 
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
--                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
--       p_success := true;
--       return;
--    end if;  
--

    -- Drop indexes -------------------

    drop_index('I10_WFS_MART_SALES_MLY');
    drop_index('I20_WFS_MART_SALES_MLY');



--**************************************************************************************************
-- Main loop
--**************************************************************************************************

--    -- skip days already done
--    
--   
--    select /*+ parallel(t,4) */
--      max(tran_date) into g_date_done
--    from   WFS_MART_SALES_MLY   t
--    where tran_date > sysdate - 90; -- limit scan for performance
--
--
--    if g_date_done > g_date_start then
--       g_date_start := g_date_done + 1;
--    end if;


-- initialise with parameter values
g_date_start:= p_date_start;
g_date_end:= p_date_end;


    g_date_to_do := g_date_start;

    while g_date_to_do <= g_date_end and g_success = TRUE
    loop


       -- ****** main load *************
       SALES_MART_LOAD_DAY(g_date_to_do, g_success);
       -- ******************************

       g_recs_cnt_day:=SQL%ROWCOUNT;
       l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Inserted:  '||g_recs_cnt_day;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


       g_recs_read     :=  g_recs_read + g_recs_cnt_day;
       g_recs_inserted :=  g_recs_inserted + g_recs_cnt_day;

       commit;  -- NB. write_log already does a commit !

       g_date_to_do := g_date_to_do +1;

    end loop;



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



    if g_success then
       -- only recreate indexes if load was successful\
       -- avoid wasting time creating index when load is incomplete and indexes have to be dropped again


--**************************************************************************************************
-- Retention maintenance    
--**************************************************************************************************

     -- excluded for now due to reservations about partitions deletions by DWH team





--**************************************************************************************************
-- restore indexes 
--**************************************************************************************************



--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Re-creating Index (Customer_no)...' );
      l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Restoring Index I10_WFS_MART_SALES_MLY (CUSTOMER_NO)...' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
      execute immediate 'CREATE INDEX '||
        '"DWH_WFS_PERFORMANCE"."I10_WFS_MART_SALES_MLY" ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_MLY" '||
        '("CUSTOMER_NO") '|| 
        'NOlogging TABLESPACE "'||l_tablespace||'" PARALLEL (degree 8)';
      
      execute immediate 'ALTER INDEX '||
        '"DWH_WFS_PERFORMANCE"."I10_WFS_MART_SALES_MLY" LOGGING NOPARALLEL ';



      l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Restoring Index I20_WFS_MART_SALES_MLY (ITEM_NO)...' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
      execute immediate 'CREATE INDEX '||
        '"DWH_WFS_PERFORMANCE"."I20_WFS_MART_SALES_MLY" ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_MLY" '||
        '("ITEM_NO") '|| 
        'NOlogging TABLESPACE "'||l_tablespace||'"  PARALLEL (degree 8)';
      
      execute immediate 'ALTER INDEX '||
        '"DWH_WFS_PERFORMANCE"."I20_WFS_MART_SALES_MLY" LOGGING NOPARALLEL ';

      l_text := 'indexes I10_WFS_MART_SALES_MLY, I20_WFS_MART_SALES_MLY created';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 


--**************************************************************************************************
-- gather statistics
--**************************************************************************************************

-- skip gather stats  - let DBA maintenance do this overnight.
/*
      l_text := 'gathering statistics ...';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

--     DBMS_STATS.SET_TABLE_PREFS('DWH_WFS_PERFORMANCE','WFS_MART_SALES_MLY','INCREMENTAL','TRUE');  
--     done by dba, need only do once

    -- analyse all unanalysed partitions, one partition at a time
     DWH_DBA_WFS.stats_partitions_outstanding (
          'DWH_WFS_PERFORMANCE',
          'WFS_MART_SALES_MLY',
          g_analysed_count,
          g_analysed_success );

      if g_analysed_success = false then
         l_text := 'gather_table_stats failed';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
      else 
         l_text := 'gather_table_stats : '||g_analysed_count||' partitions analysed' ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
      end if;   
*/

      p_success := true;
      commit;


--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    else
      rollback;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');
      l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
                || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      p_success := false;

    end if;

    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


exception

    when dwh_errors.e_insert_error then
       rollback;
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

    when others then
       rollback;
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;



end wh_prf_wfs_654u_FIX;
