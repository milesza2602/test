--------------------------------------------------------
--  DDL for Procedure TMP_WFS_MART_SALES_CPY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."TMP_WFS_MART_SALES_CPY" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Sales/Rewards take-on data COPY
--  Date:        2017-03-22
--  Author:      Naresh Chauhan
--  Purpose:     Proc to enable SALES mart data copy 'offline'
--                from TMP_.. tables in W7071603 schema
--                    
--  Tables:      Input  - 
--               Output - 
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-03-22 N Chauhan - created - based on WH_PRF_CUST_322U
--  2016-03-23 N Chauhan - fixed table name uppercase for index create.


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
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'TMP_WFS_MART_SALES_CPY';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales/Rewards Marts takeon data copy';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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


    l_text := 'WFS Sales Mart data copy STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';


    l_text :=  'Dropping Indexes I10_WFS_MART_SALES_MLY, I20_WFS_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE immediate('DROP INDEX DWH_WFS_PERFORMANCE.I10_WFS_MART_SALES_MLY');
    EXECUTE immediate('DROP INDEX DWH_WFS_PERFORMANCE.I20_WFS_MART_SALES_MLY');
    

    l_text :=  'Copying data to WFS_MART_SALES_MLY ex TMP_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    insert /*+ APPEND parallel(s,4) */
    into DWH_WFS_PERFORMANCE.WFS_MART_SALES_MLY s
    (TRAN_DATE
    ,LOCATION_NO
    ,TILL_NO
    ,TRAN_NO
    ,ITEM_NO
    ,TRAN_TIME
    ,ITEM_TRAN_QTY
    ,SUBGROUP_NO
    ,ONLINE_PURCHASE_IND
    ,CUSTOMER_NO
    ,ITEM_TRAN_AMT
    ,DISCOUNT_SELLING
    ,TOTAL_BASKET
    ,TRAN_WFS_SC
    ,TRAN_WFS_CC
    ,TRAN_HYBRID_CARD
    ,TRAN_VISA
    ,TRAN_DEBIT_CARD
    ,TRAN_MASTER_CARD
    ,TRAN_DINERS_CARD
    ,TRAN_AMEX
    ,TRAN_CASH
    ,TRAN_WFS_CC_LOYALTY_VOUCHER
    ,TRAN_THRESHOLD_VOUCHER
    ,TRAN_GIFT_CARD
    ,TRAN_BUY_AID
    ,TRAN_UNKNOWN
    ,WW_FIN_YEAR_NO
    ,WW_FIN_MONTH_NO
    ,WW_FIN_WEEK_NO
    ,WW_FIN_DAY_NO
    ,ID_NUMBER
    ,WFS_CUSTOMER_NO
    ,CUSTOMER_STATUS
    ,AGE
    ,GENDER
    ,WFS_PRODUCT
    ,WFS_ACCOUNT_NO
    ,SC_ACCOUNT_STATUS
    ,SC_CREDIT_LIMIT
    ,SC_CURRENT_BALANCE
    ,SC_OPEN_TO_BUY
    ,SC_DATE_OPENED
    ,SC_DATE_CLOSED
    ,SC_DATE_CHGOFF
    ,SC_DATE_LAST_STATEMENT
    ,SC_SHOPABLE_IND
    ,SC_MOB
    ,DELINQUENCY_CYCLE
    ,STMT_DATE_LAST_STATEMENT
    ,ISC_RISK_CAT_STMT
    ,CUSTOMER_KEY
    ,CC_ACCOUNT_NO
    ,CC_ACCOUNT_STATUS
    ,CC_SHOPABLE_IND
    ,CC_DATE_OPENED
    ,CC_DATE_CLOSED
    ,CC_MOB
    ,CC_CURRENT_BALANCE
    ,CC_CREDIT_LIMIT
    ,CC_OPEN_TO_BUY
    ,CC_ACCOUNT_STATUS_CLASS
    ,CC_EXCLUDE_IND
    ,CC_SECONDARY_CARD_IND
    ,CC_PRE_CLASS
    ,CC_CLASS
    ,SC_IND
    ,CC_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,LOYALTY_TIERS
    ,LITTLEWORLD_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,WODIFFERENCE_IND
    ,DISCOVERY_IND
    ,WREWARDS_PROMOTION_NO
    ,WREWARDS_ITEMS_COUNT
    ,WREWARDS_DISCOUNT
    ,WREWARDS_SALES_VALUE
    ,WREWARDS_PERCENTAGE
    ,DIFF_REWARDS_PROMOTION_NO
    ,DIFF_REWARDS_ITEMS_COUNT
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_PERCENTAGE
    ,LAST_UPDATED_DATE
    )
    
    select  /*+ parallel(t,4) full(t) */
    
     TRAN_DATE
    ,LOCATION_NO
    ,TILL_NO
    ,TRAN_NO
    ,ITEM_NO
    ,TRAN_TIME
    ,ITEM_TRAN_QTY
    ,SUBGROUP_NO
    ,ONLINE_PURCHASE_IND
    ,CUSTOMER_NO
    ,ITEM_TRAN_AMT
    ,DISCOUNT_SELLING
    ,TOTAL_BASKET
    ,TRAN_WFS_SC
    ,TRAN_WFS_CC
    ,TRAN_HYBRID_CARD
    ,TRAN_VISA
    ,TRAN_DEBIT_CARD
    ,TRAN_MASTER_CARD
    ,TRAN_DINERS_CARD
    ,TRAN_AMEX
    ,TRAN_CASH
    ,TRAN_WFS_CC_LOYALTY_VOUCHER
    ,TRAN_THRESHOLD_VOUCHER
    ,TRAN_GIFT_CARD
    ,TRAN_BUY_AID
    ,TRAN_UNKNOWN
    ,WW_FIN_YEAR_NO
    ,WW_FIN_MONTH_NO
    ,WW_FIN_WEEK_NO
    ,WW_FIN_DAY_NO
    ,ID_NUMBER
    ,WFS_CUSTOMER_NO
    ,CUSTOMER_STATUS
    ,AGE
    ,GENDER
    ,WFS_PRODUCT
    ,WFS_ACCOUNT_NO
    ,SC_ACCOUNT_STATUS
    ,SC_CREDIT_LIMIT
    ,SC_CURRENT_BALANCE
    ,SC_OPEN_TO_BUY
    ,SC_DATE_OPENED
    ,SC_DATE_CLOSED
    ,SC_DATE_CHGOFF
    ,SC_DATE_LAST_STATEMENT
    ,SC_SHOPABLE_IND
    ,SC_MOB
    ,DELINQUENCY_CYCLE
    ,STMT_DATE_LAST_STATEMENT
    ,ISC_RISK_CAT_STMT
    ,CUSTOMER_KEY
    ,CC_ACCOUNT_NO
    ,CC_ACCOUNT_STATUS
    ,CC_SHOPABLE_IND
    ,CC_DATE_OPENED
    ,CC_DATE_CLOSED
    ,CC_MOB
    ,CC_CURRENT_BALANCE
    ,CC_CREDIT_LIMIT
    ,CC_OPEN_TO_BUY
    ,CC_ACCOUNT_STATUS_CLASS
    ,CC_EXCLUDE_IND
    ,CC_SECONDARY_CARD_IND
    ,CC_PRE_CLASS
    ,CC_CLASS
    ,SC_IND
    ,CC_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,LOYALTY_TIERS
    ,LITTLEWORLD_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,WODIFFERENCE_IND
    ,DISCOVERY_IND
    ,WREWARDS_PROMOTION_NO
    ,WREWARDS_ITEMS_COUNT
    ,WREWARDS_DISCOUNT
    ,WREWARDS_SALES_VALUE
    ,WREWARDS_PERCENTAGE
    ,DIFF_REWARDS_PROMOTION_NO
    ,DIFF_REWARDS_ITEMS_COUNT
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_PERCENTAGE
    ,LAST_UPDATED_DATE
    from w7071603.TMP_MART_SALES_MLY t;
    
    g_recs_inserted :=  SQL%ROWCOUNT;
    
    commit;
    
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    /* ============================================================================*/
    
    
    l_text :=  'Copying data to WFS_MART_SALES_MLY ex TMP_MART_SALES_MLY_2';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    
    insert /*+ APPEND parallel(s,4) */
    into DWH_WFS_PERFORMANCE.WFS_MART_SALES_MLY s
    (TRAN_DATE
    ,LOCATION_NO
    ,TILL_NO
    ,TRAN_NO
    ,ITEM_NO
    ,TRAN_TIME
    ,ITEM_TRAN_QTY
    ,SUBGROUP_NO
    ,ONLINE_PURCHASE_IND
    ,CUSTOMER_NO
    ,ITEM_TRAN_AMT
    ,DISCOUNT_SELLING
    ,TOTAL_BASKET
    ,TRAN_WFS_SC
    ,TRAN_WFS_CC
    ,TRAN_HYBRID_CARD
    ,TRAN_VISA
    ,TRAN_DEBIT_CARD
    ,TRAN_MASTER_CARD
    ,TRAN_DINERS_CARD
    ,TRAN_AMEX
    ,TRAN_CASH
    ,TRAN_WFS_CC_LOYALTY_VOUCHER
    ,TRAN_THRESHOLD_VOUCHER
    ,TRAN_GIFT_CARD
    ,TRAN_BUY_AID
    ,TRAN_UNKNOWN
    ,WW_FIN_YEAR_NO
    ,WW_FIN_MONTH_NO
    ,WW_FIN_WEEK_NO
    ,WW_FIN_DAY_NO
    ,ID_NUMBER
    ,WFS_CUSTOMER_NO
    ,CUSTOMER_STATUS
    ,AGE
    ,GENDER
    ,WFS_PRODUCT
    ,WFS_ACCOUNT_NO
    ,SC_ACCOUNT_STATUS
    ,SC_CREDIT_LIMIT
    ,SC_CURRENT_BALANCE
    ,SC_OPEN_TO_BUY
    ,SC_DATE_OPENED
    ,SC_DATE_CLOSED
    ,SC_DATE_CHGOFF
    ,SC_DATE_LAST_STATEMENT
    ,SC_SHOPABLE_IND
    ,SC_MOB
    ,DELINQUENCY_CYCLE
    ,STMT_DATE_LAST_STATEMENT
    ,ISC_RISK_CAT_STMT
    ,CUSTOMER_KEY
    ,CC_ACCOUNT_NO
    ,CC_ACCOUNT_STATUS
    ,CC_SHOPABLE_IND
    ,CC_DATE_OPENED
    ,CC_DATE_CLOSED
    ,CC_MOB
    ,CC_CURRENT_BALANCE
    ,CC_CREDIT_LIMIT
    ,CC_OPEN_TO_BUY
    ,CC_ACCOUNT_STATUS_CLASS
    ,CC_EXCLUDE_IND
    ,CC_SECONDARY_CARD_IND
    ,CC_PRE_CLASS
    ,CC_CLASS
    ,SC_IND
    ,CC_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,LOYALTY_TIERS
    ,LITTLEWORLD_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,WODIFFERENCE_IND
    ,DISCOVERY_IND
    ,WREWARDS_PROMOTION_NO
    ,WREWARDS_ITEMS_COUNT
    ,WREWARDS_DISCOUNT
    ,WREWARDS_SALES_VALUE
    ,WREWARDS_PERCENTAGE
    ,DIFF_REWARDS_PROMOTION_NO
    ,DIFF_REWARDS_ITEMS_COUNT
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_PERCENTAGE
    ,LAST_UPDATED_DATE
    )
    
    select  /*+ parallel(t,4) full(t) */
    
     TRAN_DATE
    ,LOCATION_NO
    ,TILL_NO
    ,TRAN_NO
    ,ITEM_NO
    ,TRAN_TIME
    ,ITEM_TRAN_QTY
    ,SUBGROUP_NO
    ,ONLINE_PURCHASE_IND
    ,CUSTOMER_NO
    ,ITEM_TRAN_AMT
    ,DISCOUNT_SELLING
    ,TOTAL_BASKET
    ,TRAN_WFS_SC
    ,TRAN_WFS_CC
    ,TRAN_HYBRID_CARD
    ,TRAN_VISA
    ,TRAN_DEBIT_CARD
    ,TRAN_MASTER_CARD
    ,TRAN_DINERS_CARD
    ,TRAN_AMEX
    ,TRAN_CASH
    ,TRAN_WFS_CC_LOYALTY_VOUCHER
    ,TRAN_THRESHOLD_VOUCHER
    ,TRAN_GIFT_CARD
    ,TRAN_BUY_AID
    ,TRAN_UNKNOWN
    ,WW_FIN_YEAR_NO
    ,WW_FIN_MONTH_NO
    ,WW_FIN_WEEK_NO
    ,WW_FIN_DAY_NO
    ,ID_NUMBER
    ,WFS_CUSTOMER_NO
    ,CUSTOMER_STATUS
    ,AGE
    ,GENDER
    ,WFS_PRODUCT
    ,WFS_ACCOUNT_NO
    ,SC_ACCOUNT_STATUS
    ,SC_CREDIT_LIMIT
    ,SC_CURRENT_BALANCE
    ,SC_OPEN_TO_BUY
    ,SC_DATE_OPENED
    ,SC_DATE_CLOSED
    ,SC_DATE_CHGOFF
    ,SC_DATE_LAST_STATEMENT
    ,SC_SHOPABLE_IND
    ,SC_MOB
    ,DELINQUENCY_CYCLE
    ,STMT_DATE_LAST_STATEMENT
    ,ISC_RISK_CAT_STMT
    ,CUSTOMER_KEY
    ,CC_ACCOUNT_NO
    ,CC_ACCOUNT_STATUS
    ,CC_SHOPABLE_IND
    ,CC_DATE_OPENED
    ,CC_DATE_CLOSED
    ,CC_MOB
    ,CC_CURRENT_BALANCE
    ,CC_CREDIT_LIMIT
    ,CC_OPEN_TO_BUY
    ,CC_ACCOUNT_STATUS_CLASS
    ,CC_EXCLUDE_IND
    ,CC_SECONDARY_CARD_IND
    ,CC_PRE_CLASS
    ,CC_CLASS
    ,SC_IND
    ,CC_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,LOYALTY_TIERS
    ,LITTLEWORLD_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,WODIFFERENCE_IND
    ,DISCOVERY_IND
    ,WREWARDS_PROMOTION_NO
    ,WREWARDS_ITEMS_COUNT
    ,WREWARDS_DISCOUNT
    ,WREWARDS_SALES_VALUE
    ,WREWARDS_PERCENTAGE
    ,DIFF_REWARDS_PROMOTION_NO
    ,DIFF_REWARDS_ITEMS_COUNT
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_PERCENTAGE
    ,LAST_UPDATED_DATE
    from w7071603.TMP_MART_SALES_MLY_2 t;
    
    g_recs_inserted :=  SQL%ROWCOUNT;
    
    commit;
    
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    /* ============================================================================*/
    
    
    l_text :=  'Restoring Index I10_WFS_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE immediate('CREATE INDEX DWH_WFS_PERFORMANCE.I10_WFS_MART_SALES_MLY ON '||
           'DWH_WFS_PERFORMANCE.WFS_MART_SALES_MLY (CUSTOMER_NO)NOlogging '||
           'TABLESPACE WFS_PRF_MASTER_03 PARALLEL (degree 16)');
    
    EXECUTE immediate('ALTER INDEX DWH_WFS_PERFORMANCE.I10_WFS_MART_SALES_MLY LOGGING NOPARALLEL');

    
    l_text :=  'Restoring Index I20_WFS_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    EXECUTE immediate('CREATE INDEX DWH_WFS_PERFORMANCE.I20_WFS_MART_SALES_MLY ON '||
           'DWH_WFS_PERFORMANCE.WFS_MART_SALES_MLY (ITEM_NO)NOlogging '||
           'TABLESPACE WFS_PRF_MASTER_03 PARALLEL (degree 16)');
    
    EXECUTE immediate('ALTER INDEX DWH_WFS_PERFORMANCE.I20_WFS_MART_SALES_MLY LOGGING NOPARALLEL');



    
    /* ============================================================================*/
    
    
    l_text :=  'Copying data to WFS_MART_SALES_RWDS_BU_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    
    insert /*+ APPEND parallel(b,4) */
    into DWH_WFS_PERFORMANCE.WFS_MART_SALES_RWDS_BU_MLY b
    (CAL_YEAR_MONTH_NO
    ,WFS_PRODUCT_IND
    ,BUSINESS_UNIT_NO
    ,CC_COLOUR
    ,ONLINE_PURCHASE_IND
    ,BUSINESS_UNIT_NAME
    ,CUSTOMERS_UNIQUE_ALL
    ,CUST_UNIQUE_WREWARDS
    ,CUST_UNIQUE_WREWARDS_SC
    ,CUST_UNIQUE_WREWARDS_CC
    ,CUST_UNIQUE_WREWARDS_VCHR
    ,CUST_UNIQUE_DIFF_REWARDS
    ,CUST_UNIQUE_DIFF_REWARDS_SC
    ,CUST_UNIQUE_DIFF_REWARDS_CC
    ,CUST_UNIQUE_DIFF_REWARDS_VCHR
    ,CUST_UNIQUE_PAID_BY_SC
    ,CUST_UNIQUE_PAID_BY_CC
    ,TRAN_ALL
    ,TRAN_WREWARDS_ITEMS_10
    ,TRAN_WREWARDS_ITEMS_20
    ,TRAN_DIFF_REWARDS_ITEMS
    ,ITEM_SALES_GROSS_AMOUNT
    ,ITEM_DISCOUNT_AMOUNT
    ,BASKETS_UNIQUE_ALL
    ,BASKETS_UNIQUE_PAID_BY_SC
    ,BASKETS_UNIQUE_PAID_BY_CC
    ,BASKETS_UNIQUE_WREWARDS
    ,BASKETS_UNIQUE_WREWARDS_SC
    ,BASKETS_UNIQUE_WREWARDS_CC
    ,BASKETS_UNIQUE_WREWARDS_VCHR
    ,BASKETS_UNIQUE_DIFF_REWARDS
    ,BASKETS_UNIQUE_DIFF_REWARDS_SC
    ,BASKETS_UNIQUE_DIFF_REWARDS_CC
    ,BASKETS_UNIQUE_DIFF_RWDS_VCHR
    ,WREWARDS_SALES_VALUE_10
    ,WREWARDS_DISCOUNT_10
    ,WREWARDS_SALES_VALUE_20
    ,WREWARDS_DISCOUNT_20
    ,WRWDS_DISCNT_WHEN_DIFF_RWDS
    ,WRWDS_EARNED_SUM_TRN_WFS_SC
    ,WRWDS_EARNED_SUM_TRN_WFS_CC
    ,WRWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_EARNED_SUM_TRN_SC
    ,DIFF_RWDS_EARNED_SUM_TRN_CC
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_RWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_SALES_SC_SUM_VALUE
    ,DIFF_RWDS_SALES_SC_SUM_TRN
    ,DIFF_RWDS_SALES_CC_SUM_VALUE
    ,DIFF_RWDS_SALES_CC_SUM_TRN
    ,DIFF_RWDS_DISCNT_PD_BY_SC
    ,DIFF_RWDS_DISCNT_PD_BY_CC
    ,DIFF_RWDS_DISCNT_PD_BY_GC
    ,DIFF_RWDS_DISCNT_PD_BY_CCLV
    ,DIFF_RWDS_DISCNT_PD_BY_THRV
    ,WFS_SC
    ,WFS_CC
    ,VISA
    ,DEBIT
    ,CASH
    ,MASTER
    ,HYBRID
    ,DINERS
    ,AMEX
    ,UNKNOWN
    ,BUYAID
    ,GIFT
    ,VOUCHERS
    ,THRESHOLD
    ,WFS_SPEND
    ,OTHER_SPEND
    ,VOUCHER_SPEND
    ,TOTAL_SPEND
    ,LAST_UPDATED_DATE
    )
    
    select  /*+ parallel(t,4)  full(t) */
    
     CAL_YEAR_MONTH_NO
    ,WFS_PRODUCT_IND
    ,BUSINESS_UNIT_NO
    ,CC_COLOUR
    ,ONLINE_PURCHASE_IND
    ,BUSINESS_UNIT_NAME
    ,CUSTOMERS_UNIQUE_ALL
    ,CUST_UNIQUE_WREWARDS
    ,CUST_UNIQUE_WREWARDS_SC
    ,CUST_UNIQUE_WREWARDS_CC
    ,CUST_UNIQUE_WREWARDS_VCHR
    ,CUST_UNIQUE_DIFF_REWARDS
    ,CUST_UNIQUE_DIFF_REWARDS_SC
    ,CUST_UNIQUE_DIFF_REWARDS_CC
    ,CUST_UNIQUE_DIFF_REWARDS_VCHR
    ,CUST_UNIQUE_PAID_BY_SC
    ,CUST_UNIQUE_PAID_BY_CC
    ,TRAN_ALL
    ,TRAN_WREWARDS_ITEMS_10
    ,TRAN_WREWARDS_ITEMS_20
    ,TRAN_DIFF_REWARDS_ITEMS
    ,ITEM_SALES_GROSS_AMOUNT
    ,ITEM_DISCOUNT_AMOUNT
    ,BASKETS_UNIQUE_ALL
    ,BASKETS_UNIQUE_PAID_BY_SC
    ,BASKETS_UNIQUE_PAID_BY_CC
    ,BASKETS_UNIQUE_WREWARDS
    ,BASKETS_UNIQUE_WREWARDS_SC
    ,BASKETS_UNIQUE_WREWARDS_CC
    ,BASKETS_UNIQUE_WREWARDS_VCHR
    ,BASKETS_UNIQUE_DIFF_REWARDS
    ,BASKETS_UNIQUE_DIFF_REWARDS_SC
    ,BASKETS_UNIQUE_DIFF_REWARDS_CC
    ,BASKETS_UNIQUE_DIFF_RWDS_VCHR
    ,WREWARDS_SALES_VALUE_10
    ,WREWARDS_DISCOUNT_10
    ,WREWARDS_SALES_VALUE_20
    ,WREWARDS_DISCOUNT_20
    ,WRWDS_DISCNT_WHEN_DIFF_RWDS
    ,WRWDS_EARNED_SUM_TRN_WFS_SC
    ,WRWDS_EARNED_SUM_TRN_WFS_CC
    ,WRWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_EARNED_SUM_TRN_SC
    ,DIFF_RWDS_EARNED_SUM_TRN_CC
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_RWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_SALES_SC_SUM_VALUE
    ,DIFF_RWDS_SALES_SC_SUM_TRN
    ,DIFF_RWDS_SALES_CC_SUM_VALUE
    ,DIFF_RWDS_SALES_CC_SUM_TRN
    ,DIFF_RWDS_DISCNT_PD_BY_SC
    ,DIFF_RWDS_DISCNT_PD_BY_CC
    ,DIFF_RWDS_DISCNT_PD_BY_GC
    ,DIFF_RWDS_DISCNT_PD_BY_CCLV
    ,DIFF_RWDS_DISCNT_PD_BY_THRV
    ,WFS_SC
    ,WFS_CC
    ,VISA
    ,DEBIT
    ,CASH
    ,MASTER
    ,HYBRID
    ,DINERS
    ,AMEX
    ,UNKNOWN
    ,BUYAID
    ,GIFT
    ,VOUCHERS
    ,THRESHOLD
    ,WFS_SPEND
    ,OTHER_SPEND
    ,VOUCHER_SPEND
    ,TOTAL_SPEND
    ,LAST_UPDATED_DATE
    from w7071603.TMP_MART_SALES_RWDS_BU_MLY t;
    
    g_recs_inserted :=  SQL%ROWCOUNT;
    
    commit;
    
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    /* ============================================================================*/
    
    
    l_text :=  'Copying data to WFS_MART_SALES_RWDS_CUST_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    
    insert /*+ APPEND parallel(c,4) */
    into DWH_WFS_PERFORMANCE.WFS_MART_SALES_RWDS_CUST_MLY c
    
     ( CAL_YEAR_MONTH_NO
    ,WFS_PRODUCT_IND
    ,CC_COLOUR
    ,LOYALTY_TIERS
    ,ONLINE_PURCHASE_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,LITTLEWORLD_IND
    ,DISCOVERY_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,CUST_UNIQUE_ALL
    ,CUST_UNIQUE_WREWARDS
    ,CUST_UNIQUE_WREWARDS_SC
    ,CUST_UNIQUE_WREWARDS_CC
    ,CUST_UNIQUE_WREWARDS_VCHR
    ,CUST_UNIQUE_DIFF_REWARDS
    ,CUST_UNIQUE_DIFF_REWARDS_SC
    ,CUST_UNIQUE_DIFF_REWARDS_CC
    ,CUST_UNIQUE_DIFF_REWARDS_VCHR
    ,CUST_UNIQUE_PAID_BY_SC
    ,CUST_UNIQUE_PAID_BY_CC
    ,TRAN_ALL
    ,TRAN_WREWARDS_ITEMS_10
    ,TRAN_WREWARDS_ITEMS_20
    ,TRAN_DIFF_REWARDS_ITEMS
    ,ITEM_SALES_GROSS_AMOUNT
    ,ITEM_DISCOUNT_AMOUNT
    ,BASKETS_UNIQUE_ALL
    ,BASKETS_UNIQUE_PAID_BY_SC
    ,BASKETS_UNIQUE_PAID_BY_CC
    ,BASKETS_UNIQUE_WREWARDS
    ,BASKETS_UNIQUE_WREWARDS_SC
    ,BASKETS_UNIQUE_WREWARDS_CC
    ,BASKETS_UNIQUE_WREWARDS_VCHR
    ,BASKETS_UNIQUE_DIFF_REWARDS
    ,BASKETS_UNIQUE_DIFF_REWARDS_SC
    ,BASKETS_UNIQUE_DIFF_REWARDS_CC
    ,BASKETS_UNIQUE_DIFF_RWDS_VCHR
    ,WREWARDS_SALES_VALUE_10
    ,WREWARDS_DISCOUNT_10
    ,WREWARDS_SALES_VALUE_20
    ,WREWARDS_DISCOUNT_20
    ,WRWDS_DISCNT_WHEN_DIFF_RWDS
    ,WRWDS_EARNED_SUM_TRN_WFS_SC
    ,WRWDS_EARNED_SUM_TRN_WFS_CC
    ,WRWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_EARNED_SUM_TRN_SC
    ,DIFF_RWDS_EARNED_SUM_TRN_CC
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_RWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_SALES_SC_SUM_VALUE
    ,DIFF_RWDS_SALES_SC_SUM_TRN
    ,DIFF_RWDS_SALES_CC_SUM_VALUE
    ,DIFF_RWDS_SALES_CC_SUM_TRN
    ,DIFF_RWDS_DISCNT_PD_BY_SC
    ,DIFF_RWDS_DISCNT_PD_BY_CC
    ,DIFF_RWDS_DISCNT_PD_BY_GC
    ,DIFF_RWDS_DISCNT_PD_BY_CCLV
    ,DIFF_RWDS_DISCNT_PD_BY_THRV
    ,WFS_SC
    ,WFS_CC
    ,VISA
    ,DEBIT
    ,CASH
    ,MASTER
    ,HYBRID
    ,DINERS
    ,AMEX
    ,UNKNOWN
    ,BUYAID
    ,GIFT
    ,VOUCHERS
    ,THRESHOLD
    ,WFS_SPEND
    ,OTHER_SPEND
    ,VOUCHER_SPEND
    ,TOTAL_SPEND
    ,LAST_UPDATED_DATE )
    
    select  /*+ parallel(t,4)  full(t) */
    
     CAL_YEAR_MONTH_NO
    ,WFS_PRODUCT_IND
    ,CC_COLOUR
    ,LOYALTY_TIERS
    ,ONLINE_PURCHASE_IND
    ,MYSCHOOL_IND
    ,WWDIFFERENCE_IND
    ,LITTLEWORLD_IND
    ,DISCOVERY_IND
    ,FOODS_SEGMENT
    ,NON_FOODS_SEGMENT
    ,CUST_UNIQUE_ALL
    ,CUST_UNIQUE_WREWARDS
    ,CUST_UNIQUE_WREWARDS_SC
    ,CUST_UNIQUE_WREWARDS_CC
    ,CUST_UNIQUE_WREWARDS_VCHR
    ,CUST_UNIQUE_DIFF_REWARDS
    ,CUST_UNIQUE_DIFF_REWARDS_SC
    ,CUST_UNIQUE_DIFF_REWARDS_CC
    ,CUST_UNIQUE_DIFF_REWARDS_VCHR
    ,CUST_UNIQUE_PAID_BY_SC
    ,CUST_UNIQUE_PAID_BY_CC
    ,TRAN_ALL
    ,TRAN_WREWARDS_ITEMS_10
    ,TRAN_WREWARDS_ITEMS_20
    ,TRAN_DIFF_REWARDS_ITEMS
    ,ITEM_SALES_GROSS_AMOUNT
    ,ITEM_DISCOUNT_AMOUNT
    ,BASKETS_UNIQUE_ALL
    ,BASKETS_UNIQUE_PAID_BY_SC
    ,BASKETS_UNIQUE_PAID_BY_CC
    ,BASKETS_UNIQUE_WREWARDS
    ,BASKETS_UNIQUE_WREWARDS_SC
    ,BASKETS_UNIQUE_WREWARDS_CC
    ,BASKETS_UNIQUE_WREWARDS_VCHR
    ,BASKETS_UNIQUE_DIFF_REWARDS
    ,BASKETS_UNIQUE_DIFF_REWARDS_SC
    ,BASKETS_UNIQUE_DIFF_REWARDS_CC
    ,BASKETS_UNIQUE_DIFF_RWDS_VCHR
    ,WREWARDS_SALES_VALUE_10
    ,WREWARDS_DISCOUNT_10
    ,WREWARDS_SALES_VALUE_20
    ,WREWARDS_DISCOUNT_20
    ,WRWDS_DISCNT_WHEN_DIFF_RWDS
    ,WRWDS_EARNED_SUM_TRN_WFS_SC
    ,WRWDS_EARNED_SUM_TRN_WFS_CC
    ,WRWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_EARNED_SUM_TRN_SC
    ,DIFF_RWDS_EARNED_SUM_TRN_CC
    ,DIFF_REWARDS_SALES_VALUE
    ,DIFF_REWARDS_DISCOUNT
    ,DIFF_RWDS_EARNED_SUM_GC_LV_TV
    ,DIFF_RWDS_SALES_SC_SUM_VALUE
    ,DIFF_RWDS_SALES_SC_SUM_TRN
    ,DIFF_RWDS_SALES_CC_SUM_VALUE
    ,DIFF_RWDS_SALES_CC_SUM_TRN
    ,DIFF_RWDS_DISCNT_PD_BY_SC
    ,DIFF_RWDS_DISCNT_PD_BY_CC
    ,DIFF_RWDS_DISCNT_PD_BY_GC
    ,DIFF_RWDS_DISCNT_PD_BY_CCLV
    ,DIFF_RWDS_DISCNT_PD_BY_THRV
    ,WFS_SC
    ,WFS_CC
    ,VISA
    ,DEBIT
    ,CASH
    ,MASTER
    ,HYBRID
    ,DINERS
    ,AMEX
    ,UNKNOWN
    ,BUYAID
    ,GIFT
    ,VOUCHERS
    ,THRESHOLD
    ,WFS_SPEND
    ,OTHER_SPEND
    ,VOUCHER_SPEND
    ,TOTAL_SPEND
    ,LAST_UPDATED_DATE
    from  w7071603.TMP_MART_SALES_RWDS_CUST_MLY t;

    g_recs_inserted :=  SQL%ROWCOUNT;

    commit;

    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    /* ============================================================================*/
    
    
    l_text :=  'Gathering table stats for table WFS_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    DBMS_STATS.SET_TABLE_PREFS('DWH_WFS_PERFORMANCE','WFS_MART_SALES_MLY','INCREMENTAL','FALSE');
    dbms_stats.gather_table_stats('DWH_WFS_PERFORMANCE','WFS_MART_SALES_MLY',
    cascade=>false,
    method_opt=>'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 1',
    ESTIMATE_PERCENT=>0.001, degree=>1, granularity=>'PARTITION');



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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


end TMP_WFS_MART_SALES_CPY;
