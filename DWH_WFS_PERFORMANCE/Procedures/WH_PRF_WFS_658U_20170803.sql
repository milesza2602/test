--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_658U_20170803
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_658U_20170803" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Sales Mart - Load Monthly Sales rewards BU Mart 
--  Date:        2017-05-31
--  Author:      Naresh Chauhan
--  Purpose:     Load Monthly Sales rewards CUST Mart
--               
--                    
--               THIS JOB RUNS DAILY, but will process once a month 
--  Tables:      Input  - 
--                    wfs_mart_sales_mly
--                    
--
--               Output - WFS_MART_SALES_RWDS_CUST_MLY

--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-05-31 N Chauhan - created. 
--  2017-06-05 N Chauhan - Check for already processed included. 
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_658U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales Mart - Load Monthly Sales Rewards CUST Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_job_desc varchar2(200):= 'Load Monthly Sales Rewards CUST Mart';
g_success boolean:= TRUE;
g_date_start date;
g_date_end date;
--g_date_to_do date;
g_yr_mth_to_do integer :=  0;

g_run_date date;
g_2nd_of_month date;
g_today date;
l_sql_stmt varchar2(200);
g_source_rows_chk integer :=0;

g_idx_drop_success boolean:= false;
g_idx_existed  boolean:= false;
g_analysed_count integer:=0;
g_analysed_success boolean:= false;



procedure REWARDS_CUST_MTH_LOAD(p_ym_todo in integer, g_success out boolean) as

--   g_ymd_todo integer:=null;
   to_do_date_start date;
   to_do_date_end date;
   
begin

   select to_date(to_char(p_ym_todo), 'YYYYMM') to_do_date_start,
           add_months(to_date(to_char(p_ym_todo), 'YYYYMM'),1)-1  to_do_date_end
      into to_do_date_start, to_do_date_end
   from dual;

--   SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'busy with month '||p_ym_todo||
--       ':   '||TO_CHAR(to_do_date_start,'yyyy-mm-dd')||' - '||TO_CHAR(to_do_date_end,'yyyy-mm-dd') );
   
 
 
      insert /*+ append parallel(rcm,4) */
      into DWH_WFS_PERFORMANCE.wfs_mart_sales_rwds_cust_mly  rcm (
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
        ,CUST_UNIQUE_DIFF_REWARDS  --,CUST_UNIQUE_DIFFREWARDS
        ,CUST_UNIQUE_DIFF_REWARDS_SC --CUST_UNIQUE_DFREWARDS_SC
        ,CUST_UNIQUE_DIFF_REWARDS_CC  --CUST_UNIQUE_DFREWARDS_CC
        ,CUST_UNIQUE_DIFF_REWARDS_VCHR  -- CUST_UNIQUE_DFREWARDS_VCHR
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
--     follwing are virtual fields:    
--        ,WFS_SPEND
--        ,OTHER_SPEND
--        ,VOUCHER_SPEND
--        ,TOTAL_SPEND
        ,LAST_UPDATED_DATE
      
      )select /*+ parallel(final,4) full(final) */
         CAL_YEAR_MONTH_NO
        ,WFS_PRODUCT_IND
      /*
        ,NVL(CC_COLOUR,'None') AS CC_COLOUR
        ,NVL(LOYALTY_TIERS,'None') AS LOYALTY_TIERS
        ,ONLINE_PURCHASE_IND
        ,NVL(MYSCHOOL_IND,0) AS MYSCHOOL_IND
        ,NVL(WWDIFFERENCE_IND,0) AS WWDIFFERENCE_IND
        ,NVL(LITTLEWORLD_IND,0) AS LITTLEWORLD_IND
        ,NVL(DISCOVERY_IND,0) AS DISCOVERY_IND
        ,NVL(FOODS_SEGMENT,'None') AS FOODS_SEGMENT
        ,NVL(NON_FOODS_SEGMENT,'None') AS NON_FOODS_SEGMENT
      */
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
        ,CUST_UNIQUE_DIFF_REWARDS  --,CUST_UNIQUE_DIFFREWARDS
        ,CUST_UNIQUE_DIFF_REWARDS_SC  --,CUST_UNIQUE_DFREWARDS_SC
        ,CUST_UNIQUE_DIFF_REWARDS_CC --,CUST_UNIQUE_DFREWARDS_CC
        ,CUST_UNIQUE_DIFF_REWARDS_VCHR  --,CUST_UNIQUE_DFREWARDS_VCHR
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
--        ,WFS_SPEND
--        ,OTHER_SPEND
--        ,VOUCHER_SPEND
--        ,TOTAL_SPEND
        ,trunc(sysdate)
      from (
        with 
        cust_basket_summary as (
          select  /*+ parallel(mrt,4) full(mrt) full(grp) */
            to_number(to_char(mrt.tran_date,'YYYYMM')) as cal_year_month_no,
            mrt.wfs_product_ind,
            mrt.customer_no,
      /*
            mrt.cc_color as cc_colour,
            mrt.loyalty_tiers, 
            mrt.online_purchase_ind,
            mrt.myschool_ind as myschool_ind,
            mrt.wwdifference_ind as wwdifference_ind,
            mrt.littleworld_ind littleworld_ind,
            mrt.discovery_ind as discovery_ind,
            mrt.foods_segment,
            mrt.non_foods_segment, 
      */
        NVL(mrt.CC_COLOR,'None') AS CC_COLOUR
        ,NVL(mrt.LOYALTY_TIERS,'None') AS LOYALTY_TIERS
        ,mrt.ONLINE_PURCHASE_IND
        ,NVL(mrt.MYSCHOOL_IND,0) AS MYSCHOOL_IND
        ,NVL(mrt.WWDIFFERENCE_IND,0) AS WWDIFFERENCE_IND
        ,NVL(mrt.LITTLEWORLD_IND,0) AS LITTLEWORLD_IND
        ,NVL(mrt.DISCOVERY_IND,0) AS DISCOVERY_IND
        ,NVL(mrt.FOODS_SEGMENT,'None') AS FOODS_SEGMENT
        ,NVL(mrt.NON_FOODS_SEGMENT,'None') AS NON_FOODS_SEGMENT,
      
            max(mrt.c2_ind)                                                                                   as       c2_customer,
            count(distinct mrt.basket_id)                                                      as       baskets_unique_all,
            count(distinct mrt.basket_id || to_char(mrt.subgroup_no)) as       tran_all,
            sum(case 
                when mrt.wrewards_percentage = 0.1 then mrt.wrewards_items_count
                else 0
                end
              ) as    tran_wrewards_items_10,
            sum(mrt.diff_rewards_items_count) as tran_diff_rewards_items,
            sum(case 
                when mrt.wrewards_percentage = 0.2 then  mrt.wrewards_items_count
                else 0
                end
              ) as    tran_wrewards_items_20,   
            COUNT(DISTINCT(CASE 
              WHEN mrt.TRAN_WFS_SC > 0 THEN mrt.basket_id 
              ELSE NULL END)
            ) AS bskts_unique_paid_by_SC,
            COUNT(DISTINCT(CASE 
              WHEN mrt.TRAN_WFS_CC > 0 THEN mrt.basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_paid_by_CC,
            COUNT(DISTINCT(CASE 
              WHEN mrt.wrewards_percentage in (0.1,0.2) THEN mrt.basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_wrewards,
            COUNT(DISTINCT(CASE 
              WHEN (mrt.diff_rewards_discount > 0)    THEN mrt.basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_diff_rewards,
            COUNT(DISTINCT(CASE 
              WHEN mrt.wrewards_percentage in (0.1,0.2) AND mrt.TRAN_WFS_SC > 0 THEN mrt.basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_wrewards_paid_SC,
            COUNT(DISTINCT(CASE 
              WHEN wrewards_percentage in (0.1,0.2) AND TRAN_WFS_CC > 0 THEN basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_wrewards_paid_CC,
            COUNT(DISTINCT(CASE 
              WHEN diff_rewards_discount > 0 AND TRAN_WFS_SC > 0 THEN basket_id 
              ELSE NULL 
              END)
            ) AS bskts_unique_dfrewards_paid_SC,
            COUNT(DISTINCT(CASE 
              WHEN mrt.diff_rewards_discount > 0 AND mrt.TRAN_WFS_CC > 0 THEN mrt.basket_id 
              ELSE null 
              END)
              ) AS bskts_unique_dfrewards_paid_CC,
            COUNT(DISTINCT(CASE 
              WHEN 
                mrt.wrewards_percentage in (0.1,0.2) AND   (
                  mrt.TRAN_Gift_Card > 0 OR mrt.TRAN_WFS_CC_Loyalty_voucher > 0 OR mrt.TRAN_Threshold_voucher > 0
                ) THEN mrt.basket_id 
              ELSE null 
              END)
            ) AS bskts_uniq_wrewards_paid_vchr,
            COUNT(DISTINCT(
              CASE 
                WHEN 
                  mrt.diff_rewards_discount > 0
                  AND   (     mrt.TRAN_Gift_Card > 0 OR mrt.TRAN_WFS_CC_Loyalty_voucher > 0 OR mrt.TRAN_Threshold_voucher > 0)
                  THEN mrt.basket_id 
                ELSE null 
              END)
            ) AS bskts_uniq_dfrewards_paid_vchr,
--            sum(mrt.ITEM_TRAN_AMT + mrt.DISCOUNT_SELLING)                      as       item_sales_gross_amount,
            sum(mrt.ITEM_TRAN_AMT)                      as       item_sales_gross_amount,
            sum(mrt.DISCOUNT_SELLING)                          as       DISCOUNT_SELLING,    
            sum(case 
                when mrt.wrewards_percentage = 0.1 then mrt.wrewards_sales_value
                else 0
              end
            ) as      wrewards_sales_value_10,
            sum(case 
                when mrt.wrewards_percentage = 0.1 then mrt.wrewards_discount
                else 0
              end
            )  as      wrewards_discount_10,
            sum(mrt.diff_rewards_sales_value)                 as      diff_rewards_sales_value,
            sum(mrt.diff_rewards_discount)                            as      diff_rewards_discount,
            sum(case 
                when mrt.wrewards_percentage in (0.1,0.2) then mrt.WRWDS_DISCNT_WHEN_DIFF_RWDS
                else 0
              end
            )  as      WRWDS_DISCNT_WHEN_DIFF_RWDS, 
            sum(case 
                when mrt.wrewards_percentage in (0.1,0.2) then mrt.WRWDS_EARNED_SUM_TRN_WFS_SC
                else 0
              end
            )  as      WRWDS_EARNED_SUM_TRN_WFS_SC,
            sum(case 
                when mrt.wrewards_percentage in (0.1,0.2) then mrt.WRWDS_EARNED_SUM_TRN_WFS_CC
                else 0
              end
            ) as      WRWDS_EARNED_SUM_TRN_WFS_CC,
            sum(mrt.DIFF_RWDS_EARNED_SUM_TRN_SC)              as      DIFF_RWDS_EARNED_SUM_TRN_SC,
            sum(mrt.DIFF_RWDS_EARNED_SUM_TRN_CC)              as      DIFF_RWDS_EARNED_SUM_TRN_CC,
            sum(case 
                when mrt.wrewards_percentage in (0.1,0.2) then mrt.WRWDS_EARNED_SUM_GC_LV_TV
                else 0
              end
            )          as      WRWDS_EARNED_SUM_GC_LV_TV,
            sum(mrt.DIFF_RWDS_EARNED_SUM_GC_LV_TV)    as      DIFF_RWDS_EARNED_SUM_GC_LV_TV,
            sum(mrt.DIFF_RWDS_SALES_SC_SUM_VALUE)     as      DIFF_RWDS_SALES_SC_SUM_VALUE,
            sum(mrt.DIFF_RWDS_SALES_SC_SUM_TRN)               as      DIFF_RWDS_SALES_SC_SUM_TRN,
            sum(mrt.DIFF_RWDS_SALES_CC_SUM_VALUE)     as      DIFF_RWDS_SALES_CC_SUM_VALUE,
            sum(mrt.DIFF_RWDS_SALES_CC_SUM_TRN)               as      DIFF_RWDS_SALES_CC_SUM_TRN,
            sum(mrt.DIFF_RWDS_DISCNT_PD_BY_SC)                as      DIFF_RWDS_DISCNT_PD_BY_SC,
            sum(mrt.DIFF_RWDS_DISCNT_PD_BY_CC)                as      DIFF_RWDS_DISCNT_PD_BY_CC,
            sum(mrt.DIFF_RWDS_DISCNT_PD_BY_GC)                as      DIFF_RWDS_DISCNT_PD_BY_GC,
            sum(mrt.DIFF_RWDS_DISCNT_PD_BY_CCLV)              as      DIFF_RWDS_DISCNT_PD_BY_CCLV,
            sum(mrt.DIFF_RWDS_DISCNT_PD_BY_THRV)              as      DIFF_RWDS_DISCNT_PD_BY_THRV,
            sum(mrt.TRAN_WFS_SC)                                       as       Wfs_SC,
            sum(mrt.TRAN_WFS_CC)                                       as       Wfs_CC,
            sum(mrt.TRAN_Visa)                                                 as       Visa,
            sum(mrt.TRAN_Debit_Card)                           as       Debit,
            sum(mrt.TRAN_Cash)                                                 as       Cash,
            sum(mrt.TRAN_Master_Card)                          as       Master,
            sum(mrt.TRAN_Hybrid_Card)                          as       Hybrid,
            sum(mrt.TRAN_Diners_Card)                          as       Diners,
            sum(mrt.TRAN_Amex)                                                 as       Amex,
            sum(mrt.TRAN_Unknown)                                      as       Unknown,
            sum(mrt.TRAN_Buy_Aid)                                      as       BuyAid,
            sum(mrt.TRAN_Gift_Card)                            as       Gift,
            sum(mrt.TRAN_WFS_CC_Loyalty_voucher)      as       Vouchers,
            sum(mrt.TRAN_Threshold_voucher)            as       Threshold,
            sum(case 
                when mrt.wrewards_percentage = 0.2 then mrt.wrewards_sales_value
                else 0
              end
            ) as      wrewards_sales_value_20,         
            sum(case 
                when mrt.wrewards_percentage = 0.2 then mrt.wrewards_discount
                else 0
              end
            ) as      wrewards_discount_20
          from                 
            DWH_WFS_PERFORMANCE.wfs_mart_sales_mly mrt    
            left join dwh_performance.dim_subgroup grp
                        on mrt.subgroup_no = grp.subgroup_no
            where
   --##############      
              tran_date between to_do_date_start and to_do_date_end

   --##############      
            and grp.business_unit_name is not null
          group by     
            to_number(to_char(tran_date,'YYYYMM')),
            wfs_product_ind,
            customer_no,
            cc_color,
            loyalty_tiers, 
            online_purchase_ind,
            myschool_ind,
            wwdifference_ind,
            littleworld_ind,
            discovery_ind,
            foods_segment,
            non_foods_segment
        )

  -- ######### main select in 'with' clause ############################ --

        select /*+ parallel(cbs, 4) full(cbs) */
            cal_year_month_no,
            wfs_product_ind,
            SUM(CASE 
                WHEN (customer_no not in (0,99999999999999)) THEN     c2_customer 
                ELSE 0 END
            )as        cust_unique_all, -- customers_unique_all,
            SUM(CASE 
            WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND   customer_no not in (0,99999999999999)
                  THEN         c2_customer 
              ELSE 0 END
            )         as       cust_unique_wrewards,
            SUM(CASE 
                WHEN (diff_rewards_discount > 0 AND   customer_no not in (0,99999999999999)) THEN c2_customer 
                ELSE 0 
              END
            )          as       cust_unique_diff_rewards,
              SUM(CASE 
              WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND WFS_SC > 0 AND  customer_no not in (0,99999999999999) THEN       c2_customer 
              ELSE 0 
            END
            )   as       cust_unique_wrewards_SC,
              SUM(CASE 
                WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND  WFS_CC > 0 AND       customer_no not in (0,99999999999999) THEN       c2_customer 
                ELSE 0 
              END
            )   as       cust_unique_wrewards_CC,
              SUM(CASE 
                WHEN (wrewards_discount_10 > 0 AND WFS_SC > 0 AND     customer_no not in (0,99999999999999)) THEN c2_customer 
                ELSE 0 
              END
            )         as       cust_unique_diff_rewards_SC,  --cust_unique_dfrewards_SC,
              SUM(CASE 
                WHEN (diff_rewards_discount > 0 AND   WFS_CC > 0 AND  customer_no not in (0,99999999999999)) THEN c2_customer 
                ELSE 0 
              END
            )          as       cust_unique_diff_rewards_CC,  --cust_unique_dfrewards_CC,
              SUM(CASE 
                WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND (     Gift > 0 OR Vouchers > 0 OR Threshold >0) AND   customer_no not in (0,99999999999999) THEN c2_customer 
                ELSE 0 
              END
            ) as       cust_unique_wrewards_vchr,
              SUM(CASE 
                WHEN (diff_rewards_discount > 0 AND (Gift > 0 OR Vouchers > 0 OR Threshold >0) AND    customer_no not in (0,99999999999999)) THEN c2_customer 
                ELSE 0 
              END
            ) as CUST_UNIQUE_DIFF_REWARDS_VCHR,  --CUST_UNIQUE_DFREWARDS_VCHR,
            sum(baskets_unique_all)                            as baskets_unique_all,
            sum(tran_all)                                      as tran_all,
            sum(tran_wrewards_items_10)                as tran_wrewards_items_10,
            sum(tran_diff_rewards_items)       as tran_diff_rewards_items,
            sum(bskts_unique_wrewards)                                as baskets_unique_wrewards,
            sum(bskts_unique_diff_rewards)                    as baskets_unique_diff_rewards,
            sum(bskts_unique_wrewards_paid_SC)         as baskets_unique_wrewards_SC,
            sum(bskts_unique_wrewards_paid_CC)         as baskets_unique_wrewards_CC,
            sum(bskts_unique_dfrewards_paid_SC)        as baskets_unique_diff_rewards_SC,  --as baskets_unique_dfrewards_SC,
            sum(bskts_unique_dfrewards_paid_CC)        as baskets_unique_diff_rewards_CC,  --as baskets_unique_dfrewards_CC,
            sum(bskts_uniq_wrewards_paid_vchr)         as baskets_unique_wrewards_vchr,  -- as baskets_uniq_wrewards_vchr,
            sum(bskts_uniq_dfrewards_paid_vchr)   as baskets_unique_diff_rwds_vchr,--     as baskets_uniq_dfrewards_vchr,
            sum(item_sales_gross_amount)                       as item_sales_gross_amount                ,
            sum(DISCOUNT_SELLING)                                     as item_discount_amount                    ,
            sum(wrewards_sales_value_10)                               as wrewards_sales_value_10                    ,
            sum(wrewards_discount_10)                                  as wrewards_discount_10                             ,
            sum(diff_rewards_sales_value)                      as diff_rewards_sales_value                  ,
            sum(diff_rewards_discount)                                 as diff_rewards_discount                     ,
            sum(WRWDS_DISCNT_WHEN_DIFF_RWDS)           as WRWDS_DISCNT_WHEN_DIFF_RWDS           ,
            sum(WRWDS_EARNED_SUM_TRN_WFS_SC)           as WRWDS_EARNED_SUM_TRN_WFS_SC       ,
            sum(WRWDS_EARNED_SUM_TRN_WFS_CC)           as WRWDS_EARNED_SUM_TRN_WFS_CC           ,
            sum(DIFF_RWDS_EARNED_SUM_TRN_SC)           as DIFF_RWDS_EARNED_SUM_TRN_SC            ,
            sum(DIFF_RWDS_EARNED_SUM_TRN_CC)           as DIFF_RWDS_EARNED_SUM_TRN_CC            ,
            sum(WRWDS_EARNED_SUM_GC_LV_TV)             as WRWDS_EARNED_SUM_GC_LV_TV              ,
            sum(DIFF_RWDS_EARNED_SUM_GC_LV_TV)                 as DIFF_RWDS_EARNED_SUM_GC_LV_TV           ,
            sum(DIFF_RWDS_SALES_SC_SUM_VALUE)                  as DIFF_RWDS_SALES_SC_SUM_VALUE            ,
            sum(DIFF_RWDS_SALES_SC_SUM_TRN)            as DIFF_RWDS_SALES_SC_SUM_TRN            ,
            sum(DIFF_RWDS_SALES_CC_SUM_VALUE)                  as DIFF_RWDS_SALES_CC_SUM_VALUE            ,
            sum(DIFF_RWDS_SALES_CC_SUM_TRN)            as DIFF_RWDS_SALES_CC_SUM_TRN             ,
            sum(DIFF_RWDS_DISCNT_PD_BY_SC)                     as DIFF_RWDS_DISCNT_PD_BY_SC             ,
            sum(DIFF_RWDS_DISCNT_PD_BY_CC)                     as DIFF_RWDS_DISCNT_PD_BY_CC             ,
            sum(DIFF_RWDS_DISCNT_PD_BY_GC)                     as DIFF_RWDS_DISCNT_PD_BY_GC             ,
            sum(DIFF_RWDS_DISCNT_PD_BY_CCLV)           as DIFF_RWDS_DISCNT_PD_BY_CCLV           ,
            sum(DIFF_RWDS_DISCNT_PD_BY_THRV)           as DIFF_RWDS_DISCNT_PD_BY_THRV          ,
            sum(Wfs_SC)                                                        as Wfs_SC          ,
            sum(Wfs_CC)                                                        as Wfs_CC          ,
            sum(Visa)                                                                  as Visa            ,
            sum(Debit)                                                         as Debit           ,
            sum(Cash)                                                                  as Cash            ,
            sum(Master)                                                        as Master          ,
            sum(Hybrid)                                                        as Hybrid          ,
            sum(Diners)                                                        as Diners          ,
            sum(Amex)                                                                  as Amex            ,
            sum(Unknown)                                                       as Unknown         ,
            sum(BuyAid)                                                        as BuyAid          ,
            sum(Gift)                                                                  as Gift            ,
            sum(Vouchers)                                                      as Vouchers        ,
            sum(Threshold)                                             as Threshold       ,
--            sum(0)                                                                    as WFS_Spend       , /*placeholders*/
--            sum(0)                                                                    as Other_Spend     , /*placeholders*/
--            sum(0)                                                                    as Voucher_Spend   , /*placeholders*/
--            sum(0)                                                                    as Total_Spend     , /*placeholders*/
            sum(bskts_unique_paid_by_SC)                      as baskets_unique_paid_by_SC,
            sum(bskts_unique_paid_by_CC)                      as baskets_unique_paid_by_CC,
            SUM(CASE 
              WHEN (Wfs_SC > 0 AND    customer_no not in (0,99999999999999)) THEN c2_customer 
              ELSE 0 
            END
            )         as   cust_unique_paid_by_SC, --    customr_unique_paid_by_SC,
              SUM(CASE 
              WHEN (Wfs_CC > 0 AND customer_no not in (0,99999999999999)) THEN c2_customer 
              ELSE 0 
            END
            )         as       cust_unique_paid_by_CC,  --customr_unique_paid_by_CC,
            cc_colour,
            loyalty_tiers, 
            online_purchase_ind,
            myschool_ind,
            wwdifference_ind,
            littleworld_ind,
            discovery_ind,
            foods_segment,
            non_foods_segment,
            sum(wrewards_sales_value_20)                       as wrewards_sales_value_20              ,        /* 104 */
            sum(wrewards_discount_20)                                  as wrewards_discount_20                  ,       /* 104 */
            sum(tran_wrewards_items_20)        as tran_wrewards_items_20               /* 104 */
        
          from        
            cust_basket_summary   cbs
          group by    
            cal_year_month_no,
            wfs_product_ind,
            cc_colour,
            loyalty_tiers, 
            online_purchase_ind,
            myschool_ind,
            wwdifference_ind,
            littleworld_ind,
            discovery_ind,
            foods_segment,
            non_foods_segment
      ) final ;
 

   g_success := true;
    
    
exception

   when others then

      rollback;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - REWARDS_CUST_MTH_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);

      g_success := false;
--      raise;

end REWARDS_CUST_MTH_LOAD;



--##############################################################################################
-- Main process
--**********************************************************************************************
 
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS Sales Mart Rewards CUST load STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

/*
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
    
    l_text := 'LOAD TABLE: '||'WFS_MART_SALES_RWDS_CUST_MLY' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     

--************************************************************************************************** 
-- DETERMINE WHEN JOB RUNS   
--**************************************************************************************************     
   

    g_today:= trunc(sysdate);

    select last_yr_fin_year_no, last_mn_fin_month_no
      into g_year_no, g_month_no
    from dim_control;

    select   max(fin_week_end_date)  -- last day of completed fin month
      into     g_run_date
    from     dim_calendar
    where    fin_year_no  =  g_year_no  and
             fin_month_no =  g_month_no;

    g_run_date := g_run_date + 1;  -- 1st day of the new fin month
    g_run_date := g_run_date + 7;  -- 8th day of the new fin month
   -- to run  after the load of final monthly load of CUST_LSS_LIFESTYLE_SEGMENTS (WH_PRF_CUST_284U)
   -- when wfs_mart_sales_mly load has completed.
   
    g_2nd_of_month:= to_date(to_char(g_today,'YYYYMM')||'02', 'YYYYMMDD');
   
    if g_run_date < g_2nd_of_month then
       g_run_date:= g_2nd_of_month;
       -- the LATER of the 8th Financial day of the month, or the 2nd day 
       -- we allow at least 2 days to ensure that all transactions from all stores are available
    end if;

--/*** temp override fiddle to force a load */   g_run_date:= trunc(sysdate);
   
    if trunc(sysdate) <> g_run_date then
       l_text      := 'This job only runs on '||to_char(g_run_date,'DD/Mon/YYYY')
                      ||' and today '||to_char(sysdate,'DD/Mon/YYYY')||' is not that day !';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
    end if;  
    
    l_text      := 'This job only runs on '||to_char(g_run_date,'DD/Mon/YYYY')
                   ||' and today '||to_char(sysdate,'DD/Mon/YYYY')||' is that day !  busy..';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
  
 
    -- determine period
    
    g_yr_mth_to_do:= to_number(to_char(add_months(sysdate,-1),'YYYYMM') );
 

    -- check if already done
    select /*+ parallel(t,4) full(t) */
      count(*) into g_count
    from   WFS_MART_SALES_RWDS_CUST_MLY   t
    where CAL_YEAR_MONTH_NO = g_yr_mth_to_do;
    if g_count > 0 then
       l_text      := 'Data already exists for '||g_yr_mth_to_do||'. Load abandoned';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := true;
       return;
    end if;    
    
    g_date_start:= to_date(to_char(add_months(sysdate,-1),'YYYYMM')||'01', 'YYYYMMDD' );
    -- first day of last completed calendar month

    g_date_end:=to_date(to_char(sysdate,'YYYYMM')||'01', 'YYYYMMDD' ) -1 ;
     -- last day of last completed calendar month


    l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Starting load for month '
              ||to_char(g_date_start, 'YYYY-MM');
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- check if source data available (for last day of period)
 
    select /*+ parallel(t,4) full(t) */
     count(*) cnt  into g_source_rows_chk
    from  WFS_MART_SALES_MLY    t
    where tran_date = g_date_end;
 
    if g_source_rows_chk < 1000000 then
       l_text      := 'Incomplete data in WFS_MART_SALES_MLY. Load abandoned.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := true;
       return;
    end if;  

   

    -- ****** main load *************
    REWARDS_CUST_MTH_LOAD(g_yr_mth_to_do, g_success);
    -- ******************************

    g_recs_read     :=  SQL%ROWCOUNT;
    g_recs_inserted :=  SQL%ROWCOUNT;

    commit;  

    l_text :=  'For month '||g_yr_mth_to_do||'  Inserted:  '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    -- NB write_log does a commit !
    
 


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
   

--**************************************************************************************************
-- Retention maintenance    
--**************************************************************************************************

     -- excluded for now due to reservations about partitions deletions by DWH team





--**************************************************************************************************
-- gather statistics
--**************************************************************************************************

-- skip gather statistics - let DBA's maintenance task do it overnight
/* 
       l_text := 'gathering statistics ...';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

--     DBMS_STATS.SET_TABLE_PREFS('DWH_WFS_PERFORMANCE','WFS_MART_SALES_RWDS_BU_MLY','INCREMENTAL','TRUE');  
--     done by dba, need only do once

    -- analyse all unanalysed partitions, one partition at a time
       DWH_DBA_WFS.stats_partitions_outstanding (
            'DWH_WFS_PERFORMANCE',
            'WFS_MART_SALES_RWDS_BU_MLY',
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
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');
      
        rollback;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
                  || '   - load for month '||g_yr_mth_to_do||'  fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    
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


end wh_prf_wfs_658u_20170803;
