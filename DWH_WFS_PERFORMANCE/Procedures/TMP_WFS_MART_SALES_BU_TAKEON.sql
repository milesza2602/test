--------------------------------------------------------
--  DDL for Procedure TMP_WFS_MART_SALES_BU_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."TMP_WFS_MART_SALES_BU_TAKEON" (p_forall_limit in integer,p_success out boolean) as 


--**************************************************************************************************
--  TMP_WFS_MART_SALES_BU_TAKEON      2017-05-25  N Chauhan
--
--  Description  WFS Sales Mart - TAKE_ON  - load rewards BU
--  Date:        2017-05-25
--  Author:      Naresh Chauhan
--  Purpose:     Load temp rewards BU mart
--               
--                    
--               
--  Tables:      Input  - 
--                    wfs_mart_sales_mly
--
--
--               Output - 
--                    WFS_MART_SALES_RWDS_BU_MLY
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-03-20 N Chauhan - created
--  2017-03-29 N Chauhan - fix for item_sales_gross_amount
--  2017-05-10 N Chauhan - fix for virtual spend fields
--  2017-05-25 N Chauhan - to create proc in prod and write direct to prod tables
--
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'TMP_WFS_MART_SALES_BU_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales/Rewards Marts RWDS_BU take-on';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




l_tablespace varchar2(50):= 'STG_STAGING';  /* UAT testing */
--l_tablespace varchar2(50):= 'WFS_PRF_MASTER_03';  /* production */




g_ym_todo integer:=null;
g_mths_todo integer:=null;
g_qry_text varchar2(1000);
g_success boolean:= TRUE;
--g_job_desc varchar2(100):= 'sales_takeon_RWDS_BU';
g_done_stat varchar(1);
--rec_cnt number(11);

g_in_safe_window boolean:= FALSE;
mth_rec_cnt number(11);




procedure SAFE_WINDOW_CHECK(safe_window_time_start in varchar2, safe_window_time_end in varchar2, g_in_safe_window out boolean) as
-- e.g. for avoiding batch

valid_time varchar2(5);
date_time1 date;
date_time2 date;
time_now date:=sysdate;


begin

   select regexp_substr(safe_window_time_start, '^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$') 
   into valid_time
   from dual;
   if valid_time is null then
       l_text :=  'safe_window_time_start incorrectly specified';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       g_in_safe_window := FALSE;
       return;
   end if;
   
   select regexp_substr(safe_window_time_end, '^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$') 
   into valid_time
   from dual;
   if valid_time is null then
       l_text :=  'safe_window_time_end incorrectly specified';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       g_in_safe_window := FALSE;
       return;
   end if;
   
   date_time1 := to_date(to_char(time_now,'YYYYMMDD ')||safe_window_time_start, 'YYYYMMDD HH24:MI'); 
   date_time2 := to_date(to_char(time_now,'YYYYMMDD ')||safe_window_time_end, 'YYYYMMDD HH24:MI');
   
   if date_time1 > date_time2 then     -- window spans over midnight
      if time_now >= date_time1 and time_now < trunc(time_now+1) then  -- before midnight
         g_in_safe_window := TRUE;
      elsif time_now < date_time2 then             -- after midnight
         g_in_safe_window := TRUE;
      else
         g_in_safe_window := FALSE;
      end if;
      return;
   else                         -- window does not spans over midnight
      if time_now >= date_time1 and time_now < date_time2 then  
         g_in_safe_window := TRUE;
      else
         g_in_safe_window := FALSE;
      end if;
   end if;
   
exception
 when others then
       l_text :=  'Safe window check fails';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       
       g_in_safe_window := FALSE;
   
end SAFE_WINDOW_CHECK;



procedure REWARDS_BU_MTH_LOAD(p_ym_todo in integer, g_success out boolean) as

   g_ymd_todo integer:=null;
   to_do_date_start date;
   to_do_date_end date;
   
  
   
begin


--   execute immediate 'alter session enable parallel dml';

   select to_date(to_char(p_ym_todo), 'YYYYMM') to_do_date_start,
           add_months(to_date(to_char(p_ym_todo), 'YYYYMM'),1)-1  to_do_date_end
      into to_do_date_start, to_do_date_end
   from dual;

--   SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'busy with month '||p_ym_todo||
--       ':   '||TO_CHAR(to_do_date_start,'yyyy-mm-dd')||' - '||TO_CHAR(to_do_date_end,'yyyy-mm-dd') );
   
 
   insert /*+ append parallel(rbm,4) */
--    into w7071603.tmp_mart_sales_rwds_bu_mly rbm (
    into DWH_WFS_PERFORMANCE.wfs_mart_sales_rwds_bu_mly rbm (
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
--     follwing are virtual fields:    
--   	,WFS_SPEND    
--   	,OTHER_SPEND
--   	,VOUCHER_SPEND
--   	,TOTAL_SPEND
   	,LAST_UPDATED_DATE
   ) 
   select  /*+ parallel(final,4) full(final) */
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
--   	,WFS_SPEND
--   	,OTHER_SPEND
--   	,VOUCHER_SPEND
--   	,TOTAL_SPEND
   	,trunc(sysdate)
   from (
   with 
   bu_basket_summary as	(
     select  /*+ PARALLEL(mrt,4) full(mrt) */
   	  to_number(to_char(tran_date,'YYYYMM')) as cal_year_month_no,
   	  wfs_product_ind,
   	  BUSINESS_UNIT_NO,
   	  business_unit_name,
   	  customer_no,
   	  cc_color as cc_colour,
   	  loyalty_tiers, 
   	  online_purchase_ind,
   	  myschool_ind as myschool_ind,
   	  wwdifference_ind as wwdifference_ind,
   	  littleworld_ind littleworld_ind,
   	  discovery_ind as discovery_ind,
   	  foods_segment,
   	  non_foods_segment, 
   	  max(c2_ind)										as 	c2_customer,
   	  count(distinct basket_id) 						as 	baskets_unique_all,
   	  count(distinct basket_id || to_char(subgroup_no))	as 	transactions_all,
   	  sum(case 
   		when wrewards_percentage = 0.1 then wrewards_items_count
   		else 0
   		end
   	  )	as	tran_wrewards_items_10,
   	  sum(diff_rewards_items_count) as tran_diff_rewards_items,
   	  sum(case 
   		when wrewards_percentage in 0.2 then wrewards_items_count
   		else 0
   		end
   	  )	as	tran_wrewards_items_20,   
   	  COUNT(DISTINCT(CASE 
   		WHEN TRAN_WFS_SC > 0 THEN basket_id 
   		ELSE NULL END)
   	  ) AS BASKETS_unique_PAID_BY_SC,
   	  COUNT(DISTINCT(CASE 
   		WHEN TRAN_WFS_CC > 0 THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_unique_PAID_BY_CC,
   	  COUNT(DISTINCT(CASE 
   		WHEN wrewards_percentage in (0.1,0.2) THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_unique_wrewards,
   	  COUNT(DISTINCT(CASE 
   		WHEN (diff_rewards_discount > 0)	THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_unique_diff_rewards,
   	  COUNT(DISTINCT(CASE 
   		WHEN wrewards_percentage in (0.1,0.2) AND TRAN_WFS_SC > 0 THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_unique_wrewards_SC,
   	  COUNT(DISTINCT(CASE 
   		WHEN wrewards_percentage in (0.1,0.2) AND TRAN_WFS_CC > 0 THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_unique_wrewards_CC,
   	  COUNT(DISTINCT(CASE 
   		WHEN diff_rewards_discount > 0 AND TRAN_WFS_SC > 0 THEN basket_id 
   		ELSE NULL 
   		END)
   	  ) AS BASKETS_UNIQUE_DIFF_REWARDS_SC,
   	  COUNT(DISTINCT(CASE 
   		WHEN diff_rewards_discount > 0 AND TRAN_WFS_CC > 0 THEN basket_id 
   		ELSE null 
   		END)
   	  ) AS BASKETS_UNIQUE_DIFF_REWARDS_CC,
   	  COUNT(DISTINCT(CASE 
   		WHEN 
   			wrewards_percentage in (0.1,0.2) 
   			AND   (	TRAN_Gift_Card > 0 OR TRAN_WFS_CC_Loyalty_voucher > 0 OR TRAN_Threshold_voucher > 0) 
   		  THEN basket_id 
   		ELSE null 
   		END)
   	  ) AS BASKETS_unique_wrewards_vchr,
   	  COUNT(DISTINCT(
   		CASE 
   		  WHEN 
   			diff_rewards_discount > 0
   			AND   (	TRAN_Gift_Card > 0 OR TRAN_WFS_CC_Loyalty_voucher > 0 OR TRAN_Threshold_voucher > 0)
   			THEN basket_id 
   		  ELSE null 
   		END)
   	  ) AS BASKETS_UNIQUE_DIFF_RWDS_VCHR,
--   	  sum(ITEM_TRAN_AMT + DISCOUNT_SELLING) 			as 	item_sales_gross_amount,
   	  sum(ITEM_TRAN_AMT) 			as 	item_sales_gross_amount,
   	  sum(DISCOUNT_SELLING) 				as 	DISCOUNT_SELLING,    
   	  sum(case 
   		  when wrewards_percentage = 0.1 then wrewards_sales_value
   		  else 0
   		end
   	  ) as	wrewards_sales_value_10,
   	  sum(case 
   		  when wrewards_percentage = 0.1 then wrewards_discount
   		  else 0
   		end
   	  ) 	as	wrewards_discount_10,
   	  sum(diff_rewards_sales_value)			as	diff_rewards_sales_value,
   	  sum(diff_rewards_discount)				as	diff_rewards_discount,
   	  sum(case 
   		  when wrewards_percentage in (0.1,0.2) then wrwds_discnt_when_DIFF_RWDS
   		  else 0
   		end
   	  ) 	as	wrwds_discnt_when_DIFF_RWDS, 
   	  sum(case 
   		  when wrewards_percentage in (0.1,0.2) then WRWDS_EARNED_SUM_TRN_WFS_SC
   		  else 0
   		end
   	  ) 	as	WRWDS_EARNED_SUM_TRN_WFS_SC,
   	  sum(case 
   		  when wrewards_percentage in (0.1,0.2) then WRWDS_EARNED_SUM_TRN_WFS_CC
   		  else 0
   		end
   	  )	as	WRWDS_EARNED_SUM_TRN_WFS_CC,
   	  sum(DIFF_RWDS_EARNED_SUM_TRN_SC)		as	DIFF_RWDS_EARNED_SUM_TRN_SC,
   	  sum(DIFF_RWDS_EARNED_SUM_TRN_CC)		as	DIFF_RWDS_EARNED_SUM_TRN_CC,
   	  sum(case 
   		  when wrewards_percentage in (0.1,0.2) then WRWDS_EARNED_SUM_GC_LV_TV
   		  else 0
   		end
   	  ) as	WRWDS_EARNED_SUM_GC_LV_TV,
   	  sum(DIFF_RWDS_EARNED_SUM_GC_LV_TV)	as	DIFF_RWDS_EARNED_SUM_GC_LV_TV,
   	  sum(DIFF_RWDS_SALES_SC_SUM_VALUE)		as	DIFF_RWDS_SALES_SC_SUM_VALUE,
   	  sum(DIFF_RWDS_SALES_SC_SUM_TRN)		as	DIFF_RWDS_SALES_SC_SUM_TRN,
   	  sum(DIFF_RWDS_SALES_CC_SUM_VALUE)		as	DIFF_RWDS_SALES_CC_SUM_VALUE,
   	  sum(DIFF_RWDS_SALES_CC_SUM_TRN)		as	DIFF_RWDS_SALES_CC_SUM_TRN,
   	  sum(DIFF_RWDS_DISCNT_PD_BY_SC)		as	DIFF_RWDS_DISCNT_PD_BY_SC,
   	  sum(DIFF_RWDS_DISCNT_PD_BY_CC)		as	DIFF_RWDS_DISCNT_PD_BY_CC,
   	  sum(DIFF_RWDS_DISCNT_PD_BY_GC)		as	DIFF_RWDS_DISCNT_PD_BY_GC,
   	  sum(DIFF_RWDS_DISCNT_PD_BY_CCLV)		as	DIFF_RWDS_DISCNT_PD_BY_CCLV,
   	  sum(DIFF_RWDS_DISCNT_PD_BY_THRV)		as	DIFF_RWDS_DISCNT_PD_BY_THRV,
   	  sum(TRAN_WFS_SC) 						as 	Wfs_SC,
   	  sum(TRAN_WFS_CC) 						as 	Wfs_CC,
   	  sum(TRAN_Visa) 						as 	Visa,
   	  sum(TRAN_Debit_Card) 					as 	Debit,
   	  sum(TRAN_Cash) 						as 	Cash,
   	  sum(TRAN_Master_Card) 				as 	Master,
   	  sum(TRAN_Hybrid_Card) 				as 	Hybrid,
   	  sum(TRAN_Diners_Card) 				as 	Diners,
   	  sum(TRAN_Amex) 						as 	Amex,
   	  sum(TRAN_Unknown) 					as 	Unknown,
   	  sum(TRAN_Buy_Aid) 					as 	BuyAid,
   	  sum(TRAN_Gift_Card) 					as 	Gift,
   	  sum(TRAN_WFS_CC_Loyalty_voucher) 		as 	Vouchers,
   	  sum(TRAN_Threshold_voucher) 			as 	Threshold,
   	  sum(case 
   		  when wrewards_percentage = 0.2 then wrewards_sales_value
   		  else 0
   		end
   	  ) as	wrewards_sales_value_20,  	
   	  sum(case 
   		  when wrewards_percentage = 0.2 then wrewards_discount
   		  else 0
   		end
   	  ) as	wrewards_discount_20
   	from (
   		select /*+ parallel(ms,4) full(ms) full(grp) */
   		  ms.*,
   		  grp.business_unit_no, 
   		  nvl(grp.business_unit_name,'UNKNOWN BU') as business_unit_name
   from  
     		  DWH_WFS_PERFORMANCE.wfs_mart_sales_mly ms   
     		  left join dwh_performance.dim_subgroup grp
     			on ms.subgroup_no = grp.subgroup_no
   		where
      
   --##############      
         tran_date between to_do_date_start and to_do_date_end
    --##############      
        
   	) mrt
   	where
   	 mrt.business_unit_name  <>  'UNKNOWN BU'       -- != 'UNKNOWN BU'
   	group by 	
   	  to_number(to_char(tran_date,'YYYYMM')),
   	  wfs_product_ind,
   	  business_unit_no,
   	  business_unit_name,
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
   ),
   bu_summary as(
   	select  /*+ parallel(bbs,4) full(bbs) */
   		cal_year_month_no,
   		wfs_product_ind,
   		business_unit_no,		
   		cc_colour,		
   		online_purchase_ind,
   		business_unit_name,
   		SUM(CASE 
   			WHEN (customer_no not in (0,99999999999999)) THEN	c2_customer 
   			ELSE 0 END
   		)as 	customers_unique_all,
   			SUM(CASE 
   			WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND	customer_no not in (0,99999999999999)
   						THEN 	c2_customer 
   		  ELSE 0 END
   		)		as 	cust_unique_wrewards,
   			SUM(CASE 
   			WHEN (diff_rewards_discount > 0 AND	customer_no not in (0,99999999999999)) THEN c2_customer
   			ELSE 0 
   		  END
   		) 		as 	cust_unique_diff_rewards,
   			SUM(CASE 
   		  WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND WFS_SC > 0 AND	customer_no not in (0,99999999999999) THEN 	c2_customer 
   		  ELSE 0 
   		END
   		)	 	as 	cust_unique_wrewards_SC,
   			SUM(CASE 
   			WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND  WFS_CC > 0 AND	customer_no not in (0,99999999999999) THEN c2_customer 
   			ELSE 0 
   		  END
   		)	 	as 	cust_unique_wrewards_CC,
   			SUM(CASE 
   			WHEN (diff_rewards_discount > 0 AND WFS_SC > 0 AND	customer_no not in (0,99999999999999)) THEN c2_customer
   			ELSE 0 
   		  END
   		)		as 	CUST_UNIQUE_DIFF_REWARDS_SC,
   			SUM(CASE 
   			WHEN (diff_rewards_discount > 0 AND   WFS_CC > 0 AND	customer_no not in (0,99999999999999)) THEN c2_customer
   			ELSE 0 
   		  END
   		) 		as CUST_UNIQUE_DIFF_REWARDS_CC,
   			SUM(CASE 
   			WHEN (wrewards_discount_10 > 0 or wrewards_discount_20 > 0) AND (	Gift > 0 OR Vouchers > 0 OR Threshold >0) AND	customer_no not in (0,99999999999999) THEN c2_customer 
   			ELSE 0 
   		  END
   		)	as 	cust_unique_wrewards_vchr,
   			SUM(CASE 
   			WHEN (diff_rewards_discount > 0 AND (Gift > 0 OR Vouchers > 0 OR Threshold >0) AND	customer_no not in (0,99999999999999)) THEN c2_customer 
   			ELSE 0 
   		  END
   		) as 	CUST_UNIQUE_DIFF_REWARDS_VCHR,
   		sum(baskets_unique_all)  				as baskets_unique_all,
   		sum(transactions_all)  					as tran_all,
   		sum(tran_wrewards_items_10)  		as tran_wrewards_items_10,
   		sum(tran_diff_rewards_items)  	as tran_diff_rewards_items,
   		sum(BASKETS_unique_wrewards)  			as baskets_unique_wrewards,
   		sum(BASKETS_unique_diff_rewards)  			as baskets_unique_diff_rewards,
   		sum(BASKETS_unique_wrewards_SC)  	as baskets_unique_wrewards_SC,
   		sum(BASKETS_unique_wrewards_CC)  	as baskets_unique_wrewards_CC,
   		sum(BASKETS_UNIQUE_DIFF_REWARDS_SC)  	as BASKETS_UNIQUE_DIFF_REWARDS_SC,
   		sum(BASKETS_UNIQUE_DIFF_REWARDS_CC)  	as BASKETS_UNIQUE_DIFF_REWARDS_CC,
   		sum(BASKETS_unique_wrewards_vchr)  	as baskets_unique_wrewards_vchr,
   		sum(BASKETS_UNIQUE_DIFF_RWDS_VCHR)  	as BASKETS_UNIQUE_DIFF_RWDS_VCHR,
   		sum(item_sales_gross_amount)  			as item_sales_gross_amount                ,
   		sum(DISCOUNT_SELLING)  					as item_discount_amount                    ,
   		sum(wrewards_sales_value_10)  				as wrewards_sales_value_10                    ,
   		sum(wrewards_discount_10)  				as wrewards_discount_10                   	   ,
   		sum(diff_rewards_sales_value)  			as diff_rewards_sales_value                  ,
   		sum(diff_rewards_discount)  				as diff_rewards_discount                     ,
   		sum(WRWDS_DISCNT_WHEN_DIFF_RWDS)  		as WRWDS_DISCNT_WHEN_DIFF_RWDS           ,
   		sum(WRWDS_EARNED_SUM_TRN_WFS_SC)  		as WRWDS_EARNED_SUM_TRN_WFS_SC       ,
   		sum(WRWDS_EARNED_SUM_TRN_WFS_CC)  		as WRWDS_EARNED_SUM_TRN_WFS_CC           ,
   		sum(DIFF_RWDS_EARNED_SUM_TRN_SC)  		as DIFF_RWDS_EARNED_SUM_TRN_SC            ,
   		sum(DIFF_RWDS_EARNED_SUM_TRN_CC)  		as DIFF_RWDS_EARNED_SUM_TRN_CC            ,
   		sum(WRWDS_EARNED_SUM_GC_LV_TV)  		as WRWDS_EARNED_SUM_GC_LV_TV              ,
   		sum(DIFF_RWDS_EARNED_SUM_GC_LV_TV)  	as DIFF_RWDS_EARNED_SUM_GC_LV_TV           ,
   		sum(DIFF_RWDS_SALES_SC_SUM_VALUE)  		as DIFF_RWDS_SALES_SC_SUM_VALUE            ,
   		sum(DIFF_RWDS_SALES_SC_SUM_TRN)  		as DIFF_RWDS_SALES_SC_SUM_TRN            ,
   		sum(DIFF_RWDS_SALES_CC_SUM_VALUE)  		as DIFF_RWDS_SALES_CC_SUM_VALUE            ,
   		sum(DIFF_RWDS_SALES_CC_SUM_TRN)  		as DIFF_RWDS_SALES_CC_SUM_TRN             ,
   		sum(DIFF_RWDS_DISCNT_PD_BY_SC)  	    as DIFF_RWDS_DISCNT_PD_BY_SC             ,
   		sum(DIFF_RWDS_DISCNT_PD_BY_CC)  	    as DIFF_RWDS_DISCNT_PD_BY_CC             ,
   		sum(DIFF_RWDS_DISCNT_PD_BY_GC)  	    as DIFF_RWDS_DISCNT_PD_BY_GC             ,
   		sum(DIFF_RWDS_DISCNT_PD_BY_CCLV)  		as DIFF_RWDS_DISCNT_PD_BY_CCLV           ,
   		sum(DIFF_RWDS_DISCNT_PD_BY_THRV)  		as DIFF_RWDS_DISCNT_PD_BY_THRV          ,
   		sum(Wfs_SC)  							as Wfs_SC          ,
   		sum(Wfs_CC)  							as Wfs_CC          ,
   		sum(Visa)  								as Visa            ,
   		sum(Debit)  							as Debit           ,
   		sum(Cash)  								as Cash            ,
   		sum(Master)  							as Master          ,
   		sum(Hybrid)  							as Hybrid          ,
   		sum(Diners)  							as Diners          ,
   		sum(Amex)  								as Amex            ,
   		sum(Unknown)  						    as Unknown         ,
   		sum(BuyAid)  							as BuyAid          ,
   		sum(Gift)  								as Gift            ,
   		sum(Vouchers)  							as Vouchers        ,
   		sum(Threshold)  						as Threshold       ,
--   		sum(0)									as WFS_Spend	   , /*placeholders*/
--   		sum(0)									as Other_Spend	   , /*placeholders*/
--   		sum(0)									as Voucher_Spend   , /*placeholders*/
--   		sum(0)									as Total_Spend     , /*placeholders*/
   		sum(BASKETS_unique_paid_by_SC)			as baskets_unique_paid_by_SC,
   		sum(BASKETS_unique_paid_by_CC)			as baskets_unique_paid_by_CC,
   		SUM(CASE 
   			  WHEN (Wfs_SC > 0 AND	customer_no not in (0,99999999999999)) THEN c2_customer 
   			  ELSE 0 
   			END
   		) as cust_unique_paid_by_SC,
   		SUM(CASE 
   			  WHEN (Wfs_CC > 0 AND customer_no not in (0,99999999999999)) THEN c2_customer
   			  ELSE 0 
   			END
   		) as cust_unique_paid_by_CC,		
   		sum(wrewards_sales_value_20)  as wrewards_sales_value_20, 	/* 104 */
   		sum(wrewards_discount_20)  		as wrewards_discount_20,	    /* 104 */
   		sum(tran_wrewards_items_20)  	as tran_wrewards_items_20		  /* 104 */
   
   	from	
   		bu_basket_summary  bbs
   	group by	
   		cal_year_month_no,
   		wfs_product_ind,
   		business_unit_no,
   		business_unit_name,
   		cc_colour,
   		online_purchase_ind			
   	)
   	select * from bu_summary
   ) final ;

   g_recs_inserted := SQL%ROWCOUNT;
   commit;
 
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted||' for month '||p_ym_todo;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_success := true;
    
    
exception

   when others then
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - REWARDS_BU_MTH_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      rollback;
      g_success := false;
      raise;

end REWARDS_BU_MTH_LOAD;
	
 
--##############################################################################################
-- Main process
--**********************************************************************************************
 
 
 
begin  

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS Sales Mart data RWDS_BU take-on STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

--   SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - starts');

    l_text :=  'WFS_MART_SALES_RWDS_BU_MLY take-on busy...';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   SELECT count(*) into g_mths_todo       -- check if anything to do
   FROM w7071603.SALES_TAKEON_CTL
   where BI_DONE_IND = 'Y' and
     (  RWBU_DONE_IND is null
      or ( RWBU_DONE_IND <> 'Y'          -- days not already done
           and RWBU_DONE_IND <> 'S') ) ; -- 'S' for skip

   if g_mths_todo = 0 then
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - No more months to do.');
      null;
   else

      SELECT max(YR_MTH_NO) into g_ym_todo   -- latest to oldest
      FROM w7071603.SALES_TAKEON_CTL
      where BI_DONE_IND = 'Y' and
        (  RWBU_DONE_IND is null
         or ( RWBU_DONE_IND <> 'Y'          -- days not already done
              and RWBU_DONE_IND <> 'S') ) ; -- 'S' for skip
         
      g_done_stat:='H';
      SELECT RWBU_DONE_IND into g_done_stat   -- check if hold is placed
      FROM w7071603.SALES_TAKEON_CTL
      where YR_MTH_NO = g_ym_todo;

   end if;
  
--SYS.DBMS_OUTPUT.PUT_LINE('loopstat  '||to_char(g_ym_todo)||'stat:'||nvl(g_done_stat,'null') );

   SAFE_WINDOW_CHECK('08:00','20:00', g_in_safe_window); -- avoid batch window

   while g_mths_todo > 0
         and   g_ym_todo is not null 
         and ( g_done_stat <> 'H' or g_done_stat is null )
         and g_success = TRUE
         and g_in_safe_window = TRUE
   loop
   
SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'processing month  '||g_ym_todo );
      

      -- *** monthly load ***********************
      REWARDS_BU_MTH_LOAD(g_ym_todo, g_success);
      -- ****************************************
      
      
      if g_success then
         
        update W7071603.SALES_TAKEON_CTL
           set
           RWBU_DONE_IND = 'Y',
           RWBU_COUNT = g_recs_inserted,
           RWBU_DONE_DATE = sysdate
           where YR_MTH_NO = g_ym_todo ; 
           commit;
      
      end if;

      SELECT count(*) into g_mths_todo       -- check if anything to do
      FROM w7071603.SALES_TAKEON_CTL
      where BI_DONE_IND = 'Y' and
        (  RWBU_DONE_IND is null
         or ( RWBU_DONE_IND <> 'Y'          -- days not already done
              and RWBU_DONE_IND <> 'S') ) ; -- 'S' for skip
   
      if g_mths_todo = 0 then
--         SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - No more months to do.');
         null;
      else   
         SELECT max(YR_MTH_NO) into g_ym_todo
         FROM w7071603.SALES_TAKEON_CTL
         where BI_DONE_IND = 'Y' and
           (  RWBU_DONE_IND is null
            or ( RWBU_DONE_IND <> 'Y'          -- days not already done
                 and RWBU_DONE_IND <> 'S') ) ; -- 'S' for skip
         
         g_done_stat:='H';
         SELECT RWBU_DONE_IND into g_done_stat   -- check if hold is placed
         FROM w7071603.SALES_TAKEON_CTL
         where YR_MTH_NO = g_ym_todo;
      end if;

      SAFE_WINDOW_CHECK('08:00','20:00', g_in_safe_window); -- avoid batch window
   
   end loop;
   
   if g_success then

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Gathering table stats...' );

      l_text :=  'Gathering table stats for table WFS_MART_SALES_RWDS_BU_MLY...';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      DBMS_STATS.SET_TABLE_PREFS('DWH_WFS_PERFORMANCE','wfs_mart_sales_rwds_bu_mly','INCREMENTAL','FALSE');
      dbms_stats.gather_table_stats('DWH_WFS_PERFORMANCE','wfs_mart_sales_rwds_bu_mly',
      cascade=>false,
      method_opt=>'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 1',
      ESTIMATE_PERCENT=>0.001, degree=>1, granularity=>'PARTITION');


--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
      l_text :=  l_description||' - ends '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      
      
      if g_done_stat = 'H' then
--         SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - hold on '||g_ym_todo );
         l_text :=  l_description||' - hold on '||g_ym_todo;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      if g_in_safe_window = FALSE then
         l_text :=  l_description||' - outside safe window';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      end if;

   else
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - fails');
      l_text :=  l_description||' - fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
   
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
exception

   when others then
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - main loop fails');
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );

      l_text :=  l_description||' - main loop fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      l_text := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   rollback;
   g_success := false;
   raise;


end tmp_wfs_mart_sales_bu_takeon;
