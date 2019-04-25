--------------------------------------------------------
--  DDL for Procedure TMP_WFS_MART_SALES_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."TMP_WFS_MART_SALES_TAKEON" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  sales_takeon_sm      2017-03-08  N Chauhan
--
--  Description  WFS Sales Mart - TAKE_ON  - load sales monthly 
--  Date:        2017-03-17
--  Author:      Naresh Chauhan
--  Purpose:     Load temp sales montly mart
--               
-- *** With temp fix to exclude duplicate basket_item records.
-- ** NB . Isolates daily record groups by tran_date for speed
--         and NOT last_updated_date, which accommodates late-comers.
--               
--  Tables:      Input  - 
--                    cust_basket_tender
--                    apex_wfs_tender_types
--                    cust_basket_item
--                    dim_item
--                    cust_basket_aux
--
--     **** NB accesses w7071603.tmp_mart_sales_cust_acc_mly 
--             where historic data was imported from SAS.
--
--               Output - DWH_WFS_PERFORMANCE.WFS_MART_SALES_MLY 
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-03-17 N Chauhan - created
--  2017-04-19 N Chauhan - restructured to run in prod
--  2017-05-09 N Chauhan - revise calc for SC_IND and CC_IND.
--  2017-05-17 N Chauhan - revise calc for SC_IND and CC_IN, for null cusstomer_no
--  2017-05-25 N Chauhan - skip stats gather, as batch job will now do this overnight.
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'TMP_WFS_MART_SALES_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales/Rewards Marts take-on';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--l_tablespace varchar2(50):= 'STG_STAGING';  /* UAT testing */
l_tablespace varchar2(50):= 'WFS_PRF_MASTER_03';  /* production */


g_ym_todo integer:=null;
g_qry_text varchar2(1000);
g_success boolean:= TRUE;
g_in_safe_window boolean:= FALSE;
g_done_stat varchar(1);
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



procedure DROP_INDEX_IF_EXIST(p_owner in varchar2, p_index in varchar2) as

    index_not_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(index_not_exists, -1418);

  begin
       execute immediate 'DROP INDEX '||p_owner||'.'||p_index;
      
  exception
    when index_not_exists then
       l_text :=  'Index '||p_index||' does not exist';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end DROP_INDEX_IF_EXIST;



procedure SALES_MTH_LOAD(p_ym_todo in integer, g_success out boolean) as

   g_ymd_todo integer:=null;

   -- get calendar dates for the YYYYMM specified, excluding dates already done
   cursor c_day_todo is
   select to_char(calendar_date, 'dd') day, calendar_date 
   from dwh_performance.dim_calendar c
   left outer join W7071603.SALES_TAKEON_BI_DAY_CTL bc 
       on BC.YR_MTH_DY_NO = to_char(calendar_date, 'yyyymmdd') 
   where cal_year_month_no = p_ym_todo  
      and ( bc.YR_MTH_DY_NO is null 
       or ( bc.DY_DONE_IND <> 'Y'          -- days not already done
            and bc.DY_DONE_IND <> 'S'  ))   -- 'S' for skip
   order by calendar_date;


begin

--   SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'busy with month '||p_ym_todo );
   
   mth_rec_cnt:=0;
   
   for dy_rec in c_day_todo 
   loop
       
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'busy with day '||dy_rec.day );
 
  
		insert /*+ APPEND  parallel(sm,4) */ 
      into DWH_WFS_PERFORMANCE.WFS_mart_sales_mly sm (
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

      with
      dly_bsk_tender_1 as (
         select /*+ parallel (cbk,4)   full(cbk)  */
                 cbk.tran_date,
                 cbk.location_no,
                 cbk.till_no,
                 cbk.tran_no,
                 cbk.tender_type_code,
                 cbk.TRAN_TIME,
                 cbk.tender_seq_no,
                 (tender_selling - change_selling)     as tender_amt,
                 change_selling                        as change_amt,
                 tender_selling,
                 case 
                 when tender_type_detail_code is null or tender_type_detail_code='' then 
                   'U/K'
                 when substr(to_char(tender_no),1,6) in ('410375','410374','400154') then
                   'WWCC'
                 when tender_type_code = 1292 and tender_no > 0 and tender_type_detail_code = 'U/K' then
                   'WWCC'
                 when tender_type_code = 1290 and tender_type_detail_code = 'U/K' and tender_no > 0 then
                   'WWISCS'
                 else
                   tender_type_detail_code
                 end
                 as tender_type_detail_code
         from
                 DWH_CUST_PERFORMANCE.cust_basket_tender cbk
         where
                 cbk.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
                 and cbk.payment_account_no is null
                 and not(cbk.tender_type_detail_code in (
                         'CASH','CASHL','CASHLR','CASHP','CASHPR','CASHREF','CASHRV','CASHV')
                         and (tender_selling - change_selling)=-0.05)

         ),
--) select count(*) from dly_bsk_tender_1 ;

      dly_bsk_tender as (
         select
                 cbk.*,
                 'TRAN_' ||
                 case
                   when trim(trn.tender_type_detail) = '' or trn.tender_type_detail is null then 'Unknown'
                   else trim(trn.tender_type_detail)
                 end as tender_type_detail
         from
                 dly_bsk_tender_1 cbk
                 left join APEX_APP_WFS_01.APEX_WFS_TENDER_TYPE trn
                 on
                    cbk.tender_type_detail_code = trn.tender_type_detail_code
          ),
--) select count(*) from dly_bsk_tender ;


            /*
              There is a problem with the source data, specifically when a Visa Loyalty voucher is used
              to make a purchase for a value less than the voucher and a Threshold voucher is used
              to purchase a Taste Magazine (item_no  = 9771728878004).
              The underlying cause of the issue is that the balance of the loyalty voucher is paid out onto
              the threshold voucher as change, which should not happen but there appear to be a number of exceptions
              to this rule. As a result, when we try to distribute the tender across items in the basket, the
              amount paid by the loyalty voucher is inflated, sometimes by a very significant amount.
              To correct this problem, we identify all baskets where this occurred and deduct the change amount
              paid on the threshold voucher from the tender amount on the loyalty voucher. This will ensure that
              the tenders and transaction totals balance.
            */
      tender_totals_by_type_src as (
         select /*+ materialize parallel(t1,4)*/
                            t1.tran_date,
                            t1.location_no,
                            t1.till_no,
                            t1.tran_no,
                            t1.TRAN_TIME,
                            t1.tender_type_detail,
                            t1.tender_amt,
                            t1.change_amt,
                            t1.tender_selling,
                            case when t1.tender_type_detail like 'TRAN_WFS Visa Loyalty voucher' then 1 else 0 end as is_cc_loyalty_vchr,
                            case when t1.tender_type_detail like 'TRAN_Threshold voucher' then 1 else 0 end as is_threshold_vchr,
                            sum(t1.tender_amt) over (partition by  t1.location_no, t1.tran_no, t1.till_no, t1.tran_date) AS  total_basket_amt,
                            sum(t1.tender_amt) over (partition by  t1.location_no, t1.tran_no, t1.till_no, t1.tran_date, t1.tender_type_detail) AS total_tender_amt,
                            row_number() over (partition by  t1.location_no, t1.tran_no, t1.till_no, t1.tran_date, t1.tender_type_detail order by tender_seq_no) as row_num,
                            max(case when t1.tender_type_detail like 'TRAN_WFS Visa Loyalty voucher' then 1 else 0 end) over (partition by t1.location_no, t1.tran_no, t1.till_no, t1.tran_date) as has_cc_loyalty_vchr,
                            max(case when t1.tender_type_detail like 'TRAN_Threshold voucher' then 1 else 0 end) over (partition by t1.location_no, t1.tran_no, t1.till_no, t1.tran_date) as has_threshold_vchr

         from
              dly_bsk_tender t1
         ),
--) select count(*) from tender_totals_by_type_src ;    415636 recs  3.2 secs


--mytbl as ( 
--select '1' one from dual )
--select * from mytbl ;

      error_tenders as (
         select /*+ parallel (itm,4)   full(itm) materialize  */
                    itm.tran_date,
                    itm.location_no,
                    itm.till_no,
                    itm.tran_no,
                    itm.ITEM_TRAN_SELLING - itm.DISCOUNT_SELLING as adjustment,
                    tenders.change_amt
         from
            (
             select distinct /*+ parallel(mx,4) */
               mx.tran_date,
               mx.location_no,
               mx.till_no,
               mx.tran_no,
               max(mx.max_tender_perc) as max_tender_perc
             from (
                    select /*+ parallel(a,4) */
                          a.tran_date,
                          a.location_no,
                          a.till_no,
                          a.tran_no,
                          case
                            when (a.total_basket_amt > 0) then  a.total_tender_amt/a.total_basket_amt
                            else 0
                          end as tender_perc,
                          max(case
                            when (a.total_basket_amt > 0) then  a.total_tender_amt/a.total_basket_amt
                            else 0
                          end) over (partition by a.tran_date,a.location_no,a.till_no,a.tran_no) as max_tender_perc
                    from
                          tender_totals_by_type_src a
                    where
                          a.has_cc_loyalty_vchr = 1 and a.has_threshold_vchr = 1
                          and row_num=1
                  ) mx
         where
            mx.max_tender_perc > 1
         group by
             mx.tran_date,
             mx.location_no,
             mx.till_no,
             mx.tran_no
           ) problems, DWH_CUST_PERFORMANCE.cust_basket_item itm, tender_totals_by_type_src tenders
         where
            itm.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
            and itm.tran_date = problems.tran_date
            and itm.location_no = problems.location_no
            and itm.till_no = problems.till_no
            and itm.tran_no = problems.tran_no
            and itm.item_no  = 9771728878004
            and tenders.location_no = problems.location_no
            and tenders.till_no = problems.till_no
            and tenders.tran_no = problems.tran_no
            and tenders.tender_type_detail like 'TRAN_Threshold voucher'
            and tenders.tender_selling = itm.item_tran_selling - itm.discount_selling
         ),

--) select * /* count(*)*/ from error_tenders ;  -- 7 sec  cnt=2

      tender_totals_by_type as (
         select /*+ parallel(src,4) full(src) full(err)  */
            src.tran_date,
            src.tran_time,
            src.location_no,
            src.till_no,
            src.tran_no,
            src.total_basket_amt,
            src.tender_type_detail,
            case
               when src.is_cc_loyalty_vchr = 1 and err.change_amt is not null then -1*err.change_amt
               when src.is_threshold_vchr  = 1 and err.change_amt is not null then err.change_amt
               else 0
            end + src.total_tender_amt as total_tender_amt,
            case
               when src.total_basket_amt is null or src.total_basket_amt = 0 then 0
               when src.total_tender_amt is null then 0
               else abs((
                  case
                          when src.is_cc_loyalty_vchr = 1 and err.change_amt is not null then -1*err.change_amt
                          when src.is_threshold_vchr  = 1 and err.change_amt is not null then err.change_amt
                          else 0
                  end + total_tender_amt) / total_basket_amt)
            end as perc_of_tender
         from
            tender_totals_by_type_src src
            left join error_tenders err on
                 src.tran_date = err.tran_date
                 and src.location_no = err.location_no
                 and src.till_no = err.till_no
                 and src.tran_no = err.tran_no
                 and (src.is_cc_loyalty_vchr = 1 or src.is_threshold_vchr = 1)
         where
            src.row_num = 1
         ),
            
--) select count(*) from tender_totals_by_type ;  -- 84 sec     /  1268secs 410175 recs   with more parallels - 1220 secs  / 8 secs with materizlise on error_tenders

      dup_keys as (       
         -- get keys of transactions having multiple customer_no's
         select  /*+ parallel (gg,4)   full(gg) */
            gg.location_no,
            gg.till_no,
            gg.tran_no,
            gg.tran_date,
            count(*) cnt
         from (           
               -- determine transactions having multiple customer_no's 
               select  /*+ parallel (g,4)   full(g) */
                  g.location_no,
                  g.till_no,
                  g.tran_no,
                  g.tran_date,
                  g.item_no,
                  count(*) cnt
               from (
                     select  /*+ parallel (bski,4)   full(bski) */
                        bski.location_no,
                        bski.till_no,
                        bski.tran_no,
                        bski.tran_date,
                        bski.item_no,
                        customer_no,
                        count(*) cnt
                     from DWH_CUST_PERFORMANCE.cust_basket_item bski   
                     where tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY') 
                     group by 
                        bski.location_no,
                        bski.till_no,
                        bski.tran_no,
                        bski.tran_date,
                        bski.item_no,
                        customer_no
                     ) g
               group by 
                  g.location_no,
                  g.till_no,
                  g.tran_no,
                  g.tran_date,
                  g.item_no
               having count(*) > 1  
               )  gg
         group by
            gg.location_no,
            gg.till_no,
            gg.tran_no,
            gg.tran_date       
         ),
         
      invalid_recs as (
            -- invalid duplicate null customer_no records
            select /*+ materialize parallel(x,4) full(x) full(d) */
             x.*
            from  DWH_CUST_PERFORMANCE.cust_basket_item x, dup_keys d
            where 
               d.location_no = x.location_no 
               and d.till_no = x.till_no
               and d.tran_no = x.tran_no
               and x.tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY')  
               and x.customer_no is null  /* invalid records */
         ),      

      basket_items1 as (
         select  /*+ parallel (bsk,4)   full(bsk) */
            bsk.customer_no,
            bsk.LOCATION_NO,
            bsk.till_no,
            bsk.tran_no,
            bsk.item_no,
            bsk.tran_date,
            bsk.tran_type,
            bsk.item_seq_no,
            bsk.item_tran_qty,
            bsk.item_tran_selling as item_tran_amt,
            --removed subgroup
            bsk.discount_selling ,
            bsk.dept_no,
            case
               when upper(bsk.tran_type) = 'V' and bsk.item_tran_selling < 0 and bsk.discount_selling > 0 then bsk.item_tran_selling - (bsk.discount_selling * -1)
               else  bsk.item_tran_selling - bsk.discount_selling
            end as item_amt 
         from
    				DWH_CUST_PERFORMANCE.cust_basket_item bsk
            
            left outer join invalid_recs x  on (
                   x.LOCATION_NO=bsk.LOCATION_NO
               and x.TILL_NO=bsk.TILL_NO
               and x.TRAN_NO=bsk.TRAN_NO
               and x.TRAN_DATE=bsk.TRAN_DATE
               and x.ITEM_SEQ_NO=bsk.ITEM_SEQ_NO
               and x.ITEM_NO=bsk.ITEM_NO )
            
         where bsk.tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY')
           and x.LOCATION_NO is null   -- exclude invalid records

         ),
--) select count(*) from basket_items1 ;  --  sec

      basket_items as (
         select /*+ parallel (bsk,4) parallel(itm,4)   full(bsk) full(itm)  */
            bsk.customer_no    ,
            bsk.LOCATION_NO,
            bsk.till_no,
            bsk.tran_no,
            bsk.item_no,
            bsk.tran_date,
            bsk.tran_type,
            bsk.item_seq_no,
            item_tran_qty,
            case
               when itm.subgroup_no is null then -1
               else itm.subgroup_no
            end as subgroup_no,
            bsk.item_tran_amt,
            bsk.discount_selling ,
            bsk.dept_no,
            bsk.item_amt 
         from basket_items1 bsk
       				left join dim_item itm 		on	bsk.item_no = itm.item_no
         ),
--) select count(*) from basket_items ;  --  sec
      basket_items_summary as (
         select /*+ parallel(i,4) */
            customer_no,
            tran_date,
            location_no,
            till_no,
            tran_no,
            item_no,
            subgroup_no,
            sum(item_tran_qty) as item_tran_qty,
            sum(item_tran_amt) as item_tran_amt,
            sum(DISCOUNT_SELLING) as DISCOUNT_SELLING,
            sum(item_amt) as item_amt
         from
            basket_items i
         group by
            customer_no,
            tran_date,
            location_no,
            till_no,
            tran_no,
            item_no,
            subgroup_no
         ),
--) select count(*) from basket_items_summary ;  --  sec
            
      all_basket_src as (
          select  /*+ parallel(a,4) parallel(b,4) */
             a.tran_date,
             a.location_no,
             a.till_no,
             a.tran_no,
             a.item_no,
             b.tran_time,
             a.subgroup_no,
             case
                     when a.customer_no is null then 99999999999999
                     else a.customer_no
             end as customer_no,
             a.item_tran_qty,
             a.item_tran_amt,
             a.discount_selling,
             b.total_basket_amt as total_basket,
             case
                     when b.tender_type_detail is null then 'Unknown'
                     else  trim(b.tender_type_detail)
             end as  tender_type_detail,
             case
                     when a.till_no = 999  then 1
                     else 0
             end as  online_flag,
             case
                     when b.tran_no is null then     a.item_amt
                     else (a.item_amt        * b.perc_of_tender)
             end as item_fin_amt
          from
             basket_items_summary    a
             left join tender_totals_by_type b
                on
                   a.tran_date = b.tran_date
                   and a.location_no = b.location_no
                   and a.till_no = b.till_no
                   and a.tran_no = b.tran_no
         ),
--) select count(*) from all_basket_src ;  -- 90  sec       /  cnt=2481161  in 20 secs with materialize for error_tenders

      excl_empties as (
         SELECT /*+ parallel(abs, 4) */
            tran_date,
            TRAN_TIME,
            location_no,
            till_no,
            tran_no,
            item_no,
            subgroup_no,
            customer_no,
            item_tran_qty,
            item_tran_amt,
            DISCOUNT_SELLING,
            replace(upper(tender_type_detail),' ','_') as tender_type_detail,
            item_fin_amt,
            total_basket
         from
            all_basket_src abs
         where
            total_basket is not null
         ),
      
    

      all_basket AS (
         SELECT  /*+ parallel(ee,4) full(ee) */
            *
         FROM excl_empties  ee
         PIVOT(
               SUM(item_fin_amt) for
                  tender_type_detail in (
                  'TRAN_WFS_SC'                   as TRAN_WFS_SC,
                  'TRAN_WW_VISA'                  as TRAN_WFS_CC,
                  'TRAN_DEBIT_CARD'               as TRAN_DEBIT_CARD,
                  'TRAN_HYBRID_CARD'              as TRAN_HYBRID_CARD,
                  'TRAN_VISA'                     as TRAN_VISA,
                  'TRAN_MASTER_CARD'              as TRAN_MASTER_CARD,
                  'TRAN_DINERS_CARD'              as TRAN_DINERS_CARD,
                  'TRAN_AMEX'                     as TRAN_AMEX,
                  'TRAN_CASH'                     as TRAN_CASH,
                  'TRAN_WFS_VISA_LOYALTY_VOUCHER' as TRAN_WFS_VISA_LOYALTY_VOUCHER,
                  'TRAN_THRESHOLD_VOUCHER'        as TRAN_THRESHOLD_VOUCHER,
                  'TRAN_GIFT_CARD'                as TRAN_GIFT_CARD,
                  'TRAN_BUY_AID'                  as TRAN_BUY_AID,
                  'TRAN_TM_VOUCHER'               as TRAN_TM_VOUCHER,
                  'TRAN_UNKNOWN'                  as TRAN_UNKNOWN
                  )
               )  
         ) ,
      rewards_groups_src as (
         select /*+ parallel(a, 4) */
            a.tran_date,
            a.LOCATION_NO,
            a.till_no,
            a.tran_no,
            a.item_no,
            /* a.primary_account_no, */
            a.item_seq_no,
            a.loyalty_group,
            case
              when TRAN_TYPE_CODE in ('LOYALTY','ATGLOY') then 1
              else 0
            end as wrewards_items_count,
            case
              when TRAN_TYPE_CODE in ('LOYALTY','ATGLOY') then a.PROMOTION_NO
              else null
            end as wrewards_promotion_no,
            case
              when TRAN_TYPE_CODE in ('LOYALTY','ATGLOY') then a.PROMOTION_DISCOUNT_AMOUNT
              else null
            end as  wrewards_discount,
            case
              when TRAN_TYPE_CODE in ('LOYALTY','ATGLOY') then a.WREWARD_SALES_VALUE
              else null
            end as  wrewards_sales_value,
            case
              when TRAN_TYPE_CODE in ('LOYALTY','ATGLOY') then ROUND(a.PROMOTION_DISCOUNT_AMOUNT / a.WREWARD_SALES_VALUE,2)
              else null
            end as  wrewards_percentage,
             case
              when TRAN_TYPE_CODE in ('WFS') then 1
              else 0
            end as diff_rewards_items_count,
            case
              when TRAN_TYPE_CODE in ('WFS') then a.promotion_no
              else null
            end as diff_rewards_promotion_no,
            case
              when TRAN_TYPE_CODE in ('WFS') then a.promotion_discount_amount
              else null
            end as  diff_rewards_discount,
            case
              when TRAN_TYPE_CODE in ('WFS') then a.wreward_sales_value
              else null
            end as  diff_rewards_sales_value,
            case
              when TRAN_TYPE_CODE in ('WFS') then round(a.promotion_discount_amount / a.wreward_sales_value         , 2)
              else null
            end as  diff_rewards_percentage
         from
            DWH_CUST_PERFORMANCE.CUST_BASKET_AUX        a
         where
            a.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
            and    a.TRAN_TYPE_CODE in ('LOYALTY','ATGLOY','WFS')
--                        and   a.promotion_discount_amount != 0
            and   a.promotion_discount_amount <> 0
         ),

--) select count(*) from rewards_groups_src     -- cnt = 70803 + 2374882 in 26 sec  with materialise for error_tenders
--union select count(*) from all_basket ;


      rewards_groups as(
         select 
            rg.tran_date,
            rg.LOCATION_NO,
            rg.till_no,
            rg.tran_no,
            rg.item_no,
            rg.item_seq_no,
            first_value(wrewards_promotion_no) over (partition by rg.tran_date,rg.LOCATION_NO,rg.till_no,rg.item_no order by wrewards_promotion_no) as wrewards_promotion_no,
            wrewards_items_count,
            rg.wrewards_discount as wrewards_discount,
            rg.wrewards_sales_value as wrewards_sales_value,
            rg.wrewards_percentage as wrewards_percentage,
            first_value(diff_rewards_promotion_no) over (partition by rg.tran_date,rg.LOCATION_NO,rg.till_no,rg.item_no order by diff_rewards_promotion_no) as diff_rewards_promotion_no,
            diff_rewards_items_count,
            rg.diff_rewards_discount as diff_rewards_discount,
            rg.diff_rewards_sales_value as diff_rewards_sales_value,
            rg.diff_rewards_percentage as diff_rewards_percentage
         from
            rewards_groups_src rg
         ),
      rewards as (
         select  /*+ parallel(rg,4) */
            rg.tran_date,
            rg.LOCATION_NO,
            rg.till_no,
            rg.tran_no,
            rg.item_no,
            rg.wrewards_promotion_no,
            sum(rg.wrewards_items_count) as wrewards_items_count,
            sum(rg.wrewards_discount) as wrewards_discount,
            sum(rg.wrewards_sales_value) as wrewards_sales_value,
            round(sum(rg.wrewards_discount) / sum(rg.wrewards_sales_value),2) as wrewards_percentage,
            rg.diff_rewards_promotion_no,
            sum(rg.diff_rewards_items_count) as diff_rewards_items_count,
            sum(rg.diff_rewards_discount) as diff_rewards_discount,
            sum(rg.diff_rewards_sales_value) as diff_rewards_sales_value,
            round(sum(rg.diff_rewards_discount) / sum(rg.diff_rewards_sales_value),2)  as diff_rewards_percentage
         from
            rewards_groups rg
         group by
            rg.tran_date,
            rg.LOCATION_NO,
            rg.till_no,
            rg.tran_no,
            rg.item_no,
            rg.wrewards_promotion_no,
            rg.diff_rewards_promotion_no
         ),

-- select count(*) from rewards  
--union select count(*) from all_basket  ;  --  128 sec       /  51742 + 2374882  in 34 sec with materialise for error_tenders

      BI_DLY as (
 
         select  /*+ parallel(a,4) parallel(r,4)  */
           a.* ,
           r.wrewards_promotion_no,
           r.wrewards_items_count,
           r.wrewards_discount,
           r.wrewards_sales_value,
           r.wrewards_percentage,
           r.diff_rewards_promotion_no,
           r.diff_rewards_items_count,
           r.diff_rewards_discount,
           r.diff_rewards_sales_value,
           r.diff_rewards_percentage
         from
           all_basket a
           left join rewards r on
                 a.tran_date       = r.tran_date
                 and a.location_no = r.location_no
                 and a.till_no     = r.till_no
                 and a.tran_no     = r.tran_no
                 and a.item_no     = r.item_no
                 
     )

        
--  ************************************************

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
			BI_DLY dly
			inner join dim_calendar CAL
				on dly.tran_date = CAL.calendar_date
			LEFT JOIN w7071603.tmp_mart_sales_cust_acc_mly MNTHLY
			  ON DLY.CUSTOMER_NO = MNTHLY.CUSTOMER_NO and mnthly.CAL_YEAR_MONTH_NO=p_ym_todo
		WHERE
			DLY.TRAN_DATE = dy_rec.calendar_date
			and DLY.TOTAL_BASKET is NOT null;
      

    g_recs_inserted :=  SQL%ROWCOUNT;
    mth_rec_cnt := mth_rec_cnt + g_recs_inserted;
    
    commit;
    
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted||' for day '||dy_rec.day;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- Update take-on control table  ------------------
   
     merge into w7071603.SALES_TAKEON_BI_DAY_CTL bi_ctl
     using (
       select to_char(dy_rec.calendar_date, 'YYYYMMDD') YR_MTH_DY_NO,
             'Y' DY_done_ind,
             g_recs_inserted DY_REC_COUNT, 
             sysdate DY_done_date from DUAL ) newvals
     on ( bi_ctl.YR_MTH_DY_NO = newvals.YR_MTH_DY_NO )
     when matched then 
       update set
           DY_done_ind = 'Y'
          ,DY_REC_COUNT = g_recs_inserted
          ,DY_done_date = sysdate
     when not matched then
        insert (
                  YR_MTH_DY_NO
                 ,DY_DONE_IND
                 ,DY_REC_COUNT
                 ,DY_DONE_DATE )
             values ( 
                  newvals.YR_MTH_DY_NO
                 ,newvals.DY_DONE_IND
                 ,newvals.DY_REC_COUNT
                 ,newvals.DY_DONE_DATE  );
      
     commit; 
           
   end loop;


   g_success := true;
    
    
exception

   when others then
      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );

    rollback;
    g_success := false;
    raise;

end SALES_MTH_LOAD;
	
 
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

    l_text := 'WFS Sales Mart data take-on STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    execute immediate 'alter session enable parallel dml';
   
    l_text :=  'Dropping Indexes I10_WFS_MART_SALES_MLY, I20_WFS_MART_SALES_MLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
     DROP_INDEX_IF_EXIST('DWH_WFS_PERFORMANCE','I10_WFS_MART_SALES_MLY');
     DROP_INDEX_IF_EXIST('DWH_WFS_PERFORMANCE','I20_WFS_MART_SALES_MLY');

    l_text :=  'WFS_MART_SALES_MLY take-on busy...';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - starts');

   SELECT max(YR_MTH_NO) into g_ym_todo   -- latest to oldest
   FROM W7071603.SALES_TAKEON_CTL
   where BI_DONE_IND is null
      or ( BI_DONE_IND <> 'Y'       -- days not already done
           and BI_DONE_IND <> 'S'); -- 'S' for skip
      
   g_done_stat:='H';
   SELECT BI_DONE_IND into g_done_stat   -- check if hold is placed
   FROM W7071603.SALES_TAKEON_CTL
   where YR_MTH_NO = g_ym_todo;
  
--SYS.DBMS_OUTPUT.PUT_LINE('loopstat  '||to_char(g_ym_todo)||'stat:'||nvl(g_done_stat,'null') );
   SAFE_WINDOW_CHECK('08:00','18:30', g_in_safe_window); -- avoid batch window

   while g_ym_todo is not null 
         and ( g_done_stat <> 'H' or g_done_stat is null )
         and g_success = TRUE
         and g_in_safe_window = TRUE
   loop
   
   --  SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'processing month  '||g_ym_todo );
      l_text :=  'processing month  '||g_ym_todo;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      SALES_MTH_LOAD(g_ym_todo, g_success);
      
      if g_success then
      
        l_text :=  dwh_constants.vc_log_records_inserted||mth_rec_cnt||' for month '||g_ym_todo;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
         
        update W7071603.SALES_TAKEON_CTL
           set
           bi_done_ind = 'Y',
           bi_count = 
            ( select sum(DY_REC_COUNT) cnt from  W7071603.SALES_TAKEON_BI_DAY_CTL
              where trunc(YR_MTH_DY_NO/100, 0) = g_ym_todo ),
           bi_done_date = sysdate
           where YR_MTH_NO = g_ym_todo ; 
           commit;
      
      end if;
      
      SELECT max(YR_MTH_NO) into g_ym_todo
      FROM W7071603.SALES_TAKEON_CTL
      where BI_DONE_IND is null
      or ( BI_DONE_IND <> 'Y'       -- days not already done
           and BI_DONE_IND <> 'S'); -- 'S' for skip
      
      g_done_stat:='H';
      SELECT BI_DONE_IND into g_done_stat   -- check if hold is placed
      FROM W7071603.SALES_TAKEON_CTL
      where YR_MTH_NO = g_ym_todo;

      SAFE_WINDOW_CHECK('08:00','18:30', g_in_safe_window); -- avoid batch window
   
   end loop;
   
   if g_success then

--/* temp skip creating indexes until full take-on complete  **************

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Re-creating Index (Customer_no)...' );

      l_text :=  'Restoring Index I10_WFS_MART_SALES_MLY...';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       
      execute immediate 'CREATE INDEX '||
        '"DWH_WFS_PERFORMANCE"."I10_WFS_MART_SALES_MLY" ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_MLY" '||
        '("CUSTOMER_NO") '|| 
        'NOlogging TABLESPACE "'||l_tablespace||'"  PARALLEL (degree 16)';
      
      execute immediate 'ALTER INDEX '||
        '"DWH_WFS_PERFORMANCE"."I10_WFS_MART_SALES_MLY" LOGGING NOPARALLEL ';


        
      l_text :=  'Restoring Index I20_WFS_MART_SALES_MLY...';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       
      execute immediate 'CREATE INDEX '||
        '"DWH_WFS_PERFORMANCE"."I20_WFS_MART_SALES_MLY" ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_MLY" '||
        '("ITEM_NO") '|| 
        'NOlogging TABLESPACE "'||l_tablespace||'"  PARALLEL (degree 16)';
      
      execute immediate 'ALTER INDEX '||
        '"DWH_WFS_PERFORMANCE"."I20_WFS_MART_SALES_MLY" LOGGING NOPARALLEL ';


--******************* */


/* skip, as new partitions will be analysed by batch job overnight **********
  
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Gathering table stats...' );
 
      l_text :=  'Gathering table stats for table WFS_MART_SALES_MLY...';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
      -- gather stats partition by partition for optimal resource usage.
      -- this code supplied by Sean Poulter
      FOR r
        IN (  SELECT num_rows,
                     last_analyzed,
                     partition_name,
                     table_name
--                FROM dba_Tab_partitions  -- privilege issues on dev
                FROM all_Tab_partitions
               WHERE     table_name = 'WFS_MART_SALES_MLY'
                     AND table_owner = 'DWH_WFS_PERFORMANCE'
                     AND LAST_ANALYZED IS NULL
            ORDER BY LAST_ANALYZED DESC)
      LOOP
        BEGIN
           DBMS_STATS.GATHER_TABLE_STATS (ownname            => 'DWH_WFS_PERFORMANCE',
                                          tabname            => 'WFS_MART_SALES_MLY',
                                          partname           => r.partition_name,
                                          estimate_percent   => 10,
                                          degree             => 1,
                                          granularity        => 'PARTITION',
                                          cascade            => TRUE);
        END;
      END LOOP;

************** */


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


end tmp_wfs_mart_sales_takeon;
