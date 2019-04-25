--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_650U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_650U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Sales Mart - load daily sales basket item mart 
--  Date:        2017-01-31
--  Author:      Naresh Chauhan
--  Purpose:     Load/Update Daily Sales basket item Mart
--               
--               Isolates groups of daily records by last_updated_date to capture late-comers.
--               This impacts on load time, changing from 2 mins to 15 mins per daily group.              
--               This is acceptible as the load runs daily, and only 1 group of records.   
--
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    cust_basket_tender
--                    apex_wfs_tender_types
--                    cust_basket_item
--                    dim_item
--                    cust_basket_aux
--
--               Output - wfs_mart_sales_basket_item
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-01-31 N Chauhan - created
--  2017-03-27 N Chauhan - finalised and use last_updated_date to catch late comers.
--  2017-05-17 N Chauhan - provide window for latecomers for performance.
--  2017-05-17 N Chauhan - restructured for merge to accommodate duplicates due to group by last_update_date
--  2017-05-22 N Chauhan - retention handling/ index restore/ stats gather  added.
--  2017-05-24 N Chauhan - retention handling excluded, due reservations about partition deletions by DWH team
--  2017-05-26 N Chauhan - check, log and exit if no data to process.
--  2017-05-29 N Chauhan - fix count in logging.
--  2018-09-26 N Chauhan - update APEX_WFS_TENDER_TYPE for any new TENDER_TYPE_DETAIL_CODE's.
--  2018-09-26 N Chauhan - Already-processed count threshold increased to ignore "future" records from prev day.
--  2018-10-04 N Chauhan - create index parallel degree changed from 16 to 8 as per dba request.
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
--g_sub                integer       :=  0;
--g_rec_out            wfs_mart_sales_basket_item%rowtype;
--g_found              boolean;
g_date               date          := trunc(sysdate);

--g_start_week         number         ;
--g_end_week           number          ;
--g_yesterday          date          := trunc(sysdate) - 1;


L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_650U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales Mart - load daily basket item mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--g_ym_todo integer:=null;
--g_qry_text varchar2(1000);
g_success boolean:= TRUE;
--g_job_desc varchar2(100):= 'sales_takeon_bi';
--g_done_stat varchar(1);
--rec_cnt number(11);
g_date_start date;
g_date_end date;
g_date_to_do date;
g_late_window integer:=45;   -- window for tran_date to check for late comers for performance.
g_mrt_chk_start_dt date;
g_done_date date;
g_recs_cnt_day   integer   :=  0;
g_recs_tnd_cnt_inserted integer :=  0;



g_retention_success  boolean:= false;
g_retention_drop_cnt integer:=0;
g_analysed_count integer:=0;
g_analysed_success boolean:= false;

g_idx_drop_success boolean:= false;
g_idx_existed  boolean:= false;

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




procedure APEX_WFS_TENDER_TYPE_new_add(p_date_to_do in date, g_success out boolean) as

begin   

insert /*+ append */
   into apex_app_wfs_01.apex_wfs_tender_type i
    (
      TENDER_TYPE_DETAIL_CODE,
      TENDER_TYPE_DETAIL,
      LAST_UPDATED_DATE,
      UPDATED_BY
      --CREATED_DATE,  -- updated by trigger
      --CREATED_BY
     )
   select /*+ materialize parallel (cbk,4)   full(cbk)  */
      distinct 
      tender_type_detail_code,
      'Uncategorised',
--      g_date as last_updated_date,
      trunc(sysdate) as last_updated_date,
      l_module_name as updated_by
      --trunc(sysdate) as created_date,
      --'Proc' as created_by
    from
       dwh_cust_performance.cust_basket_tender  cbk  
    where  
         -- cbk.last_updated_date  = trunc(sysdate-1)
         --        and cbk.tran_date between sysdate-45 and sysdate
         cbk.last_updated_date  = p_date_to_do
-- ***                 cbk.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
         and cbk.tran_date between sysdate-g_late_window and sysdate

    and not ( tender_type_detail_code is null )  -- NB: '' (empty string) is same as NULL  
    and not ( substr(to_char(tender_no),1,6) in ('410375','410374','400154') )
    and not ( tender_type_code = 1292 and tender_no > 0 and tender_type_detail_code = 'U/K' )
    and not ( tender_type_code = 1290 and tender_type_detail_code = 'U/K' and tender_no > 0 )
    and not exists
          (select /*+ nl_aj */ * from apex_app_wfs_01.apex_wfs_tender_type
           where  TENDER_TYPE_DETAIL_CODE  = cbk.tender_type_detail_code )
   ;
   g_recs_tnd_cnt_inserted := g_recs_tnd_cnt_inserted + SQL%ROWCOUNT;
   commit;

   g_success := true;


exception

   when others then
      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||
            'APEX_WFS_TENDER_TYPE_NEW_ADD Error '||sqlcode||' '||sqlerrm );

    rollback;
    g_success := false;
    raise;


end APEX_WFS_TENDER_TYPE_new_add;




procedure SALES_BSKT_ITM_DLY_LOAD(p_date_to_do in date, g_success out boolean) as


begin

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'busy with day '||p_date_to_do );

  apex_wfs_tender_type_new_add(p_date_to_do, g_success);
  if g_success = false then
     return;
  end if;



  merge /*+ APPEND   parallel(ti, 4) */ 
--   into W7071603.TMP_MART_SALES_BASKET_ITEM ti using (
   into DWH_WFS_PERFORMANCE.WFS_MART_SALES_BASKET_ITEM ti using (

      with
      dly_bsk_tender_1 as (
         select /*+ materialize parallel (cbk,4)   full(cbk)  */
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
/**/              dwh_cust_performance.cust_basket_tender  cbk  
         where
                 cbk.last_updated_date  = p_date_to_do
-- ***                 cbk.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
                 and cbk.tran_date between sysdate-g_late_window and sysdate
                 and cbk.payment_account_no is null
                 and not(cbk.tender_type_detail_code in (
                         'CASH','CASHL','CASHLR','CASHP','CASHPR','CASHREF','CASHRV','CASHV')
                         and (tender_selling - change_selling)=-0.05)

         ),


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


      error_tenders as (
         select /*+ materialize parallel (itm,4)   full(itm) materialize  */
                    itm.tran_date,
                    itm.location_no,
                    itm.till_no,
                    itm.tran_no,
                    itm.ITEM_TRAN_SELLING - itm.DISCOUNT_SELLING as adjustment,
                    tenders.change_amt
         from
            (
             select distinct /*+ materialize parallel(mx,4) */
               mx.tran_date,
               mx.location_no,
               mx.till_no,
               mx.tran_no,
               max(mx.max_tender_perc) as max_tender_perc
             from (
                    select /*+ materialize parallel(a,4) */
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
            ) problems,         
            dwh_cust_performance.cust_basket_item itm, tender_totals_by_type_src tenders
         where
            itm.last_updated_date = p_date_to_do
--***       itm.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY') 
            and itm.tran_date between sysdate-g_late_window and sysdate
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



      tender_totals_by_type as (
         select /*+materialize parallel(src,4) full(src) full(err)  */
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



      dup_keys as (       
         -- get keys of transactions having multiple customer_no's
         select  /*+ materialize parallel (gg,4)   full(gg) */
            gg.location_no,
            gg.till_no,
            gg.tran_no,
            gg.tran_date,
            count(*) cnt
         from (           
               -- determine transactions having multiple customer_no's 
               select  /*+ materialize parallel (g,4)   full(g) */
                  g.location_no,
                  g.till_no,
                  g.tran_no,
                  g.tran_date,
                  g.item_no,
                  count(*) cnt
               from (
                     select  /*+ materialize parallel (bski,4)   full(bski) */
                        bski.location_no,
                        bski.till_no,
                        bski.tran_no,
                        bski.tran_date,
                        bski.item_no,
                        customer_no,
                        count(*) cnt
                     from dwh_cust_performance.cust_basket_item bski
                     where last_updated_date = p_date_to_do   
-- **                     where tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY')
                       and tran_date between sysdate-g_late_window and sysdate
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
            from  dwh_cust_performance.cust_basket_item x, dup_keys d
            where 
               d.location_no = x.location_no 
               and d.till_no = x.till_no
               and d.tran_no = x.tran_no
               and x.last_updated_date = p_date_to_do
-- ***               and x.tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY')
               and x.tran_date between sysdate-g_late_window and sysdate
               and x.customer_no is null  /* invalid records */
         ),      


      basket_items1 as (
         select  /*+ materialize parallel (bsk,4)   full(bsk) */
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
			dwh_cust_performance.cust_basket_item bsk

            left outer join invalid_recs x  on (
                   x.LOCATION_NO=bsk.LOCATION_NO
               and x.TILL_NO=bsk.TILL_NO
               and x.TRAN_NO=bsk.TRAN_NO
               and x.TRAN_DATE=bsk.TRAN_DATE
               and x.ITEM_SEQ_NO=bsk.ITEM_SEQ_NO
               and x.ITEM_NO=bsk.ITEM_NO )
          where bsk.last_updated_date = p_date_to_do  
-- ***         where bsk.tran_date = dy_rec.calendar_date  -- to_date('10JuN2016','DDMONYYYY')
           and bsk.tran_date between sysdate-g_late_window and sysdate
           and x.LOCATION_NO is null   -- exclude invalid records

         ),


      basket_items as (
         select /*+ materialize parallel (bsk,4) parallel(itm,4)   full(bsk) full(itm)  */
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


      basket_items_summary as (
         select /*+ materialize parallel(i,4) */
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


      all_basket_src as (
          select  /*+ materialize parallel(a,4) parallel(b,4) */
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


      excl_empties as (
         SELECT /*+ materialize parallel(abs, 4) */
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
         SELECT  /*+ materialize parallel(ee,4) full(ee) */
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
         select /*+ materialize parallel(a, 4) */
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
            DWH_CUST_PERFORMANCE.CUST_BASKET_AUX  a
         where
            a.last_updated_date = p_date_to_do
-- **             a.tran_date = dy_rec.calendar_date  -- to_date('13JAN2017','DDMONYYYY')
            and a.tran_date between sysdate-g_late_window and sysdate
            and    a.TRAN_TYPE_CODE in ('LOYALTY','ATGLOY','WFS')
--                        and   a.promotion_discount_amount != 0
            and   a.promotion_discount_amount <> 0
         ),

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
         select  /*+ materialize parallel(rg,4) */
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
         )


         -- ######### main select ########################################## --

         select  /*+ materialize parallel(a,4) parallel(r,4)  */
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
           r.diff_rewards_percentage,
           trunc(g_date) as LAST_UPDATED_DATE
         from
           all_basket a
           left join rewards r on
                 a.tran_date       = r.tran_date
                 and a.location_no = r.location_no
                 and a.till_no     = r.till_no
                 and a.tran_no     = r.tran_no
                 and a.item_no     = r.item_no
        ) new_recs on (

        new_recs.TRAN_DATE    = ti.TRAN_DATE and
        new_recs.LOCATION_NO  = ti.LOCATION_NO and
        new_recs.TILL_NO      = ti.TILL_NO and 
        new_recs.TRAN_NO      = ti.TRAN_NO and
        new_recs.ITEM_NO      = ti.ITEM_NO )

    when not matched then insert (

   		     ti.TRAN_DATE
   	  ,    ti.LOCATION_NO
   	  ,    ti.TILL_NO
   	  ,    ti.TRAN_NO
   	  ,	   ti.ITEM_NO	
   	  ,	   ti.TRAN_TIME	
   	  ,    ti.SUBGROUP_NO
   	  ,    ti.ITEM_TRAN_QTY
   	  ,    ti.CUSTOMER_NO
   	  ,    ti.ITEM_TRAN_AMT
   	  ,    ti.DISCOUNT_SELLING
   	  ,    ti.TOTAL_BASKET
   	  ,    ti.TRAN_WFS_SC
   	  ,    ti.TRAN_WFS_CC
   	  ,    ti.TRAN_DEBIT_CARD
   	  ,    ti.TRAN_HYBRID_CARD
   	  ,    ti.TRAN_VISA
   	  ,    ti.TRAN_MASTER_CARD
   	  ,    ti.TRAN_DINERS_CARD
   	  ,    ti.TRAN_AMEX
   	  ,    ti.TRAN_CASH
   	  ,    ti.TRAN_WFS_VISA_LOYALTY_VOUCHER
   	  ,    ti.TRAN_THRESHOLD_VOUCHER
   	  ,    ti.TRAN_GIFT_CARD
   	  ,    ti.TRAN_BUY_AID
   	  ,    ti.TRAN_TM_VOUCHER
   	  ,    ti.TRAN_UNKNOWN
   	  ,    ti.WREWARDS_PROMOTION_NO
   	  ,    ti.WREWARDS_ITEMS_COUNT
   	  ,    ti.WREWARDS_DISCOUNT
   	  ,    ti.WREWARDS_SALES_VALUE
   	  ,    ti.WREWARDS_PERCENTAGE
   	  ,    ti.DIFF_REWARDS_PROMOTION_NO 
   	  ,    ti.DIFF_REWARDS_ITEMS_COUNT
   	  ,    ti.DIFF_REWARDS_DISCOUNT
   	  ,    ti.DIFF_REWARDS_SALES_VALUE
   	  ,    ti.DIFF_REWARDS_PERCENTAGE
   	  ,    ti.LAST_UPDATED_DATE
   	  )
   values (
           new_recs.TRAN_DATE
      ,    new_recs.LOCATION_NO
      ,    new_recs.TILL_NO
      ,    new_recs.TRAN_NO
      ,    new_recs.ITEM_NO
      ,    new_recs.TRAN_TIME
      ,    new_recs.SUBGROUP_NO
      ,    new_recs.ITEM_TRAN_QTY  
      ,    new_recs.CUSTOMER_NO
      ,    new_recs.ITEM_TRAN_AMT
      ,    new_recs.DISCOUNT_SELLING
      ,    new_recs.TOTAL_BASKET
      ,    new_recs.TRAN_WFS_SC
      ,    new_recs.TRAN_WFS_CC
      ,    new_recs.TRAN_DEBIT_CARD
      ,    new_recs.TRAN_HYBRID_CARD
      ,    new_recs.TRAN_VISA
      ,    new_recs.TRAN_MASTER_CARD
      ,    new_recs.TRAN_DINERS_CARD
      ,    new_recs.TRAN_AMEX
      ,    new_recs.TRAN_CASH
      ,    new_recs.TRAN_WFS_VISA_LOYALTY_VOUCHER
      ,    new_recs.TRAN_THRESHOLD_VOUCHER
      ,    new_recs.TRAN_GIFT_CARD
      ,    new_recs.TRAN_BUY_AID
      ,    new_recs.TRAN_TM_VOUCHER
      ,    new_recs.TRAN_UNKNOWN
      ,    new_recs.WREWARDS_PROMOTION_NO
      ,    new_recs.WREWARDS_ITEMS_COUNT
      ,    new_recs.WREWARDS_DISCOUNT
      ,    new_recs.WREWARDS_SALES_VALUE
      ,    new_recs.WREWARDS_PERCENTAGE
      ,    new_recs.DIFF_REWARDS_PROMOTION_NO 
      ,    new_recs.DIFF_REWARDS_ITEMS_COUNT
      ,    new_recs.DIFF_REWARDS_DISCOUNT
      ,    new_recs.DIFF_REWARDS_SALES_VALUE
      ,    new_recs.DIFF_REWARDS_PERCENTAGE  
      ,    new_recs.LAST_UPDATED_DATE
      )

    when matched then update 
    set
   ti.TRAN_TIME=new_recs.TRAN_TIME
  ,    ti.SUBGROUP_NO=new_recs.SUBGROUP_NO
  ,    ti.ITEM_TRAN_QTY=new_recs.ITEM_TRAN_QTY
  ,    ti.CUSTOMER_NO=new_recs.CUSTOMER_NO
  ,    ti.ITEM_TRAN_AMT=new_recs.ITEM_TRAN_AMT
  ,    ti.DISCOUNT_SELLING=new_recs.DISCOUNT_SELLING
  ,    ti.TOTAL_BASKET=new_recs.TOTAL_BASKET
  ,    ti.TRAN_WFS_SC=new_recs.TRAN_WFS_SC
  ,    ti.TRAN_WFS_CC=new_recs.TRAN_WFS_CC
  ,    ti.TRAN_DEBIT_CARD=new_recs.TRAN_DEBIT_CARD
  ,    ti.TRAN_HYBRID_CARD=new_recs.TRAN_HYBRID_CARD
  ,    ti.TRAN_VISA=new_recs.TRAN_VISA
  ,    ti.TRAN_MASTER_CARD=new_recs.TRAN_MASTER_CARD
  ,    ti.TRAN_DINERS_CARD=new_recs.TRAN_DINERS_CARD
  ,    ti.TRAN_AMEX=new_recs.TRAN_AMEX
  ,    ti.TRAN_CASH=new_recs.TRAN_CASH
  ,    ti.TRAN_WFS_VISA_LOYALTY_VOUCHER=new_recs.TRAN_WFS_VISA_LOYALTY_VOUCHER
  ,    ti.TRAN_THRESHOLD_VOUCHER=new_recs.TRAN_THRESHOLD_VOUCHER
  ,    ti.TRAN_GIFT_CARD=new_recs.TRAN_GIFT_CARD
  ,    ti.TRAN_BUY_AID=new_recs.TRAN_BUY_AID
  ,    ti.TRAN_TM_VOUCHER=new_recs.TRAN_TM_VOUCHER
  ,    ti.TRAN_UNKNOWN=new_recs.TRAN_UNKNOWN
  ,    ti.WREWARDS_PROMOTION_NO=new_recs.WREWARDS_PROMOTION_NO
  ,    ti.WREWARDS_ITEMS_COUNT=new_recs.WREWARDS_ITEMS_COUNT
  ,    ti.WREWARDS_DISCOUNT=new_recs.WREWARDS_DISCOUNT
  ,    ti.WREWARDS_SALES_VALUE=new_recs.WREWARDS_SALES_VALUE
  ,    ti.WREWARDS_PERCENTAGE=new_recs.WREWARDS_PERCENTAGE
  ,    ti.DIFF_REWARDS_PROMOTION_NO =new_recs.DIFF_REWARDS_PROMOTION_NO 
  ,    ti.DIFF_REWARDS_ITEMS_COUNT=new_recs.DIFF_REWARDS_ITEMS_COUNT
  ,    ti.DIFF_REWARDS_DISCOUNT=new_recs.DIFF_REWARDS_DISCOUNT
  ,    ti.DIFF_REWARDS_SALES_VALUE=new_recs.DIFF_REWARDS_SALES_VALUE
  ,    ti.DIFF_REWARDS_PERCENTAGE=new_recs.DIFF_REWARDS_PERCENTAGE
  ,    ti.LAST_UPDATED_DATE=new_recs.LAST_UPDATED_DATE

;

   g_success := true;


exception

   when others then
      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||
            'SALES_BSKT_ITM_DLY_LOAD Error '||sqlcode||' '||sqlerrm );

    rollback;
    g_success := false;
    raise;

end SALES_BSKT_ITM_DLY_LOAD;




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

    l_text := 'WFS SALES daily basket item table update STARTED AT '||
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
    l_text := 'LOAD TABLE: '||'wfs_mart_sales_basket_item' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';


    -- check for any missing days still to be processed, but only for current and previous month
    -- if table is empty, consider only from beginning of previous month 


    g_mrt_chk_start_dt:=trunc(add_months(sysdate,-1),'MM');  -- start checking from beginning of previous month

    -- get latest tran_date processed, ignoring a couple of, if any, future tran_dates
    with 
    dates_processed as (
          select /*+ parallel(t,4) full(t) */
             tran_date
            ,count(*) cnt
          from  DWH_WFS_PERFORMANCE.WFS_MART_SALES_BASKET_ITEM    t
          where tran_date between g_mrt_chk_start_dt and sysdate
          group by tran_date
       )
    select max(tran_date) as tran_date 
    into g_done_date
    from dates_processed
    where cnt > 1000000;   -- faster than using "having in previous group query

    if g_done_date is null then  -- no data in the window checked
       g_date_start := g_mrt_chk_start_dt;  -- start from beginning of previous month
    else 
       g_date_start := g_done_date + 1;     -- start from next day
    end if;


    g_date_end:=g_date;
    g_date_to_do := g_date_start;

    if g_date_start > g_date_end then
       p_success := true;
       l_text :=  dwh_constants.vc_log_ended ||' - no data to process - '||to_char(g_done_date,'YYYY-MM-DD')||' data already processed';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text :=  dwh_constants.vc_log_run_completed ||'  '||to_char(sysdate,'YYYY-MM-DD HH24:MI:SS');
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       return;
    end if;

    l_text :=  'Processing for period '||to_char(g_date_start, 'YYYY-MM-DD')||'  to  '||to_char(g_date_end, 'YYYY-MM-DD');
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- Drop indexes -------------------

    drop_index('I10_WFS_MT_SALES_BSK_ITM');
    drop_index('I20_WFS_MT_SALES_BSK_ITM');


    while g_date_to_do <= g_date_end and g_success = TRUE
    loop

       -- ****** main load *************
       SALES_BSKT_ITM_DLY_LOAD(g_date_to_do, g_success);
       -- ******************************

       g_recs_cnt_day := SQL%ROWCOUNT;
       g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
       g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

       commit;

       l_text :=  'For last_updated_date '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Merged:  '||g_recs_cnt_day;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       g_date_to_do := g_date_to_do +1;

    end loop;



--**************************************************************************************************
-- Retention maintenance
--**************************************************************************************************


/*  -- excluded for now due to reservations on partitions deletions

    -- disable pk and drop pk indexe
    execute immediate 'ALTER TABLE DWH_WFS_PERFORMANCE.WFS_MART_SALES_BASKET_ITEM DISABLE PRIMARY KEY';
    drop_index('PK_WFS_MT_SALES_BSK_ITM');

     -- drop old partitions
     DWH_DBA_WFS.drop_old_partitions_hival_date(
         'DWH_WFS_PERFORMANCE',
         'WFS_MART_SALES_BASKET_ITEM',
         5,
         g_retention_drop_cnt,
         g_retention_success );

     if g_retention_success = false then
        l_text := 'retention management failed';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     else 
        l_text := 'retention management : '||g_retention_drop_cnt||' partitions dropped' ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     end if;   

     g_recs_deleted :=  g_retention_drop_cnt;

     -- restore primary key index

     execute immediate 
       'CREATE UNIQUE INDEX "DWH_WFS_PERFORMANCE"."PK_WFS_MT_SALES_BSK_ITM"' 
       ||'  ON DWH_WFS_PERFORMANCE.WFS_MART_SALES_BASKET_ITEM'
       ||'  ("TRAN_DATE", "LOCATION_NO", "TILL_NO", "TRAN_NO", "ITEM_NO")'
       ||'  noLOGGING'
       ||'  TABLESPACE "'||l_tablespace||'"' 
       ||'  Parallel (degree 16)'
       ||'  LOCAL';


     execute immediate 'Alter index "DWH_WFS_PERFORMANCE"."PK_WFS_MT_SALES_BSK_ITM" logging noparallel';

     -- re-enable constraint     - NB. This must always be done AFTER the PK index is created.

     execute immediate 'ALTER TABLE "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_BASKET_ITEM" ENABLE VALIDATE'
       ||'  CONSTRAINT PK_WFS_MT_SALES_BSK_ITM';

*/ 


--**************************************************************************************************
-- restore indexes 
--**************************************************************************************************

     l_text := 'restoring index I10_WFS_MT_SALES_BSK_ITM ...';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

     execute immediate 
       'CREATE INDEX "DWH_WFS_PERFORMANCE"."I10_WFS_MT_SALES_BSK_ITM"'
       ||'  ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_BASKET_ITEM"'
       ||'  ("CUSTOMER_NO")'
       ||'  NOlogging TABLESPACE "'||l_tablespace||'" PARALLEL (degree 8)';

     execute immediate 'ALTER  INDEX "DWH_WFS_PERFORMANCE"."I10_WFS_MT_SALES_BSK_ITM" LOGGING NOPARALLEL';

     l_text := 'restoring index I20_WFS_MT_SALES_BSK_ITM ...';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

     execute immediate 
       'CREATE INDEX "DWH_WFS_PERFORMANCE"."I20_WFS_MT_SALES_BSK_ITM"'
       ||'  ON "DWH_WFS_PERFORMANCE"."WFS_MART_SALES_BASKET_ITEM"'
       ||'  ("TRAN_NO")'
       ||'  NOlogging TABLESPACE "'||l_tablespace||'"    LOCAL PARALLEL (degree 8)';

     execute immediate 'ALTER  INDEX "DWH_WFS_PERFORMANCE"."I20_WFS_MT_SALES_BSK_ITM" LOGGING NOPARALLEL';

     l_text := 'indexes I10_WFS_MT_SALES_BSK_ITM, I20_WFS_MT_SALES_BSK_ITM created';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 


--**************************************************************************************************
-- gather statistics
--**************************************************************************************************

     l_text := 'gathering statistics ...';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

    -- analyse all unanalysed partitions, one partition at a time
    DWH_DBA_WFS.stats_partitions_outstanding (
         'DWH_WFS_PERFORMANCE',
         'WFS_MART_SALES_BASKET_ITEM',
         g_analysed_count,
         g_analysed_success );

     if g_analysed_success = false then
        l_text := 'gather_table_stats failed';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     else 
        l_text := 'gather_table_stats : '||g_analysed_count||' partitions analysed' ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     end if;   


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED  '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    if g_recs_tnd_cnt_inserted > 0 then 
      l_text :=  'RECORDS INSERTED into APEX_WFS_TENDER_TYPE: '||g_recs_tnd_cnt_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;


--    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted||' partitions';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||'  '||to_char(sysdate,'YYYY-MM-DD HH24:MI:SS');
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
       l_message :=  dwh_constants.vc_log_aborted ||'  '||to_char(sysdate,'YYYY-MM-DD HH24:MI:SS');
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_message);
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       l_message :=  dwh_constants.vc_log_aborted ||'  '||to_char(sysdate,'YYYY-MM-DD HH24:MI:SS');
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_message);
       rollback;
       p_success := false;
       raise;


end wh_prf_wfs_650u;
