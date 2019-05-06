--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_652U_20170803
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_652U_20170803" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Sales Mart - Load Monthly Customer Account Mart 
--  Date:        2017-05-03
--  Author:      Naresh Chauhan
--  Purpose:     Load Monthly Customer Account Mart
--               
--                    
--               THIS JOB RUNS DAILY, but will process once a month 
--  Tables:      Input  - 
--                    dim_customer
--                    wfs_all_prod_mnth
--                    wfs_stmt_st_crd_ploan
--                    wfs_stmt_st_crd_ploan
--                    fnd_wfs_crd_acc_mly
--                    fnd_wfs_customer_absa
--                    dim_customer_portfolio
--                    cust_wod_tier_mth_detail
--                    cust_lss_lifestyle_segments
--
--               Output - wfs_mart_sales_cust_acc_mly

--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2017-05-03 N Chauhan - created
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_652U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS Sales Mart - Load Monthly Customer Account Mart';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


g_success boolean:= TRUE;
g_date_start date;
g_date_end date;
g_date_to_do date;


g_run_date date;
g_2nd_of_month date;
g_today date;
l_sql_stmt varchar2(200);



procedure prepare_customer_data(p_date_to_do in date)
  
  is
  exec_mnth date;
  loyalty_start date;
  ap_yr number;
  ap_mnth number;
  
begin

/*
	Program calculates values for the preceding month (exec_mnth)
*/

exec_mnth		:= p_date_to_do;
ap_yr 			:= extract(year from exec_mnth);
ap_mnth 		:= extract(month from exec_mnth);
loyalty_start 	:= add_months(trunc(exec_mnth,'MM'),-12);


insert /*+ APPEND*/ 

into DWH_WFS_PERFORMANCE.WFS_MART_SALES_CUST_ACC_MLY(
   CAL_YEAR_MONTH_NO	
  ,CUSTOMER_NO
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
  ,CC_SHOPABLE_IND
  ,FOODS_SEGMENT
  ,NON_FOODS_SEGMENT
  ,LOYALTY_TIERS
  ,LITTLEWORLD_IND
  ,MYSCHOOL_IND
  ,WWDIFFERENCE_IND
  ,WODIFFERENCE_IND
  ,DISCOVERY_IND
  ,LAST_UPDATED_DATE
)
select    
   TO_NUMBER(TO_CHAR(exec_mnth,'yyyymm'))	
  ,CUSTOMER_NO
  ,ID_NUMBER
  ,WFS_CUSTOMER_NO
  ,CUSTOMER_STATUS
  
  ,DWH_STDLIB_WFS.get_age_from_id(ID_NUMBER, last_day(exec_mnth) + 1) 

as AGE


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
  ,CC_SHOPABLE_IND
  ,FOODS_SEGMENT
  ,NON_FOODS_SEGMENT
  ,LOYALTY_TIERS
  ,LITTLEWORLD_IND
  ,MYSCHOOL_IND
  ,WWDIFFERENCE_IND
  ,WODIFFERENCE_IND
  ,DISCOVERY_IND
  ,TRUNC(SYSDATE)
from(
  with  
  dim_c2_customer as (
    select /*+ PARALLEL(CST,4)*/          
      CST.CUSTOMER_NO ,                            /* Extract Customer Information */
      CST.identity_document_code,
      CST.WFS_CUSTOMER_NO ,
      CST.customer_status,           
      1                      as      c2_ind,
      CST.gender_code as gender,
      CST.age_acc_holder
    from 
      DIM_CUSTOMER CST
    WHERE 
      CST.CUSTOMER_NO IS NOT NULL and CST.CUSTOMER_NO > 0
  ),                  
  Finperiod As (
    Select 
      Max(Fin_year_no*100+Fin_month_no)   As  Fin         
    FROM 
      dim_calendar
    where 
      CAL_YEAR_MONTH_NO = to_char(add_months(trunc(exec_mnth,'MM'), - 1),'YYYYMM')      
  ),                       
  trad_cal_yr_mnth As (
    Select 
      to_number(substr(fin,1,4)) trading_calendar_year, 
      to_number(substr(fin,5,2)) trading_calendar_month    
    FROM 
      FinPeriod
  ),
  cust_segment_lifestyle As (
    select /*+ PARALLEL(SEG,4)*/  
      seg.primary_customer_identifier     as  primary_account_no,
    case
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 1 then 'Modern Man'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 2 then 'Woolies for Kids'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 3 then 'Classic Cross-Shopper'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 4 then 'Modern Cross-Shopper'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 5 then 'Bare Necessities'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 6 then 'Classic Man'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 7 then 'Accessorise Me'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 8 then 'Modern Basics'
      when seg.segment_type = 'Non-Foods' and seg.segment_no = 9 then 'Premium Tastes'
      when seg.segment_type = 'Foods'     and seg.segment_no = 1 then 'Traditional'
      when seg.segment_type = 'Foods'     and seg.segment_no = 2 then 'Lunch'
      when seg.segment_type = 'Foods'     and seg.segment_no = 3 then 'Premium'
      when seg.segment_type = 'Foods'     and seg.segment_no = 4 then 'Convenience'
      when seg.segment_type = 'Foods'     and seg.segment_no = 6 then 'Budget'
      when seg.segment_type = 'Foods'     and seg.segment_no = 7 then 'Family'
      when seg.segment_type = 'Foods'     and seg.segment_no = 8 then 'Treats'                                         
      when seg.segment_no = null then 'No Segment' 
      else 'No Segment'
    end as segment,
    case
      when segment_type = 'Non-Foods' then 'Non_Foods_Segment'
      when segment_type = 'Foods' then 'Foods_Segment'
    end as segment_type_descr
    from 
      cust_lss_lifestyle_segments seg,
      trad_cal_yr_mnth
    where 
      seg.fin_year_no  = trad_cal_yr_mnth.trading_calendar_year
      and seg.fin_month_no = trad_cal_yr_mnth.trading_calendar_month            
   ),
   segment_lifestyle_transpose as (
    select /*+ PARALLEL(foods,4) PARALLEL(non_foods,4) */
      case 
        when foods.primary_account_no is null then non_foods.primary_account_no
        else foods.primary_account_no
      end               as primary_account_no,    
      non_foods.segment as Non_Foods_Segment,
      foods.segment     as Foods_Segment
    from 
    (
      select 
        primary_account_no,
        segment
      from 
        cust_segment_lifestyle   
      where
        segment_type_descr = 'Foods_Segment'
    ) foods
    full outer join
    (
      select 
        primary_account_no,
        segment
      from 
        cust_segment_lifestyle    
      where
        segment_type_descr = 'Non_Foods_Segment'
    ) non_foods
    on foods.primary_account_no = non_foods.primary_account_no
   ),         
  customer_segment_lifestyle as (
    select /*+ PARALLEL(A,4) PARALLEL(B,4)*/  
      b.customer_no, 
      b.identity_document_code,
      b.WFS_CUSTOMER_NO,
      b.gender,
      b.customer_status,
      a.foods_segment,
      a.non_foods_segment
    from 
      segment_lifestyle_transpose a
      inner join  
      dim_c2_customer b
      on  
      a.primary_account_no = b.customer_no
  ),
  
  /*
    There could be duplicate accounts when matching on ID number  - eg a previously
    closed account. We only require the LATEST. The row_number() function allows us to identify
	this record.
  */
  cust_all_prod as (
    select 
      a.*
    from (	
      select  /*+ PARALLEL(ALL_PRD,4) PARALLEL(STMT,4)*/
        all_prod.WFS_CUSTOMER_NO,                                                            
        all_prod.PRODUCT_CODE_NO as wfs_product,
        all_prod.wfs_account_no,
        all_prod.account_status as  sc_account_status,  
        all_prod.credit_limit as  sc_credit_limit,
        all_prod.current_balance as  sc_current_balance,
        all_prod.open_to_buy as  SC_OPEN_TO_BUY,
        all_prod.date_opened as  sc_date_opened,
        all_prod.date_closed as  sc_date_closed,
        all_prod.date_chgoff as  sc_date_chgoff,
        all_prod.date_last_stmt as  sc_date_last_statement,
        case 
          when all_prod.account_status in ('A','D','I') and all_prod.open_to_buy > 0 then 1 /*'Y'*/        
          else 0 /*'N' */
        end as sc_shopable_ind,        
        case 
          when all_prod.date_closed is not null then 0
          else floor(months_between(last_day(exec_mnth),all_prod.date_opened))
        end as sc_mob,
        all_prod.identity_no,
        all_prod.bureau_score,
        all_prod.behaviour_score01,
        case
          when risk_cats.risk_category is not null then risk_cats.risk_category
          else 'N/A'
        end as ISC_RISK_CAT_STMT,
        stmt.delinquency_cycle,
        stmt.statement_date	as	stmt_date_last_statement,
        row_number() over (partition by all_prod.identity_no order by all_prod.date_opened desc, date_last_stmt desc) as num
      from 
        (
		/*
			For some obscure reason, Oracle wants this as a sub query
		*/
          select 
            a.*
          from 
            wfs_all_prod_mnth a
          where 
            a.PRODUCT_CODE_NO in (1, 2, 3, 4, 5, 6, 7, 9, 21)    
            and a.identity_no is not null 
            and a.identity_no not in ('0','000000000000000','00000000000','0000000000000','000000000000','0000000000001')
            and a.credit_limit > 1 
			and a.CAL_YEAR_NO = ap_yr
			and a.cal_month_no = ap_mnth 
        ) all_prod
        left join apex_app_wfs_01.apex_wfs_isc_risk_cat  risk_cats
          on (
            all_prod.bureau_score between risk_cats.min_bureau_score and risk_cats.max_bureau_score
            and all_prod.behaviour_score01 between risk_cats.min_behaviour_score and risk_cats.max_behaviour_score
          )
        left join DWH_WFS_PERFORMANCE.WFS_STMT_ST_CRD_PLOAN stmt
          on (
            all_prod.wfs_account_no=stmt.wfs_account_no 
            and all_prod.PRODUCT_CODE_NO = stmt.PRODUCT_CODE_NO 
            and CYCLE_6 = to_number(to_char(exec_mnth,'YYYYMM'))
          )   
    ) a
    where a.num = 1
  ),
  /*
	A customer could have had more than 1 credit card account over time. We need to pull information for the LAST account
	which was opened. The row_number() function identifies the LATEST account opened for a specific id_number
  */
  wfs_cc_customer_pre as(
    SELECT 	/*+ PARALLEL(t1,4) PARALLEL(t2,4)  */
      t2.id_number								as identity_document_code,
      t1.customer_key, 
      CASE 	WHEN t1.card_account_status_code in('AAA','D1A','O1A') THEN 1 /*'Y' */
          ELSE 0 /*'N' */
      END 										as 	cc_shopable_ind,	
      t1.account_number 							as 	cc_account_no, 		
      t1.card_account_status_code 				as 	cc_account_status,  
      t1.open_date 											as 	cc_date_opened, 
      t1.closed_date											as	cc_date_closed,	
      t1.status_date											as	cc_status_date,	
      floor(months_between(last_day(exec_mnth),t1.open_date))	as 	cc_mob,			
      (account_balance*-1) + t1.total_budget_balance_amt as 	cc_current_balance, 
      (budget_limit_amount + purchase_limit_amt) 		as 	cc_credit_limit,		
      (budget_limit_amount + purchase_limit_amt)-((account_balance*-1) + t1.total_budget_balance_amt) as cc_open_to_buy, 
      case 
        when 
          t1.card_account_status_code in ('O2D','O3D','O4D','O5D','O6D','O7D','O8D') 
          and t1.delinquent_cycles_count = 1 then 'D1A' 
        else t1.card_account_status_code 
      end as cc_account_status_class,
      case 
        when 
          (substr(t1.card_account_status_code,1,1) = 'C')
          or (substr(t1.account_number,1,3) in ('666','777','222'))
          or (substr(t1.card_account_status_code,1,1) = 'F' )
          or (substr(t1.account_number,1,3) in ('999','333')) 
          or (t1.card_account_status_code in ('LAP','LEP','ISP','ESP','LWP','LLL'))
          or (t1.card_account_status_code in ('NWD','NUD','WAP'))
          then 1
         else 0 
      end as cc_exclude_ind,	
      case 
        when t1.total_budget_balance_amt is null then 0 
        else t1.total_budget_balance_amt 
      end as total_budget_balance_amt,
      case 
        when (
          case 
            when account_balance < 0 then account_balance * -1 
          end) is null then 0 
        else (case when account_balance < 0 then account_balance * -1 end) 
      end as debit_balance,
      t2.secondary_card_indicator as cc_secondary_card_ind,		
      case 
        when t1.card_account_status_code in ('LAP','LEP','ISP','ESP','LWP','LLL','DLP','TLP') 
        or substr(t1.account_number,1,3) in ('999','333') then 'Legal'
        when t1.card_account_status_code in ('A0P') then 'Arrangements 1_0'
        when t1.card_account_status_code in ('A1P') then 'Arrangements 1_1'
        when t1.card_account_status_code in ('A2P') then 'Arrangements 1_2'
        when t1.card_account_status_code in ('A3P') then 'Arrangements 1_3'
        when t1.card_account_status_code in ('A4P') then 'Arrangements 1_4'
        when t1.card_account_status_code in ('A5P') then 'Arrangements 1_5'
        when t1.card_account_status_code in ('A6P') then 'Arrangements 1_6'
        when t1.card_account_status_code in ('A7P') then 'Arrangements 1_7'
        when t1.card_account_status_code in ('D0P','D1P','D2P','D3P','D4P','T0P') then 'Debt Counselling'
        when t1.card_account_status_code in ('D1A','C1D','T1P') then 'Cycle 1'
        when t1.card_account_status_code in ('D2A','D2D','C2D','T2P','O2D') then 'Cycle 2'
        when t1.card_account_status_code in ('D3D','C3P','T3P','O3D') then 'Cycle 3'
        when t1.card_account_status_code in ('D4D','C4P','T4P','O4D') then 'Cycle 4'
        when t1.card_account_status_code in ('D5D','C5P','T5P','O5D') then 'Cycle 5'
        when t1.card_account_status_code in ('D6D','C6P','T6P','O6D') then 'Cycle 6'
        when t1.card_account_status_code in ('D7D','C7P','T7P','O7D') then 'Cycle 7'
        when t1.card_account_status_code in ('D8D','C8P','T8P','O8D') then 'Cycle 8'
        when t1.card_account_status_code in ('B0P','B1P','B2P','B3P','B4P','DCP') then 'DC Pending'
        when t1.card_account_status_code in ('ZZZ') then 'Unverified' 
        when t1.card_account_status_code in ('LSP') then 'Sold debt' 
        when t1.card_account_status_code in ('PLC') then 'Pin not activated' 
        when (substr(t1.card_account_status_code,1,1) = 'C' and (
          case 
            when total_budget_balance_amt is null then 0 
            else total_budget_balance_amt 
          end + 
          case 
            when (
            case 
              when account_balance < 0 then account_balance * -1 
            end) is null then 0 
            else (case when account_balance < 0 then account_balance * -1 end) 
          end  > 0)) then 'Pending closure' 
        when t1.card_account_status_code in ('AAA', 'O1A') then 'In-order' 
        else 'Other' 
      end as cc_pre_class,
      row_number() over (partition by t2.id_number order by t1.status_date desc, t1.open_date desc) as num
    FROM 	
      FND_WFS_CRD_ACC_MLY t1
      INNER JOIN 	FND_WFS_CUSTOMER_ABSA t2 
        ON (
          substr(t1.account_number,1,1)='4'
          and t1.customer_key = t2.customer_key
          and t1.information_date =last_day(exec_mnth)
          and t1.card_account_status_code != 'XFA'
          and t2.id_number is not null 
          and length(trim(t2.id_number)) = 13
          and trim(t2.id_number) != '0000000000000'
        )	
  
  ),
  wfs_cc_customer as (
    SELECT /*+ PARALLEL(t1,4)  */
      t1.identity_document_code, 
      t1.CUSTOMER_KEY, 
      t1.cc_shopable_ind, 
      t1.cc_account_no, 
      t1.cc_account_status, 
      t1.cc_date_opened, 
      t1.cc_date_closed, 
      t1.cc_mob, 
      t1.cc_current_balance, 
      t1.cc_credit_limit, 
      t1.cc_open_to_buy, 
      t1.cc_account_status_class, 
      case 
        when t1.cc_pre_class in ('Sold debt', 'Pending closure','Other') then 1 
        else t1.cc_exclude_ind 
      end as cc_exclude_ind, 
      t1.total_budget_balance_amt, 
      t1.debit_balance, 
      t1.cc_secondary_card_ind, 
      t1.cc_pre_class,
      case 
        when (
          substr(t1.cc_account_status_class,1,1) = 'C' 
          and not(t1.total_budget_balance_amt + t1.debit_balance > 0)
          and cc_exclude_ind > 0
        ) then 'Closed'
        when (
          (substr(t1.cc_account_no,1,3) in ('666','777','222') 
          or substr(t1.cc_account_status_class,1,1) = 'F') and t1.cc_exclude_ind > 0
        ) then 'Fraud'
        when t1.cc_account_status_class in ('XFA','NWD','NUD','WAP') and t1.cc_exclude_ind > 0 then 'Other' 
        else t1.cc_pre_class
      end as cc_class
    from
      wfs_cc_customer_pre t1
    where
      t1.num = 1
  ),
  /*
    We need to extract the LATEST status for each of the below product codes in
    DIM_CUSTOMER_PORTFOLIO. The row_number() function allows us to identify this record
  */
  wfs_portfolios as(
    select 
      a.customer_no,
      max(a.LittleWorld_ind) as LittleWorld_ind,
      max(a.MySchool_ind) as MySchool_ind,
      max(a.WWDifference_ind) as WWDifference_ind,
      max(a.WODifference_ind) as WODifference_ind,
      max(a.Discovery_ind) as Discovery_ind
    from
    (
      select /*+ PARALLEL(CST_PRT,4) */
        cst_prt.customer_no as customer_no,
        case 
          when cst_prt.PRODUCT_CODE_NO = 99 then 1
          else 0
        end as LittleWorld_ind,
        
        case	
          when cst_prt.product_code_no =19 then	1		/*CM25 - Only active to be flagged */
          else 0	
        end as MySchool_ind,		
        case	
          when cst_prt.product_code_no =28 then	1		/*CM25 - Only active to be flagged */
          else 0	
        end as WWDifference_ind,		
        case	
          when cst_prt.product_code_no =29 then	1		/*CM25 - Only active to be flagged */
          else 0	
        end as WODifference_ind,		
        case	
          when cst_prt.product_code_no =92 then	1		/*CM25 - Only active to be flagged */
          else 0	
        end as Discovery_ind,
        row_number() over (partition by cst_prt.customer_no, cst_prt.product_code_no order by cst_prt.portfolio_create_date desc, cst_prt.last_updated_date desc) as num
      from 
        DIM_CUSTOMER_PORTFOLIO cst_prt
      where
        cst_prt.product_code_no in (99,19,28,29,92)
        and cst_prt.last_updated_date <= last_day(exec_mnth)
        and upper(cst_prt.portfolio_status_desc) in ('ACTIVE','OPEN')
    ) a
    where 
      num=1
    group by
      a.customer_no
  ),
  LOYALTY_TIER as (
    select /*+ PARALLEL(LT,4) */
      max(LT.LAST_UPDATED_DATE) over (partition by LT.customer_no) as date_last_updated_max ,
      LT.customer_no,
      max(LT.fin_year_no*100 + LT.fin_month_no) over (partition by LT.customer_no) as fin,
      LT.month_tier,
      case 
        when LT.month_tier in (0,1) 	then 'Valued'
        when LT.month_tier = 2 		then 'Loyal'
        when LT.month_tier in (3,4) 	then 'VIP' 
        ELSE 'Missing' 
      END as 	loyalty_tiers,
      row_number() over (partition by LT.customer_no order by LT.LAST_UPDATED_DATE desc) as row_num
    from 
      CUST_WOD_TIER_MTH_DETAIL LT
    where
      LT.LAST_UPDATED_DATE between loyalty_start and exec_mnth
  ),
  monthly_cust as (
    select /*+ PARALLEL(CST,4) PARALLEL(ALL_PRD,4) PARALLEL(CC,4)  */
      cst.customer_no,
      cst.identity_document_code as id_number,      
      cst.Foods_Segment,
      cst.Non_Foods_Segment,
      cst.gender,
      cst.customer_status,
      cst.WFS_CUSTOMER_NO,
      all_prd.wfs_account_no,
      all_prd.wfs_product,
      all_prd.sc_account_status,
      all_prd.sc_credit_limit,
      all_prd.sc_current_balance,
      all_prd.SC_OPEN_TO_BUY,
      all_prd.sc_date_opened,
      all_prd.sc_date_closed,
      all_prd.sc_date_chgoff,
      all_prd.sc_date_last_statement,
      all_prd.STMT_DATE_LAST_STATEMENT,
      all_prd.sc_shopable_ind,
      all_prd.sc_mob	,	
      all_prd.ISC_RISK_CAT_STMT,
      all_prd.delinquency_cycle ,
      cc.CUSTOMER_KEY,
      cc.cc_shopable_ind,
      cc.cc_account_no,
      cc.cc_account_status,
      cc.cc_account_status_class,
      cc.cc_date_opened,
      cc.cc_date_closed,
      cc.cc_mob,
      cc.cc_current_balance,
      cc.cc_credit_limit,
      cc.cc_open_to_buy,		
      cc.cc_exclude_ind,
      cc.cc_secondary_card_ind,
      cc.cc_pre_class,
      cc.cc_class,
      case
        when prtf.LittleWorld_ind is null then 0
        else prtf.LittleWorld_ind
      end as LittleWorld_ind,
      case
        when prtf.MySchool_ind is null then 0
        else prtf.MySchool_ind
      end as MySchool_ind,
      case
        when prtf.WWDifference_ind is null then 0
        else prtf.WWDifference_ind
      end as WWDifference_ind,
      case
        when prtf.WODifference_ind is null then 0
        else prtf.WODifference_ind
      end as WODifference_ind,
      case
        when prtf.Discovery_ind is null then 0
        else prtf.Discovery_ind
      end as Discovery_ind,
      lt.loyalty_tiers
        
    from	
      customer_segment_lifestyle cst
      left join cust_all_prod all_prd on
        --cst.WFS_CUSTOMER_NO = all_prd.WFS_CUSTOMER_NO
        --and 
		cst.identity_document_code = all_prd.identity_no
      left join wfs_cc_customer cc on
        cst.identity_document_code = cc.identity_document_code
      left join wfs_portfolios prtf on
        cst.customer_no = prtf.customer_no
      left join LOYALTY_TIER lt on
        cst.customer_no = lt.customer_no
        and lt.row_num=1
  )
  select a.* from monthly_cust a);

end;	




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

    l_text := 'WFS SALES Customer Account Mart load STARTED AT '||
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
    l_text := 'LOAD TABLE: '||'wfs_mart_sales_cust_acc_mly' ;
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
   -- to run  after the load of final monthly load of CUST_LSS_LIFESTYLE_SEGMENTS (WH_PRF_CUST_287U)
   
    g_2nd_of_month:= to_date(to_char(g_today,'YYYYMM')||'02', 'YYYYMMDD');
--/* temp fiddle*/   g_2nd_of_month:= to_date(to_char(g_today,'YYYYMM')||'03', 'YYYYMMDD');
   
   if g_run_date < g_2nd_of_month then
      g_run_date:= g_2nd_of_month;
      -- the LATER of the 8th Financial day of the month, or the 2nd day 
      -- we allow at least 2 days to ensure that all transactions from all stores are available
   end if;
  
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--    execute immediate 'alter session enable parallel dml';

    g_date_to_do:= to_date(to_char(add_months(sysdate,-1),'YYYYMM')||'01', 'YYYYMMDD' );
    -- first day of last completed calendar month


    prepare_customer_data(g_date_to_do);


    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
    commit;
    
    
--**************************************************************************************************
-- Retention maintenance
--**************************************************************************************************

-- to be added later ..
-- DWH team has reservation about dropping partitions.


--    g_recs_deleted :=  g_recs_deleted + SQL%ROWCOUNT;



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


end wh_prf_wfs_652u_20170803;
