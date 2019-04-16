--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_014B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_014B" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        MAY 2015
--  Author:      Alastair de Wet
--  Purpose:     Create Dim _customer_allprod dimention table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_all_prod_cpy
--               Output - fnd_wfs_all_prod
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
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
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;


g_wfs_customer_no        stg_vsn_all_prod_cpy.wfs_customer_no%type;
g_wfs_account_no         stg_vsn_all_prod_cpy.wfs_account_no%type;
g_product_code_no        stg_vsn_all_prod_cpy.product_code_no%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_014U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS ALL PROD MASTER EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_all_prod_cpy
where (wfs_customer_no,wfs_account_no,product_code_no)
in
(select wfs_customer_no,wfs_account_no,product_code_no
from stg_vsn_all_prod_cpy
group by wfs_customer_no,wfs_account_no,product_code_no
having count(*) > 1)
order by wfs_customer_no,wfs_account_no,product_code_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_all_prod is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_vsn_all_prod_cpy cpy,
              fnd_wfs_all_prod fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and
              cpy.wfs_account_no        = fnd.wfs_account_no and
              cpy.product_code_no       = fnd.product_code_no and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.wfs_customer_no,cpy.wfs_account_no,cpy.product_code_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no  := 0;
   g_wfs_account_no   := 0; 
   g_product_code_no  := 0;  

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no  and
        dupp_record.wfs_account_no    = g_wfs_account_no   and
        dupp_record.product_code_no   = g_product_code_no  then
        update stg_vsn_all_prod_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_wfs_customer_no    := dupp_record.wfs_customer_no;
    g_wfs_account_no     := dupp_record.wfs_account_no;
    g_product_code_no    := dupp_record.product_code_no;


   end loop;

   commit;

   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end remove_duplicates;

--**************************************************************************************************
-- Insert dummy m aster records to ensure RI
--**************************************************************************************************
procedure create_dummy_masters as
begin

--******************************************************************************


--       g_recs_dummy := g_recs_dummy + sql%rowcount;
      commit;


 --******************************************************************************


  exception
      when dwh_errors.e_insert_error then
       l_message := 'DUMMY INS - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'DUMMY INS  - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end create_dummy_masters;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_all_prod fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            cpy.	WFS_ACCOUNT_NO	,
            cpy.	WFS_CUSTOMER_NO	,
            cpy.	PRODUCT_CODE_NO	,
            cpy.	IDENTITY_NO	,
            cpy.	ACCOUNT_STATUS	,
            cpy.	APPLICATION_NO	,
            cpy.	APPLICATION_SCORE	,
            cpy.	BEHAVIOUR_SCORE01	,
            cpy.	BEHAVIOUR_SCORE02	,
            cpy.	BEHAVIOUR_SCORE03	,
            cpy.	BEHAVIOUR_SCORE04	,
            cpy.	BEHAVIOUR_SCORE05	,
            cpy.	BEHAVIOUR_SCORE06	,
            cpy.	BEHAVIOUR_SCORE07	,
            cpy.	BEHAVIOUR_SCORE08	,
            cpy.	BEHAVIOUR_SCORE09	,
            cpy.	BEHAVIOUR_SCORE10	,
            cpy.	BEHAVIOUR_SCORE11	,
            cpy.	BEHAVIOUR_SCORE12	,
            cpy.	PROPENSTY_SCORE01	,
            cpy.	PROPENSTY_SCORE02	,
            cpy.	PROPENSTY_SCORE03	,
            cpy.	PROPENSTY_SCORE04	,
            cpy.	PROPENSTY_SCORE05	,
            cpy.	PROPENSTY_SCORE06	,
            cpy.	PROPENSTY_SCORE07	,
            cpy.	PROPENSTY_SCORE08	,
            cpy.	PROPENSTY_SCORE09	,
            cpy.	PROPENSTY_SCORE10	,
            cpy.	PROPENSTY_SCORE11	,
            cpy.	PROPENSTY_SCORE12	,
            cpy.	ATTRITION_SCORE01	,
            cpy.	ATTRITION_SCORE02	,
            cpy.	ATTRITION_SCORE03	,
            cpy.	ATTRITION_SCORE04	,
            cpy.	ATTRITION_SCORE05	,
            cpy.	ATTRITION_SCORE06	,
            cpy.	ATTRITION_SCORE07	,
            cpy.	ATTRITION_SCORE08	,
            cpy.	ATTRITION_SCORE09	,
            cpy.	ATTRITION_SCORE10	,
            cpy.	ATTRITION_SCORE11	,
            cpy.	ATTRITION_SCORE12	,
            cpy.	DATE_OPENED	,
            cpy.	DATE_LAST_PCHS	,
            cpy.	CREDIT_LIMIT	,
            cpy.	CURRENT_BALANCE	,
            cpy.	OPEN_TO_BUY	,
            cpy.	LAST_PCHS_VAL	,
            cpy.	PCHS_VAL_YTD	,
            cpy.	PCHS_VAL_LTD	,
            cpy.	STORE_OF_PREF1	,
            cpy.	STORE_OF_PREF2	,
            cpy.	STORE_OF_PREF3	,
            cpy.	BLOCK_CODE1	,
            cpy.	BLOCK_CODE2	,
            cpy.	NO_OF_CARDS	,
            cpy.	DATE_LAST_UPDATED	,
            cpy.	CHGOFF_VAL	,
            cpy.	CHGOFF_RSN1	,
            cpy.	CHGOFF_RSN2	,
            cpy.	CHGOFF_STATUS	,
            cpy.	TIMES_IN_COLL	,
            cpy.	DATE_APPLICATION	,
            cpy.	DATE_CHGOFF	,
            cpy.	DATE_CLOSED	,
            cpy.	DATE_HIGHEST_BAL	,
            cpy.	DATE_LAST_ACTIVITY	,
            cpy.	DATE_LAST_AGE	,
            cpy.	DATE_LAST_CRLM	,
            cpy.	DATE_LAST_PYMT	,
            cpy.	DATE_LAST_RATE_CHG	,
            cpy.	DATE_LAST_REAGE	,
            cpy.	DATE_LAST_RECLASS	,
            cpy.	DATE_LAST_RETURN	,
            cpy.	DATE_LAST_REVPMT	,
            cpy.	DATE_LAST_RTNCHQ	,
            cpy.	DATE_LAST_STMT	,
            cpy.	DAYS_TILL_CHGOFF	,
            cpy.	HIGHEST_BAL_VAL	,
            cpy.	PREV_CREDIT_CLASS	,
            cpy.	PREV_CREDIT_LIMIT	,
            cpy.	PREV_INT_VAL_YTD	,
            cpy.	PREV_INT_PD_YTD	,
            cpy.	MARKET_FLAG_01	,
            cpy.	MARKET_FLAG_02	,
            cpy.	MARKET_FLAG_03	,
            cpy.	MARKET_FLAG_04	,
            cpy.	MARKET_FLAG_05	,
            cpy.	MARKET_FLAG_06	,
            cpy.	MARKET_FLAG_07	,
            cpy.	MARKET_FLAG_08	,
            cpy.	MARKET_FLAG_09	,
            cpy.	MARKET_FLAG_10	,
            cpy.	MARKET_FLAG_11	,
            cpy.	MARKET_FLAG_12	,
            cpy.	MARKET_FLAG_13	,
            cpy.	MARKET_FLAG_14	,
            cpy.	MARKET_FLAG_15	,
            cpy.	MARKET_FLAG_16	,
            cpy.	MARKET_FLAG_17	,
            cpy.	MARKET_FLAG_18	,
            cpy.	MARKET_FLAG_19	,
            cpy.	MARKET_FLAG_20	,
            cpy.	LAST_PYMT_VAL	,
            cpy.	PROMOTION_CODE1	,
            cpy.	PROMOTION_CODE2	,
            cpy.	PROMOTION_CODE3	,
            cpy.	PROMOTION_CODE4	,
            cpy.	PROMOTION_STATUS1	,
            cpy.	PROMOTION_STATUS2	,
            cpy.	PROMOTION_STATUS3	,
            cpy.	PROMOTION_STATUS4	,
            cpy.	RETAIL_PLAN_CODE	,
            cpy.	STATMT_FLAG	,
            cpy.	STATMT_MSG_NO_1	,
            cpy.	STATMT_MSG_NO_2	,
            cpy.	WRITE_OFF_DAYS	,
            cpy.	INS_INCENTV_STORE	,
            cpy.	INS_CANCEL_DATE	,
            cpy.	INS_DT_LST_BILLED	,
            cpy.	INS_DT_LST_CLAIM	,
            cpy.	INS_EFFECTV_DATE	,
            cpy.	INS_ENRLLMNT_STATE	,
            cpy.	INS_LAST_PREMIUM	,
            cpy.	INS_PREMIUM_MTD	,
            cpy.	INS_PREMIUM	,
            cpy.	INS_PREMIUM_STATE	,
            cpy.	INS_PRODUCT	,
            cpy.	INS_RSN_CANCELLED	,
            cpy.	INS_REINSTMT_DATE	,
            cpy.	INS_STATUS	,
            cpy.	PLAN_PMT_OVRD_FLAG	,
            cpy.	MKTG_PROMO	,
            cpy.	NO_OF_STORE_PREF	,
            cpy.	RETURN_MAIL_CNT	,
            cpy.	LOAN_DRAWDOWN_VAL	,
            cpy.	LOAN_INSTALMENT	,
            cpy.	LOAN_REPAY_PERIOD	,
            cpy.	LOAN_TRACKER	,
            cpy.	SDS_REF	,
            cpy.	TEST_DIGIT	,
            cpy.	TEST_DIGIT_GRP	,
            cpy.	DEBIT_ORDER_FLAG	,
            cpy.	DEBIT_ORDER_DY	,
            cpy.	DEBIT_ORDER_DUE	,
            cpy.	DTLST_ACCSTAT_CHG	,
            cpy.	LCP_IND	,
            cpy.	COMPANION_CARE_IND	,
            cpy.	ACCIDENT_BENFT_IND	,
            cpy.	CBP_IND	,
            cpy.	LBP_IND	,
            cpy.	PTP_STATUS	,
            cpy.	DATE_CRED_LIMIT	,
            cpy.	COMP_CARE_LST_PREM	,
            cpy.	COMP_CARE_EFF_DATE	,
            cpy.	ACC_BENFT_LST_PREM	,
            cpy.	ACC_BENFT_EFF_DATE	,
            cpy.	OVERDUE_AMT	,
            cpy.	MIN_PAYMENT	,
            cpy.	PAYMENT_DATE	,
            cpy.	ACCOUNT_CONTACT_ID	,
            cpy.	TTD_IND	,
            cpy.	BUREAU_SCORE	,
            cpy.	VIKING_CODE	,
            cpy.	VIKING_DATE	,
            cpy.	VIKING_AMT	,
            cpy.	DEBIT_ORDER_PROJ_AMT	,
            cpy.	DEBIT_ORDER_BR_CD	,
            cpy.	DEBIT_ORDER_EXP_DT	,
            cpy.	DEBIT_ORDER_ACC_TYPE	,
            cpy.	DEBIT_ORDER_ACC_NO	,
            cpy.	DEBIT_ORDER_PYMT_IND	,
            cpy.	DD_STATUS	,
            cpy.	CLIM_REVIEW	,
            cpy.	DD_LOAD_AMT	,
            cpy.	DATE_FIRST_PURCH	,
            cpy.	INSURANCE_ACTIVE_IND	,
            cpy.	LOAN_RESTRUCT_IND	,
            cpy.	LOAN_RESTRUCT_DATE	,
            cpy.	RESIDENCE_ID	,
            cpy.	DEBIT_ORDER_REVERSAL_COUNT	,
            cpy.	DEBIT_ORDER_INTERIM_PMT	,
            cpy.	DEBIT_ORDER_REMITT_METHOD	,
            cpy.	STAFF_COMPANY_CODE	,
            cpy.	WRITE_OFF_IND	,
            cpy.	WRITE_OFF_DATE	,
            cpy.	WRITE_OFF_VALUE	,
            cpy.	INITIATION_FEE	,
            cpy.	MONTHLY_SERVICE_FEE	,
            cpy.	INITIAL_INTEREST_RATE	,
            cpy.	DELIVERY_METHOD	,
            cpy.	DELIVERY_ADDRESS	,
            g_date as last_updated_date,
            cpy.	LEGAL_STATUS	,
            cpy.	LEGAL_STATUS_DATE	,
            cpy.	FIRST_PLACEMENT_INDICATOR	,
            cpy.	FIRST_PLACEMENT_DATE	,
            cpy.	SECOND_PLACEMENT_INDICATOR	,
            cpy.	SECOND_PLACEMENT_DATE	,
            cpy.	THIRD_PLACEMENT_INDICATOR	,
            cpy.	THIRD_PLACEMENT_DATE	,
            cpy.	MONTH6_REVIEW_INDICATOR	,
            cpy.	MONTH6_REVIEW_DATE	 

       from  stg_vsn_all_prod_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_wfs_all_prod
       where  wfs_customer_no    = cpy.wfs_customer_no and
              wfs_account_no     = cpy.wfs_account_no and
              product_code_no    = cpy.product_code_no  
              )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       and sys_process_code = 'N';


      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



FOR upd_rec IN c_stg_vsn_all_prod
   loop
     update fnd_wfs_all_prod fnd
     set    fnd.	IDENTITY_NO	      =	upd_rec.	IDENTITY_NO	,
            fnd.	ACCOUNT_STATUS	  =	upd_rec.	ACCOUNT_STATUS	,
            fnd.	APPLICATION_NO	  =	upd_rec.	APPLICATION_NO	,
            fnd.	APPLICATION_SCORE	=	upd_rec.	APPLICATION_SCORE	,
            fnd.	BEHAVIOUR_SCORE01	=	upd_rec.	BEHAVIOUR_SCORE01	,
            fnd.	BEHAVIOUR_SCORE02	=	upd_rec.	BEHAVIOUR_SCORE02	,
            fnd.	BEHAVIOUR_SCORE03	=	upd_rec.	BEHAVIOUR_SCORE03	,
            fnd.	BEHAVIOUR_SCORE04	=	upd_rec.	BEHAVIOUR_SCORE04	,
            fnd.	BEHAVIOUR_SCORE05	=	upd_rec.	BEHAVIOUR_SCORE05	,
            fnd.	BEHAVIOUR_SCORE06	=	upd_rec.	BEHAVIOUR_SCORE06	,
            fnd.	BEHAVIOUR_SCORE07	=	upd_rec.	BEHAVIOUR_SCORE07	,
            fnd.	BEHAVIOUR_SCORE08	=	upd_rec.	BEHAVIOUR_SCORE08	,
            fnd.	BEHAVIOUR_SCORE09	=	upd_rec.	BEHAVIOUR_SCORE09	,
            fnd.	BEHAVIOUR_SCORE10	=	upd_rec.	BEHAVIOUR_SCORE10	,
            fnd.	BEHAVIOUR_SCORE11	=	upd_rec.	BEHAVIOUR_SCORE11	,
            fnd.	BEHAVIOUR_SCORE12	=	upd_rec.	BEHAVIOUR_SCORE12	,
            fnd.	PROPENSTY_SCORE01	=	upd_rec.	PROPENSTY_SCORE01	,
            fnd.	PROPENSTY_SCORE02	=	upd_rec.	PROPENSTY_SCORE02	,
            fnd.	PROPENSTY_SCORE03	=	upd_rec.	PROPENSTY_SCORE03	,
            fnd.	PROPENSTY_SCORE04	=	upd_rec.	PROPENSTY_SCORE04	,
            fnd.	PROPENSTY_SCORE05	=	upd_rec.	PROPENSTY_SCORE05	,
            fnd.	PROPENSTY_SCORE06	=	upd_rec.	PROPENSTY_SCORE06	,
            fnd.	PROPENSTY_SCORE07	=	upd_rec.	PROPENSTY_SCORE07	,
            fnd.	PROPENSTY_SCORE08	=	upd_rec.	PROPENSTY_SCORE08	,
            fnd.	PROPENSTY_SCORE09	=	upd_rec.	PROPENSTY_SCORE09	,
            fnd.	PROPENSTY_SCORE10	=	upd_rec.	PROPENSTY_SCORE10	,
            fnd.	PROPENSTY_SCORE11	=	upd_rec.	PROPENSTY_SCORE11	,
            fnd.	PROPENSTY_SCORE12	=	upd_rec.	PROPENSTY_SCORE12	,
            fnd.	ATTRITION_SCORE01	=	upd_rec.	ATTRITION_SCORE01	,
            fnd.	ATTRITION_SCORE02	=	upd_rec.	ATTRITION_SCORE02	,
            fnd.	ATTRITION_SCORE03	=	upd_rec.	ATTRITION_SCORE03	,
            fnd.	ATTRITION_SCORE04	=	upd_rec.	ATTRITION_SCORE04	,
            fnd.	ATTRITION_SCORE05	=	upd_rec.	ATTRITION_SCORE05	,
            fnd.	ATTRITION_SCORE06	=	upd_rec.	ATTRITION_SCORE06	,
            fnd.	ATTRITION_SCORE07	=	upd_rec.	ATTRITION_SCORE07	,
            fnd.	ATTRITION_SCORE08	=	upd_rec.	ATTRITION_SCORE08	,
            fnd.	ATTRITION_SCORE09	=	upd_rec.	ATTRITION_SCORE09	,
            fnd.	ATTRITION_SCORE10	=	upd_rec.	ATTRITION_SCORE10	,
            fnd.	ATTRITION_SCORE11	=	upd_rec.	ATTRITION_SCORE11	,
            fnd.	ATTRITION_SCORE12	=	upd_rec.	ATTRITION_SCORE12	,
            fnd.	DATE_OPENED	=	upd_rec.	DATE_OPENED	,
            fnd.	DATE_LAST_PCHS	=	upd_rec.	DATE_LAST_PCHS	,
            fnd.	CREDIT_LIMIT	=	upd_rec.	CREDIT_LIMIT	,
            fnd.	CURRENT_BALANCE	=	upd_rec.	CURRENT_BALANCE	,
            fnd.	OPEN_TO_BUY	=	upd_rec.	OPEN_TO_BUY	,
            fnd.	LAST_PCHS_VAL	=	upd_rec.	LAST_PCHS_VAL	,
            fnd.	PCHS_VAL_YTD	=	upd_rec.	PCHS_VAL_YTD	,
            fnd.	PCHS_VAL_LTD	=	upd_rec.	PCHS_VAL_LTD	,
            fnd.	STORE_OF_PREF1	=	upd_rec.	STORE_OF_PREF1	,
            fnd.	STORE_OF_PREF2	=	upd_rec.	STORE_OF_PREF2	,
            fnd.	STORE_OF_PREF3	=	upd_rec.	STORE_OF_PREF3	,
            fnd.	BLOCK_CODE1	=	upd_rec.	BLOCK_CODE1	,
            fnd.	BLOCK_CODE2	=	upd_rec.	BLOCK_CODE2	,
            fnd.	NO_OF_CARDS	=	upd_rec.	NO_OF_CARDS	,
            fnd.	DATE_LAST_UPDATED	=	upd_rec.	DATE_LAST_UPDATED	,
            fnd.	CHGOFF_VAL	=	upd_rec.	CHGOFF_VAL	,
            fnd.	CHGOFF_RSN1	=	upd_rec.	CHGOFF_RSN1	,
            fnd.	CHGOFF_RSN2	=	upd_rec.	CHGOFF_RSN2	,
            fnd.	CHGOFF_STATUS	=	upd_rec.	CHGOFF_STATUS	,
            fnd.	TIMES_IN_COLL	=	upd_rec.	TIMES_IN_COLL	,
            fnd.	DATE_APPLICATION	=	upd_rec.	DATE_APPLICATION	,
            fnd.	DATE_CHGOFF	=	upd_rec.	DATE_CHGOFF	,
            fnd.	DATE_CLOSED	=	upd_rec.	DATE_CLOSED	,
            fnd.	DATE_HIGHEST_BAL	=	upd_rec.	DATE_HIGHEST_BAL	,
            fnd.	DATE_LAST_ACTIVITY	=	upd_rec.	DATE_LAST_ACTIVITY	,
            fnd.	DATE_LAST_AGE	=	upd_rec.	DATE_LAST_AGE	,
            fnd.	DATE_LAST_CRLM	=	upd_rec.	DATE_LAST_CRLM	,
            fnd.	DATE_LAST_PYMT	=	upd_rec.	DATE_LAST_PYMT	,
            fnd.	DATE_LAST_RATE_CHG	=	upd_rec.	DATE_LAST_RATE_CHG	,
            fnd.	DATE_LAST_REAGE	=	upd_rec.	DATE_LAST_REAGE	,
            fnd.	DATE_LAST_RECLASS	=	upd_rec.	DATE_LAST_RECLASS	,
            fnd.	DATE_LAST_RETURN	=	upd_rec.	DATE_LAST_RETURN	,
            fnd.	DATE_LAST_REVPMT	=	upd_rec.	DATE_LAST_REVPMT	,
            fnd.	DATE_LAST_RTNCHQ	=	upd_rec.	DATE_LAST_RTNCHQ	,
            fnd.	DATE_LAST_STMT	=	upd_rec.	DATE_LAST_STMT	,
            fnd.	DAYS_TILL_CHGOFF	=	upd_rec.	DAYS_TILL_CHGOFF	,
            fnd.	HIGHEST_BAL_VAL	=	upd_rec.	HIGHEST_BAL_VAL	,
            fnd.	PREV_CREDIT_CLASS	=	upd_rec.	PREV_CREDIT_CLASS	,
            fnd.	PREV_CREDIT_LIMIT	=	upd_rec.	PREV_CREDIT_LIMIT	,
            fnd.	PREV_INT_VAL_YTD	=	upd_rec.	PREV_INT_VAL_YTD	,
            fnd.	PREV_INT_PD_YTD	=	upd_rec.	PREV_INT_PD_YTD	,
            fnd.	MARKET_FLAG_01	=	upd_rec.	MARKET_FLAG_01	,
            fnd.	MARKET_FLAG_02	=	upd_rec.	MARKET_FLAG_02	,
            fnd.	MARKET_FLAG_03	=	upd_rec.	MARKET_FLAG_03	,
            fnd.	MARKET_FLAG_04	=	upd_rec.	MARKET_FLAG_04	,
            fnd.	MARKET_FLAG_05	=	upd_rec.	MARKET_FLAG_05	,
            fnd.	MARKET_FLAG_06	=	upd_rec.	MARKET_FLAG_06	,
            fnd.	MARKET_FLAG_07	=	upd_rec.	MARKET_FLAG_07	,
            fnd.	MARKET_FLAG_08	=	upd_rec.	MARKET_FLAG_08	,
            fnd.	MARKET_FLAG_09	=	upd_rec.	MARKET_FLAG_09	,
            fnd.	MARKET_FLAG_10	=	upd_rec.	MARKET_FLAG_10	,
            fnd.	MARKET_FLAG_11	=	upd_rec.	MARKET_FLAG_11	,
            fnd.	MARKET_FLAG_12	=	upd_rec.	MARKET_FLAG_12	,
            fnd.	MARKET_FLAG_13	=	upd_rec.	MARKET_FLAG_13	,
            fnd.	MARKET_FLAG_14	=	upd_rec.	MARKET_FLAG_14	,
            fnd.	MARKET_FLAG_15	=	upd_rec.	MARKET_FLAG_15	,
            fnd.	MARKET_FLAG_16	=	upd_rec.	MARKET_FLAG_16	,
            fnd.	MARKET_FLAG_17	=	upd_rec.	MARKET_FLAG_17	,
            fnd.	MARKET_FLAG_18	=	upd_rec.	MARKET_FLAG_18	,
            fnd.	MARKET_FLAG_19	=	upd_rec.	MARKET_FLAG_19	,
            fnd.	MARKET_FLAG_20	=	upd_rec.	MARKET_FLAG_20	,
            fnd.	LAST_PYMT_VAL	=	upd_rec.	LAST_PYMT_VAL	,
            fnd.	PROMOTION_CODE1	=	upd_rec.	PROMOTION_CODE1	,
            fnd.	PROMOTION_CODE2	=	upd_rec.	PROMOTION_CODE2	,
            fnd.	PROMOTION_CODE3	=	upd_rec.	PROMOTION_CODE3	,
            fnd.	PROMOTION_CODE4	=	upd_rec.	PROMOTION_CODE4	,
            fnd.	PROMOTION_STATUS1	=	upd_rec.	PROMOTION_STATUS1	,
            fnd.	PROMOTION_STATUS2	=	upd_rec.	PROMOTION_STATUS2	,
            fnd.	PROMOTION_STATUS3	=	upd_rec.	PROMOTION_STATUS3	,
            fnd.	PROMOTION_STATUS4	=	upd_rec.	PROMOTION_STATUS4	,
            fnd.	RETAIL_PLAN_CODE	=	upd_rec.	RETAIL_PLAN_CODE	,
            fnd.	STATMT_FLAG	=	upd_rec.	STATMT_FLAG	,
            fnd.	STATMT_MSG_NO_1	=	upd_rec.	STATMT_MSG_NO_1	,
            fnd.	STATMT_MSG_NO_2	=	upd_rec.	STATMT_MSG_NO_2	,
            fnd.	WRITE_OFF_DAYS	=	upd_rec.	WRITE_OFF_DAYS	,
            fnd.	INS_INCENTV_STORE	=	upd_rec.	INS_INCENTV_STORE	,
            fnd.	INS_CANCEL_DATE	=	upd_rec.	INS_CANCEL_DATE	,
            fnd.	INS_DT_LST_BILLED	=	upd_rec.	INS_DT_LST_BILLED	,
            fnd.	INS_DT_LST_CLAIM	=	upd_rec.	INS_DT_LST_CLAIM	,
            fnd.	INS_EFFECTV_DATE	=	upd_rec.	INS_EFFECTV_DATE	,
            fnd.	INS_ENRLLMNT_STATE	=	upd_rec.	INS_ENRLLMNT_STATE	,
            fnd.	INS_LAST_PREMIUM	=	upd_rec.	INS_LAST_PREMIUM	,
            fnd.	INS_PREMIUM_MTD	=	upd_rec.	INS_PREMIUM_MTD	,
            fnd.	INS_PREMIUM	=	upd_rec.	INS_PREMIUM	,
            fnd.	INS_PREMIUM_STATE	=	upd_rec.	INS_PREMIUM_STATE	,
            fnd.	INS_PRODUCT	=	upd_rec.	INS_PRODUCT	,
            fnd.	INS_RSN_CANCELLED	=	upd_rec.	INS_RSN_CANCELLED	,
            fnd.	INS_REINSTMT_DATE	=	upd_rec.	INS_REINSTMT_DATE	,
            fnd.	INS_STATUS	=	upd_rec.	INS_STATUS	,
            fnd.	PLAN_PMT_OVRD_FLAG	=	upd_rec.	PLAN_PMT_OVRD_FLAG	,
            fnd.	MKTG_PROMO	=	upd_rec.	MKTG_PROMO	,
            fnd.	NO_OF_STORE_PREF	=	upd_rec.	NO_OF_STORE_PREF	,
            fnd.	RETURN_MAIL_CNT	=	upd_rec.	RETURN_MAIL_CNT	,
            fnd.	LOAN_DRAWDOWN_VAL	=	upd_rec.	LOAN_DRAWDOWN_VAL	,
            fnd.	LOAN_INSTALMENT	=	upd_rec.	LOAN_INSTALMENT	,
            fnd.	LOAN_REPAY_PERIOD	=	upd_rec.	LOAN_REPAY_PERIOD	,
            fnd.	LOAN_TRACKER	=	upd_rec.	LOAN_TRACKER	,
            fnd.	SDS_REF	=	upd_rec.	SDS_REF	,
            fnd.	TEST_DIGIT	=	upd_rec.	TEST_DIGIT	,
            fnd.	TEST_DIGIT_GRP	=	upd_rec.	TEST_DIGIT_GRP	,
            fnd.	DEBIT_ORDER_FLAG	=	upd_rec.	DEBIT_ORDER_FLAG	,
            fnd.	DEBIT_ORDER_DY	=	upd_rec.	DEBIT_ORDER_DY	,
            fnd.	DEBIT_ORDER_DUE	=	upd_rec.	DEBIT_ORDER_DUE	,
            fnd.	DTLST_ACCSTAT_CHG	=	upd_rec.	DTLST_ACCSTAT_CHG	,
            fnd.	LCP_IND	=	upd_rec.	LCP_IND	,
            fnd.	COMPANION_CARE_IND	=	upd_rec.	COMPANION_CARE_IND	,
            fnd.	ACCIDENT_BENFT_IND	=	upd_rec.	ACCIDENT_BENFT_IND	,
            fnd.	CBP_IND	=	upd_rec.	CBP_IND	,
            fnd.	LBP_IND	=	upd_rec.	LBP_IND	,
            fnd.	PTP_STATUS	=	upd_rec.	PTP_STATUS	,
            fnd.	DATE_CRED_LIMIT	=	upd_rec.	DATE_CRED_LIMIT	,
            fnd.	COMP_CARE_LST_PREM	=	upd_rec.	COMP_CARE_LST_PREM	,
            fnd.	COMP_CARE_EFF_DATE	=	upd_rec.	COMP_CARE_EFF_DATE	,
            fnd.	ACC_BENFT_LST_PREM	=	upd_rec.	ACC_BENFT_LST_PREM	,
            fnd.	ACC_BENFT_EFF_DATE	=	upd_rec.	ACC_BENFT_EFF_DATE	,
            fnd.	OVERDUE_AMT	=	upd_rec.	OVERDUE_AMT	,
            fnd.	MIN_PAYMENT	=	upd_rec.	MIN_PAYMENT	,
            fnd.	PAYMENT_DATE	=	upd_rec.	PAYMENT_DATE	,
            fnd.	ACCOUNT_CONTACT_ID	=	upd_rec.	ACCOUNT_CONTACT_ID	,
            fnd.	TTD_IND	=	upd_rec.	TTD_IND	,
            fnd.	BUREAU_SCORE	=	upd_rec.	BUREAU_SCORE	,
            fnd.	VIKING_CODE	=	upd_rec.	VIKING_CODE	,
            fnd.	VIKING_DATE	=	upd_rec.	VIKING_DATE	,
            fnd.	VIKING_AMT	=	upd_rec.	VIKING_AMT	,
            fnd.	DEBIT_ORDER_PROJ_AMT	=	upd_rec.	DEBIT_ORDER_PROJ_AMT	,
            fnd.	DEBIT_ORDER_BR_CD	=	upd_rec.	DEBIT_ORDER_BR_CD	,
            fnd.	DEBIT_ORDER_EXP_DT	=	upd_rec.	DEBIT_ORDER_EXP_DT	,
            fnd.	DEBIT_ORDER_ACC_TYPE	=	upd_rec.	DEBIT_ORDER_ACC_TYPE	,
            fnd.	DEBIT_ORDER_ACC_NO	=	upd_rec.	DEBIT_ORDER_ACC_NO	,
            fnd.	DEBIT_ORDER_PYMT_IND	=	upd_rec.	DEBIT_ORDER_PYMT_IND	,
            fnd.	DD_STATUS	=	upd_rec.	DD_STATUS	,
            fnd.	CLIM_REVIEW	=	upd_rec.	CLIM_REVIEW	,
            fnd.	DD_LOAD_AMT	=	upd_rec.	DD_LOAD_AMT	,
            fnd.	DATE_FIRST_PURCH	=	upd_rec.	DATE_FIRST_PURCH	,
            fnd.	INSURANCE_ACTIVE_IND	=	upd_rec.	INSURANCE_ACTIVE_IND	,
            fnd.	LOAN_RESTRUCT_IND	=	upd_rec.	LOAN_RESTRUCT_IND	,
            fnd.	LOAN_RESTRUCT_DATE	=	upd_rec.	LOAN_RESTRUCT_DATE	,
            fnd.	RESIDENCE_ID	=	upd_rec.	RESIDENCE_ID	,
            fnd.	DEBIT_ORDER_REVERSAL_COUNT	=	upd_rec.	DEBIT_ORDER_REVERSAL_COUNT	,
            fnd.	DEBIT_ORDER_INTERIM_PMT	=	upd_rec.	DEBIT_ORDER_INTERIM_PMT	,
            fnd.	DEBIT_ORDER_REMITT_METHOD	=	upd_rec.	DEBIT_ORDER_REMITT_METHOD	,
            fnd.	STAFF_COMPANY_CODE	=	upd_rec.	STAFF_COMPANY_CODE	,
            fnd.	WRITE_OFF_IND	=	upd_rec.	WRITE_OFF_IND	,
            fnd.	WRITE_OFF_DATE	=	upd_rec.	WRITE_OFF_DATE	,
            fnd.	WRITE_OFF_VALUE	=	upd_rec.	WRITE_OFF_VALUE	,
            fnd.	INITIATION_FEE	=	upd_rec.	INITIATION_FEE	,
            fnd.	MONTHLY_SERVICE_FEE	=	upd_rec.	MONTHLY_SERVICE_FEE	,
            fnd.	INITIAL_INTEREST_RATE	=	upd_rec.	INITIAL_INTEREST_RATE	,
            fnd.	DELIVERY_METHOD	=	upd_rec.	DELIVERY_METHOD	,
            fnd.	DELIVERY_ADDRESS	=	upd_rec.	DELIVERY_ADDRESS	,
            fnd.	LAST_UPDATED_DATE	=	g_date,
            fnd.	LEGAL_STATUS	=	upd_rec.	LEGAL_STATUS	,
            fnd.	LEGAL_STATUS_DATE	=	upd_rec.	LEGAL_STATUS_DATE	,
            fnd.	FIRST_PLACEMENT_INDICATOR	=	upd_rec.	FIRST_PLACEMENT_INDICATOR	,
            fnd.	FIRST_PLACEMENT_DATE	=	upd_rec.	FIRST_PLACEMENT_DATE	,
            fnd.	SECOND_PLACEMENT_INDICATOR	=	upd_rec.	SECOND_PLACEMENT_INDICATOR	,
            fnd.	SECOND_PLACEMENT_DATE	=	upd_rec.	SECOND_PLACEMENT_DATE	,
            fnd.	THIRD_PLACEMENT_INDICATOR	=	upd_rec.	THIRD_PLACEMENT_INDICATOR	,
            fnd.	THIRD_PLACEMENT_DATE	=	upd_rec.	THIRD_PLACEMENT_DATE	,
            fnd.	MONTH6_REVIEW_INDICATOR	=	upd_rec.	MONTH6_REVIEW_INDICATOR	,
            fnd.	MONTH6_REVIEW_DATE	=	upd_rec.	MONTH6_REVIEW_DATE	 

     where  fnd.	WFS_ACCOUNT_NO	  =	upd_rec.	WFS_ACCOUNT_NO	and
            fnd.	WFS_CUSTOMER_NO	  =	upd_rec.	WFS_CUSTOMER_NO	and
            fnd.	PRODUCT_CODE_NO	  =	upd_rec.	PRODUCT_CODE_NO	and
            (
              nvl(fnd.identity_no                     ,0) <> upd_rec.identity_no or
              nvl(fnd.account_status                  ,0) <> upd_rec.account_status or
              nvl(fnd.application_no                  ,0) <> upd_rec.application_no or
              nvl(fnd.application_score               ,0) <> upd_rec.application_score or
              nvl(fnd.behaviour_score01               ,0) <> upd_rec.behaviour_score01 or
              nvl(fnd.behaviour_score02               ,0) <> upd_rec.behaviour_score02 or
              nvl(fnd.behaviour_score03               ,0) <> upd_rec.behaviour_score03 or
              nvl(fnd.behaviour_score04               ,0) <> upd_rec.behaviour_score04 or
              nvl(fnd.behaviour_score05               ,0) <> upd_rec.behaviour_score05 or
              nvl(fnd.behaviour_score06               ,0) <> upd_rec.behaviour_score06 or
              nvl(fnd.behaviour_score07               ,0) <> upd_rec.behaviour_score07 or
              nvl(fnd.behaviour_score08               ,0) <> upd_rec.behaviour_score08 or
              nvl(fnd.behaviour_score09               ,0) <> upd_rec.behaviour_score09 or
              nvl(fnd.behaviour_score10               ,0) <> upd_rec.behaviour_score10 or
              nvl(fnd.behaviour_score11               ,0) <> upd_rec.behaviour_score11 or
              nvl(fnd.behaviour_score12               ,0) <> upd_rec.behaviour_score12 or
              nvl(fnd.propensty_score01               ,0) <> upd_rec.propensty_score01 or
              nvl(fnd.propensty_score02               ,0) <> upd_rec.propensty_score02 or
              nvl(fnd.propensty_score03               ,0) <> upd_rec.propensty_score03 or
              nvl(fnd.propensty_score04               ,0) <> upd_rec.propensty_score04 or
              nvl(fnd.propensty_score05               ,0) <> upd_rec.propensty_score05 or
              nvl(fnd.propensty_score06               ,0) <> upd_rec.propensty_score06 or
              nvl(fnd.propensty_score07               ,0) <> upd_rec.propensty_score07 or
              nvl(fnd.propensty_score08               ,0) <> upd_rec.propensty_score08 or
              nvl(fnd.propensty_score09               ,0) <> upd_rec.propensty_score09 or
              nvl(fnd.propensty_score10               ,0) <> upd_rec.propensty_score10 or
              nvl(fnd.propensty_score11               ,0) <> upd_rec.propensty_score11 or
              nvl(fnd.propensty_score12               ,0) <> upd_rec.propensty_score12 or
              nvl(fnd.attrition_score01               ,0) <> upd_rec.attrition_score01 or
              nvl(fnd.attrition_score02               ,0) <> upd_rec.attrition_score02 or
              nvl(fnd.attrition_score03               ,0) <> upd_rec.attrition_score03 or
              nvl(fnd.attrition_score04               ,0) <> upd_rec.attrition_score04 or
              nvl(fnd.attrition_score05               ,0) <> upd_rec.attrition_score05 or
              nvl(fnd.attrition_score06               ,0) <> upd_rec.attrition_score06 or
              nvl(fnd.attrition_score07               ,0) <> upd_rec.attrition_score07 or
              nvl(fnd.attrition_score08               ,0) <> upd_rec.attrition_score08 or
              nvl(fnd.attrition_score09               ,0) <> upd_rec.attrition_score09 or
              nvl(fnd.attrition_score10               ,0) <> upd_rec.attrition_score10 or
              nvl(fnd.attrition_score11               ,0) <> upd_rec.attrition_score11 or
              nvl(fnd.attrition_score12               ,0) <> upd_rec.attrition_score12 or
              nvl(fnd.date_opened                     ,'1 Jan 1900') <> upd_rec.date_opened or
              nvl(fnd.date_last_pchs                  ,'1 Jan 1900') <> upd_rec.date_last_pchs or
              nvl(fnd.credit_limit                    ,0) <> upd_rec.credit_limit or
              nvl(fnd.current_balance                 ,0) <> upd_rec.current_balance or
              nvl(fnd.open_to_buy                     ,0) <> upd_rec.open_to_buy or
              nvl(fnd.last_pchs_val                   ,0) <> upd_rec.last_pchs_val or
              nvl(fnd.pchs_val_ytd                    ,0) <> upd_rec.pchs_val_ytd or
              nvl(fnd.pchs_val_ltd                    ,0) <> upd_rec.pchs_val_ltd or
              nvl(fnd.store_of_pref1                  ,0) <> upd_rec.store_of_pref1 or
              nvl(fnd.store_of_pref2                  ,0) <> upd_rec.store_of_pref2 or
              nvl(fnd.store_of_pref3                  ,0) <> upd_rec.store_of_pref3 or
              nvl(fnd.block_code1                     ,0) <> upd_rec.block_code1 or
              nvl(fnd.block_code2                     ,0) <> upd_rec.block_code2 or
              nvl(fnd.no_of_cards                     ,0) <> upd_rec.no_of_cards or
              nvl(fnd.date_last_updated               ,'1 Jan 1900') <> upd_rec.date_last_updated or
              nvl(fnd.chgoff_val                      ,0) <> upd_rec.chgoff_val or
              nvl(fnd.chgoff_rsn1                     ,0) <> upd_rec.chgoff_rsn1 or
              nvl(fnd.chgoff_rsn2                     ,0) <> upd_rec.chgoff_rsn2 or
              nvl(fnd.chgoff_status                   ,0) <> upd_rec.chgoff_status or
              nvl(fnd.times_in_coll                   ,0) <> upd_rec.times_in_coll or
              nvl(fnd.date_application                ,'1 Jan 1900') <> upd_rec.date_application or
              nvl(fnd.date_chgoff                     ,'1 Jan 1900') <> upd_rec.date_chgoff or
              nvl(fnd.date_closed                     ,'1 Jan 1900') <> upd_rec.date_closed or
              nvl(fnd.date_highest_bal                ,'1 Jan 1900') <> upd_rec.date_highest_bal or
              nvl(fnd.date_last_activity              ,'1 Jan 1900') <> upd_rec.date_last_activity or
              nvl(fnd.date_last_age                   ,'1 Jan 1900') <> upd_rec.date_last_age or
              nvl(fnd.date_last_crlm                  ,'1 Jan 1900') <> upd_rec.date_last_crlm or
              nvl(fnd.date_last_pymt                  ,'1 Jan 1900') <> upd_rec.date_last_pymt or
              nvl(fnd.date_last_rate_chg              ,'1 Jan 1900') <> upd_rec.date_last_rate_chg or
              nvl(fnd.date_last_reage                 ,'1 Jan 1900') <> upd_rec.date_last_reage or
              nvl(fnd.date_last_reclass               ,'1 Jan 1900') <> upd_rec.date_last_reclass or
              nvl(fnd.date_last_return                ,'1 Jan 1900') <> upd_rec.date_last_return or
              nvl(fnd.date_last_revpmt                ,'1 Jan 1900') <> upd_rec.date_last_revpmt or
              nvl(fnd.date_last_rtnchq                ,'1 Jan 1900') <> upd_rec.date_last_rtnchq or
              nvl(fnd.date_last_stmt                  ,'1 Jan 1900') <> upd_rec.date_last_stmt or
              nvl(fnd.days_till_chgoff                ,0) <> upd_rec.days_till_chgoff or
              nvl(fnd.highest_bal_val                 ,0) <> upd_rec.highest_bal_val or
              nvl(fnd.prev_credit_class               ,0) <> upd_rec.prev_credit_class or
              nvl(fnd.prev_credit_limit               ,0) <> upd_rec.prev_credit_limit or
              nvl(fnd.prev_int_val_ytd                ,0) <> upd_rec.prev_int_val_ytd or
              nvl(fnd.prev_int_pd_ytd                 ,0) <> upd_rec.prev_int_pd_ytd or
              nvl(fnd.market_flag_01                  ,0) <> upd_rec.market_flag_01 or
              nvl(fnd.market_flag_02                  ,0) <> upd_rec.market_flag_02 or
              nvl(fnd.market_flag_03                  ,0) <> upd_rec.market_flag_03 or
              nvl(fnd.market_flag_04                  ,0) <> upd_rec.market_flag_04 or
              nvl(fnd.market_flag_05                  ,0) <> upd_rec.market_flag_05 or
              nvl(fnd.market_flag_06                  ,0) <> upd_rec.market_flag_06 or
              nvl(fnd.market_flag_07                  ,0) <> upd_rec.market_flag_07 or
              nvl(fnd.market_flag_08                  ,0) <> upd_rec.market_flag_08 or
              nvl(fnd.market_flag_09                  ,0) <> upd_rec.market_flag_09 or
              nvl(fnd.market_flag_10                  ,0) <> upd_rec.market_flag_10 or
              nvl(fnd.market_flag_11                  ,0) <> upd_rec.market_flag_11 or
              nvl(fnd.market_flag_12                  ,0) <> upd_rec.market_flag_12 or
              nvl(fnd.market_flag_13                  ,0) <> upd_rec.market_flag_13 or
              nvl(fnd.market_flag_14                  ,0) <> upd_rec.market_flag_14 or
              nvl(fnd.market_flag_15                  ,0) <> upd_rec.market_flag_15 or
              nvl(fnd.market_flag_16                  ,0) <> upd_rec.market_flag_16 or
              nvl(fnd.market_flag_17                  ,0) <> upd_rec.market_flag_17 or
              nvl(fnd.market_flag_18                  ,0) <> upd_rec.market_flag_18 or
              nvl(fnd.market_flag_19                  ,0) <> upd_rec.market_flag_19 or
              nvl(fnd.market_flag_20                  ,0) <> upd_rec.market_flag_20 or
              nvl(fnd.last_pymt_val                   ,0) <> upd_rec.last_pymt_val or
              nvl(fnd.promotion_code1                 ,0) <> upd_rec.promotion_code1 or
              nvl(fnd.promotion_code2                 ,0) <> upd_rec.promotion_code2 or
              nvl(fnd.promotion_code3                 ,0) <> upd_rec.promotion_code3 or
              nvl(fnd.promotion_code4                 ,0) <> upd_rec.promotion_code4 or
              nvl(fnd.promotion_status1               ,0) <> upd_rec.promotion_status1 or
              nvl(fnd.promotion_status2               ,0) <> upd_rec.promotion_status2 or
              nvl(fnd.promotion_status3               ,0) <> upd_rec.promotion_status3 or
              nvl(fnd.promotion_status4               ,0) <> upd_rec.promotion_status4 or
              nvl(fnd.retail_plan_code                ,0) <> upd_rec.retail_plan_code or
              nvl(fnd.statmt_flag                     ,0) <> upd_rec.statmt_flag or
              nvl(fnd.statmt_msg_no_1                 ,0) <> upd_rec.statmt_msg_no_1 or
              nvl(fnd.statmt_msg_no_2                 ,0) <> upd_rec.statmt_msg_no_2 or
              nvl(fnd.write_off_days                  ,0) <> upd_rec.write_off_days or
              nvl(fnd.ins_incentv_store               ,0) <> upd_rec.ins_incentv_store or
              nvl(fnd.ins_cancel_date                 ,'1 Jan 1900') <> upd_rec.ins_cancel_date or
              nvl(fnd.ins_dt_lst_billed               ,'1 Jan 1900') <> upd_rec.ins_dt_lst_billed or
              nvl(fnd.ins_dt_lst_claim                ,'1 Jan 1900') <> upd_rec.ins_dt_lst_claim or
              nvl(fnd.ins_effectv_date                ,'1 Jan 1900') <> upd_rec.ins_effectv_date or
              nvl(fnd.ins_enrllmnt_state              ,0) <> upd_rec.ins_enrllmnt_state or
              nvl(fnd.ins_last_premium                ,0) <> upd_rec.ins_last_premium or
              nvl(fnd.ins_premium_mtd                 ,0) <> upd_rec.ins_premium_mtd or
              nvl(fnd.ins_premium                     ,0) <> upd_rec.ins_premium or
              nvl(fnd.ins_premium_state               ,0) <> upd_rec.ins_premium_state or
              nvl(fnd.ins_product                     ,0) <> upd_rec.ins_product or
              nvl(fnd.ins_rsn_cancelled               ,0) <> upd_rec.ins_rsn_cancelled or
              nvl(fnd.ins_reinstmt_date               ,'1 Jan 1900') <> upd_rec.ins_reinstmt_date or
              nvl(fnd.ins_status                      ,0) <> upd_rec.ins_status or
              nvl(fnd.plan_pmt_ovrd_flag              ,0) <> upd_rec.plan_pmt_ovrd_flag or
              nvl(fnd.mktg_promo                      ,0) <> upd_rec.mktg_promo or
              nvl(fnd.no_of_store_pref                ,0) <> upd_rec.no_of_store_pref or
              nvl(fnd.return_mail_cnt                 ,0) <> upd_rec.return_mail_cnt or
              nvl(fnd.loan_drawdown_val               ,0) <> upd_rec.loan_drawdown_val or
              nvl(fnd.loan_instalment                 ,0) <> upd_rec.loan_instalment or
              nvl(fnd.loan_repay_period               ,0) <> upd_rec.loan_repay_period or
              nvl(fnd.loan_tracker                    ,0) <> upd_rec.loan_tracker or
              nvl(fnd.sds_ref                         ,0) <> upd_rec.sds_ref or
              nvl(fnd.test_digit                      ,0) <> upd_rec.test_digit or
              nvl(fnd.test_digit_grp                  ,0) <> upd_rec.test_digit_grp or
              nvl(fnd.debit_order_flag                ,0) <> upd_rec.debit_order_flag or
              nvl(fnd.debit_order_dy                  ,0) <> upd_rec.debit_order_dy or
              nvl(fnd.debit_order_due                 ,0) <> upd_rec.debit_order_due or
              nvl(fnd.dtlst_accstat_chg               ,'1 Jan 1900') <> upd_rec.dtlst_accstat_chg or
              nvl(fnd.lcp_ind                         ,0) <> upd_rec.lcp_ind or
              nvl(fnd.companion_care_ind              ,0) <> upd_rec.companion_care_ind or
              nvl(fnd.accident_benft_ind              ,0) <> upd_rec.accident_benft_ind or
              nvl(fnd.cbp_ind                         ,0) <> upd_rec.cbp_ind or
              nvl(fnd.lbp_ind                         ,0) <> upd_rec.lbp_ind or
              nvl(fnd.ptp_status                      ,0) <> upd_rec.ptp_status or
              nvl(fnd.date_cred_limit                 ,'1 Jan 1900') <> upd_rec.date_cred_limit or
              nvl(fnd.comp_care_lst_prem              ,0) <> upd_rec.comp_care_lst_prem or
              nvl(fnd.comp_care_eff_date              ,'1 Jan 1900') <> upd_rec.comp_care_eff_date or
              nvl(fnd.acc_benft_lst_prem              ,0) <> upd_rec.acc_benft_lst_prem or
              nvl(fnd.acc_benft_eff_date              ,'1 Jan 1900') <> upd_rec.acc_benft_eff_date or
              nvl(fnd.overdue_amt                     ,0) <> upd_rec.overdue_amt or
              nvl(fnd.min_payment                     ,0) <> upd_rec.min_payment or
              nvl(fnd.payment_date                    ,'1 Jan 1900') <> upd_rec.payment_date or
              nvl(fnd.account_contact_id              ,0) <> upd_rec.account_contact_id or
              nvl(fnd.ttd_ind                         ,0) <> upd_rec.ttd_ind or
              nvl(fnd.bureau_score                    ,0) <> upd_rec.bureau_score or
              nvl(fnd.viking_code                     ,0) <> upd_rec.viking_code or
              nvl(fnd.viking_date                     ,'1 Jan 1900') <> upd_rec.viking_date or
              nvl(fnd.viking_amt                      ,0) <> upd_rec.viking_amt or
              nvl(fnd.debit_order_proj_amt            ,0) <> upd_rec.debit_order_proj_amt or
              nvl(fnd.debit_order_br_cd               ,0) <> upd_rec.debit_order_br_cd or
              nvl(fnd.debit_order_exp_dt              ,'1 Jan 1900') <> upd_rec.debit_order_exp_dt or
              nvl(fnd.debit_order_acc_type            ,0) <> upd_rec.debit_order_acc_type or
              nvl(fnd.debit_order_acc_no              ,0) <> upd_rec.debit_order_acc_no or
              nvl(fnd.debit_order_pymt_ind            ,0) <> upd_rec.debit_order_pymt_ind or
              nvl(fnd.dd_status                       ,0) <> upd_rec.dd_status or
              nvl(fnd.clim_review                     ,0) <> upd_rec.clim_review or
              nvl(fnd.dd_load_amt                     ,0) <> upd_rec.dd_load_amt or
              nvl(fnd.date_first_purch                ,'1 Jan 1900') <> upd_rec.date_first_purch or
              nvl(fnd.insurance_active_ind            ,0) <> upd_rec.insurance_active_ind or
              nvl(fnd.loan_restruct_ind               ,0) <> upd_rec.loan_restruct_ind or
              nvl(fnd.loan_restruct_date              ,'1 Jan 1900') <> upd_rec.loan_restruct_date or
              nvl(fnd.residence_id                    ,0) <> upd_rec.residence_id or
              nvl(fnd.debit_order_reversal_count      ,0) <> upd_rec.debit_order_reversal_count or
              nvl(fnd.debit_order_interim_pmt         ,0) <> upd_rec.debit_order_interim_pmt or
              nvl(fnd.debit_order_remitt_method       ,0) <> upd_rec.debit_order_remitt_method or
              nvl(fnd.staff_company_code              ,0) <> upd_rec.staff_company_code or
              nvl(fnd.write_off_ind                   ,0) <> upd_rec.write_off_ind or
              nvl(fnd.write_off_date                  ,'1 Jan 1900') <> upd_rec.write_off_date or
              nvl(fnd.write_off_value                 ,0) <> upd_rec.write_off_value or
              nvl(fnd.initiation_fee                  ,0) <> upd_rec.initiation_fee or
              nvl(fnd.monthly_service_fee             ,0) <> upd_rec.monthly_service_fee or
              nvl(fnd.initial_interest_rate           ,0) <> upd_rec.initial_interest_rate or
              nvl(fnd.delivery_method                 ,0) <> upd_rec.delivery_method or
              nvl(fnd.delivery_address                ,0) <> upd_rec.delivery_address or
              nvl(fnd.LEGAL_STATUS                    ,0) <> upd_rec.LEGAL_STATUS or
              nvl(fnd.LEGAL_STATUS_DATE               ,'1 Jan 1900') <> upd_rec.LEGAL_STATUS_DATE or
              nvl(fnd.FIRST_PLACEMENT_INDICATOR       ,0) <> upd_rec.FIRST_PLACEMENT_INDICATOR or
              nvl(fnd.FIRST_PLACEMENT_DATE            ,'1 Jan 1900') <> upd_rec.FIRST_PLACEMENT_DATE or
              nvl(fnd.SECOND_PLACEMENT_INDICATOR      ,0) <> upd_rec.SECOND_PLACEMENT_INDICATOR or
              nvl(fnd.SECOND_PLACEMENT_DATE           ,'1 Jan 1900') <> upd_rec.SECOND_PLACEMENT_DATE or
              nvl(fnd.THIRD_PLACEMENT_INDICATOR       ,0) <> upd_rec.THIRD_PLACEMENT_INDICATOR or
              nvl(fnd.THIRD_PLACEMENT_DATE            ,'1 Jan 1900') <> upd_rec.THIRD_PLACEMENT_DATE or
              nvl(fnd.MONTH6_REVIEW_INDICATOR         ,0) <> upd_rec.MONTH6_REVIEW_INDICATOR or
              nvl(fnd.MONTH6_REVIEW_DATE              ,'1 Jan 1900') <> upd_rec.MONTH6_REVIEW_DATE

            );

      g_recs_updated := g_recs_updated + 1;
   end loop;


      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;


--**************************************************************************************************
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin


--   g_recs_hospital := g_recs_hospital + sql%rowcount;

   commit;


   exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
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
-- Call the bulk routines
--**************************************************************************************************


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

--    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    create_dummy_masters;

    select count(*)
    into   g_recs_read
    from   stg_vsn_all_prod_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    flagged_records_hospital;


--    Taken out for better performance --------------------
--    update stg_vsn_all_prod_cpy
--    set    sys_process_code = 'Y';





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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


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
       RAISE;
end wh_fnd_cust_014B;
