-- ****** Object: Procedure W7131037.WH_PRF_CUST_157_0410 Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_157_0410" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        FEB 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust value segment table in the performance layer
--               with added value ex basket item.
--  Tables:      Input  - dim_customer
--               Output - cust_talktome
--  Packages:    constants, dwh_log, dwh_valid
--
--   !!!!!!!!!!!!!!!!!!!!!!!THIS IS THE NON FOODS PROGRAM !!!!!!!!!!!!!!!!!!!!!!!
--
--  Maintenance:
--  HardCode Value 295 needs to change regularly as inflation happens - address_variables
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            cust_talktome%rowtype;
g_customer_key       fnd_wfs_customer_absa.customer_key%type ;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_max_information_date  date;
g_max_run_date          date;

g_chgoff_count          integer;
g_zw_count              integer;
g_open_count            integer;
g_cr_chgoff_count       integer;
g_count                 integer;
g_storecard_count       integer;

g_max_port_change_date  date;
g_max_port_close_date   date;
g_max_legal_date        date;
g_max_chgoff_date       date;

g_have_open_acc         integer;
g_crcard_open           integer;
g_other_open            integer;
g_stcard_open           integer;

g_chgoff_ind            integer;

L_MESSAGE            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_157U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE cust_talktome EX dim_customer';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_talktome%rowtype index by binary_integer;
type tbl_array_u is table of cust_talktome%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_dim_customer is
select    /*+ FULL(dc)  parallel (4) */
          dc.customer_no,
          max(	dc.wfs_customer_no)	 wfs_customer_no,
          max(	dc.title_code)	 title_code,
          max(	dc.first_middle_name_initial)	 first_middle_name_initial,
          max(	dc.first_name)	 first_name,
          max(	dc.last_name)	 last_name,
          max(	dc.postal_address_line_1)	 postal_address_line_1,
          max(	dc.postal_address_line_2)	 postal_address_line_2,
          max(	dc.postal_address_line_3)	 postal_address_line_3,
          max(	dc.postal_code)	 postal_code,
          max(	dc.home_phone_no)	 home_phone_no,
          max(	dc.work_phone_no)	 work_phone_no,
          max(	dc.preferred_language)	 preferred_language,
          max(	dc.age_acc_holder)	 age_acc_holder,
          max(	dc.gender_code  )  	 gender_code  ,
          max(	dc.last_transaction_date  )  	 last_transaction_date  ,
          max(	dc.c2_create_date)	 c2_create_date,
          max(	dc.ww_dm_sms_opt_out_ind)	 ww_dm_sms_opt_out_ind,
          max(	dc.ww_dm_email_opt_out_ind)	 ww_dm_email_opt_out_ind,
          max(	dc.ww_dm_post_opt_out_ind)	 ww_dm_post_opt_out_ind,
          max(	dc.ww_dm_phone_opt_out_ind)	 ww_dm_phone_opt_out_ind,
          max(	dc.ww_man_sms_opt_out_ind)	 ww_man_sms_opt_out_ind,
          max(	dc.ww_man_email_opt_out_ind)	 ww_man_email_opt_out_ind,
          max(	dc.ww_man_post_opt_out_ind)	 ww_man_post_opt_out_ind,
          max(	dc.ww_man_phone_opt_out_ind)	 ww_man_phone_opt_out_ind,
          max(	dc.wfs_dm_sms_opt_out_ind)	 wfs_dm_sms_opt_out_ind,
          max(	dc.wfs_dm_email_opt_out_ind)	 wfs_dm_email_opt_out_ind,
          max(	dc.wfs_dm_post_opt_out_ind)	 wfs_dm_post_opt_out_ind,
          max(	dc.wfs_dm_phone_opt_out_ind)	 wfs_dm_phone_opt_out_ind,
          max(	dc.wfs_con_sms_opt_out_ind)	 wfs_con_sms_opt_out_ind,
          max(	dc.wfs_con_email_opt_out_ind)	 wfs_con_email_opt_out_ind,
          max(	dc.wfs_con_post_opt_out_ind)	 wfs_con_post_opt_out_ind,
          max(	dc.wfs_con_phone_opt_out_ind)	 wfs_con_phone_opt_out_ind,
          max(	dc.preference_1_ind)	 preference_1_ind,
          max(	dc.preference_2_ind)	 preference_2_ind,
          max(	dc.preference_3_ind)	 preference_3_ind,
          max(	dc.preference_4_ind)	 preference_4_ind,
          max(	dc.preference_5_ind)	 preference_5_ind,
          max(	dc.preference_6_ind)	 preference_6_ind,
          max(	dc.preference_7_ind)	 preference_7_ind,
          max(	dc.identity_document_code)	 identity_document_code,
          sum((case when cp.product_code_no in (1,2,3,4,6,9,21,80) and cp.portfolio_status_desc <> 'Closed' then 1 end)) stcard,    --maybe max
          sum((case when cp.product_code_no in (20)                and cp.portfolio_status_desc <> 'Closed' then 1 end)) crcard,
          sum((case when cp.product_code_no in (28,30)             and cp.portfolio_status_desc <> 'Closed' then 1 end)) diffcard,
          sum((case when cp.product_code_no in (19)                and cp.portfolio_status_desc <> 'Closed' then 1 end)) myschool,
          sum((case when cp.product_code_no in (92)                and cp.portfolio_status_desc <> 'Closed' then 1 end)) vitality,
          sum((case when cp.product_code_no in (99)                and cp.portfolio_status_desc <> 'Closed' then 1 end)) littlew,
          sum((case when cp.product_code_no in (24)                and cp.portfolio_status_desc <> 'Closed' then 1 end)) other,
          sum((case when cp.product_code_no in (1)                 and cp.portfolio_status_desc <> 'Closed' then product_no end)) wfs_account_no
from      dim_customer dc,
          dim_customer_portfolio  cp
WHERE     dc.last_transaction_date = '30 SEP 2015' and
          dc.customer_no           = cp.customer_no  and
          cp.product_code_no       in (1,2,3,4,6,9,19,20,21,24,28,30,80,92,99)
group by  dc.customer_no          ;
--where  a.c2_create_date        > g_date - 730
--or     a.last_transaction_date > g_date - 730 ;


g_rec_in             c_dim_customer%rowtype;

-- For input bulk collect --
type stg_array is table of c_dim_customer%rowtype;
a_stg_input      stg_array;


--cursor c_dim_portfolio_choff is
--   select  /*+ parallel (cp,4) */  *
--   from   dim_customer_portfolio  cp
--   where  cp.customer_no           =  g_rec_in.customer_no and
--          cp.product_code_no in (1,2,3,4,6,9,21,80) and
--          cp.portfolio_status_desc <> 'Closed';

--**************************************************************************************************
-- get data ex the ABSA customer master joining on id no.
--**************************************************************************************************
procedure read_portfolio as

begin

  g_have_open_acc         := 0 ;
  g_crcard_open           := 0 ;
  g_other_open            := 0 ;
  g_stcard_open           := 0 ;

--WHAT ACCOUNTS DO I HAVE
        if nvl(g_rec_in.stcard,0)   > 0    then g_rec_out.storcard_ind       := 1; end if;
        if nvl(g_rec_in.crcard,0)   > 0    then g_rec_out.creditcard_ind     := 1; end if;
        if nvl(g_rec_in.diffcard,0) > 0    then g_rec_out.differencecard_ind := 1; end if;
        if nvl(g_rec_in.myschool,0) > 0    then g_rec_out.myschool_ind       := 1; end if;
        if nvl(g_rec_in.vitality,0) > 0    then g_rec_out.vitality_ind       := 1; end if;
        if nvl(g_rec_in.littlew,0)  > 0    then g_rec_out.littleworld_ind    := 1; end if;
        if nvl(g_rec_in.other,0)    > 0    then g_other_open                 := 1; end if;

-- DO I HAVE ANY OPEN ACCOUNTS

    if  g_rec_out.storcard_ind         = 1 or
        g_rec_out.creditcard_ind       = 1 or
        g_rec_out.differencecard_ind   = 1 or
        g_rec_out.myschool_ind         = 1 or
        g_rec_out.littleworld_ind      = 1 or
        g_rec_out.vitality_ind         = 1 or
        g_other_open                   = 1 then
        g_have_open_acc               := 1;
    end if;
    if  g_rec_out.storcard_ind         = 1 then
        g_stcard_open                 := 1;
    end if;
    if  g_rec_out.creditcard_ind       = 1 then
        g_crcard_open                 := 1;
    end if;
    if  g_rec_out.differencecard_ind   = 1 or
        g_rec_out.myschool_ind         = 1 or
        g_rec_out.littleworld_ind      = 1 or
        g_rec_out.vitality_ind         = 1 then
        g_other_open                  := 1;
    end if;

    commit;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'READ PORTFOLIO FAILED '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end read_portfolio;



--**************************************************************************************************
-- get data ex the ABSA customer master joining on id no.
--**************************************************************************************************
procedure get_crd_daily_transactions as

begin
   g_max_legal_date := '1 Jan 2000';
   g_customer_key   := null;

-- Determine if you can find aan ABSA master/transaction based on ID on dim_customer driving table
   begin

   select MAX(customer_key)
   into   g_customer_key
   from   fnd_wfs_customer_absa
   where  id_number = g_rec_in.identity_document_code;

   exception
            when no_data_found then
               g_rec_out.deceased_ind                  := 0 ;
               g_rec_out.delinquent_ind                := 0 ;
               g_rec_out.debt_review_ind               := 0 ;
               g_rec_out.creditcard_otb                := 0 ;
               g_rec_out.closed_charged_off_ind        := 0 ;
               return;
   end;

-- CREDITCARD_OTB

   begin
   select  sum(purchase_limit_amt + total_budget_balance_amt  + account_balance)   creditcard_otb, max(nvl(transfer_to_legal_orig_date,'1 Jan 2000'))
   into    g_rec_out.creditcard_otb, g_max_legal_date
   from    fnd_wfs_crd_acc_dly
   where   information_date = g_max_information_date and
           customer_key = g_customer_key ;

   exception
            when no_data_found then
               g_rec_out.creditcard_otb                 := 0 ;
   end;

--DECEASED,DELINQUENT AND DEBT REVIEW
   begin
   select distinct 1 as debt_review_ind
   into   g_rec_out.debt_review_ind
   from   fnd_wfs_crd_acc_dly
   where  information_date = g_max_information_date and
          customer_key = g_customer_key and
          substr(account_number,1,3) <> '333' and
          card_account_status_code in ('D0P','D1P','D2P','D3P','D4P','B0P','B1P','B2P','B3P','B4P','DCP','TLP','T0P','T1P','T2P','T3P','T4P','T5P','T6P','T7P','T8P');
   exception
            when no_data_found then
               g_rec_out.debt_review_ind               := 0 ;
   end;

   begin
   select distinct 1 as deceased_ind
   into   g_rec_out.deceased_ind
   from   fnd_wfs_crd_acc_dly
   where  information_date = g_max_information_date and
          customer_key = g_customer_key and
          card_account_status_code in ('ESP','LDP');
   exception
            when no_data_found then
               g_rec_out.deceased_ind                  := 0 ;
   end;

   begin
   select distinct 1 as delinqent_ind
   into   g_rec_out.delinquent_ind
   from   fnd_wfs_crd_acc_dly
   where  information_date = g_max_information_date and
          customer_key = g_customer_key and
          substr(account_number,1,3) <> '333' and
          delinquent_cycles_count not in (0,1,2,3);


   exception
            when no_data_found then
               g_rec_out.delinquent_ind                := 0 ;
   end;

commit;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'GET_CRD_DAILY_TRANSACTIONS '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end get_crd_daily_transactions;

--**************************************************************************************************
-- GET DATA EX 60 DAY vISION TABLE FOR STORE CARDS
--**************************************************************************************************
procedure get_60dy_transactions as

begin

   g_storecard_count := 0;
   g_max_chgoff_date     := '1 Jan 1900';

-- g_rec_in.wfs_customer_no may be null or may not find a record on 60dy
   if g_rec_in.wfs_customer_no is null then
      return;
   end if;

--STORE_OTB   --DECEASED,DELINQUENT AND DEBT REVIEW
   begin
   select   /*+ parallel (dy,4) */
   NVL(sum(credit_limit - current_balance),0)    storecard_otb, count(*),max(chgoff_date),
   max((case when  block_code_1 in ('D','K') then 1 end)) deceased_ind,
   max((case when  delinquency_cycle not in ('0','1','2','3')  and   account_status in ('A','D','I') then 1 end)) delinquent_ind,
   max((case when  block_code_1 in ('T','U','Q')  and  account_status not in ('A','D','I') then 1 end)) debt_review_ind ,
   max((case when  product_code_no = 1  and  account_status  in ('Z','W') AND g_stcard_open     = 1 then 1 end)) charge_off_ind
   into    g_rec_out.storecard_otb, g_storecard_count,g_max_chgoff_date,
           g_rec_out.deceased_ind,g_rec_out.delinquent_ind,g_rec_out.debt_review_ind, g_chgoff_ind
   from    fnd_wfs_cust_perf_60dy dy
   where   run_date = g_max_run_date
   and     g_rec_in.wfs_customer_no = wfs_customer_no
   and     product_code_no in (1,2,3,4,5,6,21,80);

   exception
            when no_data_found then
               g_rec_out.storecard_otb                  := 0 ;

   end;



commit;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'GET_60DY_TRANSACTIONS '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end get_60dy_transactions;

--**************************************************************************************************
-- CALCULATE CLOSED_CHARGEOFF STATUS
--**************************************************************************************************
procedure calculate_closed_crgoff_status as

begin

   g_rec_out.closed_charged_off_ind := 0;
   g_cr_chgoff_count  := 0;
   g_chgoff_count     := 0;
   g_zw_count         := 0;

  if g_other_open  = 0 then

-- SEE IF ANY CREDIT CARDS ARE CHARGED OFF
    if g_crcard_open     = 1 then
 /*
       begin
       select  count(*)
       into    g_cr_chgoff_count
       from    fnd_wfs_crd_acc_dly
       where   information_date = g_max_information_date and
               customer_key     = g_customer_key and
               transfer_to_legal_orig_date is not null
               ;

       exception
                when no_data_found then
                   g_cr_chgoff_count                 := 0 ;
       end;


       if  g_cr_chgoff_count > 0 then
           g_crcard_open    := 0;
       end if;
*/
      if g_max_legal_date > '1 Jan 2000' then
              g_crcard_open    := 0;
      end if;

    end if;

-- PROCESS STORE CARD TO SEE IF CHARGED OFF
    if g_stcard_open     = 1 then
/*
      begin
         select  count(*)
         into    g_zw_count
         from    fnd_wfs_cust_perf_60dy
         where   g_max_run_date           = run_date
         and     g_rec_in.wfs_customer_no = wfs_customer_no
         and     g_rec_in.wfs_account_no  = wfs_account_no
         and     product_code_no  = 1
         and     account_status in ('Z','W');

         exception
                  when no_data_found then
                     g_zw_count                 := 0 ;
         end;

         g_chgoff_count := g_chgoff_count + g_zw_count;

       if  g_chgoff_count   =  g_rec_in.stcard then
           g_stcard_open    := 0;
     end if;
*/
     if   g_chgoff_ind     =  g_rec_in.stcard then
          g_stcard_open    := 0;
     end if;

    end if;
  end if;

--SET 'HAVE OPEN ACCOUNT' BASED ON ABOVE
  if  g_crcard_open     = 0 and
      g_stcard_open     = 0 and
      g_other_open      = 0 then
      g_have_open_acc  := 0 ;
  end if;

--IF ALL ACCOUNTS CHARGED OFF OR CLOSED THEN
  if g_have_open_acc = 0 then

--GET MAX PORTFOLIO DATES
       select max(portfolio_change_date),max(portfolio_close_date)
       into   g_max_port_change_date,g_max_port_close_date
       from   dim_customer_portfolio  cp
       where  cp.customer_no           =  g_rec_in.customer_no and
              cp.product_code_no in (1,2,3,4,6,9,19,20,21,24,28,30,80,92,99);

--SET TO CLOSED/CHARGED OFF IF CONDITION MET
      if g_max_port_change_date > g_rec_in.last_transaction_date or
         g_max_port_close_date  > g_rec_in.last_transaction_date or
         g_max_legal_date       > g_rec_in.last_transaction_date or
         g_max_chgoff_date      > g_rec_in.last_transaction_date then
         g_rec_out.closed_charged_off_ind := 1;
      end if;
  end if;

commit;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'CLOSED CHARGEOFF STATUS '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end calculate_closed_crgoff_status;


--**************************************************************************************************
-- CLEAN DATA
--**************************************************************************************************
procedure apply_business_rules_and_clean as

begin


commit;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'APPLY_BUSINESS_RULES '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end apply_business_rules_and_clean;




--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.customer_no                    := g_rec_in.customer_no;
   g_rec_out.run_date                       := g_date;
   g_rec_out.wfs_customer_no	              := g_rec_in.wfs_customer_no;
   g_rec_out.wfs_account_no	                := null;
   g_rec_out.title_code 	                  := g_rec_in.title_code;
   g_rec_out.first_middle_name_initial 	    := g_rec_in.first_middle_name_initial;
   g_rec_out.first_name 	                  := g_rec_in.first_name;
   g_rec_out.last_name 	                    := g_rec_in.last_name;
   g_rec_out.postal_address_line_1 	        := g_rec_in.postal_address_line_1;
   g_rec_out.postal_address_line_2 	        := g_rec_in.postal_address_line_2;
   g_rec_out.postal_address_line_3 	        := g_rec_in.postal_address_line_3;
   g_rec_out.postal_code	                  := g_rec_in.postal_code;
   g_rec_out.home_phone_no	                := g_rec_in.home_phone_no;
   g_rec_out.work_phone_no	                := g_rec_in.work_phone_no;
   g_rec_out.preferred_language 	          := g_rec_in.preferred_language;
   g_rec_out.account_holder_age             := g_rec_in.age_acc_holder;
   g_rec_out.gender_code                    := g_rec_in.gender_code  ;
   g_rec_out.max_tran_date                  := g_rec_in.last_transaction_date  ;
   g_rec_out.c2_create_date 	              := g_rec_in.c2_create_date;
   g_rec_out.ww_dm_sms_opt_out_ind          := g_rec_in.ww_dm_sms_opt_out_ind;
   g_rec_out.ww_dm_email_opt_out_ind        := g_rec_in.ww_dm_email_opt_out_ind;
   g_rec_out.ww_dm_post_opt_out_ind         := g_rec_in.ww_dm_post_opt_out_ind;
   g_rec_out.ww_dm_phone_opt_out_ind        := g_rec_in.ww_dm_phone_opt_out_ind;
   g_rec_out.ww_man_sms_opt_out_ind         := g_rec_in.ww_man_sms_opt_out_ind;
   g_rec_out.ww_man_email_opt_out_ind       := g_rec_in.ww_man_email_opt_out_ind;
   g_rec_out.ww_man_post_opt_out_ind        := g_rec_in.ww_man_post_opt_out_ind;
   g_rec_out.ww_man_phone_opt_out_ind       := g_rec_in.ww_man_phone_opt_out_ind;
   g_rec_out.wfs_dm_sms_opt_out_ind         := g_rec_in.wfs_dm_sms_opt_out_ind;
   g_rec_out.wfs_dm_email_opt_out_ind       := g_rec_in.wfs_dm_email_opt_out_ind;
   g_rec_out.wfs_dm_post_opt_out_ind        := g_rec_in.wfs_dm_post_opt_out_ind;
   g_rec_out.wfs_dm_phone_opt_out_ind       := g_rec_in.wfs_dm_phone_opt_out_ind;
   g_rec_out.wfs_con_sms_opt_out_ind        := g_rec_in.wfs_con_sms_opt_out_ind;
   g_rec_out.wfs_con_email_opt_out_ind      := g_rec_in.wfs_con_email_opt_out_ind;
   g_rec_out.wfs_con_post_opt_out_ind       := g_rec_in.wfs_con_post_opt_out_ind;
   g_rec_out.wfs_con_phone_opt_out_ind      := g_rec_in.wfs_con_phone_opt_out_ind;
   g_rec_out.preference_1_ind               := g_rec_in.preference_1_ind;
   g_rec_out.preference_2_ind               := g_rec_in.preference_2_ind;
   g_rec_out.preference_3_ind               := g_rec_in.preference_3_ind;
   g_rec_out.preference_4_ind               := g_rec_in.preference_4_ind;
   g_rec_out.preference_5_ind               := g_rec_in.preference_5_ind;
   g_rec_out.preference_6_ind               := g_rec_in.preference_6_ind;
   g_rec_out.preference_7_ind               := g_rec_in.preference_7_ind;

   g_rec_out.storcard_ind                   := 0 ;
   g_rec_out.creditcard_ind                 := 0 ;
   g_rec_out.differencecard_ind             := 0 ;
   g_rec_out.myschool_ind                   := 0 ;
   g_rec_out.littleworld_ind                := 0 ;
   g_rec_out.vitality_ind                   := 0 ;


   read_portfolio;

   g_rec_out.storecard_otb                 := 0 ;
   g_rec_out.creditcard_otb                := 0 ;
   g_rec_out.closed_charged_off_ind        := 0 ;
   g_rec_out.debt_review_ind               := 0 ;
   g_rec_out.deceased_ind                  := 0 ;
   g_rec_out.delinquent_ind                := 0 ;
   g_max_legal_date                        := '1 Jan 2000';
   g_max_chgoff_date                       := '1 Jan 1900';

   get_crd_daily_transactions;

   get_60dy_transactions;

   calculate_closed_crgoff_status;

   g_rec_out.email_address 	               := null;
   g_rec_out.cell_no 	                     := null;
   g_rec_out.birthday_month                := null;

   apply_business_rules_and_clean;

   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into cust_talktome values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||SQLERRM(-SQL%BULK_EXCEPTIONS(I).ERROR_CODE)||
                       ' '||a_tbl_insert(g_error_index).customer_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;




--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
-- Place record into array for later bulk writing

   a_count_i               := a_count_i + 1;
   a_tbl_insert(a_count_i) := g_rec_out;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    execute immediate 'alter session enable parallel dml';

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF cust_talktome EX dim_customer STARTED AT '||
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

--**************************************************************************************************
-- Look up range of weeks to be processed and store in variables
--**************************************************************************************************
---PLACE YOUR ONE OFF READS FROM THE CALENDAR TO GET DATE RANGES, FILTER DATES ETC ETC.


    select   max(information_date)
    into     g_max_information_date
    from     fnd_wfs_crd_acc_dly;

    select   max(run_date)
    into     g_max_run_date
    from     fnd_wfs_cust_perf_60dy;

    l_text := 'INFORMATION DATE BEING ACCESSED:- '||g_max_information_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := '60 DY DATE BEING ACCESSED:- '||g_max_run_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- DROP ALL DATA FROM OUTPUT TABLE UP FRONT
--**************************************************************************************************

    l_text := 'TRUNCATE TABLE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table cust_talktome';

--**************************************************************************************************
    l_text := 'TALKTOME PROCESS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    open c_dim_customer;
    fetch c_dim_customer bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_dim_customer bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_customer;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;

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
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
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

END "WH_PRF_CUST_157_0410";
