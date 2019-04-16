--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_014OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_014OLD" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alastair de Wet
--  Purpose:     Create Dim _customer_card dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_vsn_all_prod_cpy
--               Output - fnd_wfs_all_prod
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_vsn_all_prod_hsp.sys_process_msg%type;
g_rec_out            fnd_wfs_all_prod%rowtype;
g_rec_in             stg_vsn_all_prod_cpy%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_014U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WFS_ALL_PROD DIM EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_vsn_all_prod_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_wfs_all_prod%rowtype index by binary_integer;
type tbl_array_u is table of fnd_wfs_all_prod%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_vsn_all_prod_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_vsn_all_prod_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_vsn_all_prod is
   select *
   from stg_vsn_all_prod_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                          := 'N';

   g_rec_out.wfs_account_no                  := g_rec_in.wfs_account_no;
   g_rec_out.wfs_customer_no                 := g_rec_in.wfs_customer_no;
   g_rec_out.product_code_no                 := g_rec_in.product_code_no;
   g_rec_out.identity_no                     := g_rec_in.identity_no;
   g_rec_out.account_status                  := g_rec_in.account_status;
   g_rec_out.application_no                  := g_rec_in.application_no;
   g_rec_out.application_score               := g_rec_in.application_score;
   g_rec_out.behaviour_score01               := g_rec_in.behaviour_score01;
   g_rec_out.behaviour_score02               := g_rec_in.behaviour_score02;
   g_rec_out.behaviour_score03               := g_rec_in.behaviour_score03;
   g_rec_out.behaviour_score04               := g_rec_in.behaviour_score04;
   g_rec_out.behaviour_score05               := g_rec_in.behaviour_score05;
   g_rec_out.behaviour_score06               := g_rec_in.behaviour_score06;
   g_rec_out.behaviour_score07               := g_rec_in.behaviour_score07;
   g_rec_out.behaviour_score08               := g_rec_in.behaviour_score08;
   g_rec_out.behaviour_score09               := g_rec_in.behaviour_score09;
   g_rec_out.behaviour_score10               := g_rec_in.behaviour_score10;
   g_rec_out.behaviour_score11               := g_rec_in.behaviour_score11;
   g_rec_out.behaviour_score12               := g_rec_in.behaviour_score12;
   g_rec_out.propensty_score01               := g_rec_in.propensty_score01;
   g_rec_out.propensty_score02               := g_rec_in.propensty_score02;
   g_rec_out.propensty_score03               := g_rec_in.propensty_score03;
   g_rec_out.propensty_score04               := g_rec_in.propensty_score04;
   g_rec_out.propensty_score05               := g_rec_in.propensty_score05;
   g_rec_out.propensty_score06               := g_rec_in.propensty_score06;
   g_rec_out.propensty_score07               := g_rec_in.propensty_score07;
   g_rec_out.propensty_score08               := g_rec_in.propensty_score08;
   g_rec_out.propensty_score09               := g_rec_in.propensty_score09;
   g_rec_out.propensty_score10               := g_rec_in.propensty_score10;
   g_rec_out.propensty_score11               := g_rec_in.propensty_score11;
   g_rec_out.propensty_score12               := g_rec_in.propensty_score12;
   g_rec_out.attrition_score01               := g_rec_in.attrition_score01;
   g_rec_out.attrition_score02               := g_rec_in.attrition_score02;
   g_rec_out.attrition_score03               := g_rec_in.attrition_score03;
   g_rec_out.attrition_score04               := g_rec_in.attrition_score04;
   g_rec_out.attrition_score05               := g_rec_in.attrition_score05;
   g_rec_out.attrition_score06               := g_rec_in.attrition_score06;
   g_rec_out.attrition_score07               := g_rec_in.attrition_score07;
   g_rec_out.attrition_score08               := g_rec_in.attrition_score08;
   g_rec_out.attrition_score09               := g_rec_in.attrition_score09;
   g_rec_out.attrition_score10               := g_rec_in.attrition_score10;
   g_rec_out.attrition_score11               := g_rec_in.attrition_score11;
   g_rec_out.attrition_score12               := g_rec_in.attrition_score12;
   g_rec_out.date_opened                     := g_rec_in.date_opened;
   g_rec_out.date_last_pchs                  := g_rec_in.date_last_pchs;
   g_rec_out.credit_limit                    := g_rec_in.credit_limit;
   g_rec_out.current_balance                 := g_rec_in.current_balance;
   g_rec_out.open_to_buy                     := g_rec_in.open_to_buy;
   g_rec_out.last_pchs_val                   := g_rec_in.last_pchs_val;
   g_rec_out.pchs_val_ytd                    := g_rec_in.pchs_val_ytd;
   g_rec_out.pchs_val_ltd                    := g_rec_in.pchs_val_ltd;
   g_rec_out.store_of_pref1                  := g_rec_in.store_of_pref1;
   g_rec_out.store_of_pref2                  := g_rec_in.store_of_pref2;
   g_rec_out.store_of_pref3                  := g_rec_in.store_of_pref3;
   g_rec_out.block_code1                     := g_rec_in.block_code1;
   g_rec_out.block_code2                     := g_rec_in.block_code2;
   g_rec_out.no_of_cards                     := g_rec_in.no_of_cards;
   g_rec_out.date_last_updated               := g_rec_in.date_last_updated;
   g_rec_out.chgoff_val                      := g_rec_in.chgoff_val;
   g_rec_out.chgoff_rsn1                     := g_rec_in.chgoff_rsn1;
   g_rec_out.chgoff_rsn2                     := g_rec_in.chgoff_rsn2;
   g_rec_out.chgoff_status                   := g_rec_in.chgoff_status;
   g_rec_out.times_in_coll                   := g_rec_in.times_in_coll;
   g_rec_out.date_application                := g_rec_in.date_application;
   g_rec_out.date_chgoff                     := g_rec_in.date_chgoff;
   g_rec_out.date_closed                     := g_rec_in.date_closed;
   g_rec_out.date_highest_bal                := g_rec_in.date_highest_bal;
   g_rec_out.date_last_activity              := g_rec_in.date_last_activity;
   g_rec_out.date_last_age                   := g_rec_in.date_last_age;
   g_rec_out.date_last_crlm                  := g_rec_in.date_last_crlm;
   g_rec_out.date_last_pymt                  := g_rec_in.date_last_pymt;
   g_rec_out.date_last_rate_chg              := g_rec_in.date_last_rate_chg;
   g_rec_out.date_last_reage                 := g_rec_in.date_last_reage;
   g_rec_out.date_last_reclass               := g_rec_in.date_last_reclass;
   g_rec_out.date_last_return                := g_rec_in.date_last_return;
   g_rec_out.date_last_revpmt                := g_rec_in.date_last_revpmt;
   g_rec_out.date_last_rtnchq                := g_rec_in.date_last_rtnchq;
   g_rec_out.date_last_stmt                  := g_rec_in.date_last_stmt;
   g_rec_out.days_till_chgoff                := g_rec_in.days_till_chgoff;
   g_rec_out.highest_bal_val                 := g_rec_in.highest_bal_val;
   g_rec_out.prev_credit_class               := g_rec_in.prev_credit_class;
   g_rec_out.prev_credit_limit               := g_rec_in.prev_credit_limit;
   g_rec_out.prev_int_val_ytd                := g_rec_in.prev_int_val_ytd;
   g_rec_out.prev_int_pd_ytd                 := g_rec_in.prev_int_pd_ytd;
   g_rec_out.market_flag_01                  := g_rec_in.market_flag_01;
   g_rec_out.market_flag_02                  := g_rec_in.market_flag_02;
   g_rec_out.market_flag_03                  := g_rec_in.market_flag_03;
   g_rec_out.market_flag_04                  := g_rec_in.market_flag_04;
   g_rec_out.market_flag_05                  := g_rec_in.market_flag_05;
   g_rec_out.market_flag_06                  := g_rec_in.market_flag_06;
   g_rec_out.market_flag_07                  := g_rec_in.market_flag_07;
   g_rec_out.market_flag_08                  := g_rec_in.market_flag_08;
   g_rec_out.market_flag_09                  := g_rec_in.market_flag_09;
   g_rec_out.market_flag_10                  := g_rec_in.market_flag_10;
   g_rec_out.market_flag_11                  := g_rec_in.market_flag_11;
   g_rec_out.market_flag_12                  := g_rec_in.market_flag_12;
   g_rec_out.market_flag_13                  := g_rec_in.market_flag_13;
   g_rec_out.market_flag_14                  := g_rec_in.market_flag_14;
   g_rec_out.market_flag_15                  := g_rec_in.market_flag_15;
   g_rec_out.market_flag_16                  := g_rec_in.market_flag_16;
   g_rec_out.market_flag_17                  := g_rec_in.market_flag_17;
   g_rec_out.market_flag_18                  := g_rec_in.market_flag_18;
   g_rec_out.market_flag_19                  := g_rec_in.market_flag_19;
   g_rec_out.market_flag_20                  := g_rec_in.market_flag_20;
   g_rec_out.last_pymt_val                   := g_rec_in.last_pymt_val;
   g_rec_out.promotion_code1                 := g_rec_in.promotion_code1;
   g_rec_out.promotion_code2                 := g_rec_in.promotion_code2;
   g_rec_out.promotion_code3                 := g_rec_in.promotion_code3;
   g_rec_out.promotion_code4                 := g_rec_in.promotion_code4;
   g_rec_out.promotion_status1               := g_rec_in.promotion_status1;
   g_rec_out.promotion_status2               := g_rec_in.promotion_status2;
   g_rec_out.promotion_status3               := g_rec_in.promotion_status3;
   g_rec_out.promotion_status4               := g_rec_in.promotion_status4;
   g_rec_out.retail_plan_code                := g_rec_in.retail_plan_code;
   g_rec_out.statmt_flag                     := g_rec_in.statmt_flag;
   g_rec_out.statmt_msg_no_1                 := g_rec_in.statmt_msg_no_1;
   g_rec_out.statmt_msg_no_2                 := g_rec_in.statmt_msg_no_2;
   g_rec_out.write_off_days                  := g_rec_in.write_off_days;
   g_rec_out.ins_incentv_store               := g_rec_in.ins_incentv_store;
   g_rec_out.ins_cancel_date                 := g_rec_in.ins_cancel_date;
   g_rec_out.ins_dt_lst_billed               := g_rec_in.ins_dt_lst_billed;
   g_rec_out.ins_dt_lst_claim                := g_rec_in.ins_dt_lst_claim;
   g_rec_out.ins_effectv_date                := g_rec_in.ins_effectv_date;
   g_rec_out.ins_enrllmnt_state              := g_rec_in.ins_enrllmnt_state;
   g_rec_out.ins_last_premium                := g_rec_in.ins_last_premium;
   g_rec_out.ins_premium_mtd                 := g_rec_in.ins_premium_mtd;
   g_rec_out.ins_premium                     := g_rec_in.ins_premium;
   g_rec_out.ins_premium_state               := g_rec_in.ins_premium_state;
   g_rec_out.ins_product                     := g_rec_in.ins_product;
   g_rec_out.ins_rsn_cancelled               := g_rec_in.ins_rsn_cancelled;
   g_rec_out.ins_reinstmt_date               := g_rec_in.ins_reinstmt_date;
   g_rec_out.ins_status                      := g_rec_in.ins_status;
   g_rec_out.plan_pmt_ovrd_flag              := g_rec_in.plan_pmt_ovrd_flag;
   g_rec_out.mktg_promo                      := g_rec_in.mktg_promo;
   g_rec_out.no_of_store_pref                := g_rec_in.no_of_store_pref;
   g_rec_out.return_mail_cnt                 := g_rec_in.return_mail_cnt;
   g_rec_out.loan_drawdown_val               := g_rec_in.loan_drawdown_val;
   g_rec_out.loan_instalment                 := g_rec_in.loan_instalment;
   g_rec_out.loan_repay_period               := g_rec_in.loan_repay_period;
   g_rec_out.loan_tracker                    := g_rec_in.loan_tracker;
   g_rec_out.sds_ref                         := g_rec_in.sds_ref;
   g_rec_out.test_digit                      := g_rec_in.test_digit;
   g_rec_out.test_digit_grp                  := g_rec_in.test_digit_grp;
   g_rec_out.debit_order_flag                := g_rec_in.debit_order_flag;
   g_rec_out.debit_order_dy                  := g_rec_in.debit_order_dy;
   g_rec_out.debit_order_due                 := g_rec_in.debit_order_due;
   g_rec_out.dtlst_accstat_chg               := g_rec_in.dtlst_accstat_chg;
   g_rec_out.lcp_ind                         := g_rec_in.lcp_ind;
   g_rec_out.companion_care_ind              := g_rec_in.companion_care_ind;
   g_rec_out.accident_benft_ind              := g_rec_in.accident_benft_ind;
   g_rec_out.cbp_ind                         := g_rec_in.cbp_ind;
   g_rec_out.lbp_ind                         := g_rec_in.lbp_ind;
   g_rec_out.ptp_status                      := g_rec_in.ptp_status;
   g_rec_out.date_cred_limit                 := g_rec_in.date_cred_limit;
   g_rec_out.comp_care_lst_prem              := g_rec_in.comp_care_lst_prem;
   g_rec_out.comp_care_eff_date              := g_rec_in.comp_care_eff_date;
   g_rec_out.acc_benft_lst_prem              := g_rec_in.acc_benft_lst_prem;
   g_rec_out.acc_benft_eff_date              := g_rec_in.acc_benft_eff_date;
   g_rec_out.overdue_amt                     := g_rec_in.overdue_amt;
   g_rec_out.min_payment                     := g_rec_in.min_payment;
   g_rec_out.payment_date                    := g_rec_in.payment_date;
   g_rec_out.account_contact_id              := g_rec_in.account_contact_id;
   g_rec_out.ttd_ind                         := g_rec_in.ttd_ind;
   g_rec_out.bureau_score                    := g_rec_in.bureau_score;
   g_rec_out.viking_code                     := g_rec_in.viking_code;
   g_rec_out.viking_date                     := g_rec_in.viking_date;
   g_rec_out.viking_amt                      := g_rec_in.viking_amt;
   g_rec_out.debit_order_proj_amt            := g_rec_in.debit_order_proj_amt;
   g_rec_out.debit_order_br_cd               := g_rec_in.debit_order_br_cd;
   g_rec_out.debit_order_exp_dt              := g_rec_in.debit_order_exp_dt;
   g_rec_out.debit_order_acc_type            := g_rec_in.debit_order_acc_type;
   g_rec_out.debit_order_acc_no              := g_rec_in.debit_order_acc_no;
   g_rec_out.debit_order_pymt_ind            := g_rec_in.debit_order_pymt_ind;
   g_rec_out.dd_status                       := g_rec_in.dd_status;
   g_rec_out.clim_review                     := g_rec_in.clim_review;
   g_rec_out.dd_load_amt                     := g_rec_in.dd_load_amt;
   g_rec_out.date_first_purch                := g_rec_in.date_first_purch;
   g_rec_out.insurance_active_ind            := g_rec_in.insurance_active_ind;
   g_rec_out.loan_restruct_ind               := g_rec_in.loan_restruct_ind;
   g_rec_out.loan_restruct_date              := g_rec_in.loan_restruct_date;
   g_rec_out.residence_id                    := g_rec_in.residence_id;
   g_rec_out.debit_order_reversal_count      := g_rec_in.debit_order_reversal_count;
   g_rec_out.debit_order_interim_pmt         := g_rec_in.debit_order_interim_pmt;
   g_rec_out.debit_order_remitt_method       := g_rec_in.debit_order_remitt_method;
   g_rec_out.staff_company_code              := g_rec_in.staff_company_code;
   g_rec_out.write_off_ind                   := g_rec_in.write_off_ind;
   g_rec_out.write_off_date                  := g_rec_in.write_off_date;
   g_rec_out.write_off_value                 := g_rec_in.write_off_value;
   g_rec_out.initiation_fee                  := g_rec_in.initiation_fee;
   g_rec_out.monthly_service_fee             := g_rec_in.monthly_service_fee;
   g_rec_out.initial_interest_rate           := g_rec_in.initial_interest_rate;
   g_rec_out.delivery_method                 := g_rec_in.delivery_method;
   g_rec_out.delivery_address                := g_rec_in.delivery_address;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.legal_status                    := g_rec_in.legal_status;
   g_rec_out.legal_status_date               := g_rec_in.legal_status_date;
   g_rec_out.first_placement_indicator       := g_rec_in.first_placement_indicator;
   g_rec_out.first_placement_date            := g_rec_in.first_placement_date;
   g_rec_out.second_placement_indicator      := g_rec_in.second_placement_indicator;
   g_rec_out.second_placement_date           := g_rec_in.second_placement_date;
   g_rec_out.third_placement_indicator       := g_rec_in.third_placement_indicator;
   g_rec_out.third_placement_date            := g_rec_in.third_placement_date;
   g_rec_out.month6_review_indicator         := g_rec_in.month6_review_indicator;
   g_rec_out.month6_review_date              := g_rec_in.month6_review_date;



--   if not dwh_valid.indicator_field(g_rec_out.active_ind) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
--     return;
--   end if;


   if not dwh_cust_valid.fnd_product_code(g_rec_out.product_code_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_c2_product_code_not_found;
     l_text          := dwh_cust_constants.vc_c2_product_code_not_found||g_rec_out.product_code_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_vsn_all_prod_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert /*+append parallel(allp,4)*/ into fnd_wfs_all_prod allp values a_tbl_insert(i);

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
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).wfs_account_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update fnd_wfs_all_prod
       set    identity_no                     = a_tbl_update(i).identity_no,
              account_status                  = a_tbl_update(i).account_status,
              application_no                  = a_tbl_update(i).application_no,
              application_score               = a_tbl_update(i).application_score,
              behaviour_score01               = a_tbl_update(i).behaviour_score01,
              behaviour_score02               = a_tbl_update(i).behaviour_score02,
              behaviour_score03               = a_tbl_update(i).behaviour_score03,
              behaviour_score04               = a_tbl_update(i).behaviour_score04,
              behaviour_score05               = a_tbl_update(i).behaviour_score05,
              behaviour_score06               = a_tbl_update(i).behaviour_score06,
              behaviour_score07               = a_tbl_update(i).behaviour_score07,
              behaviour_score08               = a_tbl_update(i).behaviour_score08,
              behaviour_score09               = a_tbl_update(i).behaviour_score09,
              behaviour_score10               = a_tbl_update(i).behaviour_score10,
              behaviour_score11               = a_tbl_update(i).behaviour_score11,
              behaviour_score12               = a_tbl_update(i).behaviour_score12,
              propensty_score01               = a_tbl_update(i).propensty_score01,
              propensty_score02               = a_tbl_update(i).propensty_score02,
              propensty_score03               = a_tbl_update(i).propensty_score03,
              propensty_score04               = a_tbl_update(i).propensty_score04,
              propensty_score05               = a_tbl_update(i).propensty_score05,
              propensty_score06               = a_tbl_update(i).propensty_score06,
              propensty_score07               = a_tbl_update(i).propensty_score07,
              propensty_score08               = a_tbl_update(i).propensty_score08,
              propensty_score09               = a_tbl_update(i).propensty_score09,
              propensty_score10               = a_tbl_update(i).propensty_score10,
              propensty_score11               = a_tbl_update(i).propensty_score11,
              propensty_score12               = a_tbl_update(i).propensty_score12,
              attrition_score01               = a_tbl_update(i).attrition_score01,
              attrition_score02               = a_tbl_update(i).attrition_score02,
              attrition_score03               = a_tbl_update(i).attrition_score03,
              attrition_score04               = a_tbl_update(i).attrition_score04,
              attrition_score05               = a_tbl_update(i).attrition_score05,
              attrition_score06               = a_tbl_update(i).attrition_score06,
              attrition_score07               = a_tbl_update(i).attrition_score07,
              attrition_score08               = a_tbl_update(i).attrition_score08,
              attrition_score09               = a_tbl_update(i).attrition_score09,
              attrition_score10               = a_tbl_update(i).attrition_score10,
              attrition_score11               = a_tbl_update(i).attrition_score11,
              attrition_score12               = a_tbl_update(i).attrition_score12,
              date_opened                     = a_tbl_update(i).date_opened,
              date_last_pchs                  = a_tbl_update(i).date_last_pchs,
              credit_limit                    = a_tbl_update(i).credit_limit,
              current_balance                 = a_tbl_update(i).current_balance,
              open_to_buy                     = a_tbl_update(i).open_to_buy,
              last_pchs_val                   = a_tbl_update(i).last_pchs_val,
              pchs_val_ytd                    = a_tbl_update(i).pchs_val_ytd,
              pchs_val_ltd                    = a_tbl_update(i).pchs_val_ltd,
              store_of_pref1                  = a_tbl_update(i).store_of_pref1,
              store_of_pref2                  = a_tbl_update(i).store_of_pref2,
              store_of_pref3                  = a_tbl_update(i).store_of_pref3,
              block_code1                     = a_tbl_update(i).block_code1,
              block_code2                     = a_tbl_update(i).block_code2,
              no_of_cards                     = a_tbl_update(i).no_of_cards,
              date_last_updated               = a_tbl_update(i).date_last_updated,
              chgoff_val                      = a_tbl_update(i).chgoff_val,
              chgoff_rsn1                     = a_tbl_update(i).chgoff_rsn1,
              chgoff_rsn2                     = a_tbl_update(i).chgoff_rsn2,
              chgoff_status                   = a_tbl_update(i).chgoff_status,
              times_in_coll                   = a_tbl_update(i).times_in_coll,
              date_application                = a_tbl_update(i).date_application,
              date_chgoff                     = a_tbl_update(i).date_chgoff,
              date_closed                     = a_tbl_update(i).date_closed,
              date_highest_bal                = a_tbl_update(i).date_highest_bal,
              date_last_activity              = a_tbl_update(i).date_last_activity,
              date_last_age                   = a_tbl_update(i).date_last_age,
              date_last_crlm                  = a_tbl_update(i).date_last_crlm,
              date_last_pymt                  = a_tbl_update(i).date_last_pymt,
              date_last_rate_chg              = a_tbl_update(i).date_last_rate_chg,
              date_last_reage                 = a_tbl_update(i).date_last_reage,
              date_last_reclass               = a_tbl_update(i).date_last_reclass,
              date_last_return                = a_tbl_update(i).date_last_return,
              date_last_revpmt                = a_tbl_update(i).date_last_revpmt,
              date_last_rtnchq                = a_tbl_update(i).date_last_rtnchq,
              date_last_stmt                  = a_tbl_update(i).date_last_stmt,
              days_till_chgoff                = a_tbl_update(i).days_till_chgoff,
              highest_bal_val                 = a_tbl_update(i).highest_bal_val,
              prev_credit_class               = a_tbl_update(i).prev_credit_class,
              prev_credit_limit               = a_tbl_update(i).prev_credit_limit,
              prev_int_val_ytd                = a_tbl_update(i).prev_int_val_ytd,
              prev_int_pd_ytd                 = a_tbl_update(i).prev_int_pd_ytd,
              market_flag_01                  = a_tbl_update(i).market_flag_01,
              market_flag_02                  = a_tbl_update(i).market_flag_02,
              market_flag_03                  = a_tbl_update(i).market_flag_03,
              market_flag_04                  = a_tbl_update(i).market_flag_04,
              market_flag_05                  = a_tbl_update(i).market_flag_05,
              market_flag_06                  = a_tbl_update(i).market_flag_06,
              market_flag_07                  = a_tbl_update(i).market_flag_07,
              market_flag_08                  = a_tbl_update(i).market_flag_08,
              market_flag_09                  = a_tbl_update(i).market_flag_09,
              market_flag_10                  = a_tbl_update(i).market_flag_10,
              market_flag_11                  = a_tbl_update(i).market_flag_11,
              market_flag_12                  = a_tbl_update(i).market_flag_12,
              market_flag_13                  = a_tbl_update(i).market_flag_13,
              market_flag_14                  = a_tbl_update(i).market_flag_14,
              market_flag_15                  = a_tbl_update(i).market_flag_15,
              market_flag_16                  = a_tbl_update(i).market_flag_16,
              market_flag_17                  = a_tbl_update(i).market_flag_17,
              market_flag_18                  = a_tbl_update(i).market_flag_18,
              market_flag_19                  = a_tbl_update(i).market_flag_19,
              market_flag_20                  = a_tbl_update(i).market_flag_20,
              last_pymt_val                   = a_tbl_update(i).last_pymt_val,
              promotion_code1                 = a_tbl_update(i).promotion_code1,
              promotion_code2                 = a_tbl_update(i).promotion_code2,
              promotion_code3                 = a_tbl_update(i).promotion_code3,
              promotion_code4                 = a_tbl_update(i).promotion_code4,
              promotion_status1               = a_tbl_update(i).promotion_status1,
              promotion_status2               = a_tbl_update(i).promotion_status2,
              promotion_status3               = a_tbl_update(i).promotion_status3,
              promotion_status4               = a_tbl_update(i).promotion_status4,
              retail_plan_code                = a_tbl_update(i).retail_plan_code,
              statmt_flag                     = a_tbl_update(i).statmt_flag,
              statmt_msg_no_1                 = a_tbl_update(i).statmt_msg_no_1,
              statmt_msg_no_2                 = a_tbl_update(i).statmt_msg_no_2,
              write_off_days                  = a_tbl_update(i).write_off_days,
              ins_incentv_store               = a_tbl_update(i).ins_incentv_store,
              ins_cancel_date                 = a_tbl_update(i).ins_cancel_date,
              ins_dt_lst_billed               = a_tbl_update(i).ins_dt_lst_billed,
              ins_dt_lst_claim                = a_tbl_update(i).ins_dt_lst_claim,
              ins_effectv_date                = a_tbl_update(i).ins_effectv_date,
              ins_enrllmnt_state              = a_tbl_update(i).ins_enrllmnt_state,
              ins_last_premium                = a_tbl_update(i).ins_last_premium,
              ins_premium_mtd                 = a_tbl_update(i).ins_premium_mtd,
              ins_premium                     = a_tbl_update(i).ins_premium,
              ins_premium_state               = a_tbl_update(i).ins_premium_state,
              ins_product                     = a_tbl_update(i).ins_product,
              ins_rsn_cancelled               = a_tbl_update(i).ins_rsn_cancelled,
              ins_reinstmt_date               = a_tbl_update(i).ins_reinstmt_date,
              ins_status                      = a_tbl_update(i).ins_status,
              plan_pmt_ovrd_flag              = a_tbl_update(i).plan_pmt_ovrd_flag,
              mktg_promo                      = a_tbl_update(i).mktg_promo,
              no_of_store_pref                = a_tbl_update(i).no_of_store_pref,
              return_mail_cnt                 = a_tbl_update(i).return_mail_cnt,
              loan_drawdown_val               = a_tbl_update(i).loan_drawdown_val,
              loan_instalment                 = a_tbl_update(i).loan_instalment,
              loan_repay_period               = a_tbl_update(i).loan_repay_period,
              loan_tracker                    = a_tbl_update(i).loan_tracker,
              sds_ref                         = a_tbl_update(i).sds_ref,
              test_digit                      = a_tbl_update(i).test_digit,
              test_digit_grp                  = a_tbl_update(i).test_digit_grp,
              debit_order_flag                = a_tbl_update(i).debit_order_flag,
              debit_order_dy                  = a_tbl_update(i).debit_order_dy,
              debit_order_due                 = a_tbl_update(i).debit_order_due,
              dtlst_accstat_chg               = a_tbl_update(i).dtlst_accstat_chg,
              lcp_ind                         = a_tbl_update(i).lcp_ind,
              companion_care_ind              = a_tbl_update(i).companion_care_ind,
              accident_benft_ind              = a_tbl_update(i).accident_benft_ind,
              cbp_ind                         = a_tbl_update(i).cbp_ind,
              lbp_ind                         = a_tbl_update(i).lbp_ind,
              ptp_status                      = a_tbl_update(i).ptp_status,
              date_cred_limit                 = a_tbl_update(i).date_cred_limit,
              comp_care_lst_prem              = a_tbl_update(i).comp_care_lst_prem,
              comp_care_eff_date              = a_tbl_update(i).comp_care_eff_date,
              acc_benft_lst_prem              = a_tbl_update(i).acc_benft_lst_prem,
              acc_benft_eff_date              = a_tbl_update(i).acc_benft_eff_date,
              overdue_amt                     = a_tbl_update(i).overdue_amt,
              min_payment                     = a_tbl_update(i).min_payment,
              payment_date                    = a_tbl_update(i).payment_date,
              account_contact_id              = a_tbl_update(i).account_contact_id,
              ttd_ind                         = a_tbl_update(i).ttd_ind,
              bureau_score                    = a_tbl_update(i).bureau_score,
              viking_code                     = a_tbl_update(i).viking_code,
              viking_date                     = a_tbl_update(i).viking_date,
              viking_amt                      = a_tbl_update(i).viking_amt,
              debit_order_proj_amt            = a_tbl_update(i).debit_order_proj_amt,
              debit_order_br_cd               = a_tbl_update(i).debit_order_br_cd,
              debit_order_exp_dt              = a_tbl_update(i).debit_order_exp_dt,
              debit_order_acc_type            = a_tbl_update(i).debit_order_acc_type,
              debit_order_acc_no              = a_tbl_update(i).debit_order_acc_no,
              debit_order_pymt_ind            = a_tbl_update(i).debit_order_pymt_ind,
              dd_status                       = a_tbl_update(i).dd_status,
              clim_review                     = a_tbl_update(i).clim_review,
              dd_load_amt                     = a_tbl_update(i).dd_load_amt,
              date_first_purch                = a_tbl_update(i).date_first_purch,
              insurance_active_ind            = a_tbl_update(i).insurance_active_ind,
              loan_restruct_ind               = a_tbl_update(i).loan_restruct_ind,
              loan_restruct_date              = a_tbl_update(i).loan_restruct_date,
              residence_id                    = a_tbl_update(i).residence_id,
              debit_order_reversal_count      = a_tbl_update(i).debit_order_reversal_count,
              debit_order_interim_pmt         = a_tbl_update(i).debit_order_interim_pmt,
              debit_order_remitt_method       = a_tbl_update(i).debit_order_remitt_method,
              staff_company_code              = a_tbl_update(i).staff_company_code,
              write_off_ind                   = a_tbl_update(i).write_off_ind,
              write_off_date                  = a_tbl_update(i).write_off_date,
              write_off_value                 = a_tbl_update(i).write_off_value,
              initiation_fee                  = a_tbl_update(i).initiation_fee,
              monthly_service_fee             = a_tbl_update(i).monthly_service_fee,
              initial_interest_rate           = a_tbl_update(i).initial_interest_rate,
              delivery_method                 = a_tbl_update(i).delivery_method,
              delivery_address                = a_tbl_update(i).delivery_address,
              legal_status                    = a_tbl_update(i).legal_status,
              legal_status_date               = a_tbl_update(i).legal_status_date,
              first_placement_indicator       = a_tbl_update(i).first_placement_indicator,
              first_placement_date            = a_tbl_update(i).first_placement_date,
              second_placement_indicator      = a_tbl_update(i).second_placement_indicator,
              second_placement_date           = a_tbl_update(i).second_placement_date,
              third_placement_indicator       = a_tbl_update(i).third_placement_indicator,
              third_placement_date            = a_tbl_update(i).third_placement_date,
              month6_review_indicator         = a_tbl_update(i).month6_review_indicator,
              month6_review_date              = a_tbl_update(i).month6_review_date,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  wfs_account_no                  = a_tbl_update(i).wfs_account_no and
              wfs_customer_no                 = a_tbl_update(i).wfs_customer_no and
              product_code_no                 = a_tbl_update(i).product_code_no and
              (
              nvl(identity_no                     ,0) <> a_tbl_update(i).identity_no or
              nvl(account_status                  ,0) <> a_tbl_update(i).account_status or
              nvl(application_no                  ,0) <> a_tbl_update(i).application_no or
              nvl(application_score               ,0) <> a_tbl_update(i).application_score or
              nvl(behaviour_score01               ,0) <> a_tbl_update(i).behaviour_score01 or
              nvl(behaviour_score02               ,0) <> a_tbl_update(i).behaviour_score02 or
              nvl(behaviour_score03               ,0) <> a_tbl_update(i).behaviour_score03 or
              nvl(behaviour_score04               ,0) <> a_tbl_update(i).behaviour_score04 or
              nvl(behaviour_score05               ,0) <> a_tbl_update(i).behaviour_score05 or
              nvl(behaviour_score06               ,0) <> a_tbl_update(i).behaviour_score06 or
              nvl(behaviour_score07               ,0) <> a_tbl_update(i).behaviour_score07 or
              nvl(behaviour_score08               ,0) <> a_tbl_update(i).behaviour_score08 or
              nvl(behaviour_score09               ,0) <> a_tbl_update(i).behaviour_score09 or
              nvl(behaviour_score10               ,0) <> a_tbl_update(i).behaviour_score10 or
              nvl(behaviour_score11               ,0) <> a_tbl_update(i).behaviour_score11 or
              nvl(behaviour_score12               ,0) <> a_tbl_update(i).behaviour_score12 or
              nvl(propensty_score01               ,0) <> a_tbl_update(i).propensty_score01 or
              nvl(propensty_score02               ,0) <> a_tbl_update(i).propensty_score02 or
              nvl(propensty_score03               ,0) <> a_tbl_update(i).propensty_score03 or
              nvl(propensty_score04               ,0) <> a_tbl_update(i).propensty_score04 or
              nvl(propensty_score05               ,0) <> a_tbl_update(i).propensty_score05 or
              nvl(propensty_score06               ,0) <> a_tbl_update(i).propensty_score06 or
              nvl(propensty_score07               ,0) <> a_tbl_update(i).propensty_score07 or
              nvl(propensty_score08               ,0) <> a_tbl_update(i).propensty_score08 or
              nvl(propensty_score09               ,0) <> a_tbl_update(i).propensty_score09 or
              nvl(propensty_score10               ,0) <> a_tbl_update(i).propensty_score10 or
              nvl(propensty_score11               ,0) <> a_tbl_update(i).propensty_score11 or
              nvl(propensty_score12               ,0) <> a_tbl_update(i).propensty_score12 or
              nvl(attrition_score01               ,0) <> a_tbl_update(i).attrition_score01 or
              nvl(attrition_score02               ,0) <> a_tbl_update(i).attrition_score02 or
              nvl(attrition_score03               ,0) <> a_tbl_update(i).attrition_score03 or
              nvl(attrition_score04               ,0) <> a_tbl_update(i).attrition_score04 or
              nvl(attrition_score05               ,0) <> a_tbl_update(i).attrition_score05 or
              nvl(attrition_score06               ,0) <> a_tbl_update(i).attrition_score06 or
              nvl(attrition_score07               ,0) <> a_tbl_update(i).attrition_score07 or
              nvl(attrition_score08               ,0) <> a_tbl_update(i).attrition_score08 or
              nvl(attrition_score09               ,0) <> a_tbl_update(i).attrition_score09 or
              nvl(attrition_score10               ,0) <> a_tbl_update(i).attrition_score10 or
              nvl(attrition_score11               ,0) <> a_tbl_update(i).attrition_score11 or
              nvl(attrition_score12               ,0) <> a_tbl_update(i).attrition_score12 or
              nvl(date_opened                     ,'1 Jan 1900') <> a_tbl_update(i).date_opened or
              nvl(date_last_pchs                  ,'1 Jan 1900') <> a_tbl_update(i).date_last_pchs or
              nvl(credit_limit                    ,0) <> a_tbl_update(i).credit_limit or
              nvl(current_balance                 ,0) <> a_tbl_update(i).current_balance or
              nvl(open_to_buy                     ,0) <> a_tbl_update(i).open_to_buy or
              nvl(last_pchs_val                   ,0) <> a_tbl_update(i).last_pchs_val or
              nvl(pchs_val_ytd                    ,0) <> a_tbl_update(i).pchs_val_ytd or
              nvl(pchs_val_ltd                    ,0) <> a_tbl_update(i).pchs_val_ltd or
              nvl(store_of_pref1                  ,0) <> a_tbl_update(i).store_of_pref1 or
              nvl(store_of_pref2                  ,0) <> a_tbl_update(i).store_of_pref2 or
              nvl(store_of_pref3                  ,0) <> a_tbl_update(i).store_of_pref3 or
              nvl(block_code1                     ,0) <> a_tbl_update(i).block_code1 or
              nvl(block_code2                     ,0) <> a_tbl_update(i).block_code2 or
              nvl(no_of_cards                     ,0) <> a_tbl_update(i).no_of_cards or
              nvl(date_last_updated               ,'1 Jan 1900') <> a_tbl_update(i).date_last_updated or
              nvl(chgoff_val                      ,0) <> a_tbl_update(i).chgoff_val or
              nvl(chgoff_rsn1                     ,0) <> a_tbl_update(i).chgoff_rsn1 or
              nvl(chgoff_rsn2                     ,0) <> a_tbl_update(i).chgoff_rsn2 or
              nvl(chgoff_status                   ,0) <> a_tbl_update(i).chgoff_status or
              nvl(times_in_coll                   ,0) <> a_tbl_update(i).times_in_coll or
              nvl(date_application                ,'1 Jan 1900') <> a_tbl_update(i).date_application or
              nvl(date_chgoff                     ,'1 Jan 1900') <> a_tbl_update(i).date_chgoff or
              nvl(date_closed                     ,'1 Jan 1900') <> a_tbl_update(i).date_closed or
              nvl(date_highest_bal                ,'1 Jan 1900') <> a_tbl_update(i).date_highest_bal or
              nvl(date_last_activity              ,'1 Jan 1900') <> a_tbl_update(i).date_last_activity or
              nvl(date_last_age                   ,'1 Jan 1900') <> a_tbl_update(i).date_last_age or
              nvl(date_last_crlm                  ,'1 Jan 1900') <> a_tbl_update(i).date_last_crlm or
              nvl(date_last_pymt                  ,'1 Jan 1900') <> a_tbl_update(i).date_last_pymt or
              nvl(date_last_rate_chg              ,'1 Jan 1900') <> a_tbl_update(i).date_last_rate_chg or
              nvl(date_last_reage                 ,'1 Jan 1900') <> a_tbl_update(i).date_last_reage or
              nvl(date_last_reclass               ,'1 Jan 1900') <> a_tbl_update(i).date_last_reclass or
              nvl(date_last_return                ,'1 Jan 1900') <> a_tbl_update(i).date_last_return or
              nvl(date_last_revpmt                ,'1 Jan 1900') <> a_tbl_update(i).date_last_revpmt or
              nvl(date_last_rtnchq                ,'1 Jan 1900') <> a_tbl_update(i).date_last_rtnchq or
              nvl(date_last_stmt                  ,'1 Jan 1900') <> a_tbl_update(i).date_last_stmt or
              nvl(days_till_chgoff                ,0) <> a_tbl_update(i).days_till_chgoff or
              nvl(highest_bal_val                 ,0) <> a_tbl_update(i).highest_bal_val or
              nvl(prev_credit_class               ,0) <> a_tbl_update(i).prev_credit_class or
              nvl(prev_credit_limit               ,0) <> a_tbl_update(i).prev_credit_limit or
              nvl(prev_int_val_ytd                ,0) <> a_tbl_update(i).prev_int_val_ytd or
              nvl(prev_int_pd_ytd                 ,0) <> a_tbl_update(i).prev_int_pd_ytd or
              nvl(market_flag_01                  ,0) <> a_tbl_update(i).market_flag_01 or
              nvl(market_flag_02                  ,0) <> a_tbl_update(i).market_flag_02 or
              nvl(market_flag_03                  ,0) <> a_tbl_update(i).market_flag_03 or
              nvl(market_flag_04                  ,0) <> a_tbl_update(i).market_flag_04 or
              nvl(market_flag_05                  ,0) <> a_tbl_update(i).market_flag_05 or
              nvl(market_flag_06                  ,0) <> a_tbl_update(i).market_flag_06 or
              nvl(market_flag_07                  ,0) <> a_tbl_update(i).market_flag_07 or
              nvl(market_flag_08                  ,0) <> a_tbl_update(i).market_flag_08 or
              nvl(market_flag_09                  ,0) <> a_tbl_update(i).market_flag_09 or
              nvl(market_flag_10                  ,0) <> a_tbl_update(i).market_flag_10 or
              nvl(market_flag_11                  ,0) <> a_tbl_update(i).market_flag_11 or
              nvl(market_flag_12                  ,0) <> a_tbl_update(i).market_flag_12 or
              nvl(market_flag_13                  ,0) <> a_tbl_update(i).market_flag_13 or
              nvl(market_flag_14                  ,0) <> a_tbl_update(i).market_flag_14 or
              nvl(market_flag_15                  ,0) <> a_tbl_update(i).market_flag_15 or
              nvl(market_flag_16                  ,0) <> a_tbl_update(i).market_flag_16 or
              nvl(market_flag_17                  ,0) <> a_tbl_update(i).market_flag_17 or
              nvl(market_flag_18                  ,0) <> a_tbl_update(i).market_flag_18 or
              nvl(market_flag_19                  ,0) <> a_tbl_update(i).market_flag_19 or
              nvl(market_flag_20                  ,0) <> a_tbl_update(i).market_flag_20 or
              nvl(last_pymt_val                   ,0) <> a_tbl_update(i).last_pymt_val or
              nvl(promotion_code1                 ,0) <> a_tbl_update(i).promotion_code1 or
              nvl(promotion_code2                 ,0) <> a_tbl_update(i).promotion_code2 or
              nvl(promotion_code3                 ,0) <> a_tbl_update(i).promotion_code3 or
              nvl(promotion_code4                 ,0) <> a_tbl_update(i).promotion_code4 or
              nvl(promotion_status1               ,0) <> a_tbl_update(i).promotion_status1 or
              nvl(promotion_status2               ,0) <> a_tbl_update(i).promotion_status2 or
              nvl(promotion_status3               ,0) <> a_tbl_update(i).promotion_status3 or
              nvl(promotion_status4               ,0) <> a_tbl_update(i).promotion_status4 or
              nvl(retail_plan_code                ,0) <> a_tbl_update(i).retail_plan_code or
              nvl(statmt_flag                     ,0) <> a_tbl_update(i).statmt_flag or
              nvl(statmt_msg_no_1                 ,0) <> a_tbl_update(i).statmt_msg_no_1 or
              nvl(statmt_msg_no_2                 ,0) <> a_tbl_update(i).statmt_msg_no_2 or
              nvl(write_off_days                  ,0) <> a_tbl_update(i).write_off_days or
              nvl(ins_incentv_store               ,0) <> a_tbl_update(i).ins_incentv_store or
              nvl(ins_cancel_date                 ,'1 Jan 1900') <> a_tbl_update(i).ins_cancel_date or
              nvl(ins_dt_lst_billed               ,'1 Jan 1900') <> a_tbl_update(i).ins_dt_lst_billed or
              nvl(ins_dt_lst_claim                ,'1 Jan 1900') <> a_tbl_update(i).ins_dt_lst_claim or
              nvl(ins_effectv_date                ,'1 Jan 1900') <> a_tbl_update(i).ins_effectv_date or
              nvl(ins_enrllmnt_state              ,0) <> a_tbl_update(i).ins_enrllmnt_state or
              nvl(ins_last_premium                ,0) <> a_tbl_update(i).ins_last_premium or
              nvl(ins_premium_mtd                 ,0) <> a_tbl_update(i).ins_premium_mtd or
              nvl(ins_premium                     ,0) <> a_tbl_update(i).ins_premium or
              nvl(ins_premium_state               ,0) <> a_tbl_update(i).ins_premium_state or
              nvl(ins_product                     ,0) <> a_tbl_update(i).ins_product or
              nvl(ins_rsn_cancelled               ,0) <> a_tbl_update(i).ins_rsn_cancelled or
              nvl(ins_reinstmt_date               ,'1 Jan 1900') <> a_tbl_update(i).ins_reinstmt_date or
              nvl(ins_status                      ,0) <> a_tbl_update(i).ins_status or
              nvl(plan_pmt_ovrd_flag              ,0) <> a_tbl_update(i).plan_pmt_ovrd_flag or
              nvl(mktg_promo                      ,0) <> a_tbl_update(i).mktg_promo or
              nvl(no_of_store_pref                ,0) <> a_tbl_update(i).no_of_store_pref or
              nvl(return_mail_cnt                 ,0) <> a_tbl_update(i).return_mail_cnt or
              nvl(loan_drawdown_val               ,0) <> a_tbl_update(i).loan_drawdown_val or
              nvl(loan_instalment                 ,0) <> a_tbl_update(i).loan_instalment or
              nvl(loan_repay_period               ,0) <> a_tbl_update(i).loan_repay_period or
              nvl(loan_tracker                    ,0) <> a_tbl_update(i).loan_tracker or
              nvl(sds_ref                         ,0) <> a_tbl_update(i).sds_ref or
              nvl(test_digit                      ,0) <> a_tbl_update(i).test_digit or
              nvl(test_digit_grp                  ,0) <> a_tbl_update(i).test_digit_grp or
              nvl(debit_order_flag                ,0) <> a_tbl_update(i).debit_order_flag or
              nvl(debit_order_dy                  ,0) <> a_tbl_update(i).debit_order_dy or
              nvl(debit_order_due                 ,0) <> a_tbl_update(i).debit_order_due or
              nvl(dtlst_accstat_chg               ,'1 Jan 1900') <> a_tbl_update(i).dtlst_accstat_chg or
              nvl(lcp_ind                         ,0) <> a_tbl_update(i).lcp_ind or
              nvl(companion_care_ind              ,0) <> a_tbl_update(i).companion_care_ind or
              nvl(accident_benft_ind              ,0) <> a_tbl_update(i).accident_benft_ind or
              nvl(cbp_ind                         ,0) <> a_tbl_update(i).cbp_ind or
              nvl(lbp_ind                         ,0) <> a_tbl_update(i).lbp_ind or
              nvl(ptp_status                      ,0) <> a_tbl_update(i).ptp_status or
              nvl(date_cred_limit                 ,'1 Jan 1900') <> a_tbl_update(i).date_cred_limit or
              nvl(comp_care_lst_prem              ,0) <> a_tbl_update(i).comp_care_lst_prem or
              nvl(comp_care_eff_date              ,'1 Jan 1900') <> a_tbl_update(i).comp_care_eff_date or
              nvl(acc_benft_lst_prem              ,0) <> a_tbl_update(i).acc_benft_lst_prem or
              nvl(acc_benft_eff_date              ,'1 Jan 1900') <> a_tbl_update(i).acc_benft_eff_date or
              nvl(overdue_amt                     ,0) <> a_tbl_update(i).overdue_amt or
              nvl(min_payment                     ,0) <> a_tbl_update(i).min_payment or
              nvl(payment_date                    ,'1 Jan 1900') <> a_tbl_update(i).payment_date or
              nvl(account_contact_id              ,0) <> a_tbl_update(i).account_contact_id or
              nvl(ttd_ind                         ,0) <> a_tbl_update(i).ttd_ind or
              nvl(bureau_score                    ,0) <> a_tbl_update(i).bureau_score or
              nvl(viking_code                     ,0) <> a_tbl_update(i).viking_code or
              nvl(viking_date                     ,'1 Jan 1900') <> a_tbl_update(i).viking_date or
              nvl(viking_amt                      ,0) <> a_tbl_update(i).viking_amt or
              nvl(debit_order_proj_amt            ,0) <> a_tbl_update(i).debit_order_proj_amt or
              nvl(debit_order_br_cd               ,0) <> a_tbl_update(i).debit_order_br_cd or
              nvl(debit_order_exp_dt              ,'1 Jan 1900') <> a_tbl_update(i).debit_order_exp_dt or
              nvl(debit_order_acc_type            ,0) <> a_tbl_update(i).debit_order_acc_type or
              nvl(debit_order_acc_no              ,0) <> a_tbl_update(i).debit_order_acc_no or
              nvl(debit_order_pymt_ind            ,0) <> a_tbl_update(i).debit_order_pymt_ind or
              nvl(dd_status                       ,0) <> a_tbl_update(i).dd_status or
              nvl(clim_review                     ,0) <> a_tbl_update(i).clim_review or
              nvl(dd_load_amt                     ,0) <> a_tbl_update(i).dd_load_amt or
              nvl(date_first_purch                ,'1 Jan 1900') <> a_tbl_update(i).date_first_purch or
              nvl(insurance_active_ind            ,0) <> a_tbl_update(i).insurance_active_ind or
              nvl(loan_restruct_ind               ,0) <> a_tbl_update(i).loan_restruct_ind or
              nvl(loan_restruct_date              ,'1 Jan 1900') <> a_tbl_update(i).loan_restruct_date or
              nvl(residence_id                    ,0) <> a_tbl_update(i).residence_id or
              nvl(debit_order_reversal_count      ,0) <> a_tbl_update(i).debit_order_reversal_count or
              nvl(debit_order_interim_pmt         ,0) <> a_tbl_update(i).debit_order_interim_pmt or
              nvl(debit_order_remitt_method       ,0) <> a_tbl_update(i).debit_order_remitt_method or
              nvl(staff_company_code              ,0) <> a_tbl_update(i).staff_company_code or
              nvl(write_off_ind                   ,0) <> a_tbl_update(i).write_off_ind or
              nvl(write_off_date                  ,'1 Jan 1900') <> a_tbl_update(i).write_off_date or
              nvl(write_off_value                 ,0) <> a_tbl_update(i).write_off_value or
              nvl(initiation_fee                  ,0) <> a_tbl_update(i).initiation_fee or
              nvl(monthly_service_fee             ,0) <> a_tbl_update(i).monthly_service_fee or
              nvl(initial_interest_rate           ,0) <> a_tbl_update(i).initial_interest_rate or
              nvl(delivery_method                 ,0) <> a_tbl_update(i).delivery_method or
              nvl(delivery_address                ,0) <> a_tbl_update(i).delivery_address or
              nvl(LEGAL_STATUS                    ,0) <> a_tbl_update(i).LEGAL_STATUS or
              nvl(LEGAL_STATUS_DATE               ,'1 Jan 1900') <> a_tbl_update(i).LEGAL_STATUS_DATE or
              nvl(FIRST_PLACEMENT_INDICATOR       ,0) <> a_tbl_update(i).FIRST_PLACEMENT_INDICATOR or
              nvl(FIRST_PLACEMENT_DATE            ,'1 Jan 1900') <> a_tbl_update(i).FIRST_PLACEMENT_DATE or
              nvl(SECOND_PLACEMENT_INDICATOR      ,0) <> a_tbl_update(i).SECOND_PLACEMENT_INDICATOR or
              nvl(SECOND_PLACEMENT_DATE           ,'1 Jan 1900') <> a_tbl_update(i).SECOND_PLACEMENT_DATE or
              nvl(THIRD_PLACEMENT_INDICATOR       ,0) <> a_tbl_update(i).THIRD_PLACEMENT_INDICATOR or
              nvl(THIRD_PLACEMENT_DATE            ,'1 Jan 1900') <> a_tbl_update(i).THIRD_PLACEMENT_DATE or
              nvl(MONTH6_REVIEW_INDICATOR         ,0) <> a_tbl_update(i).MONTH6_REVIEW_INDICATOR or
              nvl(MONTH6_REVIEW_DATE              ,'1 Jan 1900') <> a_tbl_update(i).MONTH6_REVIEW_DATE
              );

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).wfs_account_no ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_vsn_all_prod_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_cust_valid.fnd_wfs_all_prod (g_rec_out.wfs_account_no,g_rec_out.wfs_customer_no,g_rec_out.product_code_no);
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).wfs_account_no   = g_rec_out.wfs_account_no and
            a_tbl_insert(i).wfs_customer_no  = g_rec_out.wfs_customer_no and
            a_tbl_insert(i).product_code_no  = g_rec_out.product_code_no then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
 --     local_bulk_staging_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;

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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    
    g_forall_limit := 100;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_wfs_all_prod EX CUST CENTRAL STARTED AT '||
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


    if TO_CHAR(sysdate, 'DAY') = 'SUNDAY' or
       TO_CHAR(sysdate, 'DAY') = 'MONDAY' then

      l_text := 'Purge data on the allprod master on Weekend:- '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      execute immediate 'truncate table fnd_wfs_all_prod';
    end if;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_vsn_all_prod;
    fetch c_stg_vsn_all_prod bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_stg_vsn_all_prod bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_vsn_all_prod;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
   -- local_bulk_staging_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_run_completed ||sysdate;
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

END WH_FND_CUST_014OLD;
