--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_256U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_256U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Apr 2012
--  Author:      Alastair de Wet
--  Purpose:     Generate the ed beneficiary dimention  sk type 2 load program
--  Tables:      Input  - dim_hr_bee_ed_beneficiary
--               Output - dim_hr_bee_ed_beneficiary_hist
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    : Add new columns to dim_hr_bee_ed_beneficiary
--                    no_of_jobs_created
--                    ed_sd_indicator
--                    effective_date
--                    enterprise_beneficiary_status
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
g_recs_read         integer       :=  0;
g_recs_inserted     integer       :=  0;
g_recs_updated      integer       :=  0;

g_rec_out         dim_hr_bee_ed_beneficiary_hist%rowtype;
g_count             integer       :=  0;
g_found             boolean;
g_insert_rec        boolean;
g_date              date          := trunc(sysdate);
g_this_mn_start_date date          ;
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_256U';
l_name              sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'CREATE dim_hr_bee_ed_beneficiary_hist EX bee_supplier mast';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;

cursor c_dim_hr_bee_ed_beneficiary is
   select  *
   from   dim_hr_bee_ed_beneficiary  ;

g_rec_in            c_dim_hr_bee_ed_beneficiary%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.ed_beneficiary_id               := g_rec_in.ed_beneficiary_id;
   g_rec_out.vendor_no                       := g_rec_in.vendor_no;
   g_rec_out.ed_beneficiary                  := g_rec_in.ed_beneficiary;
   g_rec_out.loan_account_no                 := g_rec_in.loan_account_no;
   g_rec_out.bee_business_unit_code          := g_rec_in.bee_business_unit_code;
   g_rec_out.bee_business_unit_desc          := g_rec_in.bee_business_unit_desc;
   g_rec_out.enterprise_size                 := g_rec_in.enterprise_size;
   g_rec_out.black_ownership_perc            := g_rec_in.black_ownership_perc;
   g_rec_out.black_female_ownership_perc     := g_rec_in.black_female_ownership_perc;
   g_rec_out.bee_status                      := g_rec_in.bee_status;
   g_rec_out.grant_contribution_ind          := g_rec_in.grant_contribution_ind;
   g_rec_out.direct_cost_ind                 := g_rec_in.direct_cost_ind;
   g_rec_out.overhead_cost_ind               := g_rec_in.overhead_cost_ind;
   g_rec_out.payment_terms_ind               := g_rec_in.payment_terms_ind;
   g_rec_out.settlement_discount_ind         := g_rec_in.settlement_discount_ind;
   g_rec_out.std_loans_black_owned_ind       := g_rec_in.std_loans_black_owned_ind;
   g_rec_out.standard_loan_other_ind         := g_rec_in.standard_loan_other_ind;
   g_rec_out.interest_fee_loan_ind           := g_rec_in.interest_fee_loan_ind;
   g_rec_out.guarantees_ind                  := g_rec_in.guarantees_ind;
   g_rec_out.lower_interest_ind              := g_rec_in.lower_interest_ind;
   g_rec_out.time_with_ww_empl_ind           := g_rec_in.time_with_ww_empl_ind;
   g_rec_out.ethnicity_ownership             := g_rec_in.ethnicity_ownership;
   g_rec_out.letter_of_thanks_received_ind   := g_rec_in.letter_of_thanks_received_ind;
   g_rec_out.bee_certificate_expiry_date     := g_rec_in.bee_certificate_expiry_date;
   g_rec_out.valid_certificate_ind           := g_rec_in.valid_certificate_ind;
   g_rec_out.type_of_agreement_required      := g_rec_in.type_of_agreement_required;
   g_rec_out.moa_signed_ind                  := g_rec_in.moa_signed_ind;
   g_rec_out.aod_signed_ind                  := g_rec_in.aod_signed_ind;
   g_rec_out.agreement_in_place_ind          := g_rec_in.agreement_in_place_ind;
   g_rec_out.inception_date                  := g_rec_in.inception_date;
   g_rec_out.agreement_expiry_date           := g_rec_in.agreement_expiry_date;
   g_rec_out.ed_category                     := g_rec_in.ed_category;
   g_rec_out.customer_id                     := g_rec_in.customer_id;
   g_rec_out.of_ed_beneficiary_id            := g_rec_in.of_ed_beneficiary_id;
   g_rec_out.ed_beneficiary_no               := g_rec_in.ed_beneficiary_no;
   g_rec_out.customer_name                   := g_rec_in.customer_name;
   g_rec_out.business_type                   := g_rec_in.business_type;
   g_rec_out.category_code                   := g_rec_in.category_code;
   g_rec_out.address1                        := g_rec_in.address1;
   g_rec_out.address2                        := g_rec_in.address2;
   g_rec_out.address3                        := g_rec_in.address3;
   g_rec_out.city                            := g_rec_in.city;
   g_rec_out.state                           := g_rec_in.state;
   g_rec_out.postal_code                     := g_rec_in.postal_code;
   g_rec_out.country_code                    := g_rec_in.country_code;
   g_rec_out.status                          := g_rec_in.status;
   g_rec_out.sk1_company_code                := g_rec_in.sk1_company_code;
   g_rec_out.company_code                    := g_rec_in.company_code;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.no_of_jobs_created              := g_rec_in.no_of_jobs_created;
   g_rec_out.ed_sd_indicator                 := g_rec_in.ed_sd_indicator;
   g_rec_out.effective_date                  := g_rec_in.effective_date;
   g_rec_out.enterprise_beneficiary_status   := g_rec_in.enterprise_beneficiary_status;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_rec_out.ed_beneficiary_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Write valid data out to the supplier master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
    from   dim_hr_bee_ed_beneficiary_hist
    where  ed_beneficiary_id = g_rec_out.ed_beneficiary_id and
           sk2_active_to_date = dwh_constants.sk_to_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   dwh_hr_valid.did_beneficiary_change
   (g_rec_out.ed_beneficiary_id,g_this_mn_start_date,g_rec_out.payment_terms_ind,
    g_rec_out.settlement_discount_ind,g_rec_out.std_loans_black_owned_ind,
    g_rec_out.standard_loan_other_ind,g_rec_out.ed_category,g_rec_out.ed_sd_indicator,g_insert_rec);

   if g_insert_rec then
      g_rec_out.sk2_ed_beneficiary_id := hr_seq.nextval;
      g_rec_out.sk2_active_from_date  := g_this_mn_start_date;
      g_rec_out.sk2_active_to_date    := dwh_constants.sk_to_date;
      g_rec_out.sk2_latest_record_ind := 1;
      if not g_found then
         g_rec_out.sk2_active_from_date   := '1 Jan 2005';
      end if;
      insert into dim_hr_bee_ed_beneficiary_hist values g_rec_out;
      g_recs_inserted                 := g_recs_inserted + sql%rowcount;
   else
      update dim_hr_bee_ed_beneficiary_hist
      set     ed_beneficiary_id               = g_rec_out.ed_beneficiary_id,
              vendor_no                       = g_rec_out.vendor_no,
              ed_beneficiary                  = g_rec_out.ed_beneficiary,
              loan_account_no                 = g_rec_out.loan_account_no,
              bee_business_unit_code          = g_rec_out.bee_business_unit_code,
              bee_business_unit_desc          = g_rec_out.bee_business_unit_desc,
              enterprise_size                 = g_rec_out.enterprise_size,
              black_ownership_perc            = g_rec_out.black_ownership_perc,
              black_female_ownership_perc     = g_rec_out.black_female_ownership_perc,
              bee_status                      = g_rec_out.bee_status,
              grant_contribution_ind          = g_rec_out.grant_contribution_ind,
              direct_cost_ind                 = g_rec_out.direct_cost_ind,
              overhead_cost_ind               = g_rec_out.overhead_cost_ind,
              payment_terms_ind               = g_rec_out.payment_terms_ind,
              settlement_discount_ind         = g_rec_out.settlement_discount_ind,
              std_loans_black_owned_ind       = g_rec_out.std_loans_black_owned_ind,
              standard_loan_other_ind         = g_rec_out.standard_loan_other_ind,
              interest_fee_loan_ind           = g_rec_out.interest_fee_loan_ind,
              guarantees_ind                  = g_rec_out.guarantees_ind,
              lower_interest_ind              = g_rec_out.lower_interest_ind,
              time_with_ww_empl_ind           = g_rec_out.time_with_ww_empl_ind,
              ethnicity_ownership             = g_rec_out.ethnicity_ownership,
              letter_of_thanks_received_ind   = g_rec_out.letter_of_thanks_received_ind,
              bee_certificate_expiry_date     = g_rec_out.bee_certificate_expiry_date,
              valid_certificate_ind           = g_rec_out.valid_certificate_ind,
              type_of_agreement_required      = g_rec_out.type_of_agreement_required,
              moa_signed_ind                  = g_rec_out.moa_signed_ind,
              aod_signed_ind                  = g_rec_out.aod_signed_ind,
              agreement_in_place_ind          = g_rec_out.agreement_in_place_ind,
              inception_date                  = g_rec_out.inception_date,
              agreement_expiry_date           = g_rec_out.agreement_expiry_date,
              ed_category                     = g_rec_out.ed_category,
              customer_id                     = g_rec_out.customer_id,
              of_ed_beneficiary_id            = g_rec_out.of_ed_beneficiary_id,
              ed_beneficiary_no               = g_rec_out.ed_beneficiary_no,
              customer_name                   = g_rec_out.customer_name,
              business_type                   = g_rec_out.business_type,
              category_code                   = g_rec_out.category_code,
              address1                        = g_rec_out.address1,
              address2                        = g_rec_out.address2,
              address3                        = g_rec_out.address3,
              city                            = g_rec_out.city,
              state                           = g_rec_out.state,
              postal_code                     = g_rec_out.postal_code,
              country_code                    = g_rec_out.country_code,
              status                          = g_rec_out.status,
              sk1_company_code                = g_rec_out.sk1_company_code,
              company_code                    = g_rec_out.company_code,
               no_of_jobs_created             = g_rec_out.no_of_jobs_created,
              ed_sd_indicator                 = g_rec_out.ed_sd_indicator,
              effective_date                  = g_rec_out.effective_date,
              enterprise_beneficiary_status   = g_rec_out.enterprise_beneficiary_status,
              last_updated_date               = g_date
      where   ed_beneficiary_id               = g_rec_out.ed_beneficiary_id and
              sk2_active_to_date              = dwh_constants.sk_to_date;

      g_recs_updated              := g_recs_updated + sql%rowcount;
   end if;



  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' '||g_rec_out.ed_beneficiary_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_out.ed_beneficiary_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF supplier MASTER SK2 VERSION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

   select this_mn_start_date
   into   g_this_mn_start_date
   from   dim_calendar
   where  calendar_date = g_date - 15;

--**************************************************************************************************
    for v_dim_hr_bee_ed_beneficiary in c_dim_hr_bee_ed_beneficiary
    loop
      g_recs_read := g_recs_read + 1;

      if g_recs_read mod 10000 = 0 then
         l_text := dwh_constants.vc_log_records_processed||
         to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      g_rec_in := v_dim_hr_bee_ed_beneficiary;
      local_address_variable;
      local_write_output;

    end loop;

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_out.ed_beneficiary_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_prf_hr_256u;
