--------------------------------------------------------
--  DDL for Procedure WH_FND_HR_204U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_FND_HR_204U" (p_forall_limit in integer,p_success out boolean)
AS
--**************************************************************************************************
--  Date:        DEC. 2011
--  Author:      ALASTAIR DE WET
--  Purpose:     Create HR ed beneficiary fact table in the foundation layer
--               with input ex staging table from Oracle Financials.
--  Tables:      Input  - stg_excel_ed_beneficiary_cpy
--               Output - fnd_hr_excel_ed_beneficiary
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    : Add new columns to fnd_hr_excel_ed_beneficiary
--                    no_of_jobs_created
--                    ed_sd_indicator
--                    effective_date
--                    enterprise_beneficiary_status

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
g_hospital_text      stg_excel_ed_beneficiary_hsp.sys_process_msg%type;
g_rec_out            fnd_hr_excel_ed_beneficiary%rowtype;
g_rec_in             stg_excel_ed_beneficiary_cpy%rowtype;
g_found              boolean;
g_count              number;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_HR_204U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE HR BEE ED BENEFICIARY DATA EX EXCEL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_excel_ed_beneficiary_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_hr_excel_ed_beneficiary%rowtype index by binary_integer;
type tbl_array_u is table of fnd_hr_excel_ed_beneficiary%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_excel_ed_beneficiary_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_excel_ed_beneficiary_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_excel_ed_beneficiary is
   select *
   from stg_excel_ed_beneficiary_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
PROCEDURE local_address_variables
AS
BEGIN
   g_hospital           := 'N';

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
   g_rec_out.no_of_jobs_created              := g_rec_in.no_of_jobs_created;
   g_rec_out.ed_sd_indicator                 := g_rec_in.ed_sd_indicator;
   g_rec_out.effective_date                  := g_rec_in.effective_date;
   g_rec_out.enterprise_beneficiary_status   := g_rec_in.enterprise_beneficiary_status;


   g_rec_out.last_updated_date               := g_date;

   if not dwh_hr_valid.fnd_hr_bee_business_unit (g_rec_out.bee_business_unit_code ) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_bee_bu;
     return;
   end if;
   if not dwh_valid.indicator_field(g_rec_out.grant_contribution_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.direct_cost_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.overhead_cost_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.payment_terms_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.settlement_discount_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.std_loans_black_owned_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.standard_loan_other_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.interest_fee_loan_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.guarantees_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.lower_interest_ind ) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.time_with_ww_empl_ind ) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.letter_of_thanks_received_ind ) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.valid_certificate_ind ) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.moa_signed_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.aod_signed_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.agreement_in_place_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;




   exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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

   insert into stg_excel_ed_beneficiary_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
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
       insert into fnd_hr_excel_ed_beneficiary values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).ed_beneficiary_id ;
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
       update fnd_hr_excel_ed_beneficiary
       set    vendor_no                       = a_tbl_update(i).vendor_no,
              ed_beneficiary                  = a_tbl_update(i).ed_beneficiary,
              loan_account_no                 = a_tbl_update(i).loan_account_no,
              bee_business_unit_code          = a_tbl_update(i).bee_business_unit_code,
              bee_business_unit_desc          = a_tbl_update(i).bee_business_unit_desc,
              enterprise_size                 = a_tbl_update(i).enterprise_size,
              black_ownership_perc            = a_tbl_update(i).black_ownership_perc,
              black_female_ownership_perc     = a_tbl_update(i).black_female_ownership_perc,
              bee_status                      = a_tbl_update(i).bee_status,
              grant_contribution_ind          = a_tbl_update(i).grant_contribution_ind,
              direct_cost_ind                 = a_tbl_update(i).direct_cost_ind,
              overhead_cost_ind               = a_tbl_update(i).overhead_cost_ind,
              payment_terms_ind               = a_tbl_update(i).payment_terms_ind,
              settlement_discount_ind         = a_tbl_update(i).settlement_discount_ind,
              std_loans_black_owned_ind       = a_tbl_update(i).std_loans_black_owned_ind,
              standard_loan_other_ind         = a_tbl_update(i).standard_loan_other_ind,
              interest_fee_loan_ind           = a_tbl_update(i).interest_fee_loan_ind,
              guarantees_ind                  = a_tbl_update(i).guarantees_ind,
              lower_interest_ind              = a_tbl_update(i).lower_interest_ind,
              time_with_ww_empl_ind           = a_tbl_update(i).time_with_ww_empl_ind,
              ethnicity_ownership             = a_tbl_update(i).ethnicity_ownership,
              letter_of_thanks_received_ind   = a_tbl_update(i).letter_of_thanks_received_ind,
              bee_certificate_expiry_date     = a_tbl_update(i).bee_certificate_expiry_date,
              valid_certificate_ind           = a_tbl_update(i).valid_certificate_ind,
              type_of_agreement_required      = a_tbl_update(i).type_of_agreement_required,
              moa_signed_ind                  = a_tbl_update(i).moa_signed_ind,
              aod_signed_ind                  = a_tbl_update(i).aod_signed_ind,
              agreement_in_place_ind          = a_tbl_update(i).agreement_in_place_ind,
              inception_date                  = a_tbl_update(i).inception_date,
              agreement_expiry_date           = a_tbl_update(i).agreement_expiry_date,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              no_of_jobs_created              = a_tbl_update(i).no_of_jobs_created,
              ed_sd_indicator                 = a_tbl_update(i).ed_sd_indicator,
              effective_date                  = a_tbl_update(i).effective_date,
              enterprise_beneficiary_status   = a_tbl_update(i).enterprise_beneficiary_status
       where  ed_beneficiary_id               = a_tbl_update(i).ed_beneficiary_id ;

  g_rec_out.last_updated_date := g_date;
       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).ed_beneficiary_id ;
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
       update stg_excel_ed_beneficiary_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
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

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_hr_excel_ed_beneficiary
   where  ed_beneficiary_id      = g_rec_out.ed_beneficiary_id ;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).ed_beneficiary_id  = g_rec_out.ed_beneficiary_id then
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
      local_bulk_staging_update;

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
       l_message := dwh_hr_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_hr_excel_ed_beneficiary EX oracle fin STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_excel_ed_beneficiary;
    fetch c_stg_excel_ed_beneficiary bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
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
    fetch c_stg_excel_ed_beneficiary bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_excel_ed_beneficiary;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_FND_HR_204U;
