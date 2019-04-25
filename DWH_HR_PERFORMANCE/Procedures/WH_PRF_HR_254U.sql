--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_254U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_254U" (p_forall_limit in integer,p_success out boolean) AS
--**************************************************************************************************
--  Date:        Apr 2012
--  Author:      Alastair de Wet
--  Purpose:     Generate the supplier dimention  sk type 2 load program
--  Tables:      Input  - dim_hr_bee_supplier
--               Output - dim_hr_bee_supplier_hist
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--      Date      : Jul 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    : Add more columns to dim_hr_bee_supplier_hst
--                    empowering_num
--                    sd_beneficiary_num
--                    designated_group_num
--                    sector_code
--                    sector_code_alignment_date
--                    supplier_take_on_date
--                    vat_no
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

 g_rec_out         dim_hr_bee_supplier_hist%rowtype;

g_found             boolean;
g_insert_rec        boolean;
g_date              date          := trunc(sysdate);
g_this_mn_start_date date          ;
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_254U';
l_name              sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'CREATE dim_hr_bee_supplier_hist EX bee_supplier mast';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;

cursor c_dim_hr_bee_supplier is
   select  *
   from   dim_hr_bee_supplier  ;

g_rec_in            c_dim_hr_bee_supplier%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.org_id                          := g_rec_in.org_id;
   g_rec_out.supplier_id                     := g_rec_in.supplier_id;
   g_rec_out.supplier_name                   := g_rec_in.supplier_name;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.supplier_type_code              := g_rec_in.supplier_type_code;
   g_rec_out.local_import                    := g_rec_in.local_import;
   g_rec_out.country_code                    := g_rec_in.country_code;
   g_rec_out.pay_group                       := g_rec_in.pay_group;
   g_rec_out.discount_terms                  := g_rec_in.discount_terms;
   g_rec_out.bee_status                      := g_rec_in.bee_status;
   g_rec_out.verification_status             := g_rec_in.verification_status;
   g_rec_out.expiry_date                     := g_rec_in.expiry_date;
   g_rec_out.of_business_unit_code           := g_rec_in.of_business_unit_code;
   g_rec_out.bee_classification_code         := g_rec_in.bee_classification_code;
   g_rec_out.black_ownership_perc            := g_rec_in.black_ownership_perc;
   g_rec_out.black_female_ownership_perc     := g_rec_in.black_female_ownership_perc;
   g_rec_out.sanas_no                        := g_rec_in.sanas_no;
   g_rec_out.verification_agency             := g_rec_in.verification_agency;
   g_rec_out.black_professional_company_num  := g_rec_in.black_professional_company_num;
   g_rec_out.ed_beneficiary_num             := g_rec_in.ed_beneficiary_num;
   g_rec_out.procurement_lead                := g_rec_in.procurement_lead;
   g_rec_out.value_adding_num                := g_rec_in.value_adding_num;
   g_rec_out.category_desc                   := g_rec_in.category_desc;
   g_rec_out.payment_terms_ind               := g_rec_in.payment_terms_ind;
   g_rec_out.settlement_discount_ind         := g_rec_in.settlement_discount_ind;
   g_rec_out.supplier_long_desc              := g_rec_in.supplier_long_desc;
   g_rec_out.bee_business_unit_code          := g_rec_in.bee_business_unit_code;
   g_rec_out.sk1_bee_business_unit_code      := g_rec_in.sk1_bee_business_unit_code;
   g_rec_out.supplier_status                 := g_rec_in.supplier_status;
   g_rec_out.recognition_level_perc          := g_rec_in.recognition_level_perc;
    g_rec_out.empowering_num 	               := g_rec_in.empowering_num;
   g_rec_out.sd_beneficiary_num	             := g_rec_in.sd_beneficiary_num;
   g_rec_out.designated_group_num            := g_rec_in.designated_group_num;
   g_rec_out.sector_code	                   := g_rec_in.sector_code;
   g_rec_out.sector_code_alignment_date	     := g_rec_in.sector_code_alignment_date;
   g_rec_out.supplier_take_on_date	         := g_rec_in.supplier_take_on_date;
   g_rec_out.vat_no	                         := g_rec_in.vat_no;


   g_rec_out.last_updated_date               := g_date;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_rec_out.supplier_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Write valid data out to the supplier master table
--**************************************************************************************************
procedure local_write_output as
begin

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   dwh_hr_valid.did_supplier_change
   (g_rec_out.supplier_id,g_this_mn_start_date,g_rec_out.verification_status,g_rec_out.recognition_level_perc,
    g_rec_out.ed_beneficiary_num,g_rec_out.value_adding_num,g_rec_out.black_professional_company_num,
    g_rec_out.black_ownership_perc ,g_rec_out.black_female_ownership_perc,g_rec_out.bee_classification_code,
    g_insert_rec);

   if g_insert_rec then
      g_rec_out.sk2_supplier_id       := hr_seq.nextval;
      g_rec_out.sk2_active_from_date  := g_this_mn_start_date;
      g_rec_out.sk2_active_to_date    := dwh_constants.sk_to_date;
      g_rec_out.sk2_latest_record_ind := 1;
      insert into dim_hr_bee_supplier_hist values g_rec_out;
      g_recs_inserted                 := g_recs_inserted + sql%rowcount;
   else
      update dim_hr_bee_supplier_hist
      set     org_id                          = g_rec_out.org_id,
              supplier_id                     = g_rec_out.supplier_id,
              supplier_name                   = g_rec_out.supplier_name,
              supplier_no                     = g_rec_out.supplier_no,
              supplier_type_code              = g_rec_out.supplier_type_code,
              local_import                    = g_rec_out.local_import,
              country_code                    = g_rec_out.country_code,
              pay_group                       = g_rec_out.pay_group,
              discount_terms                  = g_rec_out.discount_terms,
              bee_status                      = g_rec_out.bee_status,
              verification_status             = g_rec_out.verification_status,
              expiry_date                     = g_rec_out.expiry_date,
              of_business_unit_code           = g_rec_out.of_business_unit_code,
              bee_classification_code         = g_rec_out.bee_classification_code,
              black_ownership_perc            = g_rec_out.black_ownership_perc,
              black_female_ownership_perc     = g_rec_out.black_female_ownership_perc,
              sanas_no                        = g_rec_out.sanas_no,
              verification_agency             = g_rec_out.verification_agency,
              black_professional_company_num  = g_rec_out.black_professional_company_num,
              ed_beneficiary_num              = g_rec_out.ed_beneficiary_num,
              procurement_lead                = g_rec_out.procurement_lead,
              value_adding_num               = g_rec_out.value_adding_num,
              category_desc                   = g_rec_out.category_desc,
              payment_terms_ind               = g_rec_out.payment_terms_ind,
              settlement_discount_ind         = g_rec_out.settlement_discount_ind,
              supplier_long_desc              = g_rec_out.supplier_long_desc,
              bee_business_unit_code          = g_rec_out.bee_business_unit_code,
              sk1_bee_business_unit_code      = g_rec_out.sk1_bee_business_unit_code,
              supplier_status                 = g_rec_out.supplier_status,
              recognition_level_perc          = g_rec_out.recognition_level_perc,
              empowering_num	                = g_rec_out.empowering_num,
              sd_beneficiary_num	            = g_rec_out.sd_beneficiary_num,
              designated_group_num            = g_rec_out.designated_group_num,
              sector_code	                    = g_rec_out.sector_code,
              sector_code_alignment_date	    = g_rec_out.sector_code_alignment_date,
              supplier_take_on_date	          = g_rec_out.supplier_take_on_date,
              vat_no	                        = g_rec_out.vat_no,
              last_updated_date               = g_date

      where  supplier_id                     = g_rec_out.supplier_id and
             sk2_active_to_date              = dwh_constants.sk_to_date;

      g_recs_updated              := g_recs_updated + sql%rowcount;
   end if;



  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' '||g_rec_out.supplier_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_out.supplier_id;
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
    for v_dim_hr_bee_supplier in c_dim_hr_bee_supplier
    loop
      g_recs_read := g_recs_read + 1;

      if g_recs_read mod 10000 = 0 then
         l_text := dwh_constants.vc_log_records_processed||
         to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      g_rec_in := v_dim_hr_bee_supplier;
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
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_out.supplier_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_prf_hr_254u;
