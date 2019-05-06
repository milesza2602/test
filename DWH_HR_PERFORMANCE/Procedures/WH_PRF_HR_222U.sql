--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_222U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_222U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create dim_hr_bee_supplier dimention table in the performance layer
--               with added value ex foundation layer fnd_hr_bee_supplier.
--  Tables:      Input  - fnd_hr_bee_supplier
--               Output - dim_hr_bee_supplier
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    : Add more columns to fnd_hr_bee_supplier
--                    empowering_num
--                    sd_beneficiary_num
--                    designated_group_num
--                    sector_code
--                    sector_code_alignment_date
--                    supplier_take_on_date
--                    vat_no
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
g_rec_out            dim_hr_bee_supplier%rowtype;
g_rec_in             fnd_hr_bee_supplier%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_222U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE dim_hr_bee_supplier EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of fnd_hr_bee_supplier%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_hr_bee_supplier%rowtype index by binary_integer;
type tbl_array_u is table of dim_hr_bee_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_bee_supplier is
   select *
   from fnd_hr_bee_supplier;

-- No where clause used as we need to refresh all records for better continuity. Volumes are very small so no impact

--   where last_updated_date = g_date;


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
   g_rec_out.country_code                    := g_rec_in.country_code;
   g_rec_out.local_import                    := g_rec_in.local_import;
   g_rec_out.pay_group                       := g_rec_in.pay_group;
   g_rec_out.discount_terms                  := g_rec_in.discount_terms;
   g_rec_out.bee_status                      := g_rec_in.bee_status;
   g_rec_out.verification_status             := upper(g_rec_in.verification_status);
   g_rec_out.expiry_date                     := g_rec_in.expiry_date;
   g_rec_out.of_business_unit_code           := g_rec_in.of_business_unit_code;
   g_rec_out.bee_classification_code         := upper(g_rec_in.bee_classification_code);
   g_rec_out.black_ownership_perc            := g_rec_in.black_ownership_perc;
   g_rec_out.black_female_ownership_perc     := g_rec_in.black_female_ownership_perc;
   g_rec_out.sanas_no                        := g_rec_in.sanas_no;
   g_rec_out.verification_agency             := g_rec_in.verification_agency;
   g_rec_out.black_professional_company_num  := g_rec_in.black_professional_company_num;
   g_rec_out.ed_beneficiary_num              := g_rec_in.ed_beneficiary_num;
   g_rec_out.procurement_lead                := g_rec_in.procurement_lead;
   g_rec_out.value_adding_num                := g_rec_in.value_adding_num;
   g_rec_out.category_desc                   := g_rec_in.category_desc;
   g_rec_out.payment_terms_ind               := g_rec_in.payment_terms_ind;
   g_rec_out.settlement_discount_ind         := g_rec_in.settlement_discount_ind;
   g_rec_out.supplier_status                 := g_rec_in.supplier_status;
   g_rec_out.empowering_num 	               := g_rec_in.empowering_num;
   g_rec_out.sd_beneficiary_num	             := g_rec_in.sd_beneficiary_num;
   g_rec_out.designated_group_num            := g_rec_in.designated_group_num;
   g_rec_out.sector_code	                   := g_rec_in.sector_code;
   g_rec_out.sector_code_alignment_date	     := g_rec_in.sector_code_alignment_date;
   g_rec_out.supplier_take_on_date	         := g_rec_in.supplier_take_on_date;
   g_rec_out.vat_no	                         := g_rec_in.vat_no;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.bee_business_unit_code          := null;
   g_rec_out.sk1_bee_business_unit_code      := null;

   begin
      select bee_business_unit_code
      into   g_rec_out.bee_business_unit_code
      from   fnd_hr_of_bee_bu_map
      where  of_business_unit_code = g_rec_out.of_business_unit_code;

      select sk1_bee_business_unit_code
      into   g_rec_out.sk1_bee_business_unit_code
      from   dim_hr_bee_bu
      where  bee_business_unit_code = g_rec_out.bee_business_unit_code;

      exception
         when no_data_found then
              g_rec_out.bee_business_unit_code     := null;
              g_rec_out.sk1_bee_business_unit_code := null;
   end;

    begin
      g_rec_out.recognition_level_perc     := 0;

      select recognition_level_perc
      into   g_rec_out.recognition_level_perc
      from   fnd_hr_recognition_lvl
      where  bee_status = g_rec_out.bee_status;

      exception
         when no_data_found then
              g_rec_out.recognition_level_perc     := 0;
    end;
---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------

   g_rec_out.supplier_long_desc  := g_rec_in.supplier_id||' - '||
                                    substr(g_rec_out.supplier_name,1,48);

   exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into dim_hr_bee_supplier values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).supplier_id;
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
      update dim_hr_bee_supplier
         SET  org_id                          = a_tbl_update(i).org_id,
              supplier_name                   = a_tbl_update(i).supplier_name,
              supplier_no                     = a_tbl_update(i).supplier_no,
              supplier_type_code              = a_tbl_update(i).supplier_type_code,
              local_import                    = a_tbl_update(i).local_import,
              country_code                    = a_tbl_update(i).country_code,
              pay_group                       = a_tbl_update(i).pay_group,
              discount_terms                  = a_tbl_update(i).discount_terms,
              bee_status                      = a_tbl_update(i).bee_status,
              verification_status             = a_tbl_update(i).verification_status,
              expiry_date                     = a_tbl_update(i).expiry_date,
              of_business_unit_code           = a_tbl_update(i).of_business_unit_code,
              bee_classification_code         = a_tbl_update(i).bee_classification_code,
              black_ownership_perc            = a_tbl_update(i).black_ownership_perc,
              black_female_ownership_perc     = a_tbl_update(i).black_female_ownership_perc,
              sanas_no                        = a_tbl_update(i).sanas_no,
              verification_agency             = a_tbl_update(i).verification_agency,
              black_professional_company_num  = a_tbl_update(i).black_professional_company_num,
              ed_beneficiary_num              = a_tbl_update(i).ed_beneficiary_num,
              procurement_lead                = a_tbl_update(i).procurement_lead,
              value_adding_num                = a_tbl_update(i).value_adding_num,
              category_desc                   = a_tbl_update(i).category_desc,
              payment_terms_ind               = a_tbl_update(i).payment_terms_ind,
              settlement_discount_ind         = a_tbl_update(i).settlement_discount_ind,
              supplier_status                 = a_tbl_update(i).supplier_status,
              recognition_level_perc          = a_tbl_update(i).recognition_level_perc,
              bee_business_unit_code          = a_tbl_update(i).bee_business_unit_code,
              sk1_bee_business_unit_code      = a_tbl_update(i).sk1_bee_business_unit_code,
              supplier_long_desc              = a_tbl_update(i).supplier_long_desc,
              empowering_num 	                = a_tbl_update(i).empowering_num,
              sd_beneficiary_num	            = a_tbl_update(i).sd_beneficiary_num,
              designated_group_num            = a_tbl_update(i).designated_group_num,
              sector_code	                    = a_tbl_update(i).sector_code,
              sector_code_alignment_date	    = a_tbl_update(i).sector_code_alignment_date,
              supplier_take_on_date	          = a_tbl_update(i).supplier_take_on_date,
              vat_no	                        = a_tbl_update(i).vat_no
       where  supplier_id                     = a_tbl_update(i).supplier_id;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


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
                       ' '||a_tbl_update(g_error_index).supplier_ID;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_hr_valid.dim_hr_bee_supplier(g_rec_out.supplier_id);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_supplier_id  := hr_seq.nextval;
      a_count_i                  := a_count_i + 1;
      a_tbl_insert(a_count_i)    := g_rec_out;
   else
      a_count_u                  := a_count_u + 1;
      a_tbl_update(a_count_u)    := g_rec_out;
   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
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
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_hr_bee_supplier EX fnd_hr_bee_supplier STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_hr_bee_supplier;
    fetch c_fnd_hr_bee_supplier bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_hr_bee_supplier bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_bee_supplier;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_run_completed||sysdate;
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

end wh_prf_hr_222u;
