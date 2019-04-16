--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_020U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_020U_OLD" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alastair de Wet
--  Purpose:     Create Dim _customer dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_c2_customer_cpy
--               Output - fnd_customer
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
g_hospital_text      stg_c2_customer_hsp.sys_process_msg%type;
g_rec_out            fnd_customer%rowtype;
g_rec_in             stg_c2_customer_cpy%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_020U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER  DIM EX CUSTOMER CENTRAL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_c2_customer_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_customer%rowtype index by binary_integer;
type tbl_array_u is table of fnd_customer%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_c2_customer_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_c2_customer_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_c2_customer is
   select *
   from stg_c2_customer_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                          := 'N';

   g_rec_out.customer_no                     := g_rec_in.customer_no ;
   G_REC_OUT.CUSTOMER_NO                     := G_REC_IN.CUSTOMER_NO;
   g_rec_out.wfs_customer_no                 := g_rec_in.wfs_customer_no;
   g_rec_out.identity_document_code          := g_rec_in.identity_document_code;
   g_rec_out.identity_document_type          := g_rec_in.identity_document_type;
   g_rec_out.passport_no                     := g_rec_in.passport_no;
   g_rec_out.passport_expiry_date            := g_rec_in.passport_expiry_date;
   g_rec_out.passport_issue_country_code     := g_rec_in.passport_issue_country_code;
   g_rec_out.individual_ind                  := g_rec_in.individual_ind;
   g_rec_out.customer_status                 := g_rec_in.customer_status;
   g_rec_out.opt_in_ind                      := g_rec_in.opt_in_ind;
   g_rec_out.glid_ind                        := g_rec_in.glid_ind;
   g_rec_out.itc_ind                         := g_rec_in.itc_ind;
   g_rec_out.fica_status                     := g_rec_in.fica_status;
   g_rec_out.no_marketing_via_phone_ind      := g_rec_in.no_marketing_via_phone_ind;
   g_rec_out.no_marketing_via_sms_ind        := g_rec_in.no_marketing_via_sms_ind;
   g_rec_out.no_share_my_details_ind         := g_rec_in.no_share_my_details_ind;
   g_rec_out.c2_create_date                  := g_rec_in.c2_create_date;
   g_rec_out.last_detail_confirm_date        := g_rec_in.last_detail_confirm_date;
   g_rec_out.last_web_access_date            := g_rec_in.last_web_access_date;
   g_rec_out.fica_change_date                := g_rec_in.fica_change_date;
   g_rec_out.last_itc_query_date             := g_rec_in.last_itc_query_date;
   g_rec_out.title_code                      := g_rec_in.title_code;
   g_rec_out.first_middle_name_initial       := g_rec_in.first_middle_name_initial;
   g_rec_out.first_name                      := g_rec_in.first_name;
   g_rec_out.preferred_name                  := g_rec_in.preferred_name;
   g_rec_out.last_name                       := g_rec_in.last_name;
   g_rec_out.maiden_name                     := g_rec_in.maiden_name;
   g_rec_out.birth_date                      := g_rec_in.birth_date;
   g_rec_out.gender_code                     := g_rec_in.gender_code;
   g_rec_out.marital_status                  := g_rec_in.marital_status;
   g_rec_out.marital_contract_type           := g_rec_in.marital_contract_type;
   g_rec_out.num_minor                       := g_rec_in.num_minor;
   g_rec_out.preferred_language              := g_rec_in.preferred_language;
   g_rec_out.customer_home_language          := g_rec_in.customer_home_language;
   g_rec_out.residential_country_code        := g_rec_in.residential_country_code;
   g_rec_out.primary_com_medium              := g_rec_in.primary_com_medium;
   g_rec_out.primary_com_language            := g_rec_in.primary_com_language;
   g_rec_out.secondary_com_medium            := g_rec_in.secondary_com_medium;
   g_rec_out.secondary_com_language          := g_rec_in.secondary_com_language;
   g_rec_out.postal_address_line_1           := g_rec_in.postal_address_line_1;
   g_rec_out.postal_address_line_2           := g_rec_in.postal_address_line_2;
   g_rec_out.postal_address_line_3           := g_rec_in.postal_address_line_3;
   g_rec_out.postal_code                     := g_rec_in.postal_code;
   g_rec_out.postal_city_name                := g_rec_in.postal_city_name;
   g_rec_out.postal_province_name            := g_rec_in.postal_province_name;
   g_rec_out.postal_country_code             := g_rec_in.postal_country_code;
   g_rec_out.postal_address_occupation_date  := g_rec_in.postal_address_occupation_date;
   g_rec_out.postal_num_returned_mail        := g_rec_in.postal_num_returned_mail;
   g_rec_out.physical_address_line_1         := g_rec_in.physical_address_line_1;
   g_rec_out.physical_address_line2          := g_rec_in.physical_address_line2;
   g_rec_out.physical_suburb_name            := g_rec_in.physical_suburb_name;
   g_rec_out.physical_postal_code            := g_rec_in.physical_postal_code;
   g_rec_out.physical_city_name              := g_rec_in.physical_city_name;
   g_rec_out.physical_province_name          := g_rec_in.physical_province_name;
   g_rec_out.physical_country_code           := g_rec_in.physical_country_code;
   g_rec_out.physical_address_occuptn_date   := g_rec_in.physical_address_occuptn_date;
   g_rec_out.physical_num_returned_mail      := g_rec_in.physical_num_returned_mail;
   g_rec_out.home_phone_country_code         := g_rec_in.home_phone_country_code;
   g_rec_out.home_phone_area_code            := g_rec_in.home_phone_area_code;
   g_rec_out.home_phone_no                   := g_rec_in.home_phone_no;
   g_rec_out.home_phone_extension_no         := g_rec_in.home_phone_extension_no;
   g_rec_out.home_fax_country_code           := g_rec_in.home_fax_country_code;
   g_rec_out.home_fax_area_code              := g_rec_in.home_fax_area_code;
   g_rec_out.home_fax_no                     := g_rec_in.home_fax_no;
   g_rec_out.home_cell_country_code          := g_rec_in.home_cell_country_code;
   g_rec_out.home_cell_area_code             := g_rec_in.home_cell_area_code;
   g_rec_out.home_cell_no                    := g_rec_in.home_cell_no;
   g_rec_out.home_email_address              := g_rec_in.home_email_address;
   g_rec_out.employment_status_ind           := g_rec_in.employment_status_ind;
   g_rec_out.company_name                    := g_rec_in.company_name;
   g_rec_out.company_type                    := g_rec_in.company_type;
   g_rec_out.employee_no                     := g_rec_in.employee_no;
   g_rec_out.employee_dept                   := g_rec_in.employee_dept;
   g_rec_out.employee_job_title              := g_rec_in.employee_job_title;
   g_rec_out.work_phone_country_code         := g_rec_in.work_phone_country_code;
   g_rec_out.work_phone_area_code            := g_rec_in.work_phone_area_code;
   g_rec_out.work_phone_no                   := g_rec_in.work_phone_no;
   g_rec_out.work_phone_extension_no         := g_rec_in.work_phone_extension_no;
   g_rec_out.work_fax_country_code           := g_rec_in.work_fax_country_code;
   g_rec_out.work_fax_area_code              := g_rec_in.work_fax_area_code;
   g_rec_out.work_fax_no                     := g_rec_in.work_fax_no;
   g_rec_out.work_cell_country_code          := g_rec_in.work_cell_country_code;
   g_rec_out.work_cell_area_code             := g_rec_in.work_cell_area_code;
   g_rec_out.work_cell_no                    := g_rec_in.work_cell_no;
   g_rec_out.work_email_address              := g_rec_in.work_email_address;
   g_rec_out.home_cell_failure_ind           := g_rec_in.home_cell_failure_ind;
   g_rec_out.home_cell_date_last_updated     := g_rec_in.home_cell_date_last_updated;
   g_rec_out.home_email_failure_ind          := g_rec_in.home_email_failure_ind;
   g_rec_out.home_email_date_last_updated    := g_rec_in.home_email_date_last_updated;
   g_rec_out.home_phone_failure_ind          := g_rec_in.home_phone_failure_ind;
   g_rec_out.home_phone_date_last_updated    := g_rec_in.home_phone_date_last_updated;
   g_rec_out.no_marketing_via_email_ind      := g_rec_in.no_marketing_via_email_ind;
   g_rec_out.no_marketing_via_post_ind       := g_rec_in.no_marketing_via_post_ind;
   G_REC_OUT.POST_ADDR_DATE_LAST_UPDATED     := G_REC_IN.POST_ADDR_DATE_LAST_UPDATED;
   g_rec_out.wfs_customer_no_txt_ver         := g_rec_in.wfs_customer_no_txt_ver;
   g_rec_out.work_cell_failure_ind           := g_rec_in.work_cell_failure_ind;
   g_rec_out.work_cell_date_last_updated     := g_rec_in.work_cell_date_last_updated;
   g_rec_out.work_email_failure_ind          := g_rec_in.work_email_failure_ind;
   g_rec_out.work_email_date_last_updated    := g_rec_in.work_email_date_last_updated;
   g_rec_out.work_phone_failure_ind          := g_rec_in.work_phone_failure_ind;
   g_rec_out.work_phone_date_last_updated    := g_rec_in.work_phone_date_last_updated;
   g_rec_out.ww_online_customer_no           := g_rec_in.ww_online_customer_no;
   g_rec_out.legal_language_description      := g_rec_in.legal_language_description;
   g_rec_out.estatement_email                := g_rec_in.estatement_email;
   g_rec_out.estatement_date_last_updated    := g_rec_in.estatement_date_last_updated;
   g_rec_out.estatement_email_failure_ind    := g_rec_in.estatement_email_failure_ind;
   g_rec_out.last_updated_date               := g_date;

   if  nvl(g_rec_out.home_cell_failure_ind,0) <> 0 then
       g_rec_out.home_cell_failure_ind :=1;
   end if;

   if  nvl(g_rec_out.home_email_failure_ind,0)  <> 0 then
       g_rec_out.home_email_failure_ind :=1;
   end if;

   if  nvl(g_rec_out.work_cell_failure_ind,0)  <> 0 then
       g_rec_out.work_cell_failure_ind :=1;
   end if;

   if   nvl(g_rec_out.work_email_failure_ind,0) <> 0 then
       g_rec_out.work_email_failure_ind :=1;
   end if;

   if   nvl(g_rec_out.work_phone_failure_ind,0) <> 0 then
       g_rec_out.work_phone_failure_ind :=1;
   end if;

   if   nvl(g_rec_out.no_marketing_via_email_ind,0) <> 0 then
       g_rec_out.no_marketing_via_email_ind :=1;
   end if;

   if   nvl(g_rec_out.no_marketing_via_post_ind,0) <> 0 then
       g_rec_out.no_marketing_via_post_ind :=1;
   end if;

   if   nvl(g_rec_out.estatement_email_failure_ind,0) <> 0 then
       g_rec_out.estatement_email_failure_ind :=1;
   end if;


--   if not dwh_valid.indicator_field(g_rec_out.active_ind) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
--     return;
--   end if;



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

   insert into stg_c2_customer_hsp values g_rec_in;
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
       insert into fnd_customer values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).CUSTOMER_no ;
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
       update FND_CUSTOMER
       set    wfs_customer_no                 = a_tbl_update(i).wfs_customer_no,
              identity_document_code          = a_tbl_update(i).identity_document_code,
              identity_document_type          = a_tbl_update(i).identity_document_type,
              passport_no                     = a_tbl_update(i).passport_no,
              passport_expiry_date            = a_tbl_update(i).passport_expiry_date,
              passport_issue_country_code     = a_tbl_update(i).passport_issue_country_code,
              individual_ind                  = a_tbl_update(i).individual_ind,
              customer_status                 = a_tbl_update(i).customer_status,
              opt_in_ind                      = a_tbl_update(i).opt_in_ind,
              glid_ind                        = a_tbl_update(i).glid_ind,
              itc_ind                         = a_tbl_update(i).itc_ind,
              fica_status                     = a_tbl_update(i).fica_status,
              no_marketing_via_phone_ind      = a_tbl_update(i).no_marketing_via_phone_ind,
              no_marketing_via_sms_ind        = a_tbl_update(i).no_marketing_via_sms_ind,
              no_share_my_details_ind         = a_tbl_update(i).no_share_my_details_ind,
              c2_create_date                  = a_tbl_update(i).c2_create_date,
              last_detail_confirm_date        = a_tbl_update(i).last_detail_confirm_date,
              last_web_access_date            = a_tbl_update(i).last_web_access_date,
              fica_change_date                = a_tbl_update(i).fica_change_date,
              last_itc_query_date             = a_tbl_update(i).last_itc_query_date,
              title_code                      = a_tbl_update(i).title_code,
              first_middle_name_initial       = a_tbl_update(i).first_middle_name_initial,
              first_name                      = a_tbl_update(i).first_name,
              preferred_name                  = a_tbl_update(i).preferred_name,
              last_name                       = a_tbl_update(i).last_name,
              maiden_name                     = a_tbl_update(i).maiden_name,
              birth_date                      = a_tbl_update(i).birth_date,
              gender_code                     = a_tbl_update(i).gender_code,
              marital_status                  = a_tbl_update(i).marital_status,
              marital_contract_type           = a_tbl_update(i).marital_contract_type,
              num_minor                       = a_tbl_update(i).num_minor,
              preferred_language              = a_tbl_update(i).preferred_language,
              customer_home_language          = a_tbl_update(i).customer_home_language,
              residential_country_code        = a_tbl_update(i).residential_country_code,
              primary_com_medium              = a_tbl_update(i).primary_com_medium,
              primary_com_language            = a_tbl_update(i).primary_com_language,
              secondary_com_medium            = a_tbl_update(i).secondary_com_medium,
              secondary_com_language          = a_tbl_update(i).secondary_com_language,
              postal_address_line_1           = a_tbl_update(i).postal_address_line_1,
              postal_address_line_2           = a_tbl_update(i).postal_address_line_2,
              postal_address_line_3           = a_tbl_update(i).postal_address_line_3,
              postal_code                     = a_tbl_update(i).postal_code,
              postal_city_name                = a_tbl_update(i).postal_city_name,
              postal_province_name            = a_tbl_update(i).postal_province_name,
              postal_country_code             = a_tbl_update(i).postal_country_code,
              postal_address_occupation_date  = a_tbl_update(i).postal_address_occupation_date,
              postal_num_returned_mail        = a_tbl_update(i).postal_num_returned_mail,
              physical_address_line_1         = a_tbl_update(i).physical_address_line_1,
              physical_address_line2          = a_tbl_update(i).physical_address_line2,
              physical_suburb_name            = a_tbl_update(i).physical_suburb_name,
              physical_postal_code            = a_tbl_update(i).physical_postal_code,
              physical_city_name              = a_tbl_update(i).physical_city_name,
              physical_province_name          = a_tbl_update(i).physical_province_name,
              physical_country_code           = a_tbl_update(i).physical_country_code,
              physical_address_occuptn_date   = a_tbl_update(i).physical_address_occuptn_date,
              physical_num_returned_mail      = a_tbl_update(i).physical_num_returned_mail,
              home_phone_country_code         = a_tbl_update(i).home_phone_country_code,
              home_phone_area_code            = a_tbl_update(i).home_phone_area_code,
              home_phone_no                   = a_tbl_update(i).home_phone_no,
              home_phone_extension_no         = a_tbl_update(i).home_phone_extension_no,
              home_fax_country_code           = a_tbl_update(i).home_fax_country_code,
              home_fax_area_code              = a_tbl_update(i).home_fax_area_code,
              home_fax_no                     = a_tbl_update(i).home_fax_no,
              home_cell_country_code          = a_tbl_update(i).home_cell_country_code,
              home_cell_area_code             = a_tbl_update(i).home_cell_area_code,
              home_cell_no                    = a_tbl_update(i).home_cell_no,
              home_email_address              = a_tbl_update(i).home_email_address,
              employment_status_ind           = a_tbl_update(i).employment_status_ind,
              company_name                    = a_tbl_update(i).company_name,
              company_type                    = a_tbl_update(i).company_type,
              employee_no                     = a_tbl_update(i).employee_no,
              employee_dept                   = a_tbl_update(i).employee_dept,
              employee_job_title              = a_tbl_update(i).employee_job_title,
              work_phone_country_code         = a_tbl_update(i).work_phone_country_code,
              work_phone_area_code            = a_tbl_update(i).work_phone_area_code,
              work_phone_no                   = a_tbl_update(i).work_phone_no,
              work_phone_extension_no         = a_tbl_update(i).work_phone_extension_no,
              work_fax_country_code           = a_tbl_update(i).work_fax_country_code,
              work_fax_area_code              = a_tbl_update(i).work_fax_area_code,
              work_fax_no                     = a_tbl_update(i).work_fax_no,
              work_cell_country_code          = a_tbl_update(i).work_cell_country_code,
              work_cell_area_code             = a_tbl_update(i).work_cell_area_code,
              work_cell_no                    = a_tbl_update(i).work_cell_no,
              work_email_address              = a_tbl_update(i).work_email_address,
              home_cell_failure_ind           = a_tbl_update(i).home_cell_failure_ind,
              home_cell_date_last_updated     = a_tbl_update(i).home_cell_date_last_updated,
              home_email_failure_ind          = a_tbl_update(i).home_email_failure_ind,
              home_email_date_last_updated    = a_tbl_update(i).home_email_date_last_updated,
              home_phone_failure_ind          = a_tbl_update(i).home_phone_failure_ind,
              home_phone_date_last_updated    = a_tbl_update(i).home_phone_date_last_updated,
              no_marketing_via_email_ind      = a_tbl_update(i).no_marketing_via_email_ind,
              no_marketing_via_post_ind       = a_tbl_update(i).no_marketing_via_post_ind,
              POST_ADDR_DATE_LAST_UPDATED     = A_TBL_UPDATE(I).POST_ADDR_DATE_LAST_UPDATED,
              wfs_customer_no_txt_ver         = a_tbl_update(i).wfs_customer_no_txt_ver,
              work_cell_failure_ind           = a_tbl_update(i).work_cell_failure_ind,
              work_cell_date_last_updated     = a_tbl_update(i).work_cell_date_last_updated,
              work_email_failure_ind          = a_tbl_update(i).work_email_failure_ind,
              work_email_date_last_updated    = a_tbl_update(i).work_email_date_last_updated,
              work_phone_failure_ind          = a_tbl_update(i).work_phone_failure_ind,
              work_phone_date_last_updated    = a_tbl_update(i).work_phone_date_last_updated,
              ww_online_customer_no           = a_tbl_update(i).ww_online_customer_no,
              legal_language_description      = a_tbl_update(i).legal_language_description,
              estatement_email                = a_tbl_update(i).estatement_email,
              estatement_date_last_updated    = a_tbl_update(i).estatement_date_last_updated,
              estatement_email_failure_ind    = a_tbl_update(i).estatement_email_failure_ind,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  customer_no                     = a_tbl_update(i).customer_no  ;

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
                       ' '||a_tbl_update(g_error_index).customer_no ;
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
       update stg_c2_customer_cpy
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
   g_found := dwh_cust_valid.fnd_customer (g_rec_out.customer_no );
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).customer_no  = g_rec_out.customer_no  then
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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_customer EX CUST CENTRAL STARTED AT '||
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
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_c2_customer;
    fetch c_stg_c2_customer bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_c2_customer bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_c2_customer;
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

END WH_FND_CUST_020U_OLD;
