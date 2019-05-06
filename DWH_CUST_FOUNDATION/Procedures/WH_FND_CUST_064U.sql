--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_064U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_064U" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alastair de Wet
--  Purpose:     Create WOD_Application dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_c2_wod_application_cpy
--               Output - fnd_wod_application
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
g_hospital_text      stg_c2_wod_application_hsp.sys_process_msg%type;
g_rec_out            fnd_wod_application%rowtype;
g_rec_in             stg_c2_wod_application_cpy%rowtype;
g_found              boolean;
g_count              number        :=  0;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_064U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WOD_APPLICATION DIM EX CUSTOMER CENTRAL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_c2_wod_application_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_wod_application%rowtype index by binary_integer;
type tbl_array_u is table of fnd_wod_application%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_c2_wod_application_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_c2_wod_application_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_c2_wod_application is
   select *
   from stg_c2_wod_application_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.application_id                  := g_rec_in.application_id;
   g_rec_out.address_line1                   := g_rec_in.address_line1;
   g_rec_out.address_line2                   := g_rec_in.address_line2;
   g_rec_out.application_date                := g_rec_in.application_date;
   g_rec_out.application_status              := g_rec_in.application_status;
   g_rec_out.application_status_desc         := g_rec_in.application_status_desc;
   g_rec_out.birth_date                      := g_rec_in.birth_date;
   g_rec_out.cell_phone_no                   := g_rec_in.cell_phone_no;
   g_rec_out.channel                         := g_rec_in.channel;
   g_rec_out.child1_birthdate                := g_rec_in.child1_birthdate;
   g_rec_out.child1_gender                   := g_rec_in.child1_gender;
   g_rec_out.child1_name                     := g_rec_in.child1_name;
   g_rec_out.child1_surname                  := g_rec_in.child1_surname;
   g_rec_out.child2_birthdate                := g_rec_in.child2_birthdate;
   g_rec_out.child2_gender                   := g_rec_in.child2_gender;
   g_rec_out.child2_name                     := g_rec_in.child2_name;
   g_rec_out.child2_surname                  := g_rec_in.child2_surname;
   g_rec_out.child3_birthdte                 := g_rec_in.child3_birthdte;
   g_rec_out.child3_gender                   := g_rec_in.child3_gender;
   g_rec_out.child3_name                     := g_rec_in.child3_name;
   g_rec_out.child3_surname                  := g_rec_in.child3_surname;
   g_rec_out.child4_birthdate                := g_rec_in.child4_birthdate;
   g_rec_out.child4_gender                   := g_rec_in.child4_gender;
   g_rec_out.child4_name                     := g_rec_in.child4_name;
   g_rec_out.child4_surname                  := g_rec_in.child4_surname;
   g_rec_out.child5_birthdate                := g_rec_in.child5_birthdate;
   g_rec_out.child5_gender                   := g_rec_in.child5_gender;
   g_rec_out.child5_name                     := g_rec_in.child5_name;
   g_rec_out.child5_surname                  := g_rec_in.child5_surname;
   g_rec_out.child6_birthdate                := g_rec_in.child6_birthdate;
   g_rec_out.child6_gender                   := g_rec_in.child6_gender;
   g_rec_out.child6_name                     := g_rec_in.child6_name;
   g_rec_out.child6_surname                  := g_rec_in.child6_surname;
   g_rec_out.country_desc                    := g_rec_in.country_desc;
   g_rec_out.created_myschool                := g_rec_in.created_myschool;
   g_rec_out.customer_no                     := g_rec_in.customer_no;
   g_rec_out.pregnancy_due_date              := g_rec_in.pregnancy_due_date;
   g_rec_out.email_address                   := g_rec_in.email_address;
   g_rec_out.error_status                    := g_rec_in.error_status;
   g_rec_out.cust_first_name                 := g_rec_in.cust_first_name;
   g_rec_out.gender                          := g_rec_in.gender;
   g_rec_out.home_phone_no                   := g_rec_in.home_phone_no;
   g_rec_out.identity_no                     := g_rec_in.identity_no;
   g_rec_out.cust_initials                   := g_rec_in.cust_initials;
   g_rec_out.littleworld                     := g_rec_in.littleworld;
   g_rec_out.myschool_card_no                := g_rec_in.myschool_card_no;
   g_rec_out.myschool                        := g_rec_in.myschool;
   g_rec_out.cust_surname                    := g_rec_in.cust_surname;
   g_rec_out.passport_no                     := g_rec_in.passport_no;
   g_rec_out.permission_3rd_party            := g_rec_in.permission_3rd_party;
   g_rec_out.permission_email                := g_rec_in.permission_email;
   g_rec_out.permission_phone                := g_rec_in.permission_phone;
   g_rec_out.permission_pos                  := g_rec_in.permission_pos;
   g_rec_out.permission_post                 := g_rec_in.permission_post;
   g_rec_out.permission_sms                  := g_rec_in.permission_sms;
   g_rec_out.popout_card_no                  := g_rec_in.popout_card_no;
   g_rec_out.postal_code                     := g_rec_in.postal_code;
   g_rec_out.preferred_language_desc         := g_rec_in.preferred_language_desc;
   g_rec_out.appl_processed                  := g_rec_in.appl_processed;
   g_rec_out.relationship_id                 := g_rec_in.relationship_id;
   g_rec_out.referral_desc                   := g_rec_in.referral_desc;
   g_rec_out.suburb                          := g_rec_in.suburb;
   g_rec_out.terms_conditions_wod            := g_rec_in.terms_conditions_wod;
   g_rec_out.terms_conditions_lw             := g_rec_in.terms_conditions_lw;
   g_rec_out.title_desc                      := g_rec_in.title_desc;
   g_rec_out.work_phone_no                   := g_rec_in.work_phone_no;
   g_rec_out.receive_card_post               := g_rec_in.receive_card_post;
   g_rec_out.receive_card_collect            := g_rec_in.receive_card_collect;
   g_rec_out.application_customer            := g_rec_in.application_customer;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.twitter_handle                  := g_rec_in.twitter_handle;


--   if not dwh_valid.indicator_field(g_rec_out.embossed_ind) then
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

   insert into stg_c2_wod_application_hsp values g_rec_in;
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
       insert into fnd_wod_application values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).application_id;
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
       update fnd_wod_application
       set    address_line1                   = a_tbl_update(i).address_line1,
              address_line2                   = a_tbl_update(i).address_line2,
              application_date                = a_tbl_update(i).application_date,
              application_status              = a_tbl_update(i).application_status,
              application_status_desc         = a_tbl_update(i).application_status_desc,
              birth_date                      = a_tbl_update(i).birth_date,
              cell_phone_no                   = a_tbl_update(i).cell_phone_no,
              channel                         = a_tbl_update(i).channel,
              child1_birthdate                = a_tbl_update(i).child1_birthdate,
              child1_gender                   = a_tbl_update(i).child1_gender,
              child1_name                     = a_tbl_update(i).child1_name,
              child1_surname                  = a_tbl_update(i).child1_surname,
              child2_birthdate                = a_tbl_update(i).child2_birthdate,
              child2_gender                   = a_tbl_update(i).child2_gender,
              child2_name                     = a_tbl_update(i).child2_name,
              child2_surname                  = a_tbl_update(i).child2_surname,
              child3_birthdte                 = a_tbl_update(i).child3_birthdte,
              child3_gender                   = a_tbl_update(i).child3_gender,
              child3_name                     = a_tbl_update(i).child3_name,
              child3_surname                  = a_tbl_update(i).child3_surname,
              child4_birthdate                = a_tbl_update(i).child4_birthdate,
              child4_gender                   = a_tbl_update(i).child4_gender,
              child4_name                     = a_tbl_update(i).child4_name,
              child4_surname                  = a_tbl_update(i).child4_surname,
              child5_birthdate                = a_tbl_update(i).child5_birthdate,
              child5_gender                   = a_tbl_update(i).child5_gender,
              child5_name                     = a_tbl_update(i).child5_name,
              child5_surname                  = a_tbl_update(i).child5_surname,
              child6_birthdate                = a_tbl_update(i).child6_birthdate,
              child6_gender                   = a_tbl_update(i).child6_gender,
              child6_name                     = a_tbl_update(i).child6_name,
              child6_surname                  = a_tbl_update(i).child6_surname,
              country_desc                    = a_tbl_update(i).country_desc,
              created_myschool                = a_tbl_update(i).created_myschool,
              customer_no                     = a_tbl_update(i).customer_no,
              pregnancy_due_date              = a_tbl_update(i).pregnancy_due_date,
              email_address                   = a_tbl_update(i).email_address,
              error_status                    = a_tbl_update(i).error_status,
              cust_first_name                 = a_tbl_update(i).cust_first_name,
              gender                          = a_tbl_update(i).gender,
              home_phone_no                   = a_tbl_update(i).home_phone_no,
              identity_no                     = a_tbl_update(i).identity_no,
              cust_initials                   = a_tbl_update(i).cust_initials,
              littleworld                     = a_tbl_update(i).littleworld,
              myschool_card_no                = a_tbl_update(i).myschool_card_no,
              myschool                        = a_tbl_update(i).myschool,
              cust_surname                    = a_tbl_update(i).cust_surname,
              passport_no                     = a_tbl_update(i).passport_no,
              permission_3rd_party            = a_tbl_update(i).permission_3rd_party,
              permission_email                = a_tbl_update(i).permission_email,
              permission_phone                = a_tbl_update(i).permission_phone,
              permission_pos                  = a_tbl_update(i).permission_pos,
              permission_post                 = a_tbl_update(i).permission_post,
              permission_sms                  = a_tbl_update(i).permission_sms,
              popout_card_no                  = a_tbl_update(i).popout_card_no,
              postal_code                     = a_tbl_update(i).postal_code,
              preferred_language_desc         = a_tbl_update(i).preferred_language_desc,
              appl_processed                  = a_tbl_update(i).appl_processed,
              relationship_id                 = a_tbl_update(i).relationship_id,
              referral_desc                   = a_tbl_update(i).referral_desc,
              suburb                          = a_tbl_update(i).suburb,
              terms_conditions_wod            = a_tbl_update(i).terms_conditions_wod,
              terms_conditions_lw             = a_tbl_update(i).terms_conditions_lw,
              title_desc                      = a_tbl_update(i).title_desc,
              work_phone_no                   = a_tbl_update(i).work_phone_no,
              receive_card_post               = a_tbl_update(i).receive_card_post,
              receive_card_collect            = a_tbl_update(i).receive_card_collect,
              application_customer            = a_tbl_update(i).application_customer,
              twitter_handle                  = a_tbl_update(i).twitter_handle,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  application_id                  = a_tbl_update(i).application_id  ;

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
                       ' '||a_tbl_update(g_error_index).application_id;
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
       update stg_c2_wod_application_cpy
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
   g_found := FALSE;

-- Check to see if present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_wod_application
   where  application_id = g_rec_out.application_id;

   if g_count = 1 then
      g_found := TRUE;
   end if;
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).application_id  = g_rec_out.application_id  then
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

    l_text := 'LOAD OF fnd_wod_application EX CUST CENTRAL STARTED AT '||
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
    open c_stg_c2_wod_application;
    fetch c_stg_c2_wod_application bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_c2_wod_application bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_c2_wod_application;
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

END WH_FND_CUST_064U;
