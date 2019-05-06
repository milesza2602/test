--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_626U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_626U_LOAD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create the invoice matching fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_invoice_matching_cpy
--               Output - fnd_rtl_invoice_matching
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_invoice_matching_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_invoice_matching%rowtype;
g_rec_in             stg_rms_invoice_matching_cpy%rowtype;
g_found              boolean;
g_insert_rec         boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_626U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE INVOICE MATCHING FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_invoice_matching_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_invoice_matching%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_invoice_matching%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_invoice_matching_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_invoice_matching_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor stg_rms_invoice_matching is
   select *
   from stg_rms_invoice_matching_arc
    where sys_load_date = '10 aug 14'
    and  sys_source_batch_id = 10360
   order by sys_source_batch_id,sys_source_sequence_no;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';
   g_rec_out.doc_no                         := g_rec_in.doc_no;
   g_rec_out.im_doc_detail_reason_codes_id  := g_rec_in.im_doc_detail_reason_codes_id;
   g_rec_out.item_no                        := g_rec_in.item_no;
   g_rec_out.doc_type                       := g_rec_in.doc_type;
   g_rec_out.doc_status_code                := g_rec_in.doc_status_code;
   g_rec_out.order_no                       := g_rec_in.order_no;
   g_rec_out.location_no                    := g_rec_in.location_no;
   g_rec_out.loc_type_code                  := g_rec_in.loc_type_code;
   g_rec_out.doc_date                       := g_rec_in.doc_date;
   g_rec_out.doc_create_date                := g_rec_in.doc_create_date;
   g_rec_out.supplier_no                    := g_rec_in.supplier_no;
   g_rec_out.supplier_inv_no                := g_rec_in.supplier_inv_no;
   g_rec_out.match_id                       := g_rec_in.match_id;
   g_rec_out.match_date                     := g_rec_in.match_date;
   g_rec_out.approval_user_id               := g_rec_in.approval_user_id;
   g_rec_out.approval_date                  := g_rec_in.approval_date;
   g_rec_out.pre_paid_ind                   := g_rec_in.pre_paid_ind;
   g_rec_out.pre_paid_user_id               := g_rec_in.pre_paid_user_id;
   g_rec_out.post_date                      := g_rec_in.post_date;
   g_rec_out.total_cost                     := g_rec_in.total_cost;
   g_rec_out.total_qty                      := g_rec_in.total_qty;
   g_rec_out.manually_paid_ind              := g_rec_in.manually_paid_ind;
   g_rec_out.custom_doc_ref_id_1            := g_rec_in.custom_doc_ref_id_1;
   g_rec_out.custom_doc_ref_id_2            := g_rec_in.custom_doc_ref_id_2;
   g_rec_out.custom_doc_ref_id_3            := g_rec_in.custom_doc_ref_id_3;
   g_rec_out.custom_doc_ref_id_4            := g_rec_in.custom_doc_ref_id_4;
   g_rec_out.doc_last_update_user_id        := g_rec_in.doc_last_update_user_id;
   g_rec_out.doc_last_update_datetime       := g_rec_in.doc_last_update_datetime;
   g_rec_out.ref_doc_no                     := g_rec_in.ref_doc_no;
   g_rec_out.ref_auth_no                    := g_rec_in.ref_auth_no;
   g_rec_out.pre_match_cost_ind             := g_rec_in.pre_match_cost_ind;
   g_rec_out.variance_within_tolerance      := g_rec_in.variance_within_tolerance;
   g_rec_out.resolution_adj_total_cost      := g_rec_in.resolution_adj_total_cost;
   g_rec_out.resolution_adj_total_qty       := g_rec_in.resolution_adj_total_qty;
   g_rec_out.consignment_ind                := g_rec_in.consignment_ind;
   g_rec_out.rtv_ind                        := g_rec_in.rtv_ind;
   g_rec_out.total_cost_incl_vat            := g_rec_in.total_cost_incl_vat;
   g_rec_out.reason_id                      := g_rec_in.reason_id;
   g_rec_out.doc_detail_status_code         := g_rec_in.doc_detail_status_code;
   g_rec_out.matched_cost_ind               := g_rec_in.matched_cost_ind;
   g_rec_out.matched_units_ind              := g_rec_in.matched_units_ind;
   g_rec_out.adjusted_unit_cost             := g_rec_in.adjusted_unit_cost;
   g_rec_out.adjusted_qty                   := g_rec_in.adjusted_qty;
   g_rec_out.unit_cost                      := g_rec_in.unit_cost;
   g_rec_out.invoice_qty                    := g_rec_in.invoice_qty;
   g_rec_out.resolution_adj_unit_cost       := g_rec_in.resolution_adj_unit_cost;
   g_rec_out.resolution_adj_qty             := g_rec_in.resolution_adj_qty;
   g_rec_out.inv_detail_status_code         := g_rec_in.inv_detail_status_code;
   g_rec_out.doc_detail_vat_code            := g_rec_in.doc_detail_vat_code;
   g_rec_out.doc_detail_vat_rate_perc       := g_rec_in.doc_detail_vat_rate_perc;
   g_rec_out.doc_dtl_commnt_create_user_id  := g_rec_in.doc_dtl_commnt_create_user_id;
   g_rec_out.last_updated_date              := g_date;

   if not dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_supplier_not_found;
     l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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

   insert into stg_rms_invoice_matching_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
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
       insert into fnd_rtl_invoice_matching values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).doc_no||
                       ' '||a_tbl_insert(g_error_index).im_doc_detail_reason_codes_id||
                       ' '||a_tbl_insert(g_error_index).item_no;
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
       update fnd_rtl_invoice_matching
       set    doc_no                         = a_tbl_update(i).doc_no,
              im_doc_detail_reason_codes_id  = a_tbl_update(i).im_doc_detail_reason_codes_id,
              item_no                        = a_tbl_update(i).item_no,
              doc_type                       = a_tbl_update(i).doc_type,
              doc_status_code                = a_tbl_update(i).doc_status_code,
              order_no                       = a_tbl_update(i).order_no,
              location_no                    = a_tbl_update(i).location_no,
              loc_type_code                  = a_tbl_update(i).loc_type_code,
              doc_date                       = a_tbl_update(i).doc_date,
              doc_create_date                = a_tbl_update(i).doc_create_date,
              supplier_no                    = a_tbl_update(i).supplier_no,
              supplier_inv_no                = a_tbl_update(i).supplier_inv_no,
              match_id                       = a_tbl_update(i).match_id,
              match_date                     = a_tbl_update(i).match_date,
              approval_user_id               = a_tbl_update(i).approval_user_id,
              approval_date                  = a_tbl_update(i).approval_date,
              pre_paid_ind                   = a_tbl_update(i).pre_paid_ind,
              pre_paid_user_id               = a_tbl_update(i).pre_paid_user_id,
              post_date                      = a_tbl_update(i).post_date,
              total_cost                     = a_tbl_update(i).total_cost,
              total_qty                      = a_tbl_update(i).total_qty,
              manually_paid_ind              = a_tbl_update(i).manually_paid_ind,
              custom_doc_ref_id_1            = a_tbl_update(i).custom_doc_ref_id_1,
              custom_doc_ref_id_2            = a_tbl_update(i).custom_doc_ref_id_2,
              custom_doc_ref_id_3            = a_tbl_update(i).custom_doc_ref_id_3,
              custom_doc_ref_id_4            = a_tbl_update(i).custom_doc_ref_id_4,
              doc_last_update_user_id        = a_tbl_update(i).doc_last_update_user_id,
              doc_last_update_datetime       = a_tbl_update(i).doc_last_update_datetime,
              ref_doc_no                     = a_tbl_update(i).ref_doc_no,
              ref_auth_no                    = a_tbl_update(i).ref_auth_no,
              pre_match_cost_ind             = a_tbl_update(i).pre_match_cost_ind,
              variance_within_tolerance      = a_tbl_update(i).variance_within_tolerance,
              resolution_adj_total_cost      = a_tbl_update(i).resolution_adj_total_cost,
              resolution_adj_total_qty       = a_tbl_update(i).resolution_adj_total_qty,
              consignment_ind                = a_tbl_update(i).consignment_ind,
              rtv_ind                        = a_tbl_update(i).rtv_ind,
              total_cost_incl_vat            = a_tbl_update(i).total_cost_incl_vat,
              reason_id                      = a_tbl_update(i).reason_id,
              doc_detail_status_code         = a_tbl_update(i).doc_detail_status_code,
              matched_cost_ind               = a_tbl_update(i).matched_cost_ind,
              matched_units_ind              = a_tbl_update(i).matched_units_ind,
              adjusted_unit_cost             = a_tbl_update(i).adjusted_unit_cost,
              adjusted_qty                   = a_tbl_update(i).adjusted_qty,
              unit_cost                      = a_tbl_update(i).unit_cost,
              invoice_qty                    = a_tbl_update(i).invoice_qty,
              resolution_adj_unit_cost       = a_tbl_update(i).resolution_adj_unit_cost,
              resolution_adj_qty             = a_tbl_update(i).resolution_adj_qty,
              inv_detail_status_code         = a_tbl_update(i).inv_detail_status_code,
              doc_detail_vat_code            = a_tbl_update(i).doc_detail_vat_code,
              doc_detail_vat_rate_perc       = a_tbl_update(i).doc_detail_vat_rate_perc,
              doc_dtl_commnt_create_user_id  = a_tbl_update(i).doc_dtl_commnt_create_user_id,
              last_updated_date              = a_tbl_update(i).last_updated_date
       where  doc_no = a_tbl_update(i).doc_no
       and    im_doc_detail_reason_codes_id = a_tbl_update(i).im_doc_detail_reason_codes_id
       and    item_no = a_tbl_update(i).item_no;

       g_recs_updated := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).doc_no||
                       ' '||a_tbl_update(g_error_index).im_doc_detail_reason_codes_id||
                       ' '||a_tbl_update(g_error_index).item_no;
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
       update stg_rms_invoice_matching_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
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
   into g_count
   from fnd_rtl_invoice_matching
   where  doc_no = g_rec_out.doc_no
   and    im_doc_detail_reason_codes_id = g_rec_out.im_doc_detail_reason_codes_id
   and    item_no = g_rec_out.item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if (a_tbl_insert(i).doc_no = g_rec_out.doc_no and
             a_tbl_insert(i).im_doc_detail_reason_codes_id = g_rec_out.im_doc_detail_reason_codes_id and
             a_tbl_insert(i).item_no = g_rec_out.item_no) then
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
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;


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

    l_text := 'LOAD OF FND_RTL_INVOICE_MATCHING EX POS STARTED AT '||
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

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open stg_rms_invoice_matching;
    fetch stg_rms_invoice_matching bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
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
    fetch stg_rms_invoice_matching bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close stg_rms_invoice_matching;
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
end wh_fnd_corp_626u_load;
