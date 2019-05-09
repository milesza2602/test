--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_634U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_634U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create the rtv item fact table in the foundation layer
--               with input ex staging table from SIMS.
--  Tables:      Input  - stg_sims_rtl_rtv_item_cpy
--               Output - fnd_rtl_rtv_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  17 June 2009 - QC 1809 - rremove validation for supplier if Zero
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
g_hospital_text      stg_sims_rtl_rtv_item_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_rtv_item%rowtype;
g_rec_in             stg_sims_rtl_rtv_item_cpy%rowtype;
g_found              boolean;
g_insert_rec         boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_634U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTV ITEM FACTS EX SIMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_sims_rtl_rtv_item_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_rtv_item%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_rtv_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_sims_rtl_rtv_item_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_sims_rtl_rtv_item_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor stg_sims_rtl_rtv_item is
   select *
   from stg_sims_rtl_rtv_item_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';
   g_rec_out.rtv_reference_no               := g_rec_in.rtv_reference_no;
   g_rec_out.item_no                        := g_rec_in.item_no;
   g_rec_out.create_date                    := g_rec_in.create_date;
   g_rec_out.create_user_name               := g_rec_in.create_user_name;
   g_rec_out.create_user_staff_id           := g_rec_in.create_user_staff_id;
   g_rec_out.origin_code                    := g_rec_in.origin_code;
   g_rec_out.supplier_no                    := g_rec_in.supplier_no;
   g_rec_out.overall_rtv_status             := g_rec_in.overall_rtv_status;
   g_rec_out.safety_alert_ind               := g_rec_in.safety_alert_ind;
   g_rec_out.rtv_reason_code                := g_rec_in.rtv_reason_code;
   g_rec_out.rtv_reason_desc                := g_rec_in.rtv_reason_desc;
   g_rec_out.disposal_no                    := g_rec_in.disposal_no;
   g_rec_out.disposal_desc                  := g_rec_in.disposal_desc;
--   g_rec_out.department_no                  := g_rec_in.department_no;
   g_rec_out.ho_manual_rtv_ref_no           := g_rec_in.ho_manual_rtv_ref_no;
   g_rec_out.create_user_id                 := g_rec_in.create_user_id;
   g_rec_out.liability_ind                  := g_rec_in.liability_ind;
   g_rec_out.supplier_batch_code            := g_rec_in.supplier_batch_code;
   g_rec_out.start_sell_by_date             := g_rec_in.start_sell_by_date;
   g_rec_out.sell_by_end_date               := g_rec_in.sell_by_end_date;
   g_rec_out.destroy_sold_ind               := g_rec_in.destroy_sold_ind;
   g_rec_out.ho_reviewed_uid                := g_rec_in.ho_reviewed_uid;
   g_rec_out.ho_reviewed_date               := g_rec_in.ho_reviewed_date;
   g_rec_out.ho_reviewed_status             := g_rec_in.ho_reviewed_status;
   g_rec_out.rejection_reason_desc          := g_rec_in.rejection_reason_desc;
   g_rec_out.last_updated_date              := g_date;

   -- rtv_reference_no validation TBC

   if not dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not g_rec_out.supplier_no = 0 then
      if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
        g_hospital      := 'Y';
        g_hospital_text := dwh_constants.vc_supplier_not_found;
        l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;

--   if not dwh_valid.fnd_department(g_rec_out.department_no) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_dept_not_found;
--     l_text          := dwh_constants.vc_dept_not_found||g_rec_out.department_no;
--     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   end if;

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

   insert into stg_sims_rtl_rtv_item_hsp values g_rec_in;
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
       insert into fnd_rtl_rtv_item values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).rtv_reference_no||
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
       update fnd_rtl_rtv_item
       set    rtv_reference_no               = a_tbl_update(i).rtv_reference_no,
              item_no                        = a_tbl_update(i).item_no,
              create_date                    = a_tbl_update(i).create_date,
              create_user_name               = a_tbl_update(i).create_user_name,
              create_user_staff_id           = a_tbl_update(i).create_user_staff_id,
              origin_code                    = a_tbl_update(i).origin_code,
              supplier_no                    = a_tbl_update(i).supplier_no,
              overall_rtv_status             = a_tbl_update(i).overall_rtv_status,
              safety_alert_ind               = a_tbl_update(i).safety_alert_ind,
              rtv_reason_code                = a_tbl_update(i).rtv_reason_code,
              rtv_reason_desc                = a_tbl_update(i).rtv_reason_desc,
              disposal_no                    = a_tbl_update(i).disposal_no,
              disposal_desc                  = a_tbl_update(i).disposal_desc,
--              department_no                  = a_tbl_update(i).department_no,
              ho_manual_rtv_ref_no           = a_tbl_update(i).ho_manual_rtv_ref_no,
              create_user_id                 = a_tbl_update(i).create_user_id,
              liability_ind                  = a_tbl_update(i).liability_ind,
              supplier_batch_code            = a_tbl_update(i).supplier_batch_code,
              start_sell_by_date             = a_tbl_update(i).start_sell_by_date,
              sell_by_end_date               = a_tbl_update(i).sell_by_end_date,
              destroy_sold_ind               = a_tbl_update(i).destroy_sold_ind,
              ho_reviewed_uid                = a_tbl_update(i).ho_reviewed_uid,
              ho_reviewed_date               = a_tbl_update(i).ho_reviewed_date,
              ho_reviewed_status             = a_tbl_update(i).ho_reviewed_status,
              rejection_reason_desc          = a_tbl_update(i).rejection_reason_desc,
              last_updated_date              = a_tbl_update(i).last_updated_date
       where  rtv_reference_no = a_tbl_update(i).rtv_reference_no
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
                       ' '||a_tbl_update(g_error_index).rtv_reference_no||
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
       update stg_sims_rtl_rtv_item_cpy
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
   from fnd_rtl_rtv_item
   where  rtv_reference_no = g_rec_out.rtv_reference_no
   and    item_no = g_rec_out.item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if (a_tbl_insert(i).rtv_reference_no = g_rec_out.rtv_reference_no and
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

    l_text := 'LOAD OF FND_RTL_RTV_ITEM EX POS STARTED AT '||
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
    open stg_sims_rtl_rtv_item;
    fetch stg_sims_rtl_rtv_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch stg_sims_rtl_rtv_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close stg_sims_rtl_rtv_item;
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
end wh_fnd_corp_634u;