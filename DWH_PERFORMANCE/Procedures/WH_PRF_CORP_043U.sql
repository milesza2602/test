--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_043U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_043U" (p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      Christie Koorts
--  Purpose:     Create style colour supplier dimension by fact table in the performance layer
--               rolled up from item supplier.
 --  Tables:      Input  -   rtl_item_supplier
--               Output -   rtl_sc_supplier
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_sc_supplier%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_043U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_SC_SUPPLIER EX RTL_ITEM_SUPPLIER';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--
-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_sc_supplier%rowtype index by binary_integer;
type tbl_array_u is table of rtl_sc_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
--
cursor c_rtl_item_supplier is
   select di.sk1_style_colour_no,
          ris.sk1_supplier_no,
          max(ris.primary_supplier_ind) as primary_supplier_ind,
          max(ris.supplier_label_desc) as supplier_label_desc,
          max(ris.direct_shipment_ind) as direct_shipment_ind,
          max(ris.purchase_type_no) as purchase_type_no,
          max(ris.local_ind) as local_ind,
          max(ris.direct_ind) as direct_ind,
          max(ris.derived_country_code) as derived_country_code
   from rtl_item_supplier ris,
        dim_item di
   where ris.sk1_item_no = di.sk1_item_no
   --exclude null sk1_style_colour_no's
   and di.sk1_style_colour_no is not null
   and di.tran_ind      = 1
   and ris.delete_ind  <> 1
   group by di.sk1_style_colour_no, ris.sk1_supplier_no;
--
-- Input record declared as cursor%rowtype
g_rec_in             c_rtl_item_supplier%rowtype;
--
-- Input bulk collect table declared
type stg_array is table of c_rtl_item_supplier%rowtype;
a_stg_input      stg_array;
--
-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.
--
--   where last_updated_date >= g_yesterday;
--   order by district_no
--
-- order by only where sequencing is essential to the correct loading of data
--
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin
--
   g_rec_out.sk1_style_colour_no               := g_rec_in.sk1_style_colour_no;
   g_rec_out.sk1_supplier_no                   := g_rec_in.sk1_supplier_no;
   g_rec_out.primary_supplier_ind              := g_rec_in.primary_supplier_ind;
   g_rec_out.supplier_label_desc               := g_rec_in.supplier_label_desc;
   g_rec_out.direct_shipment_ind               := g_rec_in.direct_shipment_ind;
   g_rec_out.purchase_type_no                  := g_rec_in.purchase_type_no;
   g_rec_out.local_ind                         := g_rec_in.local_ind;
   g_rec_out.direct_ind                        := g_rec_in.direct_ind;
   g_rec_out.derived_country_code              := g_rec_in.derived_country_code;
   g_rec_out.last_updated_date                 := g_date;

   if g_rec_out.derived_country_code  = 'AAA' then
      g_rec_out.derived_country_code := 'TBC';
   end if;

   begin

       select sk1_purchase_type_no
       into   g_rec_out.sk1_purchase_type_no
       from   dim_purchase_type
       where  purchase_type_no  = g_rec_out.purchase_type_no;

       exception
         when no_data_found then
           l_message := 'NO RECORD FOUND ON DIM_PURCHASE_TYPE FOR TYPE '||g_rec_out.purchase_type_no||'  '||sqlcode||' '||sqlerrm;
           dwh_log.record_error(l_module_name,sqlcode,l_message);
           raise;

   end ;
--
--
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
--
   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into rtl_sc_supplier values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
--
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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;
--
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin
--
   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update rtl_sc_supplier
      set    sk1_style_colour_no               = a_tbl_update(i).sk1_style_colour_no,
             sk1_supplier_no                   = a_tbl_update(i).sk1_supplier_no,
             primary_supplier_ind              = a_tbl_update(i).primary_supplier_ind,
             supplier_label_desc               = a_tbl_update(i).supplier_label_desc,
             direct_shipment_ind               = a_tbl_update(i).direct_shipment_ind,
             purchase_type_no                  = a_tbl_update(i).purchase_type_no,
             local_ind                         = a_tbl_update(i).local_ind,
             direct_ind                        = a_tbl_update(i).direct_ind,
             derived_country_code              = a_tbl_update(i).derived_country_code,
             last_updated_date                 = a_tbl_update(i).last_updated_date,
             sk1_purchase_type_no              = a_tbl_update(i).sk1_purchase_type_no
      where sk1_style_colour_no = a_tbl_update(i).sk1_style_colour_no
      and sk1_supplier_no = a_tbl_update(i).sk1_supplier_no;
--
      g_recs_updated := g_recs_updated + a_tbl_update.count;
--
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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;
--
--
--
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
--
begin
--
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into g_count
   from rtl_sc_supplier
   where sk1_style_colour_no = g_rec_out.sk1_style_colour_no
   and sk1_supplier_no = g_rec_out.sk1_supplier_no;
--
   if g_count = 1 then
      g_found := TRUE;
   end if;
--
-- Place record into array for later bulk writing
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;
--
   a_count := a_count + 1;
--
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
--
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
--
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
--
      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
--
end local_write_output;
--
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
--
    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
    l_text := 'LOAD OF RTL_SC_SUPPLIER EX RTL_ITEM_SUPPLIER STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
--
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
--**************************************************************************************************
--**************************************************************************************************
-- Truncate output table
--**************************************************************************************************

    l_text := 'Truncate Table RTL_SC_SUPPLIER '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_performance.rtl_sc_supplier';

--
--**************************************************************************************************
    open c_rtl_item_supplier;
    fetch c_rtl_item_supplier bulk collect into a_stg_input limit g_forall_limit;
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
--
         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;
--
      end loop;
    fetch c_rtl_item_supplier bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_item_supplier;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
--
      local_bulk_insert;
      local_bulk_update;
--
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
--
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
--
      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
end wh_prf_corp_043u;
