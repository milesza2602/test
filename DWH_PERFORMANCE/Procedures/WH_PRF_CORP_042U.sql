--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_042U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_042U" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Item Supplier fact table in the performance layer
--               with input ex RMS fnd_item_supplier table from foundation layer.
--  Tables:      Input  - fnd_item_supplier
--               Output - rtl_item_supplier
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  28 May 2010 - Country of origing enhancement see QC 3751
--   31 AUG 2010 - QC3994 - Fix/change required to calculation of
--                      DERIVED_ORIGIN_COUNTRY_CODE on RTL_ITEM_SUPPLIER
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_item_supplier%rowtype;
g_country_code       fnd_item_supplier_country.origin_country_code%type;
g_found              boolean;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_042U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM SUPPLIER FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_item_supplier%rowtype index by binary_integer;
type tbl_array_u is table of rtl_item_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_item_supplier is
   select fis.*,
          di.sk1_item_no,
          ds.sk1_supplier_no,
          ds.prim_ord_country_code,
          di.item_level_no,
          di.tran_level_no
   from   fnd_item_supplier fis,
          dim_item di,
          dim_supplier ds
   where  fis.item_no                = di.item_no
   and    fis.supplier_no            = ds.supplier_no
   and    fis.last_updated_date      = g_date;
--   and ds.supplier_no = 11264
--and di.style_colour_no = 100717288;
-- For input bulk collect --
type stg_array is table of c_fnd_item_supplier%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_item_supplier%rowtype;

cursor c_fnd_diff_countries is
    with diff_countries as
    ( select   di.style_colour_no,
               ris.sk1_supplier_no,
               count(distinct ris.primary_country_code)
      from     rtl_item_supplier ris,
               dim_item di
      where    ris.delete_ind <> 1 and
               ris.sk1_item_no     = di.sk1_item_no
      group by di.style_colour_no, ris.sk1_supplier_no
      having   count(distinct ris.primary_country_code) > 1)
    select  rtl.sk1_item_no,rtl.sk1_supplier_no
    from    rtl_item_supplier rtl,
            dim_item dit,
            diff_countries dc
    where   dc.style_colour_no      = dit.style_colour_no and
            dit.sk1_item_no         = rtl.sk1_item_no and
            dc.sk1_supplier_no      = rtl.sk1_supplier_no
            ;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.primary_supplier_ind            := g_rec_in.primary_supplier_ind;
   g_rec_out.vendor_product_no               := g_rec_in.vendor_product_no;
   g_rec_out.supplier_label_desc             := g_rec_in.supplier_label_desc;
   g_rec_out.consignment_rate                := g_rec_in.consignment_rate;
   g_rec_out.supplier_diff_1_code            := g_rec_in.supplier_diff_1_code;
   g_rec_out.supplier_diff_2_code            := g_rec_in.supplier_diff_2_code;
   g_rec_out.supplier_diff_3_code            := g_rec_in.supplier_diff_3_code;
   g_rec_out.supplier_diff_4_code            := g_rec_in.supplier_diff_4_code;
   g_rec_out.pallet_code                     := g_rec_in.pallet_code;
   g_rec_out.case_code                       := g_rec_in.case_code;
   g_rec_out.inner_case_code                 := g_rec_in.inner_case_code;
   g_rec_out.discontinue_date                := g_rec_in.discontinue_date;
   g_rec_out.direct_shipment_ind             := g_rec_in.direct_shipment_ind;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;

-- Reference_items have   item_level_no > tran_level_no
-- We need the reference_items to have a derived_country_code = 'AAA'
-- so that if the max function is used when rolling to RTL_SC_SUPPLIER,
--  the deleted record will be ignored.
-- ie. Only valid(or T-level) items will be rolled
if g_rec_in.item_level_no <= g_rec_in.tran_level_no
then
   begin
   -- replaced code for qc3994
--       select max(origin_country_code), max(origin_country_code)
--       into   g_country_code,g_rec_out.primary_country_code
--       from   fnd_item_supplier_country
--       where  item_no     = g_rec_in.item_no and
--              supplier_no = g_rec_in.supplier_no and
--              primary_origin_country_ind = 1 group by g_rec_out.primary_country_code    ;
       select max(origin_country_code), max(origin_country_code)
       into   g_country_code,g_rec_out.primary_country_code
       from   fnd_item_supplier_country
       where  item_no     = g_rec_in.item_no and
              supplier_no = g_rec_in.supplier_no and
              primary_origin_country_ind = 1
;


       exception
         when no_data_found then
           g_country_code := 'ZA';
           g_rec_out.primary_country_code := 'TBC';
   end;

   if  g_country_code is null then
       g_country_code := 'ZA';
       g_rec_out.primary_country_code := 'TBC';
   end if;
   g_rec_out.delete_ind                      := 0;
   else
          g_country_code := 'AAA';
       g_rec_out.primary_country_code := 'AAA';
    g_rec_out.delete_ind                      := 1;
 end if;

   g_rec_out.derived_country_code            := g_rec_out.primary_country_code;
--   g_rec_out.delete_ind                      := 0;
   g_rec_out.purchase_type_no                := 1;
   g_rec_out.purchase_type_name              := 'LOCAL';
   g_rec_out.local_ind                       := 1;
   g_rec_out.direct_ind                      := 1;

   if g_country_code = 'ZA' and g_rec_in.prim_ord_country_code =  'ZA' then
      g_rec_out.purchase_type_no                := 1;
      g_rec_out.purchase_type_name              := 'LOCAL';
      g_rec_out.local_ind                       := 1;
      g_rec_out.direct_ind                      := 1;
   end if;
   if g_country_code = 'ZA' and g_rec_in.prim_ord_country_code <> 'ZA' then
      g_rec_out.purchase_type_no                := 2;
      g_rec_out.purchase_type_name              := 'DIRECT IMPORT';
      g_rec_out.local_ind                       := 0;
      g_rec_out.direct_ind                      := 1;
   end if;
   if g_country_code <> 'ZA' and g_rec_in.prim_ord_country_code =  'ZA' then
      g_rec_out.purchase_type_no                := 3;
      g_rec_out.purchase_type_name              := 'INDIRECT IMPORT';
      g_rec_out.local_ind                       := 1;
      g_rec_out.direct_ind                      := 0;
   end if;
   if g_country_code <> 'ZA' and g_rec_in.prim_ord_country_code <> 'ZA' then
      g_rec_out.purchase_type_no                := 2;
      g_rec_out.purchase_type_name              := 'DIRECT IMPORT';
      g_rec_out.local_ind                       := 0;
      g_rec_out.direct_ind                      := 1;
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
   end;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_item_supplier values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no;
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
       update rtl_item_supplier
       set    primary_supplier_ind            = a_tbl_update(i).primary_supplier_ind,
              vendor_product_no               = a_tbl_update(i).vendor_product_no,
              supplier_label_desc             = a_tbl_update(i).supplier_label_desc,
              consignment_rate                = a_tbl_update(i).consignment_rate,
              supplier_diff_1_code            = a_tbl_update(i).supplier_diff_1_code,
              supplier_diff_2_code            = a_tbl_update(i).supplier_diff_2_code,
              supplier_diff_3_code            = a_tbl_update(i).supplier_diff_3_code,
              supplier_diff_4_code            = a_tbl_update(i).supplier_diff_4_code,
              pallet_code                     = a_tbl_update(i).pallet_code,
              case_code                       = a_tbl_update(i).case_code,
              inner_case_code                 = a_tbl_update(i).inner_case_code,
              discontinue_date                = a_tbl_update(i).discontinue_date,
              direct_shipment_ind             = a_tbl_update(i).direct_shipment_ind,
              purchase_type_no                = a_tbl_update(i).purchase_type_no,
              purchase_type_name              = a_tbl_update(i).purchase_type_name,
              local_ind                       = a_tbl_update(i).local_ind ,
              direct_ind                      = a_tbl_update(i).direct_ind,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              sk1_purchase_type_no            = a_tbl_update(i).sk1_purchase_type_no,
              primary_country_code            = a_tbl_update(i).primary_country_code,
              delete_ind                      = a_tbl_update(i).delete_ind,
              derived_country_code            = a_tbl_update(i).derived_country_code
       where  sk1_item_no                     = a_tbl_update(i).sk1_item_no  and
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_item_supplier
   where  sk1_item_no      = g_rec_out.sk1_item_no  and
          sk1_supplier_no  = g_rec_out.sk1_supplier_no;

   if g_count = 1 then
      g_found := TRUE;
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

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;
--**************************************************************************************************
-- SETTING RECS to DELETED
--**************************************************************************************************
procedure set_records_as_deleted as
begin
    update rtl_item_supplier
    set    delete_ind           = 1,
           derived_country_code = 'AAA';

   exception
     when others then
       l_message := 'SET_RECORDS_DELETED - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end set_records_as_deleted;

--**************************************************************************************************
-- SETTING RECS to TBC where MULTILE COUNTRIES PER STYLE/COLOUR
--**************************************************************************************************
procedure set_records_as_tbc as
begin
   for diff_record in c_fnd_diff_countries
   loop
     update rtl_item_supplier
     set    derived_country_code = 'TBC'
     where  sk1_item_no          = diff_record.sk1_item_no   and
            sk1_supplier_no      = diff_record.sk1_supplier_no;

     g_recs_tbc  := g_recs_tbc  + 1;
   end loop;
   exception
     when others then
       l_message := 'SET_RECORDS_AS_TBC - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end set_records_as_tbc;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_ITEM_SUPPLIER EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--    G_DATE := '5 AUG 2010';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Set all records to deleted at start of run
--**************************************************************************************************
    l_text := 'SETTING ALL RECORDS TO DELETED STATUS' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    set_records_as_deleted;
    l_text := 'RECORDS SET TO DELETED STATUS COMPLETED' ;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_item_supplier;
    fetch c_fnd_item_supplier bulk collect into a_stg_input limit g_forall_limit;
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

        local_address_variables;
        local_write_output;

      end loop;
    fetch c_fnd_item_supplier bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_item_supplier;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
   local_bulk_insert;
   local_bulk_update;

--**************************************************************************************************
-- Set derived country to TBC where multiple countries per style/col
--**************************************************************************************************
    l_text := 'SETTING RECS to TBC where MULTILE COUNTRIES PER STYLE/COLOUR' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    set_records_as_tbc;
    l_text := 'RECORDS SET TO TBC '||g_recs_tbc ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
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

END WH_PRF_CORP_042U;
