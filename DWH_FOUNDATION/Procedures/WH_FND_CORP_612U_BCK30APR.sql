--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_612U_BCK30APR
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_612U_BCK30APR" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
-- backup before DJ change
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Shipment fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_rtl_shipment
--               Output - fnd_rtl_shipment
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  18 May 2010 - Add Debtors commission to output table and calculate - QC 3774
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
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_shipment_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_shipment%rowtype;
g_rec_in             stg_rms_rtl_shipment_cpy%rowtype;
g_found              boolean;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_612U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE SHIPMENT FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of stg_rms_rtl_shipment_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_shipment%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_shipment%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_rtl_shipment_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_rtl_shipment_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_stg_rms_rtl_shipment is
   select *
   from stg_rms_rtl_shipment_cpy
   where sys_process_code = 'N' and
         to_loc_no      between p_from_loc_no and p_to_loc_no
   order by sys_source_sequence_no;   -- do not include sys_source_batch_id as part of order by QC 1013

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out.shipment_no                     := g_rec_in.shipment_no;
   g_rec_out.seq_no                          := g_rec_in.seq_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.po_no                           := g_rec_in.po_no;
   g_rec_out.sdn_no                          := g_rec_in.sdn_no;
   g_rec_out.asn_id                          := g_rec_in.asn_id;
   g_rec_out.ship_date                       := g_rec_in.ship_date;
   g_rec_out.receive_date                    := g_rec_in.receive_date;
   g_rec_out.shipment_status_code            := g_rec_in.shipment_status_code;
   g_rec_out.inv_match_status_code           := g_rec_in.inv_match_status_code;
   g_rec_out.inv_match_date                  := g_rec_in.inv_match_date;
   g_rec_out.to_loc_no                       := g_rec_in.to_loc_no;
   g_rec_out.from_loc_no                     := g_rec_in.from_loc_no;
   g_rec_out.courier_id                      := g_rec_in.courier_id;
   g_rec_out.ext_ref_in_id                   := g_rec_in.ext_ref_in_id;
   g_rec_out.dist_no                         := g_rec_in.dist_no;
   g_rec_out.dist_type                       := g_rec_in.dist_type;
   g_rec_out.ref_item_no                     := g_rec_in.ref_item_no;
   g_rec_out.tsf_alloc_no                    := g_rec_in.tsf_alloc_no;
   g_rec_out.tsf_po_link_no                  := g_rec_in.tsf_po_link_no;
   g_rec_out.ibt_type                        := g_rec_in.ibt_type;
   g_rec_out.sims_ref_id                     := g_rec_in.sims_ref_id;
   g_rec_out.du_id                           := g_rec_in.du_id;
   g_rec_out.shipment_line_status_code       := g_rec_in.shipment_line_status_code;
   g_rec_out.received_qty                    := g_rec_in.received_qty;
   g_rec_out.cost_price                      := g_rec_in.cost_price;
   g_rec_out.reg_rsp                         := g_rec_in.reg_rsp;
   g_rec_out.asn_qty                         := g_rec_in.asn_qty;
   g_rec_out.actual_mass                     := g_rec_in.actual_mass;
   g_rec_out.carton_status_code              := g_rec_in.carton_status_code;
   g_rec_out.carton_status_desc              := g_rec_in.carton_status_desc;
   g_rec_out.sdn_qty                         := g_rec_in.sdn_qty;
   g_rec_out.po_not_before_date              := g_rec_in.po_not_before_date;
   g_rec_out.contract_no                     := g_rec_in.contract_no;
   g_rec_out.actl_rcpt_date                  := g_rec_in.actl_rcpt_date;
   g_rec_out.supp_pack_size                  := g_rec_in.supp_pack_size;
   g_rec_out.cancelled_qty                   := g_rec_in.cancelled_qty;
   g_rec_out.auto_receipt_code               := g_rec_in.auto_receipt_code;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.final_loc_no                    := nvl(g_rec_in.final_loc_no, g_rec_in.to_loc_no);
   g_rec_out.last_updated_date               := g_date;

   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_source_code;
     return;
   end if;

   if not dwh_valid.fnd_location(g_rec_out.to_loc_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.to_loc_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if g_rec_out.from_loc_no is not null then
      if not dwh_valid.fnd_location(g_rec_out.from_loc_no) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_location_not_found;
         l_text          := dwh_constants.vc_location_not_found||g_rec_out.from_loc_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
      end if;
   end if;

   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if g_rec_out.supplier_no is not null then
      if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_supplier_not_found;
         l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
      end if;
   end if;

   begin
      select r.debtors_commission_perc
      into   g_rec_out.to_loc_debt_comm_perc
      from   rtl_loc_dept_dy r
      join   dim_location l        on r.sk1_location_no   = l.sk1_location_no
      join   dim_item i            on r.sk1_department_no = i.sk1_department_no
      where  l.location_no         =  g_rec_out.to_loc_no
      and    i.item_no             =  g_rec_out.item_no
      and    r.post_date           =  g_rec_out.ship_date;
      exception
         when no_data_found then
           g_rec_out.to_loc_debt_comm_perc := 0;
   end;
   if g_rec_out.to_loc_debt_comm_perc is null then
      g_rec_out.to_loc_debt_comm_perc := 0;
   end if;

      begin
      select r.debtors_commission_perc
      into   g_rec_out.from_loc_debt_comm_perc
      from   rtl_loc_dept_dy r
      join   dim_location l        on r.sk1_location_no   = l.sk1_location_no
      join   dim_item i            on r.sk1_department_no = i.sk1_department_no
      where  l.location_no         =  g_rec_out.from_loc_no
      and    i.item_no             =  g_rec_out.item_no
      and    r.post_date           =  g_rec_out.ship_date;
      exception
         when no_data_found then
           g_rec_out.from_loc_debt_comm_perc := 0;
   end;
   if g_rec_out.from_loc_debt_comm_perc is null then
      g_rec_out.from_loc_debt_comm_perc := 0;
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

   insert into stg_rms_rtl_shipment_hsp values g_rec_in;
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
       insert into fnd_rtl_shipment values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).shipment_no||
                       ' '||a_tbl_insert(g_error_index).seq_no||
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
       update fnd_rtl_shipment
       set    supplier_no                     = a_tbl_update(i).supplier_no,
              po_no                           = a_tbl_update(i).po_no,
              sdn_no                          = a_tbl_update(i).sdn_no,
              asn_id                          = a_tbl_update(i).asn_id,
              ship_date                       = a_tbl_update(i).ship_date,
              receive_date                    = a_tbl_update(i).receive_date,
              shipment_status_code            = a_tbl_update(i).shipment_status_code,
              inv_match_status_code           = a_tbl_update(i).inv_match_status_code,
              inv_match_date                  = a_tbl_update(i).inv_match_date,
              to_loc_no                       = a_tbl_update(i).to_loc_no,
              from_loc_no                     = a_tbl_update(i).from_loc_no,
              courier_id                      = a_tbl_update(i).courier_id,
              ext_ref_in_id                   = a_tbl_update(i).ext_ref_in_id,
              dist_no                         = a_tbl_update(i).dist_no,
              dist_type                       = a_tbl_update(i).dist_type,
              ref_item_no                     = a_tbl_update(i).ref_item_no,
              tsf_alloc_no                    = a_tbl_update(i).tsf_alloc_no,
              tsf_po_link_no                  = a_tbl_update(i).tsf_po_link_no,
              ibt_type                        = a_tbl_update(i).ibt_type,
              sims_ref_id                     = a_tbl_update(i).sims_ref_id,
              du_id                           = a_tbl_update(i).du_id,
              shipment_line_status_code       = a_tbl_update(i).shipment_line_status_code,
              received_qty                    = a_tbl_update(i).received_qty,
              cost_price                      = a_tbl_update(i).cost_price,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              asn_qty                         = a_tbl_update(i).asn_qty,
              actual_mass                     = a_tbl_update(i).actual_mass,
              carton_status_code              = a_tbl_update(i).carton_status_code,
              carton_status_desc              = a_tbl_update(i).carton_status_desc,
              sdn_qty                         = a_tbl_update(i).sdn_qty,
              po_not_before_date              = a_tbl_update(i).po_not_before_date,
              contract_no                     = a_tbl_update(i).contract_no,
              actl_rcpt_date                  = a_tbl_update(i).actl_rcpt_date,
              supp_pack_size                  = a_tbl_update(i).supp_pack_size,
              cancelled_qty                   = a_tbl_update(i).cancelled_qty,
              auto_receipt_code               = a_tbl_update(i).auto_receipt_code,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              to_loc_debt_comm_perc           = a_tbl_update(i).to_loc_debt_comm_perc,
              final_loc_no                    = a_tbl_update(i).final_loc_no,
              from_loc_debt_comm_perc         = a_tbl_update(i).from_loc_debt_comm_perc
       where  shipment_no                     = a_tbl_update(i).shipment_no
       and    seq_no                          = a_tbl_update(i).seq_no
       and    item_no                         = a_tbl_update(i).item_no;

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
                       ' '||a_tbl_update(g_error_index).shipment_no||
                       ' '||a_tbl_update(g_error_index).seq_no||
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
       update stg_rms_rtl_shipment_cpy
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
   into   g_count
   from   fnd_rtl_shipment
   where  shipment_no      = g_rec_out.shipment_no  and
          seq_no           = g_rec_out.seq_no and
          item_no          = g_rec_out.item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).shipment_no    = g_rec_out.shipment_no  and
            a_tbl_insert(i).seq_no         = g_rec_out.seq_no and
            a_tbl_insert(i).item_no        = g_rec_out.item_no then
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF FND_RTL_SHIPMENT EX OM STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_rtl_shipment;
    fetch c_stg_rms_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_rtl_shipment;
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
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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


END WH_FND_CORP_612U_BCK30APR;
