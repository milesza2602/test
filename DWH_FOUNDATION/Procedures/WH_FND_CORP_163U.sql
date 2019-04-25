--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_163U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_163U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2013
--  Author:      Quentin Smit
--  Purpose:     Create Store Order specific shipments table from existing shipments table
--
--  Tables:      Input  - fnd_rtl_shipment
--               Output - fnd_rtl_shipment_st_ord
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
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
g_count              number        :=  0;
g_weigh_ind          number        :=  0;
g_rec_out            fnd_rtl_shipment_st_ord%rowtype;
g_rec_in             fnd_rtl_shipment%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_163U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STORE-ORDER SPECIFIC SHIPMENT DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type fnd_array is table of fnd_rtl_shipment%rowtype;
a_fnd_input      fnd_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_shipment_st_ord%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_shipment_st_ord%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_shipment is
   select /*+ PARALLEL(a,4) */ a.*
   from fnd_rtl_shipment a, dim_item b
   --, fnd_jdaff_dept_rollout dptr
   where a.last_updated_date = g_date
     and a.item_no = b.item_no
--     and b.department_no = dptr.department_no
--     and dptr.department_live_ind = 'Y'
     AND B.BUSINESS_UNIT_NO = 50 
   --  AND (PO_NO >= 3000000 OR PO_NO IS NULL) -- INTERIM MEASURE TO PREVENT OLD PO'S BEING OVERWRITTEN WITH RE-CYLCLED PO'S;

;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
v_count              number        :=  0;
begin

   g_rec_out.seq_no                 := g_rec_in.seq_no;
   g_rec_out.shipment_no            := g_rec_in.shipment_no;
   g_rec_out.item_no                := g_rec_in.item_no;

   g_rec_out.supplier_no            := g_rec_in.supplier_no;
   g_rec_out.po_no                  := g_rec_in.po_no;
   g_rec_out.sdn_no                 := g_rec_in.sdn_no;
   g_rec_out.ship_date              := g_rec_in.ship_date;
   g_rec_out.receive_date           := g_rec_in.receive_date;
   g_rec_out.shipment_status_code   := g_rec_in.shipment_status_code;
   g_rec_out.to_loc_no              := g_rec_in.to_loc_no;
   g_rec_out.from_loc_no            := g_rec_in.from_loc_no;
   g_rec_out.received_qty           := g_rec_in.received_qty;
   g_rec_out.reg_rsp                := g_rec_in.reg_rsp;
   g_rec_out.sdn_qty                := g_rec_in.sdn_qty;
   g_rec_out.actl_rcpt_date         := g_rec_in.actl_rcpt_date;

   g_rec_out.last_updated_date       := g_date;

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
       insert into fnd_rtl_shipment_st_ord values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).seq_no||
                       ' '||a_tbl_insert(g_error_index).shipment_no||
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
       update fnd_rtl_shipment_st_ord
       set    supplier_no                     = a_tbl_update(i).supplier_no ,
              po_no                           = a_tbl_update(i).po_no,
              sdn_no                          = a_tbl_update(i).sdn_no,
              ship_date                       = a_tbl_update(i).ship_date,
              receive_date                    = a_tbl_update(i).receive_date,
              shipment_status_code            = a_tbl_update(i).shipment_status_code,
              to_loc_no                       = a_tbl_update(i).to_loc_no,
              from_loc_no                     = a_tbl_update(i).from_loc_no,
              received_qty                    = a_tbl_update(i).received_qty,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              sdn_qty                         = a_tbl_update(i).sdn_qty,
              actl_rcpt_date                  = a_tbl_update(i).actl_rcpt_date,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  seq_no                          = a_tbl_update(i).seq_no
         and  shipment_no                     = a_tbl_update(i).shipment_no
         and  item_no                         = a_tbl_update(i).item_no;


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
                       ' '||a_tbl_update(g_error_index).seq_no||
                       ' '||a_tbl_update(g_error_index).shipment_no||
                       ' '||a_tbl_update(g_error_index).item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
 g_found := TRUE;

 g_found := FALSE;
  select count(1)
   into   g_count
   from   fnd_rtl_shipment_st_ord
   where  shipment_no    = g_rec_out.shipment_no
     and  item_no        = g_rec_out.item_no
     and  seq_no         = g_rec_out.seq_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).seq_no   = g_rec_out.seq_no and
            a_tbl_insert(i).shipment_no = g_rec_out.shipment_no and
            a_tbl_insert(i).item_no     = g_rec_out.item_no then
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
-- Main process
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_rtl_shipment_st_ord EX fnd_rtl_shipment STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '26/NOV/13';              --QST
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_shipment;
    fetch c_fnd_rtl_shipment bulk collect into a_fnd_input limit g_forall_limit;
    while a_fnd_input.count > 0
    loop
      for i in 1 .. a_fnd_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_fnd_input(i);
         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_shipment bulk collect into a_fnd_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_shipment;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

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
end wh_fnd_corp_163u;
