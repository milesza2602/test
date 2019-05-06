--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_650U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_650U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Create IA summary rollup fact table in the performance layer
--               with input ex Inventory adj RMS table from foundation layer.
--  Tables:      Input  - fnd_rtl_inventory_adj
--               Output - rtl_inv_adj_summary
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_inv_adj_summary%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_650U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE INVENTORY ADJ EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_inv_adj_summary%rowtype index by binary_integer;
type tbl_array_u is table of rtl_inv_adj_summary%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_inventory_adj is
   with   chgfria as
  (select location_no, item_no, reason_code, liability_type,
          liability_code, post_date, inv_adj_type
   from   fnd_rtl_inventory_adj
   where  last_updated_date = g_date
   group  by location_no, item_no, reason_code, liability_type,
          liability_code, post_date, inv_adj_type)

   select dl.sk1_location_no,
          di.sk1_item_no,
          iar.sk1_ia_reason_code,
          ialt.sk1_ia_liability_type,
          ialc.sk1_ia_liability_code,
          ia.post_date,
          iat.sk1_ia_type,
          max(dlh.sk2_location_no) as sk2_location_no,
          max(dih.sk2_item_no) as sk2_item_no,
          sum(nvl(ia.inv_adj_qty,0)) as inv_adj_qty,
          sum(nvl(ia.inv_adj_selling,0)) as inv_adj_selling,
          sum(nvl(ia.inv_adj_cost,0)) as inv_adj_cost,
          max(di.sk1_department_no) as sk1_department_no,
          max(dl.chain_no) as chain_no
   from   chgfria cf,
          fnd_rtl_inventory_adj ia,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh,
          dim_ia_liability_code ialc,
          dim_ia_liability_type ialt,
          dim_ia_type iat,
          dim_ia_reason_code iar
   where  cf.location_no             = ia.location_no and
          cf.item_no                 = ia.item_no and
          cf.reason_code             = ia.reason_code and
          cf.liability_type          = ia.liability_type and
          cf.liability_code          = ia.liability_code and
          cf.post_date               = ia.post_date and
          cf.inv_adj_type            = ia.inv_adj_type and
          ia.item_no                 = di.item_no and
          ia.location_no             = dl.location_no and
          ia.item_no                 = dih.item_no and
          ia.post_date               between dih.sk2_active_from_date and dih.sk2_active_to_date and
          ia.location_no             = dlh.location_no and
          ia.post_date               between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          ia.liability_code          = ialc.ia_liability_code and
          ia.liability_type          = ialt.ia_liability_type and
          ia.reason_code             = iar.ia_reason_code and
          ia.inv_adj_type            = iat.ia_type
   group  by dl.sk1_location_no, di.sk1_item_no, iar.sk1_ia_reason_code, ialt.sk1_ia_liability_type,
          ialc.sk1_ia_liability_code, ia.post_date, iat.sk1_ia_type;

g_rec_in             c_fnd_rtl_inventory_adj%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_inventory_adj%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_ia_reason_code              := g_rec_in.sk1_ia_reason_code;
   g_rec_out.sk1_ia_liability_type           := g_rec_in.sk1_ia_liability_type;
   g_rec_out.sk1_ia_liability_code           := g_rec_in.sk1_ia_liability_code;
   g_rec_out.sk1_ia_type                     := g_rec_in.sk1_ia_type;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.inv_adj_qty                     := g_rec_in.inv_adj_qty;
   g_rec_out.inv_adj_selling                 := g_rec_in.inv_adj_selling;
   g_rec_out.inv_adj_cost                    := g_rec_in.inv_adj_cost;

   g_rec_out.last_updated_date               := g_date;

   g_rec_out.inv_adj_fr_cost                 := '';
   if g_rec_in.chain_no = 20 then
      begin
         select debtors_commission_perc
         into   g_debtors_commission_perc
         from   rtl_loc_dept_dy
         where  sk1_location_no       = g_rec_out.sk1_location_no and
                sk1_department_no     = g_rec_in.sk1_department_no and
                post_date             = g_rec_out.post_date;
         exception
            when no_data_found then
              g_debtors_commission_perc := 0;
      end;
      g_rec_out.inv_adj_fr_cost                := nvl(g_rec_out.inv_adj_cost,0) + round((nvl(g_rec_out.inv_adj_cost,0) * g_debtors_commission_perc / 100),2);
   end if;

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
       insert into rtl_inv_adj_summary values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_inv_adj_summary
       set    inv_adj_qty                     = a_tbl_update(i).inv_adj_qty,
              inv_adj_selling                 = a_tbl_update(i).inv_adj_selling,
              inv_adj_cost                    = a_tbl_update(i).inv_adj_cost,
              inv_adj_fr_cost                 = a_tbl_update(i).inv_adj_fr_cost,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no    and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no           and
              sk1_ia_reason_code              = a_tbl_update(i).sk1_ia_reason_code    and
              sk1_ia_liability_type           = a_tbl_update(i).sk1_ia_liability_type and
              sk1_ia_liability_code           = a_tbl_update(i).sk1_ia_liability_code and
              sk1_ia_type                     = a_tbl_update(i).sk1_ia_type           and
              post_date                       = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
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
   from   rtl_inv_adj_summary
   where  sk1_location_no       = g_rec_out.sk1_location_no    and
          sk1_item_no           = g_rec_out.sk1_item_no        and
          sk1_ia_reason_code    = g_rec_out.sk1_ia_reason_code and
          sk1_ia_liability_type = g_rec_out.sk1_ia_liability_type and
          sk1_ia_liability_code = g_rec_out.sk1_ia_liability_code and
          sk1_ia_type           = g_rec_out.sk1_ia_type           and
          post_date             = g_rec_out.post_date;

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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_inv_adj_summary EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_inventory_adj;
    fetch c_fnd_rtl_inventory_adj bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_inventory_adj bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_inventory_adj;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

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

end wh_prf_corp_650u;
