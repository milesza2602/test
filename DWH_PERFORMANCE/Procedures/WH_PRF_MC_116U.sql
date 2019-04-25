--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_116U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_116U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID Sparse sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer.
--  Tables:      Input  - rtl_mc_loc_item_dy_rms_sparse
--               Output - rtl_mc_loc_item_dy_rms_sparse
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
g_rec_out            rtl_mc_loc_item_dy_rms_sparse%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_116U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CALCULATE THE RMS SPARSE MEASURES EX ITSELF';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_mc_loc_item_dy_rms_sparse%rowtype index by binary_integer;
type tbl_array_u is table of rtl_mc_loc_item_dy_rms_sparse%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_rtl_mc_loc_item_dy_rms_sp is
   select prf_lid.sk1_item_no,
          prf_lid.sk1_location_no,
          prf_lid.post_date,
          prf_lid.trunked_qty,
          prf_lid.trunked_cases,
          prf_lid.trunked_selling,
          prf_lid.trunked_cost,
          prf_lid.grn_qty,
          prf_lid.grn_cases,
          prf_lid.grn_selling,
          prf_lid.grn_cost,
          prf_lid.shrink_qty,
          prf_lid.shrink_cost,
          prf_lid.shrink_selling,
          prf_lid.gain_qty,
          prf_lid.gain_cost,
          prf_lid.gain_selling,
          prf_lid.waste_qty,
          prf_lid.claim_qty,
          prf_lid.self_supply_qty,
--MC--          
          prf_lid.trunked_selling_local,
          prf_lid.trunked_cost_local,
          prf_lid.grn_selling_local,
          prf_lid.grn_cost_local,
          prf_lid.shrink_cost_local,
          prf_lid.shrink_selling_local,
          prf_lid.gain_cost_local,
          prf_lid.gain_selling_local,
          
          di.standard_uom_code,
          nvl(prf_li.num_units_per_tray,1) num_units_per_tray,
          nvl(prf_li.this_wk_catalog_ind,0) this_wk_catalog_ind,
          dd.gifting_dept_ind,
          dd.book_magazine_dept_ind,
          nvl(diu.stock_error_ind_desc_699,'0') uda_699_ind
   from   rtl_mc_loc_item_dy_rms_sparse prf_lid,
          dim_item di,
          dim_department dd,
          rtl_location_item  prf_li,
          dim_item_uda diu
   where  prf_lid.last_updated_date  = g_date and
          prf_lid.sk1_item_no        = di.sk1_item_no and
          di.sk1_department_no       = dd.sk1_department_no and
          prf_lid.sk1_item_no        = prf_li.sk1_item_no(+) and
          prf_lid.sk1_location_no    = prf_li.sk1_location_no(+) and
          prf_lid.sk1_item_no        = diu.sk1_item_no(+) and
          di.business_unit_no = 50;



g_rec_in                   c_rtl_mc_loc_item_dy_rms_sp%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_mc_loc_item_dy_rms_sp%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.post_date                       := g_rec_in.post_date;

   g_rec_out.dc_delivered_qty                := 0;
   g_rec_out.dc_delivered_cases              := 0;
   g_rec_out.dc_delivered_cost               := 0;
   g_rec_out.dc_delivered_selling            := 0;
   g_rec_out.dc_delivered_cost_local         := 0;
   g_rec_out.dc_delivered_selling_local      := 0;

   if g_rec_in.gifting_dept_ind        = 0 and
      g_rec_in.book_magazine_dept_ind  = 0 then
      g_rec_out.dc_delivered_qty             := nvl(g_rec_in.trunked_qty,0)     + nvl(g_rec_in.grn_qty,0);
      g_rec_out.dc_delivered_cases           := nvl(g_rec_in.trunked_cases,0)   + nvl(g_rec_in.grn_cases,0);
      g_rec_out.dc_delivered_cost            := nvl(g_rec_in.trunked_cost,0)    + nvl(g_rec_in.grn_cost,0);
      g_rec_out.dc_delivered_selling         := nvl(g_rec_in.trunked_selling,0) + nvl(g_rec_in.grn_selling,0);
      g_rec_out.dc_delivered_cost_local      := nvl(g_rec_in.trunked_cost_local,0)    + nvl(g_rec_in.grn_cost_local,0);
      g_rec_out.dc_delivered_selling_local   := nvl(g_rec_in.trunked_selling_local,0) + nvl(g_rec_in.grn_selling_local,0);
   end if;

   if g_rec_in.num_units_per_tray <> 0 then
   g_rec_out.shrink_cases                    := round(nvl(g_rec_in.shrink_qty,0) / g_rec_in.num_units_per_tray,0);
   g_rec_out.gain_cases                      := round(nvl(g_rec_in.gain_qty,0)   / g_rec_in.num_units_per_tray,0);
   g_rec_out.shrinkage_cases                 := nvl(g_rec_out.shrink_cases,0)      + nvl(g_rec_out.gain_cases,0);
   g_rec_out.abs_shrinkage_cases             := nvl(abs(g_rec_out.shrink_cases),0) + nvl(abs(g_rec_out.gain_cases),0);
   end if;
   
   if g_rec_in.uda_699_ind = 'Y' and  g_rec_in.this_wk_catalog_ind = 1 then
      g_rec_out.abs_shrinkage_selling_dept   := nvl(abs(g_rec_in.shrink_selling),0) + nvl(abs(g_rec_in.gain_selling),0);
      g_rec_out.abs_shrinkage_cost_dept      := nvl(abs(g_rec_in.shrink_cost),0)    + nvl(abs(g_rec_in.gain_cost),0);
      g_rec_out.abs_shrinkage_qty_dept       := nvl(abs(g_rec_in.shrink_qty),0)     + nvl(abs(g_rec_in.gain_qty),0);
      g_rec_out.abs_shrinkage_cases_dept     := nvl(abs(g_rec_out.shrink_cases),0)   + nvl(abs(g_rec_out.gain_cases),0);
      g_rec_out.abs_shrinkage_selling_dept_lcl  := nvl(abs(g_rec_in.shrink_selling_local),0) + nvl(abs(g_rec_in.gain_selling_local),0);
      g_rec_out.abs_shrinkage_cost_dept_local   := nvl(abs(g_rec_in.shrink_cost_local),0)    + nvl(abs(g_rec_in.gain_cost_local),0);      
   end if;

   if g_rec_in.num_units_per_tray <> 0 then
   g_rec_out.waste_cases                      := round(nvl(g_rec_in.waste_qty,0)         / g_rec_in.num_units_per_tray,0);
   g_rec_out.claim_cases                      := round(nvl(g_rec_in.claim_qty,0)         / g_rec_in.num_units_per_tray,0);
   g_rec_out.self_supply_cases                := round(nvl(g_rec_in.self_supply_qty,0)   / g_rec_in.num_units_per_tray,0);
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
       insert into rtl_mc_loc_item_dy_rms_sparse values a_tbl_insert(i);

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
       update rtl_mc_loc_item_dy_rms_sparse
       set    dc_delivered_qty                = a_tbl_update(i).dc_delivered_qty,
              dc_delivered_cases              = a_tbl_update(i).dc_delivered_cases,
              dc_delivered_selling            = a_tbl_update(i).dc_delivered_selling,
              dc_delivered_cost               = a_tbl_update(i).dc_delivered_cost,
              shrink_cases                    = a_tbl_update(i).shrink_cases,
              gain_cases                      = a_tbl_update(i).gain_cases,
              shrinkage_cases                 = a_tbl_update(i).shrinkage_cases,
              abs_shrinkage_cases             = a_tbl_update(i).abs_shrinkage_cases,
              abs_shrinkage_selling_dept      = a_tbl_update(i).abs_shrinkage_selling_dept,
              abs_shrinkage_cost_dept         = a_tbl_update(i).abs_shrinkage_cost_dept,
              abs_shrinkage_qty_dept          = a_tbl_update(i).abs_shrinkage_qty_dept,
              abs_shrinkage_cases_dept        = a_tbl_update(i).abs_shrinkage_cases_dept,
              waste_cases                     = a_tbl_update(i).waste_cases,
              claim_cases                     = a_tbl_update(i).claim_cases,
              self_supply_cases               = a_tbl_update(i).self_supply_cases,

              dc_delivered_selling_local      = a_tbl_update(i).dc_delivered_selling_local,
              dc_delivered_cost_local         = a_tbl_update(i).dc_delivered_cost_local,              
              abs_shrinkage_selling_dept_lcl  = a_tbl_update(i).abs_shrinkage_selling_dept_lcl,
              abs_shrinkage_cost_dept_local   = a_tbl_update(i).abs_shrinkage_cost_dept_local             
              
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no      and
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
   g_found := TRUE;

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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_mc_loc_item_dy_rms_sparse EX ITSELF STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
---    g_date := '15 jan 2009'; For testing only
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_mc_loc_item_dy_rms_sp;
    fetch c_rtl_mc_loc_item_dy_rms_sp bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_mc_loc_item_dy_rms_sp bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_mc_loc_item_dy_rms_sp;

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
end wh_prf_mc_116u;
