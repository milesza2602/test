--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_639U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_639U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RTV ex SIMS rollup fact table in the performance layer
--               with input ex SIMS RTV  table from foundation layer.
--  Tables:      Input  - fnd_rtl_rtv_transaction
--               Output - rtl_rtv_tran_loc_dept_dy
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
g_rec_out            rtl_rtv_tran_loc_dept_dy%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_639U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RTV SIMS DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_rtv_tran_loc_dept_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_rtv_tran_loc_dept_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_rtl_rtv_transaction is
   select sum(nvl(rtv_t.rtv_mass,0))  as rtv_mass,
          sum(nvl(rtv_t.rtv_qty,0))   as rtv_qty,
          sum(nvl(rtv_t.rtv_cases,0)) as rtv_cases,
--          sum(nvl(rtv_t.rtv_qty,0)   * nvl(zi.cost_price,0))      as rtv_cost,
-- Cost price  not available on table Have to Calculate cost price ex case and Num upt.
          sum(nvl(rtv_t.rtv_qty,0)   * (nvl(zi.case_cost,0) / nvl(zi.num_units_per_tray,1)))      as rtv_cost,
          sum(nvl(rtv_t.rtv_qty,0)   * nvl(zi.reg_rsp_excl_vat,0)) as rtv_selling,
          sum(nvl(rtv_t.rtv_cases,0) * nvl(zi.case_cost,0))        as rtv_case_cost,
          sum(nvl(rtv_t.rtv_cases,0) * nvl(zi.case_selling_excl_vat,0)) as rtv_case_selling,
          sum(nvl(zi.case_mass,0)) as rtv_case_mass,
          rtv_t.post_date,
          di.sk1_department_no,
          max(di.business_unit_no) as business_unit_no,
          dl.sk1_location_no,
          max(dlh.sk2_location_no) as sk2_location_no,
          ds.sk1_supplier_no,
          dr.sk1_rtv_reason_code
   from   fnd_rtl_rtv_transaction rtv_t,
          fnd_rtl_rtv_item rtv_i,
          dim_item di,
          dim_location dl,
          dim_location_hist dlh,
          dim_supplier ds,
          dim_rtv_reason_code dr,
          rtl_zone_item_om zi
   where  rtv_t.post_date               > (g_date - 15) and
          rtv_t.item_no                 = rtv_i.item_no and
          rtv_t.rtv_reference_no        = rtv_i.rtv_reference_no and
          rtv_t.item_no                 = di.item_no  and
          rtv_t.location_no             = dl.location_no   and
          rtv_t.location_no             = dlh.location_no and
          rtv_t.post_date               between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          rtv_i.supplier_no             = ds.supplier_no and
          rtv_i.rtv_reason_code         = dr.rtv_reason_code and
          dl.sk1_fd_zone_group_zone_no  = zi.sk1_zone_group_zone_no and
          di.sk1_item_no                = zi.sk1_item_no(+) and
          di.business_unit_no           = 50
   group by di.sk1_department_no, dl.sk1_location_no, dr.sk1_rtv_reason_code, ds.sk1_supplier_no, rtv_t.post_date ;


g_rec_in             c_fnd_rtl_rtv_transaction%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_rtv_transaction%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


   g_rec_out.sk1_department_no               := g_rec_in.sk1_department_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_rtv_reason_code             := g_rec_in.sk1_rtv_reason_code;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.rtv_mass                        := g_rec_in.rtv_mass;
   g_rec_out.rtv_qty                         := g_rec_in.rtv_qty;
   g_rec_out.rtv_cases                       := g_rec_in.rtv_cases;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.rtv_cost                        := g_rec_in.rtv_cost;
   g_rec_out.rtv_selling                     := g_rec_in.rtv_selling;
   g_rec_out.rtv_case_selling                := g_rec_in.rtv_case_selling;
   g_rec_out.rtv_case_cost                   := g_rec_in.rtv_case_cost;
   g_rec_out.rtv_case_mass                   := g_rec_in.rtv_case_mass;

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
       insert into rtl_rtv_tran_loc_dept_dy values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_department_no||
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
       update rtl_rtv_tran_loc_dept_dy
       set    rtv_mass                        = a_tbl_update(i).rtv_mass,
              rtv_qty                         = a_tbl_update(i).rtv_qty,
              rtv_cases                       = a_tbl_update(i).rtv_cases,
              rtv_cost                        = a_tbl_update(i).rtv_cost,
              rtv_selling                     = a_tbl_update(i).rtv_selling,
              rtv_case_selling                = a_tbl_update(i).rtv_case_selling,
              rtv_case_cost                   = a_tbl_update(i).rtv_case_cost,
              rtv_case_mass                   = a_tbl_update(i).rtv_case_mass,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no      and
              sk1_department_no               = a_tbl_update(i).sk1_department_no    and
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no      and
              sk1_rtv_reason_code             = a_tbl_update(i).sk1_rtv_reason_code  and
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
                       ' '||a_tbl_update(g_error_index).sk1_department_no||
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
   from   rtl_rtv_tran_loc_dept_dy
   where  sk1_location_no     = g_rec_out.sk1_location_no      and
          sk1_department_no   = g_rec_out.sk1_department_no    and
          sk1_supplier_no     = g_rec_out.sk1_supplier_no      and
          sk1_rtv_reason_code = g_rec_out.sk1_rtv_reason_code  and
          post_date           = g_rec_out.post_date;

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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_rtv_tran_loc_dept_dy EX FOUNDATION STARTED AT '||
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
    open c_fnd_rtl_rtv_transaction;
    fetch c_fnd_rtl_rtv_transaction bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_rtv_transaction bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_rtv_transaction;
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
end wh_prf_corp_639u;
