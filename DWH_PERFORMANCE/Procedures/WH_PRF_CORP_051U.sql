--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_051U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_051U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Zone Item OM fact table in the performance layer
--               with input ex OM fnd_zone_item_om table from foundation layer.
--  Tables:      Input  - fnd_zone_item_om
--               Output - rtl_zone_item_om
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
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
g_rec_out            rtl_zone_item_om%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_051U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE ITEM OM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_om%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_om%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_zone_item_om is
   select fiz.*,
          di.sk1_item_no,
          dz.sk1_zone_group_zone_no
   from   fnd_zone_item_om fiz,
          dim_item di,
          dim_zone dz,
          fnd_jdaff_dept_rollout dept
   where  fiz.item_no             = di.item_no  and
          fiz.zone_no             = dz.zone_no and
          fiz.zone_group_no       = dz.zone_group_no and
          di.department_no        = dept.department_no and
          dept.department_live_ind = 'N';

-- order by only where sequencing is essential to the correct loading of data

-- For input bulk collect --
type stg_array is table of c_fnd_zone_item_om%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_zone_item_om%rowtype;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.direct_delivery_ind             := g_rec_in.direct_delivery_ind;
   g_rec_out.from_loc_no                     := g_rec_in.from_loc_no;
   g_rec_out.case_mass                       := g_rec_in.case_mass;
   g_rec_out.tray_size_code                  := g_rec_in.tray_size_code;
   g_rec_out.num_units_per_tray              := g_rec_in.num_units_per_tray;
   g_rec_out.case_selling_excl_vat           := g_rec_in.case_selling_excl_vat;
   g_rec_out.case_cost                       := g_rec_in.case_cost;
--   g_rec_out.product_status_code             := g_rec_in.product_status_code;
   g_rec_out.reg_rsp_excl_vat                := g_rec_in.reg_rsp_excl_vat;
   g_rec_out.cost_price                      := g_rec_in.cost_price;
   g_rec_out.num_shelf_life_days             := g_rec_in.num_shelf_life_days;
   g_rec_out.num_supplier_leadtime_days      := g_rec_in.num_supplier_leadtime_days;
   g_rec_out.banker_ind                      := g_rec_in.banker_ind;
   g_rec_out.early_delivery_ind              := g_rec_in.early_delivery_ind;
   g_rec_out.mu_pack_height_code             := g_rec_in.mu_pack_height_code;
   g_rec_out.mu_extsn_sleeve_code            := g_rec_in.mu_extsn_sleeve_code;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.num_extra_leadtime_days         := g_rec_in.num_extra_leadtime_days;
--   g_rec_out.reg_delivery_pattern_code       := g_rec_in.reg_delivery_pattern_code;
   if g_rec_in.reg_delivery_pattern_code is null
   or g_rec_in.reg_delivery_pattern_code = ' '
   then
    g_rec_out.reg_delivery_pattern_code := 2;
   else
    g_rec_out.reg_delivery_pattern_code := to_number(translate(g_rec_in.reg_delivery_pattern_code,'YNO0', '1322'));
    end if;
   g_rec_out.reg_shelf_life_day_1            := g_rec_in.reg_shelf_life_day_1;
   g_rec_out.reg_shelf_life_day_2            := g_rec_in.reg_shelf_life_day_2;
   g_rec_out.reg_shelf_life_day_3            := g_rec_in.reg_shelf_life_day_3;
   g_rec_out.reg_shelf_life_day_4            := g_rec_in.reg_shelf_life_day_4;
   g_rec_out.reg_shelf_life_day_5            := g_rec_in.reg_shelf_life_day_5;
   g_rec_out.reg_shelf_life_day_6            := g_rec_in.reg_shelf_life_day_6;
   g_rec_out.reg_shelf_life_day_7            := g_rec_in.reg_shelf_life_day_7;
--   g_rec_out.product_status_1_code           := g_rec_in.product_status_1_code;
   g_rec_out.num_mu_per_tray                 := g_rec_in.num_mu_per_tray;
   g_rec_out.reg_rsp                         := g_rec_in.reg_rsp;
   g_rec_out.case_selling                    := g_rec_in.case_selling;
   g_rec_out.last_updated_date               := g_date;
  Case
        when g_rec_in.product_status_code = 'A'
            then g_rec_out.product_status_code             :=1 ;
        when g_rec_in.product_status_code = 'D'
            then g_rec_out.product_status_code             :=4 ;
        when g_rec_in.product_status_code = 'N'
            then g_rec_out.product_status_code             :=14 ;
        when g_rec_in.product_status_code = 'O'
            then g_rec_out.product_status_code             :=15;
        when g_rec_in.product_status_code = 'U'
            then g_rec_out.product_status_code             :=21 ;
        when g_rec_in.product_status_code = 'X'
            then g_rec_out.product_status_code             :=24;
        when g_rec_in.product_status_code = 'Z'
            then g_rec_out.product_status_code             :=26;
        when g_rec_in.product_status_code is null
            then g_rec_out.product_status_code             :=0;
        else g_rec_out.product_status_code             :=0 ;
  end case;
  Case
        when g_rec_in.product_status_1_code = 'A'
            then g_rec_out.product_status_1_code             :=1 ;
        when g_rec_in.product_status_1_code = 'D'
            then g_rec_out.product_status_1_code             :=4 ;
        when g_rec_in.product_status_1_code = 'N'
            then g_rec_out.product_status_1_code             :=14 ;
        when g_rec_in.product_status_1_code = 'O'
            then g_rec_out.product_status_1_code             :=15;
        when g_rec_in.product_status_1_code = 'U'
            then g_rec_out.product_status_1_code             :=21 ;
        when g_rec_in.product_status_1_code = 'X'
            then g_rec_out.product_status_1_code             :=24;
        when g_rec_in.product_status_1_code = 'Z'
            then g_rec_out.product_status_1_code             :=26;
        when g_rec_in.product_status_1_code is null
            then g_rec_out.product_status_1_code             :=0;
        else g_rec_out.product_status_1_code             :=0 ;
  end case;

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_zone_group_zone_no          := g_rec_in.sk1_zone_group_zone_no;

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
       insert into rtl_zone_item_om values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no;
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
       update rtl_zone_item_om
       set    direct_delivery_ind             = a_tbl_update(i).direct_delivery_ind,
              from_loc_no                     = a_tbl_update(i).from_loc_no,
              case_mass                       = a_tbl_update(i).case_mass,
              tray_size_code                  = a_tbl_update(i).tray_size_code,
              num_units_per_tray              = a_tbl_update(i).num_units_per_tray,
              case_selling_excl_vat           = a_tbl_update(i).case_selling_excl_vat,
              case_cost                       = a_tbl_update(i).case_cost,
              product_status_code             = a_tbl_update(i).product_status_code,
              reg_rsp_excl_vat                = a_tbl_update(i).reg_rsp_excl_vat,
              cost_price                      = a_tbl_update(i).cost_price,
              num_shelf_life_days             = a_tbl_update(i).num_shelf_life_days,
              num_supplier_leadtime_days      = a_tbl_update(i).num_supplier_leadtime_days,
              banker_ind                      = a_tbl_update(i).banker_ind,
              early_delivery_ind              = a_tbl_update(i).early_delivery_ind,
              mu_pack_height_code             = a_tbl_update(i).mu_pack_height_code,
              mu_extsn_sleeve_code            = a_tbl_update(i).mu_extsn_sleeve_code,
              supplier_no                     = a_tbl_update(i).supplier_no,
              num_extra_leadtime_days         = a_tbl_update(i).num_extra_leadtime_days,
              reg_delivery_pattern_code       = a_tbl_update(i).reg_delivery_pattern_code,
              reg_shelf_life_day_1            = a_tbl_update(i).reg_shelf_life_day_1,
              reg_shelf_life_day_2            = a_tbl_update(i).reg_shelf_life_day_2,
              reg_shelf_life_day_3            = a_tbl_update(i).reg_shelf_life_day_3,
              reg_shelf_life_day_4            = a_tbl_update(i).reg_shelf_life_day_4,
              reg_shelf_life_day_5            = a_tbl_update(i).reg_shelf_life_day_5,
              reg_shelf_life_day_6            = a_tbl_update(i).reg_shelf_life_day_6,
              reg_shelf_life_day_7            = a_tbl_update(i).reg_shelf_life_day_7,
              product_status_1_code           = a_tbl_update(i).product_status_1_code,
              num_mu_per_tray                 = a_tbl_update(i).num_mu_per_tray,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              case_selling                    = a_tbl_update(i).case_selling,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_item_no                     = a_tbl_update(i).sk1_item_no  and
              sk1_zone_group_zone_no          = a_tbl_update(i).sk1_zone_group_zone_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no;
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
   from   rtl_zone_item_om
   where  sk1_item_no      = g_rec_out.sk1_item_no  and
          sk1_zone_group_zone_no  = g_rec_out.sk1_zone_group_zone_no;

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

    l_text := 'LOAD OF RTL_ZONE_ITEM_OM EX FOUNDATION STARTED AT '||
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
    open c_fnd_zone_item_om;
    fetch c_fnd_zone_item_om bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_zone_item_om bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_zone_item_om;
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
end wh_prf_corp_051u;
