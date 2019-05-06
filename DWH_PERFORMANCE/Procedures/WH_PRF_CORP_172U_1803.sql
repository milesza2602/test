--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_172U_1803
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_172U_1803" (p_forall_limit in integer,p_success out boolean,p_run_no in integer) as

--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      Alastair de Wet
--  Purpose:     Create location item day price fact table in the performance layer with
--               added value ex foundation layer location_item and a distinct list of transactions
--  Tables:      Input  -   fnd_location_item, fnd_rtl_loc_item_dy_rms_.... various
--               Output -   rtl_loc_item_dy_rms_price
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--               Design TBC, not complete at the time of development.
--  Scheduling:  This load has been designed to run after midnight in order to allow for
--               incoming transactions to be loaded to DWH and therefore uses g_yesterday
--               as the calendar_date on the target table.
--
--  Maintenance:
--  19 Feb 2009 - A. Joshua : TD-879 - Include fnd_rtl_loc_item_dy_rdf_sale in list of tables for pricing population
--
--  27 Sep 2016 - Q. Smit   : Chg-1034 - Wrong table name used for the OM replacement - fnd_loc_item_dy_ff_ord instead of fnd_RTL_loc_item_dy_ff_ord
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
g_rec_out            rtl_loc_item_dy_rms_price%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_172U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_LOC_ITEM_DY_RMS_PRICE EX FND_LOCATION_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rms_price%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_price%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_run_2 is
   with loc_item_list as
   (
   ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_pos_jv lid, dim_item di
     where  lid.last_updated_date = g_date  and
            lid.item_no           = di.item_no and
            business_unit_no      = 50)
   UNION
   ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_om_ord lid
     where  lid.last_updated_date = g_date - 1 and post_date <= g_date )
     UNION
     ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_ff_ord lid      --fnd_loc_item_dy_ff_ord
     where  lid.last_updated_date = g_date - 1 and post_date <= g_date )
   UNION
   ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_rdf_sale lid
     where  lid.last_updated_date = g_date )
   )
   select dl.sk1_location_no,
          di.sk1_item_no,
          lil.post_date as calendar_date,
          dlh.sk2_location_no,
          dih.sk2_item_no,
          fli.reg_rsp,
          (fli.reg_rsp * 100 / (100 + di.vat_rate_perc)) reg_rsp_excl_vat,
          fli.selling_uom_code,
          fli.prom_rsp,
          fli.clearance_ind,
          nvl(fli.num_units_per_tray,1) as num_units_per_tray,
          nvl(di.vat_rate_perc,0) as vat_rate_perc,
          di.standard_uom_code,
          nvl(di.static_mass,1) as static_mass,
          nvl(di.random_mass_ind,0) as random_mass_ind

   from   loc_item_list lil,
          fnd_location_item fli,
          dim_location dl,
          dim_item di,
          dim_location_hist dlh,
          dim_item_hist dih
   where  lil.location_no = dl.location_no
   and    lil.item_no     = di.item_no
   and    lil.location_no = dlh.location_no
   and    lil.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   and    lil.item_no     = dih.item_no
   and    lil.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date
   and    lil.location_no = fli.location_no
   and    lil.item_no     = fli.item_no ;

cursor c_fnd_zone_item_store is
   with loc_item_list as
   (
   ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_rms_sale lid, dim_item di
     where  lid.last_updated_date = g_date  and
            lid.item_no           = di.item_no and
            business_unit_no      = 50)
--   UNION
--   ( select distinct lid.location_no,lid.item_no,post_date
--     from   fnd_rtl_loc_item_dy_rms_stk_ch lid
--     where  lid.last_updated_date = g_date  )
   UNION
   ( select distinct lid.location_no,lid.item_no,post_date
     from   fnd_rtl_loc_item_dy_rms_stk_fd lid
     where  lid.last_updated_date = g_date )
   )
   select dl.sk1_location_no,
          di.sk1_item_no,
          lil.post_date as calendar_date,
          dlh.sk2_location_no,
          dih.sk2_item_no,
          fli.reg_rsp,
          (fli.reg_rsp * 100 / (100 + di.vat_rate_perc)) reg_rsp_excl_vat,
          fli.selling_uom_code,
          fli.prom_rsp,
          fli.clearance_ind,
          nvl(fli.num_units_per_tray,1) as num_units_per_tray,
          nvl(di.vat_rate_perc,0) as vat_rate_perc,
          di.standard_uom_code,
          nvl(di.static_mass,1) as static_mass,
          nvl(di.random_mass_ind,0) as random_mass_ind
   from   loc_item_list lil,
          fnd_location_item fli,
          dim_location dl,
          dim_item di,
          dim_location_hist dlh,
          dim_item_hist dih
   where  lil.location_no = dl.location_no
   and    lil.item_no     = di.item_no
   and    lil.location_no = dlh.location_no
   and    lil.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   and    lil.item_no     = dih.item_no
   and    lil.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date
   and    lil.location_no = fli.location_no
   and    lil.item_no     = fli.item_no ;
-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_zone_item_store%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_zone_item_store%rowtype;
a_stg_input      stg_array;


-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_location_no               := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                   := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date                 := g_rec_in.calendar_date;
   g_rec_out.sk2_location_no               := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                   := g_rec_in.sk2_item_no;
   g_rec_out.reg_rsp                       := g_rec_in.reg_rsp;
   g_rec_out.reg_rsp_excl_vat              := g_rec_in.reg_rsp_excl_vat;
   g_rec_out.selling_uom_code              := g_rec_in.selling_uom_code;
   g_rec_out.prom_rsp                      := g_rec_in.prom_rsp;
   if g_rec_in.clearance_ind = 1 then
      g_rec_out.clear_rsp                  := g_rec_in.reg_rsp;
   else
      g_rec_out.clear_rsp                  := null;
   end if;
   g_rec_out.num_units_per_tray            := g_rec_in.num_units_per_tray;
   g_rec_out.vat_rate_perc                 := g_rec_in.vat_rate_perc;

   g_rec_out.ruling_rsp                    := g_rec_out.reg_rsp;
   if g_rec_out.prom_rsp is not null then
      g_rec_out.ruling_rsp                 := g_rec_out.prom_rsp;
   end if;
   if g_rec_out.clear_rsp is not null then
      g_rec_out.ruling_rsp                 := g_rec_out.clear_rsp;
   end if;
   g_rec_out.ruling_rsp_excl_vat           := g_rec_out.ruling_rsp  * 100 / (100 + g_rec_out.vat_rate_perc);

   if g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
      g_rec_out.case_selling                  := g_rec_out.num_units_per_tray * g_rec_out.ruling_rsp * g_rec_in.static_mass;
      g_rec_out.case_selling_excl_vat         := g_rec_out.num_units_per_tray * g_rec_out.ruling_rsp_excl_vat * g_rec_in.static_mass;
   else
      g_rec_out.case_selling                  := g_rec_out.num_units_per_tray * g_rec_out.ruling_rsp;
      g_rec_out.case_selling_excl_vat         := g_rec_out.num_units_per_tray * g_rec_out.ruling_rsp_excl_vat;
   end if;


   g_rec_out.last_updated_date             := g_date;


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

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into rtl_loc_item_dy_rms_price values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
      update rtl_loc_item_dy_rms_price
      set sk1_location_no               = a_tbl_update(i).sk1_location_no,
          sk1_item_no                   = a_tbl_update(i).sk1_item_no,
          calendar_date                 = a_tbl_update(i).calendar_date,
          sk2_location_no               = a_tbl_update(i).sk2_location_no,
          sk2_item_no                   = a_tbl_update(i).sk2_item_no,
          reg_rsp                       = a_tbl_update(i).reg_rsp,
          reg_rsp_excl_vat              = a_tbl_update(i).reg_rsp_excl_vat,
          selling_uom_code              = a_tbl_update(i).selling_uom_code,
          prom_rsp                      = a_tbl_update(i).prom_rsp,
          clear_rsp                     = a_tbl_update(i).clear_rsp,
          num_units_per_tray            = a_tbl_update(i).num_units_per_tray,
          vat_rate_perc                 = a_tbl_update(i).vat_rate_perc,
          ruling_rsp                    = a_tbl_update(i).ruling_rsp,
          ruling_rsp_excl_vat           = a_tbl_update(i).ruling_rsp_excl_vat,
          case_selling                  = a_tbl_update(i).case_selling,
          case_selling_excl_vat         = a_tbl_update(i).case_selling_excl_vat,
          last_updated_date             = a_tbl_update(i).last_updated_date
      where sk1_location_no       =  a_tbl_update(i).sk1_location_no
      and   sk1_item_no           =  a_tbl_update(i).sk1_item_no
      and   calendar_date         =  a_tbl_update(i).calendar_date
      and   (reg_rsp              <> a_tbl_update(i).reg_rsp or
             selling_uom_code     <> a_tbl_update(i).selling_uom_code or
             prom_rsp             <> a_tbl_update(i).prom_rsp or
             clear_rsp            <> a_tbl_update(i).clear_rsp or
             num_units_per_tray   <> a_tbl_update(i).num_units_per_tray or
             vat_rate_perc        <> a_tbl_update(i).vat_rate_perc);

      g_recs_updated := g_recs_updated + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).calendar_date;
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
   into g_count
   from rtl_loc_item_dy_rms_price
   where sk1_location_no = g_rec_out.sk1_location_no
   and   sk1_item_no = g_rec_out.sk1_item_no
   and   calendar_date = g_rec_out.calendar_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
-- QC1095 Foods only and updates removed with program split into 2 runs 1 for RMS data and 1 for the rest later in day.
-- Updates should not be required as prices should not change BUT!!! if they do if if some need for updates is req then it is easy to put it back
-- by removing comment below. Could have excluded in driving cursor for better efficiency but that would be more difficult to change later if req..

   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count := a_count + 1;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
   end if;

--   a_count := a_count + 1;

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
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday||'     Run number:- '||p_run_no);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOC_ITEM_DY_RMS_PRICE EX FND_LOCATION_ITEM STARTED AT '||
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
  if p_run_no = 1 then
    open c_fnd_zone_item_store;
    fetch c_fnd_zone_item_store bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_zone_item_store bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_zone_item_store;
  else
    open c_run_2;
    fetch c_run_2 bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_run_2 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_run_2;
  end if;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

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

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
end wh_prf_corp_172u_1803;
