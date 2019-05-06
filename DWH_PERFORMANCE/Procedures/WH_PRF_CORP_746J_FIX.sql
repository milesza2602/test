--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_746J_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_746J_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JAN 2009
--  Author:      Alastair de Wet
--  Purpose:     Load zone_item_dy_om_ord table in performance layer
--               with input ex zone_item_supp_po_plan table from performance layer (Foods Only).
--  Tables:      Input  - fnd_zone_item_supp_om_po_plan
--               Output - rtl_zone_item_dy_om_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  03 Mar 2018   VAT 2018 (increase) amendements as required                                       REF03Mar2018
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
g_rec_out            rtl_zone_item_dy_om_ord%rowtype;
g_day                dim_calendar.fin_day_no%type                        := 1;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_today              date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate) - 7;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_746J_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP ZONE ITEM DATA EX FOUNDATION (JDA)';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_om_ord%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_om_ord%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
cursor c_fnd_zone_item_supp_om_po_pl is
  select /*+ full (zis) parallel (zis,8) full (zi) parallel (zi,8) full (fi_vr) parallel (fi_vr,8) */
          sum(nvl(day01_cases,0)) day01_cases,
          sum(nvl(day02_cases,0)) day02_cases,
          sum(nvl(day03_cases,0)) day03_cases,
          sum(nvl(day04_cases,0)) day04_cases,
          sum(nvl(day05_cases,0)) day05_cases,
          sum(nvl(day06_cases,0)) day06_cases,
          sum(nvl(day07_cases,0)) day07_cases,
          sum(nvl(day08_cases,0)) day08_cases,
          sum(nvl(day09_cases,0)) day09_cases,
          sum(nvl(day10_cases,0)) day10_cases,
          sum(nvl(day11_cases,0)) day11_cases,
          sum(nvl(day12_cases,0)) day12_cases,
          sum(nvl(day13_cases,0)) day13_cases,
          di.sk1_item_no,
          dz.sk1_zone_group_zone_no,
          max(dih.sk2_item_no) sk2_item_no,
          max(di.standard_uom_code) standard_uom_code,
          max(nvl(di.static_mass,1)) static_mass,
          max(nvl(di.random_mass_ind,0)) random_mass_ind,
          max(NVL(fi_vr.vat_rate_perc, di.vat_rate_perc))  vat_rate_perc,       --VAT rate change
          max(nvl(zi.num_units_per_tray,1)) num_units_per_tray ,
          max(nvl(prc.reg_rsp_excl_vat,0)) reg_rsp_excl_vat     --replaced as its not found on fnd_zone_item
  
   from   fnd_zone_item_supp_ff_po_plan zis
   join dim_item di       on zis.item_no = di.item_no
   join dim_item_hist dih on zis.item_no = dih.item_no
   
   left outer join fnd_zone_item zi on zis.zone_no       = zi.zone_no
                                   and zis.zone_group_no = zi.zone_group_no
                                   and zis.item_no       = zi.item_no
          
   join dim_zone dz on zis.zone_no       = dz.zone_no
                   and zis.zone_group_no = dz.zone_group_no
                     
   join dim_location dl on zis.to_loc_no = dl.location_no                            --JDA

   join rtl_loc_item_dy_rms_price prc on dl.sk1_location_no = prc.sk1_location_no    --JDA
                                     and di.sk1_item_no     = prc.sk1_item_no
                                     and g_date             = prc.calendar_date
                                     
   left outer join FND_ITEM_VAT_RATE fi_vr  on zis.item_no       = fi_vr.item_no                               --VAT rate change                       
                                            and dl.vat_region_no = fi_vr.vat_region_no                         --VAT rate change                                                      
                                            and g_date between fi_vr.active_from_date and fi_vr.active_to_date  --VAT rate change

  where g_date         between dih.sk2_active_from_date and dih.sk2_active_to_date 
  group by di.sk1_item_no, dz.sk1_zone_group_zone_no;

g_rec_in             c_fnd_zone_item_supp_om_po_pl%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_zone_item_supp_om_po_pl%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_zone_group_zone_no          := g_rec_in.sk1_zone_group_zone_no;
   g_rec_out.post_date                       := g_today;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   if g_day = 1 then
      g_rec_out.st_order_cases_1  := g_rec_in.day01_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day02_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day03_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day04_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day05_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day07_cases;
   end if;
   if g_day = 2 then
      g_rec_out.st_order_cases_1  := g_rec_in.day02_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day03_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day04_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day05_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day08_cases;
   end if;
      if g_day = 3 then
      g_rec_out.st_order_cases_1  := g_rec_in.day03_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day04_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day05_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day08_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day09_cases;
   end if;
      if g_day = 4 then
      g_rec_out.st_order_cases_1  := g_rec_in.day04_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day05_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day08_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day09_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day10_cases;
   end if;
      if g_day = 5 then
      g_rec_out.st_order_cases_1  := g_rec_in.day05_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day08_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day09_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day10_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day11_cases;
   end if;
      if g_day = 6 then
      g_rec_out.st_order_cases_1  := g_rec_in.day06_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day08_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day09_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day10_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day11_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day12_cases;
   end if;
      if g_day = 7 then
      g_rec_out.st_order_cases_1  := g_rec_in.day07_cases;
      g_rec_out.st_order_cases_2  := g_rec_in.day08_cases;
      g_rec_out.st_order_cases_3  := g_rec_in.day09_cases;
      g_rec_out.st_order_cases_4  := g_rec_in.day10_cases;
      g_rec_out.st_order_cases_5  := g_rec_in.day11_cases;
      g_rec_out.st_order_cases_6  := g_rec_in.day12_cases;
      g_rec_out.st_order_cases_7  := g_rec_in.day13_cases;
   end if;
   g_rec_out.last_updated_date               := g_date;


/*
   begin
      select wac,reg_rsp_excl_vat,num_units_per_tray
      into   g_wac,g_reg_rsp_excl_vat,g_num_units_per_tray
      from   rtl_loc_item_dy_rms_price
      where  sk1_item_no     =  g_rec_out.sk1_item_no and
             sk1_location_no =  g_rec_out.sk1_location_no and
             calendar_date   =  g_date;
      exception
      when no_data_found then
             g_wac                := 0;
             g_reg_rsp_excl_vat   := 0;
             g_num_units_per_tray := 1;
   end;
*/



   if g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
      g_rec_out.st_order_selling_1            := g_rec_out.st_order_cases_1 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_2            := g_rec_out.st_order_cases_2 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_3            := g_rec_out.st_order_cases_3 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_4            := g_rec_out.st_order_cases_4 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_5            := g_rec_out.st_order_cases_5 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_6            := g_rec_out.st_order_cases_6 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.st_order_selling_7            := g_rec_out.st_order_cases_7 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
   else
      g_rec_out.st_order_selling_1            := g_rec_out.st_order_cases_1 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_2            := g_rec_out.st_order_cases_2 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_3            := g_rec_out.st_order_cases_3 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_4            := g_rec_out.st_order_cases_4 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_5            := g_rec_out.st_order_cases_5 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_6            := g_rec_out.st_order_cases_6 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
      g_rec_out.st_order_selling_7            := g_rec_out.st_order_cases_7 * g_rec_in.num_units_per_tray * g_rec_in.reg_rsp_excl_vat ;
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
       insert into rtl_zone_item_dy_om_ord values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no||
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
       update rtl_zone_item_dy_om_ord
       set    st_order_cases_1                = a_tbl_update(i).st_order_cases_1,
              st_order_cases_2                = a_tbl_update(i).st_order_cases_2,
              st_order_cases_3                = a_tbl_update(i).st_order_cases_3,
              st_order_cases_4                = a_tbl_update(i).st_order_cases_4,
              st_order_cases_5                = a_tbl_update(i).st_order_cases_5,
              st_order_cases_6                = a_tbl_update(i).st_order_cases_6,
              st_order_cases_7                = a_tbl_update(i).st_order_cases_7,
              st_order_selling_1              = a_tbl_update(i).st_order_selling_1,
              st_order_selling_2              = a_tbl_update(i).st_order_selling_2,
              st_order_selling_3              = a_tbl_update(i).st_order_selling_3,
              st_order_selling_4              = a_tbl_update(i).st_order_selling_4,
              st_order_selling_5              = a_tbl_update(i).st_order_selling_5,
              st_order_selling_6              = a_tbl_update(i).st_order_selling_6,
              st_order_selling_7              = a_tbl_update(i).st_order_selling_7,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_zone_group_zone_no          = a_tbl_update(i).sk1_zone_group_zone_no      and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no    and
              post_date                       = a_tbl_update(i).post_date ;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
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
   from   rtl_zone_item_dy_om_ord
   where  sk1_zone_group_zone_no  = g_rec_out.sk1_zone_group_zone_no      and
          sk1_item_no             = g_rec_out.sk1_item_no    and
          post_date               = g_rec_out.post_date;

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
    l_text := 'LOAD rtl_zone_item_dy_om_ord EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '6 aug 16'; --> remove
    g_today := g_date + 1;
    select fin_day_no
    into   g_day
    from   dim_calendar
    where  calendar_date = g_today;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_zone_item_supp_om_po_pl;
    fetch c_fnd_zone_item_supp_om_po_pl bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_zone_item_supp_om_po_pl bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_zone_item_supp_om_po_pl;
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

end wh_prf_corp_746j_fix;
