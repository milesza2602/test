--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_742J
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_742J" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2009
--  Author:      M Munnik
--  Purpose:     Update DC PLAN PO data at zone item suppl level to OM fact table in the performance layer
--               with input ex foundation layer.
--               Three weeks' data on each record on foundation layer
--               must be un-pivotted to result in every day of the three weeks, on a seperate record.
--  Tables:      Input  - fnd_zone_item_supp_om_po_plan
--               Output - rtl_zone_item_dy_supp_po_plan
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
g_rec_out            rtl_zone_item_dy_supp_po_plan%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_742J';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ZONE/ITEM/SUPP PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_zone_item_supp_po_plan is
   select   dz.sk1_zone_group_zone_no, di.sk1_item_no, ds.sk1_supplier_no, dc.calendar_date, dih.sk2_item_no,
            dlf.sk1_location_no sk1_from_loc_no, dlt.sk1_location_no sk1_to_loc_no,
            dlhf.sk2_location_no sk2_from_loc_no, dlht.sk2_location_no sk2_to_loc_no,
            sop.sysdata dc_supp_inbound_cases, trunc(sysdate) last_updated_date,
            (sop.sysdata * nvl(zi.case_selling_excl_vat,0)) dc_supp_inbound_selling
   from
   (select  zone_group_no, zone_no, item_no, supplier_no, from_loc_no, to_loc_no,
            (case (to_number(substr(syscol,6,1))) when 1 then g_year1 when 2 then g_year2 else g_year3 end) yearno,
            (case (to_number(substr(syscol,6,1))) when 1 then g_week1 when 2 then g_week2 else g_week3 end) weekno,
            to_number(substr(syscol,12,1)) dayno,
            syscol,
            sysdata
   from     fnd_zone_item_supp_om_po_plan
   unpivot  include nulls (sysdata for syscol in (week_1_day_1_cases,
                                                  week_1_day_2_cases,
                                                  week_1_day_3_cases,
                                                  week_1_day_4_cases,
                                                  week_1_day_5_cases,
                                                  week_1_day_6_cases,
                                                  week_1_day_7_cases,
                                                  week_2_day_1_cases,
                                                  week_2_day_2_cases,
                                                  week_2_day_3_cases,
                                                  week_2_day_4_cases,
                                                  week_2_day_5_cases,
                                                  week_2_day_6_cases,
                                                  week_2_day_7_cases,
                                                  week_3_day_1_cases,
                                                  week_3_day_2_cases,
                                                  week_3_day_3_cases,
                                                  week_3_day_4_cases,
                                                  week_3_day_5_cases,
                                                  week_3_day_6_cases,
                                                  week_3_day_7_cases))) sop
   join     dim_calendar dc         on  sop.yearno        = dc.fin_year_no
                                    and sop.weekno        = dc.fin_week_no
                                    and sop.dayno         = dc.fin_day_no
   left outer join
            fnd_zone_item zi     on  sop.zone_group_no    = zi.zone_group_no
                                    and sop.zone_no       = zi.zone_no
                                    and sop.item_no       = zi.item_no

   join     dim_zone dz             on  sop.zone_group_no = dz.zone_group_no
                                    and sop.zone_no       = dz.zone_no
   join     dim_item di             on  sop.item_no       = di.item_no
   join     dim_item_hist dih       on  sop.item_no       = dih.item_no
                                    and dc.calendar_date  between dih.sk2_active_from_date and dih.sk2_active_to_date
   join     dim_supplier ds         on  sop.supplier_no   = ds.supplier_no
   join     dim_location dlf        on  sop.from_loc_no   = dlf.location_no
   join     dim_location_hist dlhf  on  sop.from_loc_no   = dlhf.location_no
                                    and dc.calendar_date  between dlhf.sk2_active_from_date and dlhf.sk2_active_to_date
   join     dim_location dlt        on  sop.to_loc_no     = dlt.location_no
   join     dim_location_hist dlht  on  sop.to_loc_no     = dlht.location_no
                                    and dc.calendar_date  between dlht.sk2_active_from_date and dlht.sk2_active_to_date
   order by dz.sk1_zone_group_zone_no, di.sk1_item_no, ds.sk1_supplier_no, dc.calendar_date;

-- This procedure does not select only where last_updated_date = g_date, because fnd_zone_item_supp_om_po_plan
-- gets fully refreshed every day.

-- For input bulk collect --
type stg_array is table of c_zone_item_supp_po_plan%rowtype;
a_stg_input          stg_array;
g_rec_in             c_zone_item_supp_po_plan%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                       := g_rec_in;
   g_rec_out.last_updated_date     := g_date;

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
       insert into rtl_zone_item_dy_supp_po_plan values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no||
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
       update rtl_zone_item_dy_supp_po_plan
       set    row                    = a_tbl_update(i)
       where  sk1_zone_group_zone_no = a_tbl_update(i).sk1_zone_group_zone_no
       and    sk1_item_no            = a_tbl_update(i).sk1_item_no
       and    sk1_supplier_no        = a_tbl_update(i).sk1_supplier_no
       and    calendar_date          = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
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
   g_count :=0;

-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_zone_item_dy_supp_po_plan
   where  sk1_zone_group_zone_no = g_rec_out.sk1_zone_group_zone_no
   and    sk1_item_no            = g_rec_out.sk1_item_no
   and    sk1_supplier_no        = g_rec_out.sk1_supplier_no
   and    calendar_date          = g_rec_out.calendar_date;

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
    l_text := 'LOAD OF rtl_zone_item_dy_supp_po_plan EX JDA FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    select this_week_start_date, fin_year_no, fin_week_no
    into   g_start_date,         g_year1,     g_week1
    from   dim_calendar
    where  calendar_date = g_date + 1;

    g_end_date := g_start_date + 20;

    select fin_year_no, fin_week_no
    into   g_year2,     g_week2
    from   dim_calendar
    where  calendar_date = g_start_date + 7;

    select fin_year_no, fin_week_no
    into   g_year3,     g_week3
    from   dim_calendar
    where  calendar_date = g_end_date;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_zone_item_supp_po_plan;
    fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
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
       fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_zone_item_supp_po_plan;
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

end wh_prf_corp_742j;
