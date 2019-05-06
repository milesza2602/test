--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_762U_Q
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_762U_Q" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2009
--  Author:      M Munnik
--  Purpose:     Update DC PLANNING data to OM fact table in the performance layer
--               with input ex OM fnd_loc_item_jdaff_wh_plan table from foundation layer.
--               On a Monday, week 1 on the input table belongs to previous week.
--               Therefore, on Mondays, only week 2 and week 3 are updated to the output table.
--  Tables:      Input  - fnd_loc_item_jdaff_wh_plan
--               Output - rtl_loc_item_dy_wh_plan
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
g_recs_deleted       integer       :=  0;
g_recs               integer       :=  0;
g_cases              rtl_loc_item_dy_wh_plan.dc_plan_store_cases%type;
g_rec_out            rtl_loc_item_dy_wh_plan%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_762U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_wh_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_wh_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_jda_wh_plan is
   select   dl.sk1_location_no, di.sk1_item_no, dc.calendar_date, dlh.sk2_location_no, dih.sk2_item_no,
            max(owp.sysdata) dc_plan_store_cases, trunc(sysdate) last_updated_date
   from
   (select  location_no,
            item_no,
            (case (to_number(substr(syscol,6,1))) when 1 then g_year1 when 2 then g_year2 else g_year3 end) yearno,
            (case (to_number(substr(syscol,6,1))) when 1 then g_week1 when 2 then g_week2 else g_week3 end) weekno,
            to_number(substr(syscol,12,1)) dayno,
            syscol,
            sysdata
   from     fnd_loc_item_jdaff_wh_plan
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
                                                  week_3_day_7_cases))) owp
   join     dim_calendar dc         on  owp.yearno      =  dc.fin_year_no
                                    and owp.weekno      =  dc.fin_week_no
                                    and owp.dayno       =  dc.fin_day_no
   join     dim_location dl         on  owp.location_no =  dl.location_no
   join     dim_location_hist dlh   on  owp.location_no =  dlh.location_no
                                    and dc.calendar_date   between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   join     dim_item di             on  owp.item_no     =  di.item_no
   join     dim_item_hist dih       on  owp.item_no     =  dih.item_no
                                    and dc.calendar_date   between dih.sk2_active_from_date and dih.sk2_active_to_date
   where    dc.calendar_date                               between g_start_date and g_end_date
   group by dl.sk1_location_no, di.sk1_item_no, dc.calendar_date, dlh.sk2_location_no, dih.sk2_item_no
   order by dc.calendar_date, dl.sk1_location_no, di.sk1_item_no;

-- This procedure does not select only where last_updated_date = g_date, because fnd_loc_item_jdaff_wh_plan
-- gets fully refreshed every day.

-- For input bulk collect --
type stg_array is table of c_jda_wh_plan%rowtype;
a_stg_input          stg_array;
g_rec_in             c_jda_wh_plan%rowtype;

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
       insert into rtl_loc_item_dy_wh_plan values a_tbl_insert(i);

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
       update rtl_loc_item_dy_wh_plan
       set    row                    =  a_tbl_update(i)
       where  calendar_date          =  a_tbl_update(i).calendar_date
       and    sk1_location_no        =  a_tbl_update(i).sk1_location_no
       and    sk1_item_no            =  a_tbl_update(i).sk1_item_no;

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
   select count(1), sum(dc_plan_store_cases)
   into   g_count,  g_cases
   from   rtl_loc_item_dy_wh_plan
   where  calendar_date    = g_rec_out.calendar_date
   and    sk1_location_no  = g_rec_out.sk1_location_no
   and    sk1_item_no      = g_rec_out.sk1_item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count := a_count + 1;
   else
      if g_cases <> g_rec_out.dc_plan_store_cases then
         a_count_u               := a_count_u + 1;
         a_tbl_update(a_count_u) := g_rec_out;
         a_count := a_count + 1;
      end if;
   end if;

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
    l_text := 'LOAD OF rtl_loc_item_dy_wh_plan EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;
    if g_today_day = 1 then
       g_end_date   := g_start_date + 13;
       g_year2      := g_year1;
       g_week2      := g_week1;
       select fin_year_no, fin_week_no
       into   g_year1,     g_week1
       from   dim_calendar
       where  calendar_date = g_start_date - 7;
    else
       g_end_date := g_start_date + 20;
       select fin_year_no, fin_week_no
       into   g_year2,     g_week2
       from   dim_calendar
       where  calendar_date = g_start_date + 7;
    end if;
    select fin_year_no, fin_week_no
    into   g_year3,     g_week3
    from   dim_calendar
    where  calendar_date = g_end_date;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_jda_wh_plan;
    fetch c_jda_wh_plan bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 1000000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_jda_wh_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jda_wh_plan;
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

end wh_prf_corp_762u_q;
