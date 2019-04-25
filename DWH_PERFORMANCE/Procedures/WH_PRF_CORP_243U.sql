--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_243U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_243U" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2012
--  Author:      Quentin Smit
--  Purpose:     Roll ROS data from day to week
--  Tables:      Input  -   rtl_loc_item_dy_rate_of_sale
--               Output -   rtl_loc_item_wk_rate_of_sale
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_rec_out            rtl_loc_item_wk_rate_of_sale%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_243U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL RATE OF SALE FROM DAY TO WEEK FOR 2 WEEKS BACK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_fin_year_no        number;
l_fin_week_no        number;
l_last_wk_start_date date;
l_last_wk_end_date   date;
l_last_week_no       number;
l_last_wk_fin_year_no number;
l_max_avg_wk         number(18,7) :=0 ;
l_max_sum_wk         number(18,7) :=0 ;
l_last_item_no      rtl_loc_item_wk_rate_of_sale.sk1_item_no%type :=0;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_wk_rate_of_sale%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_wk_rate_of_sale%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_week1             number;
l_week2             number;
l_year1             number;
l_year2             number;
l_wk1_date1         date;
l_wk1_date2         date;
l_wk1_date3         date;
l_wk1_date4         date;
l_wk1_date5         date;
l_wk1_date6         date;
l_wk1_date7         date;
l_wk2_date1         date;
l_wk2_date2         date;
l_wk2_date3         date;
l_wk2_date4         date;
l_wk2_date5         date;
l_wk2_date6         date;
l_wk2_date7         date;

l_current_proc_date date;
l_calc_start_date   date;
l_calc_end_date     date;


cursor c_rate_of_sale_wk is
   select  f.calendar_date, sk1_location_no, sk1_item_no,
  sum(units_per_day)/7 units_per_week,
  avg(units_per_day) avg_units_per_day
 from rtl_loc_item_dy_rate_of_sale f
   where f.calendar_date between l_last_wk_start_date and l_last_wk_end_date
 group by f.calendar_date, sk1_location_no, sk1_item_no
 order by f.calendar_date, sk1_item_no, units_per_week desc, avg_units_per_day desc;

g_rec_in      c_rate_of_sale_wk%rowtype;

-- For input bulk collect --
type stg_array is table of c_rate_of_sale_wk%rowtype;
a_rate_of_sale_wk      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   if g_rec_in.sk1_item_no = l_last_item_no then
      if g_rec_in.units_per_week > l_max_avg_wk then
         l_max_avg_wk := g_rec_in.units_per_week;
      else
        g_rec_in.units_per_week := l_max_avg_wk;

      end if;

      if g_rec_in.avg_units_per_day > l_max_sum_wk then
         l_max_sum_wk := g_rec_in.avg_units_per_day;
      else
         g_rec_in.avg_units_per_day := l_max_sum_wk;
      end if;

   else
      l_last_item_no := g_rec_in.sk1_item_no;
      l_max_avg_wk := g_rec_in.units_per_week;
      l_max_sum_wk := g_rec_in.avg_units_per_day;
   end if;
  /*
   if g_rec_in.calendar_date <> l_current_proc_date then
  l_current_proc_date := g_rec_in.calendar_date;
      select fin_year_no, fin_week_no
        into l_fin_year_no, l_fin_week_no
        from dim_calendar
       where calendar_date = l_current_proc_date;
      l_text := 'Current date being processed = ' || l_current_proc_date || ' year = ' || l_fin_year_no || ' week = ' || l_fin_week_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
  */
   g_rec_out.sk1_location_no                := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                    := g_rec_in.sk1_item_no;
   g_rec_out.fin_year_no                    := l_fin_year_no;
   g_rec_out.fin_week_no                    := l_fin_week_no;
   g_rec_out.units_per_week                 := g_rec_in.units_per_week;
   g_rec_out.avg_units_per_day              := g_rec_in.avg_units_per_day;
   g_rec_out.last_updated_date              := g_date;

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
      insert into rtl_loc_item_wk_rate_of_sale values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
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
      update rtl_loc_item_wk_rate_of_sale
      set    units_per_week            = a_tbl_update(i).units_per_week,
             avg_units_per_day            = a_tbl_update(i).avg_units_per_day,
             last_updated_date              = a_tbl_update(i).last_updated_date
      where  sk1_location_no                = a_tbl_update(i).sk1_location_no
        and  sk1_item_no                    = a_tbl_update(i).sk1_item_no
        and  fin_year_no                    = a_tbl_update(i).fin_year_no
        and  fin_week_no                    = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no;
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
     from   rtl_loc_item_wk_rate_of_sale
    where sk1_location_no     = g_rec_out.sk1_location_no
      and sk1_item_no         = g_rec_out.sk1_item_no
      and fin_year_no         = g_rec_out.fin_year_no
      and fin_week_no         = g_rec_out.fin_week_no;

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
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLL OF RTL_LOC_ITEM_WK_RATE_OF_SALE STARTED AT '||
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

    select today_fin_year_no,
           today_fin_week_no,
           last_wk_fin_week_no,            -- last_completed_week
           last_wk_fin_year_no,            -- last week's fin year
           last_wk_start_date,
           last_wk_end_date
      into l_fin_year_no,
           l_fin_week_no,
           l_last_week_no,
           l_last_wk_fin_year_no,
           l_last_wk_start_date,
           l_last_wk_end_date
      from dim_control_report;

      l_calc_start_date := l_last_wk_start_date;
      l_calc_end_date   := l_last_wk_end_date;

       select calendar_date
         into l_last_wk_start_date
         from dim_calendar
         where calendar_date = to_date(l_calc_start_date) - 7;

       select calendar_date
         into l_last_wk_end_date
         from dim_calendar
         where calendar_date = to_date(l_calc_end_date) - 7;

    select fin_year_no, fin_week_no
      into l_fin_year_no, l_fin_week_no
      from dim_calendar
      where calendar_date = l_last_wk_start_date;

      l_text := 'Weeking being processed is : - '|| l_fin_week_no ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      l_text := 'processing start date = '|| l_last_wk_start_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'processing end date = '|| l_last_wk_end_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_rate_of_sale_wk;
    fetch c_rate_of_sale_wk bulk collect into a_rate_of_sale_wk limit g_forall_limit;
    while a_rate_of_sale_wk.count > 0
    loop
      for i in 1 .. a_rate_of_sale_wk.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 200000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_rate_of_sale_wk(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_rate_of_sale_wk bulk collect into a_rate_of_sale_wk limit g_forall_limit;
    end loop;
    close c_rate_of_sale_wk;
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
end wh_prf_corp_243u;
