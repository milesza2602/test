--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_007U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_007U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create calendar week dim table in the performance layer
--               with input ex dim_calendar table from performance layer.
--  Tables:      Input  - dim_calendar
--               Output - dim_calendar_wk
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dim_calendar_wk%rowtype;

g_found              boolean;

g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_007U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE DIM CALENDAR to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_calendar_wk%rowtype index by binary_integer;
type tbl_array_u is table of dim_calendar_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_dim_calendar is
   select dc.*
   from   dim_calendar dc
   where  dc.fin_day_no         =  7;


g_rec_in             c_dim_calendar%rowtype;
-- For input bulk collect --
type stg_array is table of c_dim_calendar%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin





   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.fin_month_no                    := g_rec_in.fin_month_no;
   g_rec_out.cal_year_no                     := g_rec_in.cal_year_no;
   g_rec_out.fin_half_no                     := g_rec_in.fin_half_no;
   g_rec_out.fin_quarter_no                  := g_rec_in.fin_quarter_no;
   g_rec_out.ly_fin_year_no                  := g_rec_in.ly_fin_year_no;
   g_rec_out.ly_fin_week_no                  := g_rec_in.ly_fin_week_no;
   g_rec_out.cal_year_month_no               := g_rec_in.cal_year_month_no;
   g_rec_out.this_mn_start_date              := g_rec_in.this_mn_start_date;
   g_rec_out.this_mn_end_date                := g_rec_in.this_mn_end_date;
   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.this_week_end_date              := g_rec_in.this_week_end_date;
   g_rec_out.season_no                       := g_rec_in.season_no;
   g_rec_out.season_name                     := g_rec_in.season_name;
   g_rec_out.month_name                      := g_rec_in.month_name;
   g_rec_out.month_short_name                := g_rec_in.month_short_name;
   g_rec_out.fin_week_code                   := g_rec_in.fin_week_code;
   g_rec_out.fin_week_short_desc             := g_rec_in.fin_week_short_desc;
   g_rec_out.fin_week_long_desc              := g_rec_in.fin_week_long_desc;
   g_rec_out.num_fin_week_timespan_days      := g_rec_in.num_fin_week_timespan_days;
   g_rec_out.fin_week_end_date               := g_rec_in.fin_week_end_date;
   g_rec_out.fin_month_code                  := g_rec_in.fin_month_code;
   g_rec_out.fin_month_short_desc            := g_rec_in.fin_month_short_desc;
   g_rec_out.fin_month_long_desc             := g_rec_in.fin_month_long_desc;
   g_rec_out.num_fin_month_timespan_days     := g_rec_in.num_fin_month_timespan_days;
   g_rec_out.fin_month_end_date              := g_rec_in.fin_month_end_date;
   g_rec_out.fin_quarter_code                := g_rec_in.fin_quarter_code;
   g_rec_out.fin_quarter_short_desc          := g_rec_in.fin_quarter_short_desc;
   g_rec_out.fin_quarter_long_desc           := g_rec_in.fin_quarter_long_desc;
   g_rec_out.num_fin_quarter_timespan_days   := g_rec_in.num_fin_quarter_timespan_days;
   g_rec_out.fin_quarter_end_date            := g_rec_in.fin_quarter_end_date;
   g_rec_out.fin_half_code                   := g_rec_in.fin_half_code;
   g_rec_out.fin_half_short_desc             := g_rec_in.fin_half_short_desc;
   g_rec_out.fin_half_long_desc              := g_rec_in.fin_half_long_desc;
   g_rec_out.num_fin_half_timespan_days      := g_rec_in.num_fin_half_timespan_days;
   g_rec_out.fin_half_end_date               := g_rec_in.fin_half_end_date;
   g_rec_out.fin_year_code                   := g_rec_in.fin_year_code;
   g_rec_out.fin_year_short_desc             := g_rec_in.fin_year_short_desc;
   g_rec_out.fin_year_long_desc              := g_rec_in.fin_year_long_desc;
   g_rec_out.num_fin_year_timespan_days      := g_rec_in.num_fin_year_timespan_days;
   g_rec_out.fin_year_end_date               := g_rec_in.fin_year_end_date;
   g_rec_out.season_code                     := g_rec_in.season_code;
   g_rec_out.season_short_desc               := g_rec_in.season_short_desc;
   g_rec_out.season_long_desc                := g_rec_in.season_long_desc;
   g_rec_out.num_season_timespan_days        := g_rec_in.num_season_timespan_days;
   g_rec_out.season_start_date               := g_rec_in.season_start_date;
   g_rec_out.season_end_date                 := g_rec_in.season_end_date;
   g_rec_out.total                           := g_rec_in.total;
   g_rec_out.total_short_desc                := g_rec_in.total_short_desc;
   g_rec_out.total_long_desc                 := g_rec_in.total_long_desc;
   g_rec_out.num_total_timespan_days         := g_rec_in.num_total_timespan_days;
   g_rec_out.total_end_date                  := g_rec_in.total_end_date;
   g_rec_out.ly_fin_week_code                := g_rec_in.ly_fin_week_code;
   g_rec_out.order_by_seq_no                 := g_rec_in.order_by_seq_no;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.fin_half_season_long_desc       := g_rec_in.fin_half_season_long_desc ;
   g_rec_out.completed_fin_day_ind           := g_rec_in.completed_fin_day_ind ;
   g_rec_out.completed_fin_week_ind          := g_rec_in.completed_fin_week_ind ;
   g_rec_out.completed_fin_month_ind         := g_rec_in.completed_fin_month_ind ;
   g_rec_out.completed_fin_quarter_ind       := g_rec_in.completed_fin_quarter_ind ;
   g_rec_out.completed_fin_half_ind          := g_rec_in.completed_fin_half_ind ;
   g_rec_out.completed_fin_year_ind          := g_rec_in.completed_fin_year_ind ;
   g_rec_out.completed_cal_year_ind          := g_rec_in.completed_cal_year_ind ;
   g_rec_out.completed_season_ind            := g_rec_in.completed_season_ind ;
   g_rec_out.cal_month_short_desc            := g_rec_in.cal_month_short_desc;
   g_rec_out.fin_half_season_short_desc      := g_rec_in.fin_half_season_short_desc;

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
       insert into dim_calendar_wk values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
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
       update  dim_calendar_wk
       set     fin_month_no                    = a_tbl_update(i).fin_month_no,
               cal_year_no                     = a_tbl_update(i).cal_year_no,
               fin_half_no                     = a_tbl_update(i).fin_half_no,
               fin_quarter_no                  = a_tbl_update(i).fin_quarter_no,
               ly_fin_year_no                  = a_tbl_update(i).ly_fin_year_no,
               ly_fin_week_no                  = a_tbl_update(i).ly_fin_week_no,
               cal_year_month_no               = a_tbl_update(i).cal_year_month_no,
               this_mn_start_date              = a_tbl_update(i).this_mn_start_date,
               this_mn_end_date                = a_tbl_update(i).this_mn_end_date,
               this_week_start_date            = a_tbl_update(i).this_week_start_date,
               this_week_end_date              = a_tbl_update(i).this_week_end_date,
               season_no                       = a_tbl_update(i).season_no,
               season_name                     = a_tbl_update(i).season_name,
               month_name                      = a_tbl_update(i).month_name,
               month_short_name                = a_tbl_update(i).month_short_name,
               fin_week_code                   = a_tbl_update(i).fin_week_code,
               fin_week_short_desc             = a_tbl_update(i).fin_week_short_desc,
               fin_week_long_desc              = a_tbl_update(i).fin_week_long_desc,
               num_fin_week_timespan_days      = a_tbl_update(i).num_fin_week_timespan_days,
               fin_week_end_date               = a_tbl_update(i).fin_week_end_date,
               fin_month_code                  = a_tbl_update(i).fin_month_code,
               fin_month_short_desc            = a_tbl_update(i).fin_month_short_desc,
               fin_month_long_desc             = a_tbl_update(i).fin_month_long_desc,
               num_fin_month_timespan_days     = a_tbl_update(i).num_fin_month_timespan_days,
               fin_month_end_date              = a_tbl_update(i).fin_month_end_date,
               fin_quarter_code                = a_tbl_update(i).fin_quarter_code,
               fin_quarter_short_desc          = a_tbl_update(i).fin_quarter_short_desc,
               fin_quarter_long_desc           = a_tbl_update(i).fin_quarter_long_desc,
               num_fin_quarter_timespan_days   = a_tbl_update(i).num_fin_quarter_timespan_days,
               fin_quarter_end_date            = a_tbl_update(i).fin_quarter_end_date,
               fin_half_code                   = a_tbl_update(i).fin_half_code,
               fin_half_short_desc             = a_tbl_update(i).fin_half_short_desc,
               fin_half_long_desc              = a_tbl_update(i).fin_half_long_desc,
               num_fin_half_timespan_days      = a_tbl_update(i).num_fin_half_timespan_days,
               fin_half_end_date               = a_tbl_update(i).fin_half_end_date,
               fin_year_code                   = a_tbl_update(i).fin_year_code,
               fin_year_short_desc             = a_tbl_update(i).fin_year_short_desc,
               fin_year_long_desc              = a_tbl_update(i).fin_year_long_desc,
               num_fin_year_timespan_days      = a_tbl_update(i).num_fin_year_timespan_days,
               fin_year_end_date               = a_tbl_update(i).fin_year_end_date,
               season_code                     = a_tbl_update(i).season_code,
               season_short_desc               = a_tbl_update(i).season_short_desc,
               season_long_desc                = a_tbl_update(i).season_long_desc,
               num_season_timespan_days        = a_tbl_update(i).num_season_timespan_days,
               season_start_date               = a_tbl_update(i).season_start_date,
               season_end_date                 = a_tbl_update(i).season_end_date,
               total                           = a_tbl_update(i).total,
               total_short_desc                = a_tbl_update(i).total_short_desc,
               total_long_desc                 = a_tbl_update(i).total_long_desc,
               num_total_timespan_days         = a_tbl_update(i).num_total_timespan_days,
               total_end_date                  = a_tbl_update(i).total_end_date,
               ly_fin_week_code                = a_tbl_update(i).ly_fin_week_code,
               order_by_seq_no                 = a_tbl_update(i).order_by_seq_no,
               fin_half_season_long_desc       = a_tbl_update(i).fin_half_season_long_desc,
               completed_fin_day_ind           = a_tbl_update(i).completed_fin_day_ind,
               completed_fin_week_ind          = a_tbl_update(i).completed_fin_week_ind,
               completed_fin_month_ind         = a_tbl_update(i).completed_fin_month_ind,
               completed_fin_quarter_ind       = a_tbl_update(i).completed_fin_quarter_ind,
               completed_fin_half_ind          = a_tbl_update(i).completed_fin_half_ind,
               completed_fin_year_ind          = a_tbl_update(i).completed_fin_year_ind,
               completed_cal_year_ind          = a_tbl_update(i).completed_cal_year_ind,
               completed_season_ind            = a_tbl_update(i).completed_season_ind,
               cal_month_short_desc            = a_tbl_update(i).cal_month_short_desc,
               fin_half_season_short_desc      = a_tbl_update(i).fin_half_season_short_desc,
               last_updated_date               = a_tbl_update(i).last_updated_date
       where   fin_year_no                     = a_tbl_update(i).fin_year_no         and
               fin_week_no                     = a_tbl_update(i).fin_week_no         ;

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
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   from   dim_calendar_wk
   where  fin_year_no        = g_rec_out.fin_year_no      and
          fin_week_no        = g_rec_out.fin_week_no;

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

    l_text := 'ROLLUP OF dim_calendar_wk EX DAY LEVEL STARTED AT '||
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
    open c_dim_calendar;
    fetch c_dim_calendar bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_dim_calendar bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_calendar;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

    commit;


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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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
end wh_prf_corp_007u;
