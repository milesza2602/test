--------------------------------------------------------
--  DDL for Procedure XWH_PRF_AST_100U_WLFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."XWH_PRF_AST_100U_WLFIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alfonso Joshua
--  Purpose:     Identify Continuity/Fashionable Stylecolours as input into JDA Assort Style Card Mart
--  Tables:      Input  -   rtl_sc_trading
--               Output -   rtl_sc_continuity_wk
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
g_rec_out            rtl_sc_continuity_wk%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_curr_season_start_date date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'XWH_PRF_AST_100U_WLFIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT CONTINUITY/FASHIONABLE STYLECOLs - C&H JDA ASSORT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_season_start_date date;
l_season_end_date   date;
l_fin_year_no       integer;
l_fin_week_no       integer;
l_first_date        date;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_sc_continuity_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_sc_continuity_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


   cursor c_cont_fash is
      select /*+parallel(t,4) */ distinct
         sk1_style_colour_no, 
         continuity_ind,
         season_first_trade_date,
         case when continuity_ind = 1 then
                   c3.fin_year_no||lpad(c3.fin_week_no,2,'0') 
              else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') 
         end  cont_prev_week_code,
         case when continuity_ind=1 then
                     -- set first week to start of season if it's less
            case when c1.fin_year_no||lpad(c1.fin_week_no,2,'0') < ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0') then--season start week code 
                          ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0')
                 else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end 
         else '999999' end cont_start_week_code,
         case when continuity_ind=1 then 
                   c2.fin_year_no||lpad(c2.fin_week_no,2,'0')
              else '999999' end cont_end_week_code,
         min(case when c1.calendar_date < season_first_trade_date then '999999'
                  else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end) 
             over (partition by sk1_style_colour_no) fash_start_week_code,
         few.fin_year_no||lpad(few.fin_week_no,2,'0') fash_end_week_code

      from  rtl_sc_trading t
       join dim_calendar c1 on c1.calendar_date between 
            case when continuity_ind=0 then t.season_first_trade_date else g_date - (7 * 12) end
             and
            case when continuity_ind=0 then LEAST(t.season_first_trade_date + 43,g_date) else g_date end 
-- get the rpl end week code (6 weeks from calendar date)          
       join dim_calendar c2 on c2.calendar_date = c1.calendar_date + (7*6)
-- get the previous week's code of the rpl end week code
       join dim_calendar c3 on c3.calendar_date = c2.calendar_date - 7
-- get the week code for the season start date
       join dim_calendar ssw on ssw.calendar_date = g_curr_season_start_date     
-- get the fashion end week code
       join dim_calendar few on few.calendar_date = c1.calendar_date + 7          
                                                                     
     where  t.season_start_date = g_curr_season_start_date
      and   case when continuity_ind = 1 then c1.calendar_date 
            else to_date('19000101','yyyymmdd') end <=  g_date - 42
     order by
            sk1_style_colour_no, 
            case when continuity_ind = 1 then
                      c3.fin_year_no||lpad(c3.fin_week_no,2,'0') 
                 else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') 
            end;
            
g_rec_in c_cont_fash%rowtype;

-- For input bulk collect --
type stg_array is table of c_cont_fash%rowtype;
a_item_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_style_colour_no              := g_rec_in.sk1_style_colour_no;
   g_rec_out.continuity_ind                   := g_rec_in.continuity_ind;
   g_rec_out.season_first_trade_date          := g_rec_in.season_first_trade_date;
   g_rec_out.cont_prev_week_code              := g_rec_in.cont_prev_week_code;
   g_rec_out.cont_start_week_code             := g_rec_in.cont_start_week_code;
   g_rec_out.cont_end_week_code               := g_rec_in.cont_end_week_code;
   g_rec_out.fash_start_week_code             := g_rec_in.fash_start_week_code;
   g_rec_out.fash_end_week_code               := g_rec_in.fash_end_week_code;

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
      insert into rtl_sc_continuity_wk values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + sql%rowcount;


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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no;
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
      update rtl_sc_continuity_wk
      set    continuity_ind             = a_tbl_update(i).continuity_ind,
             season_first_trade_date    = a_tbl_update(i).season_first_trade_date,
             cont_prev_week_code        = a_tbl_update(i).cont_prev_week_code,
             cont_start_week_code       = a_tbl_update(i).cont_start_week_code,
             cont_end_week_code         = a_tbl_update(i).cont_end_week_code,
             fash_start_week_code       = a_tbl_update(i).fash_start_week_code,
             fash_end_week_code         = a_tbl_update(i).fash_end_week_code
--             last_updated_date          = a_tbl_update(i).last_updated_date
      where  sk1_style_colour_no        = a_tbl_update(i).sk1_style_colour_no  ;

      g_recs_updated := g_recs_updated + sql%rowcount;


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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no;
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
   from   rtl_sc_continuity_wk
   where  sk1_style_colour_no  = g_rec_out.sk1_style_colour_no;

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

    l_text := 'LOAD OF RTL_SC_CONTINUITY_WK STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
  --   G_DATE := '5 SEP 2016';
     G_DATE := '1 AUG 2016';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select season_start_date
    into   g_curr_season_start_date
    from   dim_calendar 
    where  calendar_date = g_date;
      l_text := 'Season start_date = '||g_curr_season_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    open c_cont_fash;
    fetch c_cont_fash bulk collect into a_item_input limit g_forall_limit;
    while a_item_input.count > 0
    loop
      for i in 1 .. a_item_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_item_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_cont_fash bulk collect into a_item_input limit g_forall_limit;
    end loop;
    close c_cont_fash;
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
end Xwh_prf_ast_100u_WLFIX;
