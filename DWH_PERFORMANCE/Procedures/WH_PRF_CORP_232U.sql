--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_232U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_232U" 
                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alfonso Joshua
--  Purpose:     Create Style Colour Trading Store data needed for JDA Assort
--  Tables:      Input  - rtl_item_trading,
--               Output - rtl_sc_trading
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--   wendy lyttle - 29 september 2014 -- adding 'C&H:-Noos' to description 
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_count              integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_sc_trading%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_season_start_date  date;
g_season_end_date  date;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_232U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE Style Colour Trading EX Item Trading';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_sc_trading%rowtype index by binary_integer;
type tbl_array_u is table of rtl_sc_trading%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_item_trading is
        select ld.sk1_style_colour_no,
               min(it.first_trade_date) first_trade_date,
               min(nvl(it.season_first_trade_date,g_season_start_date)) season_first_trade_date,
               CASE WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Continuity','Core','Key') THEN 1
  --wl 29/sep/2014          --        WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Input','Input Volume','Trial')
                    WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Input','Input Volume','Trial','Noos')
                         OR ld.RANGE_STRUCTURE_CH_DESC_104 = 'No Value' THEN 0
               END  continuity_ind,
               it.sk1_business_unit_no,
               g_season_start_date season_start_date
        from   rtl_item_trading it
        join   dim_item i
           on  it.sk1_item_no = i.sk1_item_no
        join   dim_sc_uda ld
           on  i.sk1_style_colour_no = ld.sk1_style_colour_no
        where  NVL(it.season_first_trade_date,to_date('19000101','yyyymmdd')) >= g_season_start_date
        and    i.business_unit_no <> 50
        group by ld.sk1_style_colour_no, CASE WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Continuity','Core','Key') THEN 1
               --     WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Input','Input Volume','Trial')
                    WHEN substr(ld.RANGE_STRUCTURE_CH_DESC_104,instr(ld.RANGE_STRUCTURE_CH_DESC_104,'-')+1) IN ('Input','Input Volume','Trial','Noos')
                         OR ld.RANGE_STRUCTURE_CH_DESC_104 = 'No Value' THEN 0
               END, it.sk1_business_unit_no, g_season_start_date;

g_rec_in             c_rtl_item_trading%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_item_trading%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_style_colour_no          := g_rec_in.sk1_style_colour_no;
   g_rec_out.season_start_date            := g_rec_in.season_start_date;
   g_rec_out.first_trade_date             := g_rec_in.first_trade_date;
   g_rec_out.season_first_trade_date      := g_rec_in.season_first_trade_date;
   g_rec_out.continuity_ind               := g_rec_in.continuity_ind;
   g_rec_out.sk1_business_unit_no         := g_rec_in.sk1_business_unit_no;
   g_rec_out.last_updated_date            := g_date;

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
      insert into rtl_sc_trading values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).season_start_date;
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
      update rtl_sc_trading
      set    first_trade_date          = a_tbl_update(i).first_trade_date,
             season_first_trade_date   = a_tbl_update(i).season_first_trade_date,
             continuity_ind            = a_tbl_update(i).continuity_ind,
             sk1_business_unit_no      = a_tbl_update(i).sk1_business_unit_no,
             last_updated_date         = a_tbl_update(i).last_updated_date
      where  sk1_style_colour_no       = a_tbl_update(i).sk1_style_colour_no
       and   season_start_date         = a_tbl_update(i).season_start_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).season_start_date;
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
   from   rtl_sc_trading
   where  sk1_style_colour_no  = g_rec_out.sk1_style_colour_no
    and   season_start_date    = g_rec_out.season_start_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_sc_trading EX rtl_item_trading STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 /*   select season_start_date, season_end_date
      into g_season_start_date, g_season_end_date
      from dim_calendar
     where calendar_date = g_date;
-- Above commented out code was changed on 16 may 2013 as it 
--  should be using fin_half_year and not current_season_start_date
     
 */    
   select fin_half_no, fin_year_no
   into g_fin_half_no, g_fin_year_no
   from dim_calendar
   where calendar_date = g_date;

   select distinct min(season_start_date), max(fin_half_end_date)
   into g_season_start_date, g_season_end_date
   from dim_calendar
   where fin_half_no = g_fin_half_no
   and   fin_year_no = g_fin_year_no;

   l_text := 'Data extract from '||g_season_start_date|| ' to '||g_season_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_rtl_item_trading;
    fetch c_rtl_item_trading bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_rtl_item_trading bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_item_trading;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_232u;
