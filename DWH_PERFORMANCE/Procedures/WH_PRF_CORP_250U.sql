--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_250U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_250U" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- This procedure has been changed to cater for C&GM only (ie. business_unit_no <> 50)
-- Wendy - 14 Feb 2013
--***************************************************************************************************
-- This procedure will run weekly on a Saturday.
-- It will take current week as last completed week.
--**************************************************************************************************
--  Date:        February 2013
--  Author:      Wendy lyttle
--  Purpose:     Extract first sale date per item for last 6 weeks - update first_trade_date, no_of_weeks
--  Tables:      Input  -   rtl_loc_item_rms_dense
--               Output -   rtl_item_trading
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  qc4835     31 Jan 2013 - change to process all items-
--                             but..
--                             a.) Insert if new
--                             b.) Update season_first_trade_date for clothing and home only for the current season
--             17 oct 2013 -  hint changed from /* +  to /*+
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
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
g_rec_out            rtl_item_trading%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_week_start_date date;
g_season_start_date date;
g_season_end_date   date;
g_upd2       integer       :=  0;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_250U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT FIRST SALE DATE PER ITEM CH';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_item_trading%rowtype index by binary_integer;
type tbl_array_u is table of rtl_item_trading%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_first_sale_date is
   select /*+ PARALLEL(f,4) FULL(f) */
          f.sk1_item_no, min(post_date) as first_trade_date, di.sk1_business_unit_no, di.business_unit_no
     from rtl_loc_item_dy_rms_dense f,
          dim_item di
    where f.sk1_item_no = di.sk1_item_no
      and f.post_date >= g_date - 41
      and f.post_date <= g_date
      and f.sales != 0
      and f.sales is not null
      and di.business_unit_no <> 50
      and f.sk1_item_no not in(select rit.sk1_item_no from rtl_item_trading rit)
      group by f.sk1_item_no, di.sk1_business_unit_no, di.business_unit_no;


g_rec_in      c_first_sale_date%rowtype;

-- For input bulk collect --
type stg_array is table of c_first_sale_date%rowtype;
a_item_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_item_no                      := g_rec_in.sk1_item_no;
   g_rec_out.sk1_business_unit_no             := g_rec_in.sk1_business_unit_no;
   g_rec_out.first_trade_date                 := g_rec_in.first_trade_date;
   g_rec_out.last_updated_date                := g_date;

--** No_of_weeks will be recalculated for all at the end

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
      insert into rtl_item_trading values a_tbl_insert(i);
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
      update rtl_item_trading
      set    season_first_trade_date          = a_tbl_update(i).season_first_trade_date,
             last_updated_date                = a_tbl_update(i).last_updated_date
      where  sk1_item_no                      = a_tbl_update(i).sk1_item_no  ;

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
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      if not g_found then
        local_bulk_insert;

        a_tbl_insert  := a_empty_set_i;
        a_count_i     := 0;
        a_count       := 0;

        commit;
      end if;
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

    l_text := 'LOAD OF rtl_item_trading CH STARTED AT '||
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
    execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Get start and end dates for current season - only for C&H
--**************************************************************************************************
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
    open c_first_sale_date;
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
    while a_item_input.count > 0
    loop
      for i in 1 .. a_item_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 2000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_item_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
    end loop;
    close c_first_sale_date;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

     local_bulk_insert;

--**************************************************************************************************
-- Update of no_of_weeks from first_trade_date to current date
-- Is used by FOODs only for the time being but we are still updating for C&GM
--**************************************************************************************************

     select this_week_start_date
     into g_this_week_start_date
     from dim_calendar
     where calendar_date = g_date;
     
     update rtl_item_trading rit 
     set no_of_weeks = (
            select (g_this_week_start_date - dc.this_week_start_date) / 7 DIFF7
              from dim_calendar dc
             where dc.calendar_date = rit.first_trade_date
            group by (g_this_week_start_date - dc.this_week_start_date) / 7)
     where sk1_item_no in (select sk1_item_no from dim_item where business_unit_no <> 50);
     g_upd2 := g_upd2 + sql%rowcount;
     commit;
     l_text := 'Update of no_of_weeks = '||g_upd2;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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

END WH_PRF_CORP_250U;
