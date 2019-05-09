--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_229T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_229T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2012
--  Author:      Quentin Smit
--  Purpose:     Extract first sale date per item for last 6 weeks
--  Tables:      Input  -   rtl_loc_item_rms_dense
--               Output -   rtl_item_trading
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--             17 oct 2013 -  hint changed from /* +  to /*+
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
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

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_229T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT FIRST SALE DATE PER ITEM';
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
l_season_start_date date;
l_season_end_date   date;
l_fin_year_no       integer;
l_fin_week_no       integer;
l_first_date        date;

cursor c_first_sale_date is
   with aa as (
     select /*+ PARALLEL(f,4) FULL(f) */
          sk1_item_no
     from rtl_item_trading 
    where sk1_business_unit_no <> 3
  ),
   
   dense as (
   select /*+ PARALLEL(f,4) FULL(f) */
          f.sk1_item_no, min(post_date) as season_first_trade_date
     from rtl_loc_item_dy_rms_dense f, aa di
    where f.sk1_item_no = di.sk1_item_no
      and f.post_date >= '24/DEC/12'
   AND     f.sales != 0
      and f.sales is not null
    group by f.sk1_item_no
  )
   select * from dense;  

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
   g_rec_out.season_first_trade_date          := g_rec_in.season_first_trade_date;
   g_rec_out.last_updated_date                := g_date;
--    l_text := '1 g_rec_out.season_first_trade_date='||g_rec_OUT.season_first_trade_date||' g_rec_IN.season_first_trade_date='||g_rec_IN.season_first_trade_date;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   -- #############################################################################
   -- Calculate number of weeks since first sale to current week
   -- Get the date of fin_day_no 1 for the week that the item was first on sale for
   -- and use this date to calculate the number of weeks
   -- #############################################################################
   --select fin_year_no, fin_week_no
   --  into l_fin_year_no, l_fin_week_no
   --  from dim_calendar
   --  where calendar_date = to_date(g_rec_in.first_trade_date);

   --select calendar_date
   --  into l_first_date
   --  from dim_calendar
   -- where fin_year_no = l_fin_year_no
   --   and fin_week_no = l_fin_week_no
   --   and fin_day_no = 1;

   --select (to_date(g_date) - to_date(l_first_date))/7
   --  into g_rec_out.no_of_weeks
   --  from dual;
   --#######################################################
   -- For C&H get the first sale date for the current season
   --#######################################################
   --if g_rec_in.business_unit_no <> 50 then
   --   select min(post_date)
   --     into g_rec_out.season_first_trade_date
   --     from rtl_loc_item_dy_rms_dense
   --    where post_date between l_season_start_date and l_season_end_date;
   --end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
/*procedure local_bulk_insert as
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

*/
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update dwh_PERFORMANCE.rtl_item_trading
      set    --sk1_business_unit_no             = a_tbl_update(i).sk1_business_unit_no,
             --first_trade_date                 = a_tbl_update(i).first_trade_date,
             season_first_trade_date          = a_tbl_update(i).season_first_trade_date,
             --no_of_weeks                      = a_tbl_update(i).no_of_weeks,
             last_updated_date                = a_tbl_update(i).last_updated_date
      where  sk1_item_no                      = a_tbl_update(i).sk1_item_no  ;
      g_recs_updated := g_recs_updated + a_tbl_update.count;
      commit;
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
       
--    commit;
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
   from   rtl_item_trading
   where  sk1_item_no        = g_rec_out.sk1_item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
--    l_text := '2 g_rec_out.season_first_trade_date='||g_rec_OUT.season_first_trade_date||' g_rec_IN.season_first_trade_date='||g_rec_IN.season_first_trade_date;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
      if g_found then
      --local_bulk_insert;
      local_bulk_update;

        a_tbl_insert  := a_empty_set_i;
        a_tbl_update  := a_empty_set_u;
        a_count_i     := 0;
        a_count_u     := 0;
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

    l_text := 'UPDATE OF RTL_ITEM_TRADING STARTED AT '||
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
    select season_start_date, season_end_date
      into l_season_start_date, l_season_end_date
      from dim_calendar
     where calendar_date = g_date;
     
     --update rtl_item_trading set no_of_weeks = no_of_weeks + 1;
     --commit;
     --l_text := 'No of weeks for each existing item increased by 1';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_first_sale_date;
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
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
    fetch c_first_sale_date bulk collect into a_item_input limit g_forall_limit;
    end loop;
    close c_first_sale_date;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      --local_bulk_insert;
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
end wh_prf_corp_229t;