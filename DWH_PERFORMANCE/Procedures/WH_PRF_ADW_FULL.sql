--------------------------------------------------------
--  DDL for Procedure WH_PRF_ADW_FULL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_ADW_FULL" 
(p_forall_limit in integer,p_success out boolean,p_start_date in date,p_end_date in date) as
--**************************************************************************************************
--  Date:        Sep 2011
--  Author:      ADW
--  Purpose:     Full spinner for fixes
--  Tables:      Input  -   rtl_loc_item_dy_rms_dense
--               Output -   rtl_loc_item_wk_rms_dense
--  Packages:    constants, dwh_log, dwh_valid

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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_fin_week_no        integer       :=  0;
g_fin_year_no        integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date;
g_this_week_start_date     date;
g_calendar_date      date;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_ADW_FULL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FIX rtl_loc_item_wk_rms_dense EX rtl_loc_item_dy_rms_dense';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_wk_rms_dense%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_wk_rms_dense%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_dy_rms_dense is
with dy as (
select
      sk1_location_no,
      sk1_item_no,
      sum(nvl(sdn_in_cases,0)) sdn_in_cases ,
      sum(nvl(sdn_in_qty,0)) sdn_in_qty,
      sum(nvl(actl_store_rcpt_qty,0)) actl_store_rcpt_qty,
      sum(nvl(actl_store_rcpt_selling,0)) actl_store_rcpt_selling,
      sum(nvl(actl_store_rcpt_cost,0)) actl_store_rcpt_cost
from  rtl_loc_item_dy_rms_dense
where post_date between G_THIS_WEEK_START_DATE and G_THIS_WEEK_START_DATE + 6
group by sk1_location_no,sk1_item_no
order by sk1_item_no)

select dy.sk1_location_no,
       dy.sk1_item_no,
       dy.sdn_in_cases,
       dy.sdn_in_qty,
       dy.actl_store_rcpt_qty,
       dy.actl_store_rcpt_selling,
       dy.actl_store_rcpt_cost
from   dy 
left outer join rtl_loc_item_wk_rms_dense dns on
       fin_year_no         = G_FIN_YEAR_NO 
and    fin_week_no         = G_FIN_WEEK_NO
and    dns.sk1_item_no     = dy.sk1_item_no 
and    dns.sk1_location_no = dy.sk1_location_no
where  (nvl(dns.sdn_in_cases,0)              <> dy.sdn_in_cases
     or nvl(dns.actl_store_rcpt_qty,0)       <> dy.actl_store_rcpt_qty
     or nvl(dns.actl_store_rcpt_selling,0)   <> dy.actl_store_rcpt_selling
     or nvl(dns.actl_store_rcpt_cost,0)      <> dy.actl_store_rcpt_cost)
order by sk1_item_no;
 
-- Input record declared as cursor%rowtype
g_rec_in             c_rtl_loc_item_dy_rms_dense%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_rtl_loc_item_dy_rms_dense%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin
 
   g_rec_out.sk1_location_no                := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                    := g_rec_in.sk1_item_no;
   g_rec_out.fin_year_no                    := g_fin_year_no;
   g_rec_out.fin_week_no                    := g_fin_week_no;
   g_rec_out.fin_week_code                  := 'W'||g_fin_year_no||g_fin_week_no;
   g_rec_out.this_week_start_date           := G_THIS_WEEK_START_DATE;
   g_rec_out.sdn_in_cases                   := g_rec_in.sdn_in_cases;
   g_rec_out.sdn_in_qty                     := g_rec_in.sdn_in_qty;
   g_rec_out.actl_store_rcpt_qty            := g_rec_in.actl_store_rcpt_qty;
   g_rec_out.actl_store_rcpt_selling        := g_rec_in.actl_store_rcpt_selling;
   g_rec_out.actl_store_rcpt_cost           := g_rec_in.actl_store_rcpt_cost;
   g_rec_out.last_updated_date              := '11 May 1960';

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin
--DBMS_OUTPUT.PUT_LINE('UPDATE');
   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update rtl_loc_item_wk_rms_dense 
      set   sdn_in_cases            = a_tbl_update(i).sdn_in_cases,
            sdn_in_qty              = a_tbl_update(i).sdn_in_qty,
            actl_store_rcpt_qty     = a_tbl_update(i).actl_store_rcpt_qty,
            actl_store_rcpt_selling = a_tbl_update(i).actl_store_rcpt_selling,
            actl_store_rcpt_cost    = a_tbl_update(i).actl_store_rcpt_cost,
            last_updated_date       = a_tbl_update(i).last_updated_date
      where sk1_location_no         = a_tbl_update(i).sk1_location_no
      and   sk1_item_no             = a_tbl_update(i).sk1_item_no
      and   fin_year_no             = a_tbl_update(i).fin_year_no
      and   fin_week_no             = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_loc_item_wk_rms_dense values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).fin_week_no||                       
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_loc_item_wk_rms_dense
   where  sk1_location_no    = g_rec_out.sk1_location_no     and
          sk1_item_no        = g_rec_out.sk1_item_no         and
          fin_year_no        = g_rec_out.fin_year_no         and
          fin_week_no        = g_rec_out.fin_week_no     ;
          
    

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
   
     
      local_bulk_update;
      local_bulk_insert;
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
    l_text := 'FIX OF rtl_loc_item_wk_rms_dense EX rtl_loc_item_dy_rms_dense STARTED '||
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
 
    select fin_week_no, fin_year_no, this_week_start_date
    into g_fin_week_no, g_fin_year_no, g_this_week_start_date
    from dim_calendar
    where calendar_date = p_start_date;
    
    l_text := 'DATES BEING PROCESSED - '||
    g_fin_week_no||' '|| g_fin_year_no||' '|| g_this_week_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--**************************************************************************************************
    open c_rtl_loc_item_dy_rms_dense;
    fetch c_rtl_loc_item_dy_rms_dense bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_rms_dense bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_rms_dense;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
 
      local_bulk_update;
      local_bulk_insert;
      COMMIT;
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

end wh_prf_adw_full;
