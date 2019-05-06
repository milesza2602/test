--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_299U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_299U" 

                   (p_forall_limit in integer,p_success out boolean,p_start_date in date,p_end_date in date)
                   as
--**************************************************************************************************
--  Date:        June 2009
--  Author:      S Le Roux
--  Purpose:     Generate missing records of  debtors comm period specified.
--               The procedure can also generate debtors comm records for a specified period,
--               when supplying the start- and end-date input parameters.
--  Tables:      Input  - fnd_of_fin_debtors_comm
--               Output - fnd_of_fin_debtors_comm
--  Packages:    constants, dwh_log,
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
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
--g_forall_limit      integer       :=  dwh_constants.vc_forall_limit;
g_forall_limit      integer       := 1000;
g_recs_read         integer       :=  0;
g_recs_inserted     integer       :=  0;
g_recs_updated      integer       :=  0;
g_error_count       number        :=  0;
g_error_index       number        :=  0;
g_count             integer       :=  0;
g_found             boolean;
g_date              date;
g_start_date        date;
g_end_date          date;
g_begin_date        date := trunc(sysdate - 4);  -- start rollover of records where data does not exist 4 days back from current day
idx1                integer := 0;
idx                 integer := 0;
g_rec_out           fnd_of_fin_debtors_comm%rowtype;
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_299U';
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts ;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'ROLLOVER DEBTORS COMMISSION ON fnd_of_fin_debtors_comm';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_of_fin_debtors_comm%rowtype index by binary_integer;
type tbl_array_u is table of fnd_of_fin_debtors_comm%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_fnd_fin_debtors_comm is
   select *
   from    fnd_of_fin_debtors_comm fdc
   where post_date = g_begin_date + idx
   ;

g_rec_in   c_fnd_fin_debtors_comm%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_fin_debtors_comm%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                                 := null;

   g_rec_out.debtors_business_id             := g_rec_in.debtors_business_id ;
   g_rec_out.customer_billing_no             := g_rec_in.customer_billing_no;
   g_rec_out.store_billing_id                := g_rec_in.store_billing_id;
   g_rec_out.post_date                       := g_rec_in.post_date + 1;
   g_rec_out.debtors_commission_perc         := g_rec_in.debtors_commission_perc;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||
                    g_rec_out.debtors_business_id||' '||g_rec_out.customer_billing_no||' '||g_rec_out.post_date;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into fnd_of_fin_debtors_comm values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).debtors_business_id||
                       ' '||a_tbl_insert(g_error_index).customer_billing_no||
                       ' '||a_tbl_insert(g_error_index).store_billing_id||
                       ' '||a_tbl_insert(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
      end loop;
      raise;


end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update fnd_of_fin_debtors_comm
      set    debtors_commission_perc  = a_tbl_update(i).debtors_commission_perc,
             last_updated_date        = a_tbl_update(i).last_updated_date
      where  debtors_business_id      = a_tbl_update(i).debtors_business_id
      and    customer_billing_no      = a_tbl_update(i).customer_billing_no
      and    store_billing_id         = a_tbl_update(i).store_billing_id
      and    post_date                = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).debtors_business_id||
                       ' '||a_tbl_update(g_error_index).customer_billing_no||
                       ' '||a_tbl_update(g_error_index).store_billing_id||
                       ' '||a_tbl_update(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;


end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to output table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_of_fin_debtors_comm
   where  debtors_business_id         = g_rec_out.debtors_business_id
      and    customer_billing_no      = g_rec_out.customer_billing_no
      and    store_billing_id         = g_rec_out.store_billing_id
      and    post_date                = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;
--      local_bulk_update;
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
    p_success := false;
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'GENERATE DEBTORS COMM STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

    dwh_lookup.dim_control(g_date);
    if p_start_date is not null and p_end_date is not null then
       g_start_date := p_start_date;
       g_end_date   := p_end_date;
    else
       g_start_date := g_date - 2;
       g_end_date   := g_date + 2;
    end if;

    l_text       := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text       := 'GENERATE RECORDS FOR - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

 for idx1 in 1 .. 40    --- starts rollover of records from 4 days(begin_date) back to 6 days in the future
 loop
    open c_fnd_fin_debtors_comm;
    idx := idx + 1;
    fetch c_fnd_fin_debtors_comm bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;


      end loop;
    fetch c_fnd_fin_debtors_comm bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_fin_debtors_comm;
    commit;
    local_bulk_insert;
    a_tbl_insert  := a_empty_set_i;
    a_tbl_update  := a_empty_set_u;
    a_count_i     := 0;
    a_count_u     := 0;
    a_count       := 0;
    commit;
end loop;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
--    local_bulk_update;
commit;

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
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm||' '||
                    g_rec_out.debtors_business_id||' '||g_rec_out.customer_billing_no||' '||g_rec_out.post_date;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||
                    g_rec_out.debtors_business_id||' '||g_rec_out.customer_billing_no||' '||g_rec_out.post_date;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_fnd_corp_299U;
