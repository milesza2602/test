--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_083Q
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_083Q" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Load dim_prom dimention table in performance layer ex foundation layer.
--  Tables:      Input  - fnd_prom
--               Output - dim_prom
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  5 Aug 2009 DEFECT 2089 - Prm Cube: Table column update request on DIM_PROM
--  25 OCT 2010 - 3996- Add wr_reward_ind column to DIM_PROM - will be updated
--                      later
-- 
--  wendy lyttle 5 july 2012 removed to allow thru -and      pl.prom_no <>  313801
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_prom%rowtype;
g_rec_in             dim_prom%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_083Q';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DIM_PROM EX FND_PROM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of dim_prom%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_prom%rowtype index by binary_integer;
type tbl_array_u is table of dim_prom%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_prom is
   select 0 sk1_prom_no, prom_no, prom_name, prom_desc, prom_start_date, prom_end_date, prom_start_time,
          prom_end_time, currency_code, status_type, num_forecast_qty, event_no, create_date,
          create_user_id, approval_date, approval_user_id, extract_date, impact_run_date, proj_sales_method,
          leadtime_method_code, like_period_prom_start_date, like_period_prom_end_date, like_period_ldtime_start_date,
          like_period_leadtime_end_date, soh_leadtime_perc, soh_prom_sales_perc, prom_comment,
          prom_level_type, zone_group_no, prom_type,
          (case prom_type when 'MM' then 'MIX & MATCH' when 'MU' then 'MULTI-UNIT'
                          when 'SK' then 'SIMPLE'      when 'TH' then 'THRESHHOLD' end) prom_type_desc,
          0 excl_prom_from_reporting_ind, ' ' total, ' ' total_desc, trunc(sysdate) last_updated_date,
          '' prom_log_desc, 0
   From   Fnd_Prom where prom_no = 531770;
-- removed to allow thru
-- 5 july 2012
 --  where prom_no <> 313801;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out                            := g_rec_in;

   g_rec_out.total                      := 'TOTAL';
   g_rec_out.total_desc                 := 'ALL PROMOTIONS';
   g_rec_out.prom_long_desc             := g_rec_in.prom_no||' - '||g_rec_in.prom_name;
   g_rec_out.last_updated_date          := g_date;

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
      insert into dim_prom values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).prom_no;
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
      update dim_prom
      set    prom_name                        = a_tbl_update(i).prom_name,
             prom_desc                        = a_tbl_update(i).prom_desc,
             prom_start_date                  = a_tbl_update(i).prom_start_date,
             prom_end_date                    = a_tbl_update(i).prom_end_date,
             prom_start_time                  = a_tbl_update(i).prom_start_time,
             prom_end_time                    = a_tbl_update(i).prom_end_time,
             currency_code                    = a_tbl_update(i).currency_code,
             status_type                      = a_tbl_update(i).status_type,
             num_forecast_qty                 = a_tbl_update(i).num_forecast_qty,
             event_no                         = a_tbl_update(i).event_no,
             create_date                      = a_tbl_update(i).create_date,
             create_user_id                   = a_tbl_update(i).create_user_id,
             approval_date                    = a_tbl_update(i).approval_date,
             approval_user_id                 = a_tbl_update(i).approval_user_id,
             extract_date                     = a_tbl_update(i).extract_date,
             impact_run_date                  = a_tbl_update(i).impact_run_date,
             proj_sales_method                = a_tbl_update(i).proj_sales_method,
             leadtime_method_code             = a_tbl_update(i).leadtime_method_code,
             like_period_prom_start_date      = a_tbl_update(i).like_period_prom_start_date,
             like_period_prom_end_date        = a_tbl_update(i).like_period_prom_end_date,
             like_period_ldtime_start_date    = a_tbl_update(i).like_period_ldtime_start_date,
             like_period_leadtime_end_date    = a_tbl_update(i).like_period_leadtime_end_date,
             soh_leadtime_perc                = a_tbl_update(i).soh_leadtime_perc,
             soh_prom_sales_perc              = a_tbl_update(i).soh_prom_sales_perc,
             prom_comment                     = a_tbl_update(i).prom_comment,
             prom_level_type                  = a_tbl_update(i).prom_level_type,
             zone_group_no                    = a_tbl_update(i).zone_group_no,
             prom_type                        = a_tbl_update(i).prom_type,
             prom_type_desc                   = a_tbl_update(i).prom_type_desc,
             last_updated_date                = a_tbl_update(i).last_updated_date,
             prom_long_desc                   = a_tbl_update(i).prom_long_desc
      where  prom_no                          = a_tbl_update(i).prom_no;

      g_recs_updated    := g_recs_updated + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).prom_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := dwh_valid.dim_prom(g_rec_out.prom_no);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_prom_no   := merch_hierachy_seq.nextval;
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
      a_tbl_insert     := a_empty_set_i;
      a_tbl_update     := a_empty_set_u;
      a_count_i        := 0;
      a_count_u        := 0;
      a_count          := 0;
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

    l_text := 'LOAD OF DIM_PROM EX FND_PROM STARTED '||
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

--**************************************************************************************************
    open c_fnd_prom;
    fetch c_fnd_prom bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_prom bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_prom;

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

end wh_prf_corp_083q;
