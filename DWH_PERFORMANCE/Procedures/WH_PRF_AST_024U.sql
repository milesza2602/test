--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_024U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_024U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2012
--  Author:      Alfonso Joshua
--  Purpose:     Load Pre-Actual Plan (current week) from performance level.
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_pln
--               Output - rtl_chn_geo_grd_sc_wk_ast_pact
--  Packages:    constants, dwh_log, dwh_valid.
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sk1_plan_type_no   number(9)     := 64;
g_rec_out            rtl_chn_geo_grd_sc_wk_ast_pact%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_024U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_bam;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORT FACT PRE-ACTUAL PLAN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_chn_geo_grd_sc_wk_ast_pact%rowtype index by binary_integer;
type tbl_array_u is table of rtl_chn_geo_grd_sc_wk_ast_pact%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_chn_geo_grd_sc_wk_ast_pln is
   select pln.sk1_chain_no,
          pln.sk1_geography_no,
          pln.sk1_grade_no,
          pln.sk1_style_colour_no,
          pln.fin_year_no,
          pln.fin_week_no,
          g_sk1_plan_type_no as sk1_plan_type_no,
          pln.fin_week_code,
          pln.sales_qty,
          pln.sales_cost,
          pln.sales,
          pln.clearance_cost,
          pln.prom_cost,
          pln.store_intake_qty,
          pln.store_intake_cost,
          pln.store_intake_selling,
          pln.target_stock_qty,
          pln.target_stock_cost,
          pln.target_stock_selling,
          pln.pres_min,
          pln.store_count,
          nvl(pln.time_on_offer_ind,0)  time_on_offer_ind,
          pln.facings,
          pln.sit_qty,
          pln.sales_qty as aps_sales_qty,
          pln.sales_margin,
          pln.store_intake_margin,
          pln.target_stock_margin,
          0 as selling_price,
          0 as cost_price,
          0 as prom_reg_qty,
          0 as prom_reg_cost,
          0 as prom_reg_selling,
          0 as clear_sales_qty,
          0 as clear_sales_cost,
          0 as clear_sales_selling,
          0 as prom_sales_qty,
          0 as prom_sales_cost,
          0 as prom_sales_selling,
          0 as reg_sales_qty,
          0 as reg_sales_cost,
          0 as reg_sales_selling,
          g_date as last_updated_date
    from  rtl_chn_geo_grd_sc_wk_ast_pln pln,
          dim_control ctl
    where ctl.today_date        = g_date
     and  pln.fin_year_no       = ctl.today_fin_year_no
     and  pln.fin_week_no       = ctl.today_fin_week_no
     and  pln.sk1_plan_type_no  = 60;
  
g_rec_in                   c_chn_geo_grd_sc_wk_ast_pln%rowtype;
-- For input bulk collect --
type stg_array is table of c_chn_geo_grd_sc_wk_ast_pln%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_chain_no                    := g_rec_in.sk1_chain_no;
   g_rec_out.sk1_geography_no                := g_rec_in.sk1_geography_no;
   g_rec_out.sk1_grade_no                    := g_rec_in.sk1_grade_no;
   g_rec_out.sk1_style_colour_no             := g_rec_in.sk1_style_colour_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.sk1_plan_type_no                := g_sk1_plan_type_no;
   g_rec_out.fin_week_code                   := g_rec_in.fin_week_code;
   g_rec_out.sales_qty	                     := g_rec_in.sales_qty;
   g_rec_out.sales_cost	                     := g_rec_in.sales_cost;
   g_rec_out.sales                           := g_rec_in.sales;
   g_rec_out.clearance_cost	                 := g_rec_in.clearance_cost;
   g_rec_out.prom_cost	                     := g_rec_in.prom_cost;
   g_rec_out.store_intake_qty                := g_rec_in.store_intake_qty;
   g_rec_out.store_intake_cost	             := g_rec_in.store_intake_cost;
   g_rec_out.store_intake_selling            := g_rec_in.store_intake_selling;
   g_rec_out.target_stock_qty                := g_rec_in.target_stock_qty;
   g_rec_out.target_stock_cost               := g_rec_in.target_stock_cost;
   g_rec_out.target_stock_selling            := g_rec_in.target_stock_selling;
   g_rec_out.pres_min                        := g_rec_in.pres_min;
   g_rec_out.store_count	                   := g_rec_in.store_count;
   g_rec_out.time_on_offer_ind               := g_rec_in.time_on_offer_ind;
   g_rec_out.aps_sales_qty	                 := g_rec_in.sales_qty;
   g_rec_out.sales_margin  	                 := g_rec_in.sales - g_rec_in.sales_cost;
   g_rec_out.store_intake_margin             := g_rec_in.store_intake_selling - g_rec_in.store_intake_cost;
   g_rec_out.target_stock_margin             := g_rec_in.target_stock_selling - g_rec_in.target_stock_cost;
--   g_rec_out.this_week_start_date	           := g_rec_in.this_week_start_date;
   g_rec_out.last_updated_date               := g_date;

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
       insert into rtl_chn_geo_grd_sc_wk_ast_pact values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_chain_no||
                       ' '||a_tbl_insert(g_error_index).sk1_geography_no||
                       ' '||a_tbl_insert(g_error_index).sk1_grade_no||
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no||
                       ' '||a_tbl_insert(g_error_index).sk1_plan_type_no;
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
       update rtl_chn_geo_grd_sc_wk_ast_pact
       set    fin_week_code	                 = a_tbl_update(i).fin_week_code,
              sales_qty	                     = a_tbl_update(i).sales_qty,
              sales_cost	                   = a_tbl_update(i).sales_cost,
              sales	                         = a_tbl_update(i).sales,
              clearance_cost	               = a_tbl_update(i).clearance_cost,
              prom_cost	                     = a_tbl_update(i).prom_cost,
              store_intake_qty	             = a_tbl_update(i).store_intake_qty,
              store_intake_cost	             = a_tbl_update(i).store_intake_cost,
              store_intake_selling           = a_tbl_update(i).store_intake_selling,
              target_stock_qty	             = a_tbl_update(i).target_stock_qty,
              target_stock_cost              = a_tbl_update(i).target_stock_cost,
              target_stock_selling	         = a_tbl_update(i).target_stock_selling,
              pres_min 	                     = a_tbl_update(i).pres_min,
              store_count	                   = a_tbl_update(i).store_count,
              time_on_offer_ind              = a_tbl_update(i).time_on_offer_ind,
              facings  	                     = a_tbl_update(i).facings,
              sit_qty	                       = a_tbl_update(i).sit_qty,
              aps_sales_qty	                 = a_tbl_update(i).aps_sales_qty,
              sales_margin	                 = a_tbl_update(i).sales_margin,
              store_intake_margin	           = a_tbl_update(i).store_intake_margin,
              target_stock_margin            = a_tbl_update(i).target_stock_margin,
--              this_week_start_date           = a_tbl_update(i).this_week_start_date,
              last_updated_date              = a_tbl_update(i).last_updated_date
       where  sk1_chain_no                   = a_tbl_update(i).sk1_chain_no  and
              sk1_geography_no               = a_tbl_update(i).sk1_geography_no  and
              sk1_grade_no                   = a_tbl_update(i).sk1_grade_no  and
              sk1_style_colour_no            = a_tbl_update(i).sk1_style_colour_no  and
              fin_year_no                    = a_tbl_update(i).fin_year_no  and
              fin_week_no                    = a_tbl_update(i).fin_week_no  and
              sk1_plan_type_no               = a_tbl_update(i).sk1_plan_type_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_chain_no||
                       ' '||a_tbl_update(g_error_index).sk1_geography_no||
                       ' '||a_tbl_update(g_error_index).sk1_grade_no||
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no||
                       ' '||a_tbl_update(g_error_index).sk1_plan_type_no;
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
/*
   select count(1)
   into   g_count
   from   rtl_chn_geo_grd_sc_wk_ast_pact
   where  sk1_chain_no        = g_rec_out.sk1_chain_no and
          sk1_grade_no        = g_rec_out.sk1_grade_no and
          sk1_style_colour_no = g_rec_out.sk1_style_colour_no and
          sk1_plan_type_no    = g_rec_out.sk1_plan_type_no and
          fin_year_no         = g_rec_out.fin_year_no      and
          fin_week_no         = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

/*
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_chain_no        = g_rec_out.sk1_chain_no and
            a_tbl_insert(i).sk1_grade_no        = g_rec_out.sk1_grade_no and
            a_tbl_insert(i).sk1_style_colour_no = g_rec_out.sk1_style_colour_no and
            a_tbl_insert(i).sk1_plan_type_no    = g_rec_out.sk1_plan_type_no and
            a_tbl_insert(i).fin_year_no         = g_rec_out.fin_year_no and
            a_tbl_insert(i).fin_week_no         = g_rec_out.fin_week_no
             then
            g_found := TRUE;
         end if;
      end loop;
   end if;
*/

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

    l_text := 'LOAD OF RTL_CHN_GEO_GRD_SC_WK_AST_PACT EX PERFORMANCE STARTED AT '||
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
    open c_chn_geo_grd_sc_wk_ast_pln;
    fetch c_chn_geo_grd_sc_wk_ast_pln bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_chn_geo_grd_sc_wk_ast_pln bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_chn_geo_grd_sc_wk_ast_pln;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
--    local_bulk_update;

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
end wh_prf_ast_024u;