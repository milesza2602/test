--------------------------------------------------------
--  DDL for Procedure WH_PRF_MP_020U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MP_020U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create the product area dept plan type table in the performance layer
--               with input ex MP table from foundation layer.
--  Tables:      Input  - fnd_chain_dept_wk_plan
--               Output - rtl_area_dept_wk_mp_plan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Jan 2009 - defect 444  - Change to fnd_chain_dept_wk_plan structure
--              - defect 446  - Change to rtl_area_dept_wk_mp_plan structure
--  03 Mar 2009 - defect 1029 - Lookup sk1_area_no for SA Stores Area(SA Corporate area_no = 9951)
--  28 Feb 2012 - defect 4627 - Total Planning Project - add 4 new store measures
--                            - Remove 2 measures (pln_store_intk_fr_fast_selling,pln_store_intk_fr_rpl_selling)
--  12 Jan 2015 - defect 5043 - Total Planning - March release 2015 (drop 4 measures and add 3 new measures)
--  02 Mar 2015 - defect 5043 - Total Planning - March release 2015 (rename measure)
--  07 jul 2015 - Cater for DJ/Corporate area split

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
g_corp_sk1_area_no   number(9)     :=  0;
g_dj_sk1_area_no     number(9)     :=  0;
g_rec_out            rtl_area_dept_wk_mp_plan%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MP_020U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE MP PRODUCT AREA DEPT PLAN TYPE FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_area_dept_wk_mp_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_area_dept_wk_mp_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_fnd_chain_dept_wk_plan is
   select (CASE WHEN fnd_csw.chain_no in (10)  THEN g_corp_sk1_area_no
                else g_dj_sk1_area_no
            END) sk1_area_no, 
          dc.sk1_chain_no,
          dd.sk1_department_no,
          dp.sk1_plan_type_no,
          dcal.this_week_start_date,
          fnd_csw.fin_year_no,
          fnd_csw.fin_week_no,
          fnd_csw.pln_sales_qty,
          fnd_csw.pln_sales,
          fnd_csw.pln_sales_cost,
          fnd_csw.pln_sales_margin,
          fnd_csw.pln_net_mkdn,
          fnd_csw.pln_store_opening_stk_qty,
          fnd_csw.pln_store_opening_stk_selling,
          fnd_csw.pln_store_opening_stk_cost,
          fnd_csw.pln_store_closing_stk_qty,
          fnd_csw.pln_store_closing_stk_selling,
          fnd_csw.pln_store_closing_stk_cost,
          fnd_csw.pln_store_intk_qty,
          fnd_csw.pln_store_intk_selling,
          fnd_csw.pln_store_intk_cost,
          fnd_csw.pln_store_intk_tot_selling,
-- Drop 2 measures for Total Planning --> 4627
--          fnd_csw.pln_store_intk_fr_fast_selling,
--          fnd_csw.pln_store_intk_fr_rpl_selling,
-- Add 3 measures for Total Planning --> 5043
          fnd_csw.pln_store_clr_mkdn_selling,
          fnd_csw.pln_store_prom_mkdn_selling,
          fnd_csw.pln_store_price_adjustment,
-- Drop 4 measures for Total Planning --> 5043
--        fnd_csw.pln_store_intk_fran_selling,
--        fnd_csw.pln_store_latest_order_fr_sell,
--        fnd_csw.pln_store_inter_fr_selling,
--        fnd_csw.pln_store_inter_jv_selling,
-- Rename measure for Total Planning --> 5043
          fnd_csw.pln_store_inter_afr_selling
    from  fnd_chain_dept_wk_plan fnd_csw,
          dim_chain dc,
          dim_department dd,
          dim_plan_type dp,
          dim_calendar dcal
    where fnd_csw.chain_no          = dc.chain_no and
          fnd_csw.department_no     = dd.department_no and
          fnd_csw.plan_type_no      = dp.plan_type_no and
          fnd_csw.fin_year_no       = dcal.fin_year_no and
          fnd_csw.fin_week_no       = dcal.fin_week_no and
          dcal.fin_day_no           = 3 and
          fnd_csw.last_updated_date = g_date;

g_rec_in                   c_fnd_chain_dept_wk_plan%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_chain_dept_wk_plan%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_area_no                     := g_rec_in.sk1_area_no;
   g_rec_out.sk1_department_no               := g_rec_in.sk1_department_no;
   g_rec_out.sk1_plan_type_no                := g_rec_in.sk1_plan_type_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.fin_week_code                   := ('W'||g_rec_in.fin_year_no||g_rec_in.fin_week_no);
   g_rec_out.pln_sales_qty	                 := g_rec_in.pln_sales_qty;
   g_rec_out.pln_sales	                     := g_rec_in.pln_sales;
   g_rec_out.pln_sales_cost                  := g_rec_in.pln_sales_cost;
   g_rec_out.pln_net_mkdn	                   := g_rec_in.pln_net_mkdn;
   g_rec_out.pln_store_opening_stk_qty	     := g_rec_in.pln_store_opening_stk_qty;
   g_rec_out.pln_store_opening_stk_selling	 := g_rec_in.pln_store_opening_stk_selling;
   g_rec_out.pln_store_opening_stk_cost	     := g_rec_in.pln_store_opening_stk_cost;
   g_rec_out.pln_store_closing_stk_qty	     := g_rec_in.pln_store_closing_stk_qty;
   g_rec_out.pln_store_closing_stk_selling   := g_rec_in.pln_store_closing_stk_selling;
   g_rec_out.pln_store_closing_stk_cost	     := g_rec_in.pln_store_closing_stk_cost;
   g_rec_out.pln_store_intk_qty	             := g_rec_in.pln_store_intk_qty;
   g_rec_out.pln_store_intk_selling          := g_rec_in.pln_store_intk_selling;
   g_rec_out.pln_store_intk_cost	           := g_rec_in.pln_store_intk_cost;
   g_rec_out.pln_sales_margin                := g_rec_in.pln_sales_margin;
   g_rec_out.pln_store_open_stock_margin     := g_rec_in.pln_store_opening_stk_selling - g_rec_in.pln_store_opening_stk_cost;
   g_rec_out.pln_store_intk_margin           := g_rec_in.pln_store_intk_selling - g_rec_in.pln_store_intk_cost;
   g_rec_out.pln_store_closing_stock_margin  := g_rec_in.pln_store_closing_stk_selling - g_rec_in.pln_store_closing_stk_cost;
   g_rec_out.pln_store_intk_tot_selling      := g_rec_in.pln_store_intk_tot_selling;
    g_rec_out.last_updated_date              := g_date;
-- Drop 2 measures for Total Planning --> 4627
--   g_rec_out.pln_store_intk_fr_fast_selling	 := g_rec_in.pln_store_intk_fr_fast_selling;
--   g_rec_out.pln_store_intk_fr_rpl_selling   := g_rec_in.pln_store_intk_fr_rpl_selling;
-- Add 3 measures for Total Planning --> 5043
   g_rec_out.pln_store_clr_mkdn_selling	     := g_rec_in.pln_store_clr_mkdn_selling;
   g_rec_out.pln_store_prom_mkdn_selling	   := g_rec_in.pln_store_prom_mkdn_selling;
   g_rec_out.pln_store_price_adjustment	     := g_rec_in.pln_store_price_adjustment;
-- Drop 4 measures for Total Planning --> 5043
-- g_rec_out.pln_store_intk_fran_selling     := g_rec_in.pln_store_intk_fr_selling;
-- g_rec_out.pln_store_latest_order_fr_sell  := g_rec_in.pln_store_latest_order_fr_sell;
-- g_rec_out.pln_store_inter_fr_selling      := g_rec_in.pln_store_inter_fr_selling;
-- g_rec_out.pln_store_inter_jv_selling      := g_rec_in.pln_store_inter_jv_selling;
-- Rename measure for Total Planning --> 5043
   g_rec_out.pln_store_inter_afr_selling    := g_rec_in.pln_store_inter_afr_selling;

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
       insert into rtl_area_dept_wk_mp_plan values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_area_no||
                       ' '||a_tbl_insert(g_error_index).sk1_department_no||
                       ' '||a_tbl_insert(g_error_index).sk1_plan_type_no||
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
       update rtl_area_dept_wk_mp_plan
       set    fin_week_code	                 = a_tbl_update(i).fin_week_code,
              this_week_start_date           = a_tbl_update(i).this_week_start_date,
              pln_sales_qty	                 = a_tbl_update(i).pln_sales_qty,
              pln_sales	                     = a_tbl_update(i).pln_sales,
              pln_sales_cost	               = a_tbl_update(i).pln_sales_cost,
              pln_net_mkdn	                 = a_tbl_update(i).pln_net_mkdn,
              pln_store_opening_stk_qty	     = a_tbl_update(i).pln_store_opening_stk_qty,
              pln_store_opening_stk_selling	 = a_tbl_update(i).pln_store_opening_stk_selling,
              pln_store_opening_stk_cost	   = a_tbl_update(i).pln_store_opening_stk_cost,
              pln_store_closing_stk_qty	     = a_tbl_update(i).pln_store_closing_stk_qty,
              pln_store_closing_stk_selling  = a_tbl_update(i).pln_store_closing_stk_selling,
              pln_store_closing_stk_cost	   = a_tbl_update(i).pln_store_closing_stk_cost,
              pln_store_intk_qty	           = a_tbl_update(i).pln_store_intk_qty,
              pln_store_intk_selling	       = a_tbl_update(i).pln_store_intk_selling,
              pln_store_intk_cost   	       = a_tbl_update(i).pln_store_intk_cost,
              pln_sales_margin	             = a_tbl_update(i).pln_sales_margin,
              pln_store_open_stock_margin	   = a_tbl_update(i).pln_store_open_stock_margin,
              pln_store_intk_margin	         = a_tbl_update(i).pln_store_intk_margin,
              pln_store_closing_stock_margin = a_tbl_update(i).pln_store_closing_stock_margin,
              pln_store_intk_tot_selling     = a_tbl_update(i).pln_store_intk_tot_selling,
              last_updated_date              = a_tbl_update(i).last_updated_date,
-- Drop 2 measures for Total Planning --> 4627
--              pln_store_intk_fr_fast_selling = a_tbl_update(i).pln_store_intk_fr_fast_selling,
--              pln_store_intk_fr_rpl_selling	 = a_tbl_update(i).pln_store_intk_fr_rpl_selling,
-- Add 3 measures for Total Planning --> 5043
              pln_store_clr_mkdn_selling   	 = a_tbl_update(i).pln_store_clr_mkdn_selling,
              pln_store_prom_mkdn_selling    = a_tbl_update(i).pln_store_prom_mkdn_selling,
              pln_store_price_adjustment   	 = a_tbl_update(i).pln_store_price_adjustment,
-- Drop 4 measures for Total Planning --> 5043
--            pln_store_intk_fran_selling	   = a_tbl_update(i).pln_store_intk_fran_selling,
--            pln_store_latest_order_fr_sell = a_tbl_update(i).pln_store_latest_order_fr_sell,
--            pln_store_inter_fr_selling     = a_tbl_update(i).pln_store_inter_fr_selling,
--            pln_store_inter_jv_selling     = a_tbl_update(i).pln_store_inter_jv_selling,
-- Rename measure for Total Planning --> 5043
              pln_store_inter_afr_selling   = a_tbl_update(i).pln_store_inter_afr_selling
       where  sk1_area_no                    = a_tbl_update(i).sk1_area_no      and
              sk1_department_no              = a_tbl_update(i).sk1_department_no  and
              sk1_plan_type_no               = a_tbl_update(i).sk1_plan_type_no and
              fin_year_no                    = a_tbl_update(i).fin_year_no      and
              fin_week_no                    = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_area_no||
                       ' '||a_tbl_update(g_error_index).sk1_department_no||
                       ' '||a_tbl_update(g_error_index).sk1_plan_type_no||
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
   from   rtl_area_dept_wk_mp_plan
   where  sk1_area_no         = g_rec_out.sk1_area_no      and
          sk1_department_no   = g_rec_out.sk1_department_no  and
          sk1_plan_type_no    = g_rec_out.sk1_plan_type_no and
          fin_year_no         = g_rec_out.fin_year_no      and
          fin_week_no         = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_area_no         = g_rec_out.sk1_area_no and
            a_tbl_insert(i).sk1_department_no   = g_rec_out.sk1_department_no and
            a_tbl_insert(i).sk1_plan_type_no    = g_rec_out.sk1_plan_type_no and
            a_tbl_insert(i).fin_year_no         = g_rec_out.fin_year_no and
            a_tbl_insert(i).fin_week_no         = g_rec_out.fin_week_no
             then
            g_found := TRUE;
         end if;
      end loop;
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

    l_text := 'LOAD OF RTL_AREA_DEPT_WK_MP_PLAN EX FOUNDATION STARTED AT '||
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
-- Area lookup
--**************************************************************************************************
    select sk1_area_no
    into g_corp_sk1_area_no
    from dim_area
    where area_no = 9951;
    
    select sk1_area_no
    into g_dj_sk1_area_no
    from dim_area
    where area_no = 1003;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_chain_dept_wk_plan;
    fetch c_fnd_chain_dept_wk_plan bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_chain_dept_wk_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_chain_dept_wk_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

begin
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_AREA_DEPT_WK_MP_PLAN',estimate_percent=>1, DEGREE => 32);
  commit;
end;
   l_text := 'RTL_AREA_DEPT_WK_MP_PLAN completed';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
end wh_prf_mp_020u;
