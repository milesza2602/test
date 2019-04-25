--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_630U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_630U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2008
--  Author:      Alastair de Wet
--  Purpose:     Create stock ledger rollup fact table in the performance layer
--               with input ex stock ledger  table from foundation layer.
--  Tables:      Input  - fnd_rtl_stock_ledger_wk
--               Output - rtl_stock_ledger_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  25 March 2009 - QC 1249 -  load data into RTL_STOCK_LEDGER_WK from FND_RTL_STOCK_LEDGER_WK
--                             where the department on FND_RTL_STOCK_LEDGER_WK exists on DIM_ITEM
--
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
g_rec_out            rtl_stock_ledger_wk%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_630U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE STOCK LEDGER WK DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_stock_ledger_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_stock_ledger_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_rtl_stock_ledger_wk is
   with dept_list as
   (
   select distinct department_no
   from   dim_item
   )
   select sum(nvl(slw.OPN_STK_RETAIL,0)) as	OPN_STK_RETAIL	,
          sum(nvl(slw.OPN_STK_COST,0)) as	OPN_STK_COST	,
          sum(nvl(slw.STOCK_ADJ_RETAIL,0)) as	STOCK_ADJ_RETAIL	,
          sum(nvl(slw.STOCK_ADJ_COST,0)) as	STOCK_ADJ_COST	,
          sum(nvl(slw.PURCH_RETAIL,0)) as	PURCH_RETAIL	,
          sum(nvl(slw.PURCH_COST,0)) as	PURCH_COST	,
          sum(nvl(slw.RTV_RETAIL,0)) as	RTV_RETAIL	,
          sum(nvl(slw.RTV_COST,0)) as	RTV_COST	,
          sum(nvl(slw.TSF_IN_RETAIL,0)) as	TSF_IN_RETAIL	,
          sum(nvl(slw.TSF_IN_COST,0)) as	TSF_IN_COST	,
          sum(nvl(slw.TSF_OUT_RETAIL,0)) as	TSF_OUT_RETAIL	,
          sum(nvl(slw.TSF_OUT_COST,0)) as	TSF_OUT_COST	,
          sum(nvl(slw.NET_SALES_RETAIL,0)) as	NET_SALES_RETAIL	,
          sum(nvl(slw.NET_SALES_RETAIL_EXCL_VAT,0)) as	NET_SALES_RETAIL_EXCL_VAT	,
          sum(nvl(slw.NET_SALES_COST,0)) as	NET_SALES_COST	,
          sum(nvl(slw.RETURNS_RETAIL,0)) as	RETURNS_RETAIL	,
          sum(nvl(slw.RETURNS_COST,0)) as	RETURNS_COST	,
          sum(nvl(slw.MARKUP_RETAIL,0)) as	MARKUP_RETAIL	,
          sum(nvl(slw.MARKUP_CAN_RETAIL,0)) as	MARKUP_CAN_RETAIL	,
          sum(nvl(slw.CLEAR_MARKDOWN_RETAIL,0)) as	CLEAR_MARKDOWN_RETAIL	,
          sum(nvl(slw.PERM_MARKDOWN_RETAIL,0)) as	PERM_MARKDOWN_RETAIL	,
          sum(nvl(slw.PROM_MARKDOWN_RETAIL,0)) as	PROM_MARKDOWN_RETAIL	,
          sum(nvl(slw.MARKDOWN_CAN_RETAIL,0)) as	MARKDOWN_CAN_RETAIL	,
          sum(nvl(slw.SHRINKAGE_RETAIL,0)) as	SHRINKAGE_RETAIL	,
          sum(nvl(slw.SHRINKAGE_COST,0)) as	SHRINKAGE_COST	,
          sum(nvl(slw.CLS_STK_RETAIL,0)) as	CLS_STK_RETAIL	,
          sum(nvl(slw.CLS_STK_COST,0)) as	CLS_STK_COST	,
          sum(nvl(slw.COST_VARIANCE_AMT,0)) as	COST_VARIANCE_AMT	,
          sum(nvl(slw.HTD_GAFS_RETAIL,0)) as	HTD_GAFS_RETAIL	,
          sum(nvl(slw.HTD_GAFS_COST,0)) as	HTD_GAFS_COST	,
          sum(nvl(slw.STOCKTAKE_ADJ_RETAIL,0)) as	STOCKTAKE_ADJ_RETAIL	,
          sum(nvl(slw.STOCKTAKE_ADJ_COST,0)) as	STOCKTAKE_ADJ_COST	,
          sum(nvl(slw.RECLASS_IN_COST,0)) as	RECLASS_IN_COST	,
          sum(nvl(slw.RECLASS_IN_RETAIL,0)) as	RECLASS_IN_RETAIL	,
          sum(nvl(slw.RECLASS_OUT_COST,0)) as	RECLASS_OUT_COST	,
          sum(nvl(slw.RECLASS_OUT_RETAIL,0)) as	RECLASS_OUT_RETAIL	,
          dc.fin_year_no,
          dc.fin_week_no,
          dd.sk1_department_no,
          dl.sk1_location_no,
          max(dlh.sk2_location_no) as sk2_location_no,
          max(dc.fin_week_code) as fin_week_code,
          max(dc.this_week_start_date) as this_week_start_date
   from   fnd_rtl_stock_ledger_wk slw,
          dim_department dd,
          dim_location dl,
          dim_location_hist dlh,
          dim_calendar dc,
          dept_list
   where  slw.fin_week_end_date         > ((select today_date from dim_control) - 10) and
          slw.department_no             = dd.department_no  and
          slw.location_no               = dl.location_no   and
          slw.location_no               = dlh.location_no and
          slw.department_no             = dept_list.department_no  and
          slw.fin_week_end_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          slw.fin_week_end_date         = dc.calendar_date
   group by dd.sk1_department_no, dl.sk1_location_no, dc.fin_week_no, dc.fin_year_no   ;


g_rec_in             c_fnd_rtl_stock_ledger_wk%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_stock_ledger_wk%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_department_no               := g_rec_in.sk1_department_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.fin_week_code                   := g_rec_in.fin_week_code;
   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.opn_stk_retail                  := g_rec_in.opn_stk_retail;
   g_rec_out.opn_stk_cost                    := g_rec_in.opn_stk_cost;
   g_rec_out.stock_adj_retail                := g_rec_in.stock_adj_retail;
   g_rec_out.stock_adj_cost                  := g_rec_in.stock_adj_cost;
   g_rec_out.purch_retail                    := g_rec_in.purch_retail;
   g_rec_out.purch_cost                      := g_rec_in.purch_cost;
   g_rec_out.rtv_retail                      := g_rec_in.rtv_retail;
   g_rec_out.rtv_cost                        := g_rec_in.rtv_cost;
   g_rec_out.tsf_in_retail                   := g_rec_in.tsf_in_retail;
   g_rec_out.tsf_in_cost                     := g_rec_in.tsf_in_cost;
   g_rec_out.tsf_out_retail                  := g_rec_in.tsf_out_retail;
   g_rec_out.tsf_out_cost                    := g_rec_in.tsf_out_cost;
   g_rec_out.net_sales_retail                := g_rec_in.net_sales_retail;
   g_rec_out.net_sales_retail_excl_vat       := g_rec_in.net_sales_retail_excl_vat;
   g_rec_out.net_sales_cost                  := g_rec_in.net_sales_cost;
   g_rec_out.returns_retail                  := g_rec_in.returns_retail;
   g_rec_out.returns_cost                    := g_rec_in.returns_cost;
   g_rec_out.markup_retail                   := g_rec_in.markup_retail;
   g_rec_out.markup_can_retail               := g_rec_in.markup_can_retail;
   g_rec_out.clear_markdown_retail           := g_rec_in.clear_markdown_retail;
   g_rec_out.perm_markdown_retail            := g_rec_in.perm_markdown_retail;
   g_rec_out.prom_markdown_retail            := g_rec_in.prom_markdown_retail;
   g_rec_out.markdown_can_retail             := g_rec_in.markdown_can_retail;
   g_rec_out.shrinkage_retail                := g_rec_in.shrinkage_retail;
   g_rec_out.shrinkage_cost                  := g_rec_in.shrinkage_cost;
   g_rec_out.cls_stk_retail                  := g_rec_in.cls_stk_retail;
   g_rec_out.cls_stk_cost                    := g_rec_in.cls_stk_cost;
   g_rec_out.cost_variance_amt               := g_rec_in.cost_variance_amt;
   g_rec_out.htd_gafs_retail                 := g_rec_in.htd_gafs_retail;
   g_rec_out.htd_gafs_cost                   := g_rec_in.htd_gafs_cost;
   g_rec_out.stocktake_adj_retail            := g_rec_in.stocktake_adj_retail;
   g_rec_out.stocktake_adj_cost              := g_rec_in.stocktake_adj_cost;
   g_rec_out.reclass_in_cost                 := g_rec_in.reclass_in_cost;
   g_rec_out.reclass_in_retail               := g_rec_in.reclass_in_retail;
   g_rec_out.reclass_out_cost                := g_rec_in.reclass_out_cost;
   g_rec_out.reclass_out_retail              := g_rec_in.reclass_out_retail;
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
       insert into rtl_stock_ledger_wk values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_department_no||
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
       update rtl_stock_ledger_wk
       set    fin_week_code                   = a_tbl_update(i).fin_week_code,
              this_week_start_date            = a_tbl_update(i).this_week_start_date,
              opn_stk_retail                  = a_tbl_update(i).opn_stk_retail,
              opn_stk_cost                    = a_tbl_update(i).opn_stk_cost,
              stock_adj_retail                = a_tbl_update(i).stock_adj_retail,
              stock_adj_cost                  = a_tbl_update(i).stock_adj_cost,
              purch_retail                    = a_tbl_update(i).purch_retail,
              purch_cost                      = a_tbl_update(i).purch_cost,
              rtv_retail                      = a_tbl_update(i).rtv_retail,
              rtv_cost                        = a_tbl_update(i).rtv_cost,
              tsf_in_retail                   = a_tbl_update(i).tsf_in_retail,
              tsf_in_cost                     = a_tbl_update(i).tsf_in_cost,
              tsf_out_retail                  = a_tbl_update(i).tsf_out_retail,
              tsf_out_cost                    = a_tbl_update(i).tsf_out_cost,
              net_sales_retail                = a_tbl_update(i).net_sales_retail,
              net_sales_retail_excl_vat       = a_tbl_update(i).net_sales_retail_excl_vat,
              net_sales_cost                  = a_tbl_update(i).net_sales_cost,
              returns_retail                  = a_tbl_update(i).returns_retail,
              returns_cost                    = a_tbl_update(i).returns_cost,
              markup_retail                   = a_tbl_update(i).markup_retail,
              markup_can_retail               = a_tbl_update(i).markup_can_retail,
              clear_markdown_retail           = a_tbl_update(i).clear_markdown_retail,
              perm_markdown_retail            = a_tbl_update(i).perm_markdown_retail,
              prom_markdown_retail            = a_tbl_update(i).prom_markdown_retail,
              markdown_can_retail             = a_tbl_update(i).markdown_can_retail,
              shrinkage_retail                = a_tbl_update(i).shrinkage_retail,
              shrinkage_cost                  = a_tbl_update(i).shrinkage_cost,
              cls_stk_retail                  = a_tbl_update(i).cls_stk_retail,
              cls_stk_cost                    = a_tbl_update(i).cls_stk_cost,
              cost_variance_amt               = a_tbl_update(i).cost_variance_amt,
              htd_gafs_retail                 = a_tbl_update(i).htd_gafs_retail,
              htd_gafs_cost                   = a_tbl_update(i).htd_gafs_cost,
              stocktake_adj_retail            = a_tbl_update(i).stocktake_adj_retail,
              stocktake_adj_cost              = a_tbl_update(i).stocktake_adj_cost,
              reclass_in_cost                 = a_tbl_update(i).reclass_in_cost,
              reclass_in_retail               = a_tbl_update(i).reclass_in_retail,
              reclass_out_cost                = a_tbl_update(i).reclass_out_cost,
              reclass_out_retail              = a_tbl_update(i).reclass_out_retail,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no      and
              sk1_department_no               = a_tbl_update(i).sk1_department_no    and
              fin_week_no                     = a_tbl_update(i).fin_week_no      and
              fin_year_no                     = a_tbl_update(i).fin_year_no ;


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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_department_no||
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
   from   rtl_stock_ledger_wk
   where  sk1_location_no     = g_rec_out.sk1_location_no      and
          sk1_department_no   = g_rec_out.sk1_department_no    and
          fin_year_no         = g_rec_out.fin_year_no      and
          fin_week_no         = g_rec_out.fin_week_no  ;

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

    l_text := 'LOAD OF rtl_stock_ledger_wk EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_stock_ledger_wk;
    fetch c_fnd_rtl_stock_ledger_wk bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_stock_ledger_wk bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_stock_ledger_wk;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;



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
end wh_prf_corp_630u;
