--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_222F_WRONG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_222F_WRONG" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Dec 2010
--  Author:      Alastair de Wet
--  Purpose:     Load Wkly forecast Table LL filtered measure table in performance layer
--               with input ex FCST & Dim tables from performance layer.
--  Tables:      Input  - RTL_LOC_ITEM_RDF_WKFCST_L1 & dim_itgem/dim_department
--               Output - RTL_LOC_ITEM_RDF_WKFCST_L1
--
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
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;

g_rec_out            RTL_LOC_ITEM_RDF_WKFCST_L1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_222F';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF RDF WKLY FCST FACTS EX RDF WKLY FCST/DIM_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_LOC_ITEM_RDF_WKFCST_L1%rowtype index by binary_integer;
type tbl_array_u is table of RTL_LOC_ITEM_RDF_WKFCST_L1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_dy_rdf_fcst is

  select  fcst.sk1_item_no,
          fcst.sk1_location_no,
          fcst.fin_year_no,
          fcst.fin_week_no,
          fcst.sales_wk_app_fcst_qty,
          di.handling_method_code,
          di.department_no,
          fcst.sales_wk_app_fcst_qty_flt
  from    RTL_LOC_ITEM_RDF_WKFCST_L1 fcst
  join    dim_item di on
          di.sk1_item_no          = fcst.sk1_item_no
  where   fcst.last_updated_date  = g_date and
          fcst.fin_year_no        = g_fin_year_no and
          fcst.fin_week_no        = g_fin_week_no and
          nvl(fcst.sales_wk_app_fcst_qty,0) <> nvl(fcst.sales_wk_app_fcst_qty_flt,0.1)and
           di.department_no not in
          (select  dd.department_no
           from dim_department dd
           where
           (jv_dept_ind            = 1 or
            book_magazine_dept_ind = 1 or
            non_core_dept_ind      = 1 or
            gifting_dept_ind       = 1 or
            packaging_dept_ind     = 1 or
            bucket_dept_ind        = 1) and
            dd.business_unit_no = 50 ) ;


g_rec_in                   c_rtl_loc_item_dy_rdf_fcst%rowtype;

-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_rdf_fcst%rowtype;
a_stg_input      stg_array;

cursor c_finyr_wk is
  select  unique fin_year_no,fin_week_no
  from    RTL_LOC_ITEM_RDF_WKFCST_L1 fcst
  where   fcst.last_updated_date  = g_date
  order by fin_year_no,fin_week_no;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                  := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no              := g_rec_in.sk1_location_no;
   g_rec_out.fin_year_no                  := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                  := g_rec_in.fin_week_no;
   g_rec_out.sales_wk_app_fcst_qty_flt    := g_rec_in.sales_wk_app_fcst_qty ;
   g_rec_out.last_updated_date            := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update RTL_LOC_ITEM_RDF_WKFCST_L1
       set    sales_wk_app_fcst_qty_flt     = a_tbl_update(i).sales_wk_app_fcst_qty_flt,
              last_updated_date             = a_tbl_update(i).last_updated_date
       where  sk1_location_no               = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                   = a_tbl_update(i).sk1_item_no      and
              fin_year_no                   = a_tbl_update(i).fin_year_no      and
              fin_week_no                   = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
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
   g_found := TRUE;
   g_count :=0;

-- Place data into and array for later writing to table in bulk

   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;


   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
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
    l_text := 'LOAD RTL_LOC_ITEM_RDF_WKFCST_L1 EX SELF TABLES STARTED '||
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
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
  for finyr_wk_rec in c_finyr_wk
    loop
    g_fin_year_no := finyr_wk_rec.fin_year_no;
    g_fin_week_no := finyr_wk_rec.fin_week_no;
    L_TEXT := 'Outer loop through program = '||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||
    g_fin_year_no||'  '||g_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    open c_rtl_loc_item_dy_rdf_fcst;
    fetch c_rtl_loc_item_dy_rdf_fcst bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 250000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_rdf_fcst bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_rdf_fcst;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

      local_bulk_update;
      A_TBL_INSERT  := A_EMPTY_SET_I;
      A_TBL_UPDATE  := A_EMPTY_SET_U;
      A_COUNT_I     := 0;
      A_COUNT_U     := 0;
      A_COUNT       := 0;
      COMMIT;
END LOOP;
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

end wh_prf_rdf_222f_WRONG;
