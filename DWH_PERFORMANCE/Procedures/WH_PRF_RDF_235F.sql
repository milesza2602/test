--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_235F
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_235F" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Mar 2011
--  Author:      Alastair de Wet
--  Purpose:     Load Daily forecast Table LL 13 week avrg measure in performance layer
--               with input ex Weekly FCST  performance layer.
--  Tables:      Input  - RTL_LOC_ITEM_RDF_WKFCST_L1
--               Output - RTL_LOC_ITEM_RDF_DYFCST_L1
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
g_sub                integer       :=  0;   
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date ;
g_end_date           date ;
g_cal_date           date ;
g_peri_end_date      date ;
g_fin_week_no        number;
g_fin_year_no        number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_235F';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF RDF 13 WKLY AVRG EX RDF WKLY TO DYFCST ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype index by binary_integer;
type tbl_array_u is table of RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_RTL_LOC_ITEM_RDF_DYFCST_L1 is

  select    fcst.sk1_item_no,
            fcst.sk1_location_no,
            fcst.sk2_item_no,
            fcst.sk2_location_no,
            sales_wk_app_fcst_qty_av,
            sales_wk_app_fcst_qty_flt_av
  from      RTL_LOC_ITEM_RDF_WKFCST_L1 fcst
  join      dim_item di 
  on        di.sk1_item_no = fcst.sk1_item_no  
  where     fin_week_no             = g_fin_week_no and
            fin_year_no             = g_fin_year_no and
            di.handling_method_code = 'S'    and
            sales_wk_app_fcst_qty_av is not null ;

g_rec_in                   c_RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype;

-- For input bulk collect --
type stg_array is table of c_RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                   := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no               := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                   := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no               := g_rec_in.sk2_location_no;
   g_rec_out.post_date                     := g_cal_date;

   g_rec_out.sales_dly_app_fcst_qty_av     := g_rec_in.sales_wk_app_fcst_qty_av ;
   g_rec_out.sales_dly_app_fcst_qty_flt_av := g_rec_in.sales_wk_app_fcst_qty_flt_av ;
   g_rec_out.last_updated_date             := g_date;

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
       insert into RTL_LOC_ITEM_RDF_DYFCST_L1 values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update RTL_LOC_ITEM_RDF_DYFCST_L1
       set    sales_dly_app_fcst_qty_av     = a_tbl_update(i).sales_dly_app_fcst_qty_av,
              sales_dly_app_fcst_qty_flt_av = a_tbl_update(i).sales_dly_app_fcst_qty_flt_av,
              last_updated_date             = a_tbl_update(i).last_updated_date
       where  sk1_location_no               = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                   = a_tbl_update(i).sk1_item_no      and
              post_date                     = a_tbl_update(i).post_date      ;

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
                       ' '||a_tbl_update(g_error_index).post_date;
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
   g_count :=0;

   select count(1) 
   into   g_count 
   from   RTL_LOC_ITEM_RDF_DYFCST_L1
   where  post_date          = g_rec_out.post_date and
          sk1_item_no        = g_rec_out.sk1_item_no and
          sk1_location_no    = g_rec_out.sk1_location_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
-- Place data into and array for later writing to table in bulk

   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count                 := a_count + 1;
   else
      a_count_u                := a_count_u + 1;
      a_tbl_update(a_count_u)  := g_rec_out;
      a_count                  := a_count + 1;
   end if;



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
    l_text := 'LOAD RTL_LOC_ITEM_RDF_DYFCST_L1 EX WK FCST STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '24/MAY/15';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    g_date := '18 jan 2011';
  
    g_cal_date := g_date + 1;

    select fin_year_no,fin_week_no
    into   g_fin_year_no,g_fin_week_no
    from   dim_calendar
    where calendar_date = g_cal_date  ;


    l_text := 'YR/WK Being processed = '||g_fin_year_no|| '   '||g_fin_week_no ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Date being written to day table = '||g_cal_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_RTL_LOC_ITEM_RDF_DYFCST_L1;
    fetch c_RTL_LOC_ITEM_RDF_DYFCST_L1 bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_RTL_LOC_ITEM_RDF_DYFCST_L1 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_RTL_LOC_ITEM_RDF_DYFCST_L1;
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

end wh_prf_rdf_235f;
