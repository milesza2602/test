--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_210F_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_210F_LOAD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        September 2009
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast table in performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - rtl_loc_item_dy_rdf_sale
--               Output - RTL_LOC_ITEM_RDF_DYFCST_L2
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  29 Apr 2009 - A. Joshua : TD-1504 - split from wh_prf_rdf_001u
--  04 Nov 2009 - W Lyttle - td2443 - Rename fields on RTL_LOC_ITEM_RDF_DYFCST_L2
--                                   & RTL_LOC_ITEM_WK_RDF_FCST
--  08 Mar 2010 - A. Joshua : TD3600 - ensure RDF load caters for late sales (RDF Sales driving table not RDF FCST)
--  10 Mar 2010 - A. Joshua : TD3556 - new measures required as per ETL (abs_app_fcst_err_qty & abs_sys_fcst_err_qty)
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
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_rec_out            RTL_LOC_ITEM_RDF_DYFCST_L2%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_210F_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD RDF DAILY FCST FACTS EX RDF SALES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_LOC_ITEM_RDF_DYFCST_L2%rowtype index by binary_integer;
type tbl_array_u is table of RTL_LOC_ITEM_RDF_DYFCST_L2%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_dy_rdf_sale is

  select  /*+ parallel(fcst,4) parallel(sale,4) */ fcst.sk1_item_no,
          fcst.sk1_location_no,
          fcst.post_date,
          fcst.fcst_err_sls_dly_app_fcst_qty,
          fcst.fcst_err_sls_dly_sys_fcst_qty,
          sale.fcst_err_corr_sales_qty,
          di.handling_method_code
  from    rtl_loc_item_dy_rdf_sale sale
  join    RTL_LOC_ITEM_RDF_DYFCST_L2 fcst on
          fcst.sk1_item_no        = sale.sk1_item_no and
          fcst.sk1_location_no    = sale.sk1_location_no and
          fcst.post_date          = sale.post_date
          and sale.post_date between '30/JUN/14' and '06/JUL/14'
          and fcst.post_date between '30/JUN/14' and '06/JUL/14'
  join    dim_item di on
          di.sk1_item_no          = sale.sk1_item_no and
          di.handling_method_code in ('P', 'S')
          --di.handling_method_code = 'P'
  --where   sale.last_updated_date  = g_date
  ;

g_rec_in                   c_rtl_loc_item_dy_rdf_sale%rowtype;

-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_rdf_sale%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                              := null;
   g_rec_out.sk1_item_no                  := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no              := g_rec_in.sk1_location_no;
   g_rec_out.post_date                    := g_rec_in.post_date;
 --  g_rec_out.last_updated_date            := g_date;
   
   --##########################################################################--
   -- A distinction needs to bemade when processing Peri and Long Life records --
   -- as a single record on RTL_LOC_ITEM_RDF_DYFCST_L2 can only be Peri or LL  --
   -- and can't have values for both measures on one record                    --
   -- PERI - handling method = 'P'                                             --
   -- LL   - handling method = 'S'                                             --
   --##########################################################################--

   if g_rec_in.fcst_err_corr_sales_qty is null then
      g_rec_out.abs_dly_app_fcst_err_qty_peri   := null; 
      g_rec_out.abs_dly_sys_fcst_err_qty_peri   := null;
      
      g_rec_out.abs_app_fcst_err_qty_ll         := null;
      g_rec_out.abs_rltv_app_fcst_err_qty_ll    := null;
      g_rec_out.abs_sys_fcst_err_qty_ll         := null;
      g_rec_out.abs_rltv_sys_fcst_err_qty_ll    := null;

  else
      if g_rec_in.fcst_err_sls_dly_app_fcst_qty is null then
         g_rec_out.abs_dly_app_fcst_err_qty_peri  := null;
         g_rec_out.abs_app_fcst_err_qty_ll        := null;
         g_rec_out.abs_rltv_app_fcst_err_qty_ll   := null;
      else
         if g_rec_in.handling_method_code = 'P' then
            g_rec_out.abs_dly_app_fcst_err_qty_peri  := abs(g_rec_in.fcst_err_sls_dly_app_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
         else
            g_rec_out.abs_app_fcst_err_qty_ll        := abs(g_rec_in.fcst_err_sls_dly_app_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
            g_rec_out.abs_rltv_app_fcst_err_qty_ll   := abs(g_rec_in.fcst_err_sls_dly_app_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
         end if;
      end if;

      if g_rec_in.fcst_err_sls_dly_sys_fcst_qty is null then
         g_rec_out.abs_dly_sys_fcst_err_qty_peri  := null;
         g_rec_out.abs_sys_fcst_err_qty_ll        := null;
         g_rec_out.abs_rltv_sys_fcst_err_qty_ll   := null;
      else
         if g_rec_in.handling_method_code = 'P' then
            g_rec_out.abs_dly_sys_fcst_err_qty_peri  := abs(g_rec_in.fcst_err_sls_dly_sys_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
         else
            g_rec_out.abs_sys_fcst_err_qty_ll        := abs(g_rec_in.fcst_err_sls_dly_sys_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
            g_rec_out.abs_rltv_sys_fcst_err_qty_ll   := abs(g_rec_in.fcst_err_sls_dly_sys_fcst_qty - g_rec_in.fcst_err_corr_sales_qty);
         end if;
      end if;
   end if;

   if g_rec_in.handling_method_code = 'P' then
      g_rec_out.abs_app_fcst_err_qty  := g_rec_out.abs_dly_app_fcst_err_qty_peri;
      g_rec_out.abs_sys_fcst_err_qty  := g_rec_out.abs_dly_sys_fcst_err_qty_peri;
   else
      g_rec_out.abs_app_fcst_err_qty  := g_rec_out.abs_app_fcst_err_qty_ll;
      g_rec_out.abs_sys_fcst_err_qty  := g_rec_out.abs_sys_fcst_err_qty_ll;
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;
/*
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into RTL_LOC_ITEM_RDF_DYFCST_L2 values a_tbl_insert(i);

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
*/
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update RTL_LOC_ITEM_RDF_DYFCST_L2
       set    abs_dly_app_fcst_err_qty_peri  = a_tbl_update(i).abs_dly_app_fcst_err_qty_peri,
              abs_dly_sys_fcst_err_qty_peri  = a_tbl_update(i).abs_dly_sys_fcst_err_qty_peri,
            
              abs_app_fcst_err_qty_ll        = a_tbl_update(i).abs_app_fcst_err_qty_ll,
              abs_rltv_app_fcst_err_qty_ll   = a_tbl_update(i).abs_rltv_app_fcst_err_qty_ll,
              abs_sys_fcst_err_qty_ll        = a_tbl_update(i).abs_sys_fcst_err_qty_ll,
              abs_rltv_sys_fcst_err_qty_ll   = a_tbl_update(i).abs_rltv_sys_fcst_err_qty_ll,
              
              abs_app_fcst_err_qty           = a_tbl_update(i).abs_app_fcst_err_qty,
              abs_sys_fcst_err_qty           = a_tbl_update(i).abs_sys_fcst_err_qty
            --  last_updated_date              = a_tbl_update(i).last_updated_date
       where  sk1_location_no                = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                    = a_tbl_update(i).sk1_item_no      and
              post_date                      = a_tbl_update(i).post_date;

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
-- Check to see if item is present on table and update/insert accordingly
   -- FOR A PURE UPDATE WHERE THE CURSOR WILL ONLY RETURN RECORDS ALREADY ON THE OUTBOUND TABLE, AN EXISTANCE CHECK ISN'T NEEDED..
  -- select /*+ PARALLEL(FCST,4) */ count(1)
  -- into   g_count
  -- from   RTL_LOC_ITEM_RDF_DYFCST_L2 FCST
  -- where  FCST.sk1_location_no    = g_rec_out.sk1_location_no  and
  --        FCST.sk1_item_no        = g_rec_out.sk1_item_no      and
  --        FCST.post_date          = g_rec_out.post_date;

   --if g_count = 1 then
      g_found := TRUE;
   --end if;
/*
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).sk1_location_no = g_rec_out.sk1_location_no and
             a_tbl_insert(i).sk1_item_no     = g_rec_out.sk1_item_no and
             a_tbl_insert(i).post_date       = g_rec_out.post_date
             then
            g_found := TRUE;
         end if;
      end loop;
   end if;
*/
-- Place data into and array for later writing to table in bulk
--   if not g_found then
--      a_count_i               := a_count_i + 1;
--      a_tbl_insert(a_count_i) := g_rec_out;
--   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
--   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
--      local_bulk_insert;
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
    l_text := 'HISTORY FIX - LOAD RTL_LOC_ITEM_RDF_DYFCST_L2 EX TEMP TABLES STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '24/MAY/15';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_loc_item_dy_rdf_sale;
    fetch c_rtl_loc_item_dy_rdf_sale bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_rdf_sale bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_rdf_sale;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
--    local_bulk_insert;
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
       
end wh_prf_rdf_210f_load;
