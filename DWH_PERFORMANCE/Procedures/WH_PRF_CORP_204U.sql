--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_204U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_204U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        May 2009
--  Author:      Alastair de Wet
--  Purpose:     Create RMS Food stockcount fact table in the performance layer
--               with input ex RMS Food rtl_loc_item_dy_catalog table from foundation layer (ECR2).
--  Tables:      Input  - rtl_loc_item_dy_catalog
--               Output - rtl_loc_item_dy_fd_stk_count
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_rec_out            rtl_loc_item_dy_fd_stk_count%rowtype;
g_found              boolean;
g_sk1_stk_count_source_code dim_fd_stock_count_source.sk1_stk_count_source_code%type;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_eergister          date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_204U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STOCK COUNT EX CATALOG ECR2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_fd_stk_count%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_fd_stk_count%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_rtl_loc_item_dy_catalog is
   select cat.sk1_item_no,
          cat.sk1_location_no,
          cat.calendar_date,
          cat.soh_adj_qty,
          nvl(sprs.waste_qty,0) waste_qty,
          nvl(dns.sales_qty,0)  sales_qty,
          nvl(dns.sdn_in_qty,0) sdn_in_qty
   from   rtl_loc_item_dy_catalog cat,
          dim_item di,
          dim_item_uda diu,
          rtl_loc_item_dy_rms_dense dns,
          rtl_loc_item_dy_rms_sparse sprs
   where
          cat.calendar_date          = g_eergister             and
          cat.sk1_item_no            = di.sk1_item_no          and
          cat.sk1_item_no            = diu.sk1_item_no         and
          cat.sk1_item_no            = dns.sk1_item_no         and
          cat.sk1_location_no        = dns.sk1_location_no     and
          cat.calendar_date          = dns.post_date           and
          cat.sk1_item_no            = sprs.sk1_item_no(+)     and
          cat.sk1_location_no        = sprs.sk1_location_no(+) and
          cat.calendar_date          = sprs.post_date(+)       and

          di.display_method_code     = 'P'            and
          di.product_class           <> 1             and
          AVAILABILITY_DESC_540      = 'Yes'          and
          cat.soh_adj_qty            > 2              and
          dns.sdn_in_qty             > 0
          ;


g_rec_in                   c_rtl_loc_item_dy_catalog%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_catalog%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_stk_count_source_code       := g_sk1_stk_count_source_code;
   g_rec_out.stocktake_date                  := g_rec_in.calendar_date + 1;

   g_rec_out.soh_qty                         := g_rec_in.soh_adj_qty;
   g_rec_out.count_ind                       := 0;
   g_rec_out.count_qty                       := null ;

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
       insert into rtl_loc_item_dy_fd_stk_count values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).stocktake_date;
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
       update rtl_loc_item_dy_fd_stk_count
       set    count_ind                       = a_tbl_update(i).count_ind,
              count_qty                       = a_tbl_update(i).count_qty,
              soh_qty                         = a_tbl_update(i).soh_qty,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no      and
              stocktake_date                  = a_tbl_update(i).stocktake_date   and
              sk1_stk_count_source_code       = a_tbl_update(i).sk1_stk_count_source_code;

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
                       ' '||a_tbl_update(g_error_index).stocktake_date;
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
   from   rtl_loc_item_dy_fd_stk_count
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no      and
          stocktake_date     = g_rec_out.stocktake_date   and
          sk1_stk_count_source_code = g_rec_out.sk1_stk_count_source_code;

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

    l_text := 'LOAD OF rtl_loc_item_dy_fd_stk_count  FOODS EX FOUNDATION STARTED AT '||
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
    g_eergister := g_date - 1;

--**************************************************************************************************
-- Look up SK1 from dim_fd_stock_count_source
--**************************************************************************************************
    select sk1_stk_count_source_code
    into   g_sk1_stk_count_source_code
    from   dim_fd_stock_count_source
    where  stk_count_source_code = 'ECR2';

    l_text := 'SK1 stock count source is :- '||g_sk1_stk_count_source_code;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_loc_item_dy_catalog;
    fetch c_rtl_loc_item_dy_catalog bulk collect into a_stg_input limit g_forall_limit;
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

         if g_rec_in.sales_qty + g_rec_in.waste_qty = g_rec_in.sdn_in_qty then
            local_write_output;
         end if;
      end loop;
    fetch c_rtl_loc_item_dy_catalog bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_catalog;
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
end wh_prf_corp_204u;