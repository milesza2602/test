--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_744U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_744U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2009
--  Author:      Alastair de Wet
--  Purpose:     Create depot item wk  fact table in the performance layer
--               with input ex CAM LLDF  table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_wk_fd
--               Output - rtl_depot_item_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
--  07 Jan 2011 - QC 4000     - Filtered estimate stock for input to cubes stock cover calc

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
g_case_selling_excl_vat   rtl_zone_item_om.case_selling_excl_vat%type;
g_rec_out            rtl_depot_item_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_wk_end        date;
g_last_wk_start      date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_744U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DEPOT ITEM WK EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_depot_item_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_depot_item_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--QC 4000 read uda table and get fields shorts_longlife_desc_542 for filtering
cursor c_fnd_rtl_loc_item_wk is
   select liw.*,
          di.sk1_item_no,
          dl.sk1_location_no,
          dl.sk1_fd_zone_group_zone_no ,
          dih.sk2_item_no ,
          dlh.sk2_location_no,
          dc.fin_week_code,
          dc.this_week_start_date,
          nvl(diu.shorts_longlife_desc_542,' ') shorts_longlife_desc_542
   from   fnd_rtl_loc_item_wk_fd liw,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh,
          dim_calendar dc,
          dim_item_uda diu
   where  liw.item_no                   = di.item_no  and
          liw.item_no                   = diu.item_no(+) and
          liw.location_no               = dl.location_no   and
          liw.fin_year_no               = dc.fin_year_no and
          liw.fin_week_no               = dc.fin_week_no and
          dc.fin_day_no                 = 4 and
          liw.item_no                   = dih.item_no and
          dc.calendar_date              between dih.sk2_active_from_date and dih.sk2_active_to_date and
          liw.location_no               = dlh.location_no and
          dc.calendar_date              between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          liw.last_updated_date         = g_date;




g_rec_in             c_fnd_rtl_loc_item_wk%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_wk%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.fin_week_code                   := g_rec_in.fin_week_code;
   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.ship_ti                         := g_rec_in.ship_ti;
   g_rec_out.ship_hi                         := g_rec_in.ship_hi;
   g_rec_out.dcs_load_type                   := g_rec_in.dcs_load_type;
   g_rec_out.est_ll_cases                    := g_rec_in.est_ll_cases;
   g_rec_out.est_ll_selling                  := g_rec_in.est_ll_selling;
   g_rec_out.est_ll_qty                      := g_rec_in.est_ll_qty;
   g_rec_out.num_proj_cover_wks              := g_rec_in.num_proj_cover_wks;
   g_rec_out.depot_fwd_boh_qty               := g_rec_in.depot_fwd_boh_qty;
   g_rec_out.recom_order_cases               := g_rec_in.recom_order_cases;
   g_rec_out.confrm_order_cases              := g_rec_in.confrm_order_cases;
   g_rec_out.corr_sales_cases                := g_rec_in.corr_sales_cases;
   g_rec_out.num_depot_cover_weeks           := g_rec_in.num_depot_cover_weeks;
   g_rec_out.num_max_dc_cover_weeks          := g_rec_in.num_max_dc_cover_weeks;
   g_rec_out.min_order_qty                   := g_rec_in.min_order_qty;
   g_rec_out.est_ll_dc_cover_cases              := null;
--QC 4000 filter on field shorts_longlife_desc_542
   if g_rec_in.shorts_longlife_desc_542 = 'Yes' then
      g_rec_out.est_ll_dc_cover_cases           := g_rec_in.est_ll_cases ;
   end if;
--   g_rec_out.product_status_code             := g_rec_in.product_status_code;
  Case
        when g_rec_in.product_status_code = 'A'
            then g_rec_out.product_status_code             :=1 ;
        when g_rec_in.product_status_code = 'D'
            then g_rec_out.product_status_code             :=4 ;
        when g_rec_in.product_status_code = 'N'
            then g_rec_out.product_status_code             :=14 ;
        when g_rec_in.product_status_code = 'O'
            then g_rec_out.product_status_code             :=15;
        when g_rec_in.product_status_code = 'U'
            then g_rec_out.product_status_code             :=21 ;
        when g_rec_in.product_status_code = 'X'
            then g_rec_out.product_status_code             :=24;
        when g_rec_in.product_status_code = 'Z'
            then g_rec_out.product_status_code             :=26;
        when g_rec_in.product_status_code is null
            then g_rec_out.product_status_code             :=0;
        else g_rec_out.product_status_code             :=0 ;
  end case;

   g_rec_out.recom_order_selling   := 0;
   g_rec_out.confrm_order_selling  := 0;
   g_rec_out.corr_sales_selling    := 0;

   begin
      g_case_selling_excl_vat := 0;

      select case_selling_excl_vat
      into   g_case_selling_excl_vat
      from   rtl_zone_item_om
      where  sk1_zone_group_zone_no = g_rec_in.sk1_fd_zone_group_zone_no and
             sk1_item_no            = g_rec_in.sk1_item_no;

      g_rec_out.recom_order_selling      := g_rec_out.recom_order_cases * g_case_selling_excl_vat;
      g_rec_out.confrm_order_selling     := g_rec_out.confrm_order_cases * g_case_selling_excl_vat;
      g_rec_out.corr_sales_selling       := g_rec_out.corr_sales_cases * g_case_selling_excl_vat;

      exception
         when no_data_found then
           l_message := 'No data found on rtl_zone_item_om lookup '||g_rec_in.item_no||' '||g_rec_in.location_no;
           dwh_log.record_error(l_module_name,sqlcode,l_message);

   end;


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
       insert into rtl_depot_item_wk values a_tbl_insert(i);

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
       update rtl_depot_item_wk
       set    fin_week_code                   = a_tbl_update(i).fin_week_code,
              this_week_start_date            = a_tbl_update(i).this_week_start_date,
              supplier_no                     = a_tbl_update(i).supplier_no,
              ship_ti                         = a_tbl_update(i).ship_ti,
              ship_hi                         = a_tbl_update(i).ship_hi,
              dcs_load_type                   = a_tbl_update(i).dcs_load_type,
              est_ll_cases                    = a_tbl_update(i).est_ll_cases,
              est_ll_selling                  = a_tbl_update(i).est_ll_selling,
              est_ll_qty                      = a_tbl_update(i).est_ll_qty,
              num_proj_cover_wks              = a_tbl_update(i).num_proj_cover_wks,
              depot_fwd_boh_qty               = a_tbl_update(i).depot_fwd_boh_qty,
              recom_order_cases               = a_tbl_update(i).recom_order_cases,
              confrm_order_cases              = a_tbl_update(i).confrm_order_cases,
              corr_sales_cases                = a_tbl_update(i).corr_sales_cases,
              num_depot_cover_weeks           = a_tbl_update(i).num_depot_cover_weeks,
              num_max_dc_cover_weeks          = a_tbl_update(i).num_max_dc_cover_weeks,
              min_order_qty                   = a_tbl_update(i).min_order_qty,
              product_status_code             = a_tbl_update(i).product_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              recom_order_selling             = a_tbl_update(i).recom_order_selling,
              confrm_order_selling            = a_tbl_update(i).confrm_order_selling,
              corr_sales_selling              = a_tbl_update(i).corr_sales_selling,
              est_ll_dc_cover_cases           = a_tbl_update(i).est_ll_dc_cover_cases

       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no     and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no         and
              fin_year_no                     = a_tbl_update(i).fin_year_no         and
              fin_week_no                     = a_tbl_update(i).fin_week_no   ;

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
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_depot_item_wk
   where  sk1_location_no    = g_rec_out.sk1_location_no   and
          sk1_item_no        = g_rec_out.sk1_item_no    and
          fin_year_no        = g_rec_out.fin_year_no  and
          fin_week_no        = g_rec_out.fin_week_no;

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

    l_text := 'LOAD OF rtl_depot_item_wk EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


    select this_wk_end_date, last_wk_start_date
    into   g_this_wk_end, g_last_wk_start
    from   dim_control;

    l_text := 'DATE RANGE BEING PROCESSED = '||g_last_wk_start|| ' TO '||g_this_wk_end;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_wk;
    fetch c_fnd_rtl_loc_item_wk bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_item_wk bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_wk;
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
end wh_prf_corp_744u;
