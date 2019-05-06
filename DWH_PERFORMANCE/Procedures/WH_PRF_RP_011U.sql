--------------------------------------------------------
--  DDL for Procedure WH_PRF_RP_011U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RP_011U" (p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
--  Date:        January 2009
--  Author:      Alfonso Joshua
--  Purpose:     Create the weekly item rollup CHBD catalog table in the performance layer
--               with input ex RP daily item catalog table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rp_catlg
--               Output - rtl_loc_item_wk_rp_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  25 feb 2010 - TD-2595 - Change to insert only due to pre-requisite drop of last completed week partition truncation
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
g_start_week         integer       :=  0;
g_start_year         integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_wk_rp_catlg%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_this_week_start_date date        := trunc(sysdate);
g_this_week_end_date   date        := trunc(sysdate);
g_fin_week_code      varchar2(7);
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RP_011U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP THE RP WEEKLY ITEM CATALOG FACTS EX PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--
/*
-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_wk_rp_catlg%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_wk_rp_catlg%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
--
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
--
--
--
cursor c_rtl_loc_item_dy_rp_catlg is
   select   liw.sk1_item_no,
            liw.sk1_location_no,
            liw.sk1_product_category_no,
            liw.sk1_criticality_ranking_no,
            liw.sk1_top_mdm_item_catg_no,
            liw.sk1_avail_uda_value_no,
            liw.rpl_ind,
            cal.fin_year_no,
            cal.fin_week_no,
            max(liw.sk2_item_no)                  as sk2_item_no,
            max(liw.sk2_location_no)              as sk2_location_no,
            max(nvl(liw.ch_catalog_ind,0))        as ch_catalog_ind,
            sum(nvl(liw.ch_num_avail_days,0))     as ch_num_avail_days,
            sum(nvl(liw.ch_num_catlg_days,0))     as ch_num_catlg_days,
            sum(nvl(liw.reg_sales_qty_catlg,0))   as reg_sales_qty_catlg,
            sum(nvl(liw.reg_sales_catlg,0))       as reg_sales_catlg,
            sum(case
               when cal.fin_day_no = 7 then nvl(liw.reg_soh_qty_catlg,0)
               end )                              as reg_soh_qty_catlg,
            sum(case
               when cal.fin_day_no = 7 then nvl(liw.reg_soh_selling_catlg,0)
               end )                              as reg_soh_selling_catlg
   from     rtl_loc_item_dy_rp_catlg liw,
            dim_calendar cal
   where    liw.post_date = cal.calendar_date and
            liw.post_date  between g_this_week_start_date and g_this_week_end_date
   group by liw.sk1_item_no,
            liw.sk1_location_no,
            liw.sk1_product_category_no,
            liw.sk1_criticality_ranking_no,
            liw.sk1_top_mdm_item_catg_no,
            liw.sk1_avail_uda_value_no,
            liw.rpl_ind,
            cal.fin_year_no,
            cal.fin_week_no;

g_rec_in                   c_rtl_loc_item_dy_rp_catlg%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_rp_catlg%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
--
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_product_category_no         := g_rec_in.sk1_product_category_no;
   g_rec_out.sk1_criticality_ranking_no      := g_rec_in.sk1_criticality_ranking_no;
   g_rec_out.sk1_top_mdm_item_catg_no        := g_rec_in.sk1_top_mdm_item_catg_no;
   g_rec_out.sk1_avail_uda_value_no          := g_rec_in.sk1_avail_uda_value_no;
   g_rec_out.rpl_ind                         := g_rec_in.rpl_ind;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.fin_week_code                   := g_fin_week_code;
   g_rec_out.this_week_start_date            := g_this_week_start_date;
   g_rec_out.num_ch_catalog_ind              := g_rec_in.ch_catalog_ind;
   g_rec_out.ch_num_avail_days               := g_rec_in.ch_num_avail_days;
   g_rec_out.ch_num_catlg_days               := g_rec_in.ch_num_catlg_days;
   g_rec_out.reg_sales_qty_catlg             := g_rec_in.reg_sales_qty_catlg;
   g_rec_out.reg_sales_catlg                 := g_rec_in.reg_sales_catlg;
   g_rec_out.reg_soh_qty_catlg               := g_rec_in.reg_soh_qty_catlg;
   g_rec_out.reg_soh_selling_catlg           := g_rec_in.reg_soh_selling_catlg;
   g_rec_out.last_update_date                := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
--
end local_address_variables;
--
--
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_loc_item_wk_rp_catlg values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_product_category_no||
                       ' '||a_tbl_insert(g_error_index).sk1_criticality_ranking_no||
                       ' '||a_tbl_insert(g_error_index).sk1_top_mdm_item_catg_no||
                       ' '||a_tbl_insert(g_error_index).sk1_avail_uda_value_no||
                       ' '||a_tbl_insert(g_error_index).rpl_ind||
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;
--

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_loc_item_wk_rp_catlg
       set    fin_week_code              = a_tbl_update(i).fin_week_code,
              this_week_start_date       = a_tbl_update(i).this_week_start_date,
              num_ch_catalog_ind         = a_tbl_update(i).num_ch_catalog_ind,
              ch_num_avail_days          = a_tbl_update(i).ch_num_avail_days,
              ch_num_catlg_days          = a_tbl_update(i).ch_num_catlg_days,
              reg_sales_qty_catlg        = a_tbl_update(i).reg_sales_qty_catlg,
              reg_sales_catlg            = a_tbl_update(i).reg_sales_catlg,
              reg_soh_qty_catlg          = a_tbl_update(i).reg_soh_qty_catlg,
              reg_soh_selling_catlg      = a_tbl_update(i).reg_soh_selling_catlg,
              last_update_date           = a_tbl_update(i).last_update_date
       where  sk1_location_no            = a_tbl_update(i).sk1_location_no and
              sk1_item_no                = a_tbl_update(i).sk1_item_no and
              sk1_product_category_no    = a_tbl_update(i).sk1_product_category_no and
              sk1_criticality_ranking_no = a_tbl_update(i).sk1_criticality_ranking_no and
              sk1_top_mdm_item_catg_no   = a_tbl_update(i).sk1_top_mdm_item_catg_no and
              sk1_avail_uda_value_no     = a_tbl_update(i).sk1_avail_uda_value_no and
              rpl_ind                    = a_tbl_update(i).rpl_ind and
              fin_year_no                = a_tbl_update(i).fin_year_no and
              fin_week_no                = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_product_category_no||
                       ' '||a_tbl_update(g_error_index).sk1_criticality_ranking_no||
                       ' '||a_tbl_update(g_error_index).sk1_top_mdm_item_catg_no||
                       ' '||a_tbl_update(g_error_index).sk1_avail_uda_value_no||
                       ' '||a_tbl_update(g_error_index).rpl_ind||
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;
--
--
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_loc_item_wk_rp_catlg
   where  sk1_location_no            = g_rec_out.sk1_location_no and
          sk1_item_no                = g_rec_out.sk1_item_no     and
          sk1_product_category_no    = g_rec_out.sk1_product_category_no    and
          sk1_criticality_ranking_no = g_rec_out.sk1_criticality_ranking_no and
          sk1_top_mdm_item_catg_no   = g_rec_out.sk1_top_mdm_item_catg_no   and
          sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no     and
          rpl_ind                    = g_rec_out.rpl_ind     and
          fin_year_no                = g_rec_out.fin_year_no and
          fin_week_no                = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
--
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_location_no            = g_rec_out.sk1_location_no and
            a_tbl_insert(i).sk1_item_no                = g_rec_out.sk1_item_no and
            a_tbl_insert(i).sk1_product_category_no    = g_rec_out.sk1_product_category_no and
            a_tbl_insert(i).sk1_criticality_ranking_no = g_rec_out.sk1_criticality_ranking_no and
            a_tbl_insert(i).sk1_top_mdm_item_catg_no   = g_rec_out.sk1_top_mdm_item_catg_no and
            a_tbl_insert(i).sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no and
            a_tbl_insert(i).rpl_ind                    = g_rec_out.rpl_ind and
            a_tbl_insert(i).fin_year_no                = g_rec_out.fin_year_no and
            a_tbl_insert(i).fin_week_no                = g_rec_out.fin_week_no
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

*/
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

    l_text := 'ROLLUP OF RTL_LOC_ITEM_WK_RP_CATLG EX FOUNDATION STARTED AT '||
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

    select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code
    into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code
    from   dim_calendar
    where  calendar_date = g_date;

    l_text := 'START WEEK/YEAR OF ROLLUP IS:- '||g_start_week||' of '||g_start_year;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
    INSERT /*+ APPEND */ INTO dwh_performance.rtl_loc_item_wk_rp_catlg liw
    select  
            liw.sk1_location_no,
            liw.sk1_item_no,
            liw.sk1_product_category_no,
            liw.sk1_criticality_ranking_no,
            liw.sk1_top_mdm_item_catg_no,
            liw.rpl_ind,
            liw.sk1_avail_uda_value_no,
            cal.fin_year_no, 
            cal.fin_week_no,
            cal.fin_week_code,
            cal.this_week_start_date,
            max(liw.sk2_item_no)                  as sk2_item_no,
            max(liw.sk2_location_no)              as sk2_location_no,
            max(nvl(liw.ch_catalog_ind,0))        as ch_catalog_ind,
            sum(nvl(liw.ch_num_avail_days,0))     as ch_num_avail_days,
            sum(nvl(liw.ch_num_catlg_days,0))     as ch_num_catlg_days,
            sum(nvl(liw.reg_sales_qty_catlg,0))   as reg_sales_qty_catlg,
            sum(nvl(liw.reg_sales_catlg,0))       as reg_sales_catlg,
            sum(case 
               when cal.fin_day_no = 7 then nvl(liw.reg_soh_qty_catlg,0)  
               end )                              as reg_soh_qty_catlg,
            sum(case 
               when cal.fin_day_no = 7 then nvl(liw.reg_soh_selling_catlg,0)  
               end )                              as reg_soh_selling_catlg,
            cal.this_week_end_date
   from     rtl_loc_item_dy_rp_catlg liw,
            dim_calendar cal
   where    liw.post_date = cal.calendar_date and
            liw.post_date  between g_this_week_start_date and g_this_week_end_date
   group by cal.fin_year_no,
            cal.fin_week_no,
            liw.sk1_location_no,
            liw.sk1_item_no,
            liw.sk1_product_category_no,
            liw.sk1_criticality_ranking_no,
            liw.sk1_top_mdm_item_catg_no,
            liw.rpl_ind,
            liw.sk1_avail_uda_value_no,
            cal.fin_week_code,
            cal.this_week_start_date,
            cal.this_week_end_date ;

   g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

   commit;
/*
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_loc_item_dy_rp_catlg;
    fetch c_rtl_loc_item_dy_rp_catlg bulk collect into a_stg_input limit g_forall_limit;
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
--
         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_rp_catlg bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_rp_catlg;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
--
    local_bulk_insert;
    local_bulk_update;

*/

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
--
--
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
--
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
end wh_prf_rp_011u;
