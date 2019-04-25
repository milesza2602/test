--------------------------------------------------------
--  DDL for Procedure WH_PRF_STATS_TEST_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_STATS_TEST_NEW" 
                                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        November 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create the daily CHBD item catalog table with RMS stock in the performance layer
--               with input ex RP table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rp_catlg
--               Output - rtl_loc_item_dy_rp_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;
g_recs_updated           integer       :=  0;
g_error_count            number        :=  0;
g_error_index            number        :=  0;
g_count                  number        :=  0;
g_found                  boolean;
g_rec_out                rtl_loc_item_dy_rp_catlg%rowtype;
g_soh_qty_decatlg        number        :=  0;
g_soh_qty                number        :=  0;
g_soh_selling            number        :=  0;
g_fin_week_no            number        :=  0;
g_fin_year_no            number        :=  0;
g_item_no_decatlg        number(18,0)  :=  0;
g_sk1_item_no_decatlg    number(18,0)  :=  0;
g_sql                    varchar2(5000);
g_date                   date;
g_this_week_start_date   date;
g_this_week_end_date     date;
g_part_name              varchar2(32)  := 'RLIDRMSSTKC_';

type stkcurtyp           is ref cursor;
stk_cv                   stkcurtyp;

l_message                sys_dwh_errlog.log_text%type;
l_module_name            sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RP_001U_STATS';
l_name                   sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name            sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name            sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name         sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                   sys_dwh_log.log_text%type ;
l_description            sys_dwh_log_summary.log_description%type  := 'STATS TEST';
l_process_type           sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i         is table of rtl_loc_item_dy_rp_catlg%rowtype index by binary_integer;
type tbl_array_u         is table of rtl_loc_item_dy_rp_catlg%rowtype index by binary_integer;
a_tbl_insert             tbl_array_i;
a_tbl_update             tbl_array_u;
a_empty_set_i            tbl_array_i;
a_empty_set_u            tbl_array_u;

a_count                  integer       := 0;
a_count_i                integer       := 0;
a_count_u                integer       := 0;

cursor c_rtl_loc_item_dy_rp_catlg is
   select r.*, trunc(sysdate) active_from_date, trunc(sysdate) active_to_date, i.item_no
   from   rtl_loc_item_dy_rp_catlg r, dim_item i
   where  1 = 2
    and   r.sk1_item_no = i.sk1_item_no;

g_rec_in         c_rtl_loc_item_dy_rp_catlg%rowtype;

-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_rp_catlg%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk1_avail_uda_value_no          := 0;
   g_rec_out.rpl_ind                         := g_rec_in.rpl_ind;
   g_rec_out.sk1_product_category_no         := g_rec_in.sk1_product_category_no;
   g_rec_out.sk1_criticality_ranking_no      := g_rec_in.sk1_criticality_ranking_no;
   g_rec_out.sk1_top_mdm_item_catg_no        := g_rec_in.sk1_top_mdm_item_catg_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.reg_soh_qty_catlg               := g_rec_in.reg_soh_qty_catlg;
   g_rec_out.reg_soh_selling_catlg           := g_rec_in.reg_soh_selling_catlg;

   if g_rec_in.active_from_date              <= g_this_week_start_date and
      g_rec_in.active_to_date                >= g_this_week_end_date then
      g_rec_out.ch_catalog_ind               := 1;
      g_rec_out.ch_num_catlg_days            := 1;  
       
      if g_rec_in.reg_soh_qty_catlg > 0 then
         g_rec_out.ch_num_avail_days         := 1;
      else
         begin
            g_item_no_decatlg                := 0;
            g_sk1_item_no_decatlg            := 0;
            
            select lnk.item_no_decatlg, b.sk1_item_no
            into   g_item_no_decatlg, g_sk1_item_no_decatlg
            from   fnd_rtl_item_wk_mdm_catlg_lnk lnk, dim_item b
            where  lnk.fin_year_no     = g_fin_year_no
             and   lnk.fin_week_no     = g_fin_week_no
             and   lnk.item_no         = g_rec_in.item_no
             and   lnk.item_no_decatlg = b.item_no;
             
            exception
               when no_data_found then
                  g_item_no_decatlg          := 0;
                  g_sk1_item_no_decatlg      := 0;
         end;
 
         if g_item_no_decatlg <> 0 then
            g_soh_qty_decatlg := 0;
             
            begin  
               select nvl(reg_soh_qty,0)
               into   g_soh_qty_decatlg
               from   rtl_loc_item_dy_rms_stock
               where  sk1_item_no     = g_sk1_item_no_decatlg
                and   sk1_location_no = g_rec_in.sk1_location_no
                and   post_date       = g_rec_in.post_date;
                
               exception
                  when no_data_found then
                     g_soh_qty_decatlg := 0;
            end;
               
            if g_soh_qty_decatlg > 0 then
               g_rec_out.ch_num_avail_days := 1;
            else 
               g_rec_out.ch_num_avail_days := 0;
            end if;
         else
            g_rec_out.ch_num_avail_days := 0;
         end if;
      end if;
   else
      g_rec_out.ch_catalog_ind              := 0;
      g_rec_out.ch_num_catlg_days           := 0;  
      g_rec_out.ch_num_avail_days           := 0;
   end if;

   g_rec_out.last_update_date                := g_date;

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
       insert into rtl_loc_item_dy_rp_catlg values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).post_date||
                       ' '||a_tbl_insert(g_error_index).sk1_avail_uda_value_no||
                       ' '||a_tbl_insert(g_error_index).rpl_ind;
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
       update rtl_loc_item_dy_rp_catlg
       set    ch_catalog_ind             = a_tbl_update(i).ch_catalog_ind,
              ch_num_avail_days          = a_tbl_update(i).ch_num_avail_days,
              ch_num_catlg_days          = a_tbl_update(i).ch_num_catlg_days,
              reg_soh_selling_catlg      = a_tbl_update(i).reg_soh_selling_catlg,
              reg_soh_qty_catlg          = a_tbl_update(i).reg_soh_qty_catlg,
              sk2_location_no            = a_tbl_update(i).sk2_location_no,
              sk2_item_no                = a_tbl_update(i).sk2_item_no,
              last_update_date           = a_tbl_update(i).last_update_date
       where  sk1_location_no            = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                = a_tbl_update(i).sk1_item_no      and
              sk1_product_category_no    = a_tbl_update(i).sk1_product_category_no and
              sk1_criticality_ranking_no = a_tbl_update(i).sk1_criticality_ranking_no and
              sk1_top_mdm_item_catg_no   = a_tbl_update(i).sk1_top_mdm_item_catg_no and
              post_date                  = a_tbl_update(i).post_date and
              sk1_avail_uda_value_no     = a_tbl_update(i).sk1_avail_uda_value_no and
              rpl_ind                    = a_tbl_update(i).rpl_ind;

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
                       ' '||a_tbl_update(g_error_index).post_date||
                       ' '||a_tbl_update(g_error_index).sk1_avail_uda_value_no||
                       ' '||a_tbl_update(g_error_index).rpl_ind;

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
   select count(1)
   into   g_count
   from   rtl_loc_item_dy_rp_catlg
   where  sk1_location_no            = g_rec_out.sk1_location_no  and
          sk1_item_no                = g_rec_out.sk1_item_no      and
          sk1_product_category_no    = g_rec_out.sk1_product_category_no    and
          sk1_criticality_ranking_no = g_rec_out.sk1_criticality_ranking_no and
          sk1_top_mdm_item_catg_no   = g_rec_out.sk1_top_mdm_item_catg_no   and
          post_date                  = g_rec_out.post_date and
          sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no and
          rpl_ind                    = g_rec_out.rpl_ind;

   if g_count = 1 then
      g_found := TRUE;
   end if;

   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).sk1_location_no            = g_rec_out.sk1_location_no and
             a_tbl_insert(i).sk1_item_no                = g_rec_out.sk1_item_no and
             a_tbl_insert(i).sk1_product_category_no    = g_rec_out.sk1_product_category_no and
             a_tbl_insert(i).sk1_criticality_ranking_no = g_rec_out.sk1_criticality_ranking_no and
             a_tbl_insert(i).sk1_top_mdm_item_catg_no   = g_rec_out.sk1_top_mdm_item_catg_no and
             a_tbl_insert(i).post_date                  = g_rec_out.post_date and
             a_tbl_insert(i).sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no and
             a_tbl_insert(i).rpl_ind                    = g_rec_out.rpl_ind then
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

--      commit;
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
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF RTL_LOC_ITEM_DY_RP_CATLG EX FOUNDATION STARTED AT '||
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
-- Look up dates from dim_calendar
--**************************************************************************************************
    select this_week_start_date, this_week_end_date, fin_week_no, fin_year_no
    into g_this_week_start_date, g_this_week_end_date, g_fin_week_no, g_fin_year_no
    from dim_calendar
    where calendar_date = g_date;

    l_text := 'MDM WEEK PROCESSED - '||g_fin_week_no||' '||g_fin_year_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_part_name := g_part_name||to_char((g_date + 1),'ddmmyy');
    l_text := 'PARTITION NAME '||g_part_name;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_sql := 'with mdm as '||
      '(select item_no, fin_year_no, fin_week_no, 1 mdm_ind '||
       'from   fnd_rtl_item_wk_mdm '||
       'where  chbd_bus_unit_top_50_item_ind = 1 and '||
       '       fin_year_no                   = '||g_fin_year_no||' and '||
       '       fin_week_no                   = '||g_fin_week_no||
       ' union '||
       'select item_no, fin_year_no, fin_week_no, 2 mdm_ind '||
       'from   fnd_rtl_item_wk_mdm '||
       'where  chbd_group_top_20_item_ind = 1 and '||
       '       fin_year_no                   = '||g_fin_year_no||' and '||
       '       fin_week_no                   = '||g_fin_week_no||
       ' union '||
       'select item_no, fin_year_no, fin_week_no, 3 mdm_ind '||
       'from   fnd_rtl_item_wk_mdm '||
       'where  chbd_dept_top_5_item_ind = 1 and '||
       '       fin_year_no                   = '||g_fin_year_no||' and '||
       '       fin_week_no                   = '||g_fin_week_no||
       ' union '||
       'select item_no, fin_year_no, fin_week_no, 4 mdm_ind '||
       'from   fnd_rtl_item_wk_mdm '||
       'where (chbd_bus_unit_top_50_item_ind = 1 or '||
       '       chbd_group_top_20_item_ind = 1 or '||
       '       chbd_dept_top_5_item_ind = 1) and '||
       '       fin_year_no                   = '||g_fin_year_no||' and '||
       '       fin_week_no                   = '||g_fin_week_no||
       ' union '||
       'select /*+ USE_HASH (fnd_ctl) */ '||
       '       distinct fnd_ctl.item_no, dc.fin_year_no, dc.fin_week_no, 5 mdm_ind '||
       'from   fnd_rtl_loc_item_dy_rp_catlg fnd_ctl, '||
       '       dim_calendar dc '||
       'where  fnd_ctl.post_date = '''||g_date||''' and '||
       '       fnd_ctl.post_date = dc.calendar_date) '||

   'select /*+ USE_HASH (mdm, fnd_lid, di, dih, stk) */ '||
   '       dl.sk1_location_no, '||
   '       di.sk1_item_no, '||
   '       dpc.sk1_product_category_no, '||
   '       dcr.sk1_criticality_ranking_no, '||
   '       dmdm.sk1_top_mdm_item_catg_no, '||
   '       di.rpl_ind, '||
   '       0 sk1_avail_uda_value_no, '||
   '       fnd_lid.post_date, '||
   '       dih.sk2_item_no, '||
   '       dlh.sk2_location_no, '||
   '       0 ch_catalog_ind, '||
   '       0 ch_num_avail_days, '||
   '       0 ch_num_catlg_days, '||
   '       0 reg_sales_qty_catlg, '||
   '       0 reg_sales_catlg, '||
   '       nvl(stk.reg_soh_qty,0) reg_soh_qty_catlg, '||
   '       nvl(stk.reg_soh_selling,0) reg_soh_selling_catlg, '||
   '       '''||g_date||''' last_update_date, '||
   '       fnd_lid.active_from_date, '||
   '       fnd_lid.active_to_date, '||
   '       fnd_lid.item_no '||
   'from   fnd_rtl_loc_item_dy_rp_catlg fnd_lid '||
   'join   dim_item di on '||
   '       fnd_lid.item_no                      = di.item_no and '||
   '       di.rpl_ind                           = 1 '||
   'join   dim_location dl on '||
   '       fnd_lid.location_no                  = dl.location_no '||
   'join   dim_item_hist dih on '||
   '       fnd_lid.item_no                      = dih.item_no and '||
   '       fnd_lid.post_date     between dih.sk2_active_from_date and dih.sk2_active_to_date '||
   'join   dim_location_hist dlh on '||
   '       fnd_lid.location_no                  = dlh.location_no and '||
   '       fnd_lid.post_date     between dlh.sk2_active_from_date and dlh.sk2_active_to_date '||
   'join   mdm on '||
   '       fnd_lid.item_no                      = mdm.item_no '||
   'join   dim_calendar dc on '||
   '       fnd_lid.post_date                    = dc.calendar_date and '||
   '       mdm.fin_year_no                      = dc.fin_year_no and '||
   '       mdm.fin_week_no                      = dc.fin_week_no '||
   'join   dim_top_mdm_item_category dmdm on '||
   '       mdm.mdm_ind                          = dmdm.top_mdm_item_catg_no '||
   'join   dim_product_category dpc on '||
   '       nvl(substr(fnd_lid.criticality_code,1,1),0) = dpc.product_category_code '||
   'join   dim_criticality_ranking dcr on '||
   '       nvl(substr(fnd_lid.criticality_code,2,1),0) = dcr.criticality_ranking_no '||
   'left outer join   rtl_loc_item_dy_rms_stock  subpartition ('||g_part_name||') stk on '||
   '       stk.sk1_item_no                      = di.sk1_item_no and '||
   '       stk.sk1_location_no                  = dl.sk1_location_no and '||
   '       stk.post_date                        = fnd_lid.post_date '||
   'where  fnd_lid.post_date                    = '''||g_date||'''';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open stk_cv for g_sql;
    fetch stk_cv bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 200000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch stk_cv bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close stk_cv;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    commit;
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

end wh_prf_stats_test_new;
