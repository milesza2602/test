--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_195U_FIX1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_195U_FIX1" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      Alastair de Wet
--  Purpose:     Create RMS Food Catalog fact table in the performance layer
--               with input ex Itself to calculate availability figures. (Update only)
--  Tables:      Input  - rtl_loc_item_dy_catalog
--               Output - rtl_loc_item_dy_catalog
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  01 Mar 2017 - Add Customer Availibility Measure and Num Facing (Chg4256 - A Joshua)
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
g_rec_out            rtl_loc_item_dy_catalog_n%rowtype;
g_found              boolean;
g_sdn_in_qty         fnd_rtl_loc_item_dy_rms_sale.sdn_in_qty%type;
g_waste_qty          fnd_rtl_loc_item_dy_rms_sale.waste_qty%type;
g_boh_adj_qty_minus1 rtl_loc_item_dy_catalog_n.boh_adj_qty %type;
g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_sod_date           date ;
-- -- AJ FIX
g_start_date         date          := '21 nov 16';
g_end_date           date          := '21 nov 16';

g_sk1_department_no  dim_department.sk1_department_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_195U_FIX1';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOOD CATALOG EX ITSELF CALCULATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_catalog_n%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_catalog_n%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--

cursor c_rtl_loc_item_dy_catalog is
   select /*+ parallel (lid,6) full (di) full (dl) full (diu) */ 
          lid.sk1_item_no,
          lid.sk1_location_no,
          lid.calendar_date,
          nvl(lid.boh_adj_qty,0) as boh_adj_qty,
          lid.fd_num_catlg_days,
          lid.fd_num_avail_days,
          di.item_no,
          di.pack_item_ind,
          dl.location_no,
          di.handling_method_code ,
          dl.st_open_date,
          dl.active_store_ind,
          dl.loc_type,
          nvl(diu.product_group_scaling_desc_501,'X')  product_group_scaling_desc_501,
          nvl(diu.availability_desc_540,'X') availability_desc_540,
          nvl(diu.product_class_desc_507 ,'X') product_class_desc_507,
          nvl(diu.new_line_indicator_desc_3502,'X') new_line_indicator_desc_3502 ,
          lid.product_status_1_code,
          lid.num_shelf_life_days, -- Chg4256
          nvl(lid.num_facings,0) num_facings 
   from   rtl_loc_item_dy_catalog_n lid,
          dim_item di,
          dim_location dl,
          dim_item_uda diu
--          dim_calendar dc
   where  --lid.calendar_date              = dc.calendar_date and
--          lid.calendar_date              between g_start_date and g_end_date and
          lid.calendar_date              = g_start_date and 
          lid.sk1_item_no                = diu.sk1_item_no(+) and
          lid.sk1_item_no                = di.sk1_item_no and
          lid.sk1_location_no            = dl.sk1_location_no ;

g_rec_in                   c_rtl_loc_item_dy_catalog%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_catalog%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   if g_rec_in.pack_item_ind                  = 0     and
      g_rec_in.active_store_ind               = 1     and
      g_rec_in.product_group_scaling_desc_501 = 'N'   and
      g_rec_in.availability_desc_540          = 'Yes' and
      g_rec_in.fd_num_catlg_days              = 1     and
      g_rec_in.product_class_desc_507        <> 'H'   and
     (g_rec_in.new_line_indicator_desc_3502 <> 'First Week New'  or
     (g_rec_in.new_line_indicator_desc_3502  = 'First Week New' and g_rec_in.handling_method_code  = 'S')) then
      g_rec_out.fd_num_cust_catlg_adj        := g_rec_in.fd_num_catlg_days;
       if g_rec_in.st_open_date is not null then
          if g_rec_in.handling_method_code  = 'S'  and
           g_rec_in.st_open_date > g_date then
               g_rec_out.fd_num_cust_catlg_adj := 0;
           end if;
        end if;
        if g_rec_in.product_status_1_code =  4  then
           g_rec_out.fd_num_cust_catlg_adj := 0;
        end if;
    else
      g_rec_out.fd_num_cust_catlg_adj       := 0;
    end if;
    
    if g_rec_in.loc_type = 'S' and g_rec_in.active_store_ind = 1 then
    begin

      g_waste_qty  := 0;

      select nvl(waste_qty,0) as waste_qty
      into   g_waste_qty
      from   fnd_rtl_loc_item_dy_rms_sale
      where  post_date   = g_rec_in.calendar_date and
             item_no     = g_rec_in.item_no and
             location_no = g_rec_in.location_no;

      exception
             when no_data_found then
                g_waste_qty  := 0;
    end;
    end if;
   
    if g_rec_out.fd_num_cust_catlg_adj > 0 then -- Chg12260
      if g_rec_in.num_facings > 0 then
         if (g_rec_in.boh_adj_qty + abs(g_waste_qty)) > g_rec_in.num_facings then 
            g_rec_out.fd_cust_avail := 1;
         else
            g_rec_out.fd_cust_avail := (g_rec_in.boh_adj_qty + abs(g_waste_qty)) / g_rec_in.num_facings; 
         end if;
      elsif g_rec_in.num_shelf_life_days >= 3 then
         if g_rec_in.boh_adj_qty + abs(g_waste_qty) < 3 then 
            g_rec_out.fd_cust_avail := 0; 
         else
            g_rec_out.fd_cust_avail := 1;
         end if;
      else
         g_rec_out.fd_cust_avail := g_rec_out.fd_num_avail_days_adj;
      end if;
   else
      g_rec_out.fd_cust_avail := 0;
   end if;

   g_rec_out.sk1_item_no     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no := g_rec_in.sk1_location_no;
   g_rec_out.calendar_date   := g_rec_in.calendar_date;
   
-- End Chg4256
--   dbms_output.put_line('num_facings '||g_rec_out.num_facings);
--   dbms_output.put_line('fd_num_cust_avail_adj '||g_rec_out.fd_num_cust_avail_adj);

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
/*
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_loc_item_dy_catalog values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update rtl_loc_item_dy_catalog_n
       set    fd_num_cust_catlg_adj           = a_tbl_update(i).fd_num_cust_catlg_adj,       
              fd_cust_avail                   = a_tbl_update(i).fd_cust_avail 
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no      and
              calendar_date                   = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).calendar_date;
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

    l_text := 'LOAD OF rtl_loc_item_dy_catalog_n  FOODS EX ITSELF STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml'; --chg4256
    
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- AJ FIX    
    l_text := 'DATES BEING PROCESSED :- '||g_start_date|| ' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_yesterday := g_date - 1;

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
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_catalog bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_catalog;
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
end wh_prf_corp_195u_fix1;
