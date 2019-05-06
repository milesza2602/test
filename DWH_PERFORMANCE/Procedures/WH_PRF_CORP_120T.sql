--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_120T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_120T" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        SEPT 2008
--  Author:      Alastair de Wet
--  Purpose:     Create ACtual store rcpt fact table in the performance layer
--               with input ex RMS Shipment  table from foundation layer.
--  Tables:      Input  - fnd_rtl_shipment
--               Output - W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
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
g_rec_out            W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype;
g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_120T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS SHIPMENT DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- 'With' creates a sub query which is treated as a table called 'lid_list' and used in the from clause of the main query.
-- This option is known as subquery factoring and eliminates the need to create a temp table of the 1st result set.
--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
cursor c_fnd_rtl_shipment is
   with lid_list as
   (
   select item_no,to_loc_no,shp.actl_rcpt_date
   from   fnd_rtl_shipment shp,
          dwh_performance.dim_location dl
   where  shp.last_updated_date  = g_date and
       --   shipment_no between 2630000 and 2665000 and
          shp.to_loc_no          = dl.location_no and
          dl.loc_type            = 'S' and
          shp.actl_rcpt_date     is not null
 --         AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   group by item_no,to_loc_no,shp.actl_rcpt_date
   )

   select sum(nvl(shp.received_qty,0)) as received_qty,
          
          --sum(nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + di.vat_rate_perc))) as received_selling,
          -- NEW CODE TO DERIVE TAX PERCENTAGE FROM VARIOUS SOURCES
          sum(case when rli.tax_perc is null then
                case when dl.vat_region_no = 1000 then
                    nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + di.VAT_RATE_PERC))
                else
                    nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + dl.default_tax_region_no_perc))
                end
              else 
                nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + rli.tax_perc  ))                               
              end) as received_selling,
           
          sum(nvl(shp.received_qty,0) * shp.cost_price) as received_cost,
          trunc(shp.actl_rcpt_date) as actl_rcpt_date,
          di.sk1_item_no,
          max(di.sk1_department_no) as sk1_department_no,
          dl.sk1_location_no,
          max(dl.chain_no) as chain_no,
          max(dl.sk1_fd_zone_group_zone_no) as sk1_fd_zone_group_zone_no ,
          max(dlh.sk2_location_no) as sk2_location_no,
          max(dih.sk2_item_no) as sk2_item_no
          
   from   fnd_rtl_shipment shp
          join lid_list  on shp.item_no                                 = lid_list.item_no
                        and shp.to_loc_no                               = lid_list.to_loc_no   
                        and shp.actl_rcpt_date                          = lid_list.actl_rcpt_date 
          join dim_item di on lid_list.item_no                          = di.item_no      
                                  
          join dim_item_hist dih on lid_list.item_no                    = dih.item_no  
          join dim_location dl on lid_list.to_loc_no                    = dl.location_no 
          join dim_location_hist dlh on lid_list.to_loc_no              = dlh.location_no     
          left outer join rtl_location_item rli on  rli.sk1_item_no     = di.sk1_item_no       
                                               and  rli.sk1_location_no = dl.sk1_location_no   
          --left outer join fnd_vr vr on di.item_no            = vr.item_no
          --                                    and dl.vat_region_no      = vr.vat_region_no
 
   where  shp.received_qty            <> 0                      
     and  shp.received_qty            is not null               
     and  lid_list.actl_rcpt_date     between dih.sk2_active_from_date and dih.sk2_active_to_date 
     and  lid_list.actl_rcpt_date     between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
     --and vr.active_from_date       < g_date 
          
   group by trunc(shp.actl_rcpt_date), di.sk1_item_no, dl.sk1_location_no ;


--   where  last_updated_date >= g_yesterday;
-- order by only where sequencing is essential to the correct loading of data

g_rec_in             c_fnd_rtl_shipment%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_shipment%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.post_date                       := g_rec_in.actl_rcpt_date;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.actl_store_rcpt_qty             := g_rec_in.received_qty;
   g_rec_out.actl_store_rcpt_selling         := g_rec_in.received_selling;
   g_rec_out.actl_store_rcpt_cost            := g_rec_in.received_cost;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.actl_store_rcpt_fr_cost         := '';

   if g_rec_in.chain_no = 20 then
      begin
         select debtors_commission_perc
         into   g_debtors_commission_perc
         from   rtl_loc_dept_dy
         where  sk1_location_no       = g_rec_out.sk1_location_no and
                sk1_department_no     = g_rec_in.sk1_department_no and
                post_date             = g_rec_out.post_date;
         exception
            when no_data_found then
              g_debtors_commission_perc := 0;
      end;
     if g_debtors_commission_perc is null then
         g_debtors_commission_perc := 0;
      end if;
      g_rec_out.actl_store_rcpt_fr_cost  := nvl(g_rec_out.actl_store_rcpt_cost,0) + round((nvl(g_rec_out.actl_store_rcpt_cost,0) * g_debtors_commission_perc / 100),2);
   end if;

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
       insert into W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q values a_tbl_insert(i);

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
       update W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q
       set    actl_store_rcpt_qty             = a_tbl_update(i).actl_store_rcpt_qty,
              actl_store_rcpt_selling         = a_tbl_update(i).actl_store_rcpt_selling,
              actl_store_rcpt_cost            = a_tbl_update(i).actl_store_rcpt_cost,
              actl_store_rcpt_fr_cost         = a_tbl_update(i).actl_store_rcpt_fr_cost,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no      and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no    and
              post_date                       = a_tbl_update(i).post_date ;

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
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q 
   where  sk1_location_no     = g_rec_out.sk1_location_no      and
          sk1_item_no         = g_rec_out.sk1_item_no    and
          post_date           = g_rec_out.post_date;

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

    l_text := 'LOAD OF W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '19/AUG/17';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_shipment;
    fetch c_fnd_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_shipment;
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
end wh_prf_corp_120t;
