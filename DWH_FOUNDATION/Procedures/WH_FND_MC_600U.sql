--------------------------------------------------------
--  DDL for Procedure WH_FND_MC_600U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MC_600U" 
                              (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create stock count fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - STG_RMS_MC_RTL_STOCK_COUNT
--               Output - fnd_MC_rtl_stock_count
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      STG_RMS_MC_RTL_STOCK_COUNT_hsp.sys_process_msg%type;
g_rec_out            fnd_MC_rtl_stock_count%rowtype;
g_rec_in             STG_RMS_MC_RTL_STOCK_COUNT_cpy%rowtype;
g_found              boolean;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MC_600U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STOCK COUNT FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of STG_RMS_MC_RTL_STOCK_COUNT_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_MC_rtl_stock_count%rowtype index by binary_integer;
type tbl_array_u is table of fnd_MC_rtl_stock_count%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_RMS_MC_RTL_STOCK_COUNT_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_RMS_MC_RTL_STOCK_COUNT_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_RMS_MC_RTL_STOCK_COUNT is
   select *
   from STG_RMS_MC_RTL_STOCK_COUNT_cpy
   where sys_process_code = 'N' 
   order by sys_source_batch_id,sys_source_sequence_no;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.stock_count_no                  := g_rec_in.stock_count_no;
   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.cycle_count_desc                := g_rec_in.cycle_count_desc;
   g_rec_out.loc_type                        := g_rec_in.loc_type;
   g_rec_out.stocktake_date                  := g_rec_in.stocktake_date;
   g_rec_out.stocktake_type                  := g_rec_in.stocktake_type;
   g_rec_out.product_level_ind               := g_rec_in.product_level_ind;
   g_rec_out.delete_ind                      := g_rec_in.delete_ind;
   g_rec_out.ww_external_count_ind           := g_rec_in.ww_external_count_ind;
   g_rec_out.snapshot_on_hand_qty            := g_rec_in.snapshot_on_hand_qty;
   g_rec_out.snapshot_in_transit_qty         := g_rec_in.snapshot_in_transit_qty;
   
   g_rec_out.processed_code                  := g_rec_in.processed_code;
   g_rec_out.physical_count_qty              := g_rec_in.physical_count_qty;
   g_rec_out.pack_comp_qty                   := g_rec_in.pack_comp_qty;

   g_rec_out.stock_count_reason_no           := g_rec_in.stock_count_reason_no;
   g_rec_out.ww_adj_uncounted_ind            := g_rec_in.ww_adj_uncounted_ind;
   g_rec_out.ww_counted_ind                  := g_rec_in.ww_counted_ind;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.snapshot_unit_cost_LOCAL              := g_rec_in.snapshot_unit_cost_LOCAL;
   g_rec_out.snapshot_unit_retail_LOCAL            := g_rec_in.snapshot_unit_retail_LOCAL;
   g_rec_out.in_transit_amt_LOCAL                  := g_rec_in.in_transit_amt_LOCAL;

   g_rec_out.snapshot_unit_cost_OPR              := g_rec_in.snapshot_unit_cost_OPR;
   g_rec_out.snapshot_unit_retail_OPR            := g_rec_in.snapshot_unit_retail_OPR;
   g_rec_out.in_transit_amt_OPR                  := g_rec_in.in_transit_amt_OPR;
   
    if not dwh_valid.indicator_field(g_rec_out.product_level_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

    if not dwh_valid.indicator_field(g_rec_out.ww_external_count_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.ww_adj_uncounted_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.ww_counted_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.delete_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   begin
      select r.debtors_commission_perc
      into   g_rec_out.debtors_commission_perc
      from   rtl_loc_dept_dy r
      join   dim_location l        on r.sk1_location_no   = l.sk1_location_no
      join   dim_item i            on r.sk1_department_no = i.sk1_department_no
      where  l.location_no         =  g_rec_out.location_no
      and    i.item_no             =  g_rec_out.item_no
      and    r.post_date           =  g_rec_out.stocktake_date;
      exception
         when no_data_found then
           g_rec_out.debtors_commission_perc := 0;
   end;
   if g_rec_out.debtors_commission_perc is null then
      g_rec_out.debtors_commission_perc := 0;
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code       := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into STG_RMS_MC_RTL_STOCK_COUNT_hsp  values g_rec_in;
   g_recs_hospital     := g_recs_hospital + sql%rowcount ;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_MC_rtl_stock_count values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).stock_count_no||
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).item_no;

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
       update fnd_MC_rtl_stock_count
       set    cycle_count_desc                = a_tbl_update(i).cycle_count_desc,
              loc_type                        = a_tbl_update(i).loc_type,
              stocktake_date                  = a_tbl_update(i).stocktake_date,
              stocktake_type                  = a_tbl_update(i).stocktake_type,
              product_level_ind               = a_tbl_update(i).product_level_ind,
              delete_ind                      = a_tbl_update(i).delete_ind,
              ww_external_count_ind           = a_tbl_update(i).ww_external_count_ind,
              snapshot_on_hand_qty            = a_tbl_update(i).snapshot_on_hand_qty,
              snapshot_in_transit_qty         = a_tbl_update(i).snapshot_in_transit_qty,

              processed_code                  = a_tbl_update(i).processed_code,
              physical_count_qty              = a_tbl_update(i).physical_count_qty,
              pack_comp_qty                   = a_tbl_update(i).pack_comp_qty,

              stock_count_reason_no           = a_tbl_update(i).stock_count_reason_no,
              ww_adj_uncounted_ind            = a_tbl_update(i).ww_adj_uncounted_ind,
              ww_counted_ind                  = a_tbl_update(i).ww_counted_ind,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              debtors_commission_perc         = a_tbl_update(i).debtors_commission_perc,
  
              snapshot_unit_cost_LOCAL             = a_tbl_update(i).snapshot_unit_cost_LOCAL,
              snapshot_unit_retail_LOCAL           = a_tbl_update(i).snapshot_unit_retail_LOCAL,
              in_transit_amt_LOCAL                 = a_tbl_update(i).in_transit_amt_LOCAL,

              snapshot_unit_cost_OPR             = a_tbl_update(i).snapshot_unit_cost_OPR,
              snapshot_unit_retail_OPR           = a_tbl_update(i).snapshot_unit_retail_OPR,
              in_transit_amt_OPR                 = a_tbl_update(i).in_transit_amt_OPR
   
       where  stock_count_no                  = a_tbl_update(i).stock_count_no
       and    location_no                     = a_tbl_update(i).location_no
       and    item_no                         = a_tbl_update(i).item_no;

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
                       ' '||a_tbl_update(g_error_index).stock_count_no||
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).item_no ;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update STG_RMS_MC_RTL_STOCK_COUNT_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_staging_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_MC_rtl_stock_count
   where  stock_count_no   = g_rec_out.stock_count_no  and
          location_no      = g_rec_out.location_no and
          item_no          = g_rec_out. item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).stock_count_no = g_rec_out.stock_count_no  and
            a_tbl_insert(i).location_no    = g_rec_out.location_no  and
            a_tbl_insert(i).item_no        = g_rec_out.item_no then
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
      local_bulk_staging_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF fnd_MC_rtl_stock_count EX OM STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    open c_STG_RMS_MC_RTL_STOCK_COUNT;
    fetch c_STG_RMS_MC_RTL_STOCK_COUNT bulk collect into a_stg_input limit g_forall_limit;
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
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_STG_RMS_MC_RTL_STOCK_COUNT bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_RMS_MC_RTL_STOCK_COUNT;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_FND_MC_600u;
