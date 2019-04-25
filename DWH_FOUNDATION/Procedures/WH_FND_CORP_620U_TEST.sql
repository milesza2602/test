--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_620U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_620U_TEST" (P_FORALL_LIMIT in integer,P_SUCCESS OUT BOOLEAN
--,P_FROM_LOC_NO in integer,P_TO_LOC_NO in integer
) as
--*************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Allocation fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_rtl_allocation_cpy
--               Output - fnd_rtl_allocation
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
--g_vat_rate_perc      dim_item.vat_rate_perc%type;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_allocation_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_allocation%rowtype;

P_FROM_LOC_NO        integer       :=  0;
p_to_loc_no        integer       :=  0;

g_found              boolean;
g_insert_rec         boolean;
--g_business_unit_no   dim_item.business_unit_no%type;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_start_date         date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_620U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ALLOCATION FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_allocation%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_allocation%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_rtl_allocation_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_rtl_allocation_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor C_STG_RMS_RTL_ALLOCATION is
   select /*+ full(stg) parallel(fr,4)*/ stg.*,
          nvl(di.vat_rate_perc,14) vat_rate_perc,
          li.wac,
          NVL(LI.REG_RSP,0)        REG_RSP,
          FR.ALLOC_NO FND_ALLOC_NO,
          stg.item_no STG_ITEM_NO,
          stg.to_loc_no STG_LOCATION_NO
   from stg_rms_rtl_allocation_cpy stg
        left outer join dim_item di          on stg.item_no          = di.item_no
        left outer join dim_location dl      on stg.to_loc_no        = dl.location_no
        left outer join rtl_location_item li on di.sk1_item_no       = li.sk1_item_no  and
                                                DL.SK1_LOCATION_NO   = LI.SK1_LOCATION_NO
        left outer join FND_RTL_ALLOCATION FR
        on FR.ALLOC_NO                        = STG.ALLOC_NO and
           FR.TO_LOC_NO                       = STG.TO_LOC_NO
        where 
        --SYS_PROCESS_CODE = 'N' and
         STG.to_loc_no      between p_from_loc_no and p_to_loc_no
   order by sys_source_batch_id,sys_source_sequence_no;

g_rec_in             c_stg_rms_rtl_allocation%rowtype;
-- For input bulk collect --
type stg_array is table of c_stg_rms_rtl_allocation%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.alloc_no                        := g_rec_in.alloc_no;
   g_rec_out.to_loc_no                       := g_rec_in.to_loc_no;
   g_rec_out.release_date                    := g_rec_in.release_date;
   g_rec_out.po_no                           := g_rec_in.po_no;
   g_rec_out.wh_no                           := g_rec_in.wh_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.alloc_status_code               := g_rec_in.alloc_status_code;
   g_rec_out.to_loc_type                     := g_rec_in.to_loc_type;
   g_rec_out.sdn_qty                         := g_rec_in.sdn_qty;
   g_rec_out.alloc_qty                       := g_rec_in.alloc_qty;
   g_rec_out.dist_qty                        := g_rec_in.dist_qty;
   g_rec_out.apportion_qty                   := g_rec_in.apportion_qty;
   g_rec_out.alloc_cancel_qty                := g_rec_in.alloc_cancel_qty;
   g_rec_out.received_qty                    := g_rec_in.received_qty;
   g_rec_out.po_grn_qty                      := g_rec_in.po_grn_qty;
   g_rec_out.ext_ref_id                      := g_rec_in.ext_ref_id;
   g_rec_out.planned_into_loc_date           := g_rec_in.planned_into_loc_date;
   g_rec_out.into_loc_date                   := g_rec_in.into_loc_date;
   g_rec_out.scale_priority_code             := g_rec_in.scale_priority_code;
   g_rec_out.trunk_ind                       := g_rec_in.trunk_ind;
   g_rec_out.overstock_qty                   := g_rec_in.overstock_qty;
   g_rec_out.priority1_qty                   := g_rec_in.priority1_qty;
   g_rec_out.safety_qty                      := g_rec_in.safety_qty;
   g_rec_out.special_qty                     := g_rec_in.special_qty;
   g_rec_out.orig_alloc_qty                  := g_rec_in.orig_alloc_qty;
   g_rec_out.alloc_line_status_code          := g_rec_in.alloc_line_status_code;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.reg_rsp_excl_vat                := g_rec_in.reg_rsp;
   G_REC_OUT.WAC                             := G_REC_IN.WAC;
--   G_REC_OUT.STG_LOCATION_NO                 := G_REC_IN.STG_LOCATION_NO;
--   G_REC_OUT.STG_ITEM_NO                 := G_REC_IN.STG_ITEM_NO;
--   G_REC_OUT.FND_ALLOC_NO                 := G_REC_IN.FND_ALLOC_NO;

   g_rec_out.last_updated_date               := g_date;


   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_source_code;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.trunk_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;


   if g_rec_IN.STG_LOCATION_NO IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_IN.to_loc_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if g_rec_IN.STG_item_no IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_IN.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

      g_rec_out.reg_rsp_excl_vat := round(g_rec_out.reg_rsp_excl_vat * 100 / (100 + g_rec_in.vat_rate_perc),2);

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
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_rms_rtl_allocation_hsp values
   (g_rec_in.sys_source_batch_id,
   g_rec_in.sys_source_sequence_no,
   g_rec_in.sys_load_date,
   g_rec_in.sys_process_code,
   g_rec_in.sys_load_system_name,
   g_rec_in.sys_middleware_batch_id,
   g_rec_in.sys_process_msg,
   g_rec_in.alloc_no,
   g_rec_in.to_loc_no,
   g_rec_in.release_date,
   g_rec_in.po_no,
   g_rec_in.wh_no,
   g_rec_in.item_no,
   g_rec_in.alloc_status_code,
   g_rec_in.to_loc_type,
   g_rec_in.sdn_qty,
   g_rec_in.alloc_qty,
   g_rec_in.dist_qty,
   g_rec_in.apportion_qty,
   g_rec_in.alloc_cancel_qty,
   g_rec_in.received_qty,
   g_rec_in.po_grn_qty,
   g_rec_in.ext_ref_id,
   g_rec_in.planned_into_loc_date,
   g_rec_in.into_loc_date,
   g_rec_in.scale_priority_code,
   g_rec_in.trunk_ind,
   g_rec_in.overstock_qty,
   g_rec_in.priority1_qty,
   g_rec_in.safety_qty,
   g_rec_in.special_qty,
   g_rec_in.orig_alloc_qty,
   g_rec_in.alloc_line_status_code,
   g_rec_in.source_data_status_code)
;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

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
       insert into fnd_rtl_allocation values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).alloc_no||
                       ' '||a_tbl_insert(g_error_index).to_loc_no;

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
       update fnd_rtl_allocation
       set    release_date                    = a_tbl_update(i).release_date,
              po_no                           = a_tbl_update(i).po_no,
              wh_no                           = a_tbl_update(i).wh_no,
              item_no                         = a_tbl_update(i).item_no,
              alloc_status_code               = a_tbl_update(i).alloc_status_code,
              to_loc_type                     = a_tbl_update(i).to_loc_type,
              sdn_qty                         = a_tbl_update(i).sdn_qty,
              alloc_qty                       = a_tbl_update(i).alloc_qty,
              dist_qty                        = a_tbl_update(i).dist_qty,
              apportion_qty                   = a_tbl_update(i).apportion_qty,
              alloc_cancel_qty                = a_tbl_update(i).alloc_cancel_qty,
              received_qty                    = a_tbl_update(i).received_qty,
              po_grn_qty                      = a_tbl_update(i).po_grn_qty,
              ext_ref_id                      = a_tbl_update(i).ext_ref_id,
              planned_into_loc_date           = a_tbl_update(i).planned_into_loc_date,
              into_loc_date                   = a_tbl_update(i).into_loc_date,
              scale_priority_code             = a_tbl_update(i).scale_priority_code,
              trunk_ind                       = a_tbl_update(i).trunk_ind,
              overstock_qty                   = a_tbl_update(i).overstock_qty,
              priority1_qty                   = a_tbl_update(i).priority1_qty,
              safety_qty                      = a_tbl_update(i).safety_qty,
              special_qty                     = a_tbl_update(i).special_qty,
              orig_alloc_qty                  = a_tbl_update(i).orig_alloc_qty,
              alloc_line_status_code          = a_tbl_update(i).alloc_line_status_code,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              wac                             = a_tbl_update(i).wac,
              reg_rsp_excl_vat                = a_tbl_update(i).reg_rsp_excl_vat,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  alloc_no                        = a_tbl_update(i).alloc_no and
              to_loc_no                       = a_tbl_update(i).to_loc_no;



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
                       ' '||a_tbl_update(g_error_index).alloc_no||
                       ' '||a_tbl_update(g_error_index).to_loc_no;

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
       update stg_rms_rtl_allocation_cpy
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
   if G_REC_IN.FND_ALLOC_NO > 0
   then G_COUNT := 1;
   END IF;


   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).alloc_no   = g_rec_out.alloc_no and
            a_tbl_insert(i).to_loc_no  = g_rec_out.to_loc_no then
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

   if A_COUNT > G_FORALL_LIMIT then
   --   local_bulk_insert;
   --   local_bulk_update;
  --    local_bulk_staging_update;

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

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

P_FROM_LOC_NO := 491;
p_to_loc_no := 99999;

    l_text := 'LOAD OF FND_RTL_ALLOCTION EX OM STARTED AT '||
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
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_rtl_allocation;
    fetch c_stg_rms_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
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
      --   if g_hospital = 'Y' then
      --      local_write_hospital;
      --   else
            LOCAL_WRITE_OUTPUT;
      --   end if;
      end loop;
    fetch c_stg_rms_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_rtl_allocation;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

  --  local_bulk_insert;
  --  local_bulk_update;
  --  local_bulk_staging_update;


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

END WH_FND_CORP_620U_TEST;
