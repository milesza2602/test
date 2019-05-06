--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_157U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_157U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Quentin Smit
--  Purpose:     Create zone_item dimention table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - stg_cam_location
--               Output - fnd_location
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_cam_location_hsp.sys_process_msg%type;
g_rec_out            fnd_location%rowtype;
g_rec_in             stg_cam_location%rowtype;
g_found              boolean;
g_valid              boolean;
g_count              integer       :=  0;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_157U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE_ITEM MASTERDATA EX AMOS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_cam_location%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_location%rowtype index by binary_integer;
type tbl_array_u is table of fnd_location%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_cam_location.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_cam_location.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_cam_location is
   select *
    from stg_cam_location_cpy
    where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';

   g_rec_out.location_no                    := g_rec_in.location_no;
   --g_rec_out.item_no                        := g_rec_in.item_no;
   g_rec_out.active_store_ind               := g_rec_in.active_store_ind;
   g_rec_out.new_store_ind                  := g_rec_in.new_store_ind;
   g_rec_out.source_data_status_code        := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date              := g_date;

   -- Fields not on this staging feed
/*
    g_rec_out.district_no  := 0;
    g_rec_out.address_line_1  := 0;
    g_rec_out.address_line_2  := 0;

    g_rec_out.city_name  := 0;
    g_rec_out.county_code  := ' ';
    g_rec_out.province_state_code  := 0;
    g_rec_out.country_code  := 0;
    g_rec_out.postal_code  := ' ';
    g_rec_out.changed_address_ind  := 0;
    g_rec_out.email_address  := 0;
    g_rec_out.channel_no  := 0;
    g_rec_out.vat_region_no  := ' ';
    g_rec_out.stock_holding_ind  := 0;
    g_rec_out.forecastable_ind  := 0;
    g_rec_out.currency_code  := ' ';
    g_rec_out.st_short_name  := 0;
    g_rec_out.st_abbrev_name  := 0;
    g_rec_out.st_scndry_name  := ' ';
    g_rec_out.st_fax_no  := ' ';
    g_rec_out.st_phone_no  := 0;
    g_rec_out.st_manager_name  := 0;
    g_rec_out.st_franchise_owner_name  := ' ';
    g_rec_out.st_sister_store_no  := ' ';
    g_rec_out.st_vat_incl_rsp_ind  := ' ';   --XX
    g_rec_out.st_open_date  := 0;
    g_rec_out.st_close_date  := 0;


    g_rec_out.ST_ACQUIRED_DATE
    g_rec_out.ST_REMODELED_DATE
    g_rec_out.ST_FORMAT_NO
    g_rec_out.ST_FORMAT_NAME
    g_rec_out.ST_CLASS_CODE
    g_rec_out.ST_MALL_NAME
    g_rec_out.ST_SHOP_CENTRE_TYPE
    g_rec_out.ST_NUM_TOTAL_SQUARE_FEET
    g_rec_out.ST_NUM_SELLING_SQUARE_FEET
    g_rec_out.ST_LINEAR_DISTANCE
    g_rec_out.ST_LANGUAGE_NO
    g_rec_out.ST_INTEGRATED_POS_IND
    g_rec_out.ST_ORIG_CURRENCY_CODE
    g_rec_out.ST_STORE_TYPE
    g_rec_out.ST_VALUE_OF_CHAIN_CLIP_CODE
    g_rec_out.ST_WW_ONLINE_PICKING_IND
    g_rec_out.ST_FOOD_SELL_STORE_IND
    g_rec_out.ST_WW_ONLINE_PICKING_RGN_CODE
    g_rec_out.ST_GEO_TERRITORY_CODE
    g_rec_out.ST_GENERATION_CODE
    g_rec_out.ST_SITE_LOCALITY_CODE
    g_rec_out.ST_SELLING_SPACE_CLIP_CODE
    g_rec_out.ST_DUN_BRADSTREET_ID
    g_rec_out.ST_DUN_BRADSTREET_LOC_ID
    g_rec_out.ST_CHBD_HANGING_SET_IND
    g_rec_out.ST_CHBD_RPL_RGN_LEADTIME_CODE
    g_rec_out.ST_CHBD_VAL_CHAIN_CLIP_CODE
    g_rec_out.ST_FD_SELL_SPACE_CLIP_CODE
    g_rec_out.ST_FD_STORE_FORMAT_CODE
    g_rec_out.ST_FD_VALUE_OF_CHAIN_CLIP_CODE
    g_rec_out.ST_FD_UNITS_SOLD_CLIP_CODE
    g_rec_out.ST_FD_CUSTOMER_TYPE_CLIP_CODE
    g_rec_out.ST_POS_TYPE
    g_rec_out.ST_POS_TRAN_NO_GENERATED_CODE
    g_rec_out.ST_SHAPE_OF_THE_CHAIN_CODE
    g_rec_out.ST_RECEIVING_IND
    g_rec_out.ST_DEFAULT_WH_NO
    g_rec_out.ST_CHBD_CLOSEST_WH_NO
    g_rec_out.ST_PROM_ZONE_NO
    g_rec_out.ST_PROM_ZONE_DESC
    g_rec_out.ST_TRANSFER_ZONE_NO
    g_rec_out.ST_TRANSFER_ZONE_DESC
    g_rec_out.ST_NUM_STOP_ORDER_DAYS
    g_rec_out.ST_NUM_START_ORDER_DAYS
    g_rec_out.WH_DISCIPLINE_TYPE
    g_rec_out.WH_STORE_NO
    g_rec_out.WH_SUPPLY_CHAIN_IND
    g_rec_out.WH_PRIMARY_SUPPLY_CHAIN_TYPE
    g_rec_out.WH_VALUE_ADD_SUPPLIER_NO
    g_rec_out.WH_ZONE_GROUP_NO
    g_rec_out.WH_ZONE_NO
    g_rec_out.WH_TRICEPS_CUSTOMER_CODE
    g_rec_out.WH_PRIMARY_VIRTUAL_WH_NO
    g_rec_out.WH_PHYSICAL_WH_NO
    g_rec_out.WH_REDIST_WH_IND
    g_rec_out.WH_RPL_IND
    g_rec_out.WH_VIRTUAL_WH_RPL_WH_NO
    g_rec_out.WH_VIRTUAL_WH_RESTRICTED_IND
    g_rec_out.WH_VIRTUAL_WH_PROTECTED_IND
    g_rec_out.WH_INVEST_BUY_WH_IND
    g_rec_out.WH_INVST_BUY_WH_AUTO_CLEAR_IND
    g_rec_out.WH_VIRTUAL_WH_INVST_BUY_WH_NO
    g_rec_out.WH_VIRTUAL_WH_TIER_TYPE
    g_rec_out.WH_BREAK_PACK_IND
    g_rec_out.WH_DELIVERY_POLICY_CODE
    g_rec_out.WH_ROUNDING_SEQ_NO
    g_rec_out.WH_INV_REPL_SEQ_NO
    g_rec_out.WH_FLOW_SUPPLY_CHAIN_IND
    g_rec_out.WH_XD_SUPPLY_CHAIN_IND
    g_rec_out.WH_HS_SUPPLY_CHAIN_IND
    g_rec_out.WH_EXPORT_WH_IND
    g_rec_out.WH_IMPORT_WH_IND
    g_rec_out.WH_DOMESTIC_WH_IND
    g_rec_out.WH_RTV_WH_IND
    g_rec_out.WH_ORG_HRCHY_TYPE
    g_rec_out.WH_ORG_HRCHY_VALUE
    g_rec_out.STORE_POS_ACTIVE_IND
    g_rec_out.SOURCE_DATA_STATUS_CODE
    g_rec_out.LAST_UPDATED_DATE
    g_rec_out.SUNDAY_STORE_TRADE_IND
    g_rec_out.STORE_CLUSTER
    g_rec_out.STORE_SIZE_FORMAT
    g_rec_out.GEOGRAPHY_NO
    g_rec_out.GRADE_NO
  */

   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_source_code;
   end if;

  if not  dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_cam_location_hsp values g_rec_in;
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
/*procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_location  values a_tbl_insert(i);

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
                       --' '||a_tbl_insert(g_error_index).item_no||
                       ' '||a_tbl_insert(g_error_index).location_no;
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
       update fnd_location
       set    active_store_ind            = a_tbl_update(i).active_store_ind,
              new_store_ind               = a_tbl_update(i).new_store_ind,
              last_updated_date           = a_tbl_update(i).last_updated_date
       where  location_no                 = a_tbl_update(i).location_no;

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
                       --' '||a_tbl_update(g_error_index).item_no||
                       ' '||a_tbl_update(g_error_index).location_no;
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
       update stg_cam_location
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
   select count(1)
   into   g_count
   from   fnd_location
   where  location_no = g_rec_out.location_no;

  if g_count = 1 then
     g_found := TRUE;
  end if;


-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).location_no   = g_rec_out.location_no then
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      --local_bulk_insert;
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_location EX JDAFF STARTED AT '||
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

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_cam_location;
    fetch c_stg_cam_location bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_cam_location bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_cam_location;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    --local_bulk_insert;
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
end wh_fnd_corp_157u;
