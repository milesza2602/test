--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_155U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_155U_BCK" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Quentin Smit
--  Purpose:     Create zone_item dimention table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - stg_catman_location_item
--               Output - fnd_location_item
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
g_hospital_text      stg_catman_location_item_hsp.sys_process_msg%type;
g_rec_out            fnd_location_item%rowtype;
g_rec_in             stg_catman_location_item%rowtype;
g_found              boolean;
g_valid              boolean;
g_count              integer       :=  0;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_155U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE_ITEM MASTERDATA EX CATMAN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_catman_location_item%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_location_item%rowtype index by binary_integer;
type tbl_array_u is table of fnd_location_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_catman_location_item.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_catman_location_item.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_catman_location_item is
   select a.*
   from stg_catman_location_item_cpy a, fnd_jdaff_dept_rollout b, dim_item c
   where sys_process_code = 'N'
     and a.item_no = c.item_no
     and c.department_no = b.department_no
     and b.department_live_ind = 'Y'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';

   g_rec_out.location_no                    := g_rec_in.location_no;
   g_rec_out.item_no                        := g_rec_in.item_no;
   g_rec_out.this_wk_catalog_ind            := g_rec_in.this_wk_catalog_ind;
   g_rec_out.next_wk_catalog_ind            := g_rec_in.next_wk_catalog_ind;
   g_rec_out.source_data_status_code        := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date              := g_date;
/*
   -- Fields not on this staging feed
    g_rec_out.NEXT_WK_DELIV_PATTERN_CODE  := ' ';
    g_rec_out.THIS_WK_DELIV_PATTERN_CODE  := ' ';
    g_rec_out.NUM_SHELF_LIFE_DAYS  := 0;
    g_rec_out.NUM_UNITS_PER_TRAY  := 0;
    g_rec_out.DIRECT_PERC  := 0;
    g_rec_out.MODEL_STOCK  := 0;
    g_rec_out.THIS_WK_CROSS_DOCK_IND  := 0;
    g_rec_out.NEXT_WK_CROSS_DOCK_IND  := 0;
    g_rec_out.THIS_WK_DIRECT_SUPPLIER_NO  := 0;
    g_rec_out.NEXT_WK_DIRECT_SUPPLIER_NO  := 0;
    g_rec_out.UNIT_PICK_IND  := 0;
    g_rec_out.STORE_ORDER_CALC_CODE  := ' ';
    g_rec_out.SAFETY_STOCK_FACTOR  := 0;
    g_rec_out.MIN_ORDER_QTY  := 0;
    g_rec_out.PROFILE_ID  := ' ';
    g_rec_out.SUB_PROFILE_ID  := 0;
    g_rec_out.REG_RSP  := 0;
    g_rec_out.SELLING_RSP  := 0;
    g_rec_out.SELLING_UOM_CODE  := ' ';
    g_rec_out.PROM_RSP  := 0;
    g_rec_out.PROM_SELLING_RSP  := 0;
    g_rec_out.PROM_SELLING_UOM_CODE  := ' ';
    g_rec_out.CLEARANCE_IND  := 0;
    g_rec_out.TAXABLE_IND  := 0;
    g_rec_out.POS_ITEM_DESC  := ' ';
    g_rec_out.POS_SHORT_DESC  := ' ';
    g_rec_out.NUM_TI_PALLET_TIER_CASES  := 0;
    g_rec_out.NUM_HI_PALLET_TIER_CASES  := 0;
    g_rec_out.STORE_ORD_MULT_UNIT_TYPE_CODE  := ' ';
    g_rec_out.LOC_ITEM_STATUS_CODE  := ' ';
    g_rec_out.LOC_ITEM_STAT_CODE_UPDATE_DATE  := ' ';   --XX
    g_rec_out.AVG_NATURAL_DAILY_WASTE_PERC  := 0;
    g_rec_out.MEAS_OF_EACH  := 0;
    g_rec_out.MEAS_OF_PRICE  := 0;
    g_rec_out.RSP_UOM_CODE  := ' ';
    g_rec_out.PRIMARY_VARIANT_ITEM_NO  := 0;
    g_rec_out.PRIMARY_COST_PACK_ITEM_NO  := 0;
    g_rec_out.PRIMARY_SUPPLIER_NO  := 0;
    g_rec_out.PRIMARY_COUNTRY_CODE  := ' ';
    g_rec_out.RECEIVE_AS_PACK_TYPE  := ' ';
    g_rec_out.SOURCE_METHOD_LOC_TYPE  := ' ';
    g_rec_out.SOURCE_LOCATION_NO  := 0;
    g_rec_out.WH_SUPPLY_CHAIN_TYPE_IND  := 0;
    g_rec_out.LAUNCH_DATE  := ' ';
    g_rec_out.POS_QTY_KEY_OPTION_CODE  := ' ';
    g_rec_out.POS_MANUAL_PRICE_ENTRY_CODE  := ' ';
    g_rec_out.DEPOSIT_CODE  := ' ';
    g_rec_out.FOOD_STAMP_IND  := 0;
    g_rec_out.POS_WIC_IND  := 0;
    g_rec_out.PROPORTIONAL_TARE_PERC  := 0;
    g_rec_out.FIXED_TARE_VALUE  := 0;
    g_rec_out.FIXED_TARE_UOM_CODE  := ' ';
    g_rec_out.POS_REWARD_ELIGIBLE_IND  := 0;
    g_rec_out.COMPARABLE_NATL_BRAND_ITEM_NO  := 0;
    g_rec_out.RETURN_POLICY_CODE  := ' ';
    g_rec_out.RED_FLAG_ALERT_IND  := 0;
    g_rec_out.POS_MARKETING_CLUB_CODE  := ' ';
    g_rec_out.REPORT_CODE  := ' ';
    g_rec_out.NUM_REQ_SELECT_SHELF_LIFE_DAYS  := 0;
    g_rec_out.NUM_REQ_RCPT_SHELF_LIFE_DAYS  := 0;
    g_rec_out.NUM_INVST_BUY_SHELF_LIFE_DAYS  := 0;
    g_rec_out.RACK_SIZE_CODE  := ' ';
    g_rec_out.FULL_PALLET_ITEM_REORDER_IND  := 0;
    g_rec_out.IN_STORE_MARKET_BASKET_CODE  := ' ';
    g_rec_out.STORAGE_LOCATION_BIN_ID  := ' ';
    g_rec_out.ALT_STORAGE_LOCATION_BIN_ID  := ' ';
    g_rec_out.STORE_REORDER_IND  := 0;
    g_rec_out.RETURNABLE_IND  := 0;
    g_rec_out.REFUNDABLE_IND  := 0;
    g_rec_out.BACK_ORDER_IND  := 0;
    g_rec_out.WEIGH_IND  := 0;
    g_rec_out.SALE_ALERT_IND  := ' ';
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

  if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
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

   insert into stg_catman_location_item_hsp values g_rec_in;
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
       insert into fnd_location_item values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).item_no||
                       ' '||a_tbl_insert(g_error_index).location_no;
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
       update fnd_location_item
       set    this_wk_catalog_ind       = a_tbl_update(i).this_wk_catalog_ind,
              next_wk_catalog_ind       = a_tbl_update(i).next_wk_catalog_ind,
              source_data_status_code   = a_tbl_update(i).source_data_status_code,
              last_updated_date         = a_tbl_update(i).last_updated_date
       where  item_no                   = a_tbl_update(i).item_no and
              location_no               = a_tbl_update(i).location_no;

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
                       ' '||a_tbl_update(g_error_index).item_no||
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
       update stg_catman_location_item_cpy
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
   from   fnd_location_item
   where  item_no     = g_rec_out.item_no and
          location_no = g_rec_out.location_no;

  if g_count = 1 then
     g_found := TRUE;
  end if;


-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).item_no       = g_rec_out.item_no and
            a_tbl_insert(i).location_no   = g_rec_out.location_no then
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
     l_text := 'Bulk write limit :- '||g_forall_limit;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_location_item EX JDAFF STARTED AT '||
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
    open c_stg_catman_location_item;
    fetch c_stg_catman_location_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_catman_location_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_catman_location_item;
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
end wh_fnd_corp_155u_BCK;