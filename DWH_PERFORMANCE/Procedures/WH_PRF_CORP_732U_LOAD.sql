--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_732U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_732U_LOAD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Create depot item dy rollup fact table in the performance layer
--               with input ex Triceps  table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_trcps_boh
--               Output - rtl_depot_item_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
-- 07 Jan 2011 - QC 4000      - Filtered estimate stock for input to cubes stock cover calc
-- 03 May 2011 - qc 4190      - DATA FIX: Case Cost for Depot Day table to be sourced from RTL_ZONE_ITEM_OM

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
g_rec_out            rtl_depot_item_dy%rowtype;
g_found              boolean;
g_case_selling_excl_vat   rtl_zone_item_om.case_selling_excl_vat%type;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_wk_end        date;
g_last_wk_start      date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_732U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD TRICEPS BOH EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_depot_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_depot_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--QC 4000 read uda table and get fields shorts_longlife_desc_542 for filtering
cursor c_fnd_rtl_loc_item_dy_boh is
   select boh.*,
          di.sk1_item_no,
          dl.sk1_location_no,
          dl.sk1_fd_zone_group_zone_no ,
          dih.sk2_item_no ,
          dlh.sk2_location_no,
          nvl(diu.shorts_longlife_desc_542,' ') shorts_longlife_desc_542
   from   fnd_rtl_loc_item_dy_trcps_boh boh,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh,
          dim_item_uda diu
   where  boh.item_no                   = di.item_no  and
          boh.item_no                   = diu.item_no(+) and
          boh.location_no               = dl.location_no   and
          boh.item_no                   = dih.item_no and
          boh.post_date                 between dih.sk2_active_from_date and dih.sk2_active_to_date and
          boh.location_no               = dlh.location_no and
          boh.post_date                 between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          boh.last_updated_date         = g_date
          and di.department_no = 81;




g_rec_in             c_fnd_rtl_loc_item_dy_boh%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_boh%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.stock_cases                     := g_rec_in.stock_cases;
 --  g_rec_out.stock_cost                      := g_rec_in.stock_cost;
   g_rec_out.shrink_cases                    := g_rec_in.shrink_cases;
   g_rec_out.gains_cases                     := g_rec_in.gains_cases;
   g_rec_out.received_cases                  := g_rec_in.received_cases;
   g_rec_out.dispatched_cases                := g_rec_in.dispatched_cases;
   g_rec_out.case_cost                       := g_rec_in.case_cost;
   g_rec_out.outstore_cases                  := g_rec_in.outstore_cases;
   g_rec_out.on_hold_cases                   := g_rec_in.on_hold_cases;
   g_rec_out.overship_cases                  := g_rec_in.overship_cases;
   g_rec_out.scratch_cases                   := g_rec_in.scratch_cases;
   g_rec_out.scratch_no_cases                := g_rec_in.scratch_no_cases;
   g_rec_out.cases_ex_outstore               := g_rec_in.cases_ex_outstore;
   g_rec_out.cases_to_outstore               := g_rec_in.cases_to_outstore;
   g_rec_out.unpicked_cases                  := g_rec_in.unpicked_cases;
   g_rec_out.on_order_cases                  := g_rec_in.on_order_cases;
   g_rec_out.pallet_qty                      := g_rec_in.pallet_qty;
   g_rec_out.pick_slot_cases                 := g_rec_in.pick_slot_cases;
   g_rec_out.case_avg_weight                 := g_rec_in.case_avg_weight;
   g_rec_out.returned_cases                  := g_rec_in.returned_cases;
   g_rec_out.shelf_life_expird               := g_rec_in.shelf_life_expird;
   g_rec_out.shelf_life_01_07                := g_rec_in.shelf_life_01_07;
   g_rec_out.shelf_life_08_14                := g_rec_in.shelf_life_08_14;
   g_rec_out.shelf_life_15_21                := g_rec_in.shelf_life_15_21;
   g_rec_out.shelf_life_22_28                := g_rec_in.shelf_life_22_28;
   g_rec_out.shelf_life_29_35                := g_rec_in.shelf_life_29_35;
   g_rec_out.shelf_life_36_49                := g_rec_in.shelf_life_36_49;
   g_rec_out.shelf_life_50_60                := g_rec_in.shelf_life_50_60;
   g_rec_out.shelf_life_61_90                := g_rec_in.shelf_life_61_90;
   g_rec_out.shelf_life_91_120               := g_rec_in.shelf_life_91_120;
   g_rec_out.shelf_life_120_up               := g_rec_in.shelf_life_120_up;
   g_rec_out.last_updated_date               := g_date;
 --  g_rec_out.product_status_code             := null;
 --  g_rec_out.product_status_1_code           := null;
   g_rec_out.product_status_code             := 0;
   g_rec_out.product_status_1_code           := 0;
   g_rec_out.stock_dc_cover_cases            := null;
--QC 4000 filter on field shorts_longlife_desc_542
   if g_rec_in.shorts_longlife_desc_542 = 'Yes' then
      g_rec_out.stock_dc_cover_cases           := g_rec_in.stock_cases ;
   end if;

      g_rec_out.stock_cost            := 0;
      g_rec_out.stock_selling         := 0;
      g_rec_out.SHRINK_SELLING        := 0;
      g_rec_out.GAINS_SELLING         := 0;
      g_rec_out.RECEIVED_SELLING      := 0;
      g_rec_out.DISPATCHED_SELLING    := 0;
      g_rec_out.OUTSTORE_SELLING      := 0;
      g_rec_out.ON_HOLD_SELLING       := 0;
      g_rec_out.OVERSHIP_SELLING      := 0;
      g_rec_out.SCRATCH_SELLING       := 0;
      g_rec_out.SCRATCH_NO_SELLING    := 0;
      g_rec_out.EX_OUTSTORE_SELLING   := 0;
      g_rec_out.TO_OUTSTORE_SELLING   := 0;
      g_rec_out.UNPICKED_SELLING      := 0;
      g_rec_out.ON_ORDER_SELLING      := 0;
      g_rec_out.PICK_SLOT_SELLING     := 0;
      g_rec_out.RETURNED_SELLING      := 0;

   begin
      select case_selling_excl_vat,
             case_cost,
             product_status_code,
             product_status_1_code,
             case_cost
      into   g_case_selling_excl_vat,
             g_rec_out.case_cost,
             g_rec_out.product_status_code,
             g_rec_out.product_status_1_code ,
             g_rec_out.stock_cost
      from   rtl_zone_item_om
      where  sk1_zone_group_zone_no = g_rec_in.sk1_fd_zone_group_zone_no and
             sk1_item_no            = g_rec_in.sk1_item_no;

      g_rec_out.stock_cost            := g_rec_out.case_cost * g_rec_out.stock_cases;
--
-- as the values for product_status_code's on rtl_zone_item_om will already be numeric
-- there is no need to convert again
      If g_rec_out.product_status_code is null
      then
      g_rec_out.product_status_code             := 0;
      end if;
      If g_rec_out.product_status_1_code is null
      then
      g_rec_out.product_status_1_code             := 0;
      end if;

      g_rec_out.stock_selling        := g_rec_out.stock_cases * g_case_selling_excl_vat;
      g_rec_out.SHRINK_SELLING       := g_rec_out.SHRINK_CASES * g_case_selling_excl_vat;
      g_rec_out.GAINS_SELLING        := g_rec_out.GAINS_CASES * g_case_selling_excl_vat;
      g_rec_out.RECEIVED_SELLING     := g_rec_out.RECEIVED_CASES * g_case_selling_excl_vat;
      g_rec_out.DISPATCHED_SELLING   := g_rec_out.DISPATCHED_CASES * g_case_selling_excl_vat;
      g_rec_out.OUTSTORE_SELLING     := g_rec_out.OUTSTORE_CASES * g_case_selling_excl_vat;
      g_rec_out.ON_HOLD_SELLING      := g_rec_out.ON_HOLD_CASES * g_case_selling_excl_vat;
      g_rec_out.OVERSHIP_SELLING     := g_rec_out.OVERSHIP_CASES * g_case_selling_excl_vat;
      g_rec_out.SCRATCH_SELLING      := g_rec_out.SCRATCH_CASES  * g_case_selling_excl_vat;
      g_rec_out.SCRATCH_NO_SELLING   := g_rec_out.SCRATCH_NO_CASES * g_case_selling_excl_vat;
      g_rec_out.EX_OUTSTORE_SELLING  := g_rec_out.CASES_EX_OUTSTORE * g_case_selling_excl_vat;
      g_rec_out.TO_OUTSTORE_SELLING  := g_rec_out.CASES_TO_OUTSTORE * g_case_selling_excl_vat;
      g_rec_out.UNPICKED_SELLING     := g_rec_out.UNPICKED_CASES * g_case_selling_excl_vat;
      g_rec_out.ON_ORDER_SELLING     := g_rec_out.ON_ORDER_CASES * g_case_selling_excl_vat;
      g_rec_out.PICK_SLOT_SELLING    := g_rec_out.PICK_SLOT_CASES * g_case_selling_excl_vat;
      g_rec_out.RETURNED_SELLING     := g_rec_out.RETURNED_CASES * g_case_selling_excl_vat;

      exception
         when no_data_found then
           l_message := 'No data found on rtl_zone_item_om lookup '||g_rec_in.item_no||' '||g_rec_in.location_no;
           dwh_log.record_error(l_module_name,sqlcode,l_message);

   end;

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
       insert into rtl_depot_item_dy values a_tbl_insert(i);

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
       update rtl_depot_item_dy
       set    stock_cases                     = a_tbl_update(i).stock_cases,
              stock_cost                      = a_tbl_update(i).stock_cost,
              stock_selling                   = a_tbl_update(i).stock_selling,
              shrink_cases                    = a_tbl_update(i).shrink_cases,
              gains_cases                     = a_tbl_update(i).gains_cases,
              received_cases                  = a_tbl_update(i).received_cases,
              dispatched_cases                = a_tbl_update(i).dispatched_cases,
              case_cost                       = a_tbl_update(i).case_cost,
              product_status_code             = a_tbl_update(i).product_status_code,
              product_status_1_code           = a_tbl_update(i).product_status_1_code,
              outstore_cases                  = a_tbl_update(i).outstore_cases,
              on_hold_cases                   = a_tbl_update(i).on_hold_cases,
              overship_cases                  = a_tbl_update(i).overship_cases,
              scratch_cases                   = a_tbl_update(i).scratch_cases,
              scratch_no_cases                = a_tbl_update(i).scratch_no_cases,
              cases_ex_outstore               = a_tbl_update(i).cases_ex_outstore,
              cases_to_outstore               = a_tbl_update(i).cases_to_outstore,
              unpicked_cases                  = a_tbl_update(i).unpicked_cases,
              on_order_cases                  = a_tbl_update(i).on_order_cases,
              pallet_qty                      = a_tbl_update(i).pallet_qty,
              pick_slot_cases                 = a_tbl_update(i).pick_slot_cases,
              case_avg_weight                 = a_tbl_update(i).case_avg_weight,
              returned_cases                  = a_tbl_update(i).returned_cases,
              shelf_life_expird               = a_tbl_update(i).shelf_life_expird,
              shelf_life_01_07                = a_tbl_update(i).shelf_life_01_07,
              shelf_life_08_14                = a_tbl_update(i).shelf_life_08_14,
              shelf_life_15_21                = a_tbl_update(i).shelf_life_15_21,
              shelf_life_22_28                = a_tbl_update(i).shelf_life_22_28,
              shelf_life_29_35                = a_tbl_update(i).shelf_life_29_35,
              shelf_life_36_49                = a_tbl_update(i).shelf_life_36_49,
              shelf_life_50_60                = a_tbl_update(i).shelf_life_50_60,
              shelf_life_61_90                = a_tbl_update(i).shelf_life_61_90,
              shelf_life_91_120               = a_tbl_update(i).shelf_life_91_120,
              shelf_life_120_up               = a_tbl_update(i).shelf_life_120_up,
              shrink_selling                  = a_tbl_update(i).shrink_selling,
              gains_selling                   = a_tbl_update(i).gains_selling,
              received_selling                = a_tbl_update(i).received_selling,
              dispatched_selling              = a_tbl_update(i).dispatched_selling,
              outstore_selling                = a_tbl_update(i).outstore_selling,
              on_hold_selling                 = a_tbl_update(i).on_hold_selling,
              overship_selling                = a_tbl_update(i).overship_selling,
              scratch_selling                 = a_tbl_update(i).scratch_selling,
              scratch_no_selling              = a_tbl_update(i).scratch_no_selling,
              ex_outstore_selling             = a_tbl_update(i).ex_outstore_selling,
              to_outstore_selling             = a_tbl_update(i).to_outstore_selling,
              unpicked_selling                = a_tbl_update(i).unpicked_selling,
              on_order_selling                = a_tbl_update(i).on_order_selling,
              pick_slot_selling               = a_tbl_update(i).pick_slot_selling,
              returned_selling                = a_tbl_update(i).returned_selling,
              stock_dc_cover_cases            = a_tbl_update(i).stock_dc_cover_cases,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no              = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no         and
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
   from   rtl_depot_item_dy
   where  sk1_location_no = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no         and
          post_date          = g_rec_out.post_date;

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

    l_text := 'LOAD OF rtl_depot_item_dy EX FOUNDATION STARTED AT '||
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
    g_date := '01/APR/14';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_dy_boh;
    fetch c_fnd_rtl_loc_item_dy_boh bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_item_dy_boh bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_boh;
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
end wh_prf_corp_732u_load;
