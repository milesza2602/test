--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_167U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_167U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF fnd_jdaff_wh_plan_wk_analysis table from foundation layer.
--
--  Tables:      Input  - fnd_jdaff_wh_plan_wk_analysis
--               Output - dwh_performance.rtl_jdaff_wh_plan_wk_analysis
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
--g_cases           dwh_performance.rtl_jdaff_wh_plan_wk_analysis.dc_plan_store_cases%type;
g_rec_out            dwh_performance.rtl_jdaff_wh_plan_wk_analysis%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_167U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.rtl_jdaff_wh_plan_wk_analysis%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.rtl_jdaff_wh_plan_wk_analysis%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
a_count_m           integer       := 0;

-- For output arrays into bulk load forall statements --
type tbl_array_m is table of dwh_performance.rtl_jdaff_wh_plan_wk_analysis%rowtype index by binary_integer;
--type tbl_array_u is table of dwh_performance.rtl_jdaff_wh_plan_wk_analysis%rowtype index by binary_integer;
a_tbl_merge        tbl_array_m;
a_empty_set_m      tbl_array_m;


cursor c_jdaff_wh_plan is
   select   /*+ PARALLEL(di,4) PARALLEL(dl,4) PARALLEL_INDEX(jdaff,4) */
            di.sk1_item_no,
            dl.sk1_location_no,
            trading_date,
            post_date,
            total_demand_unit,
            inventory_unit,
            planned_arrivals_unit,
            rec_arrival_unit,
            in_transit_unit,
            plan_ship_unit,
            rec_ship_unit,
            constraint_poh_unit,
            safety_stock_unit,
            constraint_proj_avail,
            expired_on_hand_unit,
            dc_forward_cover_day,
            alt_constraint_unused_soh_unit,
            alt_constraint_poh_unit,
            constraint_unmet_demand_unit,
            constraint_unused_soh_unit,
            expired_soh_unit,
            ignored_demand_unit,
            projected_stock_available_unit

   from     dwh_foundation.fnd_jdaff_wh_plan_wk_analysis jdaff,
            dim_item di,
            dim_location dl--,
            --dim_calendar dc

   where jdaff.item_no          = di.item_no
    and dl.location_no      = jdaff.location_no
    and jdaff.last_updated_date = g_date
--      and (di.sk1_item_no = 13894 or dl.sk1_location_no = 1525)
--    or trading_date <> '12 jul 15')
   order by  di.sk1_item_no,  dl.sk1_location_no, jdaff.trading_date;

g_rec_in             c_jdaff_wh_plan%rowtype;
-- For input bulk collect --
type stg_array is table of c_jdaff_wh_plan%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no            := g_rec_in.sk1_item_no;
   --g_rec_out.calendar_date          := g_rec_in.calendar_date;
   g_rec_out.sk1_location_no        := g_rec_in.sk1_location_no;
   g_rec_out.trading_date           := g_rec_in.trading_date;
   g_rec_out.post_date              := g_rec_in.post_date;
   g_rec_out.total_demand_unit      := g_rec_in.total_demand_unit;
   g_rec_out.inventory_unit         := g_rec_in.inventory_unit;
   g_rec_out.planned_arrivals_unit  := g_rec_in.planned_arrivals_unit;
   g_rec_out.rec_arrival_unit       := g_rec_in.rec_arrival_unit;
   g_rec_out.in_transit_unit        := g_rec_in.in_transit_unit;
   g_rec_out.plan_ship_unit         := g_rec_in.plan_ship_unit;
   g_rec_out.rec_ship_unit          := g_rec_in.rec_ship_unit;
   g_rec_out.constraint_poh_unit    := g_rec_in.constraint_poh_unit;
   g_rec_out.safety_stock_unit      := g_rec_in.safety_stock_unit;
   g_rec_out.constraint_proj_avail  := g_rec_in.constraint_proj_avail;
   g_rec_out.expired_on_hand_unit   := g_rec_in.expired_on_hand_unit;
   g_rec_out.DC_FORWARD_COVER_DAY           := g_rec_in.DC_FORWARD_COVER_DAY;
   g_rec_out.last_updated_date      := g_date;
   g_rec_out.alt_constraint_unused_soh_unit         := g_rec_in.alt_constraint_unused_soh_unit;
   g_rec_out.alt_constraint_poh_unit                := g_rec_in.alt_constraint_poh_unit;
   g_rec_out.constraint_unmet_demand_unit           := g_rec_in.constraint_unmet_demand_unit ;
   g_rec_out.constraint_unused_soh_unit             := g_rec_in.constraint_unused_soh_unit;
   g_rec_out.expired_soh_unit                       := g_rec_in.expired_soh_unit;
   g_rec_out.ignored_demand_unit                    := g_rec_in.ignored_demand_unit ;
   g_rec_out.projected_stock_available_unit         := g_rec_in.projected_stock_available_unit ;

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
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions

MERGE INTO dwh_performance.rtl_jdaff_wh_plan_wk_analysis rtl_wkpln
USING
(SELECT  a_tbl_merge(i).SK1_ITEM_NO                     as  SK1_ITEM_NO,
         a_tbl_merge(i).sk1_location_no                as  sk1_location_no,
         a_tbl_merge(i).TRADING_DATE                    as  TRADING_DATE,
         a_tbl_merge(i).POST_DATE                       as  POST_DATE,
         a_tbl_merge(i).TOTAL_DEMAND_UNIT               as  TOTAL_DEMAND_UNIT,
         a_tbl_merge(i).INVENTORY_UNIT                  as  INVENTORY_UNIT,
         a_tbl_merge(i).PLANNED_ARRIVALS_UNIT           as  PLANNED_ARRIVALS_UNIT,
         a_tbl_merge(i).REC_ARRIVAL_UNIT                as  REC_ARRIVAL_UNIT,
         a_tbl_merge(i).IN_TRANSIT_UNIT                 as  IN_TRANSIT_UNIT,
         a_tbl_merge(i).PLAN_SHIP_UNIT                  as  PLAN_SHIP_UNIT,
         a_tbl_merge(i).REC_SHIP_UNIT                   as  REC_SHIP_UNIT,
         a_tbl_merge(i).CONSTRAINT_POH_UNIT             as  CONSTRAINT_POH_UNIT,
         a_tbl_merge(i).SAFETY_STOCK_UNIT               as  SAFETY_STOCK_UNIT,
         a_tbl_merge(i).CONSTRAINT_PROJ_AVAIL           as  CONSTRAINT_PROJ_AVAIL,
         a_tbl_merge(i).EXPIRED_ON_HAND_UNIT            as  EXPIRED_ON_HAND_UNIT,
         a_tbl_merge(i).dc_forward_cover_day            as  dc_forward_cover_day,
         a_tbl_merge(i).last_updated_date               as  last_updated_date,
         a_tbl_merge(i).alt_constraint_unused_soh_unit         as  alt_constraint_unused_soh_unit,
         a_tbl_merge(i).alt_constraint_poh_unit                as  alt_constraint_poh_unit,
         a_tbl_merge(i).constraint_unmet_demand_unit           as  constraint_unmet_demand_unit ,
         a_tbl_merge(i).constraint_unused_soh_unit             as  constraint_unused_soh_unit,
         a_tbl_merge(i).expired_soh_unit                       as  expired_soh_unit,
         a_tbl_merge(i).ignored_demand_unit                    as  ignored_demand_unit,
         a_tbl_merge(i).projected_stock_available_unit         as  projected_stock_available_unit

FROM dual) mer_rtlwk
ON (rtl_wkpln.SK1_ITEM_NO = mer_rtlwk.SK1_ITEM_NO
AND rtl_wkpln.sk1_location_no = mer_rtlwk.sk1_location_no
and rtl_wkpln.trading_date = mer_rtlwk.trading_date
AND rtl_wkpln.post_date = mer_rtlwk.post_date)
WHEN MATCHED THEN
UPDATE
SET      --POST_DATE                        = mer_rtlwk.POST_DATE,
         TOTAL_DEMAND_UNIT                = mer_rtlwk.TOTAL_DEMAND_UNIT,
         INVENTORY_UNIT                   = mer_rtlwk.INVENTORY_UNIT,
         PLANNED_ARRIVALS_UNIT            = mer_rtlwk.PLANNED_ARRIVALS_UNIT,
         REC_ARRIVAL_UNIT                 = mer_rtlwk.REC_ARRIVAL_UNIT,
         IN_TRANSIT_UNIT                  = mer_rtlwk.IN_TRANSIT_UNIT,
         PLAN_SHIP_UNIT                   = mer_rtlwk.PLAN_SHIP_UNIT,
         REC_SHIP_UNIT                    = mer_rtlwk.REC_SHIP_UNIT,
         CONSTRAINT_POH_UNIT              = mer_rtlwk.CONSTRAINT_POH_UNIT,
         SAFETY_STOCK_UNIT                = mer_rtlwk.SAFETY_STOCK_UNIT,
         CONSTRAINT_PROJ_AVAIL            = mer_rtlwk.CONSTRAINT_PROJ_AVAIL,
         EXPIRED_ON_HAND_UNIT             = mer_rtlwk.EXPIRED_ON_HAND_UNIT,
         dc_forward_cover_day             = mer_rtlwk.dc_forward_cover_day,
         last_updated_date                = mer_rtlwk.last_updated_date,
         alt_constraint_unused_soh_unit        = mer_rtlwk.alt_constraint_unused_soh_unit,
         alt_constraint_poh_unit                = mer_rtlwk.alt_constraint_poh_unit,
         constraint_unmet_demand_unit           = mer_rtlwk.constraint_unmet_demand_unit ,
         constraint_unused_soh_unit             = mer_rtlwk.constraint_unused_soh_unit,
         expired_soh_unit                       = mer_rtlwk.expired_soh_unit,
         ignored_demand_unit                    = mer_rtlwk.ignored_demand_unit,
         projected_stock_available_unit         = mer_rtlwk.projected_stock_available_unit
WHEN NOT MATCHED THEN
INSERT
(
         rtl_wkpln.SK1_ITEM_NO,
         rtl_wkpln.sk1_location_no,
         rtl_wkpln.TRADING_DATE,
         rtl_wkpln.POST_DATE,
         rtl_wkpln.TOTAL_DEMAND_UNIT,
         rtl_wkpln.INVENTORY_UNIT,
         rtl_wkpln.PLANNED_ARRIVALS_UNIT,
         rtl_wkpln.REC_ARRIVAL_UNIT,
         rtl_wkpln.IN_TRANSIT_UNIT,
         rtl_wkpln.PLAN_SHIP_UNIT,
         rtl_wkpln.REC_SHIP_UNIT,
         rtl_wkpln.CONSTRAINT_POH_UNIT,
         rtl_wkpln.SAFETY_STOCK_UNIT,
         rtl_wkpln.CONSTRAINT_PROJ_AVAIL,
         rtl_wkpln.EXPIRED_ON_HAND_UNIT,
         rtl_wkpln.DC_FORWARD_COVER_DAY,
         rtl_wkpln.last_updated_date,
         rtl_wkpln.alt_constraint_unused_soh_unit,
         rtl_wkpln.alt_constraint_poh_unit,
         rtl_wkpln.constraint_unmet_demand_unit ,
         rtl_wkpln.constraint_unused_soh_unit,
         rtl_wkpln.expired_soh_unit,
         rtl_wkpln.ignored_demand_unit,
         rtl_wkpln.projected_stock_available_unit

)
VALUES
(
         mer_rtlwk.SK1_ITEM_NO,
         mer_rtlwk.sk1_location_no,
         mer_rtlwk.TRADING_DATE,
         mer_rtlwk.POST_DATE,
         mer_rtlwk.TOTAL_DEMAND_UNIT,
         mer_rtlwk.INVENTORY_UNIT,
         mer_rtlwk.PLANNED_ARRIVALS_UNIT,
         mer_rtlwk.REC_ARRIVAL_UNIT,
         mer_rtlwk.IN_TRANSIT_UNIT,
         mer_rtlwk.PLAN_SHIP_UNIT,
         mer_rtlwk.REC_SHIP_UNIT,
         mer_rtlwk.CONSTRAINT_POH_UNIT,
         mer_rtlwk.SAFETY_STOCK_UNIT,
         mer_rtlwk.CONSTRAINT_PROJ_AVAIL,
         mer_rtlwk.EXPIRED_ON_HAND_UNIT,
         mer_rtlwk.dc_forward_cover_day,
         mer_rtlwk.LAST_UPDATED_DATE,
         mer_rtlwk.alt_constraint_unused_soh_unit,
         mer_rtlwk.alt_constraint_poh_unit,
         mer_rtlwk.constraint_unmet_demand_unit ,
         mer_rtlwk.constraint_unused_soh_unit,
         mer_rtlwk.expired_soh_unit,
         mer_rtlwk.ignored_demand_unit,
         mer_rtlwk.projected_stock_available_unit

);

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).trading_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

-- Place data into and array for later writing to table in bulk
   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m) := g_rec_out;
   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
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

    l_text := 'LOAD OF dwh_performance.rtl_jdaff_wh_plan_wk_analysis EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '05/JAN/15';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_jdaff_wh_plan;
    fetch c_jdaff_wh_plan bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_jdaff_wh_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jdaff_wh_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;

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
end wh_prf_corp_167u;
