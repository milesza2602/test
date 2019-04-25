--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_151U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_151U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF fnd_jdaff_wh_plan_dy_analysis table from foundation layer.
--
--  Tables:      Input  - fnd_jdaff_wh_plan_dy_analysis
--               Output - dwh_performance.rtl_jdaff_wh_plan_dy_analysis
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.rtl_jdaff_wh_plan_dy_analysis%rowtype;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_151U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



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
    l_text := 'LOAD OF dwh_performance.rtl_jdaff_wh_plan_dy_analysis EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE /*+ PARALLEL(rtl_anl,6) */ INTO rtl_jdaff_wh_plan_dy_analysis rtl_anl using 
    (
       select  /*+ PARALLEL(jdaff,6) */ 
              di.sk1_item_no,
              dl.sk1_location_no,
              trading_date,
              jdaff.post_date,
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
      
        from  fnd_jdaff_wh_plan_dy_analysis jdaff,
              dim_item di,
              dim_location dl

  where jdaff.item_no           = di.item_no
    and jdaff.location_no       = dl.LOCATION_NO
    and jdaff.last_updated_date = g_date
    
  ) mer_mart
   on (rtl_anl.sk1_item_no      = mer_mart.sk1_item_no
    and rtl_anl.sk1_location_no = mer_mart.sk1_location_no
    and rtl_anl.trading_date    = mer_mart.trading_date
    and rtl_anl.post_date       = mer_mart.post_date)
    
  when matched then
    update 
       set  TOTAL_DEMAND_UNIT               = mer_mart.TOTAL_DEMAND_UNIT,
            INVENTORY_UNIT	                =	mer_mart.INVENTORY_UNIT,
            PLANNED_ARRIVALS_UNIT	          =	mer_mart.PLANNED_ARRIVALS_UNIT,
            REC_ARRIVAL_UNIT	              =	mer_mart.REC_ARRIVAL_UNIT,
            IN_TRANSIT_UNIT	                =	mer_mart.IN_TRANSIT_UNIT,
            PLAN_SHIP_UNIT	                =	mer_mart.PLAN_SHIP_UNIT,
            REC_SHIP_UNIT	                  =	mer_mart.REC_SHIP_UNIT,
            CONSTRAINT_POH_UNIT	            =	mer_mart.CONSTRAINT_POH_UNIT,
            SAFETY_STOCK_UNIT	              =	mer_mart.SAFETY_STOCK_UNIT,
            CONSTRAINT_PROJ_AVAIL	          =	mer_mart.CONSTRAINT_PROJ_AVAIL,
            EXPIRED_ON_HAND_UNIT	          =	mer_mart.EXPIRED_ON_HAND_UNIT,
            LAST_UPDATED_DATE	              =	g_date,
            DC_FORWARD_COVER_DAY	          =	mer_mart.DC_FORWARD_COVER_DAY,
            ALT_CONSTRAINT_UNUSED_SOH_UNIT	=	mer_mart.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
            ALT_CONSTRAINT_POH_UNIT	        =	mer_mart.ALT_CONSTRAINT_POH_UNIT,
            CONSTRAINT_UNMET_DEMAND_UNIT	  =	mer_mart.CONSTRAINT_UNMET_DEMAND_UNIT,
            CONSTRAINT_UNUSED_SOH_UNIT	    =	mer_mart.CONSTRAINT_UNUSED_SOH_UNIT,
            EXPIRED_SOH_UNIT	              =	mer_mart.EXPIRED_SOH_UNIT,
            IGNORED_DEMAND_UNIT	            =	mer_mart.IGNORED_DEMAND_UNIT,
            PROJECTED_STOCK_AVAILABLE_UNIT  =	mer_mart.PROJECTED_STOCK_AVAILABLE_UNIT
  
  when not matched then       
    insert 
      ( rtl_anl.SK1_ITEM_NO,
        rtl_anl.SK1_LOCATION_NO,
        rtl_anl.TRADING_DATE,
        rtl_anl.POST_DATE,
        rtl_anl.TOTAL_DEMAND_UNIT,
        rtl_anl.INVENTORY_UNIT,
        rtl_anl.PLANNED_ARRIVALS_UNIT,
        rtl_anl.REC_ARRIVAL_UNIT,
        rtl_anl.IN_TRANSIT_UNIT,
        rtl_anl.PLAN_SHIP_UNIT,
        rtl_anl.REC_SHIP_UNIT,
        rtl_anl.CONSTRAINT_POH_UNIT,
        rtl_anl.SAFETY_STOCK_UNIT,
        rtl_anl.CONSTRAINT_PROJ_AVAIL,
        rtl_anl.EXPIRED_ON_HAND_UNIT,
        rtl_anl.LAST_UPDATED_DATE,
        rtl_anl.DC_FORWARD_COVER_DAY,
        rtl_anl.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
        rtl_anl.ALT_CONSTRAINT_POH_UNIT,
        rtl_anl.CONSTRAINT_UNMET_DEMAND_UNIT,
        rtl_anl.CONSTRAINT_UNUSED_SOH_UNIT,
        rtl_anl.EXPIRED_SOH_UNIT,
        rtl_anl.IGNORED_DEMAND_UNIT,
        rtl_anl.PROJECTED_STOCK_AVAILABLE_UNIT
       )
    values
       (mer_mart.SK1_ITEM_NO,
        mer_mart.SK1_LOCATION_NO,
        mer_mart.TRADING_DATE,
        mer_mart.POST_DATE,
        mer_mart.TOTAL_DEMAND_UNIT,
        mer_mart.INVENTORY_UNIT,
        mer_mart.PLANNED_ARRIVALS_UNIT,
        mer_mart.REC_ARRIVAL_UNIT,
        mer_mart.IN_TRANSIT_UNIT,
        mer_mart.PLAN_SHIP_UNIT,
        mer_mart.REC_SHIP_UNIT,
        mer_mart.CONSTRAINT_POH_UNIT,
        mer_mart.SAFETY_STOCK_UNIT,
        mer_mart.CONSTRAINT_PROJ_AVAIL,
        mer_mart.EXPIRED_ON_HAND_UNIT,
        g_date,
        mer_mart.DC_FORWARD_COVER_DAY,
        mer_mart.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
        mer_mart.ALT_CONSTRAINT_POH_UNIT,
        mer_mart.CONSTRAINT_UNMET_DEMAND_UNIT,
        mer_mart.CONSTRAINT_UNUSED_SOH_UNIT,
        mer_mart.EXPIRED_SOH_UNIT,
        mer_mart.IGNORED_DEMAND_UNIT,
        mer_mart.PROJECTED_STOCK_AVAILABLE_UNIT
      )
    ;  
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
      g_recs_read :=  g_recs_read + SQL%ROWCOUNT;
      
      commit;

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

end wh_prf_corp_151u;
