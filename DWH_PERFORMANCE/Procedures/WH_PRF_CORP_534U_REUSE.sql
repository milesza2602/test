--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_534U_REUSE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_534U_REUSE" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Oct 2009
--  Author:      M Munnik
--  Purpose:     Create LIWk RMS_ALLOC rollup fact table in the performance layer
--               with input ex LIDy RMS_ALLOC table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_alloc
--               Output - rtl_loc_item_wk_rms_alloc
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_alloc%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_534U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP PERFORMANCE RMS ALLOC to WEEK LEVEL';
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
    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_alloc EX DAY LEVEL STARTED '||
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

    for g_sub in 0..14 loop
       select this_week_start_date, this_week_end_date
       into   g_start_date, g_end_date
       from   dim_calendar
       where  calendar_date = g_date - (g_sub * 7);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
--execute immediate 'alter session enable parallel dml';

       insert   /*+ append */ into rtl_loc_item_wk_rms_alloc
       select   lid.sk1_location_no,
                lid.sk1_item_no,
                dc.fin_year_no,
                dc.fin_week_no,
                max(dc.fin_week_code),
                max(dc.this_week_start_date),
                max(lid.sk2_location_no),
                max(lid.sk2_item_no),
                sum(lid.fd_alloc_selling),
                sum(lid.fd_alloc_cost),
                sum(lid.fd_alloc_qty),
                sum(lid.fd_alloc_cases),
                sum(lid.fd_apportion_selling),
                sum(lid.fd_apportion_cost),
                sum(lid.fd_apportion_qty),
                sum(lid.fd_apportion_cases),
                sum(lid.fd_sdn_selling),
                sum(lid.fd_sdn_cost),
                sum(lid.fd_sdn_qty),
                sum(lid.fd_sdn_cases),
                sum(lid.fd_orig_alloc_selling),
                sum(lid.fd_orig_alloc_cost),
                sum(lid.fd_orig_alloc_qty),
                sum(lid.fd_orig_alloc_cases),
                sum(lid.fd_dist_selling),
                sum(lid.fd_dist_cost),
                sum(lid.fd_dist_qty),
                sum(lid.fd_dist_cases),
                sum(lid.fd_received_selling),
                sum(lid.fd_received_cost),
                sum(lid.fd_received_qty),
                sum(lid.fd_received_cases),
                sum(lid.fd_alloc_cancel_selling),
                sum(lid.fd_alloc_cancel_cost),
                sum(lid.fd_alloc_cancel_qty),
                sum(lid.fd_alloc_cancel_cases),
                sum(lid.fd_p1_picking_selling),
                sum(lid.fd_p1_picking_cost),
                sum(lid.fd_p1_picking_qty),
                sum(lid.fd_p1_picking_cases),
                sum(lid.fd_p2_picking_selling),
                sum(lid.fd_p2_picking_cost),
                sum(lid.fd_p2_picking_qty),
                sum(lid.fd_p2_picking_cases),
                sum(lid.fd_p3_picking_selling),
                sum(lid.fd_p3_picking_cost),
                sum(lid.fd_p3_picking_qty),
                sum(lid.fd_p3_picking_cases),
                sum(lid.fd_p4_picking_selling),
                sum(lid.fd_p4_picking_cost),
                sum(lid.fd_p4_picking_qty),
                sum(lid.fd_p4_picking_cases),
                sum(lid.ch_apportion_qty),
                sum(lid.ch_apportion_selling),
                g_date last_updated_date
       from     rtl_loc_item_dy_rms_alloc lid
       join     dim_calendar dc       on lid.calendar_date = dc.calendar_date
       where    lid.calendar_date     between g_start_date and g_end_date 
       and      lid.sk1_item_no IN
                (
                182035,
                552207,
                540204,
                551760
                ) 
       
       group by dc.fin_year_no,
                dc.fin_week_no,
                lid.sk1_location_no,
                lid.sk1_item_no;

       g_recs_read     := g_recs_read + sql%rowcount;
       g_recs_inserted := g_recs_inserted + sql%rowcount;

       l_text := 'ROLLED UP - '||g_start_date||' to '||g_end_date||' at '||
            to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount||' total '||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
    end loop;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end wh_prf_corp_534u_reuse;
