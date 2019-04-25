--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_502U_ALOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_502U_ALOC" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
-- backup of current live wh_prf_corp_502u - taken on 22 nov 2010 for qc3977
-- new version to replace wh_prf_corp_502u
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LIWk Dense rollup fact table in the performance layer
--               with input ex lid dense table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - rtl_loc_item_wk_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -Tien Cheng
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
g_rec_out            rtl_loc_item_wk_rms_sparse%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date         date          ;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_502U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS SPARSE PERFORMANCE to WEEK';
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
    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_sparse EX DAY LEVEL STARTED '||
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

    select this_week_start_date-35
    into   g_start_date
    from   dim_calendar
    where  calendar_date = g_date;

    g_start_date := '26 jun 2017';
    g_end_date   := '23 jul 2017';

    l_text := 'RANGE OF ROLLUP IS:- '||g_start_date||' '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

merge /*+ parallel(rtl_liwrs,4) */ INTO dwh_performance.rtl_loc_item_wk_rms_sparse rtl_liwrs using
(  select  /*+ USE_HASH (lid, dc) PARALLEL (lid, 8) */ 
            lid.sk1_location_no as sk1_location_no,
            lid.sk1_item_no as sk1_item_no,
            dc.fin_year_no as fin_year_no,
            dc.fin_week_no as fin_week_no,
            max(lid.sk2_location_no) sk2_location_no,
            max(lid.sk2_item_no) sk2_item_no ,
            max(dc.fin_week_code) as fin_week_code,
            max(dc.this_week_start_date) as this_week_start_date,
            sum(lid.ch_alloc_qty) ch_alloc_qty,
            sum(lid.ch_alloc_selling) ch_alloc_selling,
            g_date as last_update_date 
   from     dwh_performance.rtl_loc_item_dy_rms_sparse lid,
            dwh_performance.dim_calendar dc
   where    lid.post_date         = dc.calendar_date  
   and      lid.post_date         between G_start_date and G_end_date
   and      ch_alloc_qty is not null
   group by lid.sk1_location_no, lid.sk1_item_no, dc.fin_year_no, dc.fin_week_no
) mer_liwrs 
    ON (rtl_liwrs.sk1_location_no     = mer_liwrs.sk1_location_no
    AND rtl_liwrs.sk1_item_no         = mer_liwrs.sk1_item_no
    AND rtl_liwrs.fin_year_no         = mer_liwrs.fin_year_no
    AND rtl_liwrs.fin_week_no         = mer_liwrs.fin_week_no)
    WHEN MATCHED THEN
     update
     set    rtl_liwrs.ch_alloc_qty                    = mer_liwrs.ch_alloc_qty,
            rtl_liwrs.ch_alloc_selling                = mer_liwrs.ch_alloc_selling,
            rtl_liwrs.last_updated_date               = g_date
     WHERE  mer_liwrs.ch_alloc_qty <>  rtl_liwrs.ch_alloc_qty
   ;

   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_updated  :=SQL%ROWCOUNT;

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

end wh_prf_corp_502u_aloc;
