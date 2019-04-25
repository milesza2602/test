--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_053U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_053U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        Mar 2018
--  Author:      Theo Filander
--  Purpose:     Create ITEM VAT RATE table in the performance layer
--               with input ex fnd_item_vat_rate table from foundation layer.
--
--  Tables:      Input  - fnd_item_vat_rate
--                        dim_item
--               Output - rtl_item_vat_rate
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_053U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM VAT RATE EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_ITEM_VAT_RATE EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    merge /*+ Parallel(rtl,12) */ into dwh_performance.rtl_item_vat_rate rtl
    using (select /*+ Parallel(fnd,12) Parallel(di,12) */ 
                  fnd.item_no,
                  di.sk1_item_no,
                  fnd.vat_region_no,
                  fnd.vat_code,
                  fnd.active_from_date,
                  fnd.vat_type,
                  fnd.vat_code_desc,
                  fnd.vat_rate_perc,
                  fnd.vat_region_type,
                  fnd.vat_region_name,
                  fnd.source_data_status_code,
                  fnd.last_updated_date,
                  fnd.active_ind,
                  fnd.active_to_date
             from dwh_foundation.fnd_item_vat_rate fnd
             inner join dwh_performance.dim_item di on (fnd.item_no = di.item_no)
             where fnd.last_updated_date    = g_date
            ) rec on (rtl.item_no           = rec.item_no and
                      rtl.vat_region_no     = rec.vat_region_no and
                      rtl.vat_code          = rec.vat_code and 
                      rtl.active_from_date  = rec.active_from_date)
     when not matched then insert (item_no,
                                   sk1_item_no,
                                   vat_region_no,
                                   vat_code,
                                   active_from_date,
                                   vat_type,
                                   vat_code_desc,
                                   vat_rate_perc,
                                   vat_region_type,
                                   vat_region_name,
                                   source_data_status_code,
                                   last_updated_date,
                                   active_ind,
                                   active_to_date    
                                  )
                           values (rec.item_no,
                                   rec.sk1_item_no,
                                   rec.vat_region_no,
                                   rec.vat_code,
                                   rec.active_from_date,
                                   rec.vat_type,
                                   rec.vat_code_desc,
                                   rec.vat_rate_perc,
                                   rec.vat_region_type,
                                   rec.vat_region_name,
                                   rec.source_data_status_code,
                                   rec.last_updated_date,
                                   rec.active_ind,
                                   rec.active_to_date
                                  )
     when matched then update set rtl.vat_type                  = rec.vat_type,
                                  rtl.vat_code_desc             = rec.vat_code_desc,
                                  rtl.vat_rate_perc             = rec.vat_rate_perc,
                                  rtl.vat_region_name           = rec.vat_region_name,
                                  rtl.source_data_status_code   = rec.source_data_status_code,
                                  rtl.last_updated_date         = rec.last_updated_date,
                                  rtl.active_ind                = rec.active_ind,
                                  rtl.active_to_date            = rec.active_to_date
     ;

    g_recs_inserted := sql%rowcount;
    commit;

    l_text := 'GATHER STATS ON DWH_PERFORMANCE.RTL_ITEM_VAT_RATE starting';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dbms_stats.gather_table_stats ('DWH_PERFORMANCE','RTL_ITEM_VAT_RATE',estimate_percent=>0.1, DEGREE => 16);
    commit;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
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
end wh_prf_corp_053u;
