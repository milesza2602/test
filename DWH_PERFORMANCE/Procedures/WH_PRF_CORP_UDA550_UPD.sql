--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_UDA550_UPD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_UDA550_UPD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2017
--  Author:      Mariska Matthee
--  Purpose:     This procedure will update the 'N' value for UDA550 to 'No Value'
--
--  Tables:      Input  -  
--               Output - 
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

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;
g_start_date         date;
g_end_date           date;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_UDA550_UPD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE UDA550 N VALUE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

--**************************************************************************************************
-- Update Performance tables
--**************************************************************************************************

    l_text := 'UPDATE PERFORMANCE TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dim_sc_uda dim
       set dim.organics_desc_550 = (select 'No Value' organics_desc_550
                                      from dim_sc_uda stg
                                     where stg.sk1_style_colour_no = 19089946
                                       and stg.sk1_style_colour_no = dim.sk1_style_colour_no)
     where exists (select stg.sk1_style_colour_no
                     from dim_sc_uda stg
                    where stg.sk1_style_colour_no = 19089946
                      and stg.sk1_style_colour_no = dim.sk1_style_colour_no);
    commit;
    
    update dim_ast_sc_uda dim
       set dim.organics_desc_550 = (select 'No Value' organics_desc_550
                                      from dim_ast_sc_uda stg
                                     where stg.sk1_style_colour_no = 19089946
                                       and stg.sk1_style_colour_no = dim.sk1_style_colour_no)
     where exists (select stg.sk1_style_colour_no
                     from dim_ast_sc_uda stg
                    where stg.sk1_style_colour_no = 19089946
                      and stg.sk1_style_colour_no = dim.sk1_style_colour_no);
    commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'UPDATE COMPLETED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
end WH_PRF_CORP_UDA550_UPD;