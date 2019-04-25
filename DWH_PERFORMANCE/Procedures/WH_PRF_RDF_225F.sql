--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_225F
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_225F" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Mar 2011
--  Author:      Alastair de Wet
--  Purpose:     Load Daily forecast Table LL 13 week avrg measure in performance layer
--               with input ex Weekly FCST  performance layer.
--  Tables:      Input  - RTL_LOC_ITEM_RDF_WKFCST_L2
--               Output - RTL_LOC_ITEM_RDF_DYFCST_L2
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_sub                integer       :=  0;   
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            RTL_LOC_ITEM_RDF_DYFCST_L2%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date ;
g_end_date           date ;
g_cal_date           date ;
g_peri_end_date      date ;
g_fin_week_no        number;
g_fin_year_no        number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_225F';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF RDF 13 WKLY AVRG EX RDF WKLY TO DYFCST ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin

    merge /*+ parallel(dyfcst,4) */  into RTL_LOC_ITEM_RDF_DYFCST_L2 dyfcst using 
    (
     select /*+ parallel(fcst,4) */
            fcst.sk1_item_no,
            fcst.sk1_location_no,
            fcst.sk2_item_no,
            fcst.sk2_location_no,
            sales_wk_app_fcst_qty_av,
            sales_wk_app_fcst_qty_flt_av
       from RTL_LOC_ITEM_RDF_WKFCST_L2 fcst
       join dim_item di on di.sk1_item_no = fcst.sk1_item_no  
      where fin_week_no             = g_fin_week_no and
            fin_year_no             = g_fin_year_no and
            di.handling_method_code = 'S'    and
            sales_wk_app_fcst_qty_av is not null 
    ) mer_mart
    
    on (dyfcst.sk1_item_no      = mer_mart.sk1_item_no
    and dyfcst.sk1_location_no  = mer_mart.sk1_location_no
    and dyfcst.post_date        = g_cal_date)
      
    when matched then
      update 
        set sales_dly_app_fcst_qty_av     = mer_mart.sales_wk_app_fcst_qty_av,
            sales_dly_app_fcst_qty_flt_av = mer_mart.sales_wk_app_fcst_qty_flt_av,
            last_updated_date             = g_date
            
    when not matched then
      insert (  
              sk1_item_no,
              sk1_location_no,
              sk2_item_no,
              sk2_location_no,
              post_date,
              sales_dly_app_fcst_qty_av,
              sales_dly_app_fcst_qty_flt_av,
              last_updated_date
              )
      values ( 
              mer_mart.sk1_item_no,
              mer_mart.sk1_location_no,
              mer_mart.sk2_item_no,
              mer_mart.sk2_location_no,
              g_cal_date,
              mer_mart.sales_wk_app_fcst_qty_av,
              mer_mart.sales_wk_app_fcst_qty_flt_av,
              g_date
              )
     ;  
     
      g_recs_read     := g_recs_read + SQL%ROWCOUNT;
      g_recs_inserted := g_recs_inserted + sql%rowcount;
      g_recs_updated  := g_recs_updated  + sql%rowcount;     
     
    commit;

    

   exception
      when dwh_errors.e_insert_error then
       l_message := 'BULK MERGE UPDATE - UPDATE ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'BULK MERGE UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end local_bulk_merge;

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
    l_text := 'LOAD RTL_LOC_ITEM_RDF_DYFCST_L2 EX WK FCST STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '24/MAY/15';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    g_cal_date := g_date + 1;

    select fin_year_no,fin_week_no
    into   g_fin_year_no,g_fin_week_no
    from   dim_calendar
    where calendar_date = g_cal_date  ;


    l_text := 'YR/WK Being processed = '||g_fin_year_no|| '   '||g_fin_week_no ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Date being written to day table = '||g_cal_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';


    local_bulk_merge;
    
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

end wh_prf_rdf_225f;
