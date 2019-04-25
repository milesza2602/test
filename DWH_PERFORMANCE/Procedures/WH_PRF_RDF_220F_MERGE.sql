--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_220F_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_220F_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Dec 2010
--  Author:      Alastair de Wet
--  Purpose:     Load Daily forecast Table LL&Peri 21 Day avrg measure in performance layer
--               with input ex FCST & Dim tables from performance layer.
--  Tables:      Input  - RTL_LOC_ITEM_RDF_DYFCST_L2
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            RTL_LOC_ITEM_RDF_DYFCST_L2%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date ;
g_end_date           date ;
g_post_date          date ;
g_fin_week_no        number;
g_fin_year_no        number;
g_min_last_upd_date  date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_220FM';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF RDF 21 Day AVRG EX RDF DAILY FCST ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- 
--**************************************************************************************************
procedure bulk_merge AS
begin

  merge /*+ parallel(dyfcst,4) */ into RTL_LOC_ITEM_RDF_DYFCST_L2 dyfcst using  
  (
    select /*+ parallel(fcst,4) */
          fcst.sk1_item_no,
          fcst.sk1_location_no,
          fcst.post_date,
          fcst.sales_dly_app_fcst_qty,
          di.handling_method_code,
          di.department_no,
          fcst.sales_dly_app_fcst_qty_flt
  from    RTL_LOC_ITEM_RDF_DYFCST_L2 fcst
  join    dim_item di on
          di.sk1_item_no          = fcst.sk1_item_no
  where   fcst.last_updated_date  = g_date and
          nvl(fcst.sales_dly_app_fcst_qty,0) <>
          nvl(fcst.sales_dly_app_fcst_qty_flt,0.1)
  
    and   di.department_no in
          (select dd.department_no
             from dim_department dd
           where not
           (jv_dept_ind            = 1 or
            book_magazine_dept_ind = 1 or
            non_core_dept_ind      = 1 or
            gifting_dept_ind       = 1 or
            packaging_dept_ind     = 1 or
            bucket_dept_ind        = 1) and
            dd.business_unit_no = 50 )
  
         ) mer_mart
       
  on (dyfcst.sk1_item_no      = mer_mart.sk1_item_no
  and dyfcst.sk1_location_no  = mer_mart.sk1_location_no
  and dyfcst.post_date        = mer_mart.post_date)
  
  when matched then
    update  
      set sales_dly_app_fcst_qty_flt     = mer_mart.sales_dly_app_fcst_qty,
         last_updated_date               = g_date
    ;
    
    commit;
  
    g_recs_read     := g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + sql%rowcount;
    g_recs_updated  := g_recs_updated  + sql%rowcount;
  
     exception
      when dwh_errors.e_insert_error then
       l_message := 'BULK MERGE UPDATE - UPDATE ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'BULK MERGE UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 
end bulk_merge;

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
    l_text := 'LOAD RTL_LOC_ITEM_RDF_DYFCST_L2 EX SELF TABLES STARTED '||
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
    execute immediate 'alter session ENABLE parallel dml';


--**************************************************************************************************
-- Do the merge.
--**************************************************************************************************

    bulk_merge;
    
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

end WH_PRF_RDF_220F_MERGE;
