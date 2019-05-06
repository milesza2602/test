--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_230FM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_230FM" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Dec 2010
--  Author:      Alastair de Wet
--  Purpose:     Load Daily forecast Table LL filtered measure table in performance layer
--               with input ex FCST & Dim tables from performance layer.
--  Tables:      Input  - RTL_LOC_ITEM_RDF_DYFCST_L1 & dim_item/dim_department
--               Output - RTL_LOC_ITEM_RDF_DYFCST_L1
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

g_rec_out            RTL_LOC_ITEM_RDF_DYFCST_L1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_230FM';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF RDF DAILY FCST FACTS EX RDF DAILY FCST/DIM_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_clean_count        integer;


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
    l_text := 'LOAD RTL_LOC_ITEM_RDF_DYFCST_L1 EX SELF TABLES STARTED '||
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
    
    l_text := 'MERGE STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';
      
   merge /*+ parallel(rdf_mart,5) */  into RTL_LOC_ITEM_RDF_DYFCST_L1 rdf_mart 
   using (   
       with pdate as (select /*+ parallel(fcstd,4) */ unique post_date from RTL_LOC_ITEM_RDF_DYFCST_L1 fcstd where fcstd.last_updated_date = g_date)

     select /*+ parallel(fcst,5) parallel(pdate,5) */ 
          fcst.sk1_item_no,
          fcst.sk1_location_no,
          fcst.post_date,
          fcst.sales_dly_app_fcst_qty,
          di.handling_method_code,
          di.department_no,
          fcst.sales_dly_app_fcst_qty_flt
  from    RTL_LOC_ITEM_RDF_DYFCST_L1 fcst, 
          pdate, 
          dim_item di
 
  where   fcst.post_date          = pdate.post_date
    and   di.sk1_item_no          = fcst.sk1_item_no
    and   fcst.last_updated_date  = g_date
    and   nvl(fcst.sales_dly_app_fcst_qty,0) <> nvl(fcst.sales_dly_app_fcst_qty_flt,0.1)
    

    and   di.department_no in
          (select  dd.department_no
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
       
  on (mer_mart.sk1_location_no = rdf_mart.sk1_location_no
  and mer_mart.sk1_item_no      = rdf_mart.sk1_item_no
  and mer_mart.post_date        = rdf_mart.post_date)
  
  when matched then
      update set sales_dly_app_fcst_qty_flt  = mer_mart.sales_dly_app_fcst_qty,
                 last_updated_date           = g_date
  ;

  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + sql%rowcount;
  g_recs_updated  := g_recs_updated  + sql%rowcount;     
  
   commit;
   
  l_text := 'MERGE DONE ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
    

    --execute immediate 'alter session set events ''10046 trace name context off'' ';

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

end wh_prf_rdf_230fm;
