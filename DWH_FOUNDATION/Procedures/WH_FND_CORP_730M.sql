--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_730M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_730M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Purchase Order fact table in the foundation layer
--               with input ex staging table from TRICEPS GRN.
--  Tables:      Input  - stg_trcps_rtl_purchase_ord_cpy
--               Output - fnd_rtl_purchase_order
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  23 Feb 2009 - A Joshua : TD-946 - Change various column attributes/names
--                                  - Update ETL for this columns
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
g_recs_duplicate     integer       :=  0;  
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_trcps_rtl_purchase_ord_hsp.sys_process_msg%type;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_730M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PURCHASE_ORDER FACTS EX TRICEPS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_trcps_rtl_purchase_ord_cpy.location_no%type; 
g_item_no           stg_trcps_rtl_purchase_ord_cpy.item_no%TYPE; 
g_po_no             stg_trcps_rtl_purchase_ord_cpy.po_no%type;


   cursor stg_dup is
      select * from stg_trcps_rtl_purchase_ord_cpy
       where (po_no, location_no,item_no)
          in
      (select po_no, location_no,item_no
         from stg_trcps_rtl_purchase_ord_cpy 
        group by po_no, location_no, item_no
       having count(*) > 1
       ) 
      order by  po_no, location_no, item_no,
      sys_source_batch_id,  sys_source_sequence_no ;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure bulk_merge as
begin

 -- merge into fnd_rtl_purchase_order fnd_po using 
  MERGE /*+ parallel(fnd_po, 4) */ INTO W6005682.FND_RTL_PURCHASE_ORDERQ FND_PO USING 
  (
    select  /*+ parallel(stg, 4) full(stg) */ stg.PO_NO,
            stg.ITEM_NO,
            stg.LOCATION_NO,
            stg.REJECTED_CASES,
            stg.ACTL_TEMPERATURE,
            stg.MASS_AUDIT_IND,
            stg.IN_TIME,
            stg.OUT_TIME,
            stg.CALC_TOLERANCE,
            stg.SUPPLIER_DELIV_NO,
            stg.SOURCE_DATA_STATUS_CODE,
            stg.REJECTED_BARCODE_IND,
            stg.REJECTED_QTY_PER_LUG_IND,
            stg.REJECTED_NUM_SELL_BY_DAYS_IND,
            stg.REJECTED_SELL_PRICE_IND,
            stg.REJECTED_IDEAL_TEMP_IND,
            stg.REJECTED_MAX_TEMP_RANGE_IND,
            stg.REJECTED_ALT_SUPP_IND,
            stg.REJECTED_OVER_DELIVERY_IND,
            stg.REJECTED_TOLERANCE_MASS_IND,
            stg.REJECTED_OUT_CASE_IND, 
            
            case when stg.rejected_cases > 0 then 1 else 0 end AS reject_ind
             
       from stg_trcps_rtl_purchase_ord_cpy stg,
            fnd_item fi,
            fnd_location fl,
            fnd_rtl_purchase_order fpo
            
       where stg.item_no      = fi.item_no
         and stg.location_no  = fl.location_no
         and stg.po_no        = fpo.po_no
         and stg.item_no      = fpo.item_no
         and stg.location_no  = fpo.location_no
         and stg.sys_process_code = 'N'
   ) mer_mart
   
   on (fnd_po.po_no         = mer_mart.po_no
  and  fnd_po.item_no       = mer_mart.item_no
  and  fnd_po.location_no   = mer_mart.location_no)
  
  when matched then
    update 
       set    rejected_cases                  = nvl(rejected_cases,0)            + nvl(mer_mart.rejected_cases,0),
              rejected_barcode                = nvl(rejected_barcode,0)          + nvl(mer_mart.rejected_barcode_ind,0),
              rejected_qty_per_lug            = nvl(rejected_qty_per_lug,0)      + nvl(mer_mart.rejected_qty_per_lug_ind,0),
              rejected_num_sell_by_days       = nvl(rejected_num_sell_by_days,0) + nvl(mer_mart.rejected_num_sell_by_days_ind,0),
              rejected_sell_price             = nvl(rejected_sell_price,0)       + nvl(mer_mart.rejected_sell_price_ind,0),
              rejected_ideal_temp             = nvl(rejected_ideal_temp,0)       + nvl(mer_mart.rejected_ideal_temp_ind,0),
              rejected_max_temp_range         = nvl(rejected_max_temp_range,0)   + nvl(mer_mart.rejected_max_temp_range_ind,0),
              rejected_alt_supp               = nvl(rejected_alt_supp,0)         + nvl(mer_mart.rejected_alt_supp_ind,0),
              rejected_over_delivery          = nvl(rejected_over_delivery,0)    + nvl(mer_mart.rejected_over_delivery_ind,0),
              rejected_tolerance_mass         = nvl(rejected_tolerance_mass,0)   + nvl(mer_mart.rejected_tolerance_mass_ind,0),
              rejected_out_case               = nvl(rejected_out_case,0)         + nvl(mer_mart.rejected_out_case_ind,0),
              actl_temperature                = mer_mart.actl_temperature,
              mass_audit_ind                  = mer_mart.mass_audit_ind,
              in_time                         = mer_mart.in_time,
              out_time                        = mer_mart.out_time,
              calc_tolerance                  = mer_mart.calc_tolerance,
              supplier_deliv_no               = mer_mart.supplier_deliv_no,
              reject_ind                      = mer_mart.reject_ind,
              last_updated_date               = g_date
       where  po_no                           = mer_mart.po_no and
              item_no                         = mer_mart.item_no and
              location_no                     = mer_mart.location_no 
       ;


  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  0;

  commit;
 
  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
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

    l_text := 'LOAD OF FND_RTL_PURCHASE_ORDER EX TRICEPS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
  select count(*)
    into   g_recs_read
    from   stg_trcps_rtl_purchase_ord_cpy                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_po_no          := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.po_no         = g_po_no and
            dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            update stg_trcps_rtl_purchase_ord_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
        g_po_no          := dupp_record.po_no;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 --=====================================================================================   
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    bulk_merge;
    
    l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--======================================================================================

   insert /*+ APPEND parallel (hsp,2) */ into stg_trcps_rtl_purchase_ord_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'N','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,LOC,SUPPLIER',
                            TMP.PO_NO,
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.REJECTED_CASES,
                            TMP.REJECTED_BARCODE_IND,
                            TMP.REJECTED_QTY_PER_LUG_IND,
                            TMP.REJECTED_NUM_SELL_BY_DAYS_IND,
                            TMP.REJECTED_SELL_PRICE_IND,
                            TMP.REJECTED_IDEAL_TEMP_IND,
                            TMP.REJECTED_MAX_TEMP_RANGE_IND,
                            TMP.REJECTED_ALT_SUPP_IND,
                            TMP.REJECTED_OVER_DELIVERY_IND,
                            TMP.REJECTED_TOLERANCE_MASS_IND,
                            TMP.REJECTED_OUT_CASE_IND,
                            TMP.ACTL_TEMPERATURE,
                            TMP.MASS_AUDIT_IND,
                            TMP.IN_TIME,
                            TMP.OUT_TIME,
                            TMP.CALC_TOLERANCE,
                            TMP.SUPPLIER_DELIV_NO,
                            TMP.SOURCE_DATA_STATUS_CODE
  
    from  stg_trcps_rtl_purchase_ord_cpy  TMP 
   where not exists
      (select *
           from   fnd_item di
           where  tmp.item_no     = di.item_no )  
         or
         not exists
      (select *
           from   fnd_location dl
           where  tmp.location_no = dl.location_no )
     and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end wh_fnd_corp_730m;
