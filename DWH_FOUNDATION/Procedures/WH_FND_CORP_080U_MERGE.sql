--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_080U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_080U_MERGE" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2015
--  Author:      Alastair de Wet
--  Purpose:     Create location_item dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_location_item_cpy
--               Output - fnd_location_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_location_item_hsp.sys_process_msg%type;
mer_mart             stg_rms_location_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_080M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_rms_location_item_cpy.location_no%type; 
g_item_no           stg_rms_location_item_cpy.item_no%TYPE; 


   cursor stg_dup is
      select * from stg_rms_location_item_cpy
      where (location_no,item_no)
      in
      (select location_no,item_no
      from stg_rms_location_item_cpy 
      group by location_no,
      item_no
      having count(*) > 1) 
      order by location_no,
      item_no,
      sys_source_batch_id desc ,sys_source_sequence_no desc;
    

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_LOCATION_ITEM EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    
 
    select count(*)
    into   g_recs_read
    from   stg_rms_location_item_cpy                                                                                                                                                                                               
    where  sys_process_code = 'Y';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            update stg_rms_location_item_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


merge /*+ parallel (fnd_mart,6) */ 
    into w6005682.fnd_location_item_qs fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from stg_rms_location_item_cpy  tmp 
    join dim_item di           on tmp.item_no = di.item_no 
    join dim_location dl       on tmp.location_no = dl.location_no 
    join dim_supplier ds       on tmp.primary_supplier_no = ds.supplier_no
    where tmp.sys_process_code = 'Y'
      and tmp.clearance_ind in (0,1)
      and tmp.taxable_ind   in (0,1)
      and tmp.wh_supply_chain_type_ind in (0,1)
  
     ) mer_mart
  
on  (mer_mart.item_no        = fnd_mart.item_no
and  mer_mart.location_no    = fnd_mart.location_no
    )
when matched then
update
set       supply_chain_type               = mer_mart.supply_chain_type,
          reg_rsp                         = mer_mart.reg_rsp,
          selling_rsp                     = mer_mart.selling_rsp,
          selling_uom_code                = mer_mart.selling_uom_code,
          prom_rsp                        = mer_mart.prom_rsp,
          prom_selling_rsp                = mer_mart.prom_selling_rsp,
          prom_selling_uom_code           = mer_mart.prom_selling_uom_code,
          clearance_ind                   = mer_mart.clearance_ind,
          taxable_ind                     = mer_mart.taxable_ind,
          pos_item_desc                   = mer_mart.pos_item_desc,
          pos_short_desc                  = mer_mart.pos_short_desc,
          num_ti_pallet_tier_cases        = mer_mart.num_ti_pallet_tier_cases,
          num_hi_pallet_tier_cases        = mer_mart.num_hi_pallet_tier_cases,
          store_ord_mult_unit_type_code   = mer_mart.store_ord_mult_unit_type_code,
          loc_item_status_code            = mer_mart.loc_item_status_code,
          loc_item_stat_code_update_date  = mer_mart.loc_item_stat_code_update_date,
          avg_natural_daily_waste_perc    = mer_mart.avg_natural_daily_waste_perc,
          meas_of_each                    = mer_mart.meas_of_each,
          meas_of_price                   = mer_mart.meas_of_price,
          rsp_uom_code                    = mer_mart.rsp_uom_code,
          primary_variant_item_no         = mer_mart.primary_variant_item_no,
          primary_cost_pack_item_no       = mer_mart.primary_cost_pack_item_no,
          primary_supplier_no             = mer_mart.primary_supplier_no,
          primary_country_code            = mer_mart.primary_country_code,
          receive_as_pack_type            = mer_mart.receive_as_pack_type,
          source_method_loc_type          = mer_mart.source_method_loc_type,
          source_location_no              = mer_mart.source_location_no,
          wh_supply_chain_type_ind        = mer_mart.wh_supply_chain_type_ind,
          source_data_status_code         = mer_mart.source_data_status_code
WHEN NOT MATCHED THEN
INSERT
(         supply_chain_type,
          reg_rsp,
          selling_rsp,
          selling_uom_code,
          prom_rsp,
          prom_selling_rsp,
          prom_selling_uom_code,
          clearance_ind,
          taxable_ind,
          pos_item_desc,
          pos_short_desc,
          num_ti_pallet_tier_cases,
          num_hi_pallet_tier_cases,
          store_ord_mult_unit_type_code,
          loc_item_status_code,
          loc_item_stat_code_update_date,
          avg_natural_daily_waste_perc,
          meas_of_each,
          meas_of_price,
          rsp_uom_code,
          primary_variant_item_no,
          primary_cost_pack_item_no,
          primary_supplier_no,
          primary_country_code,
          receive_as_pack_type,
          source_method_loc_type,
          source_location_no,
          wh_supply_chain_type_ind,
          item_no,
          location_no,
          source_data_status_code,
          last_updated_date
          )
  values
(         mer_mart.supply_chain_type,
          mer_mart.reg_rsp,
          mer_mart.selling_rsp,
          mer_mart.selling_uom_code,
          mer_mart.prom_rsp,
          mer_mart.prom_selling_rsp,
          mer_mart.prom_selling_uom_code,
          mer_mart.clearance_ind,
          mer_mart.taxable_ind,
          mer_mart.pos_item_desc,
          mer_mart.pos_short_desc,
          mer_mart.num_ti_pallet_tier_cases,
          mer_mart.num_hi_pallet_tier_cases,
          mer_mart.store_ord_mult_unit_type_code,
          mer_mart.loc_item_status_code,
          mer_mart.loc_item_stat_code_update_date,
          mer_mart.avg_natural_daily_waste_perc,
          mer_mart.meas_of_each,
          mer_mart.meas_of_price,
          mer_mart.rsp_uom_code,
          mer_mart.primary_variant_item_no,
          mer_mart.primary_cost_pack_item_no,
          mer_mart.primary_supplier_no,
          mer_mart.primary_country_code,
          mer_mart.receive_as_pack_type,
          mer_mart.source_method_loc_type,
          mer_mart.source_location_no,
          mer_mart.wh_supply_chain_type_ind,
          mer_mart.item_no,
          mer_mart.location_no,
          mer_mart.source_data_status_code,
          g_date
          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_rms_location_item_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,LOC,SUPPLIER',
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.SUPPLY_CHAIN_TYPE,
                            TMP.REG_RSP,
                            TMP.SELLING_RSP,
                            TMP.SELLING_UOM_CODE,
                            TMP.PROM_RSP,
                            TMP.PROM_SELLING_RSP,
                            TMP.PROM_SELLING_UOM_CODE,
                            TMP.CLEARANCE_IND,
                            TMP.TAXABLE_IND,
                            TMP.POS_ITEM_DESC,
                            TMP.POS_SHORT_DESC,
                            TMP.NUM_TI_PALLET_TIER_CASES,
                            TMP.NUM_HI_PALLET_TIER_CASES,
                            TMP.STORE_ORD_MULT_UNIT_TYPE_CODE,
                            TMP.LOC_ITEM_STATUS_CODE,
                            TMP.LOC_ITEM_STAT_CODE_UPDATE_DATE,
                            TMP.AVG_NATURAL_DAILY_WASTE_PERC,
                            TMP.MEAS_OF_EACH,
                            TMP.MEAS_OF_PRICE,
                            TMP.RSP_UOM_CODE,
                            TMP.PRIMARY_VARIANT_ITEM_NO,
                            TMP.PRIMARY_COST_PACK_ITEM_NO,
                            TMP.PRIMARY_SUPPLIER_NO,
                            TMP.PRIMARY_COUNTRY_CODE,
                            TMP.RECEIVE_AS_PACK_TYPE,
                            TMP.SOURCE_METHOD_LOC_TYPE,
                            TMP.SOURCE_LOCATION_NO,
                            TMP.WH_SUPPLY_CHAIN_TYPE_IND,
                            TMP.SOURCE_DATA_STATUS_CODE
    from  stg_rms_location_item_cpy  TMP 
    where ( tmp.clearance_ind not in (0,1)
         or tmp.taxable_ind   not in (0,1)
         or tmp.wh_supply_chain_type_ind not in (0,1)
         or
         not exists
          (select *
           from   dim_item di
           where  tmp.item_no   = di.item_no )  
         or
         not exists
           (select *
           from   dim_location dl
           where  tmp.location_no       = dl.location_no )
         or
         not exists
           (select *
           from   dim_supplier ds
           where  tmp.primary_supplier_no  = ds.supplier_no )
          )  
          and sys_process_code = 'Y'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;          
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
end wh_fnd_corp_080u_merge;
