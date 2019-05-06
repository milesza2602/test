--------------------------------------------------------
--  DDL for Procedure WH_FND_ADW_INSMRG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_ADW_INSMRG" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create location_item fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_location_item_cpy                                                                                                                                                                                               
--               Output - fnd_location_item                                                                                                                      
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
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
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;


g_location_no       stg_rms_location_item_cpy.location_no%type; 
g_item_no           stg_rms_location_item_cpy.item_no%TYPE; 


   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_080IM';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD LOCATION_ITEM EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_location_no    := 0; 
   g_item_no        := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.location_no   = g_location_no and
        dupp_record.item_no       = g_item_no then
        update stg_rms_location_item_cpy                                                                                                                                                                                                stg
        set     sys_process_code       = 'D'
        where   sys_source_batch_id    = dupp_record.sys_source_batch_id and
                sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_location_no    := dupp_record.location_no; 
    g_item_no        := dupp_record.item_no;


   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_location_item                                                                                                                       fnd
         (fnd.supply_chain_type,
          fnd.reg_rsp,
          fnd.selling_rsp,
          fnd.selling_uom_code,
          fnd.prom_rsp,
          fnd.prom_selling_rsp,
          fnd.prom_selling_uom_code,
          fnd.clearance_ind,
          fnd.taxable_ind,
          fnd.pos_item_desc,
          fnd.pos_short_desc,
          fnd.num_ti_pallet_tier_cases,
          fnd.num_hi_pallet_tier_cases,
          fnd.store_ord_mult_unit_type_code,
          fnd.loc_item_status_code,
          fnd.loc_item_stat_code_update_date,
          fnd.avg_natural_daily_waste_perc,
          fnd.meas_of_each,
          fnd.meas_of_price,
          fnd.rsp_uom_code,
          fnd.primary_variant_item_no,
          fnd.primary_cost_pack_item_no,
          fnd.primary_supplier_no,
          fnd.primary_country_code,
          fnd.receive_as_pack_type,
          fnd.source_method_loc_type,
          fnd.source_location_no,
          fnd.wh_supply_chain_type_ind,
          fnd.item_no,
          fnd.location_no,
          fnd.source_data_status_code,
          fnd.last_updated_date)
      select /*+ FULL(cpy)  parallel (cpy,2) */
          cpy.supply_chain_type,
          cpy.reg_rsp,
          cpy.selling_rsp,
          cpy.selling_uom_code,
          cpy.prom_rsp,
          cpy.prom_selling_rsp,
          cpy.prom_selling_uom_code,
          cpy.clearance_ind,
          cpy.taxable_ind,
          cpy.pos_item_desc,
          cpy.pos_short_desc,
          cpy.num_ti_pallet_tier_cases,
          cpy.num_hi_pallet_tier_cases,
          cpy.store_ord_mult_unit_type_code,
          cpy.loc_item_status_code,
          cpy.loc_item_stat_code_update_date,
          cpy.avg_natural_daily_waste_perc,
          cpy.meas_of_each,
          cpy.meas_of_price,
          cpy.rsp_uom_code,
          cpy.primary_variant_item_no,
          cpy.primary_cost_pack_item_no,
          cpy.primary_supplier_no,
          cpy.primary_country_code,
          cpy.receive_as_pack_type,
          cpy.source_method_loc_type,
          cpy.source_location_no,
          cpy.wh_supply_chain_type_ind,
          cpy.item_no,
          cpy.location_no,
          cpy.source_data_status_code,
          g_date as last_updated_date
       from  stg_rms_location_item_cpy                                                                                                                                                                                                cpy,
              dim_item di,
              dim_location dl,
              dim_supplier ds
       where  cpy.item_no             = di.item_no and 
              cpy.location_no         = dl.location_no and
              cpy.primary_supplier_no = ds.supplier_no and
       not exists 
      (select /*+ nl_aj */ * from fnd_location_item                                                                                                                       
       where  location_no    = cpy.location_no and
              item_no        = cpy.item_no )
         and  cpy.clearance_ind in (0,1)
         and  cpy.taxable_ind in (0,1)
         and  cpy.wh_supply_chain_type_ind in (0,1) 
         and  sys_process_code = 'N'                                                                           ;

      g_recs_inserted := g_recs_inserted + sql%rowcount;
      
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin

   MERGE /*+ parallel (fnd,6) */  INTO fnd_location_item fnd 
   USING (
         select /*+ FULL(cpy) */  
                  cpy.*
         from    stg_rms_location_item_cpy                                                                                                                         cpy 
         join dim_item di           on cpy.item_no = di.item_no 
         join dim_location dl       on cpy.location_no = dl.location_no 
         join dim_supplier ds       on cpy.primary_supplier_no = ds.supplier_no
         where cpy.clearance_ind in (0,1)
         and   cpy.taxable_ind   in (0,1)
         and   cpy.wh_supply_chain_type_ind in (0,1)
         and   cpy.sys_process_code  = 'N'
        ) mer_rec
 
   ON    (  mer_rec.location_no	            =	fnd.location_no and
            mer_rec.item_no	                =	fnd.item_no	)
   WHEN MATCHED THEN 
   UPDATE 
   SET    supply_chain_type               = mer_rec.supply_chain_type,
          reg_rsp                         = mer_rec.reg_rsp,
          selling_rsp                     = mer_rec.selling_rsp,
          selling_uom_code                = mer_rec.selling_uom_code,
          prom_rsp                        = mer_rec.prom_rsp,
          prom_selling_rsp                = mer_rec.prom_selling_rsp,
          prom_selling_uom_code           = mer_rec.prom_selling_uom_code,
          clearance_ind                   = mer_rec.clearance_ind,
          taxable_ind                     = mer_rec.taxable_ind,
          pos_item_desc                   = mer_rec.pos_item_desc,
          pos_short_desc                  = mer_rec.pos_short_desc,
          num_ti_pallet_tier_cases        = mer_rec.num_ti_pallet_tier_cases,
          num_hi_pallet_tier_cases        = mer_rec.num_hi_pallet_tier_cases,
          store_ord_mult_unit_type_code   = mer_rec.store_ord_mult_unit_type_code,
          loc_item_status_code            = mer_rec.loc_item_status_code,
          loc_item_stat_code_update_date  = mer_rec.loc_item_stat_code_update_date,
          avg_natural_daily_waste_perc    = mer_rec.avg_natural_daily_waste_perc,
          meas_of_each                    = mer_rec.meas_of_each,
          meas_of_price                   = mer_rec.meas_of_price,
          rsp_uom_code                    = mer_rec.rsp_uom_code,
          primary_variant_item_no         = mer_rec.primary_variant_item_no,
          primary_cost_pack_item_no       = mer_rec.primary_cost_pack_item_no,
          primary_supplier_no             = mer_rec.primary_supplier_no,
          primary_country_code            = mer_rec.primary_country_code,
          receive_as_pack_type            = mer_rec.receive_as_pack_type,
          source_method_loc_type          = mer_rec.source_method_loc_type,
          source_location_no              = mer_rec.source_location_no,
          wh_supply_chain_type_ind        = mer_rec.wh_supply_chain_type_ind,
          source_data_status_code         = mer_rec.source_data_status_code,
          last_updated_date               = g_date;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;

    
--************************************************************************************************** 
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_rms_location_item_hsp  hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'REFERENCIAL PROBLEM (ITEM/LOC/SUPPLIER) OR INDICATOR NOT 0 OR 1',
             	ITEM_NO,
              LOCATION_NO,
              SUPPLY_CHAIN_TYPE,
              REG_RSP,
              SELLING_RSP,
              SELLING_UOM_CODE,
              PROM_RSP,
              PROM_SELLING_RSP,
              PROM_SELLING_UOM_CODE,
              CLEARANCE_IND,
              TAXABLE_IND,
              POS_ITEM_DESC,
              POS_SHORT_DESC,
              NUM_TI_PALLET_TIER_CASES,
              NUM_HI_PALLET_TIER_CASES,
              STORE_ORD_MULT_UNIT_TYPE_CODE,
              LOC_ITEM_STATUS_CODE,
              LOC_ITEM_STAT_CODE_UPDATE_DATE,
              AVG_NATURAL_DAILY_WASTE_PERC,
              MEAS_OF_EACH,
              MEAS_OF_PRICE,
              RSP_UOM_CODE,
              PRIMARY_VARIANT_ITEM_NO,
              PRIMARY_COST_PACK_ITEM_NO,
              PRIMARY_SUPPLIER_NO,
              PRIMARY_COUNTRY_CODE,
              RECEIVE_AS_PACK_TYPE,
              SOURCE_METHOD_LOC_TYPE,
              SOURCE_LOCATION_NO,
              WH_SUPPLY_CHAIN_TYPE_IND,
              SOURCE_DATA_STATUS_CODE 
      from   stg_rms_location_item_cpy                                                                                                                                                                                                cpy
      where
         (   cpy.clearance_ind not in (0,1)
         or  cpy.taxable_ind   not in (0,1)
         or  cpy.wh_supply_chain_type_ind not in (0,1)
         or
         not exists
          (select *
           from   dim_item di
           where  cpy.item_no   = di.item_no )  
         or
         not exists
           (select *
           from   dim_location dl
           where  cpy.location_no       = dl.location_no )
         or
         not exists
           (select *
           from   dim_supplier ds
           where  cpy.primary_supplier_no  = ds.supplier_no )
          )  
      and sys_process_code = 'N'                                                                           ;
         

g_recs_hospital := g_recs_hospital + sql%rowcount;
      
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;

    

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';

 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************

    
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
  
    select count(*)
    into   g_recs_read
    from   stg_rms_location_item_cpy                                                                                                                                                                                               
    where  sys_process_code = 'N'                                                                           ;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;
    
    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    flagged_records_hospital;

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
       RAISE;

END WH_FND_ADW_INSMRG;
