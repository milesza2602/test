--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_083U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_083U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2016
--  Author:      Alfonso Joshua
--  Purpose:     Create location_item dimension table in the foundation layer
--               with input ex staging table (ex CAM LOCATION)
--  Tables:      Input  - stg_cam_location_item_cpy
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
g_hospital_text      stg_cam_location_item_hsp.sys_process_msg%type;
mer_mart             stg_cam_location_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_083U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX CAM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_cam_location_item_cpy.location_no%type; 
g_item_no           stg_cam_location_item_cpy.item_no%TYPE; 


   cursor stg_dup is
      select * from stg_cam_location_item_cpy
      where (location_no,item_no)
      in
      (select location_no,item_no
      from stg_cam_location_item_cpy 
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
    


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 
    select count(*)
    into   g_recs_read
    from   stg_cam_location_item_cpy                                                                                                                                                                                           
    where  sys_process_code = 'N';
    
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
            update stg_cam_location_item_cpy stg
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
        execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';
  
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    merge /*+ parallel (fnd_mart,6) */ 
    into fnd_location_item fnd_mart 
    using (
    select /*+ FULL(TMP) parallel (TMP,4) */  tmp.*, di.primary_supplier_no
    from stg_cam_location_item_cpy  tmp 
    join fnd_item di           on tmp.item_no = di.item_no 
    join fnd_location dl       on tmp.location_no = dl.location_no 
       where tmp.sys_process_code = 'N'
     ) mer_mart
  
    on  (mer_mart.item_no        = fnd_mart.item_no
    and  mer_mart.location_no    = fnd_mart.location_no
    )
    
    when matched then
    update
    set       num_units_per_tray          = mer_mart. num_units_per_tray,
              primary_supplier_no         = mer_mart. primary_supplier_no,
              CASE_MASS	                  =	mer_mart.	CASE_MASS	,
              CASE_SELLING_INCL_VAT	      =	mer_mart.	CASE_SELLING_INCL_VAT	,
              REG_RSP_INCL_VAT	          =	mer_mart.	REG_RSP_INCL_VAT	,
              CASE_COST_PRICE	            =	mer_mart.	CASE_COST_PRICE	,
              REG_RSP_EXCL_VAT	          =	mer_mart.	REG_RSP_EXCL_VAT	,
              CASE_SELLING_EXCL_VAT	      =	mer_mart.	CASE_SELLING_EXCL_VAT	,
              COST_PRICE	                =	mer_mart.	COST_PRICE	,
              CASE_COST_PRICE_LOCAL	      =	mer_mart.	CASE_COST_PRICE_LOCAL	,
              CASE_COST_PRICE_OPR	        =	mer_mart.	CASE_COST_PRICE_OPR	,
              CASE_SELLING_EXCL_VAT_LOCAL	=	mer_mart.	CASE_SELLING_EXCL_VAT_LOCAL	,
              CASE_SELLING_EXCL_VAT_OPR	  =	mer_mart.	CASE_SELLING_EXCL_VAT_OPR	,
              CASE_SELLING_INCL_VAT_LOCAL	=	mer_mart.	CASE_SELLING_INCL_VAT_LOCAL	,
              CASE_SELLING_INCL_VAT_OPR	  =	mer_mart.	CASE_SELLING_INCL_VAT_OPR	,
              COST_PRICE_LOCAL	          =	mer_mart.	COST_PRICE_LOCAL	,
              COST_PRICE_OPR	            =	mer_mart.	COST_PRICE_OPR	,
              REG_RSP_EXCL_VAT_LOCAL	    =	mer_mart.	REG_RSP_EXCL_VAT_LOCAL	,
              REG_RSP_EXCL_VAT_OPR	      =	mer_mart.	REG_RSP_EXCL_VAT_OPR	,
              REG_RSP_INCL_VAT_LOCAL	    =	mer_mart.	REG_RSP_INCL_VAT_LOCAL	,
              REG_RSP_INCL_VAT_OPR	      =	mer_mart.	REG_RSP_INCL_VAT_OPR	
              
              
              
/*          
    when not matched then
    insert
   (num_units_per_tray,
    item_no,
    location_no,
    last_updated_date
   )
  values
 (mer_mart.num_units_per_tray,
  mer_mart.item_no,
  mer_mart.location_no,
  g_date
 )  */
  ;
  g_recs_updated  :=  g_recs_updated  + sql%rowcount;
--  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
/*
   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
--   insert /*+ APPEND parallel (hsp,2) */ into stg_cam_location_item_hsp hsp 
--   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
/*                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,LOC',
                            tmp.SOURCE_DATA_STATUS_CODE,
                            tmp.location_no,
                            TMP.ITEM_NO,
                            TMP.POST_DATE,
                            tmp.num_units_per_tray,
                            g_date,
                            TMP.NUM_UNITS_PER_TRAY2
                           
    from  stg_cam_location_item_cpy  TMP 
    where 
         not exists
          (select *
           from   fnd_item di
           where  tmp.item_no        = di.item_no )  
         or
         not exists
           (select *
            from   fnd_location dl
            where  tmp.location_no   = dl.location_no );
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------
*/
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
end wh_fnd_corp_083u;
