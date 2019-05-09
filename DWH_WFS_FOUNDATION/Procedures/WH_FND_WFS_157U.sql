--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_157U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_157U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: OM4  Required Proof Documents
-- Tran_type: ODWH: OM4DAP   AIT: OM4DAPP
--
-- Date:       2017-03-30
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_CR_DETAIL_REQ_DOC in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_OM4_CR_DETAIL_REQ_DOC_CPY
--              Output - FND_WFS_CR_DETAIL_REQ_DOC
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2017-03-30 N Chauhan - created - based on WH_FND_WFS_410U

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_OM4_CR_DETAIL_REQ_DOC_CPY.APP_NUMBER%type;
g_unique_key2_field_val  DWH_WFS_FOUNDATION.STG_OM4_CR_DETAIL_REQ_DOC_CPY.CREDIT_DETAILS_ID%type;
g_unique_key3_field_val  DWH_WFS_FOUNDATION.STG_OM4_CR_DETAIL_REQ_DOC_CPY.PRODUCT_NAME%type;
g_unique_key4_field_val  DWH_WFS_FOUNDATION.STG_OM4_CR_DETAIL_REQ_DOC_CPY.PROOF_TYPE%type;
g_unique_key5_field_val  DWH_WFS_FOUNDATION.STG_OM4_CR_DETAIL_REQ_DOC_CPY.DOC_NAME%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_157U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'OM4  REQUIRED PROOF DOCUMENTS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
   select * from  dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy
   where (    app_number, credit_details_id, product_name, proof_type, doc_name  )
   in
   (select     app_number, credit_details_id, product_name, proof_type, doc_name 
    from dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy
    group by      app_number, credit_details_id, product_name, proof_type, doc_name
    having count(*) > 1) 
   order by
    app_number, credit_details_id, product_name, proof_type, doc_name
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_om4_cr_detail_req_doc fnd
--
     where
              fnd.app_number   = stg.app_number and      -- only ones existing in fnd
              fnd.credit_details_id   = stg.credit_details_id and      -- only ones existing in fnd
              fnd.product_name   = stg.product_name and      -- only ones existing in fnd
              fnd.proof_type   = stg.proof_type and      -- only ones existing in fnd
              fnd.doc_name   = stg.doc_name and      -- only ones existing in fnd
--
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.app_number,
              stg.credit_details_id,
              stg.product_name,
              stg.proof_type,
              stg.doc_name,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;
   g_unique_key2_field_val   := 0;
   g_unique_key3_field_val   := 0;
   g_unique_key4_field_val   := 0;
   g_unique_key5_field_val   := 0;

   for dupp_record in stg_dup
    loop
       if 
               dupp_record.app_number  = g_unique_key1_field_val
           and dupp_record.credit_details_id  = g_unique_key2_field_val
           and dupp_record.product_name  = g_unique_key3_field_val
           and dupp_record.proof_type  = g_unique_key4_field_val
           and dupp_record.doc_name  = g_unique_key5_field_val
       then 
        update dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.app_number;
       g_unique_key2_field_val :=  dupp_record.credit_details_id;
       g_unique_key3_field_val :=  dupp_record.product_name;
       g_unique_key4_field_val :=  dupp_record.proof_type;
       g_unique_key5_field_val :=  dupp_record.doc_name;
    
    end loop;
   
   commit;
 
exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ append parallel (fnd,2) */ into fnd_wfs_om4_cr_detail_req_doc fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. app_number ,
         cpy. credit_details_id ,
         cpy. product_name ,
         cpy. proof_type ,
         cpy. doc_name ,
         cpy. doc_code ,
         cpy. doc_status ,
         cpy. doc_recieved ,
         cpy. doc_in_order ,
         cpy. doc_scanned ,
         cpy. doc_url ,
         cpy. tenant_id_number,
         
         g_date as last_updated_date 
              
      from  dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy cpy
--
         left outer join dwh_wfs_foundation.fnd_wfs_om4_cr_detail_req_doc fnd on (
                 fnd.app_number  = cpy.app_number
             and fnd.credit_details_id  = cpy.credit_details_id
             and fnd.product_name  = cpy.product_name
             and fnd.proof_type  = cpy.proof_type
             and fnd.doc_name  = cpy.doc_name
             )
      where fnd.app_number is null
          
-- Any further validation goes in here - like xxx.ind in (0,1) ---  

       and sys_process_code = 'N'; 

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
-- Updates existing records in the staging table into foundation if there are changes
--**************************************************************************************************

procedure flagged_records_update as
begin


for upd_rec in c_stg
   loop
     update fnd_wfs_om4_cr_detail_req_doc fnd 
     set    
         fnd. app_number = upd_rec. app_number ,
         fnd. credit_details_id = upd_rec. credit_details_id ,
         fnd. product_name = upd_rec. product_name ,
         fnd. proof_type = upd_rec. proof_type ,
         fnd. doc_name = upd_rec. doc_name ,
         fnd. doc_code = upd_rec. doc_code ,
         fnd. doc_status = upd_rec. doc_status ,
         fnd. doc_recieved = upd_rec. doc_recieved ,
         fnd. doc_in_order = upd_rec. doc_in_order ,
         fnd. doc_scanned = upd_rec. doc_scanned ,
         fnd. doc_url = upd_rec. doc_url ,
         fnd. tenant_id_number = upd_rec. tenant_id_number
         , 
         fnd.last_updated_date          = g_date 
            
            
     where 
              fnd.app_number  = upd_rec.app_number and
              fnd.credit_details_id  = upd_rec.credit_details_id and
              fnd.product_name  = upd_rec.product_name and
              fnd.proof_type  = upd_rec.proof_type and
              fnd.doc_name  = upd_rec.doc_name and

        ( 
         nvl(fnd. app_number, 0) <> upd_rec. app_number OR
         nvl(fnd. credit_details_id, 0) <> upd_rec. credit_details_id OR
         nvl(fnd. product_name, 0) <> upd_rec. product_name OR
         nvl(fnd. proof_type, 0) <> upd_rec. proof_type OR
         nvl(fnd. doc_name, 0) <> upd_rec. doc_name OR
         nvl(fnd. doc_code, 0) <> upd_rec. doc_code OR
         nvl(fnd. doc_status, 0) <> upd_rec. doc_status OR
         nvl(fnd. doc_recieved, 0) <> upd_rec. doc_recieved OR
         nvl(fnd. doc_in_order, 0) <> upd_rec. doc_in_order OR
         nvl(fnd. doc_scanned, 0) <> upd_rec. doc_scanned OR
         nvl(fnd. doc_url, 0) <> upd_rec. doc_url OR
         nvl(fnd. tenant_id_number, 0) <> upd_rec. tenant_id_number
         );         


   if sql%rowcount = 0 then
        g_recs_nochange:= g_recs_nochange + 1;
   else
        g_recs_updated := g_recs_updated + 1;  
   end if;

   end loop;

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
-- ***** Not applicable, as there is no dependency on some other table *****
procedure flagged_records_hospital as
begin
     
      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_om4_cr_detail_req_doc_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with 0' ,

         cpy. app_number ,
         cpy. credit_details_id ,
         cpy. product_name ,
         cpy. proof_type ,
         cpy. doc_name ,
         cpy. doc_code ,
         cpy. doc_status ,
         cpy. doc_recieved ,
         cpy. doc_in_order ,
         cpy. doc_scanned ,
         cpy. doc_url ,
         cpy. tenant_id_number
              
      from   dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy cpy
--  no dependency table applicable
      where 
--
      
      
--      �       and 

-- Any further validation goes in here - like or xxx.ind not in (0,1) ---    
    
       sys_process_code = 'N';
         

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
    l_text := 'LOAD TABLE: '||'FND_WFS_CR_DETAIL_REQ_DOC' ;
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
    from   dwh_wfs_foundation.stg_om4_cr_detail_req_doc_cpy
    where  sys_process_code = 'N';

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
    
--   no referential integrity checks required --     flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_...._cpy
--    set    sys_process_code = 'Y';

 
   


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
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;  


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

end WH_FND_WFS_157U;