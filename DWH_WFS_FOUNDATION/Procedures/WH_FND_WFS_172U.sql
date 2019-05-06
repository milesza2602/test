--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_172U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_172U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: OM4 Pre Agreement
-- Tran_type: ODWH: OM4PFR   AIT: 0
--
-- Date:       2016-07-18
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_OM4_PRE_AGREEMENT in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_OM4_PRE_AGREEMENT_CPY
--              Output - FND_WFS_OM4_PRE_AGREEMENT
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2016-07-18 N Chauhan - created - based on WH_FND_WFS_410U

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


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_OM4_PRE_AGREEMENT_CPY.IDENTITY_NO%type;
g_unique_key2_field_val  DWH_WFS_FOUNDATION.STG_OM4_PRE_AGREEMENT_CPY.APP_NUMBER%type;
g_unique_key3_field_val  DWH_WFS_FOUNDATION.STG_OM4_PRE_AGREEMENT_CPY.PRODUCT_NAME%type;
--
--

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_172U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'OM4 PRE AGREEMENT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
   select * from  dwh_wfs_foundation.stg_om4_pre_agreement_cpy
   where (    identity_no, app_number, product_name  )
   in
   (select     identity_no, app_number, product_name 
    from dwh_wfs_foundation.stg_om4_pre_agreement_cpy
    group by      identity_no, app_number, product_name
    having count(*) > 1) 
   order by
    identity_no, app_number, product_name
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_om4_pre_agreement_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_om4_pre_agreement fnd
     where
              fnd.identity_no   = stg.identity_no and      -- only ones existing in fnd
              fnd.app_number   = stg.app_number and      -- only ones existing in fnd
              fnd.product_name   = stg.product_name and      -- only ones existing in fnd
--
--
--
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.identity_no,
              stg.app_number,
              stg.product_name,
--
--
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;
   g_unique_key2_field_val   := 0;
   g_unique_key3_field_val   := 0;
   --
   --

   for dupp_record in stg_dup
    loop
       if 
               dupp_record.identity_no  = g_unique_key1_field_val
           and dupp_record.app_number  = g_unique_key2_field_val
           and dupp_record.product_name  = g_unique_key3_field_val
          --
          --
       then 
        update dwh_wfs_foundation.stg_om4_pre_agreement_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.identity_no;
       g_unique_key2_field_val :=  dupp_record.app_number;
       g_unique_key3_field_val :=  dupp_record.product_name;
       --
       --
    
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
      
      insert /*+ append parallel (fnd,2) */ into fnd_wfs_om4_pre_agreement fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. identity_no ,
         cpy. app_number ,
         cpy. product_name ,
         cpy. cust_initial ,
         cpy. cust_title ,
         cpy. cust_surname ,
         cpy. cust_channel ,
         cpy. send_date ,
         cpy. email_address ,
         cpy. cell_no ,
         cpy. postal_address_line1 ,
         cpy. postal_address_line2 ,
         cpy. postal_address_line3 ,
         cpy. postal_city ,
         cpy. postal_code ,
         cpy. cla_letter_ind

         ,
         g_date as last_updated_date 
              
      from  dwh_wfs_foundation.stg_om4_pre_agreement_cpy cpy
--
         left outer join dwh_wfs_foundation.fnd_wfs_om4_pre_agreement fnd on (
                 fnd.identity_no  = cpy.identity_no
             and fnd.app_number  = cpy.app_number
             and fnd.product_name  = cpy.product_name
--
--
             )
      where fnd.identity_no is null
          
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
     update fnd_wfs_om4_pre_agreement fnd 
     set    
         fnd. identity_no = upd_rec. identity_no ,
         fnd. app_number = upd_rec. app_number ,
         fnd. product_name = upd_rec. product_name ,
         fnd. cust_initial = upd_rec. cust_initial ,
         fnd. cust_title = upd_rec. cust_title ,
         fnd. cust_surname = upd_rec. cust_surname ,
         fnd. cust_channel = upd_rec. cust_channel ,
         fnd. send_date = upd_rec. send_date ,
         fnd. email_address = upd_rec. email_address ,
         fnd. cell_no = upd_rec. cell_no ,
         fnd. postal_address_line1 = upd_rec. postal_address_line1 ,
         fnd. postal_address_line2 = upd_rec. postal_address_line2 ,
         fnd. postal_address_line3 = upd_rec. postal_address_line3 ,
         fnd. postal_city = upd_rec. postal_city ,
         fnd. postal_code = upd_rec. postal_code ,
         fnd. cla_letter_ind = upd_rec. cla_letter_ind

         , 
         fnd.last_updated_date          = g_date 
            
            
     where 
              fnd.identity_no  = upd_rec.identity_no and
              fnd.app_number  = upd_rec.app_number and
              fnd.product_name  = upd_rec.product_name and
--
--

        ( 
         nvl(fnd. identity_no, 0) <> upd_rec. identity_no OR
         nvl(fnd. app_number, 0) <> upd_rec. app_number OR
         nvl(fnd. product_name, 0) <> upd_rec. product_name OR
         nvl(fnd. cust_initial, 0) <> upd_rec. cust_initial OR
         nvl(fnd. cust_title, 0) <> upd_rec. cust_title OR
         nvl(fnd. cust_surname, 0) <> upd_rec. cust_surname OR
         nvl(fnd. cust_channel, 0) <> upd_rec. cust_channel OR
         nvl(fnd. send_date, '01 JAN 1900') <> upd_rec. send_date OR
         nvl(fnd. email_address, 0) <> upd_rec. email_address OR
         nvl(fnd. cell_no, 0) <> upd_rec. cell_no OR
         nvl(fnd. postal_address_line1, 0) <> upd_rec. postal_address_line1 OR
         nvl(fnd. postal_address_line2, 0) <> upd_rec. postal_address_line2 OR
         nvl(fnd. postal_address_line3, 0) <> upd_rec. postal_address_line3 OR
         nvl(fnd. postal_city, 0) <> upd_rec. postal_city OR
         nvl(fnd. postal_code, 0) <> upd_rec. postal_code OR
         nvl(fnd. cla_letter_ind, 0) <> upd_rec. cla_letter_ind

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
     
      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_om4_pre_agreement_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with ' ,

         cpy. identity_no ,
         cpy. app_number ,
         cpy. product_name ,
         cpy. cust_initial ,
         cpy. cust_title ,
         cpy. cust_surname ,
         cpy. cust_channel ,
         cpy. send_date ,
         cpy. email_address ,
         cpy. cell_no ,
         cpy. postal_address_line1 ,
         cpy. postal_address_line2 ,
         cpy. postal_address_line3 ,
         cpy. postal_city ,
         cpy. postal_code ,
         cpy. cla_letter_ind

              
      from   dwh_wfs_foundation.stg_om4_pre_agreement_cpy cpy
--
      where 
--
      
      
--      …       and 

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

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************

    
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
    
    select count(*)
    into   g_recs_read
    from   dwh_wfs_foundation.stg_om4_pre_agreement_cpy
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

end WH_FND_WFS_172U;
