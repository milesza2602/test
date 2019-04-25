--------------------------------------------------------
--  DDL for Procedure WH_FND_FPI_026U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_FPI_026U" 								 
                                    (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        February 2018
--  Author:      Francisca De Vaal
--
--  Tables:      Input  - STG_FPI_SUPPLIER_SITE_CPY                                                               
--               Output - FND_SUPPLIER_SITE	                                                                   
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  dd mon yyyy - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
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
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_FPI_026U';                              -- STORE PROC CHANGE
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STG_FPI_SUPPLIER_SITE_CPY EX FPI';    -- STG TABLE NAME CHANGE
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin

 
   merge  into fnd_supplier_site fnd   		                                                                   
   using (
      
    select /*+ PARALLEL(CPY1,8) */ cpy1.*
    from   stg_fpi_supplier_site_cpy cpy1,
           fnd_supplier sup
    where  cpy1.supplier_no = sup.supplier_no and
          (sys_source_batch_id, sys_source_sequence_no, supplier_code, site_code, cpy1.supplier_no
             ) in
          (select /*+ PARALLEL(8) */ sys_source_batch_id, sys_source_sequence_no,
	               	supplier_code, site_code, supplier_no
           from (select /*+ PARALLEL(tmp,8) */ tmp.*,
                 rank ()
                    over (partition by supplier_code,site_code,supplier_no
			     order by sys_source_batch_id desc, sys_source_sequence_no desc)
                 as rank
                 from stg_fpi_supplier_site_cpy tmp
           )
           where rank = 1)
    order by sys_source_batch_id desc, sys_source_sequence_no
    
         ) mer_rec
   on    (  
            fnd.supplier_code	=	mer_rec.supplier_code   and
            fnd.site_code	    =	mer_rec.site_code       and
            fnd.supplier_no	    =	mer_rec.supplier_no
	 )
   when matched then 
   update set                                                                                                       -- COLUNM NAME CHANGE 
            fnd.supplier_name	          =	mer_rec.supplier_name,
            fnd.site_name	              =	mer_rec.site_name,
            fnd.supplier_api_id	          =	mer_rec.supplier_api_id,
            fnd.supplier_contact_name	  =	mer_rec.supplier_contact_name,
            fnd.supplier_contact_email	  =	mer_rec.supplier_contact_email,
            fnd.supplier_contact_phone	  =	mer_rec.supplier_contact_phone,
            fnd.supplier_contact_cell	  =	mer_rec.supplier_contact_cell,
            fnd.supplier_type	          =	mer_rec.supplier_type,
            fnd.supplier_vat_no	          =	mer_rec.supplier_vat_no,
            fnd.supplier_website	      =	mer_rec.supplier_website,
            fnd.supplier_municipality	  =	mer_rec.supplier_municipality,
            fnd.supplier_top_grade	      =	mer_rec.supplier_top_grade,
            fnd.supplier_reg_company_no	  =	mer_rec.supplier_reg_company_no,
            fnd.supplier_address_line1	  =	mer_rec.supplier_address_line1,
            fnd.supplier_address_line2	  =	mer_rec.supplier_address_line2,
            fnd.supplier_address_line3	  =	mer_rec.supplier_address_line3,
            fnd.supplier_address_line4	  =	mer_rec.supplier_address_line4,
            fnd.site_api_id	              =	mer_rec.site_api_id,
            fnd.site_type	              =	mer_rec.site_type,
            fnd.site_puc_no	              =	mer_rec.site_puc_no,
            fnd.site_contact_phone	      =	mer_rec.site_contact_phone,
            fnd.site_top_grade	          =	mer_rec.site_top_grade,
            fnd.site_status	              =	mer_rec.site_status,
            fnd.site_address_line1	      =	mer_rec.site_address_line1,
            fnd.site_address_line2	      =	mer_rec.site_address_line2,
            fnd.site_address_line3	      =	mer_rec.site_address_line3,
            fnd.site_address_line4	      =	mer_rec.site_address_line4,
            fnd.site_lead_technologist	  =	mer_rec.site_lead_technologist,
            fnd.site_municipality	      =	mer_rec.site_municipality,
            fnd.site_certification_detail =	mer_rec.site_certification_detail,
            fnd.site_contact	          =	mer_rec.site_contact,
            fnd.production_detail	      =	mer_rec.production_detail,
            fnd.last_updated_date	      =	 g_date

   when not matched then
   insert                                                                                                      	    -- COLUNM NAME CHANGE
          (
             supplier_code,
             site_code,
             supplier_no,
             supplier_name,
             site_name,
             supplier_api_id,
             supplier_contact_name,
             supplier_contact_email,
             supplier_contact_phone,
             supplier_contact_cell,
             supplier_type,
             supplier_vat_no,
             supplier_website,
             supplier_municipality,
             supplier_top_grade,
             supplier_reg_company_no,
             supplier_address_line1,
             supplier_address_line2,
             supplier_address_line3,
             supplier_address_line4,
             site_api_id,
             site_type,
             site_puc_no,
             site_contact_phone,
             site_top_grade,
             site_status,
             site_address_line1,
             site_address_line2,
             site_address_line3,
             site_address_line4,
             site_lead_technologist,
             site_municipality,
             site_certification_detail,
             site_contact,
             production_detail,
             last_updated_date
          )
  values                                                                                                           -- COLUNM NAME CHANGE
          (         
             mer_rec.supplier_code,
             mer_rec.site_code,
             mer_rec.supplier_no,
             mer_rec.supplier_name,
             mer_rec.site_name,
             mer_rec.supplier_api_id,
             mer_rec.supplier_contact_name,
             mer_rec.supplier_contact_email,
             mer_rec.supplier_contact_phone,
             mer_rec.supplier_contact_cell,
             mer_rec.supplier_type,
             mer_rec.supplier_vat_no,
             mer_rec.supplier_website,
             mer_rec.supplier_municipality,
             mer_rec.supplier_top_grade,
             mer_rec.supplier_reg_company_no,
             mer_rec.supplier_address_line1,
             mer_rec.supplier_address_line2,
             mer_rec.supplier_address_line3,
             mer_rec.supplier_address_line4,
             mer_rec.site_api_id,
             mer_rec.site_type,
             mer_rec.site_puc_no,
             mer_rec.site_contact_phone,
             mer_rec.site_top_grade,
             mer_rec.site_status,
             mer_rec.site_address_line1,
             mer_rec.site_address_line2,
             mer_rec.site_address_line3,
             mer_rec.site_address_line4,
             mer_rec.site_lead_technologist,
             mer_rec.site_municipality,
             mer_rec.site_certification_detail,
             mer_rec.site_contact,
             mer_rec.production_detail,
             g_date
          )           
          ;  
             
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
--    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
--    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   STG_FPI_SUPPLIER_SITE_CPY                                                                           -- STG TABLE NAME CHANGE 
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;
    

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
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;            --Bulk load--
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
end wh_fnd_fpi_026u                                                                                             -- STORE PROC CHANGE 
;
