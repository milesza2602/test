--------------------------------------------------------
--  DDL for Procedure WH_FND_FPI_018U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_FPI_018U" 
                                    (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        February 2018
--  Author:      Francisca De Vaal
--  Tables:      Input  - STG_FPI_COMP_DIETARY_CPY                                                            
--               Output - FND_ITEM_SUP_COMP_ALLER_DIET                                                             
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_FPI_018U';				
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STG_FPI_COMP_DIETARY_CPY EX FPI';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
 
   merge  into fnd_item_sup_comp_aller_diet fnd  		                                                         
   using (
   
    select /*+ PARALLEL(CPY1,8) */ cpy1.*
    from   stg_fpi_comp_dietary_cpy cpy1,
           fnd_supplier sup,
           fnd_item itm,
           fnd_supplier_site ss
    where  cpy1.supplier_no   = sup.supplier_no and
           cpy1.item_no       = itm.item_no and
           cpy1.supplier_code = ss.supplier_code and
           cpy1.site_code     = ss.site_code and
           cpy1.supplier_no   = ss.supplier_no and
          (sys_source_batch_id, sys_source_sequence_no,
	         cpy1.item_no, cpy1.supplier_code, cpy1.site_code, cpy1.supplier_no, component_name, spec_version,
	        code
             ) in
          (select /*+ PARALLEL(8) */ sys_source_batch_id, sys_source_sequence_no,
	               	item_no, supplier_code, site_code, supplier_no, component_name, spec_version, code
           from (select /*+ PARALLEL(tmp,8) */ tmp.*,
                 rank ()
                    over (partition by item_no,supplier_code,site_code,supplier_no,component_name,spec_version, code
			     order by sys_source_batch_id desc, sys_source_sequence_no desc)
                 as rank
                 from stg_fpi_comp_dietary_cpy tmp                                                             -- STG TABLE NAME CHANGE 
           )
           where rank = 1)
    order by sys_source_batch_id desc, sys_source_sequence_no

           ) mer_rec
      on   ( fnd.item_no	    =	mer_rec.item_no		    and
             fnd.supplier_code  =	mer_rec.supplier_code   and
             fnd.site_code	    =	mer_rec.site_code 	    and
             fnd.supplier_no    =	mer_rec.supplier_no 	and
             fnd.component_name =	mer_rec.component_name  and
             fnd.spec_version   =   mer_rec.spec_version    and
             fnd.code	        =	mer_rec.code
        )
    when matched then 
    update set                                                                                                  -- COLUNM NAME CHANGE
             fnd.suitable_for	    =	mer_rec.suitable_for,
             fnd.last_updated_date	=   g_date
    when not matched then
    insert                                                                                                      -- COLUNM NAME CHANGE
          (
             item_no,
             supplier_code,
             site_code,
             supplier_no,
             component_name,
             spec_version,
             code,
             suitable_for,
             last_updated_date
          )
  values                                                                                                       -- COLUNM NAME CHANGE
          (         
             mer_rec.item_no,	
             mer_rec.supplier_code,	
             mer_rec.site_code,	
             mer_rec.supplier_no,	
             mer_rec.component_name,
             mer_rec.spec_version,
             mer_rec.code,	
             mer_rec.suitable_for,
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
    from   STG_FPI_COMP_DIETARY_CPY                                                            	    -- STG TABLE NAME CHANGE
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
end wh_fnd_fpi_018u													-- STORE PROC CHANGE
;
