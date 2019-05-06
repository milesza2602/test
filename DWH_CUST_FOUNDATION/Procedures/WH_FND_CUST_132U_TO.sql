--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_132U_TO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_132U_TO" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JUN 2016
--  Author:      Alastair de Wet
--               Create EA DEMOGRAPHICS  table in the foundation layer
--               with input ex staging table from LIGHTSTONE.
--  Tables:      Input  - stg_lst_ea_codes_cpy
--               Output - fnd_lst_ea_codes
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
--
--   
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
g_physical_updated   integer       :=  0;

g_ea_code        stg_lst_ea_codes_cpy.ea_code%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_132U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE EA CODES EX LIGHTSTONE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_lst_ea_codes_cpy
where (ea_code)
in
(select ea_code
from stg_lst_ea_codes_cpy 
group by ea_code 
having count(*) > 1) 
order by ea_code,
sys_source_batch_id desc ,sys_source_sequence_no desc;

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_ea_code   := '0'; 
 

for dupp_record in stg_dup
   loop

    if  dupp_record.ea_code    = g_ea_code  then
        update stg_lst_ea_codes_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_ea_code    := dupp_record.ea_code; 
 

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   MERGE  INTO fnd_lst_ea_codes fnd 
   USING (
         select /*+ FULL(cpy)  parallel (4) */  
              cpy.*
      from    dwh_cust_foundation.stg_lst_ea_codes_cpy@link_to_dev cpy
      where   cpy.sys_process_code      = 'N'
      order by   sys_source_batch_id,sys_source_sequence_no      
         ) mer_rec
   ON    (  fnd.	ea_code	          =	mer_rec.	ea_code )
   WHEN MATCHED THEN 
   UPDATE SET
            fnd.	HOUSEHOLDS_QTY	              = mer_rec.	HOUSEHOLDS_QTY	,
            fnd.	ADULTS_QTY	                  = mer_rec.	ADULTS_QTY	,
            fnd.	KIDS_0_17_QTY	                = mer_rec.	KIDS_0_17_QTY	,
            fnd.	ADULTS_18_29_QTY	            = mer_rec.	ADULTS_18_29_QTY	,
            fnd.	ADULTS_30_39_QTY	            = mer_rec.	ADULTS_30_39_QTY	,
            fnd.	ADULTS_40_49_QTY	            = mer_rec.	ADULTS_40_49_QTY	,
            fnd.	ADULTS_50_59_QTY	            = mer_rec.	ADULTS_50_59_QTY	,
            fnd.	ADULTS_60PLUS_QTY	            = mer_rec.	ADULTS_60PLUS_QTY	,
            fnd.	HOUSEHOLDS_BLACK_QTY	        = mer_rec.	HOUSEHOLDS_BLACK_QTY	,
            fnd.	HOUSEHOLDS_WHITE_QTY	        = mer_rec.	HOUSEHOLDS_WHITE_QTY	,
            fnd.	HOUSEHOLDS_INDIAN_QTY	        = mer_rec.	HOUSEHOLDS_INDIAN_QTY	,
            fnd.	HOUSEHOLDS_COLOURED_QTY	      = mer_rec.	HOUSEHOLDS_COLOURED_QTY	,
            fnd.	RACE_CODE	                    = mer_rec.	RACE_CODE	,
            fnd.	INCOME_CLASS	                = mer_rec.	INCOME_CLASS	,
            fnd.	HOUSEHOLD_INCOME_SUBRANGE	    = mer_rec.	HOUSEHOLD_INCOME_SUBRANGE	,
            fnd.	ECONOMICALLY_ACTIVE	          = mer_rec.	ECONOMICALLY_ACTIVE	,
            fnd.	PROVINCE	                    = mer_rec.	PROVINCE	,
            fnd.	LSM_INDICATOR	                = mer_rec.	LSM_INDICATOR	,
            fnd.	PROPORTION_KIDS	              = mer_rec.	PROPORTION_KIDS	,
            fnd.	AVERAGE_ADULT_AGE	            = mer_rec.	AVERAGE_ADULT_AGE	,
            fnd.	PROPERTY_OWNER_AGE_CATEGORY	  = mer_rec.	PROPERTY_OWNER_AGE_CATEGORY	,
            fnd.	FAMILY_CATEGORY	              = mer_rec.	FAMILY_CATEGORY	,
            fnd.	CARS_PER_HOUSEHOLD	          = mer_rec.	CARS_PER_HOUSEHOLD	,
            fnd.	HOUSEHOLD_ADULT_SIZE_CATEGORY	= mer_rec.	HOUSEHOLD_ADULT_SIZE_CATEGORY	,
            fnd.  last_updated_date             = g_date
   WHEN NOT MATCHED THEN
   INSERT
          (         
            EA_CODE,
            HOUSEHOLDS_QTY,
            ADULTS_QTY,
            KIDS_0_17_QTY,
            ADULTS_18_29_QTY,
            ADULTS_30_39_QTY,
            ADULTS_40_49_QTY,
            ADULTS_50_59_QTY,
            ADULTS_60PLUS_QTY,
            HOUSEHOLDS_BLACK_QTY,
            HOUSEHOLDS_WHITE_QTY,
            HOUSEHOLDS_INDIAN_QTY,
            HOUSEHOLDS_COLOURED_QTY,
            RACE_CODE,
            INCOME_CLASS,
            HOUSEHOLD_INCOME_SUBRANGE,
            ECONOMICALLY_ACTIVE,
            PROVINCE,
            LSM_INDICATOR,
            PROPORTION_KIDS,
            AVERAGE_ADULT_AGE,
            PROPERTY_OWNER_AGE_CATEGORY,
            FAMILY_CATEGORY,
            CARS_PER_HOUSEHOLD,
            HOUSEHOLD_ADULT_SIZE_CATEGORY,
            LAST_UPDATED_DATE
          )
  values
          ( mer_rec.	EA_CODE,
            mer_rec.	HOUSEHOLDS_QTY,
            mer_rec.	ADULTS_QTY,
            mer_rec.	KIDS_0_17_QTY,
            mer_rec.	ADULTS_18_29_QTY,
            mer_rec.	ADULTS_30_39_QTY,
            mer_rec.	ADULTS_40_49_QTY,
            mer_rec.	ADULTS_50_59_QTY,
            mer_rec.	ADULTS_60PLUS_QTY,
            mer_rec.	HOUSEHOLDS_BLACK_QTY,
            mer_rec.	HOUSEHOLDS_WHITE_QTY,
            mer_rec.	HOUSEHOLDS_INDIAN_QTY,
            mer_rec.	HOUSEHOLDS_COLOURED_QTY,
            mer_rec.	RACE_CODE,
            mer_rec.	INCOME_CLASS,
            mer_rec.	HOUSEHOLD_INCOME_SUBRANGE,
            mer_rec.	ECONOMICALLY_ACTIVE,
            mer_rec.	PROVINCE,
            mer_rec.	LSM_INDICATOR,
            mer_rec.	PROPORTION_KIDS,
            mer_rec.	AVERAGE_ADULT_AGE,
            mer_rec.	PROPERTY_OWNER_AGE_CATEGORY,
            mer_rec.	FAMILY_CATEGORY,
            mer_rec.	CARS_PER_HOUSEHOLD,
            mer_rec.	HOUSEHOLD_ADULT_SIZE_CATEGORY,
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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin
     
         

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
   
--    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   dwh_cust_foundation.stg_lst_ea_codes_cpy@link_to_dev
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
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
end wh_fnd_cust_132u_to;
