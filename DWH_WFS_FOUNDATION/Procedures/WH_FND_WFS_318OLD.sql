--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_318OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_318OLD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_txn_mly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_txn_mly_cpy
--               Output - fnd_wfs_crd_txn_mly
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
g_truncate_count     integer       :=  0;


g_information_date                 stg_absa_crd_txn_mly_cpy.information_date%type;  
G_ACCOUNT_NUMBER                   STG_ABSA_CRD_TXN_MLY_CPY.ACCOUNT_NUMBER%TYPE; 
g_txn_date                         stg_absa_crd_txn_mly_cpy.txn_date%type;  

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_318U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD TRANACTION MLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_txn_mly_cpy
where (information_date,
account_number,
txn_date)
in
(select information_date,
account_number,
txn_date 
from stg_absa_crd_txn_mly_cpy 
group by information_date,
account_number,
txn_date
having count(*) > 1) 
order by information_date,
account_number,
txn_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_txn_mly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_crd_txn_mly_cpy stg,
              fnd_wfs_crd_txn_mly fnd
      where   stg.information_date         = fnd.information_date  and             
              stg.account_number           = fnd.account_number    AND   
              stg.txn_date                 = fnd.txn_date       and 
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.information_date,
              stg.account_number,
              stg.txn_date,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_information_date        := '1 Jan 2000'; 
   g_account_number          := '0';
   g_txn_date                := '1 Jan 2000';
 
for dupp_record in stg_dup
   loop

    if  dupp_record.information_date        = g_information_date and
        dupp_record.account_number          = g_account_number and
        dupp_record.txn_date                = g_txn_date  then
        update stg_absa_crd_txn_mly_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_information_date         := dupp_record.information_date; 
    g_account_number           := dupp_record.account_number;
    g_txn_date                 := dupp_record.txn_date; 

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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_crd_txn_mly fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	information_date	,
             cpy.	account_number	,
             cpy.	txn_date	,
             cpy.	card_number_used	,
             cpy.	card_number_posted	,
             cpy.	primary_or_secondary_card	,
             cpy.	original_posted_date	,
             cpy.	processing_date	,
             cpy.	card_main_txn_group	,
             cpy.	card_sub_txn_group	,
             cpy.	debit_credit_flag	,
             cpy.	card_txn_type_code	,
             cpy.	card_txn_category_code	,
             cpy.	card_txn_status_code	,
             cpy.	card_amt	,
             cpy.	on_us_ind	,
             cpy.	card_txn_type_desc	,
             cpy.	card_product_type_desc	,
             cpy.	authorisation_completion_code	,
             cpy.	authorisation_response_code	,
             cpy.	card_txn_source_code	,
             cpy.	print_on_statement_ind	,
             cpy.	merchant_number	,
             cpy.	merchant_name	,
             cpy.	merchant_fin_institution_code	,
             cpy.	merchant_sic_code	,
             cpy.	merchant_city	,
             cpy.	country_code	,
             cpy.	financial_institution_desc	,
             cpy.	financial_institution_code	,
             cpy.	txn_3rd_level_code	,
             cpy.	txn_4th_level_code	,
             cpy.	txn_5th_level_code	,
             cpy.	chip_app_transaction_code	,
             cpy.	chip_condition_code	,
             cpy.	chip_time_date	,
             cpy.	point_of_sale_entry_mode	,
             g_date as last_updated_date
      from   stg_absa_crd_txn_mly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_txn_mly 
       where  information_date         = cpy.information_date and
              account_number           = cpy.account_number and
              txn_date                 = cpy.txn_date )
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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



for upd_rec in c_stg_absa_crd_txn_mly
   loop
     update fnd_wfs_crd_txn_mly fnd 
     set    fnd.	card_number_used	            =	upd_rec.	card_number_used	,
            fnd.	card_number_posted	          =	upd_rec.	card_number_posted	,
            fnd.	primary_or_secondary_card	    =	upd_rec.	primary_or_secondary_card	,
            fnd.	original_posted_date	        =	upd_rec.	original_posted_date	,
            fnd.	processing_date	              =	upd_rec.	processing_date	,
            fnd.	card_main_txn_group	          =	upd_rec.	card_main_txn_group	,
            fnd.	card_sub_txn_group	          =	upd_rec.	card_sub_txn_group	,
            fnd.	debit_credit_flag	            =	upd_rec.	debit_credit_flag	,
            fnd.	card_txn_type_code	          =	upd_rec.	card_txn_type_code	,
            fnd.	card_txn_category_code       	=	upd_rec.	card_txn_category_code	,
            fnd.	card_txn_status_code        	=	upd_rec.	card_txn_status_code	,
            fnd.	card_amt                    	=	upd_rec.	card_amt	,
            fnd.	on_us_ind	                    =	upd_rec.	on_us_ind	,
            fnd.	card_txn_type_desc	          =	upd_rec.	card_txn_type_desc	,
            fnd.	card_product_type_desc	      =	upd_rec.	card_product_type_desc	,
            fnd.	authorisation_completion_code	=	upd_rec.	authorisation_completion_code	,
            fnd.	authorisation_response_code	  =	upd_rec.	authorisation_response_code	,
            fnd.	card_txn_source_code	        =	upd_rec.	card_txn_source_code	,
            fnd.	print_on_statement_ind      	=	upd_rec.	print_on_statement_ind	,
            fnd.	merchant_number	              =	upd_rec.	merchant_number	,
            fnd.	merchant_name	                =	upd_rec.	merchant_name	,
            fnd.	merchant_fin_institution_code	=	upd_rec.	merchant_fin_institution_code	,
            fnd.	merchant_sic_code	            =	upd_rec.	merchant_sic_code	,
            fnd.	merchant_city	                =	upd_rec.	merchant_city	,
            fnd.	country_code	                =	upd_rec.	country_code	,
            fnd.	financial_institution_desc	  =	upd_rec.	financial_institution_desc	,
            fnd.	financial_institution_code	  =	upd_rec.	financial_institution_code	,
            fnd.	txn_3rd_level_code           	=	upd_rec.	txn_3rd_level_code	,
            fnd.	txn_4th_level_code	          =	upd_rec.	txn_4th_level_code	,
            fnd.	txn_5th_level_code           	=	upd_rec.	txn_5th_level_code	,
            fnd.	chip_app_transaction_code	    =	upd_rec.	chip_app_transaction_code	,
            fnd.	chip_condition_code         	=	upd_rec.	chip_condition_code	,
            fnd.	chip_time_date	              =	upd_rec.	chip_time_date	,
            fnd.	point_of_sale_entry_mode    	=	upd_rec.	point_of_sale_entry_mode	,
            fnd.  last_updated_date             = g_date
     where  fnd.	information_date              =	upd_rec.	information_date and
            fnd.	account_number               	=	upd_rec.	account_number   and
            fnd.	txn_date        	            =	upd_rec.	txn_date	     and
            ( 
            nvl(fnd.card_number_used	            ,0) <>	upd_rec.	card_number_used	or
            nvl(fnd.card_number_posted	          ,0) <>	upd_rec.	card_number_posted	or
            nvl(fnd.primary_or_secondary_card	    ,0) <>	upd_rec.	primary_or_secondary_card	or
            nvl(fnd.original_posted_date	        ,'1 Jan 1900') <>	upd_rec.	original_posted_date	or
            nvl(fnd.processing_date	              ,'1 Jan 1900') <>	upd_rec.	processing_date	or
            nvl(fnd.card_main_txn_group	          ,0) <>	upd_rec.	card_main_txn_group	or
            nvl(fnd.card_sub_txn_group	          ,0) <>	upd_rec.	card_sub_txn_group	or
            nvl(fnd.debit_credit_flag	            ,0) <>	upd_rec.	debit_credit_flag	or
            nvl(fnd.card_txn_type_code	          ,0) <>	upd_rec.	card_txn_type_code	or
            nvl(fnd.card_txn_category_code	      ,0) <>	upd_rec.	card_txn_category_code	or
            nvl(fnd.card_txn_status_code	        ,0) <>	upd_rec.	card_txn_status_code	or
            nvl(fnd.card_amt	                    ,0) <>	upd_rec.	card_amt	or
            nvl(fnd.on_us_ind                   	,0) <>	upd_rec.	on_us_ind	or
            nvl(fnd.card_txn_type_desc	          ,0) <>	upd_rec.	card_txn_type_desc	or
            nvl(fnd.card_product_type_desc	      ,0) <>	upd_rec.	card_product_type_desc	or
            nvl(fnd.authorisation_completion_code	,0) <>	upd_rec.	authorisation_completion_code	or
            nvl(fnd.authorisation_response_code 	,0) <>	upd_rec.	authorisation_response_code	or
            nvl(fnd.card_txn_source_code	        ,0) <>	upd_rec.	card_txn_source_code	or
            nvl(fnd.print_on_statement_ind	      ,0) <>	upd_rec.	print_on_statement_ind	or
            nvl(fnd.merchant_number	              ,0) <>	upd_rec.	merchant_number	or
            nvl(fnd.merchant_name	                ,0) <>	upd_rec.	merchant_name	or
            nvl(fnd.merchant_fin_institution_code	,0) <>	upd_rec.	merchant_fin_institution_code	or
            nvl(fnd.merchant_sic_code           	,0) <>	upd_rec.	merchant_sic_code	or
            nvl(fnd.merchant_city	                ,0) <>	upd_rec.	merchant_city	or
            nvl(fnd.country_code	                ,0) <>	upd_rec.	country_code	or
            nvl(fnd.financial_institution_desc	  ,0) <>	upd_rec.	financial_institution_desc	or
            nvl(fnd.financial_institution_code	  ,0) <>	upd_rec.	financial_institution_code	or
            nvl(fnd.txn_3rd_level_code	          ,0) <>	upd_rec.	txn_3rd_level_code	or
            nvl(fnd.txn_4th_level_code	          ,0) <>	upd_rec.	txn_4th_level_code	or
            nvl(fnd.txn_5th_level_code	          ,0) <>	upd_rec.	txn_5th_level_code	or
            nvl(fnd.chip_app_transaction_code	    ,0) <>	upd_rec.	chip_app_transaction_code	or
            nvl(fnd.chip_condition_code         	,0) <>	upd_rec.	chip_condition_code	or
            nvl(fnd.chip_time_date	              ,0) <>	upd_rec.	chip_time_date	or
            nvl(fnd.point_of_sale_entry_mode	    ,0) <>	upd_rec.	point_of_sale_entry_mode	 
            );
             
      g_recs_updated := g_recs_updated + 1;        
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
procedure flagged_records_hospital as
begin
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_txn_mly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	information_date	,
             cpy.	account_number	,
             cpy.	txn_date	,
             cpy.	card_number_used	,
             cpy.	card_number_posted	,
             cpy.	primary_or_secondary_card	,
             cpy.	original_posted_date	,
             cpy.	processing_date	,
             cpy.	card_main_txn_group	,
             cpy.	card_sub_txn_group	,
             cpy.	debit_credit_flag	,
             cpy.	card_txn_type_code	,
             cpy.	card_txn_category_code	,
             cpy.	card_txn_status_code	,
             cpy.	card_amt	,
             cpy.	on_us_ind	,
             cpy.	card_txn_type_desc	,
             cpy.	card_product_type_desc	,
             cpy.	authorisation_completion_code	,
             cpy.	authorisation_response_code	,
             cpy.	card_txn_source_code	,
             cpy.	print_on_statement_ind	,
             cpy.	merchant_number	,
             cpy.	merchant_name	,
             cpy.	merchant_fin_institution_code	,
             cpy.	merchant_sic_code	,
             cpy.	merchant_city	,
             cpy.	country_code	,
             cpy.	financial_institution_desc	,
             cpy.	financial_institution_code	,
             cpy.	txn_3rd_level_code	,
             cpy.	txn_4th_level_code	,
             cpy.	txn_5th_level_code	,
             cpy.	chip_app_transaction_code	,
             cpy.	chip_condition_code	,
             cpy.	chip_time_date	,
             cpy.	point_of_sale_entry_mode	
      from   stg_absa_crd_txn_mly_cpy cpy
      where  
--      (    
--      NOT EXISTS 
--        (SELECT * FROM  dim_table dim
--         where  cpy.xxx       = dim.xxx ) or
--      not exists 
--        (select * from  dim_table dim1
--         where  cpy.xxx    = dim1.xxx ) 
--      ) and 
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
    from   stg_absa_crd_txn_mly_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


--********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_crd_txn_mly_cpy
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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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
       raise;
end wh_fnd_wfs_318old;
