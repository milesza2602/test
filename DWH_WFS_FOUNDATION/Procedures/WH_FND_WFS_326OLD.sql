--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_326OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_326OLD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_gainshare fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_gainshare_cpy
--               Output - fnd_wfs_gainshare
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


g_info_date          stg_absa_gainshare_cpy.info_date%type;  
g_account_no         stg_absa_gainshare_cpy.account_no%type;
g_promised_date      stg_absa_gainshare_cpy.promised_date%type;

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_326U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS GAINSHARE EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_gainshare_cpy
where (info_date,
account_no,promised_date)
in
(select info_date,
account_no,promised_date
from stg_absa_gainshare_cpy 
group by info_date,
account_no,promised_date
having count(*) > 1) 
order by info_date,
account_no,promised_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_gainshare is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_gainshare_cpy stg,
              fnd_wfs_gainshare fnd
      where   stg.info_date             = fnd.info_date  and             
              stg.account_no            = fnd.account_no     and  
              stg.promised_date         = fnd.promised_date  and 
              stg.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.info_date,
              stg.account_no,stg.promised_date,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_info_date           := '1 Jan 2000'; 
   g_account_no          := '0';
   g_promised_date       := '1 Jan 2000';
 
for dupp_record in stg_dup
   loop

    if  dupp_record.info_date       = g_info_date and
        dupp_record.promised_date   = g_promised_date and
        dupp_record.account_no      = g_account_no then
        update stg_absa_gainshare_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_info_date           := dupp_record.info_date; 
    g_account_no          := dupp_record.account_no;
    g_promised_date       := dupp_record.promised_date;

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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_gainshare fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	info_date	,
             cpy.	account_no	,
             cpy.	company_code	,
             cpy.	product_code	,
             cpy.	promise_to_pay_amt	,
             cpy.	promised_amt_rcv	,
             cpy.	promised_date	,
             cpy.	sequence_no	,
             cpy.	date_ptp_added	,
             cpy.	coll_who_actioned	,
             cpy.	date_last_payment	,
             cpy.	success_ind	,
             cpy.	triad_field	,
             cpy.	sbu_code	,
             cpy.	ccb_code	,
             cpy.	no_days_dlq	,
             cpy.	no_payments_due	,
             cpy.	del_amt	,
             cpy.	inst_amt	,
             cpy.	current_liability	,
             cpy.	site	,
             cpy.	region_head_office	,
             cpy.	account_type	,
             cpy.	assigned_coll	,
             g_date as last_updated_date
      from   stg_absa_gainshare_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_gainshare 
       where  info_date      = cpy.info_date and
              account_no     = cpy.account_no and
              promised_date  = cpy.promised_date)
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



for upd_rec in c_stg_absa_gainshare
   loop
     update fnd_wfs_gainshare fnd 
     set    fnd.	company_code	      =	upd_rec.	company_code	,	
            fnd.	product_code	      =	upd_rec.	product_code	,	
            fnd.	promise_to_pay_amt	=	upd_rec.	promise_to_pay_amt	,	
            fnd.	promised_amt_rcv	  =	upd_rec.	promised_amt_rcv	,	
            fnd.	promised_date	      =	upd_rec.	promised_date	,	
            fnd.	sequence_no	        =	upd_rec.	sequence_no	,	
            fnd.	date_ptp_added    	=	upd_rec.	date_ptp_added	,	
            fnd.	coll_who_actioned	  =	upd_rec.	coll_who_actioned	,	
            fnd.	date_last_payment	  =	upd_rec.	date_last_payment	,	
            fnd.	success_ind	        =	upd_rec.	success_ind	,	
            fnd.	triad_field	        =	upd_rec.	triad_field	,	
            fnd.	sbu_code	          =	upd_rec.	sbu_code	,	
            fnd.	ccb_code          	=	upd_rec.	ccb_code	,	
            fnd.	no_days_dlq        	=	upd_rec.	no_days_dlq	,	
            fnd.	no_payments_due	    =	upd_rec.	no_payments_due	,	
            fnd.	del_amt            	=	upd_rec.	del_amt	,	
            fnd.	inst_amt	          =	upd_rec.	inst_amt	,	
            fnd.	current_liability	  =	upd_rec.	current_liability	,	
            fnd.	site	              =	upd_rec.	site	,	
            fnd.	region_head_office	=	upd_rec.	region_head_office	,	
            fnd.	account_type	      =	upd_rec.	account_type	,	
            fnd.	assigned_coll      	=	upd_rec.	assigned_coll	,	
            fnd.  last_updated_date   = g_date
     where  fnd.	info_date           =	upd_rec.	info_date and
            fnd.	account_no         	=	upd_rec.	account_no      and
            fnd.	promised_date       =	upd_rec.	promised_date and
            ( 
            nvl(fnd.company_code	      ,0) <>	upd_rec.	company_code	or
            nvl(fnd.product_code	      ,0) <>	upd_rec.	product_code	or
            nvl(fnd.promise_to_pay_amt	,0) <>	upd_rec.	promise_to_pay_amt	or
            nvl(fnd.promised_amt_rcv  	,0) <>	upd_rec.	promised_amt_rcv	or
--            nvl(fnd.promised_date      	,'1 Jan 1900') <>	upd_rec.	promised_date	or
            nvl(fnd.sequence_no	        ,0) <>	upd_rec.	sequence_no	or
            nvl(fnd.date_ptp_added	    ,'1 Jan 1900') <>	upd_rec.	date_ptp_added	or
            nvl(fnd.coll_who_actioned	  ,0) <>	upd_rec.	coll_who_actioned	or
            nvl(fnd.date_last_payment  	,'1 Jan 1900') <>	upd_rec.	date_last_payment	or
            nvl(fnd.success_ind	        ,0) <>	upd_rec.	success_ind	or
            nvl(fnd.triad_field	        ,0) <>	upd_rec.	triad_field	or
            nvl(fnd.sbu_code	          ,0) <>	upd_rec.	sbu_code	or
            nvl(fnd.ccb_code	          ,0) <>	upd_rec.	ccb_code	or
            nvl(fnd.no_days_dlq	        ,0) <>	upd_rec.	no_days_dlq	or
            nvl(fnd.no_payments_due	    ,0) <>	upd_rec.	no_payments_due	or
            nvl(fnd.del_amt	            ,0) <>	upd_rec.	del_amt	or
            nvl(fnd.inst_amt	          ,0) <>	upd_rec.	inst_amt	or
            nvl(fnd.current_liability	  ,0) <>	upd_rec.	current_liability	or
            nvl(fnd.site	              ,0) <>	upd_rec.	site	or
            nvl(fnd.region_head_office	,0) <>	upd_rec.	region_head_office	or
            nvl(fnd.account_type	      ,0) <>	upd_rec.	account_type	or
            nvl(fnd.assigned_coll	      ,0) <>	upd_rec.	assigned_coll	
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_gainshare_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	info_date	,
             cpy.	account_no	,
             cpy.	company_code	,
             cpy.	product_code	,
             cpy.	promise_to_pay_amt	,
             cpy.	promised_amt_rcv	,
             cpy.	promised_date	,
             cpy.	sequence_no	,
             cpy.	date_ptp_added	,
             cpy.	coll_who_actioned	,
             cpy.	date_last_payment	,
             cpy.	success_ind	,
             cpy.	triad_field	,
             cpy.	sbu_code	,
             cpy.	ccb_code	,
             cpy.	no_days_dlq	,
             cpy.	no_payments_due	,
             cpy.	del_amt	,
             cpy.	inst_amt	,
             cpy.	current_liability	,
             cpy.	site	,
             cpy.	region_head_office	,
             cpy.	account_type	,
             cpy.	assigned_coll	

      from   stg_absa_gainshare_cpy cpy
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
    from   stg_absa_gainshare_cpy
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
--    update stg_absa_gainshare_cpy
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
end wh_fnd_wfs_326old;
