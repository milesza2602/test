--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_208U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_208U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_collect fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_cust_collect_cpy
--               Output - fnd_wfs_cust_collect
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


g_wfs_customer_no    stg_vsn_cust_collect_cpy.wfs_customer_no%type; 
g_product_code_no    stg_vsn_cust_collect_cpy.product_code_no%TYPE; 
g_entered_coll_date     stg_vsn_cust_collect_cpy.entered_coll_date%type; 

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_208U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_CUST_COLLECT EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_cust_collect_cpy
where (wfs_customer_no,
product_code_no,
entered_coll_date)
in
(select wfs_customer_no,
product_code_no,
entered_coll_date
from stg_vsn_cust_collect_cpy 
group by wfs_customer_no,
product_code_no,
entered_coll_date
having count(*) > 1) 
order by wfs_customer_no,
product_code_no,
entered_coll_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


          

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_product_code_no    := 0;
   g_entered_coll_date     := '1 Jan 2000';


for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.entered_coll_date    = g_entered_coll_date then
        update stg_vsn_cust_collect_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_entered_coll_date        := dupp_record.entered_coll_date;

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

--************************************************************************************************** 
-- Insert dummy m aster records to ensure RI
--**************************************************************************************************
procedure create_dummy_masters as
begin
 
 --******************************************************************************
    
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_rep fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	wfs_rep_id	,
             'Dummy wh_fnd_wfs_208u',
             0	,
             g_date,
             1
      from   stg_vsn_cust_collect_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_rep
       where  rep_id              = cpy.wfs_rep_id )
       and    sys_process_code    = 'N' 
       and    cpy.wfs_rep_id      is not null;
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       
      commit;       
       
--******************************************************************************       
--******************************************************************************

      insert /*+ APPEND parallel (pcd,2) */ into fnd_wfs_product pcd 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             CPY.	PRODUCT_CODE_NO	,
             'Dummy wh_fnd_wfs_208U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_cust_collect_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_product
       where 	product_code_no     = cpy.product_code_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;

--******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.wfs_customer_no	,
             0,
             1,
             g_date,
             1
      from   stg_vsn_cust_collect_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_customer_product 
       where  product_no              = cpy.wfs_customer_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;
       
--******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.wfs_account_no	,
             0,
             1,
             g_date,
             1
      from   stg_vsn_cust_collect_cpy  cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_customer_product 
       where  product_no              = cpy.wfs_account_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
      commit;
      
      
--******************************************************************************
  exception
      when dwh_errors.e_insert_error then
       l_message := 'DUMMY INS - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'DUMMY INS  - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end create_dummy_masters;


--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_cust_collect fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	entered_coll_date	,
            	cpy.	wfs_account_no	,
            	cpy.	tot_due_enter_coll	,
            	cpy.	total_due	,
            	cpy.	wfs_rep_id	,
            	cpy.	rep_assign_date	,
            	cpy.	temp_rep	,
            	cpy.	tmprep_assgn_date	,
            	cpy.	delinq_reason	,
            	cpy.	purge_date	,
            	cpy.	cta_class	,
            	cpy.	cta_class_date	,
            	cpy.	ctaclas_dys_remain	,
            	cpy.	no_days_delinquent	,
            	cpy.	last_ptp_repid	,
            	cpy.	last_ptp_value	,
            	cpy.	last_ptp_date	,
            	cpy.	ptp_balance	,
            	cpy.	no_of_brkn_ptp	,
            	cpy.	satsfd_ptp_flag	,
            	cpy.	no_of_prod_actions	,
            	cpy.	no_of_nonprd_actns	,
            	cpy.	no_of_letters_sent	,
            	cpy.	next_letter_code	,
            	cpy.	last_letter_date	,
            	cpy.	last_action	,
            	cpy.	last_action_date	,
            	cpy.	last_pymt_date	,
            	cpy.	permanent_msg	,
            	cpy.	perm_msg_date	,
            	cpy.	perm_msg_rep_id	,
            	cpy.	worked_flag	,
            	'Y','',
             g_date as last_updated_date
       from  stg_vsn_cust_collect_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_cust_collect 
       where  wfs_customer_no    = cpy.wfs_customer_no and
              product_code_no    = cpy.product_code_no and
              entered_coll_date  = cpy.entered_coll_date)
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
------------------/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   MERGE  INTO fnd_wfs_cust_collect fnd 
   USING (
         select /*+ FULL(cpy)  parallel (4) */  
              cpy.*
      from    stg_vsn_cust_collect_cpy cpy,
              fnd_wfs_cust_collect fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and    
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.entered_coll_date     = fnd.entered_coll_date    and 
              cpy.sys_process_code      = 'N'  
              
              and
            ( 
            nvl(fnd.wfs_account_no	    ,0) <>	cpy.	wfs_account_no	or
            nvl(fnd.total_due	          ,0) <>	cpy.	total_due	or
            nvl(fnd.wfs_rep_id	        ,0) <>	cpy.	wfs_rep_id	or
            nvl(fnd.rep_assign_date	    ,'1 Jan 1900') <>	cpy.	rep_assign_date	or
            nvl(fnd.temp_rep	          ,0) <>	cpy.	temp_rep	or
            nvl(fnd.tmprep_assgn_date	  ,'1 Jan 1900') <>	cpy.	tmprep_assgn_date	or
            nvl(fnd.delinq_reason	      ,0) <>	cpy.	delinq_reason	or
            nvl(fnd.purge_date	        ,'1 Jan 1900') <>	cpy.	purge_date	or
            nvl(fnd.cta_class          	,0) <>	cpy.	cta_class	or
            nvl(fnd.cta_class_date	    ,'1 Jan 1900') <>	cpy.	cta_class_date	or
            nvl(fnd.ctaclas_dys_remain	,0) <>	cpy.	ctaclas_dys_remain	or
            nvl(fnd.no_days_delinquent	,0) <>	cpy.	no_days_delinquent	or
            nvl(fnd.last_ptp_repid	    ,0) <>	cpy.	last_ptp_repid	or
            nvl(fnd.last_ptp_value	    ,0) <>	cpy.	last_ptp_value	or
            nvl(fnd.last_ptp_date	      ,'1 Jan 1900') <>	cpy.	last_ptp_date	or
            nvl(fnd.ptp_balance	        ,0) <>	cpy.	ptp_balance	or
            nvl(fnd.no_of_brkn_ptp	    ,0) <>	cpy.	no_of_brkn_ptp	or
            nvl(fnd.satsfd_ptp_flag	    ,0) <>	cpy.	satsfd_ptp_flag	or
            nvl(fnd.no_of_prod_actions	,0) <>	cpy.	no_of_prod_actions	or
            nvl(fnd.no_of_nonprd_actns	,0) <>	cpy.	no_of_nonprd_actns	or
            nvl(fnd.no_of_letters_sent	,0) <>	cpy.	no_of_letters_sent	or
            nvl(fnd.next_letter_code	  ,0) <>	cpy.	next_letter_code	or
            nvl(fnd.last_letter_date	  ,'1 Jan 1900') <>	cpy.	last_letter_date	or
            nvl(fnd.last_action       	,0) <>	cpy.	last_action	or
            nvl(fnd.last_action_date	  ,'1 Jan 1900') <>	cpy.	last_action_date	or
            nvl(fnd.last_pymt_date	    ,'1 Jan 1900') <>	cpy.	last_pymt_date	or
            nvl(fnd.permanent_msg      	,0) <>	cpy.	permanent_msg	or
            nvl(fnd.perm_msg_date	      ,'1 Jan 1900') <>	cpy.	perm_msg_date	or
            nvl(fnd.perm_msg_rep_id	    ,0) <>	cpy.	perm_msg_rep_id	or
            nvl(fnd.worked_flag	        ,0) <>	cpy.	worked_flag 
            ) 
         ) mer_rec
   ON    (  fnd.	wfs_customer_no	      =	mer_rec.	wfs_customer_no and
            fnd.	product_code_no	      =	mer_rec.	product_code_no	and
            fnd.	entered_coll_date	    =	mer_rec.	entered_coll_date)
   WHEN MATCHED THEN 
   UPDATE SET
            fnd.	wfs_account_no	    =	mer_rec.	wfs_account_no	,
            fnd.	total_due         	=	mer_rec.	total_due	,
            fnd.	wfs_rep_id	        =	mer_rec.	wfs_rep_id	,
            fnd.	rep_assign_date	    =	mer_rec.	rep_assign_date	,
            fnd.	temp_rep	          =	mer_rec.	temp_rep	,
            fnd.	tmprep_assgn_date 	=	mer_rec.	tmprep_assgn_date	,
            fnd.	delinq_reason       =	mer_rec.	delinq_reason	,
            fnd.	purge_date	        =	mer_rec.	purge_date	,
            fnd.	cta_class	          =	mer_rec.	cta_class	,
            fnd.	cta_class_date    	=	mer_rec.	cta_class_date	,
            fnd.	ctaclas_dys_remain	=	mer_rec.	ctaclas_dys_remain	,
            fnd.	no_days_delinquent	=	mer_rec.	no_days_delinquent	,
            fnd.	last_ptp_repid	    =	mer_rec.	last_ptp_repid	,
            fnd.	last_ptp_value	    =	mer_rec.	last_ptp_value	,
            fnd.	last_ptp_date	      =	mer_rec.	last_ptp_date	,
            fnd.	ptp_balance       	=	mer_rec.	ptp_balance	,
            fnd.	no_of_brkn_ptp	    =	mer_rec.	no_of_brkn_ptp	,
            fnd.	satsfd_ptp_flag   	=	mer_rec.	satsfd_ptp_flag	,
            fnd.	no_of_prod_actions	=	mer_rec.	no_of_prod_actions	,
            fnd.	no_of_nonprd_actns	=	mer_rec.	no_of_nonprd_actns	,
            fnd.	no_of_letters_sent	=	mer_rec.	no_of_letters_sent	,
            fnd.	next_letter_code	  =	mer_rec.	next_letter_code	,
            fnd.	last_letter_date	  =	mer_rec.	last_letter_date	,
            fnd.	last_action	        =	mer_rec.	last_action	,
            fnd.	last_action_date	  =	mer_rec.	last_action_date	,
            fnd.	last_pymt_date	    =	mer_rec.	last_pymt_date	,
            fnd.	permanent_msg	      =	mer_rec.	permanent_msg	,
            fnd.	perm_msg_date	      =	mer_rec.	perm_msg_date	,
            fnd.	perm_msg_rep_id	    =	mer_rec.	perm_msg_rep_id	,
            fnd.	worked_flag       	=	mer_rec.	worked_flag	,
            fnd.	in_collectns_flag	  =	'Y'	,
            fnd.	left_collectns_date	=	''	,
            fnd.  last_updated_date   = g_date;  
             
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_cust_collect_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	entered_coll_date	,
            	cpy.	wfs_account_no	,
            	cpy.	tot_due_enter_coll	,
            	cpy.	total_due	,
            	cpy.	wfs_rep_id	,
            	cpy.	rep_assign_date	,
            	cpy.	temp_rep	,
            	cpy.	tmprep_assgn_date	,
            	cpy.	delinq_reason	,
              cpy.  pending_chgoff_ind,
            	cpy.	purge_date	,
            	cpy.	cta_class	,
            	cpy.	cta_class_date	,
            	cpy.	ctaclas_dys_remain	,
            	cpy.	no_days_delinquent	,
            	cpy.	last_ptp_repid	,
            	cpy.	last_ptp_value	,
            	cpy.	last_ptp_date	,
            	cpy.	ptp_balance	,
            	cpy.	no_of_brkn_ptp	,
            	cpy.	satsfd_ptp_flag	,
            	cpy.	no_of_prod_actions	,
            	cpy.	no_of_nonprd_actns	,
            	cpy.	no_of_letters_sent	,
            	cpy.	next_letter_code	,
            	cpy.	last_letter_date	,
            	cpy.	last_action	,
            	cpy.	last_action_date	,
            	cpy.	last_pymt_date	,
            	cpy.	permanent_msg	,
            	cpy.	perm_msg_date	,
            	cpy.	perm_msg_rep_id	,
            	cpy.	worked_flag	,
            	cpy.	in_collectns_flag	,
            	cpy.	left_collectns_date	
      FROM   stg_vsn_cust_collect_cpy cpy
      where  
      ( 1 =   
        (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no and cust.customer_no = 0 ) or
        1 =   
        (select dummy_ind  from  fnd_customer_product cust
         where  cpy.wfs_account_no       = cust.product_no and cust.customer_no = 0 ) or
        1 = 
        (select dummy_ind from  fnd_wfs_product pcd
         where  cpy.product_code_no       = pcd.product_code_no ) 
      ) 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
      and sys_process_code = 'N';
         

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
    
    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    create_dummy_masters;
    
    select count(*)
    into   g_recs_read
    from   stg_vsn_cust_collect_cpy
    where  sys_process_code = 'N';

    l_text := 'SET COLLECTIONS FLAG = N STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    update fnd_wfs_cust_collect
    set    in_collectns_flag = 'N'
    where  in_collectns_flag = 'Y';
    
    commit;
    

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
    
--    flagged_records_hospital;
    
    l_text := 'SET DATE LEFT COLLECTION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    update fnd_wfs_cust_collect
    set    left_collectns_date = g_date - 1
    where  last_updated_date =  g_date - 1 and
           in_collectns_flag  = 'N';
           
    commit;

--    Taken out for better performance --------------------
--    update stg_vsn_cust_collect_cpy
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
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;  


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
end wh_fnd_wfs_208u;
