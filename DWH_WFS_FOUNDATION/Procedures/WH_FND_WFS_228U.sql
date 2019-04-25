--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_228U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_228U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_rep_action_day fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_rep_action_day_cpy
--               Output - fnd_wfs_rep_action_day
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

g_rep_id             stg_vsn_rep_action_day_cpy.rep_id%type; 
g_wfs_customer_no    stg_vsn_rep_action_day_cpy.wfs_customer_no%type;  
g_product_code_no    stg_vsn_rep_action_day_cpy.product_code_no%type; 
g_action_date        stg_vsn_rep_action_day_cpy.action_date%type; 
g_action_time        stg_vsn_rep_action_day_cpy.action_time%type;  
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_228U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_REP_ACTION_DAY EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_rep_action_day_cpy
where (
rep_id,wfs_customer_no,
product_code_no,
action_date,action_time)
in
(select rep_id,wfs_customer_no,
product_code_no,
action_date,action_time
from stg_vsn_rep_action_day_cpy 
group by rep_id,wfs_customer_no,
product_code_no,
action_date,action_time
having count(*) > 1) 
order by rep_id,wfs_customer_no,
product_code_no,
action_date,action_time,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_rep_action_day is
select /*+ FULL(stg)  parallel (stg,2) */  
              cpy.	rep_id	,
              cpy.	wfs_customer_no	,
              cpy.	product_code_no	,
              cpy.	action_date	,
              cpy.	action_time	,
              cpy.	wfs_account_no	,
              cpy.	action_code	,
              cpy.	action_remarks1	,
              cpy.	action_remarks2	,
              cpy.	action_remarks3	,
              cpy.	action_value	,
              cpy.	ptp_action_date	,
              cpy.	ptp_review_date 

      from    stg_vsn_rep_action_day_cpy cpy,
              fnd_wfs_rep_action_day fnd
      where   cpy.rep_id                = fnd.rep_id    and
              cpy.wfs_customer_no       = fnd.wfs_customer_no and             
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.action_date           = fnd.action_date    and 
              cpy.action_time           = fnd.action_time    and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.rep_id,
              cpy.wfs_customer_no,
              cpy.product_code_no,
              cpy.action_date,
              cpy.action_time,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_product_code_no    := 0;
   g_rep_id             := ' '; 
   g_action_date        := '1 Jan 2000';
   g_action_time        := ' ';    

for dupp_record in stg_dup
   loop

    if  dupp_record.rep_id            = g_rep_id and
        dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.action_date       = g_action_date     and
        dupp_record.action_time       = g_action_time     then
        update stg_vsn_rep_action_day_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_rep_id             := dupp_record.rep_id; 
    g_action_date        := dupp_record.action_date;
    g_action_time        := dupp_record.action_time ;      
 

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
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_action fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	action_code	,
             'Dummy wh_fnd_wfs_228u',
             'Dum',
             'Dummy wh_fnd_wfs_228u',
             ' ',' ',' ',0,0,
             g_date,
             1
      from   stg_vsn_rep_action_day_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_action
       where  action_code         = cpy.action_code )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       
      commit;       
       
--******************************************************************************
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_rep fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	rep_id	,
             'Dummy wh_fnd_wfs_228u',
             0	,
             g_date,
             1
      from   stg_vsn_rep_action_day_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_rep
       where  rep_id              = cpy.rep_id )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       
      commit;       
       
--******************************************************************************

      insert /*+ APPEND parallel (pcd,2) */ into fnd_wfs_product pcd 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	product_code_no	,
             'Dummy wh_fnd_wfs_228u',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_rep_action_day_cpy cpy
 
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
      from   stg_vsn_rep_action_day_cpy cpy
 
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
      from   stg_vsn_rep_action_day_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_customer_product 
       where  product_no              = cpy.wfs_account_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;

--******************************************************************************

      commit;

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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_rep_action_day fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	rep_id	,
             cpy.	wfs_customer_no	,
             cpy.	product_code_no	,
             cpy.	action_date	,
             cpy.	action_time	,
             cpy.	wfs_account_no	,
             cpy.	action_code	,
             cpy.	action_remarks1	,
             cpy.	action_remarks2	,
             cpy.	action_remarks3	,
             cpy.	action_value	,
             cpy.	ptp_action_date	,
             cpy.	ptp_review_date	,
             g_date as last_updated_date
      from   stg_vsn_rep_action_day_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_rep_action_day 
       where  wfs_customer_no     = cpy.wfs_customer_no and
              product_code_no     = cpy.product_code_no and
              rep_id              = cpy.rep_id and
              action_date         = cpy.action_date and
              action_time         = cpy.action_time )
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



for upd_rec in c_stg_vsn_rep_action_day
   loop
     update fnd_wfs_rep_action_day fnd 
     set    
            fnd.	wfs_account_no  	=	upd_rec.	wfs_account_no,
            fnd.	action_code	      =	upd_rec.	action_code,
            fnd.	action_remarks1 	=	upd_rec.	action_remarks1,
            fnd.	action_remarks2  	=	upd_rec.	action_remarks2,
            fnd.	action_remarks3	  =	upd_rec.	action_remarks3,
            fnd.	action_value	    =	upd_rec.	action_value,
            fnd.	ptp_action_date	  =	upd_rec.	ptp_action_date,
            fnd.	ptp_review_date 	=	upd_rec.	ptp_review_date,
            fnd.  last_updated_date = g_date
     where  fnd.	rep_id	          =	upd_rec.	rep_id          and
            fnd.	wfs_customer_no  	=	upd_rec.	wfs_customer_no and
            fnd.	product_code_no	  =	upd_rec.	product_code_no and
            fnd.	action_date	      =	upd_rec.	action_date and
            fnd.	action_time	      =	upd_rec.	action_time and
            ( 
            nvl(fnd.wfs_account_no	 ,0) <>	upd_rec.	wfs_account_no	or
            nvl(fnd.action_code	     ,0) <>	upd_rec.	action_code	    or
            nvl(fnd.action_remarks1  ,0) <>	upd_rec.	action_remarks1	or
            nvl(fnd.action_remarks2	 ,0) <>	upd_rec.	action_remarks2	or
            nvl(fnd.action_remarks3  ,0) <>	upd_rec.	action_remarks3	or
            nvl(fnd.action_value	   ,0) <>	upd_rec.	action_value	  or
            nvl(fnd.ptp_action_date	 ,'1 Jan 1900') <>	upd_rec.	ptp_action_date	or
            nvl(fnd.ptp_review_date	 ,'1 Jan 1900') <>	upd_rec.	ptp_review_date	 

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_rep_action_day_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             cpy.	rep_id	,
             cpy.	wfs_customer_no	,
             cpy.	product_code_no	,
             cpy.	action_date	,
             cpy.	action_time	,
             cpy.	wfs_account_no	,
             cpy.	action_code	,
             cpy.	action_remarks1	,
             cpy.	action_remarks2	,
             cpy.	action_remarks3	,
             cpy.	action_value	,
             cpy.	ptp_action_date	,
             cpy.	ptp_review_date 
      from   stg_vsn_rep_action_day_cpy cpy
      where  
      ( 1 =   
       (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no  and cust.customer_no = 0 ) or
        1 = 
        (select dummy_ind from  fnd_wfs_product pcd
         where  cpy.product_code_no       = pcd.product_code_no )  or
--        1 = 
--        (select dummy_ind from  fnd_wfs_rep rep
--         where  cpy.rep_id       = rep.rep_id )  or   
        1 = 
        (select dummy_ind from  fnd_customer_product cust
         where  cpy.wfs_account_no       = cust.product_no  and cust.customer_no = 0 )   
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
    from   stg_vsn_rep_action_day_cpy
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
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_vsn_rep_action_day_cpy
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

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
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
end wh_fnd_wfs_228u;
