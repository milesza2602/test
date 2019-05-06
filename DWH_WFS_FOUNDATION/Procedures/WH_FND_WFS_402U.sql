--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_402U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_402U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_SC EMBOSS fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_sc_emboss_cpy
--               Output - fnd_wfs_sc_emboss
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


g_wfs_customer_no    stg_vsn_sc_emboss_cpy.wfs_customer_no%type;  
g_wfs_account_no     stg_vsn_sc_emboss_cpy.wfs_account_no%type; 
g_sequence_no        stg_vsn_sc_emboss_cpy.sequence_no%type; 
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_402U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_SC_EMBOSS EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_sc_emboss_cpy
where (wfs_customer_no,
wfs_account_no,
sequence_no)
in
(select wfs_customer_no,
wfs_account_no,
sequence_no
from stg_vsn_sc_emboss_cpy 
group by wfs_customer_no,
wfs_account_no,
sequence_no
having count(*) > 1) 
order by wfs_customer_no,
wfs_account_no,
sequence_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_sc_emboss is
select /*+ FULL(cpy)  parallel (cpy,2) */  
              cpy.*
      from    stg_vsn_sc_emboss_cpy cpy,
              fnd_wfs_sc_emboss fnd
      where   cpy.wfs_customer_no      = fnd.wfs_customer_no   and             
              cpy.wfs_account_no       = fnd.wfs_account_no    and   
              cpy.sequence_no          = fnd.sequence_no       and 
              cpy.sys_process_code     = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.wfs_customer_no,
              cpy.wfs_account_no,
              cpy.sequence_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_wfs_account_no     := 0;
   g_sequence_no        := 0;
 

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.wfs_account_no    = g_wfs_account_no and
        dupp_record.sequence_no       = g_sequence_no      then
        update stg_vsn_sc_emboss_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_wfs_account_no     := dupp_record.wfs_account_no;
    g_sequence_no        := dupp_record.sequence_no;
 

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.wfs_customer_no	,
             0,
             1,
             g_date,
             1
      from   stg_vsn_sc_emboss_cpy cpy
 
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
      from   stg_vsn_sc_emboss_cpy  cpy
 
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_sc_emboss fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	wfs_customer_no	,
            	cpy.	wfs_account_no	,
            	cpy.	sequence_no	,
            	cpy.	card_holder	,
            	cpy.	secondary_card_flag	,
            	cpy.	date_issued	,
            	cpy.	date_maintained	,
            	cpy.	block_code	,
            	cpy.	identity_no	,
              g_date as last_updated_date
       from   stg_vsn_sc_emboss_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_sc_emboss 
       where  wfs_customer_no     = cpy.wfs_customer_no and
              wfs_account_no      = cpy.wfs_account_no and
              sequence_no         = cpy.sequence_no  )
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



for upd_rec in c_stg_vsn_sc_emboss
   loop
     update fnd_wfs_sc_emboss fnd 
     set    fnd.	card_holder    	=	upd_rec.	card_holder	,
            fnd.	secondary_card_flag	=	upd_rec.	secondary_card_flag	,
            fnd.	date_issued	    =	upd_rec.	date_issued	,
            fnd.	date_maintained	=	upd_rec.	date_maintained	,
            fnd.	block_code	    =	upd_rec.	block_code	,
            fnd.	identity_no	    =	upd_rec.	identity_no	,

            fnd.  last_updated_date     = g_date
     where  fnd.	wfs_customer_no	      =	upd_rec.	wfs_customer_no and
            fnd.	wfs_account_no	      =	upd_rec.	wfs_account_no	and
            fnd.	sequence_no	          =	upd_rec.	sequence_no	and
            ( 
            nvl(fnd.card_holder	,0)         <>	upd_rec.	card_holder	or
            nvl(fnd.secondary_card_flag	,0) <>	upd_rec.	secondary_card_flag	or
            nvl(fnd.date_issued	,'1 Jan 1900')         <>	upd_rec.	date_issued	or
            nvl(fnd.date_maintained	,'1 Jan 1900')     <>	upd_rec.	date_maintained	or
            nvl(fnd.block_code	,0)         <>	upd_rec.	block_code	or
            nvl(fnd.identity_no	,0)         <>	upd_rec.	identity_no 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_sc_emboss_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
            	cpy.	WFS_CUSTOMER_NO	,
            	cpy.	WFS_ACCOUNT_NO	,
            	cpy.	SEQUENCE_NO	,
            	cpy.	CARD_HOLDER	,
            	cpy.	SECONDARY_CARD_FLAG	,
            	cpy.	DATE_ISSUED	,
            	cpy.	DATE_MAINTAINED	,
            	cpy.	BLOCK_CODE	,
            	cpy.	IDENTITY_NO	 
      from   stg_vsn_sc_emboss_cpy cpy
      where  
      ( 1 =   
        (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no ) or
        1 =   
        (select dummy_ind  from  fnd_customer_product cust
         where  cpy.wfs_account_no       = cust.product_no )  ) 
 
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
    from   stg_vsn_sc_emboss_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;

--    NO FURTHER NEED TO FLAG DUMMY RECORDS IN HOSPITAL 09 2016    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;


--    Taken out for better performance --------------------
--    update stg_vsn_sc_emboss_cpy
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
       RAISE;
end wh_fnd_wfs_402u;
