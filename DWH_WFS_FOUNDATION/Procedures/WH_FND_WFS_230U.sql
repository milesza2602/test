--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_230U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_230U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_tran_day fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_tran_day_cpy
--               Output - fnd_wfs_tran_day
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


g_wfs_customer_no     stg_vsn_tran_day_cpy.wfs_customer_no%type;  
g_product_code_no    stg_vsn_tran_day_cpy.product_code_no%type; 
g_tran_no            stg_vsn_tran_day_cpy.tran_no%type;  
g_tran_posting_date  stg_vsn_tran_day_cpy.tran_posting_date%type; 
g_tran_code          stg_vsn_tran_day_cpy.tran_code%type;  
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_230U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_TRAN_DAY EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_tran_day_cpy
where (wfs_customer_no,
product_code_no,
tran_no,
tran_posting_date,
tran_code)
in
(select wfs_customer_no,
product_code_no,
tran_no,
tran_posting_date,
tran_code 
from stg_vsn_tran_day_cpy 
group by wfs_customer_no,
product_code_no,
tran_no,
tran_posting_date,
tran_code 
having count(*) > 1) 
order by wfs_customer_no,
product_code_no,
tran_no,
tran_posting_date,
tran_code ,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_tran_day is
select /*+ FULL(stg)  parallel (4) */  
              stg.wfs_customer_no,
              stg.product_code_no,
              stg.tran_no,
              stg.tran_posting_date,
              stg.tran_code,
              stg.tran_effectv_date,
              stg.tran_type,
              stg.wfs_account_no,
              stg.location_no,
              stg.wfs_plan,
              stg.fds_txt_ind,
              stg.tran_value,
              stg.receipt_no,
              stg.tran_desc 
      from    stg_vsn_tran_day_cpy stg,
              fnd_wfs_tran_day fnd
      where   stg.wfs_customer_no       = fnd.wfs_customer_no and             
              stg.product_code_no       = fnd.product_code_no    and   
              stg.tran_no               = fnd.tran_no    and 
              stg.tran_posting_date     = fnd.tran_posting_date    and 
              stg.tran_code             = fnd.tran_code    and 
              stg.sys_process_code      = 'N'  
            and
            ( 
            nvl(fnd.tran_effectv_date	,'1 Jan 1900') <>	stg.	tran_effectv_date	or
            nvl(fnd.tran_type       	,0) <>	stg.	tran_type	or
            nvl(fnd.wfs_account_no	  ,0) <>	stg.	wfs_account_no	or
            nvl(fnd.location_no	      ,0) <>	stg.	location_no	or
            nvl(fnd.wfs_plan	        ,0) <>	stg.	wfs_plan	or
            nvl(fnd.fds_txt_ind     	,0) <>	stg.	fds_txt_ind	or
            nvl(fnd.tran_value      	,0) <>	stg.	tran_value	or
            nvl(fnd.receipt_no      	,0) <>	stg.	receipt_no	or
            nvl(fnd.tran_desc	        ,0) <>	stg.	tran_desc	 
            )        
      order by
              stg.wfs_customer_no,
              stg.product_code_no,
              stg.tran_no,
              stg.tran_posting_date,
              stg.tran_code,stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no     := 0; 
   g_product_code_no    := 0;
   g_tran_no            := 0; 
   g_tran_posting_date  := '1 Jan 2000';
   g_tran_code          := 0;    

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no    = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.tran_no           = g_tran_no and
        dupp_record.tran_posting_date = g_tran_posting_date and
        dupp_record.tran_code         = g_tran_code     then
        update stg_vsn_tran_day_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no     := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_tran_no            := dupp_record.tran_no; 
    g_tran_posting_date  := dupp_record.tran_posting_date;
    g_tran_code          := dupp_record.tran_code ;      
 

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

      insert /*+ APPEND parallel (pcd,2) */ into fnd_wfs_product pcd 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	product_code_no	,
             'Dummy wh_fnd_wfs_230u',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_tran_day_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_product
       where 	product_code_no     = cpy.product_code_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;

--******************************************************************************

      insert /*+ APPEND parallel (trn,2) */ into fnd_wfs_tran trn 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	tran_code	,
             'Dummy wh_fnd_wfs_230u',
             'Dummy wh_fnd_wfs_230u',
             g_date,
             1
      from   stg_vsn_tran_day_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_tran
       where 	tran_code           = cpy.tran_code )
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
      from   stg_vsn_tran_day_cpy cpy
 
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
      from   stg_vsn_tran_day_cpy cpy
 
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
      
      insert /*+ APPEND parallel (fnd,4) */ into fnd_wfs_tran_day fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
             cpy.wfs_customer_no,
             cpy.product_code_no,
             cpy.tran_no,
             cpy.tran_posting_date,
             cpy.tran_code,
             cpy.tran_effectv_date,
             cpy.tran_type,
             cpy.wfs_account_no,
             cpy.location_no,
             cpy.wfs_plan,
             cpy.fds_txt_ind,
             cpy.tran_value,
             cpy.receipt_no,
             cpy.TRAN_DESC,
             g_date as last_updated_date
       from  stg_vsn_tran_day_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_tran_day 
       where  wfs_customer_no     = cpy.wfs_customer_no and
              product_code_no     = cpy.product_code_no and
              tran_no             = cpy.tran_no and
              tran_posting_date   = cpy.tran_posting_date and
              tran_code           = cpy.tran_code )
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



for upd_rec in c_stg_vsn_tran_day
   loop
     update fnd_wfs_tran_day fnd 
     set    
            fnd.	tran_effectv_date	=	upd_rec.	tran_effectv_date	,
            fnd.	tran_type       	=	upd_rec.	tran_type	,
            fnd.	wfs_account_no	  =	upd_rec.	wfs_account_no,
            fnd.	location_no	      =	upd_rec.	location_no	,
            fnd.	wfs_plan	        =	upd_rec.	wfs_plan	,
            fnd.	fds_txt_ind     	=	upd_rec.	fds_txt_ind	,
            fnd.	tran_value      	=	upd_rec.	tran_value	,
            fnd.	receipt_no      	=	upd_rec.	receipt_no	,
            fnd.	tran_desc	        =	upd_rec.	tran_desc	,
            fnd.  last_updated_date = g_date
     where  fnd.	wfs_customer_no	  =	upd_rec.	wfs_customer_no and
            fnd.	product_code_no	  =	upd_rec.	product_code_no	and
            fnd.	tran_no	          =	upd_rec.	tran_no	and
            fnd.	tran_posting_date	=	upd_rec.	tran_posting_date	and
            fnd.	tran_code       	=	upd_rec.	tran_code	;
             
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_tran_day_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             cpy.	wfs_customer_no	,
             cpy.	product_code_no	,
             cpy.	tran_no	,
             cpy.	tran_posting_date	,
             cpy.	tran_code	,
             cpy.	tran_effectv_date	,
             cpy.	tran_type	,
             cpy.	wfs_account_no	,
             cpy.	location_no	,
             cpy.	wfs_plan	,
             cpy.	fds_txt_ind	,
             cpy.	tran_value	,
             cpy.	receipt_no	,
             cpy.	tran_desc 
      from   stg_vsn_tran_day_cpy cpy
      where  
      ( 1 =   
        (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no and cust.customer_no = 0) or
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
    from   stg_vsn_tran_day_cpy
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
--    update stg_vsn_tran_day_cpy
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
       raise;
end wh_fnd_wfs_230u;
