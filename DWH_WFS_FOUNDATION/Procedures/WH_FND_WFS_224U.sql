--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_224U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_224U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_offer fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_offer_cpy
--               Output - fnd_wfs_offer
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

g_offer_no             stg_vsn_offer_cpy.offer_no%type; 


   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_224U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_OFFER EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_offer_cpy
where (
offer_no)
in
(select offer_no
from stg_vsn_offer_cpy 
group by offer_no
having count(*) > 1) 
order by offer_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_offer is
select /*+ FULL(stg)  parallel (stg,2) */  
             	cpy.	*
      from    stg_vsn_offer_cpy cpy,
              fnd_wfs_offer fnd
      WHERE   CPY.OFFER_NO                = FND.OFFER_NO    AND
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.offer_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin



   g_offer_no             := 0; 


for dupp_record in stg_dup
   loop

    if  dupp_record.offer_no               = g_offer_no  then
        update stg_vsn_offer_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

 
    g_offer_no             := dupp_record.offer_no; 


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
--******************************************************************************

     insert /*+ APPEND parallel (pcd,2) */ into fnd_wfs_product pcd 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	offer_prod_no	,
             'Dummy wh_fnd_wfs_224U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_offer_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_product
       where 	product_code_no     = cpy.offer_prod_no )
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_offer fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	offer_no	,
            	cpy.	offer_accepted_ind	,
            	cpy.	cc_scenario_key	,
            	cpy.	offer_prod_no	,
            	cpy.	promotion_no	,
            	cpy.	create_date	,
            	cpy.	expiry_date	,
            	cpy.	cc_low_approved_amt	,
            	cpy.	cc_high_approved_amt	,
            	cpy.	cc_low_budget_amt	,
            	cpy.	cc_high_budget_amt	,
            	cpy.	cc_low_approved_interest	,
            	cpy.	cc_high_approved_interest	,
            	cpy.	cc_low_budget_interest	,
            	cpy.	cc_high_budget_interest	,
            	cpy.	cc_type_low	,
            	cpy.	cc_type_high	,
            	cpy.	application_no	,
            	cpy.	prom_extract_date	,
            	cpy.	batch_no	,
            	cpy.	change_date	,
            	cpy.	change_user_no	,
            	cpy.	customer_no	,
            	cpy.	inquiry_no	,
            	cpy.	basic_ins	,
            	cpy.	comprehensive_ins	,
            	cpy.	death_cover_ins	,
            	cpy.	basic_partner_ins_ind	,
            	cpy.	comprehensive_partner_ins_ind	,
            	cpy.	partner_death_cover	,
            	cpy.	partner_first_name	,
            	cpy.	partner_surname	,
            	cpy.	partner_id_no	,
            	cpy.	offer_status	,
            	cpy.	instalment_period	,
            	cpy.	actual_limit	,
            	cpy.	debit_order_ind	,
            	cpy.	debit_order_bank	,
            	cpy.	debit_order_acc_no	,
            	cpy.	debit_order_branch	,
            	cpy.	debit_order_branch_no	,
            	cpy.	debit_order_acc_type	,
            	cpy.	debit_order_payment_option	,
            	cpy.	debit_order_day	,
            	cpy.	rate_indicator	,
            	cpy.	first_draw_down	,
            	cpy.	vision_risk_ind	,
            	cpy.	ww_staff_ind	,

             g_date as last_updated_date
      from   stg_vsn_offer_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_offer 
       where  offer_no              = cpy.offer_no )
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



for upd_rec in c_stg_vsn_offer
   loop
     update fnd_wfs_offer fnd 
     set    fnd.	offer_accepted_ind	=	upd_rec.	offer_accepted_ind	,
            fnd.	cc_scenario_key	=	upd_rec.	cc_scenario_key	,
            fnd.	offer_prod_no	=	upd_rec.	offer_prod_no	,
            fnd.	promotion_no	=	upd_rec.	promotion_no	,
            fnd.	create_date	=	upd_rec.	create_date	,
            fnd.	expiry_date	=	upd_rec.	expiry_date	,
            fnd.	cc_low_approved_amt	=	upd_rec.	cc_low_approved_amt	,
            fnd.	cc_high_approved_amt	=	upd_rec.	cc_high_approved_amt	,
            fnd.	cc_low_budget_amt	=	upd_rec.	cc_low_budget_amt	,
            fnd.	cc_high_budget_amt	=	upd_rec.	cc_high_budget_amt	,
            fnd.	cc_low_approved_interest	=	upd_rec.	cc_low_approved_interest	,
            fnd.	cc_high_approved_interest	=	upd_rec.	cc_high_approved_interest	,
            fnd.	cc_low_budget_interest	=	upd_rec.	cc_low_budget_interest	,
            fnd.	cc_high_budget_interest	=	upd_rec.	cc_high_budget_interest	,
            fnd.	cc_type_low	=	upd_rec.	cc_type_low	,
            fnd.	cc_type_high	=	upd_rec.	cc_type_high	,
            fnd.	application_no	=	upd_rec.	application_no	,
            fnd.	prom_extract_date	=	upd_rec.	prom_extract_date	,
            fnd.	batch_no	=	upd_rec.	batch_no	,
            fnd.	change_date	=	upd_rec.	change_date	,
            fnd.	change_user_no	=	upd_rec.	change_user_no	,
            fnd.	customer_no	=	upd_rec.	customer_no	,
            fnd.	inquiry_no	=	upd_rec.	inquiry_no	,
            fnd.	basic_ins	=	upd_rec.	basic_ins	,
            fnd.	comprehensive_ins	=	upd_rec.	comprehensive_ins	,
            fnd.	death_cover_ins	=	upd_rec.	death_cover_ins	,
            fnd.	basic_partner_ins_ind	=	upd_rec.	basic_partner_ins_ind	,
            fnd.	comprehensive_partner_ins_ind	=	upd_rec.	comprehensive_partner_ins_ind	,
            fnd.	partner_death_cover	=	upd_rec.	partner_death_cover	,
            fnd.	partner_first_name	=	upd_rec.	partner_first_name	,
            fnd.	partner_surname	=	upd_rec.	partner_surname	,
            fnd.	partner_id_no	=	upd_rec.	partner_id_no	,
            fnd.	offer_status	=	upd_rec.	offer_status	,
            fnd.	instalment_period	=	upd_rec.	instalment_period	,
            fnd.	actual_limit	=	upd_rec.	actual_limit	,
            fnd.	debit_order_ind	=	upd_rec.	debit_order_ind	,
            fnd.	debit_order_bank	=	upd_rec.	debit_order_bank	,
            fnd.	debit_order_acc_no	=	upd_rec.	debit_order_acc_no	,
            fnd.	debit_order_branch	=	upd_rec.	debit_order_branch	,
            fnd.	debit_order_branch_no	=	upd_rec.	debit_order_branch_no	,
            fnd.	debit_order_acc_type	=	upd_rec.	debit_order_acc_type	,
            fnd.	debit_order_payment_option	=	upd_rec.	debit_order_payment_option	,
            fnd.	debit_order_day	=	upd_rec.	debit_order_day	,
            fnd.	rate_indicator	=	upd_rec.	rate_indicator	,
            fnd.	first_draw_down	=	upd_rec.	first_draw_down	,
            fnd.	vision_risk_ind	=	upd_rec.	vision_risk_ind	,
            fnd.	ww_staff_ind	=	upd_rec.	ww_staff_ind	,
            fnd.  last_updated_date = g_date
     where  fnd.	offer_no	          =	upd_rec.	offer_no          and
            ( 
            nvl(fnd.offer_accepted_ind	,0) <>	upd_rec.	offer_accepted_ind	or
            nvl(fnd.cc_scenario_key	,0) <>	upd_rec.	cc_scenario_key	or
            nvl(fnd.offer_prod_no	,0) <>	upd_rec.	offer_prod_no	or
            nvl(fnd.promotion_no	,0) <>	upd_rec.	promotion_no	or
            nvl(fnd.create_date	,'1 Jan 1900') <>	upd_rec.	create_date	or
            nvl(fnd.expiry_date	,'1 Jan 1900') <>	upd_rec.	expiry_date	or
            nvl(fnd.cc_low_approved_amt	,0) <>	upd_rec.	cc_low_approved_amt	or
            nvl(fnd.cc_high_approved_amt	,0) <>	upd_rec.	cc_high_approved_amt	or
            nvl(fnd.cc_low_budget_amt	,0) <>	upd_rec.	cc_low_budget_amt	or
            nvl(fnd.cc_high_budget_amt	,0) <>	upd_rec.	cc_high_budget_amt	or
            nvl(fnd.cc_low_approved_interest	,0) <>	upd_rec.	cc_low_approved_interest	or
            nvl(fnd.cc_high_approved_interest	,0) <>	upd_rec.	cc_high_approved_interest	or
            nvl(fnd.cc_low_budget_interest	,0) <>	upd_rec.	cc_low_budget_interest	or
            nvl(fnd.cc_high_budget_interest	,0) <>	upd_rec.	cc_high_budget_interest	or
            nvl(fnd.cc_type_low	,0) <>	upd_rec.	cc_type_low	or
            nvl(fnd.cc_type_high	,0) <>	upd_rec.	cc_type_high	or
            nvl(fnd.application_no	,0) <>	upd_rec.	application_no	or
            nvl(fnd.prom_extract_date	,'1 Jan 1900') <>	upd_rec.	prom_extract_date	or
            nvl(fnd.batch_no	,0) <>	upd_rec.	batch_no	or
            nvl(fnd.change_date	,'1 Jan 1900') <>	upd_rec.	change_date	or
            nvl(fnd.change_user_no	,0) <>	upd_rec.	change_user_no	or
            nvl(fnd.customer_no	,0) <>	upd_rec.	customer_no	or
            nvl(fnd.inquiry_no	,0) <>	upd_rec.	inquiry_no	or
            nvl(fnd.basic_ins	,0) <>	upd_rec.	basic_ins	or
            nvl(fnd.comprehensive_ins	,0) <>	upd_rec.	comprehensive_ins	or
            nvl(fnd.death_cover_ins	,0) <>	upd_rec.	death_cover_ins	or
            nvl(fnd.basic_partner_ins_ind	,0) <>	upd_rec.	basic_partner_ins_ind	or
            nvl(fnd.comprehensive_partner_ins_ind	,0) <>	upd_rec.	comprehensive_partner_ins_ind	or
            nvl(fnd.partner_death_cover	,0) <>	upd_rec.	partner_death_cover	or
            nvl(fnd.partner_first_name	,0) <>	upd_rec.	partner_first_name	or
            nvl(fnd.partner_surname	,0) <>	upd_rec.	partner_surname	or
            nvl(fnd.partner_id_no	,0) <>	upd_rec.	partner_id_no	or
            nvl(fnd.offer_status	,0) <>	upd_rec.	offer_status	or
            nvl(fnd.instalment_period	,0) <>	upd_rec.	instalment_period	or
            nvl(fnd.actual_limit	,0) <>	upd_rec.	actual_limit	or
            nvl(fnd.debit_order_ind	,0) <>	upd_rec.	debit_order_ind	or
            nvl(fnd.debit_order_bank	,0) <>	upd_rec.	debit_order_bank	or
            nvl(fnd.debit_order_acc_no	,0) <>	upd_rec.	debit_order_acc_no	or
            nvl(fnd.debit_order_branch	,0) <>	upd_rec.	debit_order_branch	or
            nvl(fnd.debit_order_branch_no	,0) <>	upd_rec.	debit_order_branch_no	or
            nvl(fnd.debit_order_acc_type	,0) <>	upd_rec.	debit_order_acc_type	or
            nvl(fnd.debit_order_payment_option	,0) <>	upd_rec.	debit_order_payment_option	or
            nvl(fnd.debit_order_day	,0) <>	upd_rec.	debit_order_day	or
            nvl(fnd.rate_indicator	,0) <>	upd_rec.	rate_indicator	or
            nvl(fnd.first_draw_down	,0) <>	upd_rec.	first_draw_down	or
            nvl(fnd.vision_risk_ind	,0) <>	upd_rec.	vision_risk_ind	or
            nvl(fnd.ww_staff_ind	,0) <>	upd_rec.	ww_staff_ind	 

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_offer_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             	cpy.	offer_no	,
            	cpy.	offer_accepted_ind	,
            	cpy.	cc_scenario_key	,
            	cpy.	offer_prod_no	,
            	cpy.	promotion_no	,
            	cpy.	create_date	,
            	cpy.	expiry_date	,
            	cpy.	cc_low_approved_amt	,
            	cpy.	cc_high_approved_amt	,
            	cpy.	cc_low_budget_amt	,
            	cpy.	cc_high_budget_amt	,
            	cpy.	cc_low_approved_interest	,
            	cpy.	cc_high_approved_interest	,
            	cpy.	cc_low_budget_interest	,
            	cpy.	cc_high_budget_interest	,
            	cpy.	cc_type_low	,
            	cpy.	cc_type_high	,
            	cpy.	application_no	,
            	cpy.	prom_extract_date	,
            	cpy.	batch_no	,
            	cpy.	change_date	,
            	cpy.	change_user_no	,
            	cpy.	customer_no	,
            	cpy.	inquiry_no	,
            	cpy.	basic_ins	,
            	cpy.	comprehensive_ins	,
            	cpy.	death_cover_ins	,
            	cpy.	basic_partner_ins_ind	,
            	cpy.	comprehensive_partner_ins_ind	,
            	cpy.	partner_death_cover	,
            	cpy.	partner_first_name	,
            	cpy.	partner_surname	,
            	cpy.	partner_id_no	,
            	cpy.	offer_status	,
            	cpy.	instalment_period	,
            	cpy.	actual_limit	,
            	cpy.	debit_order_ind	,
            	cpy.	debit_order_bank	,
            	cpy.	debit_order_acc_no	,
            	cpy.	debit_order_branch	,
            	cpy.	debit_order_branch_no	,
            	cpy.	debit_order_acc_type	,
            	cpy.	debit_order_payment_option	,
            	cpy.	debit_order_day	,
            	cpy.	rate_indicator	,
            	cpy.	first_draw_down	,
            	cpy.	vision_risk_ind	,
            	cpy.	ww_staff_ind	

      from   stg_vsn_offer_cpy cpy
      where  
      ( 
        1 = 
        (select dummy_ind from  fnd_wfs_product prd
         where  cpy.offer_prod_no       = prd.product_code_no ) 
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
    from   stg_vsn_offer_cpy
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
    
    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_vsn_offer_cpy
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
end wh_fnd_wfs_224u;
