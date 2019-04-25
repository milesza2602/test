--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_220U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_220U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_colrep_tallies fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_colrep_tallies_cpy
--               Output - fnd_wfs_colrep_tallies
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

g_rep_id             stg_vsn_colrep_tallies_cpy.rep_id%type; 
g_col_tally_date stg_vsn_colrep_tallies_cpy.col_tally_date%type;  

   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_220U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_COLREP_TALLIES EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_colrep_tallies_cpy
where (
rep_id,col_tally_date)
in
(select rep_id,col_tally_date
from stg_vsn_colrep_tallies_cpy 
group by rep_id,col_tally_date
having count(*) > 1) 
order by rep_id,col_tally_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_colrep_tallies is
select /*+ FULL(stg)  parallel (stg,2) */  
             	cpy.	*
      from    stg_vsn_colrep_tallies_cpy cpy,
              fnd_wfs_colrep_tallies fnd
      where   cpy.rep_id            = fnd.rep_id    and
              cpy.col_tally_date    = fnd.col_tally_date and             
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.rep_id,
              cpy.col_tally_date,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_col_tally_date := '1 Jan 2000'; 
   g_rep_id             := ' '; 


for dupp_record in stg_dup
   loop

    if  dupp_record.rep_id           = g_rep_id and
        dupp_record.col_tally_date   = g_col_tally_date  then
        update stg_vsn_colrep_tallies_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_col_tally_date     := dupp_record.col_tally_date; 
    g_rep_id             := dupp_record.rep_id; 


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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_rep fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             CPY.	REP_ID	,
             'Dummy wh_fnd_wfs_220u',
             0	,
             g_date,
             1
      from   stg_vsn_colrep_tallies_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_rep
       where  rep_id              = cpy.rep_id )
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_colrep_tallies fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	rep_id	,
            	cpy.	col_tally_date	,
            	cpy.	begin_acc_qty	,
            	cpy.	begin_acc_val	,
            	cpy.	new_acc_qty	,
            	cpy.	new_acc_val	,
            	cpy.	transfer_in_qty	,
            	cpy.	transfer_in_val	,
            	cpy.	transfer_out_qty	,
            	cpy.	transfer_out_val	,
            	cpy.	satisfied_qty	,
            	cpy.	satisfied_val	,
            	cpy.	end_batch_qty	,
            	cpy.	end_batch_val	,
            	cpy.	payment_rec_qty	,
            	cpy.	payment_rec_val	,
            	cpy.	credit_posted_qty	,
            	cpy.	credit_posted_val	,
            	cpy.	debit_posted_qty	,
            	cpy.	debit_posted_val	,
            	cpy.	reversal_qty	,
            	cpy.	reversal_val	,
            	cpy.	gen_credits_qty	,
            	cpy.	gen_credits_val	,
            	cpy.	gen_debits_qty	,
            	cpy.	gen_debits_val	,
            	cpy.	num_in_queue_qty	,
            	cpy.	num_in_queue_val	,
            	cpy.	num_work_bycol_qty	,
            	cpy.	num_work_bycol_val	,
            	cpy.	num_work_byoth_qty	,
            	cpy.	num_work_byoth_val	,
            	cpy.	num_not_work_qty	,
            	cpy.	num_not_work_val	,
            	cpy.	num_worked_qty	,
            	cpy.	num_worked_val	,
            	cpy.	prod_actions_num	,
            	cpy.	productive_perc	,
            	cpy.	broken_prom_qty	,
            	cpy.	broken_prom_val	,
            	cpy.	ptp_pmt_recd_qty	,
            	cpy.	ptp_pmt_recd_val	,

             g_date as last_updated_date
      from   stg_vsn_colrep_tallies_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_colrep_tallies 
       where  col_tally_date  = cpy.col_tally_date and
              rep_id              = cpy.rep_id )
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



for upd_rec in c_stg_vsn_colrep_tallies
   loop
     update fnd_wfs_colrep_tallies fnd 
     set    fnd.	begin_acc_qty	=	upd_rec.	begin_acc_qty	,
            fnd.	begin_acc_val	=	upd_rec.	begin_acc_val	,
            fnd.	new_acc_qty	=	upd_rec.	new_acc_qty	,
            fnd.	new_acc_val	=	upd_rec.	new_acc_val	,
            fnd.	transfer_in_qty	=	upd_rec.	transfer_in_qty	,
            fnd.	transfer_in_val	=	upd_rec.	transfer_in_val	,
            fnd.	transfer_out_qty	=	upd_rec.	transfer_out_qty	,
            fnd.	transfer_out_val	=	upd_rec.	transfer_out_val	,
            fnd.	satisfied_qty	=	upd_rec.	satisfied_qty	,
            fnd.	satisfied_val	=	upd_rec.	satisfied_val	,
            fnd.	end_batch_qty	=	upd_rec.	end_batch_qty	,
            fnd.	end_batch_val	=	upd_rec.	end_batch_val	,
            fnd.	payment_rec_qty	=	upd_rec.	payment_rec_qty	,
            fnd.	payment_rec_val	=	upd_rec.	payment_rec_val	,
            fnd.	credit_posted_qty	=	upd_rec.	credit_posted_qty	,
            fnd.	credit_posted_val	=	upd_rec.	credit_posted_val	,
            fnd.	debit_posted_qty	=	upd_rec.	debit_posted_qty	,
            fnd.	debit_posted_val	=	upd_rec.	debit_posted_val	,
            fnd.	reversal_qty	=	upd_rec.	reversal_qty	,
            fnd.	reversal_val	=	upd_rec.	reversal_val	,
            fnd.	gen_credits_qty	=	upd_rec.	gen_credits_qty	,
            fnd.	gen_credits_val	=	upd_rec.	gen_credits_val	,
            fnd.	gen_debits_qty	=	upd_rec.	gen_debits_qty	,
            fnd.	gen_debits_val	=	upd_rec.	gen_debits_val	,
            fnd.	num_in_queue_qty	=	upd_rec.	num_in_queue_qty	,
            fnd.	num_in_queue_val	=	upd_rec.	num_in_queue_val	,
            fnd.	num_work_bycol_qty	=	upd_rec.	num_work_bycol_qty	,
            fnd.	num_work_bycol_val	=	upd_rec.	num_work_bycol_val	,
            fnd.	num_work_byoth_qty	=	upd_rec.	num_work_byoth_qty	,
            fnd.	num_work_byoth_val	=	upd_rec.	num_work_byoth_val	,
            fnd.	num_not_work_qty	=	upd_rec.	num_not_work_qty	,
            fnd.	num_not_work_val	=	upd_rec.	num_not_work_val	,
            fnd.	num_worked_qty	=	upd_rec.	num_worked_qty	,
            fnd.	num_worked_val	=	upd_rec.	num_worked_val	,
            fnd.	prod_actions_num	=	upd_rec.	prod_actions_num	,
            fnd.	productive_perc	=	upd_rec.	productive_perc	,
            fnd.	broken_prom_qty	=	upd_rec.	broken_prom_qty	,
            fnd.	broken_prom_val	=	upd_rec.	broken_prom_val	,
            fnd.	ptp_pmt_recd_qty	=	upd_rec.	ptp_pmt_recd_qty	,
            fnd.	ptp_pmt_recd_val	=	upd_rec.	ptp_pmt_recd_val	,
            fnd.  last_updated_date = g_date
     where  fnd.	rep_id	          =	upd_rec.	rep_id          and
            fnd.	col_tally_date  	=	upd_rec.	col_tally_date and
            ( 
            nvl(fnd.begin_acc_qty	,0) <>	upd_rec.	begin_acc_qty	or
            nvl(fnd.begin_acc_val	,0) <>	upd_rec.	begin_acc_val	or
            nvl(fnd.new_acc_qty	,0) <>	upd_rec.	new_acc_qty	or
            nvl(fnd.new_acc_val	,0) <>	upd_rec.	new_acc_val	or
            nvl(fnd.transfer_in_qty	,0) <>	upd_rec.	transfer_in_qty	or
            nvl(fnd.transfer_in_val	,0) <>	upd_rec.	transfer_in_val	or
            nvl(fnd.transfer_out_qty	,0) <>	upd_rec.	transfer_out_qty	or
            nvl(fnd.transfer_out_val	,0) <>	upd_rec.	transfer_out_val	or
            nvl(fnd.satisfied_qty	,0) <>	upd_rec.	satisfied_qty	or
            nvl(fnd.satisfied_val	,0) <>	upd_rec.	satisfied_val	or
            nvl(fnd.end_batch_qty	,0) <>	upd_rec.	end_batch_qty	or
            nvl(fnd.end_batch_val	,0) <>	upd_rec.	end_batch_val	or
            nvl(fnd.payment_rec_qty	,0) <>	upd_rec.	payment_rec_qty	or
            nvl(fnd.payment_rec_val	,0) <>	upd_rec.	payment_rec_val	or
            nvl(fnd.credit_posted_qty	,0) <>	upd_rec.	credit_posted_qty	or
            nvl(fnd.credit_posted_val	,0) <>	upd_rec.	credit_posted_val	or
            nvl(fnd.debit_posted_qty	,0) <>	upd_rec.	debit_posted_qty	or
            nvl(fnd.debit_posted_val	,0) <>	upd_rec.	debit_posted_val	or
            nvl(fnd.reversal_qty	,0) <>	upd_rec.	reversal_qty	or
            nvl(fnd.reversal_val	,0) <>	upd_rec.	reversal_val	or
            nvl(fnd.gen_credits_qty	,0) <>	upd_rec.	gen_credits_qty	or
            nvl(fnd.gen_credits_val	,0) <>	upd_rec.	gen_credits_val	or
            nvl(fnd.gen_debits_qty	,0) <>	upd_rec.	gen_debits_qty	or
            nvl(fnd.gen_debits_val	,0) <>	upd_rec.	gen_debits_val	or
            nvl(fnd.num_in_queue_qty	,0) <>	upd_rec.	num_in_queue_qty	or
            nvl(fnd.num_in_queue_val	,0) <>	upd_rec.	num_in_queue_val	or
            nvl(fnd.num_work_bycol_qty	,0) <>	upd_rec.	num_work_bycol_qty	or
            nvl(fnd.num_work_bycol_val	,0) <>	upd_rec.	num_work_bycol_val	or
            nvl(fnd.num_work_byoth_qty	,0) <>	upd_rec.	num_work_byoth_qty	or
            nvl(fnd.num_work_byoth_val	,0) <>	upd_rec.	num_work_byoth_val	or
            nvl(fnd.num_not_work_qty	,0) <>	upd_rec.	num_not_work_qty	or
            nvl(fnd.num_not_work_val	,0) <>	upd_rec.	num_not_work_val	or
            nvl(fnd.num_worked_qty	,0) <>	upd_rec.	num_worked_qty	or
            nvl(fnd.num_worked_val	,0) <>	upd_rec.	num_worked_val	or
            nvl(fnd.prod_actions_num	,0) <>	upd_rec.	prod_actions_num	or
            nvl(fnd.productive_perc	,0) <>	upd_rec.	productive_perc	or
            nvl(fnd.broken_prom_qty	,0) <>	upd_rec.	broken_prom_qty	or
            nvl(fnd.broken_prom_val	,0) <>	upd_rec.	broken_prom_val	or
            nvl(fnd.ptp_pmt_recd_qty	,0) <>	upd_rec.	ptp_pmt_recd_qty	or
            nvl(fnd.ptp_pmt_recd_val	,0) <>	upd_rec.	ptp_pmt_recd_val 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_colrep_tallies_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             	cpy.	rep_id	,
            	cpy.	col_tally_date	,
            	cpy.	begin_acc_qty	,
            	cpy.	begin_acc_val	,
            	cpy.	new_acc_qty	,
            	cpy.	new_acc_val	,
            	cpy.	transfer_in_qty	,
            	cpy.	transfer_in_val	,
            	cpy.	transfer_out_qty	,
            	cpy.	transfer_out_val	,
            	cpy.	satisfied_qty	,
            	cpy.	satisfied_val	,
            	cpy.	end_batch_qty	,
            	cpy.	end_batch_val	,
            	cpy.	payment_rec_qty	,
            	cpy.	payment_rec_val	,
            	cpy.	credit_posted_qty	,
            	cpy.	credit_posted_val	,
            	cpy.	debit_posted_qty	,
            	cpy.	debit_posted_val	,
            	cpy.	reversal_qty	,
            	cpy.	reversal_val	,
            	cpy.	gen_credits_qty	,
            	cpy.	gen_credits_val	,
            	cpy.	gen_debits_qty	,
            	cpy.	gen_debits_val	,
            	cpy.	num_in_queue_qty	,
            	cpy.	num_in_queue_val	,
            	cpy.	num_work_bycol_qty	,
            	cpy.	num_work_bycol_val	,
            	cpy.	num_work_byoth_qty	,
            	cpy.	num_work_byoth_val	,
            	cpy.	num_not_work_qty	,
            	cpy.	num_not_work_val	,
            	cpy.	num_worked_qty	,
            	cpy.	num_worked_val	,
            	cpy.	prod_actions_num	,
            	cpy.	productive_perc	,
            	cpy.	broken_prom_qty	,
            	cpy.	broken_prom_val	,
            	cpy.	ptp_pmt_recd_qty	,
            	cpy.	ptp_pmt_recd_val	
      from   stg_vsn_colrep_tallies_cpy cpy
      where  
      ( 
        1 = 
        (select dummy_ind from  fnd_wfs_rep rep
         where  cpy.rep_id       = rep.rep_id ) 
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
    from   stg_vsn_colrep_tallies_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;
    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_vsn_colrep_tallies_cpy
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
end wh_fnd_wfs_220u;
