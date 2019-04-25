--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_216U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_216U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_colrep_bc_sum fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_colrep_bc_sum_cpy
--               Output - fnd_wfs_colrep_bc_sum
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

g_rep_id             stg_vsn_colrep_bc_sum_cpy.rep_id%type; 
g_cust_billing_cycle stg_vsn_colrep_bc_sum_cpy.cust_billing_cycle%type;  

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_216U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_COLREP_BC_SUM EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_colrep_bc_sum_cpy
where (
rep_id,cust_billing_cycle)
in
(select rep_id,cust_billing_cycle
from stg_vsn_colrep_bc_sum_cpy 
group by rep_id,cust_billing_cycle
having count(*) > 1) 
order by rep_id,cust_billing_cycle,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_colrep_bc_sum is
select /*+ FULL(stg)  parallel (stg,2) */  
             	cpy.	*
      from    stg_vsn_colrep_bc_sum_cpy cpy,
              fnd_wfs_colrep_bc_sum fnd
      where   cpy.rep_id                = fnd.rep_id    and
              cpy.cust_billing_cycle    = fnd.cust_billing_cycle and             
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.rep_id,
              cpy.cust_billing_cycle,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_cust_billing_cycle := 0; 
   g_rep_id             := ' '; 


for dupp_record in stg_dup
   loop

    if  dupp_record.rep_id               = g_rep_id and
        dupp_record.cust_billing_cycle   = g_cust_billing_cycle  then
        update stg_vsn_colrep_bc_sum_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_cust_billing_cycle    := dupp_record.cust_billing_cycle; 
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
             'Dummy wh_fnd_wfs_216u',
             0	,
             g_date,
             1
      from   stg_vsn_colrep_bc_sum_cpy cpy
 
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_colrep_bc_sum fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	rep_id	,
            	cpy.	cust_billing_cycle	,
            	cpy.	assn_op_val_cc	,
            	cpy.	assn_op_qty_cc	,
            	cpy.	trf_in_val_cc	,
            	cpy.	trf_in_qty_cc	,
            	cpy.	trf_out_val_cc	,
            	cpy.	trf_out_qty_cc	,
            	cpy.	rollup_val_cc	,
            	cpy.	rollup_qty_cc	,
            	cpy.	reassn_in_val_cc	,
            	cpy.	reassn_in_qty_cc	,
            	cpy.	reassn_out_val_cc	,
            	cpy.	reassn_out_qty_cc	,
            	cpy.	debit_adj_val_cc	,
            	cpy.	debit_adj_qty_cc	,
            	cpy.	credit_adj_val_cc	,
            	cpy.	credit_adj_qty_cc	,
            	cpy.	adj_assign_val_cc	,
            	cpy.	adj_assn_qty_cc	,
            	cpy.	pymt_val_cc	,
            	cpy.	pymt_qty_cc	,
            	cpy.	end_assn_val_cc	,
            	cpy.	end_assn_qty_cc	,
            	cpy.	satsfd_val_cc	,
            	cpy.	satsfd_qty_cc	,
            	cpy.	aged_up_val_cc	,
            	cpy.	aged_up_qty_cc	,
            	cpy.	aged_back_val_cc	,
            	cpy.	aged_back_qty_cc	,
            	cpy.	statusquo_val_cc	,
            	cpy.	statusquo_qty_cc	,
            	cpy.	other_val_cc	,
            	cpy.	other_qty_cc	,
            	cpy.	assn_op_val_pc	,
            	cpy.	assn_op_qty_pc	,
            	cpy.	trf_in_val_pc	,
            	cpy.	trf_in_qty_pc	,
            	cpy.	trf_out_val_pc	,
            	cpy.	trf_out_qty_pc	,
            	cpy.	rollup_val_pc	,
            	cpy.	rollup_qty_pc	,
            	cpy.	reassn_in_val_pc	,
            	cpy.	reassn_in_qty_pc	,
            	cpy.	reassn_out_val_pc	,
            	cpy.	reassn_out_qty_pc	,
            	cpy.	debit_adj_val_pc	,
            	cpy.	debit_adj_qty_pc	,
            	cpy.	credit_adj_val_pc	,
            	cpy.	credit_adj_qty_pc	,
            	cpy.	adj_assign_val_pc	,
            	cpy.	adj_assn_qty_pc	,
            	cpy.	pymt_val_pc	,
            	cpy.	pymt_qty_pc	,
            	cpy.	end_assn_val_pc	,
            	cpy.	end_assn_qty_pc	,
            	cpy.	satsfd_val_pc	,
            	cpy.	satsfd_qty_pc	,
            	cpy.	aged_up_val_pc	,
            	cpy.	aged_up_qty_pc	,
            	cpy.	aged_back_val_pc	,
            	cpy.	aged_back_qty_pc	,
            	cpy.	statusquo_val_pc	,
            	cpy.	statusquo_qty_pc	,
            	cpy.	other_val_pc	,
            	cpy.	other_qty_pc	,
             g_date as last_updated_date
      from   stg_vsn_colrep_bc_sum_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_colrep_bc_sum 
       where  cust_billing_cycle  = cpy.cust_billing_cycle and
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



for upd_rec in c_stg_vsn_colrep_bc_sum
   loop
     update fnd_wfs_colrep_bc_sum fnd 
     set    fnd.	assn_op_val_cc	=	upd_rec.	assn_op_val_cc	,
            fnd.	assn_op_qty_cc	=	upd_rec.	assn_op_qty_cc	,
            fnd.	trf_in_val_cc	=	upd_rec.	trf_in_val_cc	,
            fnd.	trf_in_qty_cc	=	upd_rec.	trf_in_qty_cc	,
            fnd.	trf_out_val_cc	=	upd_rec.	trf_out_val_cc	,
            fnd.	trf_out_qty_cc	=	upd_rec.	trf_out_qty_cc	,
            fnd.	rollup_val_cc	=	upd_rec.	rollup_val_cc	,
            fnd.	rollup_qty_cc	=	upd_rec.	rollup_qty_cc	,
            fnd.	reassn_in_val_cc	=	upd_rec.	reassn_in_val_cc	,
            fnd.	reassn_in_qty_cc	=	upd_rec.	reassn_in_qty_cc	,
            fnd.	reassn_out_val_cc	=	upd_rec.	reassn_out_val_cc	,
            fnd.	reassn_out_qty_cc	=	upd_rec.	reassn_out_qty_cc	,
            fnd.	debit_adj_val_cc	=	upd_rec.	debit_adj_val_cc	,
            fnd.	debit_adj_qty_cc	=	upd_rec.	debit_adj_qty_cc	,
            fnd.	credit_adj_val_cc	=	upd_rec.	credit_adj_val_cc	,
            fnd.	credit_adj_qty_cc	=	upd_rec.	credit_adj_qty_cc	,
            fnd.	adj_assign_val_cc	=	upd_rec.	adj_assign_val_cc	,
            fnd.	adj_assn_qty_cc	=	upd_rec.	adj_assn_qty_cc	,
            fnd.	pymt_val_cc	=	upd_rec.	pymt_val_cc	,
            fnd.	pymt_qty_cc	=	upd_rec.	pymt_qty_cc	,
            fnd.	end_assn_val_cc	=	upd_rec.	end_assn_val_cc	,
            fnd.	end_assn_qty_cc	=	upd_rec.	end_assn_qty_cc	,
            fnd.	satsfd_val_cc	=	upd_rec.	satsfd_val_cc	,
            fnd.	satsfd_qty_cc	=	upd_rec.	satsfd_qty_cc	,
            fnd.	aged_up_val_cc	=	upd_rec.	aged_up_val_cc	,
            fnd.	aged_up_qty_cc	=	upd_rec.	aged_up_qty_cc	,
            fnd.	aged_back_val_cc	=	upd_rec.	aged_back_val_cc	,
            fnd.	aged_back_qty_cc	=	upd_rec.	aged_back_qty_cc	,
            fnd.	statusquo_val_cc	=	upd_rec.	statusquo_val_cc	,
            fnd.	statusquo_qty_cc	=	upd_rec.	statusquo_qty_cc	,
            fnd.	other_val_cc	=	upd_rec.	other_val_cc	,
            fnd.	other_qty_cc	=	upd_rec.	other_qty_cc	,
            fnd.	assn_op_val_pc	=	upd_rec.	assn_op_val_pc	,
            fnd.	assn_op_qty_pc	=	upd_rec.	assn_op_qty_pc	,
            fnd.	trf_in_val_pc	=	upd_rec.	trf_in_val_pc	,
            fnd.	trf_in_qty_pc	=	upd_rec.	trf_in_qty_pc	,
            fnd.	trf_out_val_pc	=	upd_rec.	trf_out_val_pc	,
            fnd.	trf_out_qty_pc	=	upd_rec.	trf_out_qty_pc	,
            fnd.	rollup_val_pc	=	upd_rec.	rollup_val_pc	,
            fnd.	rollup_qty_pc	=	upd_rec.	rollup_qty_pc	,
            fnd.	reassn_in_val_pc	=	upd_rec.	reassn_in_val_pc	,
            fnd.	reassn_in_qty_pc	=	upd_rec.	reassn_in_qty_pc	,
            fnd.	reassn_out_val_pc	=	upd_rec.	reassn_out_val_pc	,
            fnd.	reassn_out_qty_pc	=	upd_rec.	reassn_out_qty_pc	,
            fnd.	debit_adj_val_pc	=	upd_rec.	debit_adj_val_pc	,
            fnd.	debit_adj_qty_pc	=	upd_rec.	debit_adj_qty_pc	,
            fnd.	credit_adj_val_pc	=	upd_rec.	credit_adj_val_pc	,
            fnd.	credit_adj_qty_pc	=	upd_rec.	credit_adj_qty_pc	,
            fnd.	adj_assign_val_pc	=	upd_rec.	adj_assign_val_pc	,
            fnd.	adj_assn_qty_pc	=	upd_rec.	adj_assn_qty_pc	,
            fnd.	pymt_val_pc	=	upd_rec.	pymt_val_pc	,
            fnd.	pymt_qty_pc	=	upd_rec.	pymt_qty_pc	,
            fnd.	end_assn_val_pc	=	upd_rec.	end_assn_val_pc	,
            fnd.	end_assn_qty_pc	=	upd_rec.	end_assn_qty_pc	,
            fnd.	satsfd_val_pc	=	upd_rec.	satsfd_val_pc	,
            fnd.	satsfd_qty_pc	=	upd_rec.	satsfd_qty_pc	,
            fnd.	aged_up_val_pc	=	upd_rec.	aged_up_val_pc	,
            fnd.	aged_up_qty_pc	=	upd_rec.	aged_up_qty_pc	,
            fnd.	aged_back_val_pc	=	upd_rec.	aged_back_val_pc	,
            fnd.	aged_back_qty_pc	=	upd_rec.	aged_back_qty_pc	,
            fnd.	statusquo_val_pc	=	upd_rec.	statusquo_val_pc	,
            fnd.	statusquo_qty_pc	=	upd_rec.	statusquo_qty_pc	,
            fnd.	other_val_pc	=	upd_rec.	other_val_pc	,
            fnd.	other_qty_pc	=	upd_rec.	other_qty_pc	,
            fnd.  last_updated_date = g_date
     where  fnd.	rep_id	          =	upd_rec.	rep_id          and
            fnd.	cust_billing_cycle  	=	upd_rec.	cust_billing_cycle and
            ( 
            nvl(fnd.rep_id	,0) <>	upd_rec.	rep_id	or
            nvl(fnd.cust_billing_cycle	,0) <>	upd_rec.	cust_billing_cycle	or
            nvl(fnd.assn_op_val_cc	,0) <>	upd_rec.	assn_op_val_cc	or
            nvl(fnd.assn_op_qty_cc	,0) <>	upd_rec.	assn_op_qty_cc	or
            nvl(fnd.trf_in_val_cc	,0) <>	upd_rec.	trf_in_val_cc	or
            nvl(fnd.trf_in_qty_cc	,0) <>	upd_rec.	trf_in_qty_cc	or
            nvl(fnd.trf_out_val_cc	,0) <>	upd_rec.	trf_out_val_cc	or
            nvl(fnd.trf_out_qty_cc	,0) <>	upd_rec.	trf_out_qty_cc	or
            nvl(fnd.rollup_val_cc	,0) <>	upd_rec.	rollup_val_cc	or
            nvl(fnd.rollup_qty_cc	,0) <>	upd_rec.	rollup_qty_cc	or
            nvl(fnd.reassn_in_val_cc	,0) <>	upd_rec.	reassn_in_val_cc	or
            nvl(fnd.reassn_in_qty_cc	,0) <>	upd_rec.	reassn_in_qty_cc	or
            nvl(fnd.reassn_out_val_cc	,0) <>	upd_rec.	reassn_out_val_cc	or
            nvl(fnd.reassn_out_qty_cc	,0) <>	upd_rec.	reassn_out_qty_cc	or
            nvl(fnd.debit_adj_val_cc	,0) <>	upd_rec.	debit_adj_val_cc	or
            nvl(fnd.debit_adj_qty_cc	,0) <>	upd_rec.	debit_adj_qty_cc	or
            nvl(fnd.credit_adj_val_cc	,0) <>	upd_rec.	credit_adj_val_cc	or
            nvl(fnd.credit_adj_qty_cc	,0) <>	upd_rec.	credit_adj_qty_cc	or
            nvl(fnd.adj_assign_val_cc	,0) <>	upd_rec.	adj_assign_val_cc	or
            nvl(fnd.adj_assn_qty_cc	,0) <>	upd_rec.	adj_assn_qty_cc	or
            nvl(fnd.pymt_val_cc	,0) <>	upd_rec.	pymt_val_cc	or
            nvl(fnd.pymt_qty_cc	,0) <>	upd_rec.	pymt_qty_cc	or
            nvl(fnd.end_assn_val_cc	,0) <>	upd_rec.	end_assn_val_cc	or
            nvl(fnd.end_assn_qty_cc	,0) <>	upd_rec.	end_assn_qty_cc	or
            nvl(fnd.satsfd_val_cc	,0) <>	upd_rec.	satsfd_val_cc	or
            nvl(fnd.satsfd_qty_cc	,0) <>	upd_rec.	satsfd_qty_cc	or
            nvl(fnd.aged_up_val_cc	,0) <>	upd_rec.	aged_up_val_cc	or
            nvl(fnd.aged_up_qty_cc	,0) <>	upd_rec.	aged_up_qty_cc	or
            nvl(fnd.aged_back_val_cc	,0) <>	upd_rec.	aged_back_val_cc	or
            nvl(fnd.aged_back_qty_cc	,0) <>	upd_rec.	aged_back_qty_cc	or
            nvl(fnd.statusquo_val_cc	,0) <>	upd_rec.	statusquo_val_cc	or
            nvl(fnd.statusquo_qty_cc	,0) <>	upd_rec.	statusquo_qty_cc	or
            nvl(fnd.other_val_cc	,0) <>	upd_rec.	other_val_cc	or
            nvl(fnd.other_qty_cc	,0) <>	upd_rec.	other_qty_cc	or
            nvl(fnd.assn_op_val_pc	,0) <>	upd_rec.	assn_op_val_pc	or
            nvl(fnd.assn_op_qty_pc	,0) <>	upd_rec.	assn_op_qty_pc	or
            nvl(fnd.trf_in_val_pc	,0) <>	upd_rec.	trf_in_val_pc	or
            nvl(fnd.trf_in_qty_pc	,0) <>	upd_rec.	trf_in_qty_pc	or
            nvl(fnd.trf_out_val_pc	,0) <>	upd_rec.	trf_out_val_pc	or
            nvl(fnd.trf_out_qty_pc	,0) <>	upd_rec.	trf_out_qty_pc	or
            nvl(fnd.rollup_val_pc	,0) <>	upd_rec.	rollup_val_pc	or
            nvl(fnd.rollup_qty_pc	,0) <>	upd_rec.	rollup_qty_pc	or
            nvl(fnd.reassn_in_val_pc	,0) <>	upd_rec.	reassn_in_val_pc	or
            nvl(fnd.reassn_in_qty_pc	,0) <>	upd_rec.	reassn_in_qty_pc	or
            nvl(fnd.reassn_out_val_pc	,0) <>	upd_rec.	reassn_out_val_pc	or
            nvl(fnd.reassn_out_qty_pc	,0) <>	upd_rec.	reassn_out_qty_pc	or
            nvl(fnd.debit_adj_val_pc	,0) <>	upd_rec.	debit_adj_val_pc	or
            nvl(fnd.debit_adj_qty_pc	,0) <>	upd_rec.	debit_adj_qty_pc	or
            nvl(fnd.credit_adj_val_pc	,0) <>	upd_rec.	credit_adj_val_pc	or
            nvl(fnd.credit_adj_qty_pc	,0) <>	upd_rec.	credit_adj_qty_pc	or
            nvl(fnd.adj_assign_val_pc	,0) <>	upd_rec.	adj_assign_val_pc	or
            nvl(fnd.adj_assn_qty_pc	,0) <>	upd_rec.	adj_assn_qty_pc	or
            nvl(fnd.pymt_val_pc	,0) <>	upd_rec.	pymt_val_pc	or
            nvl(fnd.pymt_qty_pc	,0) <>	upd_rec.	pymt_qty_pc	or
            nvl(fnd.end_assn_val_pc	,0) <>	upd_rec.	end_assn_val_pc	or
            nvl(fnd.end_assn_qty_pc	,0) <>	upd_rec.	end_assn_qty_pc	or
            nvl(fnd.satsfd_val_pc	,0) <>	upd_rec.	satsfd_val_pc	or
            nvl(fnd.satsfd_qty_pc	,0) <>	upd_rec.	satsfd_qty_pc	or
            nvl(fnd.aged_up_val_pc	,0) <>	upd_rec.	aged_up_val_pc	or
            nvl(fnd.aged_up_qty_pc	,0) <>	upd_rec.	aged_up_qty_pc	or
            nvl(fnd.aged_back_val_pc	,0) <>	upd_rec.	aged_back_val_pc	or
            nvl(fnd.aged_back_qty_pc	,0) <>	upd_rec.	aged_back_qty_pc	or
            nvl(fnd.statusquo_val_pc	,0) <>	upd_rec.	statusquo_val_pc	or
            nvl(fnd.statusquo_qty_pc	,0) <>	upd_rec.	statusquo_qty_pc	or
            nvl(fnd.other_val_pc	,0) <>	upd_rec.	other_val_pc	or
            nvl(fnd.other_qty_pc	,0) <>	upd_rec.	other_qty_pc 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_colrep_bc_sum_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             	cpy.	rep_id	,
            	cpy.	cust_billing_cycle	,
            	cpy.	assn_op_val_cc	,
            	cpy.	assn_op_qty_cc	,
            	cpy.	trf_in_val_cc	,
            	cpy.	trf_in_qty_cc	,
            	cpy.	trf_out_val_cc	,
            	cpy.	trf_out_qty_cc	,
            	cpy.	rollup_val_cc	,
            	cpy.	rollup_qty_cc	,
            	cpy.	reassn_in_val_cc	,
            	cpy.	reassn_in_qty_cc	,
            	cpy.	reassn_out_val_cc	,
            	cpy.	reassn_out_qty_cc	,
            	cpy.	debit_adj_val_cc	,
            	cpy.	debit_adj_qty_cc	,
            	cpy.	credit_adj_val_cc	,
            	cpy.	credit_adj_qty_cc	,
            	cpy.	adj_assign_val_cc	,
            	cpy.	adj_assn_qty_cc	,
            	cpy.	pymt_val_cc	,
            	cpy.	pymt_qty_cc	,
            	cpy.	end_assn_val_cc	,
            	cpy.	end_assn_qty_cc	,
            	cpy.	satsfd_val_cc	,
            	cpy.	satsfd_qty_cc	,
            	cpy.	aged_up_val_cc	,
            	cpy.	aged_up_qty_cc	,
            	cpy.	aged_back_val_cc	,
            	cpy.	aged_back_qty_cc	,
            	cpy.	statusquo_val_cc	,
            	cpy.	statusquo_qty_cc	,
            	cpy.	other_val_cc	,
            	cpy.	other_qty_cc	,
            	cpy.	assn_op_val_pc	,
            	cpy.	assn_op_qty_pc	,
            	cpy.	trf_in_val_pc	,
            	cpy.	trf_in_qty_pc	,
            	cpy.	trf_out_val_pc	,
            	cpy.	trf_out_qty_pc	,
            	cpy.	rollup_val_pc	,
            	cpy.	rollup_qty_pc	,
            	cpy.	reassn_in_val_pc	,
            	cpy.	reassn_in_qty_pc	,
            	cpy.	reassn_out_val_pc	,
            	cpy.	reassn_out_qty_pc	,
            	cpy.	debit_adj_val_pc	,
            	cpy.	debit_adj_qty_pc	,
            	cpy.	credit_adj_val_pc	,
            	cpy.	credit_adj_qty_pc	,
            	cpy.	adj_assign_val_pc	,
            	cpy.	adj_assn_qty_pc	,
            	cpy.	pymt_val_pc	,
            	cpy.	pymt_qty_pc	,
            	cpy.	end_assn_val_pc	,
            	cpy.	end_assn_qty_pc	,
            	cpy.	satsfd_val_pc	,
            	cpy.	satsfd_qty_pc	,
            	cpy.	aged_up_val_pc	,
            	cpy.	aged_up_qty_pc	,
            	cpy.	aged_back_val_pc	,
            	cpy.	aged_back_qty_pc	,
            	cpy.	statusquo_val_pc	,
            	cpy.	statusquo_qty_pc	,
            	cpy.	other_val_pc	,
            	cpy.	other_qty_pc	

      from   stg_vsn_colrep_bc_sum_cpy cpy
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
    from   stg_vsn_colrep_bc_sum_cpy
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
--    update stg_vsn_colrep_bc_sum_cpy
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
end wh_fnd_wfs_216u;
