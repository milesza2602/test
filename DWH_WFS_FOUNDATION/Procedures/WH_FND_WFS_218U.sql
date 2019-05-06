--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_218U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_218U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_colclas_bc_sum fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_colclas_bc_sum_cpy
--               Output - fnd_wfs_colclas_bc_sum
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

g_cta_class          stg_vsn_colclas_bc_sum_cpy.cta_class%type; 
g_cust_billing_cycle stg_vsn_colclas_bc_sum_cpy.cust_billing_cycle%type;  

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_218U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_COLCLAS_BC_SUM EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_colclas_bc_sum_cpy
where (
cta_class,cust_billing_cycle)
in
(select cta_class,cust_billing_cycle
from stg_vsn_colclas_bc_sum_cpy 
group by cta_class,cust_billing_cycle
having count(*) > 1) 
order by cta_class,cust_billing_cycle,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_colclas_bc_sum is
select /*+ FULL(stg)  parallel (stg,2) */  
             	cpy.	*
      from    stg_vsn_colclas_bc_sum_cpy cpy,
              fnd_wfs_colclas_bc_sum fnd
      where   cpy.cta_class             = fnd.cta_class    and
              cpy.cust_billing_cycle    = fnd.cust_billing_cycle and             
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.cta_class,
              cpy.cust_billing_cycle,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_cust_billing_cycle := 0; 
   g_cta_class          := ' '; 


for dupp_record in stg_dup
   loop

    if  dupp_record.cta_class            = g_cta_class and
        dupp_record.cust_billing_cycle   = g_cust_billing_cycle  then
        update stg_vsn_colclas_bc_sum_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_cust_billing_cycle    := dupp_record.cust_billing_cycle; 
    g_cta_class             := dupp_record.cta_class; 


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
 
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_classification fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	cta_class	,
             'Dummy wh_fnd_wfs_218u',
             g_date,
             1
      from   stg_vsn_colclas_bc_sum_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_classification
       where  class_code          = cpy.cta_class )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
    
      commit;       
       
--******************************************************************************

 
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_billing_cycle fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             CPY.	cust_billing_cycle	,
             'Dummy wh_fnd_wfs_218u',
             g_date,
             1
      from   stg_vsn_colclas_bc_sum_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_billing_cycle
       where  bill_cycle_code     = cpy.cust_billing_cycle )
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_colclas_bc_sum fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	cta_class	,
            	cpy.	cust_billing_cycle	,
            	cpy.	acc_op_val_mtd	,
            	cpy.	acc_op_qty_mtd	,
            	cpy.	acc_new_val_mtd	,
            	cpy.	acc_new_qty_mtd	,
            	cpy.	trf_in_a_val_mtd	,
            	cpy.	trf_in_a_qty_mtd	,
            	cpy.	trf_out_a_val_mtd	,
            	cpy.	trf_out_a_qty_mtd	,
            	cpy.	trf_in_m_val_mtd	,
            	cpy.	trf_in_m_qty_mtd	,
            	cpy.	trf_out_m_val_mtd	,
            	cpy.	trf_out_m_qty_mtd	,
            	cpy.	acc_satsfd_val_mtd	,
            	cpy.	acc_satsfd_qty_mtd	,
            	cpy.	acc_queue_val_mtd	,
            	cpy.	acc_queue_qty_mtd	,
            	cpy.	acc_work_val_mtd	,
            	cpy.	acc_work_qty_mtd	,
            	cpy.	acc_broke_val_mtd	,
            	cpy.	acc_broke_qty_mtd	,
            	cpy.	acc_pymt_val_mtd	,
            	cpy.	acc_pymt_qty_mtd	,
            	cpy.	acc_retn_val_mtd	,
            	cpy.	acc_retn_qty_mtd	,
            	cpy.	acc_pchs_val_mtd	,
            	cpy.	acc_pchs_qty_mtd	,
            	cpy.	acc_othcr_val_mtd	,
            	cpy.	acc_othcr_qty_mtd	,
            	cpy.	acc_gnlcr_val_mtd	,
            	cpy.	acc_gnlcr_qty_mtd	,
            	cpy.	acc_othdb_val_mtd	,
            	cpy.	acc_othdb_qty_mtd	,
            	cpy.	acc_gnldb_val_mtd	,
            	cpy.	acc_gnldb_qty_mtd	,
            	cpy.	finchg_val_mtd	,
            	cpy.	finchg_qty_mtd	,
            	cpy.	othchg_val_mtd	,
            	cpy.	othchg_qty_mtd	,
            	cpy.	acc_op_val_ctd	,
            	cpy.	acc_op_qty_ctd	,
            	cpy.	acc_new_val_ctd	,
            	cpy.	acc_new_qty_ctd	,
            	cpy.	trf_in_a_val_ctd	,
            	cpy.	trf_in_a_qty_ctd	,
            	cpy.	trf_out_a_val_ctd	,
            	cpy.	trf_out_qty_a_ctd	,
            	cpy.	trf_in_m_val_ctd	,
            	cpy.	trf_in_m_qty_ctd	,
            	cpy.	trf_out_m_val_ctd	,
            	cpy.	trf_out_m_qty_ctd	,
            	cpy.	acc_satsfd_val_ctd	,
            	cpy.	acc_satsfd_qty_ctd	,
            	cpy.	acc_queue_val_ctd	,
            	cpy.	acc_queue_qty_ctd	,
            	cpy.	acc_work_val_ctd	,
            	cpy.	acc_work_qty_ctd	,
            	cpy.	acc_broke_val_ctd	,
            	cpy.	acc_broke_qty_ctd	,
            	cpy.	acc_pymt_val_ctd	,
            	cpy.	acc_pymt_qty_ctd	,
            	cpy.	acc_retn_val_ctd	,
            	cpy.	acc_retn_qty_ctd	,
            	cpy.	acc_pchs_val_ctd	,
            	cpy.	acc_pchs_qty_ctd	,
            	cpy.	acc_othcr_val_ctd	,
            	cpy.	acc_othcr_qty_ctd	,
            	cpy.	acc_gnlcr_val_ctd	,
            	cpy.	acc_gnlcr_qty_ctd	,
            	cpy.	acc_othdb_val_ctd	,
            	cpy.	acc_othdb_qty_ctd	,
            	cpy.	acc_gnldb_val_ctd	,
            	cpy.	acc_gnldb_qty_ctd	,
            	cpy.	finchg_val_ctd	,
            	cpy.	finchg_qty_ctd	,
            	cpy.	othchg_val_ctd	,
            	cpy.	othchg_qty_ctd	,
            	cpy.	acc_op_val_pc	,
            	cpy.	acc_op_qty_pc	,
            	cpy.	acc_new_val_pc	,
            	cpy.	acc_new_qty_pc	,
            	cpy.	trf_in_a_val_pc	,
            	cpy.	trf_in_a_qty_pc	,
            	cpy.	trf_out_a_val_pc	,
            	cpy.	trf_out_qty_a_pc	,
            	cpy.	trf_in_m_val_pc	,
            	cpy.	trf_in_m_qty_pc	,
            	cpy.	trf_out_m_val_pc	,
            	cpy.	trf_out_m_qty_pc	,
            	cpy.	acc_satsfd_val_pc	,
            	cpy.	acc_satsfd_qty_pc	,
            	cpy.	acc_queue_val_pc	,
            	cpy.	acc_queue_qty_pc	,
            	cpy.	acc_work_val_pc	,
            	cpy.	acc_work_qty_pc	,
            	cpy.	acc_broke_val_pc	,
            	cpy.	acc_broke_qty_pc	,
            	cpy.	acc_pymt_val_pc	,
            	cpy.	acc_pymt_qty_pc	,
            	cpy.	acc_retn_val_pc	,
            	cpy.	acc_retn_qty_pc	,
            	cpy.	acc_pchs_val_pc	,
            	cpy.	acc_pchs_qty_pc	,
            	cpy.	acc_othcr_val_pc	,
            	cpy.	acc_othcr_qty_pc	,
            	cpy.	acc_gnlcr_val_pc	,
            	cpy.	acc_gnlcr_qty_pc	,
            	cpy.	acc_othdb_val_pc	,
            	cpy.	acc_othdb_qty_pc	,
            	cpy.	acc_gnldb_val_pc	,
            	cpy.	acc_gnldb_qty_pc	,
            	cpy.	finchg_val_pc	,
            	cpy.	finchg_qty_pc	,
            	cpy.	othchg_val_pc	,
            	cpy.	othchg_qty_pc	,
             g_date as last_updated_date
      from   stg_vsn_colclas_bc_sum_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_colclas_bc_sum 
       where  cust_billing_cycle  = cpy.cust_billing_cycle and
              cta_class              = cpy.cta_class )
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



for upd_rec in c_stg_vsn_colclas_bc_sum
   loop
     update fnd_wfs_colclas_bc_sum fnd 
     set    fnd.	acc_op_val_mtd	=	upd_rec.	acc_op_val_mtd	,
            fnd.	acc_op_qty_mtd	=	upd_rec.	acc_op_qty_mtd	,
            fnd.	acc_new_val_mtd	=	upd_rec.	acc_new_val_mtd	,
            fnd.	acc_new_qty_mtd	=	upd_rec.	acc_new_qty_mtd	,
            fnd.	trf_in_a_val_mtd	=	upd_rec.	trf_in_a_val_mtd	,
            fnd.	trf_in_a_qty_mtd	=	upd_rec.	trf_in_a_qty_mtd	,
            fnd.	trf_out_a_val_mtd	=	upd_rec.	trf_out_a_val_mtd	,
            fnd.	trf_out_a_qty_mtd	=	upd_rec.	trf_out_a_qty_mtd	,
            fnd.	trf_in_m_val_mtd	=	upd_rec.	trf_in_m_val_mtd	,
            fnd.	trf_in_m_qty_mtd	=	upd_rec.	trf_in_m_qty_mtd	,
            fnd.	trf_out_m_val_mtd	=	upd_rec.	trf_out_m_val_mtd	,
            fnd.	trf_out_m_qty_mtd	=	upd_rec.	trf_out_m_qty_mtd	,
            fnd.	acc_satsfd_val_mtd	=	upd_rec.	acc_satsfd_val_mtd	,
            fnd.	acc_satsfd_qty_mtd	=	upd_rec.	acc_satsfd_qty_mtd	,
            fnd.	acc_queue_val_mtd	=	upd_rec.	acc_queue_val_mtd	,
            fnd.	acc_queue_qty_mtd	=	upd_rec.	acc_queue_qty_mtd	,
            fnd.	acc_work_val_mtd	=	upd_rec.	acc_work_val_mtd	,
            fnd.	acc_work_qty_mtd	=	upd_rec.	acc_work_qty_mtd	,
            fnd.	acc_broke_val_mtd	=	upd_rec.	acc_broke_val_mtd	,
            fnd.	acc_broke_qty_mtd	=	upd_rec.	acc_broke_qty_mtd	,
            fnd.	acc_pymt_val_mtd	=	upd_rec.	acc_pymt_val_mtd	,
            fnd.	acc_pymt_qty_mtd	=	upd_rec.	acc_pymt_qty_mtd	,
            fnd.	acc_retn_val_mtd	=	upd_rec.	acc_retn_val_mtd	,
            fnd.	acc_retn_qty_mtd	=	upd_rec.	acc_retn_qty_mtd	,
            fnd.	acc_pchs_val_mtd	=	upd_rec.	acc_pchs_val_mtd	,
            fnd.	acc_pchs_qty_mtd	=	upd_rec.	acc_pchs_qty_mtd	,
            fnd.	acc_othcr_val_mtd	=	upd_rec.	acc_othcr_val_mtd	,
            fnd.	acc_othcr_qty_mtd	=	upd_rec.	acc_othcr_qty_mtd	,
            fnd.	acc_gnlcr_val_mtd	=	upd_rec.	acc_gnlcr_val_mtd	,
            fnd.	acc_gnlcr_qty_mtd	=	upd_rec.	acc_gnlcr_qty_mtd	,
            fnd.	acc_othdb_val_mtd	=	upd_rec.	acc_othdb_val_mtd	,
            fnd.	acc_othdb_qty_mtd	=	upd_rec.	acc_othdb_qty_mtd	,
            fnd.	acc_gnldb_val_mtd	=	upd_rec.	acc_gnldb_val_mtd	,
            fnd.	acc_gnldb_qty_mtd	=	upd_rec.	acc_gnldb_qty_mtd	,
            fnd.	finchg_val_mtd	=	upd_rec.	finchg_val_mtd	,
            fnd.	finchg_qty_mtd	=	upd_rec.	finchg_qty_mtd	,
            fnd.	othchg_val_mtd	=	upd_rec.	othchg_val_mtd	,
            fnd.	othchg_qty_mtd	=	upd_rec.	othchg_qty_mtd	,
            fnd.	acc_op_val_ctd	=	upd_rec.	acc_op_val_ctd	,
            fnd.	acc_op_qty_ctd	=	upd_rec.	acc_op_qty_ctd	,
            fnd.	acc_new_val_ctd	=	upd_rec.	acc_new_val_ctd	,
            fnd.	acc_new_qty_ctd	=	upd_rec.	acc_new_qty_ctd	,
            fnd.	trf_in_a_val_ctd	=	upd_rec.	trf_in_a_val_ctd	,
            fnd.	trf_in_a_qty_ctd	=	upd_rec.	trf_in_a_qty_ctd	,
            fnd.	trf_out_a_val_ctd	=	upd_rec.	trf_out_a_val_ctd	,
            fnd.	trf_out_qty_a_ctd	=	upd_rec.	trf_out_qty_a_ctd	,
            fnd.	trf_in_m_val_ctd	=	upd_rec.	trf_in_m_val_ctd	,
            fnd.	trf_in_m_qty_ctd	=	upd_rec.	trf_in_m_qty_ctd	,
            fnd.	trf_out_m_val_ctd	=	upd_rec.	trf_out_m_val_ctd	,
            fnd.	trf_out_m_qty_ctd	=	upd_rec.	trf_out_m_qty_ctd	,
            fnd.	acc_satsfd_val_ctd	=	upd_rec.	acc_satsfd_val_ctd	,
            fnd.	acc_satsfd_qty_ctd	=	upd_rec.	acc_satsfd_qty_ctd	,
            fnd.	acc_queue_val_ctd	=	upd_rec.	acc_queue_val_ctd	,
            fnd.	acc_queue_qty_ctd	=	upd_rec.	acc_queue_qty_ctd	,
            fnd.	acc_work_val_ctd	=	upd_rec.	acc_work_val_ctd	,
            fnd.	acc_work_qty_ctd	=	upd_rec.	acc_work_qty_ctd	,
            fnd.	acc_broke_val_ctd	=	upd_rec.	acc_broke_val_ctd	,
            fnd.	acc_broke_qty_ctd	=	upd_rec.	acc_broke_qty_ctd	,
            fnd.	acc_pymt_val_ctd	=	upd_rec.	acc_pymt_val_ctd	,
            fnd.	acc_pymt_qty_ctd	=	upd_rec.	acc_pymt_qty_ctd	,
            fnd.	acc_retn_val_ctd	=	upd_rec.	acc_retn_val_ctd	,
            fnd.	acc_retn_qty_ctd	=	upd_rec.	acc_retn_qty_ctd	,
            fnd.	acc_pchs_val_ctd	=	upd_rec.	acc_pchs_val_ctd	,
            fnd.	acc_pchs_qty_ctd	=	upd_rec.	acc_pchs_qty_ctd	,
            fnd.	acc_othcr_val_ctd	=	upd_rec.	acc_othcr_val_ctd	,
            fnd.	acc_othcr_qty_ctd	=	upd_rec.	acc_othcr_qty_ctd	,
            fnd.	acc_gnlcr_val_ctd	=	upd_rec.	acc_gnlcr_val_ctd	,
            fnd.	acc_gnlcr_qty_ctd	=	upd_rec.	acc_gnlcr_qty_ctd	,
            fnd.	acc_othdb_val_ctd	=	upd_rec.	acc_othdb_val_ctd	,
            fnd.	acc_othdb_qty_ctd	=	upd_rec.	acc_othdb_qty_ctd	,
            fnd.	acc_gnldb_val_ctd	=	upd_rec.	acc_gnldb_val_ctd	,
            fnd.	acc_gnldb_qty_ctd	=	upd_rec.	acc_gnldb_qty_ctd	,
            fnd.	finchg_val_ctd	=	upd_rec.	finchg_val_ctd	,
            fnd.	finchg_qty_ctd	=	upd_rec.	finchg_qty_ctd	,
            fnd.	othchg_val_ctd	=	upd_rec.	othchg_val_ctd	,
            fnd.	othchg_qty_ctd	=	upd_rec.	othchg_qty_ctd	,
            fnd.	acc_op_val_pc	=	upd_rec.	acc_op_val_pc	,
            fnd.	acc_op_qty_pc	=	upd_rec.	acc_op_qty_pc	,
            fnd.	acc_new_val_pc	=	upd_rec.	acc_new_val_pc	,
            fnd.	acc_new_qty_pc	=	upd_rec.	acc_new_qty_pc	,
            fnd.	trf_in_a_val_pc	=	upd_rec.	trf_in_a_val_pc	,
            fnd.	trf_in_a_qty_pc	=	upd_rec.	trf_in_a_qty_pc	,
            fnd.	trf_out_a_val_pc	=	upd_rec.	trf_out_a_val_pc	,
            fnd.	trf_out_qty_a_pc	=	upd_rec.	trf_out_qty_a_pc	,
            fnd.	trf_in_m_val_pc	=	upd_rec.	trf_in_m_val_pc	,
            fnd.	trf_in_m_qty_pc	=	upd_rec.	trf_in_m_qty_pc	,
            fnd.	trf_out_m_val_pc	=	upd_rec.	trf_out_m_val_pc	,
            fnd.	trf_out_m_qty_pc	=	upd_rec.	trf_out_m_qty_pc	,
            fnd.	acc_satsfd_val_pc	=	upd_rec.	acc_satsfd_val_pc	,
            fnd.	acc_satsfd_qty_pc	=	upd_rec.	acc_satsfd_qty_pc	,
            fnd.	acc_queue_val_pc	=	upd_rec.	acc_queue_val_pc	,
            fnd.	acc_queue_qty_pc	=	upd_rec.	acc_queue_qty_pc	,
            fnd.	acc_work_val_pc	=	upd_rec.	acc_work_val_pc	,
            fnd.	acc_work_qty_pc	=	upd_rec.	acc_work_qty_pc	,
            fnd.	acc_broke_val_pc	=	upd_rec.	acc_broke_val_pc	,
            fnd.	acc_broke_qty_pc	=	upd_rec.	acc_broke_qty_pc	,
            fnd.	acc_pymt_val_pc	=	upd_rec.	acc_pymt_val_pc	,
            fnd.	acc_pymt_qty_pc	=	upd_rec.	acc_pymt_qty_pc	,
            fnd.	acc_retn_val_pc	=	upd_rec.	acc_retn_val_pc	,
            fnd.	acc_retn_qty_pc	=	upd_rec.	acc_retn_qty_pc	,
            fnd.	acc_pchs_val_pc	=	upd_rec.	acc_pchs_val_pc	,
            fnd.	acc_pchs_qty_pc	=	upd_rec.	acc_pchs_qty_pc	,
            fnd.	acc_othcr_val_pc	=	upd_rec.	acc_othcr_val_pc	,
            fnd.	acc_othcr_qty_pc	=	upd_rec.	acc_othcr_qty_pc	,
            fnd.	acc_gnlcr_val_pc	=	upd_rec.	acc_gnlcr_val_pc	,
            fnd.	acc_gnlcr_qty_pc	=	upd_rec.	acc_gnlcr_qty_pc	,
            fnd.	acc_othdb_val_pc	=	upd_rec.	acc_othdb_val_pc	,
            fnd.	acc_othdb_qty_pc	=	upd_rec.	acc_othdb_qty_pc	,
            fnd.	acc_gnldb_val_pc	=	upd_rec.	acc_gnldb_val_pc	,
            fnd.	acc_gnldb_qty_pc	=	upd_rec.	acc_gnldb_qty_pc	,
            fnd.	finchg_val_pc	=	upd_rec.	finchg_val_pc	,
            fnd.	finchg_qty_pc	=	upd_rec.	finchg_qty_pc	,
            fnd.	othchg_val_pc	=	upd_rec.	othchg_val_pc	,
            fnd.	othchg_qty_pc	=	upd_rec.	othchg_qty_pc	,
            fnd.  last_updated_date = g_date
     where  fnd.	cta_class	          =	upd_rec.	cta_class          and
            fnd.	cust_billing_cycle  	=	upd_rec.	cust_billing_cycle and
            ( 
            nvl(fnd.acc_op_val_mtd	,0) <>	upd_rec.	acc_op_val_mtd	or
            nvl(fnd.acc_op_qty_mtd	,0) <>	upd_rec.	acc_op_qty_mtd	or
            nvl(fnd.acc_new_val_mtd	,0) <>	upd_rec.	acc_new_val_mtd	or
            nvl(fnd.acc_new_qty_mtd	,0) <>	upd_rec.	acc_new_qty_mtd	or
            nvl(fnd.trf_in_a_val_mtd	,0) <>	upd_rec.	trf_in_a_val_mtd	or
            nvl(fnd.trf_in_a_qty_mtd	,0) <>	upd_rec.	trf_in_a_qty_mtd	or
            nvl(fnd.trf_out_a_val_mtd	,0) <>	upd_rec.	trf_out_a_val_mtd	or
            nvl(fnd.trf_out_a_qty_mtd	,0) <>	upd_rec.	trf_out_a_qty_mtd	or
            nvl(fnd.trf_in_m_val_mtd	,0) <>	upd_rec.	trf_in_m_val_mtd	or
            nvl(fnd.trf_in_m_qty_mtd	,0) <>	upd_rec.	trf_in_m_qty_mtd	or
            nvl(fnd.trf_out_m_val_mtd	,0) <>	upd_rec.	trf_out_m_val_mtd	or
            nvl(fnd.trf_out_m_qty_mtd	,0) <>	upd_rec.	trf_out_m_qty_mtd	or
            nvl(fnd.acc_satsfd_val_mtd	,0) <>	upd_rec.	acc_satsfd_val_mtd	or
            nvl(fnd.acc_satsfd_qty_mtd	,0) <>	upd_rec.	acc_satsfd_qty_mtd	or
            nvl(fnd.acc_queue_val_mtd	,0) <>	upd_rec.	acc_queue_val_mtd	or
            nvl(fnd.acc_queue_qty_mtd	,0) <>	upd_rec.	acc_queue_qty_mtd	or
            nvl(fnd.acc_work_val_mtd	,0) <>	upd_rec.	acc_work_val_mtd	or
            nvl(fnd.acc_work_qty_mtd	,0) <>	upd_rec.	acc_work_qty_mtd	or
            nvl(fnd.acc_broke_val_mtd	,0) <>	upd_rec.	acc_broke_val_mtd	or
            nvl(fnd.acc_broke_qty_mtd	,0) <>	upd_rec.	acc_broke_qty_mtd	or
            nvl(fnd.acc_pymt_val_mtd	,0) <>	upd_rec.	acc_pymt_val_mtd	or
            nvl(fnd.acc_pymt_qty_mtd	,0) <>	upd_rec.	acc_pymt_qty_mtd	or
            nvl(fnd.acc_retn_val_mtd	,0) <>	upd_rec.	acc_retn_val_mtd	or
            nvl(fnd.acc_retn_qty_mtd	,0) <>	upd_rec.	acc_retn_qty_mtd	or
            nvl(fnd.acc_pchs_val_mtd	,0) <>	upd_rec.	acc_pchs_val_mtd	or
            nvl(fnd.acc_pchs_qty_mtd	,0) <>	upd_rec.	acc_pchs_qty_mtd	or
            nvl(fnd.acc_othcr_val_mtd	,0) <>	upd_rec.	acc_othcr_val_mtd	or
            nvl(fnd.acc_othcr_qty_mtd	,0) <>	upd_rec.	acc_othcr_qty_mtd	or
            nvl(fnd.acc_gnlcr_val_mtd	,0) <>	upd_rec.	acc_gnlcr_val_mtd	or
            nvl(fnd.acc_gnlcr_qty_mtd	,0) <>	upd_rec.	acc_gnlcr_qty_mtd	or
            nvl(fnd.acc_othdb_val_mtd	,0) <>	upd_rec.	acc_othdb_val_mtd	or
            nvl(fnd.acc_othdb_qty_mtd	,0) <>	upd_rec.	acc_othdb_qty_mtd	or
            nvl(fnd.acc_gnldb_val_mtd	,0) <>	upd_rec.	acc_gnldb_val_mtd	or
            nvl(fnd.acc_gnldb_qty_mtd	,0) <>	upd_rec.	acc_gnldb_qty_mtd	or
            nvl(fnd.finchg_val_mtd	,0) <>	upd_rec.	finchg_val_mtd	or
            nvl(fnd.finchg_qty_mtd	,0) <>	upd_rec.	finchg_qty_mtd	or
            nvl(fnd.othchg_val_mtd	,0) <>	upd_rec.	othchg_val_mtd	or
            nvl(fnd.othchg_qty_mtd	,0) <>	upd_rec.	othchg_qty_mtd	or
            nvl(fnd.acc_op_val_ctd	,0) <>	upd_rec.	acc_op_val_ctd	or
            nvl(fnd.acc_op_qty_ctd	,0) <>	upd_rec.	acc_op_qty_ctd	or
            nvl(fnd.acc_new_val_ctd	,0) <>	upd_rec.	acc_new_val_ctd	or
            nvl(fnd.acc_new_qty_ctd	,0) <>	upd_rec.	acc_new_qty_ctd	or
            nvl(fnd.trf_in_a_val_ctd	,0) <>	upd_rec.	trf_in_a_val_ctd	or
            nvl(fnd.trf_in_a_qty_ctd	,0) <>	upd_rec.	trf_in_a_qty_ctd	or
            nvl(fnd.trf_out_a_val_ctd	,0) <>	upd_rec.	trf_out_a_val_ctd	or
            nvl(fnd.trf_out_qty_a_ctd	,0) <>	upd_rec.	trf_out_qty_a_ctd	or
            nvl(fnd.trf_in_m_val_ctd	,0) <>	upd_rec.	trf_in_m_val_ctd	or
            nvl(fnd.trf_in_m_qty_ctd	,0) <>	upd_rec.	trf_in_m_qty_ctd	or
            nvl(fnd.trf_out_m_val_ctd	,0) <>	upd_rec.	trf_out_m_val_ctd	or
            nvl(fnd.trf_out_m_qty_ctd	,0) <>	upd_rec.	trf_out_m_qty_ctd	or
            nvl(fnd.acc_satsfd_val_ctd	,0) <>	upd_rec.	acc_satsfd_val_ctd	or
            nvl(fnd.acc_satsfd_qty_ctd	,0) <>	upd_rec.	acc_satsfd_qty_ctd	or
            nvl(fnd.acc_queue_val_ctd	,0) <>	upd_rec.	acc_queue_val_ctd	or
            nvl(fnd.acc_queue_qty_ctd	,0) <>	upd_rec.	acc_queue_qty_ctd	or
            nvl(fnd.acc_work_val_ctd	,0) <>	upd_rec.	acc_work_val_ctd	or
            nvl(fnd.acc_work_qty_ctd	,0) <>	upd_rec.	acc_work_qty_ctd	or
            nvl(fnd.acc_broke_val_ctd	,0) <>	upd_rec.	acc_broke_val_ctd	or
            nvl(fnd.acc_broke_qty_ctd	,0) <>	upd_rec.	acc_broke_qty_ctd	or
            nvl(fnd.acc_pymt_val_ctd	,0) <>	upd_rec.	acc_pymt_val_ctd	or
            nvl(fnd.acc_pymt_qty_ctd	,0) <>	upd_rec.	acc_pymt_qty_ctd	or
            nvl(fnd.acc_retn_val_ctd	,0) <>	upd_rec.	acc_retn_val_ctd	or
            nvl(fnd.acc_retn_qty_ctd	,0) <>	upd_rec.	acc_retn_qty_ctd	or
            nvl(fnd.acc_pchs_val_ctd	,0) <>	upd_rec.	acc_pchs_val_ctd	or
            nvl(fnd.acc_pchs_qty_ctd	,0) <>	upd_rec.	acc_pchs_qty_ctd	or
            nvl(fnd.acc_othcr_val_ctd	,0) <>	upd_rec.	acc_othcr_val_ctd	or
            nvl(fnd.acc_othcr_qty_ctd	,0) <>	upd_rec.	acc_othcr_qty_ctd	or
            nvl(fnd.acc_gnlcr_val_ctd	,0) <>	upd_rec.	acc_gnlcr_val_ctd	or
            nvl(fnd.acc_gnlcr_qty_ctd	,0) <>	upd_rec.	acc_gnlcr_qty_ctd	or
            nvl(fnd.acc_othdb_val_ctd	,0) <>	upd_rec.	acc_othdb_val_ctd	or
            nvl(fnd.acc_othdb_qty_ctd	,0) <>	upd_rec.	acc_othdb_qty_ctd	or
            nvl(fnd.acc_gnldb_val_ctd	,0) <>	upd_rec.	acc_gnldb_val_ctd	or
            nvl(fnd.acc_gnldb_qty_ctd	,0) <>	upd_rec.	acc_gnldb_qty_ctd	or
            nvl(fnd.finchg_val_ctd	,0) <>	upd_rec.	finchg_val_ctd	or
            nvl(fnd.finchg_qty_ctd	,0) <>	upd_rec.	finchg_qty_ctd	or
            nvl(fnd.othchg_val_ctd	,0) <>	upd_rec.	othchg_val_ctd	or
            nvl(fnd.othchg_qty_ctd	,0) <>	upd_rec.	othchg_qty_ctd	or
            nvl(fnd.acc_op_val_pc	,0) <>	upd_rec.	acc_op_val_pc	or
            nvl(fnd.acc_op_qty_pc	,0) <>	upd_rec.	acc_op_qty_pc	or
            nvl(fnd.acc_new_val_pc	,0) <>	upd_rec.	acc_new_val_pc	or
            nvl(fnd.acc_new_qty_pc	,0) <>	upd_rec.	acc_new_qty_pc	or
            nvl(fnd.trf_in_a_val_pc	,0) <>	upd_rec.	trf_in_a_val_pc	or
            nvl(fnd.trf_in_a_qty_pc	,0) <>	upd_rec.	trf_in_a_qty_pc	or
            nvl(fnd.trf_out_a_val_pc	,0) <>	upd_rec.	trf_out_a_val_pc	or
            nvl(fnd.trf_out_qty_a_pc	,0) <>	upd_rec.	trf_out_qty_a_pc	or
            nvl(fnd.trf_in_m_val_pc	,0) <>	upd_rec.	trf_in_m_val_pc	or
            nvl(fnd.trf_in_m_qty_pc	,0) <>	upd_rec.	trf_in_m_qty_pc	or
            nvl(fnd.trf_out_m_val_pc	,0) <>	upd_rec.	trf_out_m_val_pc	or
            nvl(fnd.trf_out_m_qty_pc	,0) <>	upd_rec.	trf_out_m_qty_pc	or
            nvl(fnd.acc_satsfd_val_pc	,0) <>	upd_rec.	acc_satsfd_val_pc	or
            nvl(fnd.acc_satsfd_qty_pc	,0) <>	upd_rec.	acc_satsfd_qty_pc	or
            nvl(fnd.acc_queue_val_pc	,0) <>	upd_rec.	acc_queue_val_pc	or
            nvl(fnd.acc_queue_qty_pc	,0) <>	upd_rec.	acc_queue_qty_pc	or
            nvl(fnd.acc_work_val_pc	,0) <>	upd_rec.	acc_work_val_pc	or
            nvl(fnd.acc_work_qty_pc	,0) <>	upd_rec.	acc_work_qty_pc	or
            nvl(fnd.acc_broke_val_pc	,0) <>	upd_rec.	acc_broke_val_pc	or
            nvl(fnd.acc_broke_qty_pc	,0) <>	upd_rec.	acc_broke_qty_pc	or
            nvl(fnd.acc_pymt_val_pc	,0) <>	upd_rec.	acc_pymt_val_pc	or
            nvl(fnd.acc_pymt_qty_pc	,0) <>	upd_rec.	acc_pymt_qty_pc	or
            nvl(fnd.acc_retn_val_pc	,0) <>	upd_rec.	acc_retn_val_pc	or
            nvl(fnd.acc_retn_qty_pc	,0) <>	upd_rec.	acc_retn_qty_pc	or
            nvl(fnd.acc_pchs_val_pc	,0) <>	upd_rec.	acc_pchs_val_pc	or
            nvl(fnd.acc_pchs_qty_pc	,0) <>	upd_rec.	acc_pchs_qty_pc	or
            nvl(fnd.acc_othcr_val_pc	,0) <>	upd_rec.	acc_othcr_val_pc	or
            nvl(fnd.acc_othcr_qty_pc	,0) <>	upd_rec.	acc_othcr_qty_pc	or
            nvl(fnd.acc_gnlcr_val_pc	,0) <>	upd_rec.	acc_gnlcr_val_pc	or
            nvl(fnd.acc_gnlcr_qty_pc	,0) <>	upd_rec.	acc_gnlcr_qty_pc	or
            nvl(fnd.acc_othdb_val_pc	,0) <>	upd_rec.	acc_othdb_val_pc	or
            nvl(fnd.acc_othdb_qty_pc	,0) <>	upd_rec.	acc_othdb_qty_pc	or
            nvl(fnd.acc_gnldb_val_pc	,0) <>	upd_rec.	acc_gnldb_val_pc	or
            nvl(fnd.acc_gnldb_qty_pc	,0) <>	upd_rec.	acc_gnldb_qty_pc	or
            nvl(fnd.finchg_val_pc	,0) <>	upd_rec.	finchg_val_pc	or
            nvl(fnd.finchg_qty_pc	,0) <>	upd_rec.	finchg_qty_pc	or
            nvl(fnd.othchg_val_pc	,0) <>	upd_rec.	othchg_val_pc	or
            nvl(fnd.othchg_qty_pc	,0) <>	upd_rec.	othchg_qty_pc	

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_colclas_bc_sum_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
             	cpy.	cta_class	,
            	cpy.	cust_billing_cycle	,
            	cpy.	acc_op_val_mtd	,
            	cpy.	acc_op_qty_mtd	,
            	cpy.	acc_new_val_mtd	,
            	cpy.	acc_new_qty_mtd	,
            	cpy.	trf_in_a_val_mtd	,
            	cpy.	trf_in_a_qty_mtd	,
            	cpy.	trf_out_a_val_mtd	,
            	cpy.	trf_out_a_qty_mtd	,
            	cpy.	trf_in_m_val_mtd	,
            	cpy.	trf_in_m_qty_mtd	,
            	cpy.	trf_out_m_val_mtd	,
            	cpy.	trf_out_m_qty_mtd	,
            	cpy.	acc_satsfd_val_mtd	,
            	cpy.	acc_satsfd_qty_mtd	,
            	cpy.	acc_queue_val_mtd	,
            	cpy.	acc_queue_qty_mtd	,
            	cpy.	acc_work_val_mtd	,
            	cpy.	acc_work_qty_mtd	,
            	cpy.	acc_broke_val_mtd	,
            	cpy.	acc_broke_qty_mtd	,
            	cpy.	acc_pymt_val_mtd	,
            	cpy.	acc_pymt_qty_mtd	,
            	cpy.	acc_retn_val_mtd	,
            	cpy.	acc_retn_qty_mtd	,
            	cpy.	acc_pchs_val_mtd	,
            	cpy.	acc_pchs_qty_mtd	,
            	cpy.	acc_othcr_val_mtd	,
            	cpy.	acc_othcr_qty_mtd	,
            	cpy.	acc_gnlcr_val_mtd	,
            	cpy.	acc_gnlcr_qty_mtd	,
            	cpy.	acc_othdb_val_mtd	,
            	cpy.	acc_othdb_qty_mtd	,
            	cpy.	acc_gnldb_val_mtd	,
            	cpy.	acc_gnldb_qty_mtd	,
            	cpy.	finchg_val_mtd	,
            	cpy.	finchg_qty_mtd	,
            	cpy.	othchg_val_mtd	,
            	cpy.	othchg_qty_mtd	,
            	cpy.	acc_op_val_ctd	,
            	cpy.	acc_op_qty_ctd	,
            	cpy.	acc_new_val_ctd	,
            	cpy.	acc_new_qty_ctd	,
            	cpy.	trf_in_a_val_ctd	,
            	cpy.	trf_in_a_qty_ctd	,
            	cpy.	trf_out_a_val_ctd	,
            	cpy.	trf_out_qty_a_ctd	,
            	cpy.	trf_in_m_val_ctd	,
            	cpy.	trf_in_m_qty_ctd	,
            	cpy.	trf_out_m_val_ctd	,
            	cpy.	trf_out_m_qty_ctd	,
            	cpy.	acc_satsfd_val_ctd	,
            	cpy.	acc_satsfd_qty_ctd	,
            	cpy.	acc_queue_val_ctd	,
            	cpy.	acc_queue_qty_ctd	,
            	cpy.	acc_work_val_ctd	,
            	cpy.	acc_work_qty_ctd	,
            	cpy.	acc_broke_val_ctd	,
            	cpy.	acc_broke_qty_ctd	,
            	cpy.	acc_pymt_val_ctd	,
            	cpy.	acc_pymt_qty_ctd	,
            	cpy.	acc_retn_val_ctd	,
            	cpy.	acc_retn_qty_ctd	,
            	cpy.	acc_pchs_val_ctd	,
            	cpy.	acc_pchs_qty_ctd	,
            	cpy.	acc_othcr_val_ctd	,
            	cpy.	acc_othcr_qty_ctd	,
            	cpy.	acc_gnlcr_val_ctd	,
            	cpy.	acc_gnlcr_qty_ctd	,
            	cpy.	acc_othdb_val_ctd	,
            	cpy.	acc_othdb_qty_ctd	,
            	cpy.	acc_gnldb_val_ctd	,
            	cpy.	acc_gnldb_qty_ctd	,
            	cpy.	finchg_val_ctd	,
            	cpy.	finchg_qty_ctd	,
            	cpy.	othchg_val_ctd	,
            	cpy.	othchg_qty_ctd	,
            	cpy.	acc_op_val_pc	,
            	cpy.	acc_op_qty_pc	,
            	cpy.	acc_new_val_pc	,
            	cpy.	acc_new_qty_pc	,
            	cpy.	trf_in_a_val_pc	,
            	cpy.	trf_in_a_qty_pc	,
            	cpy.	trf_out_a_val_pc	,
            	cpy.	trf_out_qty_a_pc	,
            	cpy.	trf_in_m_val_pc	,
            	cpy.	trf_in_m_qty_pc	,
            	cpy.	trf_out_m_val_pc	,
            	cpy.	trf_out_m_qty_pc	,
            	cpy.	acc_satsfd_val_pc	,
            	cpy.	acc_satsfd_qty_pc	,
            	cpy.	acc_queue_val_pc	,
            	cpy.	acc_queue_qty_pc	,
            	cpy.	acc_work_val_pc	,
            	cpy.	acc_work_qty_pc	,
            	cpy.	acc_broke_val_pc	,
            	cpy.	acc_broke_qty_pc	,
            	cpy.	acc_pymt_val_pc	,
            	cpy.	acc_pymt_qty_pc	,
            	cpy.	acc_retn_val_pc	,
            	cpy.	acc_retn_qty_pc	,
            	cpy.	acc_pchs_val_pc	,
            	cpy.	acc_pchs_qty_pc	,
            	cpy.	acc_othcr_val_pc	,
            	cpy.	acc_othcr_qty_pc	,
            	cpy.	acc_gnlcr_val_pc	,
            	cpy.	acc_gnlcr_qty_pc	,
            	cpy.	acc_othdb_val_pc	,
            	cpy.	acc_othdb_qty_pc	,
            	cpy.	acc_gnldb_val_pc	,
            	cpy.	acc_gnldb_qty_pc	,
            	cpy.	finchg_val_pc	,
            	cpy.	finchg_qty_pc	,
            	cpy.	othchg_val_pc	,
            	cpy.	othchg_qty_pc	 
      from   stg_vsn_colclas_bc_sum_cpy cpy
      where  
      ( 
        1 = 
        (select dummy_ind from  fnd_wfs_billing_cycle
         where  cpy.cust_billing_cycle       = bill_cycle_code )  or
        1 = 
        (select dummy_ind from  fnd_wfs_classification
         where  cpy.cta_class       = class_code ) 
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
    from   stg_vsn_colclas_bc_sum_cpy
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
--    update stg_vsn_colclas_bc_sum_cpy
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
end wh_fnd_wfs_218u;
