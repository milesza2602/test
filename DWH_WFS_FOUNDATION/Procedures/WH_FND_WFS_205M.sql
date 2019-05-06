--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_205M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_205M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     uPDATE fnd_wfs_cust_perf_60dy in the foundation layer
--               with input ex staging table from Vision COLLECTION.
--  Tables:      Input  - stg_vsn_cust_collect_cpy
--               Output - fnd_wfs_cust_perf_60dy                                                                      
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


   
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g05_day              integer       := TO_CHAR(current_date,'DD');

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_205M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_CUST_COLLECT EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- Update all record flaged as 'N' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin
 
   MERGE  INTO fnd_wfs_cust_perf_60dy  fnd 
   USING (
         select /*+ FULL(cpy)  parallel (4) */  
          cpy.*,
          account_status, 
          block_code_1, 
          billing_cycle, 
          cycle_due,
          chgoff_date,
          (case when cpy.pending_chgoff_ind = 'Y' and g05_day between 10 and 16 and perf.billing_cycle < g05_day
                then      'N'
                else      perf.delinquency_cycle   
          end)  delinquency_cycle,
          (case when cpy.pending_chgoff_ind = 'Y'
                then      'P'
                when cpy.pending_chgoff_ind = 'Y' and PERF.block_code_1  = 'S'
                then      'S'
                when cpy.pending_chgoff_ind = 'Y' and PERF.block_code_1  in ( 'A','I','D','J')
                then      'A'
                else      '0'   
           end)  pend_chgoff_code
   from   stg_vsn_cust_collect_cpy cpy,
          fnd_wfs_cust_perf_60dy perf
   where  g_run_date           = perf.run_date and
          cpy.product_code_no  = perf.product_code_no and
          cpy.wfs_customer_no  = perf.wfs_customer_no and
          cpy.wfs_account_no   = perf.wfs_account_no  and
          cpy.sys_process_code      = 'N'  
         ) mer_rec
   ON    (  fnd.	wfs_customer_no	      =	mer_rec.	wfs_customer_no and
            fnd.	run_date	            =	g_run_date	and
            fnd.	wfs_account_no	      =	mer_rec.	wfs_account_no	and
            fnd.	product_code_no	      =	mer_rec.	product_code_no)
   WHEN MATCHED THEN 
   UPDATE SET
            fnd.	cta_class     	    =	mer_rec.	cta_class	,
            fnd.	collector_id       	=	mer_rec.	wfs_rep_id  	,
            fnd.	pending_chgoff_ind  =	mer_rec.	pending_chgoff_ind 	,
            fnd.	pend_chgoff_code	  =	mer_rec.	pend_chgoff_code	,
            fnd.	delinquency_cycle	  =	mer_rec.	delinquency_cycle	,
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
    
    g_run_date :=  g_date + 1;

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************


    
    select count(*)
    into   g_recs_read
    from   stg_vsn_cust_collect_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

 
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
end wh_fnd_wfs_205M;
