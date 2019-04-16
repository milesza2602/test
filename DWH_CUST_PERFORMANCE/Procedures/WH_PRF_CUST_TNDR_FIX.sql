--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_TNDR_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_TNDR_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2015
--  Author:      Alastair de Wet
--  Purpose:     UPDATE BASKET TRANSACTIONS WITH CORRECT p_c_i
--  Tables:      Input  - CUST_BASKET_TENDER
--               Output - CUST_BASKET_TENDER
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
g_count              integer       :=  0;


   
g_date                date          := trunc(sysdate);
g_sdate               date          := '29 JUN 2015';
g_edate               date          := '01 JUL 2015';

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_ITEM_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE CORRECT PCI ON BASKET RECORDS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--************************************************************************************************** 
-- update pci on basket
--**************************************************************************************************
procedure update_basket as
begin
 
--******************************************************************************
g_recs_updated := 0;
 

     update /*+ FULL(cbi) parallel (cbi,4) */  CUST_BASKET_TENDER cbi
     set    primary_customer_identifier = 
             (select /*+ nl_aj */ LOYALTY_WW_SWIPE_NO
              from   CUST_BASKET
              where LOCATION_NO = CBI.LOCATION_NO AND
                    TILL_NO   = CBI.TILL_NO AND
                    TRAN_NO   = CBI.TRAN_NO AND
                    TRAN_DATE = CBI.TRAN_DATE)
     where  customer_no = 0 and
             primary_customer_identifier = 0 and
             tran_date  between g_sdate and g_edate and
             exists
             (select /*+ nl_aj */ LOYALTY_WW_SWIPE_NO
              from   CUST_BASKET
              where LOCATION_NO = CBI.LOCATION_NO AND
                    TILL_NO     = CBI.TILL_NO AND
                    TRAN_NO     = CBI.TRAN_NO AND
                    TRAN_DATE   = CBI.TRAN_DATE AND 
                    LOYALTY_WW_SWIPE_NO IS NOT NULL);   

       
       g_recs_updated := g_recs_updated + sql%rowcount;
       commit;
 
    l_text :=  'UPDATED WHERE PAC = 0 '||g_recs_updated;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
       
      
      
--******************************************************************************
  exception
      when dwh_errors.e_insert_error then
       l_message := 'BASKET - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'BASKET  - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end update_basket;



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

    
 
    l_text := 'UPDATE PCI ON BASKET STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

for g_count in 0 .. 150
loop
    
    l_text := 'UPDATE PCI dates '||g_sdate||' to '||g_edate||'  '||
    to_char(sysdate,('hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

    
    update_basket;
 
 
    
    g_edate := g_sdate - 1; 
    g_sdate := g_sdate - 7;

end loop;    


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
end wh_prf_cust_tndr_fix;
