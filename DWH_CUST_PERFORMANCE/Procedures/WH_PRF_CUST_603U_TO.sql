--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_603U_TO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_603U_TO" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        JUN 2017
--  Author:      Alastair de Wet
--  Purpose:    CREATE CUSTOMER SVOC CAMPAIGN FEEDBACK ROLLUP SUMMARY  
--  Tables:      Input  - cust_svoc_campaign_feedback  
--               Output - cust_svoc_campaign_fb_summ
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:  
--  
--  Naming conventions:
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
g_recs_last_read     integer       :=  0;
g_recs_last_inserted integer       :=  0;
g_recs_hospital      integer       :=  0;

 
g_date               date          := trunc(sysdate);


 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_603U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP  TO cust_svoc_campaign_fb_summ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'ROLLUP OF cust_svoc_campaign_fb_summ  '||
    to_char(sysdate,('dd/MON/yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
   l_text      := 'Start of Merge to ROLLUP cust_svoc_campaign_fb_summ ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
   
execute immediate 'alter session enable parallel dml';   


--************************************************************************************************** 
-- merge-- 
--**************************************************************************************************

   MERGE /*+ parallel (prf,4) */ INTO cust_svoc_campaign_fb_summ prf USING
   (
    SELECT  /*+ parallel (fb,4) */
            max(ACCOUNT_ID) ACCOUNT_ID,
            max(OYB_ACCOUNT_ID) OYB_ACCOUNT_ID,
            max(LIST_ID) LIST_ID,
            min(EVENT_DATE) EVENT_DATE,
            EMAIL_NAME,
            EMAIL_SUBJECT
    FROM   dwh_cust_performance.cust_svoc_campaign_feedback fb 
--    WHERE last_updated_date = g_date
    group by EMAIL_NAME,EMAIL_SUBJECT
   ) MER_REC
    ON    (  prf.	EMAIL_SUBJECT	      =	mer_rec.	EMAIL_SUBJECT and
             prf.	EMAIL_NAME	        =	mer_rec.	EMAIL_NAME)
            
   WHEN MATCHED THEN 
   UPDATE SET
            prf.	ACCOUNT_ID	        =	mer_rec.	ACCOUNT_ID	,
            prf.	OYB_ACCOUNT_ID	    =	mer_rec.	OYB_ACCOUNT_ID	,
            prf.	LIST_ID	            =	mer_rec.	LIST_ID	,
            prf.	EVENT_DATE	        =	mer_rec.	EVENT_DATE	,
            prf.  last_updated_date   = g_date
   WHEN NOT MATCHED THEN
   INSERT
          ( 
          ACCOUNT_ID,
          OYB_ACCOUNT_ID,
          LIST_ID,
          EVENT_DATE,
          EMAIL_NAME,
          EMAIL_SUBJECT,
          LAST_UPDATED_DATE
          )
  values
          (         
          MER_REC.ACCOUNT_ID,
          MER_REC.OYB_ACCOUNT_ID,
          MER_REC.LIST_ID,
          MER_REC.EVENT_DATE,
          MER_REC.EMAIL_NAME,
          MER_REC.EMAIL_SUBJECT,
          G_DATE
          );  


g_recs_read:=g_recs_read+SQL%ROWCOUNT;

commit;




       
l_text :=  'RECORDS WRITTEN TO prf '||g_recs_last_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true; 
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;       
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END WH_PRF_CUST_603U_TO;
