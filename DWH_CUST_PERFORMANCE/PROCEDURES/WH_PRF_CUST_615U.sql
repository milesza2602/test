--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_615U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_615U" (p_forall_limit in integer,p_success out boolean) AS 

-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-01-25
-- Author: Paul Wakefield
-- 1. Added parallel hint to select from fnd_customer_mapping
-- 
-- **************************************************************************************************

--**************************************************************************************************
--  Date:        JUN 2017
--  Author:      Alastair de Wet
--  Purpose:    CREATE CUSTOMER SVOC MAPPING EX FOUNDATION TABLE  
--  Tables:      Input  - fnd_customer_mapping  
--               Output - dim_customer_mapping 
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_615U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DIM_CUSTOMER_MAPPING EX FND TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF DIM_CUSTOMER_MAPPING EX FOUNDATION STARTED AT '||
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
    
    l_text      := 'Start of Merge to LOAD customer SVOC mapping ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
   
    execute immediate 'alter session enable parallel dml';   


--************************************************************************************************** 
-- -- 
--**************************************************************************************************

   MERGE INTO dim_customer_mapping DIM USING
   (
    SELECT /*+ parallel(8) */ *
    FROM   dwh_cust_foundation.fnd_customer_mapping 
    WHERE last_updated_date = g_date
   ) MER_REC
    ON    (  dim.subscriber_key	=   mer_rec.subscriber_key and
             dim.source_key     =	mer_rec.source_key and
             dim.source         =   mer_rec.source)
   WHEN MATCHED THEN 
   UPDATE SET dim.last_updated_date =   g_date
   WHEN NOT MATCHED THEN
   INSERT
          ( 
          source,
          source_key,
          subscriber_key,
          last_updated_date
          )
  values
          (         
          mer_rec.	source ,
          mer_rec.	source_key,
          mer_rec.	subscriber_key,
          g_date
          );  


g_recs_read:=g_recs_read+SQL%ROWCOUNT;

commit;




       
l_text :=  'RECORDS WRITTEN TO DIM '||g_recs_last_inserted;
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


END WH_PRF_CUST_615U;
