--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_283U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_283U" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    fEEDBACK EXCEPTIONS TO DIM)ITEM)CUST_LSS. Lifestyle Segmentation
--  Tables:      Input  - cust_lss_pfc_feedback  
--               Output - dim_item_cust_lss
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--   
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
g_recs_hospital      integer       :=  0;
g_recs_lev1          integer       :=  0;
g_forall_limit       integer       :=  10000;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_283U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD dim_item_cust_lss EX dim_item_cust_lss';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF dim_item_cust_lss EX dim_item_cust_lss STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

    l_text      := 'Start of update to populate PFC on dim_item_cust_lss from Feedback';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

    MERGE  /*+ parallel (rli,4) */ INTO dim_item_cust_lss rli USING
    (
       select /*+ parallel (4) */
              fb.item_no,fb.product_family_code
       from   cust_lss_pfc_feedback fb
    )  mv
    ON  (mv.ITEM_NO                 = rli.ITEM_NO)
    WHEN MATCHED THEN
    update set   rli.product_family_code    =  mv.product_family_code, 
                 rli.last_updated_date      =  g_date
    ;     

    g_recs_lev1:=g_recs_lev1+SQL%ROWCOUNT;  

commit;



--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS UPDATED FROM LEVEL 1 '||g_recs_LEV1;
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


END WH_PRF_CUST_283U;
