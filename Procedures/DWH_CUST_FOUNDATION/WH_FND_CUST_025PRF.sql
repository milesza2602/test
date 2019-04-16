--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_025PRF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_025PRF" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        AUG 2015
--  Author:      Alastair de Wet
--               Create Dim _customer dimention table in the foundation layer
--               with input ex staging table from Customer Central for prefs and concents.
--  Tables:      Input  - stg_c2_cust_pref_cpy
--               Output - dim_customer
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
--
--   
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
g_physical_updated   integer       :=  0;

g_seq_s              integer       :=  0;
g_seq_e              integer       :=  250000;
g_sub                integer       :=  0;

g_customer_no        stg_c2_cust_pref_cpy.customer_no%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_025PRF';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER DIM EX C2 CUST PREFS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_c2_cust_pref_cpy
where (customer_no)
in
(select customer_no
from stg_c2_cust_pref_cpy 
group by customer_no 
having count(*) > 1) 
order by customer_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_customer_no   := 0; 
 

for dupp_record in stg_dup
   loop

    if  dupp_record.customer_no    = g_customer_no  then
        update stg_c2_cust_pref_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_customer_no    := dupp_record.customer_no; 
 

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;
 
--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
/*+ first_rows parallel(fnd) parallel(upd_rec) */

   for g_sub in 0..52 loop 

   
    l_text := 'SEQ NOS TO BE PROCESSED:- '||G_SEQ_S||' TO '||G_SEQ_E;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   MERGE  INTO dim_customer fnd 
   USING (
         select /*+ FULL(cpy)  parallel (cpy,8) */  
              cpy.*
      from    stg_c2_cust_pref_cpy cpy
      where   cpy.sys_process_code      = 'N'  AND
              CPY.SYS_SOURCE_SEQUENCE_NO BETWEEN G_SEQ_S AND G_SEQ_E
--      order by   sys_source_batch_id,sys_source_sequence_no      
         ) mer_rec
   ON    (  fnd.	customer_no	          =	mer_rec.	customer_no )
   WHEN MATCHED THEN 
   UPDATE SET
            fnd.	ww_dm_sms_opt_out_ind	    =	mer_rec.	ww_dm_sms_opt_out_ind	,
            fnd.	ww_dm_email_opt_out_ind 	=	mer_rec.	ww_dm_email_opt_out_ind	,
            fnd.	ww_dm_post_opt_out_ind	  =	mer_rec.	ww_dm_post_opt_out_ind	,
            fnd.	ww_dm_phone_opt_out_ind	  =	mer_rec.	ww_dm_phone_opt_out_ind	,
            fnd.	ww_man_sms_opt_out_ind	  =	mer_rec.	ww_man_sms_opt_out_ind	,
            fnd.	ww_man_email_opt_out_ind	=	mer_rec.	ww_man_email_opt_out_ind	,
            fnd.	ww_man_post_opt_out_ind	  =	mer_rec.	ww_man_post_opt_out_ind	,
            fnd.	ww_man_phone_opt_out_ind	=	mer_rec.	ww_man_phone_opt_out_ind	,
            fnd.	wfs_dm_sms_opt_out_ind	  =	mer_rec.	wfs_dm_sms_opt_out_ind	,
            fnd.	wfs_dm_email_opt_out_ind	=	mer_rec.	wfs_dm_email_opt_out_ind	,
            fnd.	wfs_dm_post_opt_out_ind	  =	mer_rec.	wfs_dm_post_opt_out_ind	,
            fnd.	wfs_dm_phone_opt_out_ind	=	mer_rec.	wfs_dm_phone_opt_out_ind	,
            fnd.	wfs_con_sms_opt_out_ind	  =	mer_rec.	wfs_con_sms_opt_out_ind	,
            fnd.	wfs_con_email_opt_out_ind	=	mer_rec.	wfs_con_email_opt_out_ind	,
            fnd.	wfs_con_post_opt_out_ind	=	mer_rec.	wfs_con_post_opt_out_ind	,
            fnd.	wfs_con_phone_opt_out_ind	=	mer_rec.	wfs_con_phone_opt_out_ind	,
            fnd.	preference_1_ind	        =	mer_rec.	preference_1_ind	,
            fnd.	preference_1_no	          =	mer_rec.	preference_1_no	,
            fnd.	preference_2_ind	        =	mer_rec.	preference_2_ind	,
            fnd.	preference_2_no	          =	mer_rec.	preference_2_no	,
            fnd.	preference_3_ind        	=	mer_rec.	preference_3_ind	,
            fnd.	preference_3_no	          =	mer_rec.	preference_3_no	,
            fnd.	preference_4_ind	        =	mer_rec.	preference_4_ind	,
            fnd.	preference_4_no	          =	mer_rec.	preference_4_no	,
            fnd.	preference_5_ind        	=	mer_rec.	preference_5_ind	,
            fnd.	preference_5_no	          =	mer_rec.	preference_5_no	,
            fnd.	preference_6_ind	        =	mer_rec.	preference_6_ind	,
            fnd.	preference_6_no	          =	mer_rec.	preference_6_no	,
            fnd.	preference_7_ind	        =	mer_rec.	preference_7_ind	,
            fnd.	preference_7_no	          =	mer_rec.	preference_7_no	;
--            fnd.  last_updated_date         = g_date;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

      G_SEQ_S := G_SEQ_S + 250000;
      G_SEQ_E := G_SEQ_E + 250000;

      commit;
  end loop;

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_c2_cust_pref_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'No Customer_master exists to update - investigate!!',
              cpy.CUSTOMER_NO,
              cpy.WW_DM_SMS_OPT_OUT_IND,
              cpy.WW_DM_EMAIL_OPT_OUT_IND,
              cpy.WW_DM_POST_OPT_OUT_IND,
              cpy.WW_DM_PHONE_OPT_OUT_IND,
              cpy.WW_MAN_SMS_OPT_OUT_IND,
              cpy.WW_MAN_EMAIL_OPT_OUT_IND,
              cpy.WW_MAN_POST_OPT_OUT_IND,
              cpy.WW_MAN_PHONE_OPT_OUT_IND,
              cpy.WFS_DM_SMS_OPT_OUT_IND,
              cpy.WFS_DM_EMAIL_OPT_OUT_IND,
              cpy.WFS_DM_POST_OPT_OUT_IND,
              cpy.WFS_DM_PHONE_OPT_OUT_IND,
              cpy.WFS_CON_SMS_OPT_OUT_IND,
              cpy.WFS_CON_EMAIL_OPT_OUT_IND,
              cpy.WFS_CON_POST_OPT_OUT_IND,
              cpy.WFS_CON_PHONE_OPT_OUT_IND,
              cpy.PREFERENCE_1_IND,
              cpy.PREFERENCE_1_NO,
              cpy.PREFERENCE_2_IND,
              cpy.PREFERENCE_2_NO,
              cpy.PREFERENCE_3_IND,
              cpy.PREFERENCE_3_NO,
              cpy.PREFERENCE_4_IND,
              cpy.PREFERENCE_4_NO,
              cpy.PREFERENCE_5_IND,
              cpy.PREFERENCE_5_NO,
              cpy.PREFERENCE_6_IND,
              cpy.PREFERENCE_6_NO,
              cpy.PREFERENCE_7_IND,
              cpy.PREFERENCE_7_NO
      from   stg_c2_cust_pref_cpy cpy
      where  
        (
         not exists 
           (select * 
           from   dim_customer cst
           where  cpy.customer_no       = cst.customer_no )  
--        OR
--        not exists 
--           (select * 
--           from   dim_customer_product cp
--           where  cpy.product_no       = cp.product_no )  
         )  
         AND sys_process_code = 'N';
         

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
   
--    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   stg_c2_cust_pref_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;
    

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
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;            --Bulk load--
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
end wh_fnd_cust_025prf;
