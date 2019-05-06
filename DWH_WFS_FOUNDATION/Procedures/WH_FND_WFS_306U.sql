--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_306U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_306U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create WFS_CARD_BEH_MONTHLY fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_beh_mly_cpy
--               Output - fnd_wfs_crd_beh_mly
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
g_truncate_count     integer       :=  0;


g_information_date                 stg_absa_crd_beh_mly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_beh_mly_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_306U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD BEH SCORES MLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_beh_mly_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_crd_beh_mly_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_beh_mly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_crd_beh_mly_cpy stg,
              fnd_wfs_crd_beh_mly fnd
      where   stg.information_date         = fnd.information_date  and             
              stg.account_number           = fnd.account_number    and   
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.information_date,
              stg.account_number,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_information_date        := '1 Jan 2000'; 
   g_account_number          := '0';

 
for dupp_record in stg_dup
   loop

    if  dupp_record.information_date        = g_information_date and
        dupp_record.account_number          = g_account_number  then
        update stg_absa_crd_beh_mly_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_information_date         := dupp_record.information_date; 
    g_account_number           := dupp_record.account_number;

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_crd_beh_mly fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
             cpy. information_date,
             cpy. account_number,
             cpy.	attrition_score_month_1	,
             cpy.	attrition_score_month_2	,
             cpy.	attrition_score_month_3	,
             cpy.	attrition_score_month_4	,
             cpy.	attrition_score_month_5	,
             cpy.	attrition_score_month_6	,
             cpy.	attrition_score_month_7	,
             cpy.	attrition_score_month_8	,
             cpy.	attrition_score_month_9	,
             cpy.	attrition_score_month_10	,
             cpy.	attrition_score_month_11	,
             cpy.	attrition_score_month_12	,
             cpy.	attrition_score_month_13	,
             cpy.	attrition_raw_score_month_1	,
             cpy.	attrition_raw_score_month_2	,
             cpy.	attrition_raw_score_month_3	,
             cpy.	attrition_raw_score_month_4	,
             cpy.	attrition_raw_score_month_5	,
             cpy.	attrition_raw_score_month_6	,
             cpy.	attrition_raw_score_month_7	,
             cpy.	attrition_raw_score_month_8	,
             cpy.	attrition_raw_score_month_9	,
             cpy.	attrition_raw_score_month_10	,
             cpy.	attrition_raw_score_month_11	,
             cpy.	attrition_raw_score_month_12	,
             cpy.	attrition_raw_score_month_13	,
             cpy.	attrition_score_card_month_1	,
             cpy.	attrition_score_card_month_2	,
             cpy.	attrition_score_card_month_3	,
             cpy.	attrition_score_card_month_4	,
             cpy.	attrition_score_card_month_5	,
             cpy.	attrition_score_card_month_6	,
             cpy.	attrition_score_card_month_7	,
             cpy.	attrition_score_card_month_8	,
             cpy.	attrition_score_card_month_9	,
             cpy.	attrition_score_card_month_10	,
             cpy.	attrition_score_card_month_11	,
             cpy.	attrition_score_card_month_12	,
             cpy.	attrition_score_card_month_13	,
             cpy.	behaviour_aligned_score_mth_1	,
             cpy.	behaviour_aligned_score_mth_2	,
             cpy.	behaviour_aligned_score_mth_3	,
             cpy.	behaviour_aligned_score_mth_4	,
             cpy.	behaviour_aligned_score_mth_5	,
             cpy.	behaviour_aligned_score_mth_6	,
             cpy.	behaviour_aligned_score_mth_7	,
             cpy.	behaviour_aligned_score_mth_8	,
             cpy.	behaviour_aligned_score_mth_9	,
             cpy.	behaviour_aligned_score_mth_10	,
             cpy.	behaviour_aligned_score_mth_11	,
             cpy.	behaviour_aligned_score_mth_12	,
             cpy.	behaviour_aligned_score_mth_13	,
             cpy.	behaviour_raw_score_mth_1	,
             cpy.	behaviour_raw_score_mth_2	,
             cpy.	behaviour_raw_score_mth_3	,
             cpy.	behaviour_raw_score_mth_4	,
             cpy.	behaviour_raw_score_mth_5	,
             cpy.	behaviour_raw_score_mth_6	,
             cpy.	behaviour_raw_score_mth_7	,
             cpy.	behaviour_raw_score_mth_8	,
             cpy.	behaviour_raw_score_mth_9	,
             cpy.	behaviour_raw_score_mth_10	,
             cpy.	behaviour_raw_score_mth_11	,
             cpy.	behaviour_raw_score_mth_12	,
             cpy.	behaviour_raw_score_mth_13	,
             cpy.	behaviour_score_card_mth_1	,
             cpy.	behaviour_score_card_mth_2	,
             cpy.	behaviour_score_card_mth_3	,
             cpy.	behaviour_score_card_mth_4	,
             cpy.	behaviour_score_card_mth_5	,
             cpy.	behaviour_score_card_mth_6	,
             cpy.	behaviour_score_card_mth_7	,
             cpy.	behaviour_score_card_mth_8	,
             cpy.	behaviour_score_card_mth_9	,
             cpy.	behaviour_score_card_mth_10	,
             cpy.	behaviour_score_card_mth_11	,
             cpy.	behaviour_score_card_mth_12	,
             cpy.	behaviour_score_card_mth_13	,
             cpy.	campaign_quota_send_ind	,
             cpy.	campaign_code_1	,
             cpy.	campaign_date_1	,
             cpy.	campaign_code_2	,
             cpy.	campaign_date_2	,
             cpy.	campaign_code_3	,
             cpy.	campaign_date_3	,
             cpy.	campaign_code_4	,
             cpy.	campaign_date_4	,
             cpy.	campaign_code_5	,
             cpy.	campaign_date_5	,
             cpy.	campaign_code_6	,
             cpy.	campaign_date_6	,
             g_date as last_updated_date
      from   stg_absa_crd_beh_mly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_beh_mly 
       where  information_date         =              cpy.information_date and
              account_number           =              cpy.account_number )
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



for upd_rec in c_stg_absa_crd_beh_mly
   loop
     update fnd_wfs_crd_beh_mly fnd 
     set    fnd.	attrition_score_month_1    	  =	upd_rec.	attrition_score_month_1	,
            fnd.	attrition_score_month_2    	  =	upd_rec.	attrition_score_month_2	,
            fnd.	attrition_score_month_3      	=	upd_rec.	attrition_score_month_3	,
            fnd.	attrition_score_month_4	      =	upd_rec.	attrition_score_month_4	,
            fnd.	attrition_score_month_5	      =	upd_rec.	attrition_score_month_5	,
            fnd.	attrition_score_month_6	      =	upd_rec.	attrition_score_month_6	,
            fnd.	attrition_score_month_7   	  =	upd_rec.	attrition_score_month_7	,
            fnd.	attrition_score_month_8	      =	upd_rec.	attrition_score_month_8	,
            fnd.	attrition_score_month_9	      =	upd_rec.	attrition_score_month_9	,
            fnd.	attrition_score_month_10  	  =	upd_rec.	attrition_score_month_10	,
            fnd.	attrition_score_month_11  	  =	upd_rec.	attrition_score_month_11	,
            fnd.	attrition_score_month_12      =	upd_rec.	attrition_score_month_12	,
            fnd.	attrition_score_month_13	    =	upd_rec.	attrition_score_month_13	,
            fnd.	attrition_raw_score_month_1  	=	upd_rec.	attrition_raw_score_month_1	,
            fnd.	attrition_raw_score_month_2  	=	upd_rec.	attrition_raw_score_month_2	,
            fnd.	attrition_raw_score_month_3  	=	upd_rec.	attrition_raw_score_month_3	,
            fnd.	attrition_raw_score_month_4  	=	upd_rec.	attrition_raw_score_month_4	,
            fnd.	attrition_raw_score_month_5  	=	upd_rec.	attrition_raw_score_month_5	,
            fnd.	attrition_raw_score_month_6	  =	upd_rec.	attrition_raw_score_month_6	,
            fnd.	attrition_raw_score_month_7  	=	upd_rec.	attrition_raw_score_month_7	,
            fnd.	attrition_raw_score_month_8  	=	upd_rec.	attrition_raw_score_month_8	,
            fnd.	attrition_raw_score_month_9	  =	upd_rec.	attrition_raw_score_month_9	,
            fnd.	attrition_raw_score_month_10	=	upd_rec.	attrition_raw_score_month_10	,
            fnd.	attrition_raw_score_month_11	=	upd_rec.	attrition_raw_score_month_11	,
            fnd.	attrition_raw_score_month_12	=	upd_rec.	attrition_raw_score_month_12	,
            fnd.	attrition_raw_score_month_13	=	upd_rec.	attrition_raw_score_month_13	,
            fnd.	attrition_score_card_month_1	=	upd_rec.	attrition_score_card_month_1	,
            fnd.	attrition_score_card_month_2	=	upd_rec.	attrition_score_card_month_2	,
            fnd.	attrition_score_card_month_3	=	upd_rec.	attrition_score_card_month_3	,
            fnd.	attrition_score_card_month_4	=	upd_rec.	attrition_score_card_month_4	,
            fnd.	attrition_score_card_month_5	=	upd_rec.	attrition_score_card_month_5	,
            fnd.	attrition_score_card_month_6	=	upd_rec.	attrition_score_card_month_6	,
            fnd.	attrition_score_card_month_7	=	upd_rec.	attrition_score_card_month_7	,
            fnd.	attrition_score_card_month_8	=	upd_rec.	attrition_score_card_month_8	,
            fnd.	attrition_score_card_month_9	=	upd_rec.	attrition_score_card_month_9	,
            fnd.	attrition_score_card_month_10	=	upd_rec.	attrition_score_card_month_10	,
            fnd.	attrition_score_card_month_11	=	upd_rec.	attrition_score_card_month_11	,
            fnd.	attrition_score_card_month_12	=	upd_rec.	attrition_score_card_month_12	,
            fnd.	attrition_score_card_month_13	=	upd_rec.	attrition_score_card_month_13	,
            fnd.	behaviour_aligned_score_mth_1	=	upd_rec.	behaviour_aligned_score_mth_1	,
            fnd.	behaviour_aligned_score_mth_2	=	upd_rec.	behaviour_aligned_score_mth_2	,
            fnd.	behaviour_aligned_score_mth_3	=	upd_rec.	behaviour_aligned_score_mth_3	,
            fnd.	behaviour_aligned_score_mth_4	=	upd_rec.	behaviour_aligned_score_mth_4	,
            fnd.	behaviour_aligned_score_mth_5	=	upd_rec.	behaviour_aligned_score_mth_5	,
            fnd.	behaviour_aligned_score_mth_6	=	upd_rec.	behaviour_aligned_score_mth_6	,
            fnd.	behaviour_aligned_score_mth_7	=	upd_rec.	behaviour_aligned_score_mth_7	,
            fnd.	behaviour_aligned_score_mth_8	=	upd_rec.	behaviour_aligned_score_mth_8	,
            fnd.	behaviour_aligned_score_mth_9	=	upd_rec.	behaviour_aligned_score_mth_9	,
            fnd.	behaviour_aligned_score_mth_10	=	upd_rec.	behaviour_aligned_score_mth_10	,
            fnd.	behaviour_aligned_score_mth_11	=	upd_rec.	behaviour_aligned_score_mth_11	,
            fnd.	behaviour_aligned_score_mth_12	=	upd_rec.	behaviour_aligned_score_mth_12	,
            fnd.	behaviour_aligned_score_mth_13	=	upd_rec.	behaviour_aligned_score_mth_13	,
            fnd.	behaviour_raw_score_mth_1	    =	upd_rec.	behaviour_raw_score_mth_1	,
            fnd.	behaviour_raw_score_mth_2	    =	upd_rec.	behaviour_raw_score_mth_2	,
            fnd.	behaviour_raw_score_mth_3	    =	upd_rec.	behaviour_raw_score_mth_3	,
            fnd.	behaviour_raw_score_mth_4	    =	upd_rec.	behaviour_raw_score_mth_4	,
            fnd.	behaviour_raw_score_mth_5	    =	upd_rec.	behaviour_raw_score_mth_5	,
            fnd.	behaviour_raw_score_mth_6	    =	upd_rec.	behaviour_raw_score_mth_6	,
            fnd.	behaviour_raw_score_mth_7	    =	upd_rec.	behaviour_raw_score_mth_7	,
            fnd.	behaviour_raw_score_mth_8	    =	upd_rec.	behaviour_raw_score_mth_8	,
            fnd.	behaviour_raw_score_mth_9	    =	upd_rec.	behaviour_raw_score_mth_9	,
            fnd.	behaviour_raw_score_mth_10	  =	upd_rec.	behaviour_raw_score_mth_10	,
            fnd.	behaviour_raw_score_mth_11	  =	upd_rec.	behaviour_raw_score_mth_11	,
            fnd.	behaviour_raw_score_mth_12	  =	upd_rec.	behaviour_raw_score_mth_12	,
            fnd.	behaviour_raw_score_mth_13	  =	upd_rec.	behaviour_raw_score_mth_13	,
            fnd.	behaviour_score_card_mth_1	  =	upd_rec.	behaviour_score_card_mth_1	,
            fnd.	behaviour_score_card_mth_2	  =	upd_rec.	behaviour_score_card_mth_2	,
            fnd.	behaviour_score_card_mth_3	  =	upd_rec.	behaviour_score_card_mth_3	,
            fnd.	behaviour_score_card_mth_4	  =	upd_rec.	behaviour_score_card_mth_4	,
            fnd.	behaviour_score_card_mth_5	  =	upd_rec.	behaviour_score_card_mth_5	,
            fnd.	behaviour_score_card_mth_6	  =	upd_rec.	behaviour_score_card_mth_6	,
            fnd.	behaviour_score_card_mth_7	  =	upd_rec.	behaviour_score_card_mth_7	,
            fnd.	behaviour_score_card_mth_8	  =	upd_rec.	behaviour_score_card_mth_8	,
            fnd.	behaviour_score_card_mth_9	  =	upd_rec.	behaviour_score_card_mth_9	,
            fnd.	behaviour_score_card_mth_10	  =	upd_rec.	behaviour_score_card_mth_10	,
            fnd.	behaviour_score_card_mth_11	  =	upd_rec.	behaviour_score_card_mth_11	,
            fnd.	behaviour_score_card_mth_12	  =	upd_rec.	behaviour_score_card_mth_12	,
            fnd.	behaviour_score_card_mth_13	  =	upd_rec.	behaviour_score_card_mth_13	,
            fnd.	campaign_quota_send_ind     	=	upd_rec.	campaign_quota_send_ind	,
            fnd.	campaign_code_1	              =	upd_rec.	campaign_code_1	,
            fnd.	campaign_date_1	              =	upd_rec.	campaign_date_1	,
            fnd.	campaign_code_2	              =	upd_rec.	campaign_code_2	,
            fnd.	campaign_date_2	              =	upd_rec.	campaign_date_2	,
            fnd.	campaign_code_3	              =	upd_rec.	campaign_code_3	,
            fnd.	campaign_date_3	              =	upd_rec.	campaign_date_3	,
            fnd.	campaign_code_4	              =	upd_rec.	campaign_code_4	,
            fnd.	campaign_date_4	              =	upd_rec.	campaign_date_4	,
            fnd.	campaign_code_5	              =	upd_rec.	campaign_code_5	,
            fnd.	campaign_date_5	              =	upd_rec.	campaign_date_5	,
            fnd.	campaign_code_6	              =	upd_rec.	campaign_code_6	,
            fnd.	campaign_date_6	              =	upd_rec.	campaign_date_6	,
            fnd.  last_updated_date             = g_date
     where  fnd.	information_date              =	upd_rec.	information_date and
            fnd.	account_number              	=	upd_rec.	account_number   and
            ( 
            nvl(fnd.attrition_score_month_1	        ,0) <>	upd_rec.	attrition_score_month_1	or
            nvl(fnd.attrition_score_month_2	        ,0) <>	upd_rec.	attrition_score_month_2	or
            nvl(fnd.attrition_score_month_3	        ,0) <>	upd_rec.	attrition_score_month_3	or
            nvl(fnd.attrition_score_month_4	        ,0) <>	upd_rec.	attrition_score_month_4	or
            nvl(fnd.attrition_score_month_5	        ,0) <>	upd_rec.	attrition_score_month_5	or
            nvl(fnd.attrition_score_month_6	        ,0) <>	upd_rec.	attrition_score_month_6	or
            nvl(fnd.attrition_score_month_7	        ,0) <>	upd_rec.	attrition_score_month_7	or
            nvl(fnd.attrition_score_month_8	        ,0) <>	upd_rec.	attrition_score_month_8	or
            nvl(fnd.attrition_score_month_9	        ,0) <>	upd_rec.	attrition_score_month_9	or
            nvl(fnd.attrition_score_month_10	      ,0) <>	upd_rec.	attrition_score_month_10	or
            nvl(fnd.attrition_score_month_11	      ,0) <>	upd_rec.	attrition_score_month_11	or
            nvl(fnd.attrition_score_month_12	      ,0) <>	upd_rec.	attrition_score_month_12	or
            nvl(fnd.attrition_score_month_13	      ,0) <>	upd_rec.	attrition_score_month_13	or
            nvl(fnd.attrition_raw_score_month_1	    ,0) <>	upd_rec.	attrition_raw_score_month_1	or
            nvl(fnd.attrition_raw_score_month_2	    ,0) <>	upd_rec.	attrition_raw_score_month_2	or
            nvl(fnd.attrition_raw_score_month_3	    ,0) <>	upd_rec.	attrition_raw_score_month_3	or
            nvl(fnd.attrition_raw_score_month_4	    ,0) <>	upd_rec.	attrition_raw_score_month_4	or
            nvl(fnd.attrition_raw_score_month_5	    ,0) <>	upd_rec.	attrition_raw_score_month_5	or
            nvl(fnd.attrition_raw_score_month_6	    ,0) <>	upd_rec.	attrition_raw_score_month_6	or
            nvl(fnd.attrition_raw_score_month_7   	,0) <>	upd_rec.	attrition_raw_score_month_7	or
            nvl(fnd.attrition_raw_score_month_8	    ,0) <>	upd_rec.	attrition_raw_score_month_8	or
            nvl(fnd.attrition_raw_score_month_9	    ,0) <>	upd_rec.	attrition_raw_score_month_9	or
            nvl(fnd.attrition_raw_score_month_10	  ,0) <>	upd_rec.	attrition_raw_score_month_10	or
            nvl(fnd.attrition_raw_score_month_11	  ,0) <>	upd_rec.	attrition_raw_score_month_11	or
            nvl(fnd.attrition_raw_score_month_12	  ,0) <>	upd_rec.	attrition_raw_score_month_12	or
            nvl(fnd.attrition_raw_score_month_13	  ,0) <>	upd_rec.	attrition_raw_score_month_13	or
            nvl(fnd.attrition_score_card_month_1	  ,0) <>	upd_rec.	attrition_score_card_month_1	or
            nvl(fnd.attrition_score_card_month_2	  ,0) <>	upd_rec.	attrition_score_card_month_2	or
            nvl(fnd.attrition_score_card_month_3	  ,0) <>	upd_rec.	attrition_score_card_month_3	or
            nvl(fnd.attrition_score_card_month_4	  ,0) <>	upd_rec.	attrition_score_card_month_4	or
            nvl(fnd.attrition_score_card_month_5	  ,0) <>	upd_rec.	attrition_score_card_month_5	or
            nvl(fnd.attrition_score_card_month_6	  ,0) <>	upd_rec.	attrition_score_card_month_6	or
            nvl(fnd.attrition_score_card_month_7	  ,0) <>	upd_rec.	attrition_score_card_month_7	or
            nvl(fnd.attrition_score_card_month_8	  ,0) <>	upd_rec.	attrition_score_card_month_8	or
            nvl(fnd.attrition_score_card_month_9	  ,0) <>	upd_rec.	attrition_score_card_month_9	or
            nvl(fnd.attrition_score_card_month_10	  ,0) <>	upd_rec.	attrition_score_card_month_10	or
            nvl(fnd.attrition_score_card_month_11	  ,0) <>	upd_rec.	attrition_score_card_month_11	or
            nvl(fnd.attrition_score_card_month_12	  ,0) <>	upd_rec.	attrition_score_card_month_12	or
            nvl(fnd.attrition_score_card_month_13	  ,0) <>	upd_rec.	attrition_score_card_month_13	or
            nvl(fnd.behaviour_aligned_score_mth_1	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_1	or
            nvl(fnd.behaviour_aligned_score_mth_2	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_2	or
            nvl(fnd.behaviour_aligned_score_mth_3	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_3	or
            nvl(fnd.behaviour_aligned_score_mth_4	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_4	or
            nvl(fnd.behaviour_aligned_score_mth_5	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_5	or
            nvl(fnd.behaviour_aligned_score_mth_6	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_6	or
            nvl(fnd.behaviour_aligned_score_mth_7	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_7	or
            nvl(fnd.behaviour_aligned_score_mth_8	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_8	or
            nvl(fnd.behaviour_aligned_score_mth_9	  ,0) <>	upd_rec.	behaviour_aligned_score_mth_9	or
            nvl(fnd.behaviour_aligned_score_mth_10	,0) <>	upd_rec.	behaviour_aligned_score_mth_10	or
            nvl(fnd.behaviour_aligned_score_mth_11	,0) <>	upd_rec.	behaviour_aligned_score_mth_11	or
            nvl(fnd.behaviour_aligned_score_mth_12	,0) <>	upd_rec.	behaviour_aligned_score_mth_12	or
            nvl(fnd.behaviour_aligned_score_mth_13	,0) <>	upd_rec.	behaviour_aligned_score_mth_13	or
            nvl(fnd.behaviour_raw_score_mth_1	      ,0) <>	upd_rec.	behaviour_raw_score_mth_1	or
            nvl(fnd.behaviour_raw_score_mth_2	      ,0) <>	upd_rec.	behaviour_raw_score_mth_2	or
            nvl(fnd.behaviour_raw_score_mth_3	      ,0) <>	upd_rec.	behaviour_raw_score_mth_3	or
            nvl(fnd.behaviour_raw_score_mth_4	      ,0) <>	upd_rec.	behaviour_raw_score_mth_4	or
            nvl(fnd.behaviour_raw_score_mth_5	      ,0) <>	upd_rec.	behaviour_raw_score_mth_5	or
            nvl(fnd.behaviour_raw_score_mth_6	      ,0) <>	upd_rec.	behaviour_raw_score_mth_6	or
            nvl(fnd.behaviour_raw_score_mth_7	      ,0) <>	upd_rec.	behaviour_raw_score_mth_7	or
            nvl(fnd.behaviour_raw_score_mth_8	      ,0) <>	upd_rec.	behaviour_raw_score_mth_8	or
            nvl(fnd.behaviour_raw_score_mth_9	      ,0) <>	upd_rec.	behaviour_raw_score_mth_9	or
            nvl(fnd.behaviour_raw_score_mth_10	    ,0) <>	upd_rec.	behaviour_raw_score_mth_10	or
            nvl(fnd.behaviour_raw_score_mth_11	    ,0) <>	upd_rec.	behaviour_raw_score_mth_11	or
            nvl(fnd.behaviour_raw_score_mth_12	    ,0) <>	upd_rec.	behaviour_raw_score_mth_12	or
            nvl(fnd.behaviour_raw_score_mth_13	    ,0) <>	upd_rec.	behaviour_raw_score_mth_13	or
            nvl(fnd.behaviour_score_card_mth_1	    ,0) <>	upd_rec.	behaviour_score_card_mth_1	or
            nvl(fnd.behaviour_score_card_mth_2	    ,0) <>	upd_rec.	behaviour_score_card_mth_2	or
            nvl(fnd.behaviour_score_card_mth_3	    ,0) <>	upd_rec.	behaviour_score_card_mth_3	or
            nvl(fnd.behaviour_score_card_mth_4	    ,0) <>	upd_rec.	behaviour_score_card_mth_4	or
            nvl(fnd.behaviour_score_card_mth_5	    ,0) <>	upd_rec.	behaviour_score_card_mth_5	or
            nvl(fnd.behaviour_score_card_mth_6	    ,0) <>	upd_rec.	behaviour_score_card_mth_6	or
            nvl(fnd.behaviour_score_card_mth_7	    ,0) <>	upd_rec.	behaviour_score_card_mth_7	or
            nvl(fnd.behaviour_score_card_mth_8	    ,0) <>	upd_rec.	behaviour_score_card_mth_8	or
            nvl(fnd.behaviour_score_card_mth_9	    ,0) <>	upd_rec.	behaviour_score_card_mth_9	or
            nvl(fnd.behaviour_score_card_mth_10	    ,0) <>	upd_rec.	behaviour_score_card_mth_10	or
            nvl(fnd.behaviour_score_card_mth_11	    ,0) <>	upd_rec.	behaviour_score_card_mth_11	or
            nvl(fnd.behaviour_score_card_mth_12	    ,0) <>	upd_rec.	behaviour_score_card_mth_12	or
            nvl(fnd.behaviour_score_card_mth_13	    ,0) <>	upd_rec.	behaviour_score_card_mth_13	or
            nvl(fnd.campaign_quota_send_ind	        ,0) <>	upd_rec.	campaign_quota_send_ind	or
            nvl(fnd.campaign_code_1	                ,0) <>	upd_rec.	campaign_code_1	or
            nvl(fnd.campaign_date_1	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_1	or
            nvl(fnd.campaign_code_2	                ,0) <>	upd_rec.	campaign_code_2	or
            nvl(fnd.campaign_date_2	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_2	or
            nvl(fnd.campaign_code_3	                ,0) <>	upd_rec.	campaign_code_3	or
            nvl(fnd.campaign_date_3	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_3	or
            nvl(fnd.campaign_code_4	                ,0) <>	upd_rec.	campaign_code_4	or
            nvl(fnd.campaign_date_4	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_4	or
            nvl(fnd.campaign_code_5	                ,0) <>	upd_rec.	campaign_code_5	or
            nvl(fnd.campaign_date_5	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_5	or
            nvl(fnd.campaign_code_6	                ,0) <>	upd_rec.	campaign_code_6	or
            nvl(fnd.campaign_date_6	                ,'1 Jan 1900') <>	upd_rec.	campaign_date_6	
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_beh_mly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	account_number	,
             cpy.	information_date	,
             cpy.	attrition_score_month_1	,
             cpy.	attrition_score_month_2	,
             cpy.	attrition_score_month_3	,
             cpy.	attrition_score_month_4	,
             cpy.	attrition_score_month_5	,
             cpy.	attrition_score_month_6	,
             cpy.	attrition_score_month_7	,
             cpy.	attrition_score_month_8	,
             cpy.	attrition_score_month_9	,
             cpy.	attrition_score_month_10	,
             cpy.	attrition_score_month_11	,
             cpy.	attrition_score_month_12	,
             cpy.	attrition_score_month_13	,
             cpy.	attrition_raw_score_month_1	,
             cpy.	attrition_raw_score_month_2	,
             cpy.	attrition_raw_score_month_3	,
             cpy.	attrition_raw_score_month_4	,
             cpy.	attrition_raw_score_month_5	,
             cpy.	attrition_raw_score_month_6	,
             cpy.	attrition_raw_score_month_7	,
             cpy.	attrition_raw_score_month_8	,
             cpy.	attrition_raw_score_month_9	,
             cpy.	attrition_raw_score_month_10	,
             cpy.	attrition_raw_score_month_11	,
             cpy.	attrition_raw_score_month_12	,
             cpy.	attrition_raw_score_month_13	,
             cpy.	attrition_score_card_month_1	,
             cpy.	attrition_score_card_month_2	,
             cpy.	attrition_score_card_month_3	,
             cpy.	attrition_score_card_month_4	,
             cpy.	attrition_score_card_month_5	,
             cpy.	attrition_score_card_month_6	,
             cpy.	attrition_score_card_month_7	,
             cpy.	attrition_score_card_month_8	,
             cpy.	attrition_score_card_month_9	,
             cpy.	attrition_score_card_month_10	,
             cpy.	attrition_score_card_month_11	,
             cpy.	attrition_score_card_month_12	,
             cpy.	attrition_score_card_month_13	,
             cpy.	behaviour_aligned_score_mth_1	,
             cpy.	behaviour_aligned_score_mth_2	,
             cpy.	behaviour_aligned_score_mth_3	,
             cpy.	behaviour_aligned_score_mth_4	,
             cpy.	behaviour_aligned_score_mth_5	,
             cpy.	behaviour_aligned_score_mth_6	,
             cpy.	behaviour_aligned_score_mth_7	,
             cpy.	behaviour_aligned_score_mth_8	,
             cpy.	behaviour_aligned_score_mth_9	,
             cpy.	behaviour_aligned_score_mth_10	,
             cpy.	behaviour_aligned_score_mth_11	,
             cpy.	behaviour_aligned_score_mth_12	,
             cpy.	behaviour_aligned_score_mth_13	,
             cpy.	behaviour_raw_score_mth_1	,
             cpy.	behaviour_raw_score_mth_2	,
             cpy.	behaviour_raw_score_mth_3	,
             cpy.	behaviour_raw_score_mth_4	,
             cpy.	behaviour_raw_score_mth_5	,
             cpy.	behaviour_raw_score_mth_6	,
             cpy.	behaviour_raw_score_mth_7	,
             cpy.	behaviour_raw_score_mth_8	,
             cpy.	behaviour_raw_score_mth_9	,
             cpy.	behaviour_raw_score_mth_10	,
             cpy.	behaviour_raw_score_mth_11	,
             cpy.	behaviour_raw_score_mth_12	,
             cpy.	behaviour_raw_score_mth_13	,
             cpy.	behaviour_score_card_mth_1	,
             cpy.	behaviour_score_card_mth_2	,
             cpy.	behaviour_score_card_mth_3	,
             cpy.	behaviour_score_card_mth_4	,
             cpy.	behaviour_score_card_mth_5	,
             cpy.	behaviour_score_card_mth_6	,
             cpy.	behaviour_score_card_mth_7	,
             cpy.	behaviour_score_card_mth_8	,
             cpy.	behaviour_score_card_mth_9	,
             cpy.	behaviour_score_card_mth_10	,
             cpy.	behaviour_score_card_mth_11	,
             cpy.	behaviour_score_card_mth_12	,
             cpy.	behaviour_score_card_mth_13	,
             cpy.	campaign_quota_send_ind	,
             cpy.	campaign_code_1	,
             cpy.	campaign_date_1	,
             cpy.	campaign_code_2	,
             cpy.	campaign_date_2	,
             cpy.	campaign_code_3	,
             cpy.	campaign_date_3	,
             cpy.	campaign_code_4	,
             cpy.	campaign_date_4	,
             cpy.	campaign_code_5	,
             cpy.	campaign_date_5	,
             cpy.	campaign_code_6	,
             cpy.	campaign_date_6	
      from   stg_absa_crd_beh_mly_cpy cpy
      where  
--      (    
--      NOT EXISTS 
--        (SELECT * FROM  dim_table dim
--         where               cpy.xxx       = dim.xxx ) or
--      not exists 
--        (select * from  dim_table dim1
--         where  cpy.xxx    = dim1.xxx ) 
--      ) and 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
        sys_process_code = 'N';
         

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
    
    select count(*)
    into   g_recs_read
    from   stg_absa_crd_beh_mly_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


--********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_crd_beh_mly_cpy
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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
end wh_fnd_wfs_306u;
