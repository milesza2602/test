--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_419U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_419U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: CLI - LOAD BLAZE AFFORD LIM INFO
-- Tran_type: ODWH: CLIBOFRAL   AIT: CLIBOFRAL

--
-- Date:        2018-10-29
-- Author:      Safiyya Ismail
-- Purpose:     update table FND_WFS_CLI_BLZ_AFFORD_LIM in the Foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_C2_CLI_BLZ_AFFORD_LIM_CPY
--              Output - FND_WFS_CLI_BLZ_AFFORD_LIM
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--   2018-10-29 S Ismail - created.
--   2018-11-15 S N Chauhan - description amended.

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_C2_CLI_BLZ_AFFORD_LIM_CPY.CLI_BLZ_OFFR_AFFORD_LIMITS_NO%type;
--
--
--
--

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_419U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CLI - LOAD BLAZE AFFORD LIM INFO';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_dup is
   select * from  dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy
   where (    cli_blz_offr_afford_limits_no  )
   in
   (select     cli_blz_offr_afford_limits_no 
    from dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy
    group by      cli_blz_offr_afford_limits_no
    having count(*) > 1) 
   order by
    cli_blz_offr_afford_limits_no
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_cli_blz_afford_lim fnd
     where
              fnd.cli_blz_offr_afford_limits_no   = stg.cli_blz_offr_afford_limits_no and      -- only ones existing in fnd
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.cli_blz_offr_afford_limits_no,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;

   for dupp_record in c_stg_dup
    loop
       if 
               dupp_record.cli_blz_offr_afford_limits_no  = g_unique_key1_field_val
       then 
        update dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.cli_blz_offr_afford_limits_no;

    end loop;

   commit;

exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ append parallel (fnd,2) */ into fnd_wfs_cli_blz_afford_lim fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. cli_blz_offr_afford_limits_no ,
         cpy. cli_blaze_offer_no ,
         cpy. affordability_limit_amount ,
         cpy. payment_term ,
         cpy. bureau_based_afford_limit_amt

         ,
         g_date as last_updated_date 

      from  dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy cpy
         left outer join dwh_wfs_foundation.fnd_wfs_cli_blz_afford_lim fnd on (
                 fnd.cli_blz_offr_afford_limits_no  = cpy.cli_blz_offr_afford_limits_no
             )
      where fnd.cli_blz_offr_afford_limits_no is null

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
-- Updates existing records in the staging table into foundation if there are changes
--**************************************************************************************************

procedure flagged_records_update as
begin


for upd_rec in c_stg
   loop
     update fnd_wfs_cli_blz_afford_lim fnd 
     set    
         fnd. cli_blz_offr_afford_limits_no = upd_rec. cli_blz_offr_afford_limits_no ,
         fnd. cli_blaze_offer_no = upd_rec. cli_blaze_offer_no ,
         fnd. affordability_limit_amount = upd_rec. affordability_limit_amount ,
         fnd. payment_term = upd_rec. payment_term ,
         fnd. bureau_based_afford_limit_amt = upd_rec. bureau_based_afford_limit_amt

    , 
         fnd.last_updated_date          = g_date 


     where 
              fnd.cli_blz_offr_afford_limits_no  = upd_rec.cli_blz_offr_afford_limits_no and
--
--
--
--

        ( 
         nvl(fnd. cli_blz_offr_afford_limits_no, 0) <> upd_rec. cli_blz_offr_afford_limits_no OR
         nvl(fnd. cli_blaze_offer_no, 0) <> upd_rec. cli_blaze_offer_no OR
         nvl(fnd. affordability_limit_amount, 0) <> upd_rec. affordability_limit_amount OR
         nvl(fnd. payment_term, 0) <> upd_rec. payment_term OR
         nvl(fnd. bureau_based_afford_limit_amt, 0) <> upd_rec. bureau_based_afford_limit_amt
       );         


   if sql%rowcount = 0 then
        g_recs_nochange:= g_recs_nochange + 1;
   else
        g_recs_updated := g_recs_updated + 1;  
   end if;

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
-- ***** Not applicable, as there is no dependency on some other table *****
procedure flagged_records_hospital as
begin

      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with ' ,
         cpy. cli_blz_offr_afford_limits_no ,
         cpy. cli_blaze_offer_no ,
         cpy. affordability_limit_amount ,
         cpy. payment_term ,
         cpy. bureau_based_afford_limit_amt

      from   dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy cpy
--  no dependency table applicable
      where 

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

    l_text := 'LOAD TABLE: '||'FND_WFS_CLI_BLZ_AFFORD_LIM' ;
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
    from   dwh_wfs_foundation.stg_c2_cli_blz_afford_lim_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

/*   no referential integrity checks required
    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_hospital;
*/

--    Taken out for better performance --------------------
--    update stg_...._cpy
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
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
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
       RAISE;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;

end WH_FND_WFS_419U;