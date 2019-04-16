--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_122U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_122U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create voucher error ex FV/C2 fact table in the foundation layer
--               with input ex staging table from FV.
--  Tables:      Input  - stg_fv_voucher_error_cpy
--               Output - fnd_fv_voucher_error
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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


g_error_no     stg_fv_voucher_error_cpy.error_no%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_122U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD FV VOUCHER ERROR EX FV/C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_fv_voucher_error_cpy
where (error_no)
in
(select error_no
from stg_fv_voucher_error_cpy
group by error_no
having count(*) > 1)
order by error_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_fv_voucher_error is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_fv_voucher_error_cpy cpy,
              fnd_fv_voucher_error fnd
      where   cpy.error_no       = fnd.error_no and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.error_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_error_no  := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.error_no   = g_error_no  then
        update stg_fv_voucher_error_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_error_no    := dupp_record.error_no;


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



 --******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_card fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.redeemed_loyalty_swipe_no,
             0,0,0,1,
             g_date,
             1,g_date
      from   stg_fv_voucher_error_cpy cpy

       where not exists
      (select /*+ nl_aj */ * from fnd_customer_card
       where  card_no         = cpy.redeemed_loyalty_swipe_no )
       and    sys_process_code    = 'N'
       and   cpy.redeemed_loyalty_swipe_no is not null;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_fv_voucher_error fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	error_no	,
            	cpy.	error_description	,
            	cpy.	error_date	,
            	cpy.	voucher_no	,
            	cpy.	redeemed_date_time	,
            	cpy.	redeemed_tender_swipe_no	,
            	cpy.	redeemed_tender_seq_no	,
            	cpy.	redeemed_loyalty_swipe_no	,
            	cpy.	redeemed_store	,
            	cpy.	redeemed_till_no	,
            	cpy.	redeemed_operator_no	,
            	cpy.	redeemed_tran_no	,
            	cpy.	redeemed_amount	,
            	cpy.	post_card_offline_ind	,
              g_date as last_updated_date
       from  stg_fv_voucher_error_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_fv_voucher_error
       where  error_no    = cpy.error_no
              )
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



FOR upd_rec IN c_stg_fv_voucher_error
   loop
     update fnd_fv_voucher_error fnd
     set    fnd.	error_description	        =	upd_rec.	error_description	,
            fnd.	error_date	              =	upd_rec.	error_date	,
            fnd.	voucher_no	              =	upd_rec.	voucher_no	,
            fnd.	redeemed_date_time	      =	upd_rec.	redeemed_date_time	,
            fnd.	redeemed_tender_swipe_no	=	upd_rec.	redeemed_tender_swipe_no	,
            fnd.	redeemed_tender_seq_no	  =	upd_rec.	redeemed_tender_seq_no	,
            fnd.	redeemed_loyalty_swipe_no	=	upd_rec.	redeemed_loyalty_swipe_no	,
            fnd.	redeemed_store	          =	upd_rec.	redeemed_store	,
            fnd.	redeemed_till_no	        =	upd_rec.	redeemed_till_no	,
            fnd.	redeemed_operator_no	    =	upd_rec.	redeemed_operator_no	,
            fnd.	redeemed_tran_no	        =	upd_rec.	redeemed_tran_no	,
            fnd.	redeemed_amount	          =	upd_rec.	redeemed_amount	,
            fnd.	post_card_offline_ind   	=	upd_rec.	post_card_offline_ind	,
            fnd.  last_updated_date         = g_date
     where  fnd.	error_no	                =	upd_rec.	error_no and
            (
            nvl(fnd.error_description	        ,0) <>	upd_rec.	error_description	or
            nvl(fnd.error_date	              ,'1 Jan 1900') <>	upd_rec.	error_date	or
            nvl(fnd.voucher_no	              ,0) <>	upd_rec.	voucher_no	or
            nvl(fnd.redeemed_date_time	      ,'1 Jan 1900') <>	upd_rec.	redeemed_date_time	or
            nvl(fnd.redeemed_tender_swipe_no	,0) <>	upd_rec.	redeemed_tender_swipe_no	or
            nvl(fnd.redeemed_tender_seq_no	  ,0) <>	upd_rec.	redeemed_tender_seq_no	or
            nvl(fnd.redeemed_loyalty_swipe_no	,0) <>	upd_rec.	redeemed_loyalty_swipe_no	or
            nvl(fnd.redeemed_store	          ,0) <>	upd_rec.	redeemed_store	or
            nvl(fnd.redeemed_till_no        	,0) <>	upd_rec.	redeemed_till_no	or
            nvl(fnd.redeemed_operator_no    	,0) <>	upd_rec.	redeemed_operator_no	or
            nvl(fnd.redeemed_tran_no	        ,0) <>	upd_rec.	redeemed_tran_no	or
            nvl(fnd.redeemed_amount	          ,0) <>	upd_rec.	redeemed_amount	or
            nvl(fnd.post_card_offline_ind   	,0) <>	upd_rec.	post_card_offline_ind
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

     insert /*+ APPEND parallel (hsp,2) */ into stg_fv_voucher_error_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A CARD WAS DETECTED ON THE TRANSACTION THAT HAS A DUMMY MASTER - INVESTIGATE!!',
            	cpy.	error_no	,
            	cpy.	error_description	,
            	cpy.	error_date	,
            	cpy.	voucher_no	,
            	cpy.	redeemed_date_time	,
            	cpy.	redeemed_tender_swipe_no	,
            	cpy.	redeemed_tender_seq_no	,
            	cpy.	redeemed_loyalty_swipe_no	,
            	cpy.	redeemed_store	,
            	cpy.	redeemed_till_no	,
            	cpy.	redeemed_operator_no	,
            	cpy.	redeemed_tran_no	,
            	cpy.	redeemed_amount	,
            	cpy.	post_card_offline_ind

      FROM   stg_fv_voucher_error_cpy cpy
      where

     ( 1 =
        (select nvl(dummy_ind,0) dummy_ind  from  fnd_customer_card crd
         where  cpy.redeemed_loyalty_swipe_no	 = crd.card_no and
                cpy.redeemed_loyalty_swipe_no  is not null )
      )
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---
     and

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

    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    create_dummy_masters;

    select count(*)
    into   g_recs_read
    from   stg_fv_voucher_error_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    flagged_records_hospital;


--    Taken out for better performance --------------------
--    update stg_fv_voucher_error_cpy
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
end wh_fnd_cust_122u;
