--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_146U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_146U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Fica track n trace ex 4f fact table in the foundation layer
--               with input ex staging table from FV.
--  Tables:      Input  - stg_4f_fica_trackntrace_cpy
--               Output - fnd_uti_fica_trackntrace
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


g_end_this_week      stg_4f_fica_trackntrace_cpy.end_this_week%type;
g_card_no            stg_4f_fica_trackntrace_cpy.card_no%type;
g_customer_id_no     stg_4f_fica_trackntrace_cpy.customer_id_no%type;
g_uti_no             stg_4f_fica_trackntrace_cpy.uti_no%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_146U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD UTI_FICA_TRACKNTRACE EX 4F';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_4f_fica_trackntrace_cpy
where (end_this_week,card_no,customer_id_no,uti_no)
in
(select end_this_week,card_no,customer_id_no,uti_no
from stg_4f_fica_trackntrace_cpy
group by end_this_week,card_no,customer_id_no,uti_no
having count(*) > 1)
order by end_this_week,card_no,customer_id_no,uti_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_4f_fica_trackntrace is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_4f_fica_trackntrace_cpy cpy,
              fnd_uti_fica_trackntrace fnd
      where   cpy.end_this_week    = fnd.end_this_week and
              cpy.card_no          = fnd.card_no and
              cpy.customer_id_no   = fnd.customer_id_no and
              cpy.uti_no           = fnd.uti_no and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.end_this_week,cpy.card_no,cpy.customer_id_no,cpy.uti_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_end_this_week   := '1 Jan 2000';
   g_card_no         := ' ';
   g_customer_id_no  := ' ';
   g_uti_no          := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.end_this_week   = g_end_this_week  and
        dupp_record.card_no         = g_card_no  and
        dupp_record.customer_id_no  = g_customer_id_no  and
        dupp_record.uti_no          = g_uti_no  then
        update stg_4f_fica_trackntrace_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_end_this_week    := dupp_record.end_this_week;
    g_card_no          := dupp_record.card_no;
    g_customer_id_no   := dupp_record.customer_id_no;
    g_uti_no           := dupp_record.uti_no;


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

--      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
--      select /*+ FULL(cpy)  parallel (cpy,2) */
--             distinct
--             cpy.primary_account_no	,
--             0,
--             1,
--             g_date,
--             1
--      from   stg_4f_fica_trackntrace_cpy cpy

--       where not exists
--      (select /*+ nl_aj */ * from fnd_customer_product
--       where  product_no          = cpy.primary_account_no )
--       and    sys_process_code    = 'N'
--       and    cpy.primary_account_no is not null ;

--       g_recs_dummy := g_recs_dummy + sql%rowcount;
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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_uti_fica_trackntrace fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.end_this_week,
              cpy.card_no,
              cpy.customer_id_no,
              cpy.card_type,
              cpy.verification_doc_type,
              cpy.uti_no,
              cpy.checked_and_collected_by,
              cpy.checked_and_collected_date,
              cpy.received_by,
              cpy.fica_received_date,
              cpy.declined_by,
              cpy.fica_declined_date,
              cpy.declined_reason,
              cpy.fica_approved_date,
              cpy.approved_by,
              cpy.time_spent_in_hours_qty,
              cpy.recollection_request_date,
              g_date as last_updated_date
       from  stg_4f_fica_trackntrace_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_uti_fica_trackntrace
       where  end_this_week    = cpy.end_this_week and
              card_no          = cpy.card_no and
              customer_id_no   = cpy.customer_id_no and
              uti_no           = cpy.uti_no
              )
       AND cpy.customer_id_no IS NOT NULL
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



FOR upd_rec IN c_stg_4f_fica_trackntrace
   loop
     update fnd_uti_fica_trackntrace fnd
         set fnd.	card_type	                 =	upd_rec.	card_type	,
            fnd.	verification_doc_type	     =	upd_rec.	verification_doc_type	,
            fnd.	checked_and_collected_by	 =	upd_rec.	checked_and_collected_by	,
            fnd.	checked_and_collected_date =	upd_rec.	checked_and_collected_date	,
            fnd.	received_by	              =	upd_rec.	received_by	,
            fnd.	fica_received_date	      =	upd_rec.	fica_received_date	,
            fnd.	declined_by	              =	upd_rec.	declined_by	,
            fnd.	fica_declined_date	      =	upd_rec.	fica_declined_date	,
            fnd.	declined_reason	          =	upd_rec.	declined_reason	,
            fnd.	fica_approved_date	      =	upd_rec.	fica_approved_date	,
            fnd.	approved_by	              =	upd_rec.	approved_by	,
            fnd.	time_spent_in_hours_qty	  =	upd_rec.	time_spent_in_hours_qty	,
            fnd.	recollection_request_date	=	upd_rec.	recollection_request_date	,
            fnd.  last_updated_date         = g_date
     where  fnd.	end_this_week	            =	upd_rec.	end_this_week and
            fnd.	card_no                   =	upd_rec.	card_no and
            fnd.	customer_id_no	          =	upd_rec.	customer_id_no and
            fnd.	uti_no	                  =	upd_rec.	uti_no;


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


--   g_recs_hospital := g_recs_hospital + sql%rowcount;

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
    l_text := 'ALTER KEYED FIELDS FROM NULL TO 0 '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    UPDATE stg_4f_fica_trackntrace_cpy
    set    card_no = '0'
    where  card_no is null;

    UPDATE stg_4f_fica_trackntrace_cpy
    set    uti_no = 0
    where  uti_no is null;

    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

--    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    create_dummy_masters;

    select count(*)
    into   g_recs_read
    from   stg_4f_fica_trackntrace_cpy
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
--    update stg_4f_fica_trackntrace_cpy
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

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;


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
end wh_fnd_cust_146u;
