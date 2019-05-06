--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_165U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_165U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create WOD CORRESPONDENSE DETAIL EX C2 fact table in the foundation layer
--               with input ex staging table from FV.
--  Tables:      Input  - stg_c2_wod_correspond_dtl_cpy
--               Output - fnd_wod_correspond_detail
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


g_correspondence_id    stg_c2_wod_correspond_dtl_cpy.correspondence_id%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_165U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WOD CORRESPONDENSE DETAILS EX C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_c2_wod_correspond_dtl_cpy
where (correspondence_id)
in
(select correspondence_id
from stg_c2_wod_correspond_dtl_cpy
group by correspondence_id
having count(*) > 1)
order by correspondence_id,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_c2_wod_correspond_dtl is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_c2_wod_correspond_dtl_cpy cpy,
              FND_WOD_CORRESPOND_DETAIL FND
      where   cpy.correspondence_id      =  fnd.correspondence_id and
              cpy.sys_process_code       = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.correspondence_id,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_correspondence_id := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.correspondence_id  = g_correspondence_id then
        update stg_c2_wod_correspond_dtl_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_correspondence_id   := dupp_record.correspondence_id;


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
             cpy.card_no,
             0,0,0,1,
             g_date,
             1,g_date
      from   stg_c2_wod_correspond_dtl_cpy cpy

       where not exists
      (select /*+ nl_aj */ * from fnd_customer_card
       where  card_no         = cpy.card_no )
       and    sys_process_code    = 'N'
       and    cpy.card_no is not null;

       g_recs_dummy := g_recs_dummy + sql%rowcount;
       COMMIT;
 --******************************************************************************
--******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             DISTINCT
             cpy.vision_account_no	,
             0,
             1,
             g_date,
             1
      from   stg_c2_wod_correspond_dtl_cpy cpy

       where not exists
      (SELECT /*+ nl_aj */ * FROM FND_CUSTOMER_PRODUCT
       where  product_no          = cpy.vision_account_no )
       AND    SYS_PROCESS_CODE    = 'N'
       and    cpy.vision_account_no is not null ;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wod_correspond_detail fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	correspondence_id	,
            	cpy.	address_line1	,
            	cpy.	address_line2	,
            	cpy.	campaign_no	,
            	cpy.	card_ind	,
            	cpy.	card_no	,
            	cpy.	card_source	,
            	cpy.	cell_phone_no	,
            	cpy.	channel_ind	,
            	cpy.	child_id	,
            	cpy.	control_grp_ind	,
            	cpy.	correspondence_code	,
            	cpy.	correspondence_extract_date	,
            	cpy.	create_date	,
            	cpy.	customer_no	,
            	cpy.	distribution_status	,
            	cpy.	distrib_status_update_date	,
            	cpy.	email_address	,
            	cpy.	first_name	,
            	cpy.	initials	,
            	cpy.	language_desc	,
            	cpy.	surname	,
            	cpy.	permission_to_resend	,
            	cpy.	postal_code	,
            	cpy.	processing_ind	,
            	cpy.	product_code_no	,
            	cpy.	product	,
            	cpy.	promotion_id	,
            	cpy.	resend_period	,
            	cpy.	suburb	,
            	cpy.	title_desc	,
            	cpy.	vision_account_no	,
              g_date as last_updated_date
       from  stg_c2_wod_correspond_dtl_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_wod_correspond_detail
       where  correspondence_id   = cpy.correspondence_id
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



FOR upd_rec IN c_stg_c2_wod_correspond_dtl
   loop
     update fnd_wod_correspond_detail fnd
     set    fnd.	address_line1	=	upd_rec.	address_line1	,
            fnd.	address_line2	=	upd_rec.	address_line2	,
            fnd.	campaign_no	=	upd_rec.	campaign_no	,
            fnd.	card_ind	=	upd_rec.	card_ind	,
            fnd.	card_no	=	upd_rec.	card_no	,
            fnd.	card_source	=	upd_rec.	card_source	,
            fnd.	cell_phone_no	=	upd_rec.	cell_phone_no	,
            fnd.	channel_ind	=	upd_rec.	channel_ind	,
            fnd.	child_id	=	upd_rec.	child_id	,
            fnd.	control_grp_ind	=	upd_rec.	control_grp_ind	,
            fnd.	correspondence_code	=	upd_rec.	correspondence_code	,
            fnd.	correspondence_extract_date	=	upd_rec.	correspondence_extract_date	,
            fnd.	create_date	=	upd_rec.	create_date	,
            fnd.	customer_no	=	upd_rec.	customer_no	,
            fnd.	distribution_status	=	upd_rec.	distribution_status	,
            fnd.	distrib_status_update_date	=	upd_rec.	distrib_status_update_date	,
            fnd.	email_address	=	upd_rec.	email_address	,
            fnd.	first_name	=	upd_rec.	first_name	,
            fnd.	initials	=	upd_rec.	initials	,
            fnd.	language_desc	=	upd_rec.	language_desc	,
            fnd.	surname	=	upd_rec.	surname	,
            fnd.	permission_to_resend	=	upd_rec.	permission_to_resend	,
            fnd.	postal_code	=	upd_rec.	postal_code	,
            fnd.	processing_ind	=	upd_rec.	processing_ind	,
            fnd.	product_code_no	=	upd_rec.	product_code_no	,
            fnd.	product	=	upd_rec.	product	,
            fnd.	promotion_id	=	upd_rec.	promotion_id	,
            fnd.	resend_period	=	upd_rec.	resend_period	,
            fnd.	suburb	=	upd_rec.	suburb	,
            fnd.	title_desc	=	upd_rec.	title_desc	,
            fnd.	vision_account_no	=	upd_rec.	vision_account_no	,
            fnd.  last_updated_date         = g_date
     where  fnd.	correspondence_id	        =	upd_rec.	correspondence_id and
            (
            nvl(fnd.address_line1	,0) <>	upd_rec.	address_line1	or
            nvl(fnd.address_line2	,0) <>	upd_rec.	address_line2	or
            nvl(fnd.campaign_no	,0) <>	upd_rec.	campaign_no	or
            nvl(fnd.card_ind	,0) <>	upd_rec.	card_ind	or
            nvl(fnd.card_no	,0) <>	upd_rec.	card_no	or
            nvl(fnd.card_source	,0) <>	upd_rec.	card_source	or
            nvl(fnd.cell_phone_no	,0) <>	upd_rec.	cell_phone_no	or
            nvl(fnd.channel_ind	,0) <>	upd_rec.	channel_ind	or
            nvl(fnd.child_id	,0) <>	upd_rec.	child_id	or
            nvl(fnd.control_grp_ind	,0) <>	upd_rec.	control_grp_ind	or
            nvl(fnd.correspondence_code	,0) <>	upd_rec.	correspondence_code	or
            nvl(fnd.correspondence_extract_date	,'1 Jan 1900') <>	upd_rec.	correspondence_extract_date	or
            nvl(fnd.create_date	,'1 Jan 1900') <>	upd_rec.	create_date	or
            nvl(fnd.customer_no	,0) <>	upd_rec.	customer_no	or
            nvl(fnd.distribution_status	,0) <>	upd_rec.	distribution_status	or
            nvl(fnd.distrib_status_update_date	,'1 Jan 1900') <>	upd_rec.	distrib_status_update_date	or
            nvl(fnd.email_address	,0) <>	upd_rec.	email_address	or
            nvl(fnd.first_name	,0) <>	upd_rec.	first_name	or
            nvl(fnd.initials	,0) <>	upd_rec.	initials	or
            nvl(fnd.language_desc	,0) <>	upd_rec.	language_desc	or
            nvl(fnd.surname	,0) <>	upd_rec.	surname	or
            nvl(fnd.permission_to_resend	,0) <>	upd_rec.	permission_to_resend	or
            nvl(fnd.postal_code	,0) <>	upd_rec.	postal_code	or
            nvl(fnd.processing_ind	,0) <>	upd_rec.	processing_ind	or
            nvl(fnd.product_code_no	,0) <>	upd_rec.	product_code_no	or
            nvl(fnd.product	,0) <>	upd_rec.	product	or
            nvl(fnd.promotion_id	,0) <>	upd_rec.	promotion_id	or
            nvl(fnd.resend_period	,0) <>	upd_rec.	resend_period	or
            nvl(fnd.suburb	,0) <>	upd_rec.	suburb	or
            nvl(fnd.title_desc	,0) <>	upd_rec.	title_desc	or
            nvl(fnd.vision_account_no	,0) <>	upd_rec.	vision_account_no
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

     insert /*+ APPEND parallel (hsp,2) */ into stg_c2_wod_correspond_dtl_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'AN ACCOUNT/CARD WAS DETECTED ON THE TRANSACTION THAT HAS A DUMMY MASTER - INVESTIGATE!!',
            	cpy.	correspondence_id	,
            	cpy.	address_line1	,
            	cpy.	address_line2	,
            	cpy.	campaign_no	,
            	cpy.	card_ind	,
            	cpy.	card_no	,
            	cpy.	card_source	,
            	cpy.	cell_phone_no	,
            	cpy.	channel_ind	,
            	cpy.	child_id	,
            	cpy.	control_grp_ind	,
            	cpy.	correspondence_code	,
            	cpy.	correspondence_extract_date	,
            	cpy.	create_date	,
            	cpy.	customer_no	,
            	cpy.	distribution_status	,
            	cpy.	distrib_status_update_date	,
            	cpy.	email_address	,
            	cpy.	first_name	,
            	cpy.	initials	,
            	cpy.	language_desc	,
            	cpy.	surname	,
            	cpy.	permission_to_resend	,
            	cpy.	postal_code	,
            	cpy.	processing_ind	,
            	cpy.	product_code_no	,
            	cpy.	product	,
            	cpy.	promotion_id	,
            	cpy.	resend_period	,
            	cpy.	suburb	,
            	cpy.	title_desc	,
            	cpy.	vision_account_no
      FROM   stg_c2_wod_correspond_dtl_cpy cpy
      where

     ( 1 =
        (select nvl(dummy_ind,0) dummy_ind  from  fnd_customer_product prd
         where  cpy.vision_account_no	 = prd.product_no and
                cpy.vision_account_no  is not null )   or
       1 =
        (select nvl(dummy_ind,0) dummy_ind  from  fnd_customer_card crd
         where  cpy.card_no	    = crd.card_no and
                cpy.card_no     is not null )
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
    from   stg_c2_wod_correspond_dtl_cpy
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
--    update stg_c2_wod_correspond_dtl_cpy
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
end wh_fnd_cust_165u;
