set define off

create or replace PROCEDURE                                                                                                                    "DWH_CUST_FOUNDATION"."WH_FND_CUST_005U"      (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        JULY 2016
--  Author:      Alastair de Wet
--  Purpose:     Create CUSTOMER MASTER fact table in the foundation layer
--               with input ex staging table from AFRICA.
--  Tables:      Input  - stg_int_cust_cpy
--               Output - fnd_int_customer
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  added this comment for testing purposes
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
--  Add this comment to check the web portal checkins
--  Add another comment to check release
--  Changes not reflecting on the database
--  Add more changes
--**************************************************************************************************


g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
/*Added this variable for testing tfs functionality*/
g_dummy_var			 integer	   :=  0;


g_retailsoft_customer_no    stg_int_cust_cpy.retailsoft_customer_no%type;
g_loyalty_card_no           stg_int_cust_cpy.loyalty_card_no%type;
 
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_005U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST MASTER EX AFRICAN DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_int_cust_cpy
where (retailsoft_customer_no,
loyalty_card_no)
in
(select retailsoft_customer_no,
loyalty_card_no
from stg_int_cust_cpy
group by retailsoft_customer_no,
loyalty_card_no
having count(*) > 1)
order by retailsoft_customer_no,
loyalty_card_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_int_cust is
select /*+ FULL(cpy)  parallel (4) */
              cpy.*
      from    stg_int_cust_cpy cpy,
              fnd_int_customer fnd
      where   cpy.retailsoft_customer_no  = fnd.retailsoft_customer_no and
              cpy.loyalty_card_no         = fnd.loyalty_card_no   and
              cpy.sys_process_code        = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.retailsoft_customer_no,
              cpy.loyalty_card_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_retailsoft_customer_no  := '0';
   g_loyalty_card_no         := 0;


for dupp_record in stg_dup
   loop

    if  dupp_record.retailsoft_customer_no   = g_retailsoft_customer_no and
        dupp_record.loyalty_card_no     = g_loyalty_card_no then
        update stg_int_cust_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_retailsoft_customer_no    := dupp_record.retailsoft_customer_no;
    g_loyalty_card_no           := dupp_record.loyalty_card_no;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_int_customer fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	retailsoft_customer_no	,
             	cpy.	loyalty_card_no	,
            	cpy.	country_code	,
            	cpy.	location_no,
            	cpy.	title_code	,
             	cpy.	first_name,
            	cpy.	last_name	,
              cpy.	work_cell_no	,
              cpy.	home_email_address,
            	cpy.	home_cell_no	,
              cpy.	work_email_address	,
             	cpy.	talk_to_me_ind	,
            	cpy.	product_active_ind	,
            	cpy.	share_to_ww	,
            	cpy.  language,   
            	cpy.	created_date	,
             g_date as last_updated_date
       from  stg_int_cust_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_int_customer
       where  retailsoft_customer_no = cpy.retailsoft_customer_no and
              loyalty_card_no        = cpy.loyalty_card_no     )
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



FOR upd_rec IN c_stg_int_cust
   loop
     update fnd_int_customer fnd
     set    fnd.	country_code	=	upd_rec.	country_code	,
            fnd.	location_no	=	upd_rec.	location_no	,
            fnd.	title_code	=	upd_rec.	title_code	,
            fnd.	first_name	=	upd_rec.	first_name	,
            fnd.	last_name	=	upd_rec.	last_name	,
            fnd.	work_cell_no	=	upd_rec.	work_cell_no	,
            fnd.	home_email_address	=	upd_rec.	home_email_address	,
            fnd.	home_cell_no	=	upd_rec.	home_cell_no	,
            fnd.	work_email_address	=	upd_rec.	work_email_address	,
            fnd.	talk_to_me_ind	=	upd_rec.	talk_to_me_ind	,
            fnd.	product_active_ind	=	upd_rec.	product_active_ind	,
            fnd.	share_to_ww	=	upd_rec.	share_to_ww	,
            fnd.	language	=	upd_rec.	language	,
            fnd.	created_date	=	upd_rec.	created_date	,
            fnd.  last_updated_date   = g_date
     where  fnd.	retailsoft_customer_no	      =	upd_rec.	retailsoft_customer_no and
            fnd.	loyalty_card_no	        =	upd_rec.	loyalty_card_no	  ;

      g_recs_updated := g_recs_updated + sql%rowcount;
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
    from   stg_int_cust_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;






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
end wh_fnd_cust_005u;
/
show errors