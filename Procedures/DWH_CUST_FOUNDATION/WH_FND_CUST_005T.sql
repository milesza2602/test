--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_005T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_005T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JULY 2016
--  Author:      Alastair de Wet
--  Purpose:     Create CUSTOMER MASTER fact table in the foundation layer
--               with input ex staging table from AFRICA.
--  Tables:      Input  - STG_INT_CUST_TO
--               Output - fnd_int_customer
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



cursor c_stg_int_cust is
select /*+ FULL(cpy)  parallel (4) */
              cpy.*
      from    dwh_cust_foundation.STG_INT_CUST_TO cpy,
              fnd_int_customer fnd
      where   cpy.retailsoft_customer_no  = fnd.retailsoft_customer_no and
              cpy.loyalty_card_no         = fnd.loyalty_card_no  
      order by
              cpy.retailsoft_customer_no,
              cpy.loyalty_card_no ;






procedure flagged_records_update as
begin



FOR upd_rec IN c_stg_int_cust
   loop
     update fnd_int_customer fnd
     set    fnd.	country_code	=	upd_rec.	country_code	,
            fnd.	location_no	  =	upd_rec.	location_no	,
            fnd.	title_code	  =	upd_rec.	title_code	,
            fnd.	first_name	  =	upd_rec.	first_name	,
            fnd.	last_name	    =	upd_rec.	last_name	,
            fnd.	work_cell_no	=	upd_rec.	work_cell_no	,
            fnd.	home_email_address	=	upd_rec.	home_email_address	,
            fnd.	home_cell_no	      =	upd_rec.	home_cell_no	,
            fnd.	work_email_address	=	upd_rec.	work_email_address	,
            fnd.	talk_to_me_ind	    =	upd_rec.	talk_to_me_ind	,
            fnd.	product_active_ind	=	upd_rec.	product_active_ind	,
            fnd.	share_to_ww	        =	upd_rec.	share_to_ww	,
            fnd.	language	          =	upd_rec.	language	,
            fnd.	created_date	      =	upd_rec.	created_date	,
            fnd.  last_updated_date   = g_date
     where  fnd.	retailsoft_customer_no	 =	upd_rec.	retailsoft_customer_no and
            fnd.	loyalty_card_no	        =	upd_rec.	loyalty_card_no	  and
            (
            nvl(fnd.country_code	    ,0) <>	upd_rec.	country_code	or
            nvl(fnd.location_no	      ,0) <>	upd_rec.	location_no	or
            nvl(fnd.title_code	      ,0) <>	upd_rec.	title_code	or
            nvl(fnd.first_name	      ,0) <>	upd_rec.	first_name	or
            nvl(fnd.last_name	        ,0) <>	upd_rec.	last_name	or
            nvl(fnd.work_cell_no	    ,0) <>	upd_rec.	work_cell_no	or
            nvl(fnd.home_email_address,0) <>	upd_rec.	home_email_address	or
            nvl(fnd.home_cell_no	    ,0) <>	upd_rec.	home_cell_no	or
            nvl(fnd.work_email_address,0) <>	upd_rec.	work_email_address	or
            nvl(fnd.talk_to_me_ind	  ,0) <>	upd_rec.	talk_to_me_ind	or
            nvl(fnd.product_active_ind,0) <>	upd_rec.	product_active_ind	or
            nvl(fnd.share_to_ww	      ,0) <>	upd_rec.	share_to_ww	or
            nvl(fnd.language     	    ,0) <>	upd_rec.	language

            );

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



    select count(*)
    into   g_recs_read
    from   DWH_CUST_FOUNDATION.STG_INT_CUST_TO
    ;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

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
end wh_fnd_cust_005t;
