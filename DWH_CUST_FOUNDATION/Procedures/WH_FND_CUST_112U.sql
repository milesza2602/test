--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_112U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_112U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_collect fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_egn_cust_basket_tender_cpy
--               Output - fnd_cust_basket_tender
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


g_location_no       stg_egn_cust_basket_tender_cpy.location_no%type;
g_tran_date         stg_egn_cust_basket_tender_cpy.tran_date%type;
g_till_no           stg_egn_cust_basket_tender_cpy.till_no%type;
g_tran_no           stg_egn_cust_basket_tender_cpy.tran_no%type;
g_tender_type_code  stg_egn_cust_basket_tender_cpy.tender_type_code%type;
g_tender_seq_no     stg_egn_cust_basket_tender_cpy.tender_seq_no%type;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_112U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET TENDER EX ENGEN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_egn_cust_basket_tender_cpy
where (location_no,
tran_date,
till_no,tran_no,tender_type_code,tender_seq_no)
in
(select location_no,
tran_date,
till_no,tran_no,tender_type_code,tender_seq_no
from stg_egn_cust_basket_tender_cpy
group by location_no,
tran_date,
till_no,tran_no,tender_type_code,tender_seq_no
having count(*) > 1)
order by location_no,
tran_date,
till_no,tran_no,tender_type_code,tender_seq_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_egn_cust_basket_tender is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_egn_cust_basket_tender_cpy cpy,
              fnd_cust_basket_tender fnd
      where   cpy.location_no      = fnd.location_no and
              cpy.tran_date        = fnd.tran_date   and
              cpy.till_no          = fnd.till_no     and
              cpy.tran_no          = fnd.tran_no     and
              cpy.tender_type_code = fnd.tender_type_code     and
              cpy.tender_seq_no    = fnd.tender_seq_no     and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.location_no,
              cpy.tran_date,
              cpy.till_no,cpy.tran_no,cpy.tender_type_code,cpy.tender_seq_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_location_no      := 0;
   g_tran_date        := '1 Jan 2000';
   g_till_no          := 0;
   g_tran_no          := 0;
   g_tender_type_code := 0;
   g_tender_seq_no    := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.location_no       = g_location_no and
        dupp_record.tran_date         = g_tran_date and
        dupp_record.till_no           = g_till_no and
        dupp_record.tran_no           = g_tran_no and
        dupp_record.tender_type_code  = g_tender_type_code and
        dupp_record.tender_seq_no     = g_tender_seq_no then
        update stg_egn_cust_basket_tender_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_location_no       := dupp_record.location_no;
    g_tran_date         := dupp_record.tran_date;
    g_till_no           := dupp_record.till_no;
    g_tran_no           := dupp_record.tran_no;
    g_tender_type_code  := dupp_record.tender_type_code;
    g_tender_seq_no     := dupp_record.tender_seq_no;

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
     insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_card
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.tender_no,
             0,0,0,1,
             g_date,
             1,g_date
      from   stg_egn_cust_basket_tender_cpy cpy

       where not exists
      (select /*+ nl_aj */ * from fnd_customer_card
       where  card_no         = cpy.tender_no )
       and    sys_process_code    = 'N'
       and    cpy.tender_no is not null
       and    substr(cpy.tender_no,1,6) in (600785,400154,410374,410375,416454,416455)
       and    substr(cpy.tender_no,1,7) not in(6007854,6007855); 

       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;
--      insert /*+ APPEND parallel (fnd,2) */ into fnd_location fnd
--      select /*+ FULL(cpy)  parallel (cpy,2) */
--             distinct
--             cpy.location_no	,
--             'Dummy wh_fnd_cust_104u',
--             g_date,
--             1
--      from   stg_egn_cust_basket_tender_cpy cpy

--       where not exists
--      (select /*+ nl_aj */ * from fnd_location
--       where  location_no         = cpy.location_no )
--       and    sys_process_code    = 'N';

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_cust_basket_tender fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tender_type_code	,
            	cpy.	tender_seq_no	,
            	cpy.	tran_date	,
            	cpy.	tran_time	,
            	cpy.	tender_type_detail_code	,
            	cpy.	tender_no	,
            	cpy.	tender_selling	,
            	cpy.	change_tender_type	,
            	nvl(cpy.	change_selling,0) change_selling	,
            	cpy.	change_gift_no	,
            	cpy.	card_seq_no	,
            	cpy.	card_authorisation_id	,
            	cpy.	tender_valid_offline_ind	,
            	cpy.	payment_acc_type	,
            	cpy.	payment_acc_type_dtl_code	,
            	cpy.	payment_account_no	,
            	cpy.	loyalty_ww_swipe_no	,
            	cpy.	card_eps_no	,
            	cpy.	customer_no	,
            	nvl(cpy.	vitality_cust_ind,0) vitality_cust_ind	,
              g_date as last_updated_date,
              '','',
              cpy.	tender_selling	tender_selling_local,
              nvl(cpy.	change_selling,0) change_selling_local,'',''
       from  stg_egn_cust_basket_tender_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_basket_tender
       where  location_no       = cpy.location_no and
              tran_date         = cpy.tran_date   and
              till_no           = cpy.till_no     and
              tran_no           = cpy.tran_no     and
              tender_type_code  = cpy.tender_type_code  and
              tender_seq_no     = cpy.tender_seq_no
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



FOR upd_rec IN c_stg_egn_cust_basket_tender
   loop
     update fnd_cust_basket_tender fnd
     set    fnd.	tran_time	                =	upd_rec.	tran_time	,
            fnd.	tender_type_detail_code 	=	upd_rec.	tender_type_detail_code	,
            fnd.	tender_no	                =	upd_rec.	tender_no	,
            fnd.	tender_selling	          =	upd_rec.	tender_selling	,
            fnd.	change_tender_type	      =	upd_rec.	change_tender_type	,
            fnd.	change_selling	          =	nvl(upd_rec.	change_selling,0)	,
            fnd.	change_gift_no	          =	upd_rec.	change_gift_no	,
            fnd.	card_seq_no	              =	upd_rec.	card_seq_no	,
            fnd.	card_authorisation_id	    =	upd_rec.	card_authorisation_id	,
            fnd.	tender_valid_offline_ind	=	upd_rec.	tender_valid_offline_ind	,
            fnd.	payment_acc_type	        =	upd_rec.	payment_acc_type	,
            fnd.	payment_acc_type_dtl_code	=	upd_rec.	payment_acc_type_dtl_code	,
            fnd.	payment_account_no	      =	upd_rec.	payment_account_no	,
            fnd.	loyalty_ww_swipe_no     	=	upd_rec.	loyalty_ww_swipe_no	,
            fnd.	card_eps_no	              =	upd_rec.	card_eps_no	,
            fnd.	customer_no	              =	upd_rec.	customer_no	,
            fnd.	vitality_cust_ind	        =	nvl(upd_rec.vitality_cust_ind,0) 	,
            fnd.	TENDER_SELLING_LOCAL      =	upd_rec.	tender_selling,
            fnd.	CHANGE_SELLING_LOCAL	    =	nvl(upd_rec.	change_selling,0),
            fnd.  last_updated_date         = g_date
     where  fnd.	location_no	      =	upd_rec.	location_no and
            fnd.	tran_date	        =	upd_rec.	tran_date	  and
            fnd.	till_no	          =	upd_rec.	till_no	    and
            fnd.	tran_no	          =	upd_rec.	tran_no	    and
            fnd.  tender_type_code  = upd_rec.	tender_type_code  and
            fnd.	tender_seq_no     = upd_rec.	tender_seq_no and
            (
            nvl(fnd.tran_time	                ,0) <>	upd_rec.	tran_time	or
            nvl(fnd.tender_type_detail_code	  ,0) <>	upd_rec.	tender_type_detail_code	or
            nvl(fnd.tender_no	                ,0) <>	upd_rec.	tender_no	or
            nvl(fnd.tender_selling	          ,0) <>	upd_rec.	tender_selling	or
            nvl(fnd.change_tender_type	      ,0) <>	upd_rec.	change_tender_type	or
            nvl(fnd.change_selling	          ,0) <>	upd_rec.	change_selling	or
            nvl(fnd.change_gift_no	          ,0) <>	upd_rec.	change_gift_no	or
            nvl(fnd.card_seq_no	              ,0) <>	upd_rec.	card_seq_no	or
            nvl(fnd.card_authorisation_id	    ,0) <>	upd_rec.	card_authorisation_id	or
            nvl(fnd.tender_valid_offline_ind	,0) <>	upd_rec.	tender_valid_offline_ind	or
            nvl(fnd.payment_acc_type	        ,0) <>	upd_rec.	payment_acc_type	or
            nvl(fnd.payment_acc_type_dtl_code	,0) <>	upd_rec.	payment_acc_type_dtl_code	or
            nvl(fnd.payment_account_no	      ,0) <>	upd_rec.	payment_account_no	or
            nvl(fnd.loyalty_ww_swipe_no	      ,0) <>	upd_rec.	loyalty_ww_swipe_no	or
            nvl(fnd.card_eps_no	              ,0) <>	upd_rec.	card_eps_no	or
            nvl(fnd.customer_no	              ,0) <>	upd_rec.	customer_no	or
            nvl(fnd.vitality_cust_ind	        ,0) <>	upd_rec.	vitality_cust_ind
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

      insert /*+ APPEND parallel (hsp,2) */ into stg_egn_cust_basket_tender_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A LOCATION WAS DETECTED ON THE TRANSACTION THAT HAS NO MASTER - INVESTIGATE!!',
             	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tender_type_code	,
            	cpy.	tender_seq_no	,
            	cpy.	tran_date	,
            	cpy.	tran_time	,
            	cpy.	tender_type_detail_code	,
            	cpy.	tender_no	,
            	cpy.	tender_selling	,
            	cpy.	change_tender_type	,
            	cpy.	change_selling	,
            	cpy.	change_gift_no	,
            	cpy.	card_seq_no	,
            	cpy.	card_authorisation_id	,
            	cpy.	tender_valid_offline_ind	,
            	cpy.	payment_acc_type	,
            	cpy.	payment_acc_type_dtl_code	,
            	cpy.	payment_account_no	,
            	cpy.	loyalty_ww_swipe_no	,
            	cpy.	card_eps_no	,
            	cpy.	customer_no	,
            	cpy.	vitality_cust_ind

      FROM   stg_egn_cust_basket_tender_cpy cpy
      where
      not exists
      (select *
       from   fnd_location lcn
       where  cpy.location_no       = lcn.location_no )   AND

/*      ( 1 =
        (select dummy_ind  from  fnd_location loc
         where  cpy.location_no       = loc.location_no ) or
        1 =
        (select dummy_ind  from  fnd_item itm
         where  cpy.item_no       = itm.item_no )
      )
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---
     and
*/
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
    from   stg_egn_cust_basket_tender_cpy
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

    flagged_records_hospital;



--    Taken out for better performance --------------------
--    update stg_egn_cust_basket_tender_cpy
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
       raise;
end wh_fnd_cust_112u;
