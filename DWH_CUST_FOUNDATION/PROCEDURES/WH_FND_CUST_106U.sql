--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_106U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_106U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_basket_aux fact table in the foundation layer
--               with input ex staging table from Engen.
--  Tables:      Input  - stg_pos_cust_basket_aux_cpy
--               Output - fnd_cust_basket_aux
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


g_location_no    stg_pos_cust_basket_aux_cpy.location_no%type;
g_tran_date      stg_pos_cust_basket_aux_cpy.tran_date%type;
g_till_no        stg_pos_cust_basket_aux_cpy.till_no%type;
g_tran_no        stg_pos_cust_basket_aux_cpy.tran_no%type;
g_aux_seq_no     stg_pos_cust_basket_aux_cpy.aux_seq_no%type;



g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_106U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET_AUX EX POS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_pos_cust_basket_aux_cpy
where (location_no,
tran_date,
till_no,tran_no,aux_seq_no)
in
(select location_no,
tran_date,
till_no,tran_no,aux_seq_no
from stg_pos_cust_basket_aux_cpy
group by location_no,
tran_date,
till_no,tran_no,aux_seq_no
having count(*) > 1)
order by location_no,
tran_date,
till_no,tran_no,aux_seq_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_pos_cust_basket_aux is
select /*+ FULL(cpy)   parallel (4) */
              cpy.*
      from    stg_pos_cust_basket_aux_cpy cpy,
              fnd_cust_basket_aux fnd
      where   cpy.location_no      = fnd.location_no and
              cpy.till_no          = fnd.till_no     and
              cpy.tran_no          = fnd.tran_no     and
              cpy.tran_date        = fnd.tran_date   and
              cpy.aux_seq_no       = fnd.aux_seq_no  and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.location_no,
              cpy.tran_date,
              cpy.till_no,cpy.tran_no,cpy.aux_seq_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_location_no  := 0;
   g_tran_date    := '1 Jan 2000';
   g_till_no      := 0;
   g_tran_no      := 0;
   g_aux_seq_no  := 0;


for dupp_record in stg_dup
   loop

    if  dupp_record.location_no   = g_location_no and
        dupp_record.tran_date     = g_tran_date and
        dupp_record.till_no       = g_till_no and
        dupp_record.tran_no       = g_tran_no and
        dupp_record.aux_seq_no   = g_aux_seq_no then
        update stg_pos_cust_basket_aux_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_location_no    := dupp_record.location_no;
    g_tran_date      := dupp_record.tran_date;
    g_till_no        := dupp_record.till_no;
    g_tran_no        := dupp_record.tran_no;
    g_aux_seq_no     := dupp_record.aux_seq_no;

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

 --     insert /*+ APPEND parallel (fnd,2) */ into fnd_item fnd
 --     select /*+ FULL(cpy)  parallel (cpy,2) */
 --            distinct
 --            cpy.	item_no	,
 --            'Dummy wh_fnd_cust_104u',
--             g_date,
--             1
--      from   stg_pos_cust_basket_aux_cpy cpy

--       where not exists
--      (select /*+ nl_aj */ * from fnd_item
--       where  item_no             = cpy.item_no )
--       and    sys_process_code    = 'N';

--       g_recs_dummy := g_recs_dummy + sql%rowcount;

--      commit;

--******************************************************************************

--******************************************************************************

--      insert /*+ APPEND parallel (fnd,2) */ into fnd_location fnd
--      select /*+ FULL(cpy)  parallel (cpy,2) */
--             distinct
--             cpy.location_no	,
--             'Dummy wh_fnd_cust_104u',
--             g_date,
--             1
--      from   stg_pos_cust_basket_aux_cpy cpy

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_cust_basket_aux fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tran_date	,
            	cpy.	aux_seq_no	,
            	cpy.	tran_type_code	,
            	cpy.	cust_name	,
            	cpy.	cust_tel_no	,
            	cpy.	price_overide_code	,
            	cpy.	ppc_code	,
            	cpy.	ppc_operator	,
            	cpy.	item_no	,
            	cpy.	promotion_no	,
            	substr(cpy.	loyalty_group,1,3) loyalty_group	,
            	cpy.	promotion_discount_amount	,
            	cpy.	loyalty_partner_id	,
            	cpy.	customer_no	,
            	cpy.	item_seq_no	,
            	cpy.	wreward_sales_value	,
            	cpy.	atg_customer_no	,
             g_date as last_updated_date,
              cpy.	EMPLOYEE_ID,
              cpy.	COMPANY_CODE,
              cpy.	PROMOTION_DISCOUNT_AMT_LOCAL,
              cpy.	WREWARD_SALES_VALUE_LOCAL,
              cpy.	FORM_FACTOR_IND_NO,
              cpy.	PAN_ENTRY_MODE_NO
       from  stg_pos_cust_basket_aux_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_basket_aux
       where  location_no    = cpy.location_no and
              tran_date      = cpy.tran_date   and
              till_no        = cpy.till_no     and
              tran_no        = cpy.tran_no     and
              aux_seq_no     = cpy.aux_seq_no
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



FOR upd_rec IN c_stg_pos_cust_basket_aux
   loop
     update fnd_cust_basket_aux fnd
     set    fnd.	tran_type_code	          =	upd_rec.	tran_type_code	,
            fnd.	cust_name	                =	upd_rec.	cust_name	,
            fnd.	cust_tel_no	              =	upd_rec.	cust_tel_no	,
            fnd.	price_overide_code	      =	upd_rec.	price_overide_code	,
            fnd.	ppc_code	                =	upd_rec.	ppc_code	,
            fnd.	ppc_operator	            =	upd_rec.	ppc_operator	,
            fnd.	item_no	                  =	upd_rec.	item_no	,
            fnd.	promotion_no	            =	upd_rec.	promotion_no	,
            fnd.	loyalty_group	            =	substr(upd_rec.	loyalty_group,1,3) 	,
            fnd.	promotion_discount_amount	=	upd_rec.	promotion_discount_amount	,
            fnd.	loyalty_partner_id	      =	upd_rec.	loyalty_partner_id	,
            fnd.	customer_no	              =	upd_rec.	customer_no	,
            fnd.	item_seq_no	              =	upd_rec.	item_seq_no	,
            fnd.	wreward_sales_value     	=	upd_rec.	wreward_sales_value	,
            fnd.	atg_customer_no	          =	upd_rec.	atg_customer_no	,
            fnd.	EMPLOYEE_ID	              =	upd_rec.	EMPLOYEE_ID	,
            fnd.	COMPANY_CODE              =	upd_rec.	COMPANY_CODE	,
            fnd.	PROMOTION_DISCOUNT_AMT_LOCAL  =	upd_rec.	PROMOTION_DISCOUNT_AMT_LOCAL	,
            fnd.	WREWARD_SALES_VALUE_LOCAL	=	upd_rec.	WREWARD_SALES_VALUE_LOCAL	,
            fnd.	FORM_FACTOR_IND_NO        =	upd_rec.	FORM_FACTOR_IND_NO	,
            fnd.	PAN_ENTRY_MODE_NO	        =	upd_rec.	PAN_ENTRY_MODE_NO	,
            fnd.  last_updated_date         = g_date
     where  fnd.	location_no	      =	upd_rec.	location_no and
            fnd.	tran_date	        =	upd_rec.	tran_date	  and
            fnd.	till_no	          =	upd_rec.	till_no	    and
            fnd.	tran_no	          =	upd_rec.	tran_no	    and
            fnd.	aux_seq_no	      =	upd_rec.	aux_seq_no	    and
            (
            nvl(fnd.tran_type_code	          ,0) <>	upd_rec.	tran_type_code	or
            nvl(fnd.cust_name	                ,0) <>	upd_rec.	cust_name	or
            nvl(fnd.cust_tel_no	              ,0) <>	upd_rec.	cust_tel_no	or
            nvl(fnd.price_overide_code	      ,0) <>	upd_rec.	price_overide_code	or
            nvl(fnd.ppc_code	                ,0) <>	upd_rec.	ppc_code	or
            nvl(fnd.ppc_operator	            ,0) <>	upd_rec.	ppc_operator	or
            nvl(fnd.item_no	                  ,0) <>	upd_rec.	item_no	or
            nvl(fnd.promotion_no            	,0) <>	upd_rec.	promotion_no	or
            nvl(fnd.loyalty_group	            ,0) <>	upd_rec.	loyalty_group	or
            nvl(fnd.promotion_discount_amount	,0) <>	upd_rec.	promotion_discount_amount	or
            nvl(fnd.loyalty_partner_id	      ,0) <>	upd_rec.	loyalty_partner_id	or
            nvl(fnd.customer_no	              ,0) <>	upd_rec.	customer_no	or
            nvl(fnd.item_seq_no	              ,0) <>	upd_rec.	item_seq_no	or
            nvl(fnd.wreward_sales_value	      ,0) <>	upd_rec.	wreward_sales_value	or
            nvl(fnd.atg_customer_no	          ,0) <>	upd_rec.	atg_customer_no or
            nvl(fnd.EMPLOYEE_ID             	,0) <>	upd_rec.	EMPLOYEE_ID or
            nvl(fnd.COMPANY_CODE     	        ,0) <>	upd_rec.	COMPANY_CODE or
            nvl(fnd.FORM_FACTOR_IND_NO        ,0) <>	upd_rec.	FORM_FACTOR_IND_NO or
            nvl(fnd.PAN_ENTRY_MODE_NO     	  ,0) <>	upd_rec.	PAN_ENTRY_MODE_NO 

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

      insert /*+ APPEND parallel (hsp,2) */ into stg_pos_cust_basket_aux_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'AN ITEM/LOCATION WAS DETECTED ON THE TRANSACTION WITH NO MASTER - INVESTIGATE!!',
            	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tran_date	,
            	cpy.	aux_seq_no	,
            	cpy.	tran_type_code	,
            	cpy.	cust_name	,
            	cpy.	cust_tel_no	,
            	cpy.	price_overide_code	,
            	cpy.	ppc_code	,
            	cpy.	ppc_operator	,
            	cpy.	item_no	,
            	cpy.	promotion_no	,
            	cpy.	loyalty_group	,
            	cpy.	promotion_discount_amount	,
            	cpy.	loyalty_partner_id	,
            	cpy.	customer_no	,
            	cpy.	item_seq_no	,
            	cpy.	wreward_sales_value	,
            	cpy.	atg_customer_no,
              cpy.	EMPLOYEE_ID,
              cpy.	COMPANY_CODE,
              cpy.	PROMOTION_DISCOUNT_AMT_LOCAL,
              cpy.	WREWARD_SALES_VALUE_LOCAL,
              cpy.	FORM_FACTOR_IND_NO,
              cpy.	PAN_ENTRY_MODE_NO 
      from   stg_pos_cust_basket_aux_cpy cpy
      where
      not exists
      (select *
       from   fnd_item itm
       where  cpy.item_no       = itm.item_no ) and

      not exists
      (select *
       from   fnd_location loc
       where  cpy.location_no       = loc.location_no ) and

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
    from   stg_pos_cust_basket_aux_cpy
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
--    update stg_pos_cust_basket_aux_cpy
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
end wh_fnd_cust_106u;
