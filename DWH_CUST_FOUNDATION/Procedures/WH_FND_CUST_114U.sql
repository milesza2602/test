--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_114U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_114U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_collect fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_egn_cust_basket_item_cpy
--               Output - fnd_cust_basket_item
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


g_location_no    stg_egn_cust_basket_item_cpy.location_no%type;
g_tran_date      stg_egn_cust_basket_item_cpy.tran_date%type;
g_till_no        stg_egn_cust_basket_item_cpy.till_no%type;
g_tran_no        stg_egn_cust_basket_item_cpy.tran_no%type;
g_item_seq_no    stg_egn_cust_basket_item_cpy.item_seq_no%type;
g_item_no        stg_egn_cust_basket_item_cpy.item_no%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_114U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET_ITEM EX ENGEN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_egn_cust_basket_item_cpy
where (location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no)
in
(select location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no
from stg_egn_cust_basket_item_cpy
group by location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no
having count(*) > 1)
order by location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_pos_cust_basket_item is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_egn_cust_basket_item_cpy cpy,
              fnd_cust_basket_item fnd
      where   cpy.location_no      = fnd.location_no and
              cpy.tran_date        = fnd.tran_date   and
              cpy.till_no          = fnd.till_no     and
              cpy.tran_no          = fnd.tran_no     and
              cpy.item_seq_no      = fnd.item_seq_no and
              cpy.item_no          = fnd.item_no     and
              cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.location_no,
              cpy.tran_date,
              cpy.till_no,cpy.tran_no,cpy.item_seq_no,cpy.item_no,
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
   g_item_seq_no  := 0;
   g_item_no      := 0;


for dupp_record in stg_dup
   loop

    if  dupp_record.location_no   = g_location_no and
        dupp_record.tran_date     = g_tran_date and
        dupp_record.till_no       = g_till_no and
        dupp_record.tran_no       = g_tran_no and
        dupp_record.item_seq_no   = g_item_seq_no and
        dupp_record.item_no       = g_item_no then
        update stg_egn_cust_basket_item_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_location_no    := dupp_record.location_no;
    g_tran_date      := dupp_record.tran_date;
    g_till_no        := dupp_record.till_no;
    g_tran_no        := dupp_record.tran_no;
    g_item_no        := dupp_record.item_no;
    g_item_seq_no    := dupp_record.item_seq_no;

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
--      from   stg_egn_cust_basket_item_cpy cpy

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
--      from   stg_egn_cust_basket_item_cpy cpy

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_cust_basket_item fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tran_date	,
            	cpy.	tran_time	,
            	cpy.	item_seq_no	,
            	cpy.	item_no	,
            	cpy.	tran_type	,
            	cpy.	item_tran_selling	,
            	cpy.	vat_rate_perc	,
            	cpy.	item_tran_qty	,
            	nvl(cpy.	discount_selling,0) discount_selling	,
            	cpy.	customer_no	,
            	cpy.	dept_no	,
            	cpy.	item_input_code	,
            	cpy.	waste_discount_selling	,
            	cpy.	return_reason_code	,
            	cpy.	item_type_code	,
            	cpy.	item_ref_code	,
            	cpy.	serial_no	,
            	cpy.	gift_card_type	,
            	nvl(cpy.	vitality_cust_ind,0) vitality_cust_ind	,
            	cpy.	vitality_uda_value	,
            	cpy.	green_value	,
            	cpy.	green_factor	,
             g_date as last_updated_date,
             '','',
              cpy.	ITEM_TRAN_SELLING as ITEM_TRAN_SELLING_LOCAL,
              nvl(cpy.	discount_selling,0) as DISCOUNT_SELLING_LOCAL,
              cpy.	WASTE_DISCOUNT_SELLING as WASTE_DISCOUNT_SELLING_LOCAL,
              cpy.	GREEN_VALUE as GREEN_VALUE_LOCAL
       from  stg_egn_cust_basket_item_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_basket_item
       where  location_no    = cpy.location_no and
              tran_date      = cpy.tran_date   and
              till_no        = cpy.till_no     and
              tran_no        = cpy.tran_no     and
              item_seq_no    = cpy.item_seq_no and
              item_no        = cpy.item_no
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



FOR upd_rec IN c_stg_pos_cust_basket_item
   loop
     update fnd_cust_basket_item fnd
     set    fnd.	tran_time	              =	upd_rec.	tran_time	,
            fnd.	tran_type	              =	upd_rec.	tran_type	,
            fnd.	item_tran_selling	      =	upd_rec.	item_tran_selling	,
            fnd.	vat_rate_perc	          =	upd_rec.	vat_rate_perc	,
            fnd.	item_tran_qty	          =	upd_rec.	item_tran_qty	,
            fnd.	discount_selling	      =	nvl(upd_rec.discount_selling,0)	,
            fnd.	customer_no	            =	upd_rec.	customer_no	,
            fnd.	dept_no	                =	upd_rec.	dept_no	,
            fnd.	item_input_code	        =	upd_rec.	item_input_code	,
            fnd.	waste_discount_selling	=	upd_rec.	waste_discount_selling	,
            fnd.	return_reason_code	    =	upd_rec.	return_reason_code	,
            fnd.	item_type_code	        =	upd_rec.	item_type_code	,
            fnd.	item_ref_code	          =	upd_rec.	item_ref_code	,
            fnd.	serial_no	              =	upd_rec.	serial_no	,
            fnd.	gift_card_type	        =	upd_rec.	gift_card_type	,
            fnd.	vitality_cust_ind	      =	nvl(upd_rec.vitality_cust_ind,0) 	,
            fnd.	vitality_uda_value	    =	upd_rec.	vitality_uda_value	,
            fnd.	green_value	            =	upd_rec.	green_value	,
            fnd.	green_factor	          =	upd_rec.	green_factor	,
            fnd.	ITEM_TRAN_SELLING_LOCAL =	upd_rec.	ITEM_TRAN_SELLING	,
            fnd.	DISCOUNT_SELLING_LOCAL	=	nvl(upd_rec.discount_selling,0)	,
            fnd.	WASTE_DISCOUNT_SELLING_LOCAL = upd_rec.	WASTE_DISCOUNT_SELLING	,
            fnd.	GREEN_VALUE_LOCAL	      =	upd_rec.	GREEN_VALUE	,
            fnd.  last_updated_date       = g_date
     where  fnd.	location_no	      =	upd_rec.	location_no and
            fnd.	tran_date	        =	upd_rec.	tran_date	  and
            fnd.	till_no	          =	upd_rec.	till_no	    and
            fnd.	tran_no	          =	upd_rec.	tran_no	    and
            fnd.	item_seq_no	      =	upd_rec.	item_seq_no	    and
            fnd.	item_no	          =	upd_rec.	item_no	  and
            (
            nvl(fnd.tran_type	              ,0) <>	upd_rec.	tran_type	or
            nvl(fnd.item_tran_selling	      ,0) <>	upd_rec.	item_tran_selling	or
            nvl(fnd.vat_rate_perc	          ,0) <>	upd_rec.	vat_rate_perc	or
            nvl(fnd.item_tran_qty	          ,0) <>	upd_rec.	item_tran_qty	or
            nvl(fnd.discount_selling	      ,0) <>	upd_rec.	discount_selling	or
            nvl(fnd.customer_no	            ,0) <>	upd_rec.	customer_no	or
            nvl(fnd.dept_no	                ,0) <>	upd_rec.	dept_no	or
            nvl(fnd.item_input_code	        ,0) <>	upd_rec.	item_input_code	or
            nvl(fnd.waste_discount_selling	,0) <>	upd_rec.	waste_discount_selling	or
            nvl(fnd.return_reason_code	    ,0) <>	upd_rec.	return_reason_code	or
            nvl(fnd.item_type_code	        ,0) <>	upd_rec.	item_type_code	or
            nvl(fnd.item_ref_code	          ,0) <>	upd_rec.	item_ref_code	or
            nvl(fnd.serial_no	              ,0) <>	upd_rec.	serial_no	or
            nvl(fnd.gift_card_type        	,0) <>	upd_rec.	gift_card_type	or
            nvl(fnd.vitality_cust_ind	      ,0) <>	upd_rec.	vitality_cust_ind	or
            nvl(fnd.vitality_uda_value	    ,0) <>	upd_rec.	vitality_uda_value	or
            nvl(fnd.green_value	            ,0) <>	upd_rec.	green_value	or
            nvl(fnd.green_factor	          ,0) <>	upd_rec.	green_factor
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

      insert /*+ APPEND parallel (hsp,2) */ into stg_egn_cust_basket_item_hsp hsp
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
            	cpy.	tran_time	,
            	cpy.	item_seq_no	,
            	cpy.	item_no	,
            	cpy.	tran_type	,
            	cpy.	item_tran_selling	,
            	cpy.	vat_rate_perc	,
            	cpy.	item_tran_qty	,
            	cpy.	discount_selling	,
            	cpy.	customer_no	,
            	cpy.	dept_no	,
            	cpy.	item_input_code	,
            	cpy.	waste_discount_selling	,
            	cpy.	return_reason_code	,
            	cpy.	item_type_code	,
            	cpy.	item_ref_code	,
            	cpy.	serial_no	,
            	cpy.	gift_card_type	,
            	cpy.	vitality_cust_ind	,
            	cpy.	vitality_uda_value	,
            	cpy.	green_value	,
            	cpy.	green_factor
      FROM   stg_egn_cust_basket_item_cpy cpy
      where
      not exists
      (select *
       from   fnd_item itm
       where  cpy.item_no       = itm.item_no )   AND

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
    from   stg_egn_cust_basket_item_cpy
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
--    update stg_egn_cust_basket_item_cpy
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
end wh_fnd_cust_114u;
