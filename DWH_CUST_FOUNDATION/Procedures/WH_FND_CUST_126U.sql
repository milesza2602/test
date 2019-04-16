--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_126U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_126U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JULY 2016
--  Author:      Alastair de Wet
--  Purpose:     Create BASKET ITEM fact table in the foundation layer
--               with input ex staging table from AFRICA.
--  Tables:      Input  - stg_cust_int_basket_item_cpy
--               Output - fnd_cust_int_basket_item
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


g_location_no    stg_cust_int_basket_item_cpy.location_no%type;
g_tran_date      stg_cust_int_basket_item_cpy.tran_date%type;
g_till_no        stg_cust_int_basket_item_cpy.till_no%type;
g_tran_no        stg_cust_int_basket_item_cpy.tran_no%type;
g_item_seq_no    stg_cust_int_basket_item_cpy.item_seq_no%type;
g_item_no        stg_cust_int_basket_item_cpy.item_no%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_126U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET_ITEM EX AFRICAN DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_cust_int_basket_item_cpy
where (location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no)
in
(select location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no
from stg_cust_int_basket_item_cpy
group by location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no
having count(*) > 1)
order by location_no,
tran_date,
till_no,tran_no,item_seq_no,item_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_cust_int_basket_item is
select /*+ FULL(cpy)  parallel (4) */
              cpy.*
      from    stg_cust_int_basket_item_cpy cpy,
              fnd_cust_int_basket_item fnd
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
        update stg_cust_int_basket_item_cpy stg
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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into fnd_cust_int_basket_item fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	location_no	,
            	cpy.	till_no	,
            	cpy.	tran_no	,
            	cpy.	tran_date	,
            	cpy.	tran_time	,
            	cpy.	item_seq_no	,
            	cpy.	item_no	,
             	cpy.	dept_no	,
            	cpy.	tran_type	,
              cpy.	vat_rate_perc	,
              cpy.	item_tran_qty	,
            	cpy.	item_tran_selling	,
              cpy.	tran_amount	,
             	cpy.	discount_selling	,
            	cpy.	discount_perc	,
            	cpy.	fob	,
            	cpy.  landed_cost	,   
            	cpy.	excl_retail_selling_price	,
             g_date as last_updated_date
       from  stg_cust_int_basket_item_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_int_basket_item
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



FOR upd_rec IN c_stg_cust_int_basket_item
   loop
     update fnd_cust_int_basket_item fnd
     set    fnd.	tran_time	=	upd_rec.	tran_time	,
            fnd.	tran_type	=	upd_rec.	tran_type	,
            fnd.	dept_no	=	upd_rec.	dept_no	,
            fnd.	vat_rate_perc	=	upd_rec.	vat_rate_perc	,
            fnd.	item_tran_qty	=	upd_rec.	item_tran_qty,
            fnd.	item_tran_selling	=	upd_rec.	item_tran_selling	,
            fnd.	tran_amount	=	upd_rec.	tran_amount	,
            fnd.	discount_selling	=	upd_rec.	discount_selling	,
            fnd.	discount_perc	=	upd_rec.	discount_perc	,
            fnd.	fob =	upd_rec.	fob	,
            fnd.	landed_cost	=	upd_rec.	landed_cost	,
            fnd.	excl_retail_selling_price	=	upd_rec.	excl_retail_selling_price	,
            fnd.  last_updated_date   = g_date
     where  fnd.	location_no	      =	upd_rec.	location_no and
            fnd.	tran_date	        =	upd_rec.	tran_date	  and
            fnd.	till_no	          =	upd_rec.	till_no	    and
            fnd.	tran_no	          =	upd_rec.	tran_no	    and
            fnd.	item_seq_no	      =	upd_rec.	item_seq_no	    and
            fnd.	item_no	          =	upd_rec.	item_no	;

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
    from   stg_cust_int_basket_item_cpy
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
end wh_fnd_cust_126u;
