--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_024TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_024TEST" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create Dim customer_portfolio dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_c2_customer_portfolio_cpy
--               Output - fnd_customer_portfolio
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
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
g_truncate_count     integer       :=  0;


g_cust_portfolio_no       stg_c2_customer_portfolio_cpy.cust_portfolio_no%type;
g_customer_no        stg_c2_customer_portfolio_cpy.customer_no%type;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_024U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST PORTFOLIO MASTER EX C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_c2_customer_portfolio_cpy
where (cust_portfolio_no,customer_no)
in
(select cust_portfolio_no,customer_no
from stg_c2_customer_portfolio_cpy
group by cust_portfolio_no,customer_no
having count(*) > 1)
order by cust_portfolio_no,customer_no,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_c2_customer_portfolio is
select /*+ FULL(stg)  parallel (stg,2) */
              stg.cust_portfolio_no,
              stg.customer_no,
              stg.product_code_no,
              stg.portfolio_status_desc,
              stg.portfolio_create_date,
              stg.portfolio_change_date,
              stg.portfolio_close_date,
              stg.product_no,
              stg.portfolio_role_type_desc,
              stg.cust_prod_close_reason_code,
              stg.atg_customer_no,
              stg.partner_entity_no
      from    stg_c2_customer_portfolio_cpy stg,
              fnd_customer_portfolio fnd
      where   stg.cust_portfolio_no     = fnd.cust_portfolio_no and
              stg.customer_no           = fnd.customer_no     and
              stg.sys_process_code      = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              stg.cust_portfolio_no,stg.customer_no,stg.sys_source_batch_id,stg.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

   g_cust_portfolio_no     := 0;
   g_customer_no           := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.cust_portfolio_no   = g_cust_portfolio_no and
        dupp_record.customer_no         = g_customer_no     then
        update stg_c2_customer_portfolio_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;


    g_cust_portfolio_no    := dupp_record.cust_portfolio_no;
    g_customer_no          := dupp_record.customer_no;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_portfolio fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.cust_portfolio_no ,
             cpy.customer_no ,
             cpy.product_code_no,
             cpy.portfolio_status_desc,
             cpy.portfolio_create_date,
             cpy.portfolio_change_date,
             cpy.portfolio_close_date,
             cpy.product_no,
             cpy.portfolio_role_type_desc,
             cpy.cust_prod_close_reason_code,
             cpy.atg_customer_no,
             g_date as last_updated_date,
             cpy.partner_entity_no
      from   stg_c2_customer_portfolio_cpy cpy

       where     not exists
      (select /*+ nl_aj */  * from fnd_customer_portfolio
       where  cust_portfolio_no   = cpy.cust_portfolio_no and
              customer_no         = cpy.customer_no )
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



for upd_rec in c_stg_c2_customer_portfolio
   loop
     update fnd_customer_portfolio fnd
     set    fnd.product_code_no               = upd_rec.product_code_no,
            fnd.portfolio_status_desc         = upd_rec.portfolio_status_desc,
            fnd.portfolio_create_date         = upd_rec.portfolio_create_date,
            fnd.portfolio_change_date         = upd_rec.portfolio_change_date,
            fnd.portfolio_close_date          = upd_rec.portfolio_close_date,
            fnd.product_no                    = upd_rec.product_no,
            fnd.portfolio_role_type_desc      = upd_rec.portfolio_role_type_desc,
            fnd.cust_prod_close_reason_code   = upd_rec.cust_prod_close_reason_code,
            fnd.atg_customer_no               = upd_rec.atg_customer_no,
            fnd.partner_entity_no             = upd_rec.partner_entity_no,
            fnd.last_updated_date             = g_date
     where  fnd.cust_portfolio_no             = upd_rec.cust_portfolio_no and
            fnd.customer_no                   = upd_rec.customer_no  and
            (
            nvl(upd_rec.product_code_no             ,0) <> fnd.product_code_no or
            nvl(upd_rec.portfolio_status_desc       ,0) <> fnd.portfolio_status_desc or
            nvl(upd_rec.portfolio_create_date       ,'1 Jan 1900') <> fnd.portfolio_create_date or
            nvl(upd_rec.portfolio_change_date       ,'1 Jan 1900') <> fnd.portfolio_change_date or
            nvl(upd_rec.portfolio_close_date        ,'1 Jan 1900') <> fnd.portfolio_close_date or
            nvl(upd_rec.product_no                  ,0) <> fnd.product_no or
            nvl(upd_rec.portfolio_role_type_desc    ,0) <> fnd.portfolio_role_type_desc or
            nvl(upd_rec.cust_prod_close_reason_code ,0) <> fnd.cust_prod_close_reason_code or
            nvl(upd_rec.atg_customer_no             ,0) <> fnd.atg_customer_no or
            nvl(upd_rec.partner_entity_no           ,0) <> fnd.partner_entity_no
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

      insert /*+ APPEND parallel (hsp,2) */ into stg_c2_customer_portfolio_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.cust_portfolio_no ,
             cpy.customer_no ,
             cpy.product_code_no,
             cpy.portfolio_status_desc,
             cpy.portfolio_create_date,
             cpy.portfolio_change_date,
             cpy.portfolio_close_date,
             cpy.product_no,
             cpy.portfolio_role_type_desc,
             cpy.cust_prod_close_reason_code,
             cpy.atg_customer_no,
             cpy.partner_entity_no
      from   stg_c2_customer_portfolio_cpy cpy
      where
      (
      not exists
        (select * from  fnd_customer cust
         where  cpy.customer_no       = cust.customer_no ) or
      not exists
        (select * from  fnd_product pcd
         where  cpy.product_code_no       = pcd.product_code_no )
      )
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---
      and sys_process_code = 'N';

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

    select count(*)
    into   g_recs_read
    from   stg_c2_customer_portfolio_cpy
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
--    update stg_c2_customer_portfolio_cpy
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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
end wh_fnd_cust_024test;
