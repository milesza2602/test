--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_022U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_022U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        MAR 2015
--  Author:      Alastair de Wet
--               Create Dim _customer_card dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_c2_customer_card_cpy
--               Output - fnd_customer_card
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  07 Feb 2017  - Theo Filander
--                 Add card_create_date to  fnd_customer_card table 
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


g_card_no        stg_c2_customer_card_cpy.card_no%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_022U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER CARD DIM EX CUSTOMER CENTRAL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_c2_customer_card_cpy
where (card_no)
in
(select card_no
from stg_c2_customer_card_cpy
group by card_no
having count(*) > 1)
order by card_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_c2_customer_card is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_c2_customer_card_cpy cpy,
              fnd_customer_card fnd
      where   cpy.card_no       = fnd.card_no and
              cpy.sys_process_code = 'N' and
              (
              exists
                (select *
                from   fnd_product prd
                where  cpy.product_code_no   = prd.product_code_no )
              AND
              exists
                (select *
                from   fnd_customer cst
                where  cpy.customer_no       = cst.customer_no )
--              AND
--              exists
--                (select *
--                from   fnd_customer_product cp
--                where  cpy.product_no       = cp.product_no )
               )   and
               (
                 nvl(fnd.customer_no	            ,0) <>	cpy.	customer_no	or
                 nvl(fnd.product_no	              ,0) <>	cpy.	product_no	or
                 nvl(fnd.vision_customer_no	      ,0) <>	cpy.	vision_customer_no	or
                 nvl(fnd.product_code_no        	,0) <>	cpy.	product_code_no	 or
                 (fnd.card_create_date                <>	cpy.card_create_date or 
                   (fnd.card_create_date is NULL and 
                    cpy.card_create_date is not null)
                 ) or
                 fnd.dummy_ind                        <>  0
               )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.card_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_card_no   := 0;


for dupp_record in stg_dup
   loop

    if  dupp_record.card_no    = g_card_no  then
        update stg_c2_customer_card_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_card_no    := dupp_record.card_no;


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

--***********************************************************************************************
--IF THIS PROVES TO BE A PROBLEM IE. UNIQUE KEY CONSTRAINT ISSUES THEN RATHER REMOVE
--AND UN-COMMENT VALIDATION WHICH REJECTS MISSING PRODUCTS IN CURSOR, INSERT & HOSPITAL STATEMENT
--SEE AROUND CODE LINES 81, 209, 309 (6/3/2015)
--***********************************************************************************************
      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.product_no	,
             cpy.customer_no,
             cpy.product_code_no,
             g_date last_updated_date,
             1 dummy_ind
      from   stg_c2_customer_card_cpy cpy

       where not exists
      (select /*+ nl_aj */ * from fnd_customer_product
       where  product_no              = cpy.product_no )
       and    sys_process_code    = 'N' and
              (
              exists
                (select *
                from   fnd_product prd
                where  cpy.product_code_no   = prd.product_code_no )  AND
              exists
                (select *
                from   fnd_customer cst
                where  cpy.customer_no       = cst.customer_no )
               )  ;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_card fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	card_no	,
            	cpy.	customer_no	,
             	cpy.	product_no	,
             	cpy.	vision_customer_no	,
            	cpy.	product_code_no	,
              g_date as last_updated_date,
              0 as dummy_ind,
              cpy.	card_create_date	
       from  stg_c2_customer_card_cpy cpy,
             fnd_customer cst,
--             fnd_customer_product cp,
             fnd_product  prd
       where cpy.customer_no     = cst.customer_no and
             cpy.product_code_no = prd.product_code_no and
--             cpy.product_no      = cp.product_no and
       not exists
      (select /*+ nl_aj */ * from fnd_customer_card
       where  card_no     = cpy.card_no  )
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



FOR upd_rec IN c_stg_c2_customer_card
   loop
     update fnd_customer_card fnd
     set    fnd.	customer_no	        =	upd_rec.	customer_no	,
            fnd.	product_no	        =	upd_rec.	product_no	,
            fnd.	vision_customer_no	=	upd_rec.	vision_customer_no	,
            fnd.	product_code_no	    =	upd_rec.	product_code_no	,
            fnd.	dummy_ind	          =	0	,
            fnd.  last_updated_date   = g_date,
            fnd.  card_create_date    = upd_rec.	card_create_date	
     where  fnd.	card_no	            =	upd_rec.	card_no;


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

      insert /*+ APPEND parallel (hsp,2) */ into stg_c2_customer_card_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A COLUMN WAS DETECTED ON THE TRANSACTION THAT HAS NO MASTER - INVESTIGATE!!',
             	cpy.	card_no	,
             	cpy.	customer_no	,
              cpy.	product_no	,
              cpy.	vision_customer_no	,
            	cpy.	product_code_no,
              cpy.	card_create_date
      from   stg_c2_customer_card_cpy cpy
      where
        (
        not exists
          (select *
           from   fnd_product prd
           where  cpy.product_code_no   = prd.product_code_no )
        OR
        not exists
           (select *
           from   fnd_customer cst
           where  cpy.customer_no       = cst.customer_no )
--        OR
--        not exists
--           (select *
--           from   fnd_customer_product cp
--           where  cpy.product_no       = cp.product_no )
         )
         AND sys_process_code = 'N';


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
-- Look up batch date from dim_control
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
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
    from   stg_c2_customer_card_cpy
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
--    update stg_c2_customer_card_cpy
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

/*
   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;
*/

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
end wh_fnd_cust_022u;
