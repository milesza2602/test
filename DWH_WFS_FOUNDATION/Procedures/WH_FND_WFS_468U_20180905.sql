--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_468U_20180905
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_468U_20180905" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: Card Track and Trace - Load DSV card-in-hand data
-- Tran_type: ODWH: DSVCRD      AIT: DSVCRD   
--
-- Date:        2017-11-07
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_CARD_TRACKNTRACE in the Foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_DSV_CARD_TRACKNTRACE_CPY
--              Output - FND_WFS_CARD_TRACKNTRACE
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2017-11-07  N Chauhan - created.
--  2018-02-23  N Chauhan - fix record count check.
--  2018-02-23  N Chauhan - exclude duplicate stg recs in the updated records insert section
--  2018-07-24  S Ismail  - Removing information_date check - no need to insert new record if only information_date has changed 
--  2018-07-24  N Chauhan - latest check query restructured for better performance
--  2018-07-25  N Chauhan - restructure to run multiple information_date's one at a time
--  2018-08-14  N Chauhan - fix row count check error for multiple batch loads.

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;
g_recs_existing      integer       :=  0;
g_recs_new           integer       :=  0;
g_recs_revised       integer       :=  0;
g_day_new            integer       :=  0;
g_day_revised        integer       :=  0;
g_day_nochange       integer       :=  0;

g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_DSV_CARD_TRACKNTRACE_CPY.CARD_NO%type;
g_unique_key2_field_val  DWH_WFS_FOUNDATION.STG_DSV_CARD_TRACKNTRACE_CPY.DSV_UNIQUE_REFERENCE_NO%type;
g_unique_key3_field_val  DWH_WFS_FOUNDATION.STG_DSV_CARD_TRACKNTRACE_CPY.CUSTOMER_ID_NO%type;
g_unique_key4_field_val  DWH_WFS_FOUNDATION.STG_DSV_CARD_TRACKNTRACE_CPY.INFORMATION_DATE%type;
--

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_468U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CARD TRACK AND TRACE - LOAD DSV CARD-IN-HAND DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_dup is
   select * from  dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy
   where (    card_no, dsv_unique_reference_no, customer_id_no, information_date  )
   in
   (select     card_no, dsv_unique_reference_no, customer_id_no, information_date 
    from dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy
    group by      card_no, dsv_unique_reference_no, customer_id_no, information_date
    having count(*) > 1) 
   order by
    card_no, dsv_unique_reference_no, customer_id_no, information_date
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_info_date is
   select distinct information_date from  dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy
   where sys_process_code = 'N'
   order by information_date;   



--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
-- latest is kept, determined by sort in cursor
begin

   g_unique_key1_field_val   := 0;
   g_unique_key2_field_val   := 0;
   g_unique_key3_field_val   := 0;
   g_unique_key4_field_val   := '01/Jan/1900';
   --

   for dupp_record in c_stg_dup
    loop
       if 
               dupp_record.card_no  = g_unique_key1_field_val
           and dupp_record.dsv_unique_reference_no  = g_unique_key2_field_val
           and dupp_record.customer_id_no  = g_unique_key3_field_val
           and dupp_record.information_date  = g_unique_key4_field_val
          --
       then 
        update dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val :=  dupp_record.card_no;
       g_unique_key2_field_val :=  dupp_record.dsv_unique_reference_no;
       g_unique_key3_field_val :=  dupp_record.customer_id_no;
       g_unique_key4_field_val :=  dupp_record.information_date;
       --

    end loop;

   commit;

exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert (date_to_do DATE) as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ append parallel (fnd,2) */ into fnd_wfs_card_trackntrace fnd
          (
         card_no ,
         dsv_unique_reference_no ,
         customer_id_no ,
         information_date ,
         customer_name ,
         card_type ,
         mobile_no ,
         office_phone_no ,
         home_phone_no ,
         card_embossed_date ,
         dsv_card_received_date ,
         days_elapsed_dsv_received ,
         delivered_date ,
         branch_code_and_name ,
         days_elapsed_dsv_delivered ,
         received_by_store_date ,
         days_elapsed_store_received ,
         cust_collect_at_store_datetime ,
         days_elapsed_store_collect ,
         pending_destruction_datetime ,
         days_elapsed_pending_destr ,
         dsv_collect_frm_store_datetime ,
         card_destroyed_datetime ,
         days_elapsed_card_destroy ,
         sms_datetime ,
         days_in_store_current ,
         days_in_store_no_longer ,
         safe_in_date ,
         thermal_in_date ,
         operator_no ,
         overseas_ind ,
         delivery_type ,
         wfs_product_desc ,
         fica_code ,
         fica_instructions ,
         fica_instructions_desc ,
         card_status ,
         last_updated_date
         )


      SELECT /*+ FULL(cpy)  parallel (cpy,2) parallel(fnd,2) */
         cpy. card_no ,
         cpy. dsv_unique_reference_no ,
         cpy. customer_id_no ,
         cpy. information_date ,
         cpy. customer_name ,
         cpy. card_type ,
         cpy. mobile_no ,
         cpy. office_phone_no ,
         cpy. home_phone_no ,
         cpy. card_embossed_date ,
         cpy. dsv_card_received_date ,
         cpy. days_elapsed_dsv_received ,
         cpy. delivered_date ,
         cpy. branch_code_and_name ,
         cpy. days_elapsed_dsv_delivered ,
         cpy. received_by_store_date ,
         cpy. days_elapsed_store_received ,
         cpy. cust_collect_at_store_datetime ,
         cpy. days_elapsed_store_collect ,
         cpy. pending_destruction_datetime ,
         cpy. days_elapsed_pending_destr ,
         cpy. dsv_collect_frm_store_datetime ,
         cpy. card_destroyed_datetime ,
         cpy. days_elapsed_card_destroy ,
         cpy. sms_datetime ,
         cpy. days_in_store_current ,
         cpy. days_in_store_no_longer ,
         cpy. safe_in_date ,
         cpy. thermal_in_date ,
         cpy. operator_no ,
         cpy. overseas_ind ,
         cpy. delivery_type ,
         cpy. wfs_product_desc ,
         cpy. fica_code ,
         cpy. fica_instructions ,
         cpy. fica_instructions_desc ,
         cpy. card_status
         ,
         g_date as last_updated_date 

      from  dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy cpy
--
         left outer join dwh_wfs_foundation.fnd_wfs_card_trackntrace fnd on (
                 fnd.card_no  = cpy.card_no
             and fnd.dsv_unique_reference_no  = cpy.dsv_unique_reference_no
             and fnd.customer_id_no  = cpy.customer_id_no

-- insert only brand new card_no/dsv_unique_ref/cust_id recs,  so, exclude this key check 
--           and fnd.information_date  = cpy.information_date
--
             )
      where fnd.card_no is null

-- Any further validation goes in here - like xxx.ind in (0,1) ---  

       and cpy.information_date = date_to_do
       and cpy.sys_process_code = 'N'; 

      g_recs_new := g_recs_new + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;
      g_day_new := sql%rowcount;


      commit;




  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAGGED_RECORDS_INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAGGED_RECORDS_INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_insert;




--************************************************************************************************** 
-- Insert revised records (with different information_date) for card_no/dsv_unique_ref/cust_id recs
--   if any data has changed
--**************************************************************************************************


procedure flagged_records_insert_revised (date_to_do DATE) as

begin


      insert /*+ append parallel (fnd,2) */ into fnd_wfs_card_trackntrace fnd
          (
         card_no ,
         dsv_unique_reference_no ,
         customer_id_no ,
         information_date ,
         customer_name ,
         card_type ,
         mobile_no ,
         office_phone_no ,
         home_phone_no ,
         card_embossed_date ,
         dsv_card_received_date ,
         days_elapsed_dsv_received ,
         delivered_date ,
         branch_code_and_name ,
         days_elapsed_dsv_delivered ,
         received_by_store_date ,
         days_elapsed_store_received ,
         cust_collect_at_store_datetime ,
         days_elapsed_store_collect ,
         pending_destruction_datetime ,
         days_elapsed_pending_destr ,
         dsv_collect_frm_store_datetime ,
         card_destroyed_datetime ,
         days_elapsed_card_destroy ,
         sms_datetime ,
         days_in_store_current ,
         days_in_store_no_longer ,
         safe_in_date ,
         thermal_in_date ,
         operator_no ,
         overseas_ind ,
         delivery_type ,
         wfs_product_desc ,
         fica_code ,
         fica_instructions ,
         fica_instructions_desc ,
         card_status ,
         last_updated_date
         )

   With

      latest_existing_pk as ( 
     -- get pk's first   -- better performance than using analytical functions
         select /*+ parallel(cpy,4) full(cpy) parallel(fnd,4) */
             cpy.card_no,
             cpy.dsv_unique_reference_no,
             cpy.customer_id_no
             ,
             max(fnd.information_date) as information_date
          from  dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy cpy
                left outer join dwh_wfs_foundation.fnd_wfs_card_trackntrace fnd on (
                        fnd.card_no  = cpy.card_no
                    and fnd.dsv_unique_reference_no  = cpy.dsv_unique_reference_no
                    and fnd.customer_id_no  = cpy.customer_id_no
                    )
          where fnd.card_no is not null
            and sys_process_code = 'N'
        group by 
             cpy.card_no,
             cpy.dsv_unique_reference_no,
             cpy.customer_id_no   
      ),        
      latest_existing as (
         select /*+ parallel(fnd,2) full(fnd) parallel(lat,2) full(lat) */
          fnd.*
          from  dwh_wfs_foundation.fnd_wfs_card_trackntrace fnd
                inner join latest_existing_pk lat  on (
                        fnd.card_no  = lat.card_no
                    and fnd.dsv_unique_reference_no  = lat.dsv_unique_reference_no
                    and fnd.customer_id_no  = lat.customer_id_no
                    and fnd.information_date = lat.information_date
                    )
      )


      -- records, existing, but having revised values
      select /*+ parallel(upd_rec,2) full(upd_rec) parallel(fnd,2) full(fnd) */
         upd_rec.card_no ,
         upd_rec.dsv_unique_reference_no ,
         upd_rec.customer_id_no ,
         upd_rec.information_date ,
         upd_rec.customer_name ,
         upd_rec.card_type ,
         upd_rec.mobile_no ,
         upd_rec.office_phone_no ,
         upd_rec.home_phone_no ,
         upd_rec.card_embossed_date ,
         upd_rec.dsv_card_received_date ,
         upd_rec.days_elapsed_dsv_received ,
         upd_rec.delivered_date ,
         upd_rec.branch_code_and_name ,
         upd_rec.days_elapsed_dsv_delivered ,
         upd_rec.received_by_store_date ,
         upd_rec.days_elapsed_store_received ,
         upd_rec.cust_collect_at_store_datetime ,
         upd_rec.days_elapsed_store_collect ,
         upd_rec.pending_destruction_datetime ,
         upd_rec.days_elapsed_pending_destr ,
         upd_rec.dsv_collect_frm_store_datetime ,
         upd_rec.card_destroyed_datetime ,
         upd_rec.days_elapsed_card_destroy ,
         upd_rec.sms_datetime ,
         upd_rec.days_in_store_current ,
         upd_rec.days_in_store_no_longer ,
         upd_rec.safe_in_date ,
         upd_rec.thermal_in_date ,
         upd_rec.operator_no ,
         upd_rec.overseas_ind ,
         upd_rec.delivery_type ,
         upd_rec.wfs_product_desc ,
         upd_rec.fica_code ,
         upd_rec.fica_instructions ,
         upd_rec.fica_instructions_desc ,
         upd_rec.card_status 
         ,
         g_date as last_updated_date 


      from dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy upd_rec
            inner join latest_existing fnd on (
                    fnd.card_no  = upd_rec.card_no
                and fnd.dsv_unique_reference_no  = upd_rec.dsv_unique_reference_no
                and fnd.customer_id_no  = upd_rec.customer_id_no
                )
      where (
    --nvl(fnd. information_date, '01 JAN 1900') <> nvl(upd_rec. information_date, '01 JAN 1900') OR (no need to insert new record if only information date has changed)
         nvl(fnd. customer_name, 0) <> upd_rec. customer_name OR
         nvl(fnd. card_type, 0) <> upd_rec. card_type OR
         nvl(fnd. mobile_no, 0) <> upd_rec. mobile_no OR
         nvl(fnd. office_phone_no, 0) <> upd_rec. office_phone_no OR
         nvl(fnd. home_phone_no, 0) <> upd_rec. home_phone_no OR
         nvl(fnd. card_embossed_date, '01 JAN 1900') <> nvl(upd_rec. card_embossed_date, '01 JAN 1900') OR
         nvl(fnd. dsv_card_received_date, '01 JAN 1900') <> nvl(upd_rec. dsv_card_received_date, '01 JAN 1900') OR
    --nvl(fnd. days_elapsed_dsv_received, 0) <> upd_rec. days_elapsed_dsv_received OR
         nvl(fnd. delivered_date, '01 JAN 1900') <> nvl(upd_rec. delivered_date, '01 JAN 1900') OR
         nvl(fnd. branch_code_and_name, 0) <> upd_rec. branch_code_and_name OR
    --nvl(fnd. days_elapsed_dsv_delivered, 0) <> upd_rec. days_elapsed_dsv_delivered OR
         nvl(fnd. received_by_store_date, '01 JAN 1900') <> nvl(upd_rec. received_by_store_date, '01 JAN 1900') OR
    --nvl(fnd. days_elapsed_store_received, 0) <> upd_rec. days_elapsed_store_received OR
         nvl(fnd. cust_collect_at_store_datetime, '01 JAN 1900') <> nvl(upd_rec. cust_collect_at_store_datetime, '01 JAN 1900') OR
    --nvl(fnd. days_elapsed_store_collect, 0) <> upd_rec. days_elapsed_store_collect OR
         nvl(fnd. pending_destruction_datetime, '01 JAN 1900') <> nvl(upd_rec. pending_destruction_datetime, '01 JAN 1900') OR
    --nvl(fnd. days_elapsed_pending_destr, 0) <> upd_rec. days_elapsed_pending_destr OR
         nvl(fnd. dsv_collect_frm_store_datetime, '01 JAN 1900') <> nvl(upd_rec. dsv_collect_frm_store_datetime, '01 JAN 1900') OR
         nvl(fnd. card_destroyed_datetime, '01 JAN 1900') <> nvl(upd_rec. card_destroyed_datetime, '01 JAN 1900') OR
    --nvl(fnd. days_elapsed_card_destroy, 0) <> upd_rec. days_elapsed_card_destroy OR
         nvl(fnd. sms_datetime, '01 JAN 1900') <> nvl(upd_rec. sms_datetime, '01 JAN 1900') OR
    --nvl(fnd. days_in_store_current, 0) <> upd_rec. days_in_store_current OR
    --nvl(fnd. days_in_store_no_longer, 0) <> upd_rec. days_in_store_no_longer OR
         nvl(fnd. safe_in_date, '01 JAN 1900') <> nvl(upd_rec. safe_in_date, '01 JAN 1900') OR
         nvl(fnd. thermal_in_date, '01 JAN 1900') <> nvl(upd_rec. thermal_in_date, '01 JAN 1900') OR
         nvl(fnd. operator_no, 0) <> upd_rec. operator_no OR
         nvl(fnd. overseas_ind, 0) <> upd_rec. overseas_ind OR
         nvl(fnd. delivery_type, 0) <> upd_rec. delivery_type OR
         nvl(fnd. wfs_product_desc, 0) <> upd_rec. wfs_product_desc OR
         nvl(fnd. fica_code, 0) <> upd_rec. fica_code OR
         nvl(fnd. fica_instructions, 0) <> upd_rec. fica_instructions OR
         nvl(fnd. fica_instructions_desc, 0) <> upd_rec. fica_instructions_desc OR
         nvl(fnd. card_status, 0) <> upd_rec. card_status
         )  and
         nvl(fnd. information_date, '01 JAN 1900') < nvl(upd_rec. information_date, '01 JAN 1900')

         and upd_rec.information_date = date_to_do
         and upd_rec.sys_process_code = 'N'

      ;


   g_recs_revised := g_recs_revised + sql%rowcount;
   g_recs_inserted := g_recs_inserted + sql%rowcount;
   g_day_revised := sql%rowcount;

   commit;

   -- get count of stg recs that already have recs in fnd 
   -- to determine count of 'nochange' recs for logs
   With
   unique_existing as (
      select /*+ parallel(cpy,2) full(cpy) parallel(fnd,2)  */
         distinct
         cpy.card_no,
         cpy.dsv_unique_reference_no,
         cpy.customer_id_no,
         cpy.information_date
      from  dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy cpy
            left outer join dwh_wfs_foundation.fnd_wfs_card_trackntrace fnd on (
                    fnd.card_no  = cpy.card_no
                and fnd.dsv_unique_reference_no  = cpy.dsv_unique_reference_no
                and fnd.customer_id_no  = cpy.customer_id_no
                )
      where fnd.card_no is not null   -- existing in fnd
        and cpy.information_date = date_to_do
        and cpy.sys_process_code = 'N'
     )
     select 
     count(*) into g_recs_existing
     from unique_existing;

     g_day_nochange := g_recs_existing - g_day_revised;
     g_recs_nochange:= g_recs_nochange + g_day_nochange;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAGGED_RECORDS_INSERT_REVISED - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAGGED_RECORDS_INSERT_REVISED - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_insert_revised;




-- **** no updates to existing records required




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

    l_text := 'LOAD TABLE: '||'FND_WFS_CARD_TRACKNTRACE' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

    select /*+ parallel(cpy,2) full(cpy) */
    count(*)
    into   g_recs_read
    from   dwh_wfs_foundation.stg_dsv_card_trackntrace_cpy cpy
    where  sys_process_code = 'N';

    -- process each set of information_date data separately 

    for cur_rec in c_stg_info_date
    loop

       -- add records that have changed, rather than updating.
       flagged_records_insert_revised(cur_rec.information_date);

       l_text := 'Processed INFORMATION_DATE: '||
       to_char(cur_rec.information_date,('YYYY-MM-DD')) ||
       ' - Revision inserts       :  '||g_day_revised;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


       -- add new records 
       flagged_records_insert(cur_rec.information_date);

       l_text := 'Processed INFORMATION_DATE: '||
       to_char(cur_rec.information_date,('YYYY-MM-DD')) ||
       ' - New inserts            :  '||g_day_new;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       l_text := 'Processed INFORMATION_DATE: '||
       to_char(cur_rec.information_date,('YYYY-MM-DD')) ||
       ' - No change records      :  '||g_day_nochange;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    end loop;





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
    l_text :=  'New: '||g_recs_new||' / Revisions: '||g_recs_revised;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
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
       RAISE;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;

end WH_FND_WFS_468U_20180905;
