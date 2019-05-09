--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_206U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_206U_FIX" (p_forall_limit in integer,p_success out boolean,p_date in date) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust basket_item_sample fact table in the performance layer
--               with input ex basket_item. Random 5% sample
--  Tables:      Input  - cust_basket_item
--               Output - cust_basket_item_sample
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance: Theo Filander - Modified the program to accept a run date. in order to facilitate 
--                               a fix for week 18
--
--
-- Note: This version Attempts to do a bulk insert / update
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
g_truncate_count     integer       :=  0;



g_date               date          := nvl(p_date,trunc(sysdate));
g_process_date       date          := nvl(p_date,trunc(sysdate));

g_partname           varchar2(30);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_206U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET ITEM SAMPLE EX BASKET_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
     g_process_date := g_date - 7;

      insert /*+ APPEND parallel (prf,4) */ into cust_basket_item_sample prf
      with    tmp as  
              (select /*+ FULL(cb)  parallel (cb,12) */ 
               location_no	,till_no	,tran_no	,tran_date, rownum rn
               from  cust_basket cb
               WHERE tran_date = g_date
               and   primary_customer_identifier = 998
               ),
              basket as 
              (select /*+ FULL(tmp)  parallel (tmp,8) */ * from tmp          
               where   mod(rn,20) = 0)  
      select  /*+ FULL(bk) parallel (bk,12) parallel(bi,12) */
              bi.location_no	,
              bi.till_no	,
              bi.tran_no	,
              bi.tran_date	,
              bi.tran_time	,
              bi.item_seq_no	,
              bi.item_no	,
              bi.tran_type	,
              bi.item_tran_selling	,
              bi.vat_rate_perc	,
              bi.item_tran_qty	,
              bi.discount_selling	,
              bi.customer_no	,
              bi.dept_no	,
              bi.item_input_code	,
              bi.waste_discount_selling	,
              bi.return_reason_code	,
              bi.item_type_code	,
              bi.item_ref_code	,
              bi.serial_no	,
              bi.gift_card_type	,
              bi.vitality_cust_ind	,
              bi.vitality_uda_value	,
              bi.green_value	,
              bi.green_factor	,
              bi.primary_customer_identifier	,
              bi.last_updated_date,
              bi.employee_id,
              bi.company_code,
              bi.item_tran_selling_local,
              bi.discount_selling_local,
              bi.waste_discount_selling_local,
              bi.green_value_local
      from    cust_basket_item bi, basket bk
               where  bi.location_no = bk.location_no and
                      bi.tran_no     = bk.tran_no and
                      bi.till_no     = bk.till_no and
                      bi.tran_date   = bk.tran_date and
                      bi.tran_date   = g_date and
               not exists (
               select /*+ nl_aj */ * 
               from cust_basket_item_sample  bis
               where  bi.location_no = bis.location_no and
                      bi.tran_no     = bis.tran_no and
                      bi.till_no     = bis.till_no and
                      bi.tran_date   = bis.tran_date and
                      bi.item_no     = bis.item_no and
                      bi.item_seq_no = bis.item_seq_no
                          );



      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

    l_text := 'Cash insert completed '||g_recs_inserted||'  '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    g_process_date := g_date - 30;

      insert /*+ APPEND parallel (prf,12) */ into cust_basket_item_sample prf
      select /*+ FULL(bsk)  parallel (bsk,12) */
             bsk.location_no	,
             bsk.till_no	,
             bsk.tran_no	,
             bsk.tran_date	,
             bsk.tran_time	,
             bsk.item_seq_no	,
             bsk.item_no	,
             bsk.tran_type	,
             bsk.item_tran_selling	,
             bsk.vat_rate_perc	,
             bsk.item_tran_qty	,
             bsk.discount_selling	,
             bsk.customer_no	,
             bsk.dept_no	,
             bsk.item_input_code	,
             bsk.waste_discount_selling	,
             bsk.return_reason_code	,
             bsk.item_type_code	,
             bsk.item_ref_code	,
             bsk.serial_no	,
             bsk.gift_card_type	,
             bsk.vitality_cust_ind	,
             bsk.vitality_uda_value	,
             bsk.green_value	,
             bsk.green_factor	,
             bsk.primary_customer_identifier	,
             bsk.last_updated_date,
             bsk.employee_id,
             bsk.company_code,
             bsk.item_tran_selling_local,
             bsk.discount_selling_local,
             bsk.waste_discount_selling_local,
             bsk.green_value_local              
      from   cust_basket_item  bsk
      where  substr(primary_customer_identifier,length(primary_customer_identifier)-1,2) in(23,38,42,66,74)
--    where   primary_customer_identifier - (floor(primary_customer_identifier/100) * 100) in (23,38,42,66,74)  -- Alternative solution
      and     bsk.tran_date = g_date 
      and     not exists (
              select /*+ nl_aj */ * 
              from cust_basket_item_sample  bis
              where  bsk.location_no = bis.location_no and
                     bsk.tran_no     = bis.tran_no and
                     bsk.till_no     = bis.till_no and
                     bsk.tran_date   = bis.tran_date and
                     bsk.item_no     = bis.item_no and
                     bsk.item_seq_no = bis.item_seq_no
                         ); 

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
-- Not Required. The actual batch date is passed to the program
--**************************************************************************************************
--    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- 1. Get the subpartition name
-- 2. Truncate the partition
--**************************************************************************************************
    l_text := 'TRUNCATE PARTITION';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  


    select /*+ Parallel(do,6) */ 
           do.subobject_name
      into g_partname
      from sys.dba_objects do
     where data_object_id=(select /*+Parallel(dp,16) */ 
                                  dbms_rowid.rowid_object(rowid) from cust_basket_item_sample dp 
                            where tran_date = g_date
                              and rownum = 1
                          );
    if g_partname is not null then
        l_text := 'SUBPARTITION TO BE TRUNCATED IS :- '||g_partname;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);                     
        execute immediate 'alter table cust_basket_item_sample truncate partition '||g_partname;
    end if;
    
    commit;
    l_text := 'PARTITION TRUNCATED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_DEPT_MONTH_INVOLVE',g_partname,estimate_percent=>0.1, DEGREE => 16);
--   commit;
--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************
--    g_process_date := g_date - 7;

    select /*+ FULL(BI) PARALLEL(12) */ count(*)
    into   g_recs_read
    from   cust_basket_item bi
    where  tran_date = g_date;

    commit;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
       raise;
end wh_prf_cust_206u_fix;
