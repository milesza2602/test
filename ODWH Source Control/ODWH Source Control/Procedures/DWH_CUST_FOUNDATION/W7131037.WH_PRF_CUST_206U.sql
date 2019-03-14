-- ****** Object: Procedure W7131037.WH_PRF_CUST_206U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_206U"      (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust basket_item_sample fact table in the performance layer
--               with input ex basket_item. Random 5% sample
--  Tables:      Input  - cust_basket_item
--               Output - cust_basket_item_sample
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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



g_date               date          := trunc(sysdate);
g_process_date       date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_206U';
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
               WHERE last_updated_date = g_date
               and   tran_date           BETWEEN g_process_date AND g_date
               and   primary_customer_identifier = 998
               ),
              basket as
              (select /*+ FULL(tmp)  parallel (tmp,8) */ * from tmp
               where   mod(rn,20) = 0)
      select  /*+ FULL(bk) parallel (bk,12) parallel(bi,12) */
             	 bi.LOCATION_NO	,
             	 bi.TILL_NO	,
             	 bi.TRAN_NO	,
             	 bi.TRAN_DATE	,
             	 bi.TRAN_TIME	,
             	 bi.ITEM_SEQ_NO	,
             	 bi.ITEM_NO	,
             	 bi.TRAN_TYPE	,
             	 bi.ITEM_TRAN_SELLING	,
             	 bi.VAT_RATE_PERC	,
             	 bi.ITEM_TRAN_QTY	,
             	 bi.DISCOUNT_SELLING	,
             	 bi.CUSTOMER_NO	,
             	 bi.DEPT_NO	,
             	 bi.ITEM_INPUT_CODE	,
             	 bi.WASTE_DISCOUNT_SELLING	,
             	 bi.RETURN_REASON_CODE	,
             	 bi.ITEM_TYPE_CODE	,
             	 bi.ITEM_REF_CODE	,
             	 bi.SERIAL_NO	,
             	 bi.GIFT_CARD_TYPE	,
             	 bi.VITALITY_CUST_IND	,
             	 bi.VITALITY_UDA_VALUE	,
             	 bi.GREEN_VALUE	,
             	 bi.GREEN_FACTOR	,
             	 bi.PRIMARY_CUSTOMER_IDENTIFIER	,
             	 bi.LAST_UPDATED_DATE,
               bi.EMPLOYEE_ID,
               bi.COMPANY_CODE,
               bi.ITEM_TRAN_SELLING_LOCAL,
               bi.DISCOUNT_SELLING_LOCAL,
               bi.WASTE_DISCOUNT_SELLING_LOCAL,
               bi.GREEN_VALUE_LOCAL
      from     cust_basket_item bi, basket bk
               where  bi.location_no = bk.location_no and
                      bi.tran_no     = bk.tran_no and
                      bi.till_no     = bk.till_no and
                      bi.tran_date   = bk.tran_date and
                      bi.tran_date   BETWEEN g_process_date AND G_DATE and
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

    g_process_date := g_date - 30;

      insert /*+ APPEND parallel (prf,12) */ into cust_basket_item_sample prf
      select /*+ FULL(bsk)  parallel (bsk,12) */
             	bsk.LOCATION_NO	,
             	bsk.TILL_NO	,
             	bsk.TRAN_NO	,
             	bsk.TRAN_DATE	,
             	bsk.TRAN_TIME	,
             	bsk.ITEM_SEQ_NO	,
             	bsk.ITEM_NO	,
             	bsk.TRAN_TYPE	,
             	bsk.ITEM_TRAN_SELLING	,
             	bsk.VAT_RATE_PERC	,
             	bsk.ITEM_TRAN_QTY	,
             	bsk.DISCOUNT_SELLING	,
             	bsk.CUSTOMER_NO	,
             	bsk.DEPT_NO	,
             	bsk.ITEM_INPUT_CODE	,
             	bsk.WASTE_DISCOUNT_SELLING	,
             	bsk.RETURN_REASON_CODE	,
             	bsk.ITEM_TYPE_CODE	,
             	bsk.ITEM_REF_CODE	,
             	bsk.SERIAL_NO	,
             	bsk.GIFT_CARD_TYPE	,
             	bsk.VITALITY_CUST_IND	,
             	bsk.VITALITY_UDA_VALUE	,
             	bsk.GREEN_VALUE	,
             	bsk.GREEN_FACTOR	,
             	bsk.PRIMARY_CUSTOMER_IDENTIFIER	,
             	bsk.LAST_UPDATED_DATE,
              bsk.EMPLOYEE_ID,
              bsk.COMPANY_CODE,
              bsk.ITEM_TRAN_SELLING_LOCAL,
              bsk.DISCOUNT_SELLING_LOCAL,
              bsk.WASTE_DISCOUNT_SELLING_LOCAL,
              bsk.GREEN_VALUE_LOCAL

      from    cust_basket_item  bsk
      where   substr(primary_customer_identifier,length(primary_customer_identifier)-1,2) in(23,38,42,66,74)
--    where   primary_customer_identifier - (floor(primary_customer_identifier/100) * 100) in (23,38,42,66,74)  -- Alternative solution
      and     bsk.last_updated_date = g_date
      and     bsk.tran_date BETWEEN g_process_date AND G_DATE
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
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************
    g_process_date := g_date - 7;

    select /*+ FULL(BI) PARALLEL(12) */ count(*)
    into   g_recs_read
    from   cust_basket_item bi
    where   last_updated_date = g_date
    and     tran_date BETWEEN g_process_date AND g_date;


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
end wh_prf_cust_206u;
