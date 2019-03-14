-- ****** Object: Procedure W7131037.WH_PRF_CUST_108LOD Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_108LOD" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        AUGUST 2011
--  Author:      Alastair de Wet
--  Purpose:     TAKE ON THE LAST ONLINE DATE
--  Tables:      Input  - fnd_cust_basket
--               Output - cust_basket
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions:
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
g_recs_comm          integer       :=  0;

g_customer_no        number(20,0);
g_tabl_customer_no   number(20,0);
g_pci                number(20,0);

g_tender_no          number(20,0);
g_ww_swipe           number(20,0);
g_ext_swipe          number(20,0);
g_online_order_no    cust_basket.ww_online_order_no%type;

g_tender_no_rank     number(2,0);
g_ww_swipe_rank      number(2,0);
g_ext_swipe_rank     number(2,0);

g_location_no        cust_basket.location_no%type;
g_till_no            cust_basket.till_no%type;
g_tran_no            cust_basket.tran_no%type;
g_tran_date          cust_basket.tran_date%type;




g_found              boolean;
g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_108LOD';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TAKE ON LAST ONLINE DATE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Update the customer master with date of last transaction, online transaction & vitality purchase
--**************************************************************************************************
procedure write_date_last_trans  as
begin

--MERGE  INTO dim_customer cust
--   USING (
--   select /*+ full(cb) parallel (cb,12) */
--   cb.customer_no,
--   max(cb.tran_date) ltd
--   from   cust_basket cb
--   where  cb.tran_date    BETWEEN '26 jun 2017' and '20 sep 2017'
--   and    cb.customer_no  is not null
--   group by cb.customer_no
--         ) mer_rec
--   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
--   WHEN MATCHED THEN
--   UPDATE SET cust.	last_transaction_date =	mer_rec.	ltd
--   WHERE      mer_rec.ltd  > nvl(cust.last_transaction_date,'1 Jan 2000')  ;

    l_text := 'TAKE ON LOD '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


MERGE  INTO dim_customer cust
   USING (
   select /*+ full(cb) parallel (cb,12) */
   cb.customer_no,
   max(cb.tran_date) lod
   from   cust_basket cb
   where  (cb.till_no = 999 or cb.till_no = 997 or cb.ww_online_order_no is not null)
   and    cb.tran_date    BETWEEN '26 jun 2017' and '20 sep 2017'
   and    cb.customer_no  is not null
   group by cb.customer_no
         ) mer_rec
   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
   WHEN MATCHED THEN
   UPDATE SET cust.	last_online_date =	mer_rec.	lod
   WHERE      mer_rec.lod  > nvl(cust.last_online_date,'1 Jan 2000')  ;

   g_recs_updated := g_recs_updated  + sql%rowcount;

    l_text := 'TAKE ON LVD '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

MERGE  INTO dim_customer cust
   USING (
   select /*+ full(cb) parallel (cb,12)  */
   cb.customer_no,
   max(cb.tran_date) lvd
   from   cust_basket_item cb
   where  vitality_cust_ind = 1 and vitality_uda_value = 1
   and    cb.tran_date    BETWEEN '26 jun 2017' and '20 sep 2017'
   and    cb.customer_no  is not null
   group by cb.customer_no
         ) mer_rec
   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
   WHEN MATCHED THEN
   UPDATE SET cust. last_vitality_date =	mer_rec.	lvd
   WHERE      mer_rec.lvd  > nvl(cust.last_vitality_date,'1 Jan 2000') ;

   g_recs_inserted := g_recs_inserted  + sql%rowcount;

   exception
      when others then
       l_message := 'WRITE LAST TRANSACTION - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end write_date_last_trans;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'TAKE ON OF HISTORIC LAST UPDATED DATES '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

--**************************************************************************************************


--**************************************************************************************************
-- At end write various last transaction dates to customer master
--**************************************************************************************************

    l_text := 'START PROCESS TO WITE LAST TRANSACTION DATES - '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    write_date_last_trans;

    commit;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||'108U1'||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
        execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = true';
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END "WH_PRF_CUST_108LOD";
