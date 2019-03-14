-- ****** Object: Procedure W7131037.WH_PRF_CUST_346U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_346U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Sept 2017
--  Author:      Alastair de Wet
--  Purpose:     Create a mart summary level for Staff Discount
--               with cust_basket tables forming the basis of the data out.
--  Tables:      Input  - fnd_cust_basket & others
--               Output - cust_mart_staff_disc_summary
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


g_tender_type_det_cde1          varchar2(10 byte);
g_tender_type_det_cde2          varchar2(10 byte);
g_tender_type_det_cde3          varchar2(10 byte);
g_tender_type_det_cde4          varchar2(10 byte);
g_tender_type_det_cde5          varchar2(10 byte);

g_item_no            cust_basket_item.item_no%type;
g_location_no        cust_basket.location_no%type;
g_till_no            cust_basket.till_no%type;
g_tran_no            cust_basket.tran_no%type;
g_tran_date          cust_basket.tran_date%type;

g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_346U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE STAFF MART SUMMARY TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


cursor c_prf_cust_basket is
   select /*+ full(cb) parallel (cb,4) */
   location_no,
   till_no,
   tran_no,
   tran_date,
   tran_time,
   employee_id,
   tran_selling,
   loyalty_ww_swipe_no,
   loyalty_ext_swipe_no,
   customer_no,
   operator_id
   from   cust_basket cb
   where  cb.last_updated_date     >  g_date - 2
   and    cb.tran_date             >  g_date - 31
   and    cb.employee_id           is not null
--   and    cb.tran_type in('R','S','V','Q','P','N')
   ;

cursor c_prf_cust_basket_tender is
   select /*+ parallel (cbt,2) */
   tender_type_detail_code
   from   cust_basket_tender cbt
   where  location_no = g_location_no
   and    till_no     = g_till_no
   and    tran_no     = g_tran_no
   and    tran_date   = g_tran_date;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'STAFF DISCOUNT SUMMERY MART STARTED AT '||
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

--execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

--**************************************************************************************************
for bk_rec in c_prf_cust_basket
    loop
       g_count := 0;
       g_tender_type_det_cde1 := '';
       g_tender_type_det_cde2 := '';
       g_tender_type_det_cde3 := '';
       g_tender_type_det_cde4 := '';
       g_tender_type_det_cde5 := '';

       g_location_no      := bk_rec.location_no;
       g_till_no          := bk_rec.till_no;
       g_tran_no          := bk_rec.tran_no;
       g_tran_date        := bk_rec.tran_date;

    for bkt_rec in c_prf_cust_basket_tender
        loop
           g_count := g_count + 1;
           if g_count = 1 then   g_tender_type_det_cde1 := bkt_rec.tender_type_detail_code; end if;
           if g_count = 2 then   g_tender_type_det_cde2 := bkt_rec.tender_type_detail_code; end if;
           if g_count = 3 then   g_tender_type_det_cde3 := bkt_rec.tender_type_detail_code; end if;
           if g_count = 4 then   g_tender_type_det_cde4 := bkt_rec.tender_type_detail_code; end if;
           if g_count = 5 then   g_tender_type_det_cde5 := bkt_rec.tender_type_detail_code; end if;
        end loop;


             merge  into cust_mart_staff_disc_summary cms
             using (
             with itm as (
               select /*+ parallel (cbi,2) */
                      sum(item_tran_qty) itq
               from   cust_basket_item cbi
               where  location_no = g_location_no
               and    till_no     = g_till_no
               and    tran_no     = g_tran_no
               and    tran_date   = g_tran_date),

                    aux as (
             select /*+parallel (cba,2)  */
             sum (case when tran_type_code  = 'STAFF' then promotion_discount_amount else 0 end) comp_disc_sell ,
             sum (case when tran_type_code <> 'STAFF' then promotion_discount_amount else 0 end) prom_disc_sell
             from   cust_basket_aux cba
             where  cba.tran_date   =  g_tran_date
             and    cba.till_no     =  g_till_no
             and    cba.location_no =  g_location_no
             and    cba.tran_no     =  g_tran_no)
             select * from itm,aux
                   ) mer_rec
             on    (cms.tran_date	  =	g_tran_date and
                    cms.tran_no     =	g_tran_no and
                    cms.till_no 	  =	g_till_no and
                    cms.location_no =	g_location_no )


             when matched then
             update set
                    cms.tran_time                  =	bk_rec.tran_time ,
                    cms.employee_id                =  bk_rec.employee_id,
                    cms.tran_qty                   =  mer_rec.itq,
                    cms.tran_selling               =  bk_rec.tran_selling,
                    cms.company_discount_selling   =  mer_rec.comp_disc_sell,
                    cms.promotion_discount_selling =  mer_rec.prom_disc_sell,
                    cms.operator_id                =  bk_rec.operator_id,
                    cms.loyalty_ww_swipe_no        =  bk_rec.loyalty_ww_swipe_no,
                    cms.customer_no                =  bk_rec.customer_no,
                    cms.tender_type_detail_code1   =  g_tender_type_det_cde1,
                    cms.tender_type_detail_code2   =  g_tender_type_det_cde2,
                    cms.tender_type_detail_code3   =  g_tender_type_det_cde3,
                    cms.tender_type_detail_code4   =  g_tender_type_det_cde4,
                    cms.tender_type_detail_code5   =  g_tender_type_det_cde5
              when not matched then
              insert
                      (
                      location_no,
                      till_no,
                      tran_no,
                      tran_date,
                      tran_time,
                      employee_id,
                      tran_qty,
                      tran_selling,
                      company_discount_selling,
                      promotion_discount_selling,
                      operator_id,
                      loyalty_ww_swipe_no,
                      customer_no,
                      tender_type_detail_code1,
                      tender_type_detail_code2,
                      tender_type_detail_code3,
                      tender_type_detail_code4,
                      tender_type_detail_code5,
                      last_updated_date
                      )
              VALUES
                      (
                      g_location_no,
                      g_till_no,
                      g_tran_no,
                      g_tran_date,
                      bk_rec.tran_time,
                      bk_rec.employee_id,
                      mer_rec.itq,
                      bk_rec.tran_selling,
                      mer_rec.comp_disc_sell,
                      mer_rec.prom_disc_sell,
                      bk_rec.operator_id ,
                      bk_rec.loyalty_ww_swipe_no ,
                      bk_rec.customer_no,
                      g_tender_type_det_cde1,
                      g_tender_type_det_cde2,
                      g_tender_type_det_cde3,
                      g_tender_type_det_cde4,
                      g_tender_type_det_cde5,
                      g_date
                      )
                      ;

              g_recs_inserted := g_recs_inserted  + sql%rowcount;




    g_recs_comm := g_recs_comm + 1;
    if g_recs_comm mod 5000 = 0 then
            l_text := 'RECORDS PROCESSED - '||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_comm ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            commit;
    end if;

    end loop;

    commit;

---------------------------------------------------------------------------------------------
    l_text := 'UPDATE CUSTOMER NO WHERE NULL:- '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE /*+ parallel(4) */ INTO CUST_MART_STAFF_DISC_SUMMARY   SD
    USING
    (
    SELECT /*+ parallel(4) */
          LOCATION_NO,
          TILL_NO,
          TRAN_NO,
          TRAN_DATE,
          STF.LOYALTY_WW_SWIPE_NO,CC.CUSTOMER_NO
    FROM  CUST_MART_STAFF_DISC_SUMMARY STF,
          FND_CUSTOMER_CARD CC
    where STF.CUSTOMER_NO IS NULL
    AND   STF.LOYALTY_WW_SWIPE_NO = CC.CARD_NO
    ) MER_REC
    ON    ( SD.	LOCATION_NO	      =	mer_rec.	LOCATION_NO AND
            SD.	TILL_NO	          =	mer_rec.	TILL_NO AND
            SD.	TRAN_NO	          =	mer_rec.	TRAN_NO AND
            SD.	TRAN_DATE	        =	mer_rec.	TRAN_DATE

          )
   WHEN MATCHED THEN
   UPDATE SET
            SD.	CUSTOMER_NO	        =	mer_rec.	CUSTOMER_NO
;
            g_recs_updated := g_recs_updated  + sql%rowcount;
   COMMIT;

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
    l_text := dwh_cust_constants.vc_log_run_completed||'346U'||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
--        execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = true';
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

END "WH_PRF_CUST_346U";
