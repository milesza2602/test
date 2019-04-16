--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_348U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_348U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Dec 2017
--  Author:      Alastair de Wet
--  Purpose:     Create a summary level for Staff Sales ex Mart cust_mart_staff_disc_detail
--               forming the basis of the data out.
--  Tables:      Input  - cust_mart_staff_disc_detail
--               Output - cust_staff_sales
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Mar 2018:   VAT Changes 
--
--  Apr 2018:   Theo Filander - Change calculation of sales. Add 2 columns
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

g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_348U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE STAFF SALES AT LOC/ITEM/DAY TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'STAFF SALES STARTED AT '||
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
    
    execute immediate 'alter session enable parallel dml';
    
--execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

--**************************************************************************************************
        
           
             merge /*+ parallel (cms,8)  */ into cust_staff_sales cms
             using (
               select /*+ parallel (csd,8) parallel(di,4) full(di) parallel(pck,4) full(pck) full(dl)*/
                      tran_date,
                      csd.location_no,
                      case 
                         when di.tran_ind <> 1  then di.item_parent_no  
                         when csd.item_no = nvl(pck.pack_item_no,0) then pck.item_no 
                      else
                              csd.item_no
                      end  item_no_new,
                      max(sk1_item_no) sk1_item_no,  
                      max(sk1_location_no) sk1_location_no, 
                      max(csd.item_no)  original_item_no,
                      sum(round((item_tran_selling -(NVL(company_discount_selling,0) + NVL(promotion_discount_selling,0))) * 100/(100 + nvl(ivr.vat_rate_perc,di.vat_rate_perc)),2)) staff_sales,
                      sum(round((NVL(company_discount_selling,0) ) * 100/(100 + nvl(ivr.vat_rate_perc,di.vat_rate_perc)),2))   staff_discount_selling,
                      sum (case when till_no  = 999 then 
                                     round(((item_tran_selling -(NVL(company_discount_selling,0) + NVL(promotion_discount_selling,0))) * 100/(100 + nvl(ivr.vat_rate_perc,di.vat_rate_perc))),2) 
                                else 0 end) online_staff_sales ,
                      sum (case when till_no  = 999 then 
                                     round(((company_discount_selling ) * 100/(100 + nvl(ivr.vat_rate_perc,di.vat_rate_perc))),2)
                                else 0 end) online_staff_discount_selling,
                     sum(item_tran_qty) staff_sales_qty,
                     sum( case when till_no = 999 then
                                    item_tran_qty
                               else 0 end) online_staff_sales_qty
               from   cust_mart_staff_disc_detail csd 
               join   dim_item di       on csd.item_no      = di.item_no
               join   dim_location dl   on csd.location_no  = dl.location_no
    LEFT OUTER JOIN   fnd_item_vat_rate  ivr  on (csd.item_no   = ivr.item_no                                      -- VAT rate change
                                         and  dl.vat_region_no  = ivr.vat_region_no                                       -- VAT rate change
                                         and  tran_date between ivr.active_from_date and ivr.active_to_date)              -- VAT rate change               
    LEFT OUTER JOIN   fnd_pack_item_detail pck on csd.item_no      = pck.pack_item_no

               where  tran_date        > g_date - 31
               group by tran_date,csd.location_no,
                      case 
                         when di.tran_ind <> 1  then di.item_parent_no  
                         when csd.item_no = nvl(pck.pack_item_no,0) then pck.item_no 
                      else
                              csd.item_no
                      end  

                   ) mer_rec
             on    (cms.tran_date	  =	mer_rec.tran_date and
                    cms.location_no =	mer_rec.location_no and
                    cms.item_no     = mer_rec.item_no_new)
             when matched then 
             update set 
                    cms.staff_sales                      =	mer_rec.staff_sales ,
                    cms.staff_discount_selling           =	mer_rec.staff_discount_selling ,
                    cms.online_staff_sales               =	mer_rec.online_staff_sales  ,
                    cms.online_staff_discount_selling    =	mer_rec.online_staff_discount_selling  ,
                    cms.staff_sales_qty                  =	mer_rec.staff_sales_qty  ,
                    cms.online_staff_sales_qty           =	mer_rec.online_staff_sales_qty  ,
                    cms.last_updated_date                =  g_date 
              when not matched then
              insert
                      (         
                      tran_date,
                      location_no,
                      item_no,
                      sk1_item_no,
                      sk1_location_no,
                      original_item_no,
                      staff_sales,
                      staff_discount_selling,
                      online_staff_sales,
                      online_staff_discount_selling,
                      last_updated_date,
                      staff_sales_qty,
                      online_staff_sales_qty
                      )
              values
                      ( 
                      mer_rec.tran_date,
                      mer_rec.location_no,
                      mer_rec.item_no_new,
                      mer_rec.sk1_item_no,
                      mer_rec.sk1_location_no,
                      mer_rec.original_item_no,
                      mer_rec.staff_sales,
                      mer_rec.staff_discount_selling,
                      mer_rec.online_staff_sales,
                      mer_rec.online_staff_discount_selling,
                      g_date,
                      mer_rec.staff_sales_qty,
                      mer_rec.online_staff_sales_qty
                      )           
                      ;   
          
              g_recs_inserted := g_recs_inserted  + sql%rowcount;
 

    commit;

    l_text := 'FIX SK1 BEING PROCESSED IS:- '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    merge /*+ parallel (cms,8)  */ into cust_staff_sales cms
             using (
             select /*+ parallel (cms1,8) full(di) parallel(di,8)  */
                    di.sk1_item_no, 
                    cms1.tran_date,
                    cms1.location_no,
                    cms1.item_no
             from   cust_staff_sales cms1, 
                    dim_item di 
             where  di.item_no      = cms1.item_no
             and    tran_date       > g_date - 31
             and    di.sk1_item_no <> cms1.sk1_item_no ) mer_rec
             on    (cms.tran_date	  =	mer_rec.tran_date and
                    cms.location_no =	mer_rec.location_no and
                    cms.item_no     = mer_rec.item_no)
             when matched then 
             update set 
                    cms.sk1_item_no                      =	mer_rec.sk1_item_no  
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
    l_text :=  'Correct the SK1 code '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||'348U'||sysdate;
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

END WH_PRF_CUST_348U;
