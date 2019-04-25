--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_050U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_050U" -- STORE PROC CHANGE
                        (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Francisca de Vaal
--               Extracting weekly sales and GRN data from dim_item on selected suppliers (BridgeThorne)
--
--  Tables:      Input  - Rtl_Loc_Item_WK_rms_Dense, Rtl_Loc_Item_wk_Rms_Sparse,                         -- TABLE NAME CHANGE 
--                      - Rtl_Loc_Item_wk_Rms_stock, Rtl_supchain_loc_item_dy,
--                      - Rtl_Loc_Item_wk_Catalog
--               Output - RTL_ITEM_SUP_WK_BRTH_LIST                                                      -- TABLE NAME CHANGE 
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
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
g_physical_updated   integer       :=  0;

g_date               date          := trunc(sysdate);
g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_050U';                              -- STORE PROC CHANGE
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_ITEM_SUP_WK_BRTH_LIST EX BRTH';    -- TABLE NAME CHANGE
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_merge_update as
begin
--/*+ first_rows parallel(rtl) parallel(upd_rec) */
 
   merge into dwh_datafix.aj_rtl_item_sup_wk_brth_list rtl 
--   merge into rtl_item_sup_wk_brth_list rtl
   using (
    with 
        loc_list  as 
             (select sk1_location_no
                from dim_location
               where chain_no = 10),
        item_list as 
             (select sk1_item_no,
                     ds.sk1_supplier_no
                from dim_item di, dwh_datafix.aj_supplier_bridgethorne ds
--                from dim_item di, dim_supplier_bridgethorne ds
               where di.business_unit_no in (50)
                 and di.primary_supplier_no = ds.supplier_no
                  ),
        calendar_list as
              (select calendar_date
               from   dim_calendar 
               where  calendar_date between g_last_wk_start_date and g_last_wk_end_date
               ),
        sales_measures as 
            (  
              select /*+ parallel (dns,8) full(dns) */        
                     dl.sk1_location_no,
                     di.sk1_item_no,
                     di.sk1_supplier_no,
                     dns.fin_year_no fin_year_no,
                     dns.fin_week_no fin_week_no,
                     (dns.sales) sales_rands,
                     (dns.sales_qty) sales_units,
                     (dns.sales_margin) sales_margin        
              from rtl_loc_item_wk_rms_dense dns,
                   loc_list dl,
                   item_list di
              where dns.sk1_item_no = di.sk1_item_no
                and dns.sk1_location_no = dl.sk1_location_no
                and (dns.fin_year_no = g_last_wk_fin_year_no) 
                and fin_week_no = g_last_wk_fin_week_no 
               ), 
            waste_measures as 
            (  
              select /*+ parallel(spa,8) full(spa) */
                     dl.sk1_location_no,
                     di.sk1_item_no,
                     di.sk1_supplier_no,
                     spa.fin_year_no fin_year_no,
                     spa.fin_week_no fin_week_no,
                     (spa.waste_cost) waste_cost
              from rtl_loc_item_wk_rms_sparse spa,
                   loc_list dl,
                   item_list di
              where spa.sk1_item_no = di.sk1_item_no
                and spa.sk1_location_no = dl.sk1_location_no
                and (spa.fin_year_no = g_last_wk_fin_year_no)
                and spa.fin_week_no = g_last_wk_fin_week_no
            ),
            stock_measures as 
            (  
              select /*+ parallel(st,8) full(st) */
                     dl.sk1_location_no,
                     di.sk1_item_no,
                     di.sk1_supplier_no,
                     st.fin_year_no fin_year_no,
                     st.fin_week_no fin_week_no,
                     (st.soh_qty) soh_qty,
                     (st.boh_selling) boh_selling,
                     (st.boh_qty) boh_qty         
              from rtl_loc_item_wk_rms_stock st,
                   loc_list dl,
                   item_list di
              where st.sk1_item_no = di.sk1_item_no
                and st.sk1_location_no = dl.sk1_location_no
                and (st.fin_year_no = g_last_wk_fin_year_no) 
                and st.fin_week_no = g_last_wk_fin_week_no  
            ),
            grn_measures as 
            (  
               select /*+ parallel (po,8) full(po) full (dl) parallel (dl) full (di) */
                   dl.sk1_location_no,
                   di.sk1_supplier_no,
                   di.sk1_item_no,
                   g_last_wk_fin_year_no fin_year_no,
                   g_last_wk_fin_week_no fin_week_no,
                   sum(nvl(po.po_grn_selling,0)) po_grn_selling,
                   sum(nvl(po.po_grn_qty,0)) po_grn_qty,
                   sum(nvl(po.po_grn_cost,0)) po_grn_cost,      
                   sum(nvl(po.fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty, 
                   sum(nvl(po.fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty, 
                   sum(nvl(po.shorts_selling,0)) shorts_selling,
                   sum(nvl(po.shorts_qty,0)) shorts_qty, 
                   sum(nvl(po.shorts_cost,0)) shorts_cost       
               from rtl_supchain_loc_item_dy po,
                 loc_list dl,
                 item_list di,
                 calendar_list cal
              where po.sk1_item_no = di.sk1_item_no
              and po.sk1_location_no = dl.sk1_location_no
              and po.tran_date = cal.calendar_date
              group by   dl.sk1_location_no,
                   di.sk1_supplier_no,
                   di.sk1_item_no,
                   g_last_wk_fin_year_no ,
                   g_last_wk_fin_week_no 
            ),
            catalog_list as
            (  
              select /*+ parallel( cat,8) full(cat) */
                   dl.sk1_location_no,
                   di.sk1_supplier_no,
                   di.sk1_item_no,
                   cat.fin_year_no fin_year_no,
                   cat.fin_week_no fin_week_no,
                   cat.this_wk_catalog_ind
              from rtl_loc_item_wk_catalog cat,
                   loc_list dl,
                   item_list di
              where cat.sk1_item_no = di.sk1_item_no
                and cat.sk1_location_no = dl.sk1_location_no
                and cat.fin_year_no = g_last_wk_fin_year_no
                and cat.fin_week_no = g_last_wk_fin_week_no
             
            )
            select
            --    NVL(NVL(NVL(NVL(F0.Sk1_Location_No, F1.Sk1_Location_No), F2.Sk1_Location_No), F3.Sk1_Location_No), F4.Sk1_Location_No) Sk1_Location_No ,
                nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no), f4.sk1_item_no)  sk1_item_no,
                nvl(nvl(nvl(nvl(f0.sk1_supplier_no, f1.sk1_supplier_no), f2.sk1_supplier_no), f3.sk1_supplier_no), f4.sk1_supplier_no)  sk1_supplier_no,
                nvl(nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no), f4.fin_year_no) fin_year_no,
                nvl(nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no), f4.fin_week_no) fin_week_no,
                sum(f1.sales_rands)     as sales,  
                sum(f1.sales_units)     as sales_qty,
                sum(f1.sales_margin)    as sales_margin,
                sum(f3.soh_qty)         as soh_qty,
                sum(f3.boh_selling)     as boh_selling,
                sum(f3.boh_qty)         as boh_qty,
                sum(f2.waste_cost)      as waste_cost,
                sum(f4.po_grn_selling)  as po_grn_selling, 
                sum(f4.po_grn_qty)      as po_grn_qty,
                sum(f4.po_grn_cost)     as po_grn_cost, 
                sum(f4.fillrate_fd_latest_po_qty) as fillrate_fd_latest_po_qty, 
                sum(f4.fillrate_fd_po_grn_qty)    as fillrate_fd_po_grn_qty, 
                sum(f4.shorts_selling)            as shorts_selling, 
                sum(f4.shorts_qty)                as shorts_qty,  
                sum(f4.shorts_cost)               as shorts_cost, 
                sum(f0.this_wk_catalog_ind)       as this_wk_catalog_ind,
                 g_date as last_updated_date
            from catalog_list f0
               full outer join
                 sales_measures f1
                    on f0.sk1_location_no =  f1.sk1_location_no
                   and f0.sk1_item_no = f1.sk1_item_no
                   and f0.fin_year_no = f1.fin_year_no
                   and f0.fin_week_no = f1.fin_week_no     
               full outer join
                 waste_measures f2
                    on nvl(f0.sk1_location_no, f1.sk1_location_no) =  f2.sk1_location_no
                   and nvl(f0.sk1_item_no, f1.sk1_item_no) = f2.sk1_item_no
                   and nvl(f0.fin_year_no, f1.fin_year_no) = f2.fin_year_no
                   and nvl(f0.fin_week_no, f1.fin_week_no) = f2.fin_week_no
                full outer join
                 stock_measures f3
                    on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
                   and nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no) = f3.sk1_item_no
                   and nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no) = f3.fin_year_no
                   and nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no) = f3.fin_week_no 
                full outer join
                 grn_measures f4
                    on nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no), f3.sk1_location_no) = f4.sk1_location_no
                   and nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no) = f4.sk1_item_no
                   and nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no) = f4.fin_year_no
                   and nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no) = f4.fin_week_no  
            group by
            --    NVL(NVL(NVL(NVL(F0.Sk1_Location_No, F1.Sk1_Location_No), F2.Sk1_Location_No), F3.Sk1_Location_No), F4.Sk1_Location_No),
                nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no), f4.sk1_item_no),
                nvl(nvl(nvl(nvl(f0.sk1_supplier_no, f1.sk1_supplier_no), f2.sk1_supplier_no), f3.sk1_supplier_no), f4.sk1_supplier_no),
                nvl(nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no), f4.fin_year_no),
                nvl(nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no), f4.fin_week_no),
            g_date
        ) mer_rec
         
   on    (rtl.sk1_item_no	        =	mer_rec.sk1_item_no     and
          rtl.sk1_supplier_no  	  =	mer_rec.sk1_supplier_no and
          rtl.fin_year_no	        =	mer_rec.fin_year_no     and    
          rtl.fin_week_no         =	mer_rec.fin_week_no)
            
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
          rtl.sales                      =	mer_rec.sales,  
          rtl.sales_qty                  =	mer_rec.sales_qty,
          rtl.sales_margin               =	mer_rec.sales_margin,
          rtl.soh_qty                    =	mer_rec.soh_qty,
          rtl.boh_selling                =	mer_rec.boh_selling,
          rtl.boh_qty                    =	mer_rec.boh_qty,
          rtl.waste_cost                 =	mer_rec.waste_cost,
          rtl.po_grn_selling             =	mer_rec.po_grn_selling, 
          rtl.po_grn_qty                 =	mer_rec.po_grn_qty,
          rtl.po_grn_cost                =	mer_rec.po_grn_cost, 
          rtl.fillrate_fd_latest_po_qty  =	mer_rec.fillrate_fd_latest_po_qty, 
          rtl.fillrate_fd_po_grn_qty     =	mer_rec.fillrate_fd_po_grn_qty, 
          rtl.shorts_selling             =	mer_rec.shorts_selling, 
          rtl.shorts_qty                 =	mer_rec.shorts_qty,  
          rtl.shorts_cost                =	mer_rec.shorts_cost, 
          rtl.this_wk_catalog_ind        =	mer_rec.this_wk_catalog_ind,
          rtl.this_week_start_date       = g_last_wk_start_date,
          rtl.last_updated_date          = g_date
            
   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_item_no,
          sk1_supplier_no,
          fin_year_no,
          fin_week_no,
          sales,  
          sales_qty,
          sales_margin,
          soh_qty,
          boh_selling,
          boh_qty,
          waste_cost,
          po_grn_selling, 
          po_grn_qty,
          po_grn_cost, 
          fillrate_fd_latest_po_qty, 
          fillrate_fd_po_grn_qty, 
          shorts_selling, 
          shorts_qty,  
          shorts_cost, 
          this_wk_catalog_ind,
          this_week_start_date,
          last_updated_date
         )
  values                                                                                                           -- COLUNM NAME CHANGE 
         (          
          mer_rec.sk1_item_no,
          mer_rec.sk1_supplier_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.sales,  
          mer_rec.sales_qty,
          mer_rec.sales_margin,
          mer_rec.soh_qty,
          mer_rec.boh_selling,
          mer_rec.boh_qty,
          mer_rec.waste_cost,
          mer_rec.po_grn_selling, 
          mer_rec.po_grn_qty,
          mer_rec.po_grn_cost, 
          mer_rec.fillrate_fd_latest_po_qty, 
          mer_rec.fillrate_fd_po_grn_qty, 
          mer_rec.shorts_selling, 
          mer_rec.shorts_qty,  
          mer_rec.shorts_cost, 
          mer_rec.this_wk_catalog_ind,
          g_last_wk_start_date,
          g_date
          )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

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
 
end do_merge_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

--    dbms_output.put_line('Execute Parallel ');
    execute immediate 'alter session enable parallel dml';
 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
--    dbms_output.put_line('Control Date ');
    dwh_lookup.dim_control(g_date);
--    g_date := g_date - 7;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
--     select last_wk_fin_year_no, last_wk_fin_week_no, last_wk_start_date  
--     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date 
--     from dim_control;
     
     select fin_year_no, fin_week_no, this_week_start_date  , this_week_end_date
     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date , g_last_wk_end_date
     from   dim_calendar 
     where  calendar_date = g_date;
     
/*     select calendar_date
     into   g_calendar_date
     from   dim_calendar 
     where  calendar_date between g_last_wk_start_date and g_last_wk_end_date;
*/     
    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    dbms_output.put_line('Start Merge ');
    do_merge_update;
   
    l_text := 'MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    dbms_output.put_line('End Merge ');

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;                              --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;                                 --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;               --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
END WH_PRF_BRTH_050U                                                                                             -- STORE PROC CHANGE 
;
