--------------------------------------------------------
--  DDL for Procedure WH_PRF_FPI_008U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_FPI_008U" 
  (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Francisca de Vaal
--               Extracting the woolworths branded item for the GRN items
--
--  Tables:      Input  - rtl_supchain_loc_item_dy,                         
--                      - rtl_item_sup_wk_grn_ytd,
--               Output - rtl_item_sup_wk_grn_ytd                                                   
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
g_loop_date          date ;
g_start_date         date ;
g_end_date           date ;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_FPI_008U';                             
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_ITEM_SUP_WK_BRTH_LIST EX BRTH';    
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_merge_update as
begin
--/*+ first_rows parallel(rtl) parallel(upd_rec) */
 
   merge into  rtl_item_sup_wk_grn_ytd tmp
   using (
    with items as (
            select item_no,sk1_item_no
            from   dim_item
            where  business_unit_no = 50
            ),
         calendar_current as (
            select distinct fin_year_no,fin_week_no,this_week_start_date,ly_fin_year_no,ly_fin_week_no,
                   fin_quarter_no,fin_year_no||fin_quarter_no as fin_year_quarter_no
            from   dim_calendar 
            where  calendar_date between g_last_wk_start_date and g_last_wk_end_date
--            where calendar_date between '04/FEB/2019' and '10/FEB/2019' -- Week 33 
            ),
         calendar_list as (
            select distinct
                   calendar_date,fin_year_no,fin_week_no,this_week_start_date,ly_fin_year_no,ly_fin_week_no,
                   fin_quarter_no,fin_year_no||fin_quarter_no as fin_year_quarter_no
            from   dim_calendar 
            where  calendar_date between g_last_wk_start_date and g_last_wk_end_date
--            where calendar_date between '04/FEB/2019' and '10/FEB/2019' -- Week 31
            ),
         calendar_list_prev as (
            select distinct 
                   fin_year_no as prev_fin_year_no, fin_week_no prev_fin_week_no , this_week_start_date,
                   fin_quarter_no prev_fin_quarter_no, 
                   fin_year_no||fin_quarter_no as prev_fin_year_quarter_no
            from   dim_calendar 
            where  calendar_date in (select this_week_start_date - 7 from calendar_list)
            ),
         grn_measures as (  
            select /*+ parallel (po,4) full (bi) full (cal) */
                   po.sk1_item_no,
                   po.sk1_supplier_no,
                   cal.fin_year_no,
                   cal.fin_week_no,
                   cal.fin_quarter_no,
                   cal.fin_year_quarter_no,
                   cal.this_week_start_date,
                   sum(nvl(po.po_grn_selling,0)) po_grn_selling,
                   sum(nvl(po.po_grn_qty,0)) po_grn_qty,
                   sum(nvl(po.po_grn_cost,0)) po_grn_cost,      
                   sum(nvl(po.fillrate_fd_latest_po_qty,0)) fillrate_fd_latest_po_qty, 
                   sum(nvl(po.fillrate_fd_po_grn_qty,0)) fillrate_fd_po_grn_qty, 
                   sum(nvl(po.shorts_selling,0)) shorts_selling,
                   sum(nvl(po.shorts_qty,0)) shorts_qty, 
                   sum(nvl(po.shorts_cost,0)) shorts_cost       
            from  rtl_supchain_loc_item_dy po
                 ,rtl_item_sup_wk_wwbrand bi
                 ,items di
                 ,calendar_list cal
            where po.sk1_item_no = bi.sk1_item_no
              and po.sk1_supplier_no = bi.sk1_supplier_no
              and po.sk1_item_no     = di.sk1_item_no
              and po.tran_date       = cal.calendar_date
              and bi.fin_year_no     = cal.fin_year_no
              and bi.fin_week_no     = cal.fin_week_no
            group by po.sk1_supplier_no, po.sk1_item_no, cal.fin_year_no, cal.fin_week_no, cal.this_week_start_date,cal.fin_quarter_no, cal.fin_year_quarter_no
            ),
         GRN_YTD as (  
            select /*+ parallel (po,4) full(po)*/
                   po.sk1_item_no,
                   po.sk1_supplier_no,
                   cal.prev_fin_year_no,
                   cal.prev_fin_week_no,
                   cal.prev_fin_quarter_no,
                   cal.prev_fin_year_quarter_no,
                   po.this_week_start_date,
                   po_grn_qty_ytd   
            from  rtl_item_sup_wk_grn_ytd po
                 ,calendar_list_prev cal
            where po.fin_year_no = cal.prev_fin_year_no
               and po.fin_week_no = cal.prev_fin_week_no
--               and po.sk1_item_no     = 20384744
             --order by po.fin_week_no 
            )    --select * from    GRN_YTD ;
           select /*+ parallel (po,4) full(po)*/
                  nvl(po.sk1_item_no, ytd.sk1_item_no) sk1_item_no,
                  nvl(po.sk1_supplier_no, ytd.sk1_supplier_no) sk1_supplier_no,
                  nvl(po.fin_year_no, (select fin_year_no from calendar_current)) fin_year_no,
                  nvl(po.fin_week_no , (select fin_week_no from calendar_current)) fin_week_no,
                  nvl(po.fin_quarter_no , (select fin_quarter_no from calendar_current))  fin_quarter_no,
                  nvl(po.fin_year_quarter_no , (select fin_year_quarter_no from calendar_current)) fin_year_quarter_no,
                  nvl(po.this_week_start_date, (select this_week_start_date from calendar_current)) this_week_start_date,
                  nvl(po.po_grn_qty,0) po_grn_qty,
                  nvl(po.po_grn_qty,0) + nvl(ytd.po_grn_qty_ytd,0) po_grn_qty_ytd
             from grn_measures po
--             join calendar_current cl
--                   on po.fin_year_no = cl.fin_year_no
--                  and po.fin_week_no = cl.fin_week_no
             full outer join GRN_YTD ytd             
                   on po.sk1_item_no = ytd.sk1_item_no
                  and po.sk1_supplier_no = ytd.sk1_supplier_no        
        ) mer_rec
       
   on    (tmp.sk1_item_no	      =	mer_rec.sk1_item_no     and
          tmp.sk1_supplier_no  	  =	mer_rec.sk1_supplier_no and
          tmp.fin_year_no	      =	mer_rec.fin_year_no     and    
          tmp.fin_week_no         =	mer_rec.fin_week_no)
            
   when matched then 
   update set                                                                                                     
          tmp.this_week_start_date  =   mer_rec.this_week_start_date,
          tmp.fin_quarter_no        =   mer_rec.fin_quarter_no,
          tmp.fin_year_quarter_no   =   mer_rec.fin_year_quarter_no,
          tmp.po_grn_qty            =	mer_rec.po_grn_qty,
          tmp.po_grn_qty_ytd        =	mer_rec.po_grn_qty_ytd,
          tmp.last_updated_date     =   g_date
          
   when not matched then
   insert                                                                                                         
         (sk1_item_no,
          sk1_supplier_no,
          fin_year_no,
          fin_week_no,
          fin_quarter_no,
          fin_year_quarter_no,
          this_week_start_date,
          po_grn_qty,
          po_grn_qty_ytd,
          last_updated_date
         )
  values                                                                                                           
         (          
          mer_rec.sk1_item_no,
          mer_rec.sk1_supplier_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.fin_quarter_no,
          mer_rec.fin_year_quarter_no,
          mer_rec.this_week_start_date,
          mer_rec.po_grn_qty,
          mer_rec.po_grn_qty_ytd,
          g_date
          );  
             
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
end wh_prf_fpi_008u                                                                                           
;
