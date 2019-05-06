--------------------------------------------------------
--  DDL for Procedure WH_PRF_FPI_002U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_FPI_002U" 
(p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Francisca de Vaal
--               Extracting the woolworths branded item for the Sales items
--
--  Tables:      Input  - fnd_item_sup_prod_spec
--                      - dim_item
--                      - dim_supplier
--                      - dim_calendar
--               Output - rtl_item_sup_wk_wwbrand                                                       
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

g_date               date          :=trunc(sysdate);
g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_FPI_002U';                              
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
 
   merge into  rtl_item_sup_wk_wwbrand tmp
   using (
      with  calendar_list as (
                select distinct fin_year_no, fin_week_no,fin_quarter_no,fin_year_no||fin_quarter_no as fin_year_quarter_no,
                        this_week_start_date, this_week_end_date, calendar_date
                  from dim_calendar
                where calendar_date = g_date
--                where calendar_date between '20/SEP/2018' and '26/SEP/2018' -- Week 1
--                  where calendar_date between '25 jun 18' and '10 feb 19'
            ),
            WWBrandItem as (
            Select distinct 
                di.sk1_item_no
                ,ds.sk1_supplier_no
                ,cal.fin_year_no
                ,cal.fin_week_no
                ,cal.fin_quarter_no
                ,cal.fin_year_quarter_no
                ,ps.spec_version
                ,ps.spec_status
                ,ps.spec_type
                ,upper(ps.Brand) as Brand
                ,ps.spec_active_from_dte
                ,ps.spec_active_to_dte
            from fnd_item_sup_prod_spec ps
                 ,dim_item di
                 ,dim_supplier ds
                 ,calendar_list cal
            where ps.item_no = di.item_no
              and ps.supplier_no = ds.supplier_no
              and di.business_unit_no in (50)
              and cal.calendar_date between ps.spec_active_from_dte and ps.spec_active_to_dte
              and (ps.Brand like ('%WOOLWORTHS%') or  ps.Brand like ('%Woolworths%')or  ps.Brand like ('%woolworths%'))   
            ) 
            select distinct 
                sk1_item_no
                ,sk1_supplier_no
                ,fin_year_no
                ,fin_week_no
                ,fin_quarter_no
                ,fin_year_quarter_no
                ,spec_version
                ,spec_status
                ,spec_type
                ,Brand
--                ,spec_active_from_dte
                ,max(spec_active_from_dte) spec_active_from_dte
                ,spec_active_to_dte
                ,g_date last_updated_date
            from wwbranditem
            group by   sk1_item_no
                ,sk1_supplier_no
                ,fin_year_no
                ,fin_week_no
                ,fin_quarter_no
                ,fin_year_quarter_no
                ,spec_version
                ,spec_status
                ,spec_type
                ,brand
                ,spec_active_to_dte
--            order by sk1_item_no,sk1_supplier_no,fin_year_no,fin_week_no,spec_version
        ) mer_rec
         
   on    (tmp.sk1_item_no	      =	mer_rec.sk1_item_no     and
          tmp.sk1_supplier_no  	  =	mer_rec.sk1_supplier_no and
          tmp.fin_year_no	      =	mer_rec.fin_year_no     and    
          tmp.fin_week_no         =	mer_rec.fin_week_no     and 
          tmp.spec_version        =	mer_rec.spec_version    and 
          tmp.spec_type           =	mer_rec.spec_type
          )
            
   when matched then 
   update set                                                                                                       
          tmp.fin_quarter_no       =	mer_rec.fin_quarter_no,
          tmp.fin_year_quarter_no  =	mer_rec.fin_year_quarter_no,
          tmp.spec_status          =	mer_rec.spec_status,
          tmp.Brand                =	mer_rec.Brand,
          tmp.spec_active_from_dte =	mer_rec.spec_active_from_dte,
          tmp.spec_active_to_dte   =	mer_rec.spec_active_to_dte,
          tmp.last_updated_date    =    g_date
            
   when not matched then
   insert                                                                                                           
         (sk1_item_no,
          sk1_supplier_no,
          fin_year_no,
          fin_week_no,
          fin_quarter_no,
          fin_year_quarter_no,
          spec_version,
          spec_status,
          spec_type,
          Brand,
          spec_active_from_dte,
          spec_active_to_dte,
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
          mer_rec.spec_version,
          mer_rec.spec_status,
          mer_rec.spec_type,
          mer_rec.Brand,
          mer_rec.spec_active_from_dte,
          mer_rec.spec_active_to_dte,
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
end wh_prf_fpi_002u
;
