--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_816U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_816U" 
                        (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Alfonso Joshua
--               Extracting weekly sales and GRN data from dim_item on selected suppliers (BridgeThorne)
--
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE, RTL_LOC_ITEM_WK_RMS_SPARSE,                          
--                      - RTL_LOC_ITEM_WK_RMS_STOCK, RTL_LOC_ITEM_WK_CATALOG,
--                      - RTL_LOC_ITEM_WK_PLAN_DISP_GRP
--               Output - RTL_ITEM_WK_CLUSTER                                                       
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
--  
--
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
g_last_wk_start_date date;
g_last_wk_end_date   date;
g_calendar_date      date;
g_loop_date          date;
g_start_date         date ;
g_end_date           date ;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_816U';                              
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS FACTS ONTO PLANOGRAM CLUSTER EX CKB';   
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_merge_update as
begin

 g_loop_date := g_date;

 FOR g_sub IN 0..0
  LOOP
    g_recs_read := 0;
    SELECT
      this_week_start_date,
      this_week_end_date,
      fin_year_no,
      fin_week_no
    INTO
      g_start_date,
      g_end_date,
      g_fin_year_no,
      g_fin_week_no
    FROM dim_calendar
    WHERE calendar_date = g_loop_date - (g_sub * 7); 

   l_text       := '-------------------------------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text       := 'Rollup range is:- '||g_start_date||'  To '||g_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text       := 'Year/week range is:- '||g_fin_year_no||' '||g_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   merge /* parallel (rtl,8) append */ into dwh_performance.rtl_item_wk_cluster rtl 
   using     (select /*+ full (ans) parallel (ans,6) */ distinct
                      sk1_item_no,
                      fin_year_no,
                      fin_week_no,
                      sk1_planogram_cluster,
                      sk1_planogram_supercategory,
                      sk1_planogram_category,
                      sk1_fixture_seq_no,
                      --sk1_floorplan_seq_no,
                      this_week_start_date,
                      nvl(size_break,'NO SIZE BREAK') as size_break,
                      sum(multi_display)         as multi_display,
                      sum(sales)                 as sales,
                      sum(sales_qty)             as sales_qty,
                      sum(sales_margin)          as sales_margin,
                      sum(waste_selling)         as waste_selling,
                      sum(waste_qty)             as waste_qty,
                      sum(waste_cost)            as waste_cost,
                      sum(fd_num_cust_avail_adj) as fd_num_cust_avail_adj,
                      sum(fd_num_cust_catlg_adj) as fd_num_cust_catlg_adj,
                      sum(fd_num_avail_days_adj) as fd_num_avail_days_adj,
                      sum(fd_num_catlg_days_adj) as fd_num_catlg_days_adj, 
                      sum(fd_num_catlg_days)     as fd_num_catlg_days,
                      sum(fd_cust_avail)         as fd_cust_avail
              from    rtl_loc_item_wk_cluster ans
              where   ans.fin_year_no = g_fin_year_no 
                and   ans.fin_week_no = g_fin_week_no
--                and   ans.sk1_item_no = 28546807 
--                and   ans.sk1_fixture_seq_no = 46
              group by
                      sk1_item_no,
                      fin_year_no,
                      fin_week_no,
                      sk1_planogram_cluster,
                      sk1_planogram_supercategory,
                      sk1_planogram_category,
                      sk1_fixture_seq_no,
                      --sk1_floorplan_seq_no,
                      this_week_start_date,
                      size_break 
        ) mer_rec

   on    (rtl.sk1_item_no	                =	mer_rec.sk1_item_no             and
          rtl.fin_year_no	                =	mer_rec.fin_year_no             and    
          rtl.fin_week_no                 =	mer_rec.fin_week_no                 and
          rtl.sk1_planogram_cluster       =	mer_rec.sk1_planogram_cluster       and
          rtl.sk1_planogram_supercategory =	mer_rec.sk1_planogram_supercategory and
          rtl.sk1_planogram_category      =	mer_rec.sk1_planogram_category      and
          --rtl.sk1_floorplan_seq_no        = mer_rec.sk1_floorplan_seq_no)       
          rtl.sk1_fixture_seq_no          = mer_rec.sk1_fixture_seq_no          and
          rtl.size_break                  = mer_rec.size_break)

   when matched then 
   update set  
          rtl.multi_display              =	mer_rec.multi_display,
          rtl.sales                      =	mer_rec.sales,  
          rtl.sales_qty                  =	mer_rec.sales_qty,
          rtl.sales_margin               =	mer_rec.sales_margin,          
          rtl.waste_cost                 =	mer_rec.waste_cost,
          rtl.waste_selling              =	mer_rec.waste_selling, 
          rtl.waste_qty                  =	mer_rec.waste_qty,
          rtl.fd_num_cust_avail_adj      =	mer_rec.fd_num_cust_avail_adj, 
          rtl.fd_num_cust_catlg_adj      =	mer_rec.fd_num_cust_catlg_adj,
          rtl.fd_num_avail_days_adj      =	mer_rec.fd_num_avail_days_adj, 
          rtl.fd_num_catlg_days_adj      =	mer_rec.fd_num_catlg_days_adj, 
          rtl.this_week_start_date       =  mer_rec.this_week_start_date,
          rtl.fd_num_catlg_days          =  mer_rec.fd_num_catlg_days,
          rtl.fd_cust_avail              =  mer_rec.fd_cust_avail,
          rtl.last_updated_date          =  g_date 
--          rtl.size_break                 =  mer_rec.size_break

   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_item_no,
          fin_year_no,
          fin_week_no,
          sk1_planogram_cluster,
          sk1_planogram_supercategory,
          sk1_planogram_category,
          --sk1_floorplan_seq_no,
          sk1_fixture_seq_no,
          this_week_start_date,
          multi_display,
          sales,  
          sales_qty,
          sales_margin,
          waste_selling,
          waste_qty,
          waste_cost,
          fd_num_cust_avail_adj,
          fd_num_cust_catlg_adj,
          fd_num_avail_days_adj,
          fd_num_catlg_days_adj,
          fd_num_catlg_days,
          fd_cust_avail,
          last_updated_date,
          size_break
         )
  values                                                                                                           -- COLUNM NAME CHANGE 
         (mer_rec.sk1_item_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.sk1_planogram_cluster,
          mer_rec.sk1_planogram_supercategory,
          mer_rec.sk1_planogram_category,
          mer_rec.sk1_fixture_seq_no,
          --mer_rec.sk1_floorplan_seq_no,
          mer_rec.this_week_start_date,
          mer_rec.multi_display,
          mer_rec.sales,  
          mer_rec.sales_qty,
          mer_rec.sales_margin,
          mer_rec.waste_selling,
          mer_rec.waste_qty,
          mer_rec.waste_cost,
          mer_rec.fd_num_cust_avail_adj,
          mer_rec.fd_num_cust_catlg_adj,
          mer_rec.fd_num_avail_days_adj,
          mer_rec.fd_num_catlg_days_adj,
          mer_rec.fd_num_catlg_days,
          mer_rec.fd_cust_avail,
          g_date,
          mer_rec.size_break
          )           
          ;   

   g_recs_read      :=  g_recs_read + sql%rowcount;
   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;


   l_text := 'RECORDS PROCESSED :- '||g_recs_read||' '||g_fin_year_no||' '||g_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;

  end loop;

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

    dwh_lookup.dim_control(g_date);
--    g_date := '20 oct 18';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    do_merge_update;

    l_text := 'MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);


    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
end wh_prf_corp_816u                                                                                             -- STORE PROC CHANGE 
;
