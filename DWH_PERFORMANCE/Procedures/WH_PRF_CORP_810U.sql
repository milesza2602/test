--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_810U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_810U" 
                        (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Alfonso Joshua
--               Extracting weekly sales and GRN data from dim_item on selected suppliers (BridgeThorne)
--
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE, RTL_LOC_ITEM_WK_RMS_SPARSE,                          
--                      - RTL_LOC_ITEM_WK_RMS_STOCK, RTL_LOC_ITEM_WK_CATALOG,
--                      - RTL_LOC_ITEM_WK_PLAN_DISP_GRP
--               Output - RTL_LOC_ITEM_WK_PLAN_FACTS                                                       
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_810U';                              
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS FACTS ONTO PLANOGRAM EX CKB';   
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
  
   merge /* parallel (rtl,8) append */ into dwh_performance.rtl_loc_item_wk_plan_facts rtl 
   using (
    with 
        ans_list  as 
             (select /*+ full (ans) parallel (ans,6) */ distinct
                     sk1_location_no,
                     sk1_item_no,
                     sk1_planogram_id,
                     fin_year_no,
                     fin_week_no,
                     this_week_start_date
              from   rtl_loc_item_wk_plan_disp_grp ans
              where  ans.fin_year_no = g_fin_year_no 
                and  ans.fin_week_no = g_fin_week_no
--                where  ans.sk1_location_no in (392,566)
--                and  ans.sk1_item_no = 17533076
             ),
        distinct_key as (
            select /*+ full (ans) parallel (grp,6) */
                   distinct sk1_location_no, sk1_planogram_id, sk1_item_no, fin_year_no, fin_week_no, this_week_start_date
            from   rtl_loc_item_wk_plan_disp_grp grp
            where  fin_year_no = g_fin_year_no 
              and  fin_week_no = g_fin_week_no
--              and  sk1_item_no = 17533076
            ),
        distinct_cnt as ( 
            select distinct count(*) as cnt, sk1_location_no, sk1_item_no, fin_year_no, fin_week_no, this_week_start_date
            from distinct_key 
            group by sk1_location_no, sk1_item_no, fin_year_no, fin_week_no, this_week_start_date
--            having count(*) > 1
            ),
        multi_dsp as (
            select a.*, b.cnt as multi_display
            from distinct_key a, distinct_cnt b
            where a.fin_year_no = b.fin_year_no
             and  a.fin_week_no = b.fin_week_no
             and  a.sk1_location_no = b.sk1_location_no
             and  a.sk1_item_no = b.sk1_item_no
            ),
        sales_measures as 
            (  
              select /*+ parallel (dns,8) full(dns) full (al) parallel (al,6) */        
                     distinct
                     dns.sk1_location_no,
                     dns.sk1_item_no,
                     dns.fin_year_no,
                     dns.fin_week_no,
                     nvl(al.sk1_planogram_id,'NO PLANOGRAM') sk1_planogram_id,
                     dns.this_week_start_date,
                     dns.sales,
                     dns.sales_qty,
                     dns.sales_margin      
              from rtl_loc_item_wk_rms_dense dns,
                   ans_list al,
                   dim_item di
              where dns.fin_year_no     = al.fin_year_no(+)
                and dns.fin_week_no     = al.fin_week_no(+)
                and dns.sk1_item_no     = al.sk1_item_no(+)
                and dns.sk1_location_no = al.sk1_location_no(+)
                and dns.fin_year_no     = g_fin_year_no 
                and dns.fin_week_no     = g_fin_week_no
                and dns.sk1_item_no     = di.sk1_item_no
                and di.business_unit_no  = 50
                and (dns.sales is not null or
                     dns.sales_qty is not null or
                     dns.sales_margin is not null)
               ), 
            waste_measures as 
            (  
              select /*+ parallel(spa,8) full(spa) full (al) parallel (al,6) */
                     distinct
                     spa.sk1_location_no,
                     spa.sk1_item_no,
                     spa.fin_year_no,
                     spa.fin_week_no,
                     nvl(al.sk1_planogram_id,'NO PLANOGRAM') sk1_planogram_id,
                     spa.this_week_start_date,
                     spa.waste_selling,
                     spa.waste_qty,
                     spa.waste_cost
              from rtl_loc_item_wk_rms_sparse spa,
                   ans_list al,
                   dim_item di
              where spa.fin_year_no     = al.fin_year_no(+)
                and spa.fin_week_no     = al.fin_week_no(+)
                and spa.sk1_item_no     = al.sk1_item_no(+)
                and spa.sk1_location_no = al.sk1_location_no(+)
                and spa.fin_year_no     = g_fin_year_no
                and spa.fin_week_no     = g_fin_week_no
                and spa.sk1_item_no     = di.sk1_item_no
                and di.business_unit_no = 50
                and (spa.waste_cost is not null or
                     spa.waste_selling is not null or
                     spa.waste_qty is not null)
            ),
            stock_measures as 
            (  
              select /*+ parallel(st,8) full(st) full (al) parallel (al,6) */
                     distinct
                     st.sk1_location_no,
                     st.sk1_item_no,
                     st.fin_year_no fin_year_no,
                     st.fin_week_no fin_week_no,
                     nvl(al.sk1_planogram_id,'NO PLANOGRAM') sk1_planogram_id,
                     st.this_week_start_date,
                     st.soh_selling,
                     st.soh_qty,
                     st.boh_selling,
                     st.boh_qty
              from rtl_loc_item_wk_rms_stock st,
                   ans_list al
              where st.fin_year_no     = al.fin_year_no 
                and st.fin_week_no     = al.fin_week_no
                and st.sk1_item_no     = al.sk1_item_no
                and st.sk1_location_no = al.sk1_location_no
                and st.fin_year_no     = g_fin_year_no 
                and st.fin_week_no     = g_fin_week_no  
                and (st.soh_selling is not null or
                     st.soh_qty is not null or
                     st.boh_selling is not null or
                     st.boh_qty is not null)
            ),
            catalog_list as
            (  
              select /*+ parallel( cat,8) full(cat) full (al) parallel (al,6) */
                   distinct
                   cat.sk1_location_no,
                   cat.sk1_item_no,
                   cat.fin_year_no fin_year_no,
                   cat.fin_week_no fin_week_no,
                   nvl(al.sk1_planogram_id,'NO PLANOGRAM') sk1_planogram_id,
                   cat.this_week_start_date,
                   cat.fd_num_cust_avail_adj,
                   cat.fd_num_cust_catlg_adj,
                   cat.fd_num_avail_days_adj,
                   cat.fd_num_catlg_days_adj,
                   cat.boh_adj_qty,
                   cat.boh_adj_selling,
                   cat.soh_adj_qty,
                   cat.soh_adj_selling,
                   cat.num_units_per_tray,
                   cat.fd_num_catlg_days,
                   cat.fd_cust_avail                   
              from rtl_loc_item_wk_catalog cat,
                   ans_list al
              where cat.fin_year_no     = al.fin_year_no 
                and cat.fin_week_no     = al.fin_week_no
                and cat.sk1_item_no     = al.sk1_item_no
                and cat.sk1_location_no = al.sk1_location_no
                and cat.fin_year_no     = g_fin_year_no
                and cat.fin_week_no     = g_fin_week_no
                and (cat.fd_num_cust_avail_adj is not null or
                     cat.fd_num_cust_catlg_adj is not null or
                     cat.fd_num_avail_days_adj is not null or
                     cat.fd_num_catlg_days_adj is not null or
                     cat.boh_adj_selling       is not null or
                     cat.boh_adj_qty           is not null or
                     cat.soh_adj_selling       is not null or
                     cat.soh_adj_qty           is not null or
                     cat.fd_num_catlg_days     is not null or
                     cat.fd_cust_avail         is not null)             
            )
            select distinct
                nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no), f3.sk1_location_no), f4.sk1_location_no) sk1_location_no,
                nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no) , f4.sk1_item_no) sk1_item_no,
                nvl(nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no), f4.fin_year_no) fin_year_no,
                nvl(nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no), f4.fin_week_no) fin_week_no,
                nvl(nvl(nvl(nvl(f0.sk1_planogram_id, f1.sk1_planogram_id), f2.sk1_planogram_id), f3.sk1_planogram_id), f4.sk1_planogram_id) sk1_planogram_id,
                nvl(nvl(nvl(nvl(f0.this_week_start_date, f1.this_week_start_date), f2.this_week_start_date), f3.this_week_start_date), f4.this_week_start_date) this_week_start_date,
                sum(nvl(f4.multi_display,0))   as multi_display, 
                sum(case 
                       when f4.multi_display > 1 then 
                           (f1.sales /f4.multi_display) else
                           (f1.sales /1) end) sales,
                sum(case 
                        when f4.multi_display > 1 then 
                            (f1.sales_qty /f4.multi_display) else
                            (f1.sales_qty /1) end) sales_qty,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f1.sales_margin /f4.multi_display) else
                          (f1.sales_margin /1) end) sales_margin,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f2.waste_selling /f4.multi_display) else
                          (f2.waste_selling /1) end) waste_selling,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f2.waste_qty /f4.multi_display) else
                             (f2.waste_qty /1) end) waste_qty,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f2.waste_cost /f4.multi_display) else
                          (f2.waste_cost /1) end) waste_cost,
                sum(f0.fd_num_cust_avail_adj) as fd_num_cust_avail_adj,
                sum(f0.fd_num_cust_catlg_adj) as fd_num_cust_catlg_adj,
                sum(f0.fd_num_avail_days_adj) as fd_num_avail_days_adj,
                sum(f0.fd_num_catlg_days_adj) as fd_num_catlg_days_adj, 
                sum(f0.num_units_per_tray)    as num_units_per_tray, 
                sum(f0.fd_num_catlg_days)     as fd_num_catlg_days,
                sum(f0.fd_cust_avail)         as fd_cust_avail,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f0.soh_adj_selling /f4.multi_display) else
                          (f0.soh_adj_selling /1) end) soh_adj_selling,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f3.soh_selling /f4.multi_display) else
                          (f3.soh_selling /1) end) soh_selling,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f0.boh_adj_selling /f4.multi_display) else
                          (f0.boh_adj_selling /1) end) boh_adj_selling,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f3.boh_selling /f4.multi_display) else
                          (f3.boh_selling /1) end) boh_selling,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f0.soh_adj_qty /f4.multi_display) else
                          (f0.soh_adj_qty /1) end) soh_adj_qty,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f3.soh_qty /f4.multi_display) else
                          (f3.soh_qty /1) end) soh_qty,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f0.boh_adj_qty /f4.multi_display) else
                          (f0.boh_adj_qty /1) end) boh_adj_qty,
                sum(case 
                      when f4.multi_display > 1 then 
                          (f3.boh_qty /f4.multi_display) else
                          (f3.boh_qty /1) end) boh_qty,
                g_date                  as last_updated_date
            from catalog_list f0
                full outer join
                 sales_measures f1
                    on f0.sk1_location_no   = f1.sk1_location_no
                   and f0.sk1_item_no       = f1.sk1_item_no
                   and f0.fin_year_no       = f1.fin_year_no
                   and f0.fin_week_no       = f1.fin_week_no  
                   and f0.sk1_planogram_id  = f1.sk1_planogram_id
               full outer join
                 waste_measures f2
                    on nvl(f0.sk1_location_no, f1.sk1_location_no)   = f2.sk1_location_no
                   and nvl(f0.sk1_item_no, f1.sk1_item_no)           = f2.sk1_item_no
                   and nvl(f0.fin_year_no, f1.fin_year_no)           = f2.fin_year_no
                   and nvl(f0.fin_week_no, f1.fin_week_no)           = f2.fin_week_no
                   and nvl(f0.sk1_planogram_id, f1.sk1_planogram_id) = f2.sk1_planogram_id
               full outer join
                 stock_measures f3
                    on nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no)    = f3.sk1_location_no
                   and nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no)                = f3.sk1_item_no
                   and nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no)                = f3.fin_year_no
                   and nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no)                = f3.fin_week_no
                   and nvl(nvl(f0.sk1_planogram_id, f1.sk1_planogram_id), f2.sk1_planogram_id) = f3.sk1_planogram_id 
               full outer join
                 multi_dsp f4
                    on nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no), f3.sk1_location_no)      = f4.sk1_location_no
                   and nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no)                      = f4.sk1_item_no
                   and nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no)                      = f4.fin_year_no
                   and nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no)                      = f4.fin_week_no
                   and nvl(nvl(nvl(f0.sk1_planogram_id, f1.sk1_planogram_id), f2.sk1_planogram_id), f3.sk1_planogram_id)  = f4.sk1_planogram_id
            group by
                 nvl(nvl(nvl(nvl(f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no), f3.sk1_location_no), f4.sk1_location_no),
                 nvl(nvl(nvl(nvl(f0.sk1_item_no, f1.sk1_item_no), f2.sk1_item_no), f3.sk1_item_no) , f4.sk1_item_no), 
                 nvl(nvl(nvl(nvl(f0.fin_year_no, f1.fin_year_no), f2.fin_year_no), f3.fin_year_no), f4.fin_year_no),
                 nvl(nvl(nvl(nvl(f0.fin_week_no, f1.fin_week_no), f2.fin_week_no), f3.fin_week_no), f4.fin_week_no),
                 nvl(nvl(nvl(nvl(f0.sk1_planogram_id, f1.sk1_planogram_id), f2.sk1_planogram_id), f3.sk1_planogram_id), f4.sk1_planogram_id),
                 nvl(nvl(nvl(nvl(f0.this_week_start_date, f1.this_week_start_date), f2.this_week_start_date), f3.this_week_start_date), f4.this_week_start_date),
                 g_date
        ) mer_rec
         
   on    (rtl.sk1_item_no	        =	mer_rec.sk1_item_no     and
          rtl.sk1_location_no  	  =	mer_rec.sk1_location_no and
          rtl.fin_year_no	        =	mer_rec.fin_year_no     and    
          rtl.fin_week_no         =	mer_rec.fin_week_no     and
          rtl.sk1_planogram_id    =	mer_rec.sk1_planogram_id)
            
   when matched then 
   update set  
          rtl.multi_display              =	mer_rec.multi_display,
          rtl.sales                      =	mer_rec.sales,  
          rtl.sales_qty                  =	mer_rec.sales_qty,
          rtl.sales_margin               =	mer_rec.sales_margin,          
          rtl.waste_cost                 =	mer_rec.waste_cost,
          rtl.waste_selling              =	mer_rec.waste_selling,    
          rtl.waste_qty                  =	mer_rec.waste_qty,
          rtl.soh_selling                =	mer_rec.soh_selling,
          rtl.soh_adj_selling            =	mer_rec.soh_adj_selling,
          rtl.soh_qty                    =	mer_rec.soh_qty,
          rtl.soh_adj_qty                =	mer_rec.soh_adj_qty,
          rtl.boh_selling                =	mer_rec.boh_selling,
          rtl.boh_qty                    =	mer_rec.boh_qty,
          rtl.boh_adj_selling            =	mer_rec.boh_adj_selling,
          rtl.boh_adj_qty                =	mer_rec.boh_adj_qty,                                      
          rtl.fd_num_cust_avail_adj      =	mer_rec.fd_num_cust_avail_adj, 
          rtl.fd_num_cust_catlg_adj      =	mer_rec.fd_num_cust_catlg_adj,
          rtl.fd_num_avail_days_adj      =	mer_rec.fd_num_avail_days_adj, 
          rtl.fd_num_catlg_days_adj      =	mer_rec.fd_num_catlg_days_adj, 
          rtl.this_week_start_date       =  mer_rec.this_week_start_date,
          rtl.num_units_per_tray         =  mer_rec.num_units_per_tray,
          rtl.fd_num_catlg_days          =  mer_rec.fd_num_catlg_days,
          rtl.fd_cust_avail              =  mer_rec.fd_cust_avail,
          rtl.last_updated_date          =  g_date
            
   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_location_no,
          sk1_item_no,
          fin_year_no,
          fin_week_no,
          sk1_planogram_id,
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
          soh_adj_selling,
          soh_selling,
          boh_adj_selling,
          boh_selling,
          soh_adj_qty,
          soh_qty,
          boh_adj_qty,
          boh_qty,
          num_units_per_tray,
          fd_num_catlg_days,
          fd_cust_avail,
          last_updated_date
         )
  values                                                                                                           -- COLUNM NAME CHANGE 
         (mer_rec.sk1_location_no,
          mer_rec.sk1_item_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.sk1_planogram_id,
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
          mer_rec.soh_adj_selling,
          mer_rec.soh_selling,
          mer_rec.boh_adj_selling,
          mer_rec.boh_selling,
          mer_rec.soh_adj_qty,
          mer_rec.soh_qty,
          mer_rec.boh_adj_qty,
          mer_rec.boh_qty,
          mer_rec.num_units_per_tray,
          mer_rec.fd_num_catlg_days,
          mer_rec.fd_cust_avail,
          g_date
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
end wh_prf_corp_810u                                                                                             -- STORE PROC CHANGE 
;
