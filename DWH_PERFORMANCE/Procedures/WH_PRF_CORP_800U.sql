--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_800U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_800U" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        September 2017
--  Author:      A Joshua
--  Purpose:     Load DataMart for Planogram Display Groups and Merchandise data (sales, sparse & catalog)
--  Tables:      Input  - Planogram Info
--                        fnd_loc_planogram_dy
--                        fnd_planogram_dy_prod
--                        fnd_item_display_grp
--                      - Merchandise Info
--                        rtl_loc_item_wk_catalog
--                        rtl_loc_item_wk_rms_dense
--                        rtl_loc_item_wk_rms_sparse
--               Output - mart_fd_loc_item_wk_cluster_as
--
--  Maintenance:
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit         integer       :=  dwh_constants.vc_forall_limit;
g_recs_read            integer       :=  0;
g_recs_inserted        integer       :=  0;
g_date                 date;
g_date_loc             date;
g_date_prod            date;
g_date_grp             date;
g_season_start_date    date;
g_season_end_date      date;
g_6wk_bck_start_date   date;
g_loop_start_date      date;
g_loop_fin_year_no     number        :=  0;
g_loop_fin_week_no     number        :=  0;
g_wkday                number        :=  0;
g_sub                  integer       :=  0;
g_loop_cnt             integer       :=  0;
g_fin_week_no          dim_calendar.fin_week_no%type;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_800U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORTMENT & SPACE DISPLAY GROUPS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit := p_forall_limit;
   end if;
   
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOAD OF MART_FD_LOC_ITEM_DY_AS_GROUP STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';
   
      select max(last_updated_date) into g_date_loc  from fnd_loc_planogram_dy;
      select max(last_updated_date) into g_date_prod from fnd_planogram_dy_prod;
      select max(last_updated_date) into g_date_grp  from fnd_item_display_grp;

-- Delete data for reload

      select today_fin_day_no into g_wkday from dim_control_report;               -- BK30Sep2016        
    
      if g_wkday = 1 then
         g_loop_cnt := 5;
      else 
         g_loop_cnt := 6;
      end if;
    
      begin
         for g_sub in 1 .. g_loop_cnt                                                      -- BK30Sep2016                                                
            loop         
               select distinct this_week_start_date, fin_year_no, fin_week_no
               into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
               from   dim_calendar
               where  calendar_date = (g_date) - (g_sub * 7);      
            
            execute immediate 'alter table '|| 'DWH_PERFORMANCE' || '.'|| 'MART_FD_LOC_ITEM_WK_CLUSTER_AS' ||' truncate subpartition for ('||G_LOOP_FIN_YEAR_NO||','||G_LOOP_FIN_WEEK_NO||')';
    
            l_text := 'Truncate Partition: Year '||g_loop_fin_year_no||' Week '||g_loop_fin_week_no;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            commit;
            end loop;
      end;

-- Data reload

      g_loop_cnt := 0;
    
      select distinct count(distinct this_week_start_date) into g_loop_cnt 
      from dim_calendar where calendar_date between trunc(sysdate) - 42 and trunc(sysdate -7);
    
      for g_sub in 1..g_loop_cnt loop

         select distinct fin_year_no, fin_week_no
         into   g_fin_year_no, g_fin_week_no
         from   dim_calendar
         where  calendar_date = trunc(sysdate) - (g_sub * 7);
         
--         dbms_output.put_line('Date '||g_fin_year_no||' '||g_fin_week_no);

         l_text := 'Extract Period: Year '||g_fin_year_no||' Week '||g_fin_week_no;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         
      insert /*+ APPEND parallel (x,2) */ into mart_fd_loc_item_wk_cluster_as x
            
      with
       store_sel as (
       select /*+ full(a) */
              distinct location_no, 
              planogram_id, 
              nvl(planogram_cluster,'999') planogram_cluster 
       from   fnd_loc_planogram_dy a   
       where  last_updated_date = g_date_loc
       ),
        
       item_sel as (
       select /*+ full(a) parallel(a,4) full(b) parallel(b,4) */
              distinct a.item_no, 
              b.location_no, 
              b.planogram_id, 
              b.planogram_cluster
        from  fnd_planogram_dy_prod a, 
              store_sel b
        where a.planogram_id = b.planogram_id
         and  a.last_updated_date = g_date_prod
        ),
        
        planogram_cnt as (
        select count(*) pln_cnt, item_no, location_no
        from item_sel
        group by item_no, location_no
        ) ,
        
       display_grp_sel as (
       select /*+ full(a) full(b) parallel(b,4) full(c) full(d) */
              b.item_no, 
              b.location_no, 
              c.sk1_location_no, 
              d.sk1_item_no,
              d.fd_product_no, 
              d.fd_discipline_type,
              b.planogram_cluster,
              e.pln_cnt
       from   fnd_item_display_grp a, 
              item_sel b, 
              dim_location c, 
              dim_item d,
              planogram_cnt e
       where  a.item_no = b.item_no
        and   b.location_no = c.location_no
        and   b.item_no = d.item_no
        and   b.item_no = e.item_no
        and   b.location_no = e.location_no
        and   d.business_unit_no = 50
        and   a.last_updated_date = g_date_grp
       ) ,
/*                
       date_sel as (
       select this_wk_start_date - 21 this_wk_start_date, 
              this_wk_end_date - 7 this_wk_end_date
       from   dim_control_report
       ),  
       
       week_sel as (
       select distinct fin_year_no, fin_week_no
       from   dim_calendar_wk a, date_sel b
       where  a.this_week_start_date between b.this_wk_start_date and b.this_wk_end_date
       ),
*/        
       catalog_sel as (
       select /*+ full(a) parallel(a,4) full(c) */
              a.sk1_item_no, 
              a.sk1_location_no,
              a.fin_year_no, 
              a.fin_week_no,
              a.this_wk_catalog_ind, 
              a.next_wk_catalog_ind,
              a.product_status_code, 
              a.product_status_1_code, 
              a.fd_num_cust_avail_adj,
              a.fd_num_catlg_days_adj,
              c.fd_product_no, 
              c.fd_discipline_type,
              c.planogram_cluster,
              c.pln_cnt
       from   rtl_loc_item_wk_catalog a, 
              display_grp_sel c
--              week_sel d
       where  a.fin_year_no           = g_fin_year_no  
        and   a.fin_week_no           = g_fin_week_no   
        and   a.sk1_item_no           = c.sk1_item_no
        and   a.sk1_location_no       = c.sk1_location_no
       ) ,     
              
       sales_sel as (
       select /*+ full(a) parallel(a,4) full(c) */
              a.sk1_item_no, 
              a.sk1_location_no,
              a.fin_year_no, 
              a.fin_week_no, 
              a.sales, 
              a.sales_qty, 
              a.sales_margin,
              c.fd_product_no, 
              c.fd_discipline_type,
              c.planogram_cluster,
              c.pln_cnt
       from   rtl_loc_item_wk_rms_dense a, 
              display_grp_sel c
--              week_sel d         
       where  a.fin_year_no           = g_fin_year_no  
        and   a.fin_week_no           = g_fin_week_no 
        and   a.sk1_item_no           = c.sk1_item_no
        and   a.sk1_location_no       = c.sk1_location_no
       ),
        
       sparse_sel as (
       select /*+ full(a) parallel (a,4) full(c) */
              a.sk1_item_no, 
              a.sk1_location_no,
              a.fin_year_no, 
              a.fin_week_no,  
              a.waste_cost,
              c.fd_product_no, 
              c.fd_discipline_type,
              c.planogram_cluster,
              c.pln_cnt
       from   rtl_loc_item_wk_rms_sparse a, 
              display_grp_sel c
--              week_sel d
       where  a.fin_year_no           = g_fin_year_no  
        and   a.fin_week_no           = g_fin_week_no 
        and   a.sk1_item_no           = c.sk1_item_no
        and   a.sk1_location_no       = c.sk1_location_no
        and   waste_cost is not null
       ) ,

       all_together as (
       select /*+  parallel(extr1,6) parallel(extr2,6) parallel(extr3,6) */ 
              nvl(nvl(extr1.sk1_location_no,extr2.sk1_location_no),extr3.sk1_location_no) sk1_location_no,
              nvl(nvl(extr1.sk1_item_no,extr2.sk1_item_no),extr3.sk1_item_no) sk1_item_no,
              nvl(nvl(extr1.fin_year_no,extr2.fin_year_no),extr3.fin_year_no) fin_year_no,
              nvl(nvl(extr1.fin_week_no,extr2.fin_week_no),extr3.fin_week_no) fin_week_no,
              extr2.sales sales,
              extr2.sales_qty sales_qty,
              extr2.sales_margin sales_margin,
              extr3.waste_cost waste_cost,
              extr1.fd_num_cust_avail_adj fd_num_cust_avail_adj,
              extr1.fd_num_catlg_days_adj fd_num_catlg_days_adj,
              extr1.this_wk_catalog_ind this_wk_catalog_ind,
              extr1.next_wk_catalog_ind next_wk_catalog_ind,
              nvl(nvl(extr1.fd_product_no,extr2.fd_product_no),extr3.fd_product_no) fd_product_no,
              nvl(nvl(extr1.fd_discipline_type,extr2.fd_discipline_type),extr3.fd_discipline_type) fd_discipline_type,
              nvl(nvl(extr1.planogram_cluster,extr2.planogram_cluster),extr3.planogram_cluster) planogram_cluster,
              nvl(nvl(extr1.pln_cnt,extr2.pln_cnt),extr3.pln_cnt) pln_cnt,
              g_date as last_updated_date
       
       from   catalog_sel extr1
       
       full outer join 
              sales_sel extr2 on extr1.sk1_location_no   =     extr2.sk1_location_no
                             and extr1.sk1_item_no       =     extr2.sk1_item_no 
                             and extr1.fin_year_no       =     extr2.fin_year_no
                             and extr1.fin_week_no       =     extr2.fin_week_no
                             and extr1.planogram_cluster =     extr2.planogram_cluster
                                       
       full outer join 
              sparse_sel extr3 on nvl(extr1.sk1_location_no,   extr2.sk1_location_no)   = extr3.sk1_location_no
                              and nvl(extr1.sk1_item_no,       extr2.sk1_item_no)       = extr3.sk1_item_no
                              and nvl(extr1.fin_year_no,       extr2.fin_year_no)       = extr3.fin_year_no
                              and nvl(extr1.fin_week_no,       extr2.fin_week_no)       = extr3.fin_week_no
                              and nvl(extr1.planogram_cluster, extr2.planogram_cluster) = extr3.planogram_cluster) 

       select sk1_location_no,
              sk1_item_no,
              fin_year_no,
              fin_week_no,
              planogram_cluster,
              sum(sales) ,
              sum(sales_qty) ,
              sum(sales_margin) ,
              sum(waste_cost) ,
              sum(fd_num_cust_avail_adj),
              sum(fd_num_catlg_days_adj),
              max(this_wk_catalog_ind) ,
              max(next_wk_catalog_ind) ,
              fd_product_no,
              fd_discipline_type,
              pln_cnt,
              last_updated_date
       from   all_together 
       group by 
              sk1_location_no,
              sk1_item_no,
              fin_year_no,
              fin_week_no,
              planogram_cluster,
              fd_product_no,
              fd_discipline_type,
              pln_cnt,
              last_updated_date; 
 
       g_recs_read := g_recs_read + sql%rowcount;
       g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

       commit;
      end loop;
 
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
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

end wh_prf_corp_800u;
