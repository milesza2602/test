--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_088U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_088U" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        January 2019
--  Author:      Francisca de Vaal
--  Purpose:     Create future product status and future catalog status fact table
--  Tables:      Input  - fnd_loc_item_dy_fut_catlog,
--                      - fnd_zone_item_dy_fut_prodstat,
--                      - fnd_zone_item,
--                      - fnd_location_item,
--                      - dim_location,
--                      - dim_item,
--                      - dim_zone,
--                      - rtl_location_item,
--                      - dim_item_display_grp
--               Output - rtl_loc_item_dy_prodstatus
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;

g_recs_dummy         integer       :=  0;
g_physical_updated   integer       :=  0;

g_error_count        number        :=  0;
g_error_index        number        :=  0;

G_COUNT              number        :=  0;
g_rec_out            rtl_loc_item_dy_product_status%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_088U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_LOC_ITEM_DY_PRODSTATUS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--************************************************************************************************** 
-- Merge process from data sources
--**************************************************************************************************
procedure do_merge_update as
begin

   merge into  rtl_loc_item_dy_product_status rtl
   using (
      with prodstat as (
        select /*+ materialize full(b)*/
               a.product_status_code future_product_status_code,
               b.sk1_fd_zone_group_zone_no,
               b.sk1_location_no,
               a.zone_no,
               c.item_no,
               c.sk1_item_no,
               b.location_no,
               b.wh_fd_zone_group_no zone_group_no,               
               a.effective_date product_effective_date,
               a.last_updated_date
        from   fnd_zone_item_dy_fut_prodstat a, 
               dim_location b, 
               dim_item c
        where  a.last_updated_date = g_date
--        and    a.item_no = 6009175979931
        and    a.item_no           = c.item_no
        and    a.zone_no           = b.wh_fd_zone_no
        and    b.loc_type          = 'S'
--        and    b.area_no           = 9951
--        and    b.region_no         not in (9957,8827)
        and    b.st_close_date     > sysdate - 1
) --select count(*) from prodstat; 425
,
           catalog as (
        select /*+ materialize cardinality (a,300000) */ 
               b.sk1_fd_zone_group_zone_no,
               b.sk1_location_no,
               d.zone_no,
               c.item_no,
               c.sk1_item_no,
               b.location_no,
               a.catalog_ind future_catalog_ind, 
               a.effective_date,
               d.zone_group_no
        from   fnd_loc_item_dy_fut_catlog a,
               dim_location b,
               dim_item c,
               fnd_zone_item d
        where  a.catalog_type = 'F'
        and    a.last_updated_date = g_date
        and    a.location_no = b.location_no
        and    a.item_no = c.item_no
        and    b.loc_type          = 'S'
--        and    b.area_no           = 9951
--        and    b.region_no         not in (9957,8827)
        and    b.st_close_date     > sysdate - 1
        and    b.wh_fd_zone_group_no = 1
        and    b.wh_fd_zone_group_no = d.zone_group_no
        and    a.item_no       = d.item_no
--        and    a.item_no = 6009175979931
        ),
          future_catalog as (
          select nvl(a.item_no, b.item_no) item_no,
                 nvl(a.location_no, b.location_no) location_no,
                 nvl(a.zone_no, b.zone_no) zone_no,
                 nvl(a.zone_group_no, b.zone_group_no) zone_group_no,
                 nvl(a.product_effective_date,'31 jan 3000')  as product_effective_date,
                 a.future_product_status_code,
                 B.FUTURE_CATALOG_IND,
                 nvl(b.effective_date,'31 jan 3000')  as catalog_effective_date,
                 nvl(a.sk1_fd_zone_group_zone_no, b.sk1_fd_zone_group_zone_no) sk1_fd_zone_group_zone_no,
                 nvl(a.sk1_location_no, b.sk1_location_no) sk1_location_no,
                 nvl(a.sk1_item_no, b.sk1_item_no) sk1_item_no                 
          from prodstat a
          left outer join    catalog b
           on a.item_no = b.item_no
          and a.location_no  = b.location_no
          and a.zone_no = b.zone_no
            ) 
            
          select d.sk1_location_no, 
               d.sk1_item_no,
               d.catalog_effective_date, 
               d.sk1_fd_zone_group_zone_no sk1_zone_group_zone_no, 
               d.zone_no, 
               d.product_effective_date, --Added as per requested by Majeedah on the 10/12/2018
               d.future_catalog_ind, 
               d.future_product_status_code, 
               d.zone_group_no,
               a.this_wk_catalog_ind current_catalog_ind, 
               b.product_status_1_code current_prod_status_code,
               a.cost_price,
               a.num_units_per_tray             
        from   future_catalog d
        left outer join fnd_location_item a on
               d.item_no            = a.item_no
          and  d.location_no        = a.location_no
        left outer join fnd_zone_item b on
               d.item_no            = b.item_no
        and    d.zone_no            = b.zone_no   
        and    d.zone_group_no      = b.zone_group_no
        
        ) mer_rec
         
    on (rtl.sk1_item_no	           =	mer_rec.sk1_item_no     and
        rtl.sk1_location_no        =	mer_rec.sk1_location_no and
        rtl.catalog_effective_date =	mer_rec.catalog_effective_date  and
        rtl.product_effective_date =	mer_rec.product_effective_date  and
        rtl.sk1_zone_group_zone_no =  mer_rec.sk1_zone_group_zone_no        
        )          
    when matched then 
       update set
--              rtl.product_effective_date      =   mer_rec.product_effective_date, --Added as per requested by Majeedah on the 10/12/2018
              rtl.future_catalog_ind          =   mer_rec.future_catalog_ind, 
              rtl.future_product_status_code  =   mer_rec.future_product_status_code, 
              rtl.current_catalog_ind         =   mer_rec.current_catalog_ind, 
              rtl.current_prod_status_code    =   mer_rec.current_prod_status_code,
              rtl.cost_price                  =   mer_rec.cost_price,
              rtl.num_units_per_tray          =   mer_rec.num_units_per_tray,
              rtl.zone_no                     =   mer_rec.zone_no,
              rtl.last_updated_date           =   g_date
              
    when not matched then
       insert
            (sk1_location_no, 
             sk1_item_no, 
             catalog_effective_date, 
             product_effective_date,
             sk1_zone_group_zone_no,
             zone_no,
             future_catalog_ind, 
             future_product_status_code, 
             current_catalog_ind, 
             current_prod_status_code,
             cost_price,
             num_units_per_tray,
             last_updated_date
            )
       values
            (mer_rec.sk1_location_no, 
             mer_rec.sk1_item_no, 
             mer_rec.catalog_effective_date, -- this is the product status effective date
             mer_rec.product_effective_date, -- this is the catalog status effective date
             mer_rec.sk1_zone_group_zone_no,
             mer_rec.zone_no,             
             mer_rec.future_catalog_ind, 
             mer_rec.future_product_status_code, 
             mer_rec.current_catalog_ind, 
             mer_rec.current_prod_status_code,
             mer_rec.cost_price,
             mer_rec.num_units_per_tray,
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
     
     select fin_year_no, fin_week_no, this_week_start_date  , this_week_end_date
     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date , g_last_wk_end_date
     from   dim_calendar 
     where  calendar_date = g_date;
    
    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    execute immediate 'truncate table dwh_performance.rtl_loc_item_dy_product_status';
    
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
end wh_prf_corp_088u;
