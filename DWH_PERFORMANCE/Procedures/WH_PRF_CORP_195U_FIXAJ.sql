--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_195U_FIXAJ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_195U_FIXAJ" (p_forall_limit in integer,p_success out boolean) as
-- ******************************************************************************************
--  Date:        January 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create shelf life facing on fnd_location_item
--               with input ex Intactix fnd_shelf_edge table from foundation layer.
--  Tables:      Input  - fnd_shelf_edge
--               Output - fnd_location_item
--  Packages:    constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            fnd_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date ;
g_end_date           date ;
g_loop_date          date ;
g_fd_num_cust_catlg_adj number(7,3);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_195U_FIXAJ';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD location_item shelf life facing EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Bulk merge
--**************************************************************************************************
procedure local_bulk_merge as
begin

g_loop_date := '04 mar 18';

FOR g_sub IN 0..1
  LOOP
    g_recs_read := 0;
    select
      calendar_date,
      this_week_end_date
    INTO
      g_start_date,
      g_end_date
    FROM dim_calendar
    WHERE calendar_date = g_loop_date + g_sub; 

  l_text       := '-------------------------------------------------------------';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text       := 'Rollup range is:- '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
   merge /*+ parallel(cat,4) */ into dwh_datafix.aj_rtl_loc_item_dy_catalog_c cat using   
   --dwh_datafix.aj_rtl_loc_item_dy_catalog cat using
  (
   with Table1 as
(
select /*+ parallel (li,6) full (li) */
           lid.sk1_item_no,
           lid.sk1_location_no,
           lid.calendar_date,
           lid.fd_num_catlg_days,
           lid.product_status_1_code,
           lid.num_shelf_life_days,
           lid.fd_num_avail_days_adj,
           nvl(lid.num_facings,0) as num_facings,
           nvl(lid.boh_adj_qty,0) as boh_adj_qty,
           nvl(rms.waste_qty,0) as waste_qty,
           di.pack_item_ind,
           di.handling_method_code,
           dl.active_store_ind,
           dl.st_open_date,
           diu.product_group_scaling_desc_501,
           diu.availability_desc_540,
           diu.product_class_desc_507,
           diu.new_line_indicator_desc_3502,    
         case 
              when di.pack_item_ind = 0 and
                   dl.active_store_ind = 1 and
                   diu.product_group_scaling_desc_501 = 'N' and
                   diu.availability_desc_540 = 'Yes' and
                   lid.fd_num_catlg_days = 1 and
                   diu.product_class_desc_507 <> 'H' and
                  (diu.new_line_indicator_desc_3502 <> 'First Week New' or
                  (diu.new_line_indicator_desc_3502 = 'First Week New'  and   
                   di.handling_method_code = 'S')) 
              then lid.fd_num_catlg_days
           else 0
           end fd_num_cust_catlg_adj_1
           
     from   rtl_loc_item_dy_catalog lid
    join   dim_item di      on lid.sk1_item_no     = di.sk1_item_no
    join   dim_location dl  on lid.sk1_location_no = dl.sk1_location_no 
    left outer join dim_item_uda diu
                            on lid.sk1_item_no     = diu.sk1_item_no
    left outer join rtl_loc_item_dy_rms_sparse rms
                            on lid.sk1_item_no     = rms.sk1_item_no and
                               lid.sk1_location_no = rms.sk1_location_no and
                               lid.calendar_date   = rms.post_date

),
Table2 as 
(
select /*+ parallel (li,6) full (li) */
           sk1_item_no,
           sk1_location_no,
           calendar_date,
           fd_num_catlg_days,
           fd_num_avail_days_adj,
           product_status_1_code,
           num_shelf_life_days,
           num_facings,
           boh_adj_qty,
           waste_qty,
           pack_item_ind,
           handling_method_code,
           active_store_ind,
           st_open_date,
           product_group_scaling_desc_501,
           availability_desc_540,
           product_class_desc_507,
           new_line_indicator_desc_3502,
           fd_num_cust_catlg_adj_1,
           case when st_open_date is not null --store is open
            Then 
                case when  --Store is not open as yet going to open soon
                    handling_method_code = 'S' 
                  and st_open_date > '03 mar 18'  
                then 0 else 1
          end end fd_num_cust_catlg_adj_2

from Table1
),
Table3 as 
(
select /*+ parallel (li,6) full (li) */
           sk1_item_no,
           sk1_location_no,
           calendar_date,
           fd_num_catlg_days,
           fd_num_avail_days_adj,
           product_status_1_code,
           num_shelf_life_days,
           num_facings,
           boh_adj_qty,
           waste_qty,
           pack_item_ind,
           handling_method_code,
           active_store_ind,
           st_open_date,
           product_group_scaling_desc_501,
           availability_desc_540,
           product_class_desc_507,
           new_line_indicator_desc_3502,
           fd_num_cust_catlg_adj_1,
           fd_num_cust_catlg_adj_2,
           case 
              when product_status_1_code = 4 then 0 else 1
           end fd_num_cust_catlg_adj_3

from Table2
),
Table4 as 
(  select /*+ parallel (li,6) full (li) */
           sk1_item_no,
           sk1_location_no,
           calendar_date,
           fd_num_catlg_days,
           fd_num_avail_days_adj,
           product_status_1_code,
           num_shelf_life_days,
           num_facings,
           boh_adj_qty,
           waste_qty,
           pack_item_ind,
           handling_method_code,
           active_store_ind,
           st_open_date,
           product_group_scaling_desc_501,
           availability_desc_540,
           product_class_desc_507,
           new_line_indicator_desc_3502,
           fd_num_cust_catlg_adj_1,
           fd_num_cust_catlg_adj_2,
           fd_num_cust_catlg_adj_3,
           Case when fd_num_cust_catlg_adj_1 = 1  and 
            fd_num_cust_catlg_adj_2 = 1 and
            fd_num_cust_catlg_adj_3 = 1 then 1
                else 0
           end as fd_num_cust_catlg_adj       
from Table3
)
select /*+ parallel (li,6) full (li) */
           sk1_item_no,
           sk1_location_no,
           calendar_date,
--           fd_num_catlg_days,
--           fd_num_avail_days_adj,
--           product_status_1_code,
--           num_shelf_life_days,
--           num_facings,
--           boh_adj_qty,
--           waste_qty,
--           pack_item_ind,
--           handling_method_code,
--           active_store_ind,
--           st_open_date,
--           product_group_scaling_desc_501,
--           availability_desc_540,
--           product_class_desc_507,
--           new_line_indicator_desc_3502,
--           fd_num_cust_catlg_adj_1,
--           fd_num_cust_catlg_adj_2,
--           fd_num_cust_catlg_adj_3,
           fd_num_cust_catlg_adj,
           case 
           when fd_num_cust_catlg_adj > 0 then 
              case
                 when nvl(num_facings,0) > 0 then                 
                    case
                       when (nvl(boh_adj_qty,0) + abs(nvl(waste_qty,0))) > nvl(num_facings,0) 
                          then 1
                          else (nvl(boh_adj_qty,0) + abs(nvl(waste_qty,0))) / nvl(num_facings,0)
                    end
                 else
                 case
                 when num_shelf_life_days >= 3 then 
                    case
                       when (nvl(boh_adj_qty,0) + abs(nvl(waste_qty,0))) < 3 
                       then 0
                       else 1
                    end
                 else fd_num_avail_days_adj
                 end
              end
           else 0
           end  fd_cust_avail
from Table4          
           
--    where calendar_date = '05 mar 18'
--    and   sk1_item_no in (18697162)--261940,64474,18322282)
--    and   sk1_location_no in (692,693,694)--620,946,643,425)
       

  ) mer_mart

   on (cat.sk1_item_no            = mer_mart.sk1_item_no
   and cat.sk1_location_no        = mer_mart.sk1_location_no
   and cat.calendar_date          = mer_mart.calendar_date
      )

   when matched then
     update
     set fd_cust_avail            = mer_mart.fd_cust_avail,
         fd_num_cust_catlg_adj    = mer_mart.fd_num_cust_catlg_adj
     ;

   g_recs_read      :=  g_recs_read + sql%rowcount;
--   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
   g_recs_updated   :=  g_recs_updated + sql%rowcount;
   
   l_text := 'RECORDS PROCESSED :- '||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  COMMIT;

 end loop;

   exception
     when dwh_errors.e_insert_error then
       l_message := 'BULK MERGE - INSERT / UPDATE ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

     when others then
       l_message := 'BULK MERG - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_bulk_merge;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Update OF rtl_loc_item_dy_catalog STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    local_bulk_merge;

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
end wh_prf_corp_195u_fixaj;
