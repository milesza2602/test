--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_748U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_748U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2913
--  Author:      Quentin Smit
--  Purpose:     Update STORE ORDERS ex JDA fact table in the performance layer
--
--  Tables:      Input  - fnd_rtl_loc_item_dy_ff_st_ord
--                        dwh_foundation.fnd_rtl_loc_item_dy_ff_dir_ord
--               Output - rtl_loc_item_dy_st_dir_ord
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
g_count              number        :=  0;
g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_748U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM STORE ORDER FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOCATION_ITEM STORE ORDERS EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--/*+ APPEND USE_HASH(rtl_lidso ,mer_lidso)*/
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

--MERGE  INTO dwh_performance.rtl_loc_item_dy_st_dir_ord rtl_lidso USING
MERGE  INTO /*+ PARALLEL(rtl_lidso,4) */ dwh_performance.rtl_loc_item_dy_st_dir_ord rtl_lidso USING
(
with
  fnd_lid as (select /*+ FULL(lid) parallel(lid,4) */
                  post_date, location_no, item_no, boh_qty1, boh_1_ind, boh_qty2, boh_qty3, store_order1, store_order2, store_order3, safety_qty, trading_date,
                  special_cases, safety_cases, forecast_cases, over_cases, last_updated_date
           from fnd_rtl_loc_item_dy_ff_st_ord lid 
          where last_updated_date = g_date),   --g_date
          
  fnd_lidd as (select /*+ FULL(lidd) parallel(lidd,4) */ post_date, location_no, item_no, direct_mu_qty1, direct_mu_qty2, direct_mu_qty3, direct_mu_qty4, direct_mu_qty5, direct_mu_qty6, direct_mu_qty7,
                      last_updated_date, direct_delivery_ind
           from dwh_foundation.fnd_rtl_loc_item_dy_ff_dir_ord lidd 
          where last_updated_date = g_date),   --g_date

  cc as (select 
                nvl(fnd_lid.location_no, fnd_lidd.location_no) as location_no,
                nvl(fnd_lid.item_no, fnd_lidd.item_no)  as item_no,
                nvl(fnd_lid.post_date, fnd_lidd.post_date) as post_date,
                nvl(fnd_lid.boh_qty1, 0) as boh_1_qty,
                nvl(fnd_lid.boh_1_ind, 0) as boh_1_ind,
                nvl(fnd_lid.boh_qty2, 0) as boh_2_qty,
                nvl(fnd_lid.boh_qty3, 0) as boh_3_qty,
                nvl(fnd_lid.store_order1, 0) as store_order1,
                nvl(fnd_lid.store_order2, 0) as store_order2,
                nvl(fnd_lid.store_order3, 0) as store_order3,
                nvl(fnd_lid.safety_qty, 0) as safety_qty,             --
                nvl(fnd_lid.trading_date, '') as trading_date,        --
                nvl(fnd_lid.special_cases, 0) as special_cases,       --
                nvl(fnd_lid.safety_cases, 0) as safety_cases,         --
                nvl(fnd_lid.forecast_cases, 0) as forecast_cases,     --
                nvl(fnd_lid.over_cases, 0)      as over_cases,             --

                nvl(fnd_lidd.direct_mu_qty1, 0) as direct_mu_qty1,
                nvl(fnd_lidd.direct_mu_qty2, 0) as direct_mu_qty2,
                nvl(fnd_lidd.direct_mu_qty3, 0) as direct_mu_qty3,
                nvl(fnd_lidd.direct_mu_qty4, 0) as direct_mu_qty4,
                nvl(fnd_lidd.direct_mu_qty5, 0) as direct_mu_qty5,
                nvl(fnd_lidd.direct_mu_qty6, 0) as direct_mu_qty6,
                nvl(fnd_lidd.direct_mu_qty7, 0) as direct_mu_qty7,
                nvl(fnd_lidd.direct_delivery_ind,0) as direct_delivery_ind,
                nvl(fnd_lid.last_updated_date, fnd_lidd.last_updated_date) as last_updated_date
                --g_date as last_updated_date

      from fnd_lid
      full outer join fnd_lidd on  fnd_lid.post_date = fnd_lidd.post_date
                              and fnd_lid.location_no     = fnd_lidd.location_no
                              and fnd_lid.item_no         = fnd_lidd.item_no),

  dd as (select cc.post_date,
                dl.sk1_location_no,
                di.sk1_item_no,
                cc.boh_1_qty,
                cc.boh_1_ind,
                cc.boh_2_qty,
                cc.boh_3_qty,
                cc.store_order1,
                cc.store_order2,
                cc.store_order3,
                cc.safety_qty,
                cc.trading_date,
                cc.direct_mu_qty1,
                cc.direct_mu_qty2,
                cc.direct_mu_qty3,
                cc.direct_mu_qty4,
                cc.direct_mu_qty5,
                cc.direct_mu_qty6,
                cc.direct_mu_qty7,
                cc.last_updated_date,
                cc.direct_delivery_ind
           from cc, dim_location dl, dim_item di
          where cc.location_no  = dl.location_no
            and cc.item_no      = di.item_no)

select * from dd order by post_date desc
) mer_lidso

ON  (mer_lidso.sk1_item_no = rtl_lidso.sk1_item_no
and mer_lidso.sk1_location_no = rtl_lidso.sk1_location_no
and mer_lidso.post_date = rtl_lidso.post_date )


WHEN MATCHED THEN
UPDATE
SET       boh_1_qty                       = mer_lidso.boh_1_qty,
          boh_1_ind                       = mer_lidso.boh_1_ind,
          boh_2_qty                       = mer_lidso.boh_2_qty,
          boh_3_qty                       = mer_lidso.boh_3_qty,
          store_order1                    = mer_lidso.store_order1,
          store_order2                    = mer_lidso.store_order2,
          store_order3                    = mer_lidso.store_order3,
          safety_qty                      = mer_lidso.safety_qty,
          --special_cases                   = mer_lidso.special_cases,
          --forecast_cases                  = mer_lidso.forecast_cases,
          --safety_cases                    = mer_lidso.safety_cases,
          --over_cases                      = mer_lidso.over_cases,
          trading_date                    = mer_lidso.trading_date,
          direct_mu_qty1                  = mer_lidso.direct_mu_qty1,
          direct_mu_qty2                  = mer_lidso.direct_mu_qty2,
          direct_mu_qty3                  = mer_lidso.direct_mu_qty3,
          direct_mu_qty4                  = mer_lidso.direct_mu_qty4,
          direct_mu_qty5                  = mer_lidso.direct_mu_qty5,
          direct_mu_qty6                  = mer_lidso.direct_mu_qty6,
          direct_mu_qty7                  = mer_lidso.direct_mu_qty7,
          last_updated_date               = mer_lidso.last_updated_date,
          direct_delivery_ind             = mer_lidso.direct_delivery_ind
WHEN NOT MATCHED THEN
INSERT
(         sk1_location_no,
          sk1_item_no,
          post_date,
          boh_1_qty,
          boh_1_ind,
          boh_2_qty,
          boh_3_qty,
          store_order1,
          store_order2,
          store_order3,
          safety_qty,
          --special_cases,
          --forecast_cases,
          --safety_cases,
          --over_cases,
          trading_date,
          direct_mu_qty1,
          direct_mu_qty2,
          direct_mu_qty3,
          direct_mu_qty4,
          direct_mu_qty5,
          direct_mu_qty6,
          direct_mu_qty7,
          last_updated_date,
          direct_delivery_ind)
  values
(         --CASE dwh_log.merge_counter(dwh_log.c_inserting)
          --WHEN 0 THEN 
          mer_lidso.sk1_location_no,
          --END,
          mer_lidso.sk1_item_no,
          mer_lidso.post_date,
          mer_lidso.boh_1_qty,
          mer_lidso.boh_1_ind,
          mer_lidso.boh_2_qty,
          mer_lidso.boh_3_qty,
          mer_lidso.store_order1,
          mer_lidso.store_order2,
          mer_lidso.store_order3,
          mer_lidso.safety_qty,
          --mer_lidso.special_cases,
          --mer_lidso.forecast_cases,
          --mer_lidso.safety_cases,
          --mer_lidso.over_cases,
          mer_lidso.trading_date,
          mer_lidso.direct_mu_qty1,
          mer_lidso.direct_mu_qty2,
          mer_lidso.direct_mu_qty3,
          mer_lidso.direct_mu_qty4,
          mer_lidso.direct_mu_qty5,
          mer_lidso.direct_mu_qty6,
          mer_lidso.direct_mu_qty7,
          mer_lidso.last_updated_date,
          mer_lidso.direct_delivery_ind);

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=SQL%ROWCOUNT;

--g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);
--g_recs_inserted:=dwh_log.get_merge_insert_count;
--g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||0;
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
end wh_prf_corp_748U;
