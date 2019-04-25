--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_211M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_211M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2014
--  Author:      Quentin Smit
--  Purpose:     Create ROQ/Cust orders fact table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - rtl_loc_item_dy_st_dir_ord
--                        DIM_ITEM
--               Output - fnd_rtl_loc_item_dy_ff_ord
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
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_rec_out            fnd_rtl_loc_item_dy_ff_ord%rowtype;
g_found              boolean;
g_insert_rec         boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_211M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ROQ/CUST ORDERS FACTS EX RTL PERF TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_from_date          rtl_loc_item_dy_st_dir_ord.post_date%type;
g_to_date            rtl_loc_item_dy_st_dir_ord.post_date%type;



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

    l_text := 'LOAD OF fnd_rtl_loc_item_dy_ff_ord EX JDA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date:= '21/MAR/15';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    SELECT MIN(POST_DATE), MAX(POST_DATE)
      INTO G_FROM_DATE, G_TO_DATE
      FROM rtl_loc_item_dy_st_dir_ord 
     WHERE LAST_UPDATED_DATE = G_DATE;
     
    l_text := 'DATES RANGE BEING PROCESSED :- '||G_FROM_DATE || ' TO ' || G_TO_DATE;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

MERGE /*+ full(fnd_lidfford) PARALLEL(fnd_lidfford,6) */ INTO dwh_foundation.fnd_rtl_loc_item_dy_ff_ord fnd_lidfford USING
     (
      select /*+ PARALLEL(a,6) full(a) full(b) full(c)*/  b.location_no, c.item_no, a.post_date, sum(a.store_order1) roq_qty, 
               0 cust_order_cases,    --sum(e.amended_po_cases) cust_order_cases, 
               c.var_weight_ind, a.last_updated_date
        from rtl_loc_item_dy_st_dir_ord a, dim_location b, dim_item c, fnd_jdaff_dept_rollout d
       where a.sk1_location_no = b.sk1_location_no
         and a.sk1_item_no     = c.sk1_item_no
         --and a.last_updated_date = g_date
         and a.post_date between g_from_date and g_to_date
         and c.department_no = d.department_no
         and d.department_live_ind = 'Y'
        group by b.location_no, c.item_no, a.post_date, c.var_weight_ind, a.last_updated_date

         ) mer_lidso
         
ON  (mer_lidso.item_no     = fnd_lidfford.item_no
 and mer_lidso.location_no = fnd_lidfford.location_no
 and mer_lidso.post_date   = fnd_lidfford.post_date

and fnd_lidfford.post_date between g_from_date and g_to_date)

WHEN MATCHED THEN
UPDATE
SET       roq_qty                       = mer_lidso.roq_qty,
          cust_order_cases              = mer_lidso.cust_order_cases,
          weigh_ind                     = mer_lidso.var_weight_ind,
          last_updated_date             = mer_lidso.last_updated_date
WHEN NOT MATCHED THEN
INSERT
(         location_no,
          item_no,
          post_date,
          roq_qty,
          cust_order_cases,
          weigh_ind,
          last_updated_date)
  values
(         --CASE dwh_log.merge_counter(dwh_log.c_inserting)
          --WHEN 0 THEN mer_lidso.location_no
          --END,
          mer_lidso.location_no,
          mer_lidso.item_no,
          mer_lidso.post_date,
          mer_lidso.roq_qty,            -- roq_qty
          mer_lidso.cust_order_cases,   -- cust order cases
          mer_lidso.var_weight_ind,     -- weigh ind
          mer_lidso.last_updated_date);

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=SQL%ROWCOUNT;
g_recs_updated:=SQL%ROWCOUNT;

--g_recs_inserted:=dwh_log.get_merge_insert_count;
--g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);

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
end wh_fnd_corp_211m;
