--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_150U_WENDY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_150U_WENDY" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create item vat rate dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_item_vat_rate_cpy
--               Output - fnd_item_vat_rate
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  25 Sep 2015 : Chg 38403 - rewrote module to be a bulk merge to optimise AND added logic for a new 
--                            field added to the table - active_ind - which is used to indicate the 
--                            most current item / vate region record.
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
g_recs_active_upd    integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_item_vat_rate_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_ITEM_VAT_RATE%rowtype;
g_rec_in             stg_rms_item_vat_rate_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_150U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM VAT RATE MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_vat_region_no       stg_rms_item_vat_rate_cpy.vat_region_no%type; 
g_item_no             stg_rms_item_vat_rate_cpy.item_no%TYPE; 
g_vat_code            stg_rms_item_vat_rate_cpy.vat_code%type;
g_active_from_date    stg_rms_item_vat_rate_cpy.active_from_date%type;
g_mod_cnt             integer;


cursor stg_dup is
 select * from stg_rms_item_vat_rate_cpy
  where (ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE)
      in
      (select ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE
         from stg_rms_item_vat_rate_cpy 
     group by ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE
      having count(*) > 1) 
      order by ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE, sys_source_batch_id desc ,sys_source_sequence_no desc;
      
      
cursor fnd_upd is
    with aa as (
select item_no, vat_region_no, vat_code, max(active_from_date) max_active_from_date
from DWH_FOUNDATION.FND_ITEM_VAT_RATE 
where active_from_date < g_date
group by item_no, vat_region_no, vat_code
) 
select b.item_no, b.vat_region_no, b.vat_code, b.active_from_date 
  from DWH_FOUNDATION.FND_ITEM_VAT_RATE b , aa
              where aa.item_no = b.item_no
                and aa.vat_region_no = b.vat_region_no
                and aa.vat_code = b.vat_code
                and aa.max_active_from_date = b.active_from_date
;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_ITEM_VAT_RATE EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   merge /*+ parallel (fnd_mart,6) */ 
    into DWH_FOUNDATION.FND_ITEM_VAT_RATE fnd_mart 
    using (
         with aa as ( 
            select /*+ materialize full(b) parallel(b,8) */
              item_no, vat_region_no,  max(active_from_date) max_active_from_date
            from DWH_FOUNDATION.FND_ITEM_VAT_RATE B
            where active_from_date <= g_date
            group by item_no, vat_region_no
            ) ,
         AB AS (   select /*+ MATERIALIZE FULL(B) PARALLEL(B,4) */ 
              b.item_no, b.vat_region_no,  b.active_from_date 
              from DWH_FOUNDATION.FND_ITEM_VAT_RATE b , aa
                          where aa.item_no = b.item_no
                            and aa.vat_region_no = b.vat_region_no
                           and aa.max_active_from_date = b.active_from_date)
                           SELECT * FROM AB
          ) mer_mart
  
    on  (mer_mart.item_no           = fnd_mart.item_no
    and  mer_mart.vat_region_no     = fnd_mart.vat_region_no
  --  and  mer_mart.vat_code          = fnd_mart.vat_code
    and  mer_mart.active_from_date  = fnd_mart.active_from_date
        )
    when matched then
    update
    set       ACTIVE_IND                = 1
    ;   
    
    g_recs_active_upd := SQL%ROWCOUNT;
    
    commit;
    
    l_text := 'MARKING ACTIVE RECORDS END - '|| g_recs_active_upd || ' - records marked as active';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;          
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
       
end wh_fnd_corp_150u_wendy;
