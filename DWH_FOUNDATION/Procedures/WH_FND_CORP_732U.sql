--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_732U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_732U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Purchase Order fact table in the foundation layer
--               with input ex staging table from TRICEPS GRN.
--  Tables:      Input  - stg_triceps_boh_cpy
--               Output - FND_RTL_LOC_ITEM_DY_TRCPS_BOH
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  23 Feb 2009 - A Joshua : TD-946 - Change various column attributes/names
--                                  - Update ETL for this columns
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
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_triceps_boh_hsp.sys_process_msg%type;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_732U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PURCHASE_ORDER FACTS EX TRICEPS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_triceps_boh_cpy.location_no%type; 
g_item_no           stg_triceps_boh_cpy.item_no%TYPE; 
g_post_date             stg_triceps_boh_cpy.post_date%type;


   cursor stg_dup is
      select * from stg_triceps_boh_cpy
       where (post_date, location_no,item_no)
          in
      (select post_date, location_no,item_no
         from stg_triceps_boh_cpy 
        group by post_date, location_no, item_no
       having count(*) > 1
       ) 
      order by  post_date, location_no, item_no,
      sys_source_batch_id desc,sys_source_sequence_no desc;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure bulk_merge as
begin

  MERGE /*+ parallel(fnd_po, 4) */ INTO FND_RTL_LOC_ITEM_DY_TRCPS_BOH FND_PO USING 
  (
    select  /*+ parallel(stg, 4) full(stg) */ stg.*
       from stg_triceps_boh_cpy stg,
            fnd_item fi,
            fnd_location fl
            
       where stg.item_no          = fi.item_no
         and stg.location_no      = fl.location_no
         and stg.sys_process_code = 'N'
   ) mer_mart
   
   on (fnd_po.post_date     = mer_mart.post_date
  and  fnd_po.item_no       = mer_mart.item_no
  and  fnd_po.location_no   = mer_mart.location_no)
  
  when matched then
    update 
       set    stock_cases                     = mer_mart.stock_cases,
              stock_cost                      = mer_mart.stock_cost,
              shrink_cases                    = mer_mart.shrink_cases,
              gains_cases                     = mer_mart.gains_cases,
              received_cases                  = mer_mart.received_cases,
              dispatched_cases                = mer_mart.dispatched_cases,
              case_cost                       = mer_mart.case_cost,
              outstore_cases                  = mer_mart.outstore_cases,
              on_hold_cases                   = mer_mart.on_hold_cases,
              overship_cases                  = mer_mart.overship_cases,
              scratch_cases                   = mer_mart.scratch_cases,
              scratch_no_cases                = mer_mart.scratch_no_cases,
              cases_ex_outstore               = mer_mart.cases_ex_outstore,
              cases_to_outstore               = mer_mart.cases_to_outstore,
              unpicked_cases                  = mer_mart.unpicked_cases,
              on_order_cases                  = mer_mart.on_order_cases,
              pallet_qty                      = mer_mart.pallet_qty,
              pick_slot_cases                 = mer_mart.pick_slot_cases,
              case_avg_weight                 = mer_mart.case_avg_weight,
              returned_cases                  = mer_mart.returned_cases,
              shelf_life_expird               = mer_mart.shelf_life_expird,
              shelf_life_01_07                = mer_mart.shelf_life_01_07,
              shelf_life_08_14                = mer_mart.shelf_life_08_14,
              shelf_life_15_21                = mer_mart.shelf_life_15_21,
              shelf_life_22_28                = mer_mart.shelf_life_22_28,
              shelf_life_29_35                = mer_mart.shelf_life_29_35,
              shelf_life_36_49                = mer_mart.shelf_life_36_49,
              shelf_life_50_60                = mer_mart.shelf_life_50_60,
              shelf_life_61_90                = mer_mart.shelf_life_61_90,
              shelf_life_91_120               = mer_mart.shelf_life_91_120,
              shelf_life_120_up               = mer_mart.shelf_life_120_up,
              source_data_status_code         = mer_mart.source_data_status_code,
              last_updated_date               = g_date

  when not matched then
    insert (
              POST_DATE,
              ITEM_NO,
              LOCATION_NO,
              STOCK_CASES,
              STOCK_COST,
              SHRINK_CASES,
              GAINS_CASES,
              RECEIVED_CASES,
              DISPATCHED_CASES,
              CASE_COST,
              OUTSTORE_CASES,
              ON_HOLD_CASES,
              OVERSHIP_CASES,
              SCRATCH_CASES,
              SCRATCH_NO_CASES,
              CASES_EX_OUTSTORE,
              CASES_TO_OUTSTORE,
              UNPICKED_CASES,
              ON_ORDER_CASES,
              PALLET_QTY,
              PICK_SLOT_CASES,
              CASE_AVG_WEIGHT,
              RETURNED_CASES,
              SHELF_LIFE_EXPIRD,
              SHELF_LIFE_01_07,
              SHELF_LIFE_08_14,
              SHELF_LIFE_15_21,
              SHELF_LIFE_22_28,
              SHELF_LIFE_29_35,
              SHELF_LIFE_36_49,
              SHELF_LIFE_50_60,
              SHELF_LIFE_61_90,
              SHELF_LIFE_91_120,
              SHELF_LIFE_120_UP,
              SOURCE_DATA_STATUS_CODE,
              LAST_UPDATED_DATE
           )
    values (
              mer_mart.POST_DATE,
              mer_mart.ITEM_NO,
              mer_mart.LOCATION_NO,
              mer_mart.STOCK_CASES,
              mer_mart.STOCK_COST,
              mer_mart.SHRINK_CASES,
              mer_mart.GAINS_CASES,
              mer_mart.RECEIVED_CASES,
              mer_mart.DISPATCHED_CASES,
              mer_mart.CASE_COST,
              mer_mart.OUTSTORE_CASES,
              mer_mart.ON_HOLD_CASES,
              mer_mart.OVERSHIP_CASES,
              mer_mart.SCRATCH_CASES,
              mer_mart.SCRATCH_NO_CASES,
              mer_mart.CASES_EX_OUTSTORE,
              mer_mart.CASES_TO_OUTSTORE,
              mer_mart.UNPICKED_CASES,
              mer_mart.ON_ORDER_CASES,
              mer_mart.PALLET_QTY,
              mer_mart.PICK_SLOT_CASES,
              mer_mart.CASE_AVG_WEIGHT,
              mer_mart.RETURNED_CASES,
              mer_mart.SHELF_LIFE_EXPIRD,
              mer_mart.SHELF_LIFE_01_07,
              mer_mart.SHELF_LIFE_08_14,
              mer_mart.SHELF_LIFE_15_21,
              mer_mart.SHELF_LIFE_22_28,
              mer_mart.SHELF_LIFE_29_35,
              mer_mart.SHELF_LIFE_36_49,
              mer_mart.SHELF_LIFE_50_60,
              mer_mart.SHELF_LIFE_61_90,
              mer_mart.SHELF_LIFE_91_120,
              mer_mart.SHELF_LIFE_120_UP,
              mer_mart.SOURCE_DATA_STATUS_CODE,
              g_date
            )
       ;


  --g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  0;

  commit;
 
  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end bulk_merge;

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

    l_text := 'LOAD OF FND_RTL_LOC_ITEM_DY_TRCPS_BOH EX TRICEPS STARTED AT '||
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
    
  select count(*)
    into   g_recs_read
    from   stg_triceps_boh_cpy                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_post_date      := '01/JAN/1999';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.post_date     = g_post_date and
            dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            update stg_triceps_boh_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
        g_post_date      := dupp_record.post_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
    
    bulk_merge;
    
--**************************************************************************************************    
 
    l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_triceps_boh_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,LOC,SUPPLIER',
                            TMP.POST_DATE,
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.STOCK_CASES,
                            TMP.STOCK_COST,
                            TMP.SHRINK_CASES,
                            TMP.GAINS_CASES,
                            TMP.RECEIVED_CASES,
                            TMP.DISPATCHED_CASES,
                            TMP.CASE_COST,
                            TMP.OUTSTORE_CASES,
                            TMP.ON_HOLD_CASES,
                            TMP.OVERSHIP_CASES,
                            TMP.SCRATCH_CASES,
                            TMP.SCRATCH_NO_CASES,
                            TMP.CASES_EX_OUTSTORE,
                            TMP.CASES_TO_OUTSTORE,
                            TMP.UNPICKED_CASES,
                            TMP.ON_ORDER_CASES,
                            TMP.PALLET_QTY,
                            TMP.PICK_SLOT_CASES,
                            TMP.CASE_AVG_WEIGHT,
                            TMP.RETURNED_CASES,
                            TMP.SHELF_LIFE_EXPIRD,
                            TMP.SHELF_LIFE_01_07,
                            TMP.SHELF_LIFE_08_14,
                            TMP.SHELF_LIFE_15_21,
                            TMP.SHELF_LIFE_22_28,
                            TMP.SHELF_LIFE_29_35,
                            TMP.SHELF_LIFE_36_49,
                            TMP.SHELF_LIFE_50_60,
                            TMP.SHELF_LIFE_61_90,
                            TMP.SHELF_LIFE_91_120,
                            TMP.SHELF_LIFE_120_UP,
                            TMP.SOURCE_DATA_STATUS_CODE
                
    from  stg_triceps_boh_cpy  TMP 
   where not exists
      (select *
           from   fnd_item di
           where  tmp.item_no     = di.item_no )  
         or
         not exists
      (select *
           from   fnd_location dl
           where  tmp.location_no = dl.location_no )
     and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
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
end wh_fnd_corp_732u;
