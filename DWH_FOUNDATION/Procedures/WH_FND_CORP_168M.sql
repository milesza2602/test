--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_168M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_168M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2015
--  Author:      Alastair de Wet
--  Purpose:     Create location_item dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_jda_st_plan_anlysis_dy_cpy
--               Output - FND_JDAFF_ST_PLAN_ANALYSIS_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_recs_duplicate     integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_jda_st_plan_anlysis_dy_hsp.sys_process_msg%type;
mer_mart             stg_jda_st_plan_anlysis_dy_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_169M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_jda_st_plan_anlysis_dy_cpy.location_no%type; 
g_item_no           stg_jda_st_plan_anlysis_dy_cpy.item_no%TYPE; 
g_trading_date      stg_jda_st_plan_anlysis_dy_cpy.trading_date%TYPE; 
g_post_date         stg_jda_st_plan_anlysis_dy_cpy.post_date%TYPE; 


   cursor stg_dup is
      select * from stg_jda_st_plan_anlysis_dy_cpy
      where (location_no, item_no, trading_date, post_date)
      in
      (select location_no, item_no, trading_date, post_date
      from stg_jda_st_plan_anlysis_dy_cpy 
      group by location_no, item_no, trading_date, post_date
      having count(*) > 1) 
      order by location_no,
              item_no, 
              trading_date, 
              post_date,
              sys_source_batch_id desc ,
              sys_source_sequence_no desc;
    

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

    l_text := 'LOAD OF FND_JDAFF_ST_PLAN_ANALYSIS_DY EX RMS STARTED AT '||
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
    
 
    select count(*)
    into   g_recs_read
    from   stg_jda_st_plan_anlysis_dy_cpy                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_trading_date   := '01/JAN/1900';
   g_post_date      := '01/JAN/1900';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no and
            dupp_record.trading_date  = g_trading_date and
            dupp_record.post_date     = g_post_date 
            then
            update stg_jda_st_plan_anlysis_dy_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no     := dupp_record.location_no; 
        g_item_no         := dupp_record.item_no;
        g_trading_date    := dupp_record.trading_date;
        g_post_date       := dupp_record.post_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


merge /*+ parallel (fnd_mart,6) */ 
    into FND_JDAFF_ST_PLAN_ANALYSIS_DY fnd_mart 
    using (
      select /*+ FULL(TMP) */ tmp.*
       from stg_jda_st_plan_anlysis_dy_cpy  tmp 
       join fnd_item di           on tmp.item_no = di.item_no 
       join fnd_location dl       on tmp.location_no = dl.location_no 
       where tmp.sys_process_code = 'N'

  
     ) mer_mart
  
on  (mer_mart.item_no         = fnd_mart.item_no
and  mer_mart.location_no     = fnd_mart.location_no
and  mer_mart.trading_date    = fnd_mart.trading_date
and  mer_mart.post_date       = fnd_mart.post_date
    )
when matched then
update
set       TOTAL_DEMAND_UNIT               = mer_mart.TOTAL_DEMAND_UNIT,
          INVENTORY_UNIT                  = mer_mart.INVENTORY_UNIT,
          PLANNED_ARRIVALS_UNIT           = mer_mart.PLANNED_ARRIVALS_UNIT,
          PLANNED_ARRIVALS_CASE           = mer_mart.PLANNED_ARRIVALS_CASE,
          REC_ARRIVAL_UNIT                = mer_mart.REC_ARRIVAL_UNIT,
          REC_ARRIVAL_CASE                = mer_mart.REC_ARRIVAL_CASE,
          IN_TRANSIT_UNIT                 = mer_mart.IN_TRANSIT_UNIT,
          IN_TRANSIT_CASE                 = mer_mart.IN_TRANSIT_CASE,
          CONSTRAINT_POH_UNIT             = mer_mart.CONSTRAINT_POH_UNIT,
          safety_stock_unit               = mer_mart.safety_stock_unit,
          CONSTRAINED_EXPIRED_STOCK       = mer_mart.CONSTRAINED_EXPIRED_STOCK,
          constr_store_cover_day          = mer_mart.constr_store_cover_day,
          last_updated_date               = g_date,
  -- SS Added 7 new columns 07/may/2015
          ALT_CONSTRAINT_UNUSED_SOH_UNIT  = mer_mart.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
          ALT_CONSTRAINT_POH_UNIT         = mer_mart.ALT_CONSTRAINT_POH_UNIT,
          CONSTRAINT_UNMET_DEMAND_UNIT    = mer_mart.CONSTRAINT_UNMET_DEMAND_UNIT,
          CONSTRAINT_UNUSED_SOH_UNIT      = mer_mart.CONSTRAINT_UNUSED_SOH_UNIT,
          EXPIRED_SOH_UNIT                = mer_mart.EXPIRED_SOH_UNIT,
          IGNORED_DEMAND_UNIT             = mer_mart.IGNORED_DEMAND_UNIT,
          projected_stock_available_unit  = mer_mart.projected_stock_available_unit
          
WHEN NOT MATCHED THEN
INSERT
(         ITEM_NO,
          LOCATION_NO,
          TRADING_DATE,
          POST_DATE,
          TOTAL_DEMAND_UNIT,
          INVENTORY_UNIT,
          PLANNED_ARRIVALS_UNIT,
          PLANNED_ARRIVALS_CASE,
          REC_ARRIVAL_UNIT,
          REC_ARRIVAL_CASE,
          IN_TRANSIT_UNIT,
          IN_TRANSIT_CASE,
          CONSTRAINT_POH_UNIT,
          safety_stock_unit,
          CONSTRAINED_EXPIRED_STOCK,
          constr_store_cover_day,
          LAST_UPDATED_DATE ,
          ALT_CONSTRAINT_UNUSED_SOH_UNIT,
          ALT_CONSTRAINT_POH_UNIT,
          CONSTRAINT_UNMET_DEMAND_UNIT,
          CONSTRAINT_UNUSED_SOH_UNIT,
          EXPIRED_SOH_UNIT,
          IGNORED_DEMAND_UNIT,
          projected_stock_available_unit
          )
  values
(         mer_mart.ITEM_NO,
          mer_mart.LOCATION_NO,
          mer_mart.TRADING_DATE,
          mer_mart.POST_DATE,
          mer_mart.TOTAL_DEMAND_UNIT,
          mer_mart.INVENTORY_UNIT,
          mer_mart.PLANNED_ARRIVALS_UNIT,
          mer_mart.PLANNED_ARRIVALS_CASE,
          mer_mart.REC_ARRIVAL_UNIT,
          mer_mart.REC_ARRIVAL_CASE,
          mer_mart.IN_TRANSIT_UNIT,
          mer_mart.IN_TRANSIT_CASE,
          mer_mart.CONSTRAINT_POH_UNIT,
          mer_mart.safety_stock_unit,
          mer_mart.CONSTRAINED_EXPIRED_STOCK,
          mer_mart.constr_store_cover_day,
          g_date ,
          mer_mart.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
          mer_mart.ALT_CONSTRAINT_POH_UNIT,
          mer_mart.CONSTRAINT_UNMET_DEMAND_UNIT,
          mer_mart.CONSTRAINT_UNUSED_SOH_UNIT,
          mer_mart.EXPIRED_SOH_UNIT,
          mer_mart.IGNORED_DEMAND_UNIT,
          mer_mart.projected_stock_available_unit
          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_jda_st_plan_anlysis_dy_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM, LOC',
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.TRADING_DATE,
                            TMP.POST_DATE,
                            TMP.TOTAL_DEMAND_UNIT,
                            TMP.INVENTORY_UNIT,
                            TMP.PLANNED_ARRIVALS_UNIT,
                            TMP.PLANNED_ARRIVALS_CASE,
                            TMP.REC_ARRIVAL_UNIT,
                            TMP.REC_ARRIVAL_CASE,
                            TMP.IN_TRANSIT_UNIT,
                            TMP.IN_TRANSIT_CASE,
                            TMP.CONSTRAINT_POH_UNIT,
                            TMP.SAFETY_STOCK_UNIT,
                            TMP.CONSTRAINED_EXPIRED_STOCK,
                            TMP.CONSTR_STORE_COVER_DAY,
                            TMP.LAST_UPDATED_DATE,
                            TMP.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
                            TMP.ALT_CONSTRAINT_POH_UNIT,
                            TMP.CONSTRAINT_UNMET_DEMAND_UNIT,
                            TMP.CONSTRAINT_UNUSED_SOH_UNIT,
                            TMP.EXPIRED_SOH_UNIT,
                            TMP.IGNORED_DEMAND_UNIT,
                            TMP.PROJECTED_STOCK_AVAILABLE_UNIT
                            
    from  stg_jda_st_plan_anlysis_dy_cpy  TMP 
     where not exists
          (select *
             from   fnd_item di
            where  tmp.item_no   = di.item_no )  
         or
         not exists
           (select *
              from   fnd_location dl
             where  tmp.location_no       = dl.location_no )
         
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
    l_text :=  'DUPLICATES REMOVED '||g_recs_duplicate;          
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
end wh_fnd_corp_168m;
