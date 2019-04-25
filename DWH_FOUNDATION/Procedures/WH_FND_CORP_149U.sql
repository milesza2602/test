--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_149U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_149U" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as

--**************************************************************************************************
--  Date:        April 2013
--  Author:      Quentin Smit
--  Purpose:     Create Store Order Fact table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_jdaff_st_ord_cpy
--               Output - dwh_foundation.fnd_rtl_loc_item_dy_ff_st_ord
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
--
--  Sept 2016   - Q.Smit - change to bulk merge, output table made interval partitioned
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
g_hospital_text      stg_rms_location_item_hsp.sys_process_msg%type;
mer_mart             stg_rms_location_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_149U'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STORE ORDERS EX JDAFF';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


g_location_no       stg_jdaff_st_ord_cpy.location_no%type; 
g_item_no           stg_jdaff_st_ord_cpy.item_no%TYPE; 
g_post_date         stg_jdaff_st_ord_cpy.post_date%TYPE;
g_from_loc_no       integer;
g_to_loc_no         integer;

g_from_date         date := g_date - 729;
g_to_date           date := g_date + 8;


  cursor stg_dup is
      select * from dwh_foundation.stg_jdaff_st_ord_cpy
      where (post_date, location_no, item_no)
      in
      (select post_date, location_no, item_no
         from stg_jdaff_st_ord_cpy 
        group by post_date, location_no, item_no
       having count(*) > 1) 
        order by location_no, item_no, sys_source_batch_id desc ,sys_source_sequence_no desc;

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

    l_text := 'LOAD OF fnd_rtl_loc_item_dy_ff_st_ord EX OM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    if p_from_loc_no = 0 then
       g_from_loc_no := 0;
       g_to_loc_no   := 99999;
    end if;
    
    if p_from_loc_no  = 351 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
       l_text := 'PROCESSES NO DATA';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
    
    if p_from_loc_no  = 491 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
       l_text := 'PROCESSES NO DATA';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
    
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

if g_to_loc_no   > 0 then 

       select count(*)
         into  g_recs_read
         from  stg_jdaff_st_ord_cpy                                                                                                                                                                                               
        where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_post_date      := '01/JAN/1900';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.post_date     = g_post_date and
            dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            
            update stg_jdaff_st_ord_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no     := dupp_record.location_no; 
        g_item_no         := dupp_record.item_no;
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


    merge /*+ parallel(fnd_mart,6) */ 
        into fnd_rtl_loc_item_dy_ff_st_ord fnd_mart 
        using (
        select /*+ FULL(TMP) */ tmp.*
        from stg_jdaff_st_ord_cpy  tmp 
        join fnd_item di           on tmp.item_no = di.item_no 
        join fnd_location dl       on tmp.location_no = dl.location_no 
       where tmp.sys_process_code = 'N'
       
         --and (post_date > (g_date - 729) and  post_date < (g_date + 8)
         -- do not process if post_date < g_date - 730 or post_date > today + 7 days
       
         ) mer_mart
      
    on  (mer_mart.post_date       = fnd_mart.post_date
    and  mer_mart.item_no         = fnd_mart.item_no
    and  mer_mart.location_no     = fnd_mart.location_no
        )
    when matched then
    update
    set     boh_qty1                        = mer_mart.boh_qty1,
            boh_1_ind                       = mer_mart.boh_1_ind,
            boh_qty2                        = mer_mart.boh_qty2,
            boh_qty3                        = mer_mart.boh_qty3,
            store_order1                    = mer_mart.store_order1,
            store_order2                    = mer_mart.store_order2,
            store_order3                    = mer_mart.store_order3,
            safety_qty                      = mer_mart.safety_qty,
            special_cases                   = mer_mart.special_cases,
            forecast_cases                  = mer_mart.forecast_cases,
            safety_cases                    = mer_mart.safety_cases,
            over_cases                      = mer_mart.over_cases,
            trading_date                    = mer_mart.trading_date,
            source_data_status_code         = mer_mart.source_data_status_code,
            last_updated_date               = g_date
                  
    WHEN NOT MATCHED THEN
    INSERT
    (         POST_DATE,
              LOCATION_NO,
              ITEM_NO,
              BOH_QTY1,
              BOH_1_IND,
              BOH_QTY2,
              BOH_QTY3,
              STORE_ORDER1,
              STORE_ORDER2,
              STORE_ORDER3,
              SAFETY_QTY,
              SPECIAL_CASES,
              FORECAST_CASES,
              SAFETY_CASES,
              OVER_CASES,
              TRADING_DATE,
              SOURCE_DATA_STATUS_CODE,
              LAST_UPDATED_DATE
              )
      values
    (         MER_MART.POST_DATE,
              MER_MART.LOCATION_NO,
              MER_MART.ITEM_NO,
              MER_MART.BOH_QTY1,
              MER_MART.BOH_1_IND,
              MER_MART.BOH_QTY2,
              MER_MART.BOH_QTY3,
              MER_MART.STORE_ORDER1,
              MER_MART.STORE_ORDER2,
              MER_MART.STORE_ORDER3,
              MER_MART.SAFETY_QTY,
              MER_MART.SPECIAL_CASES,
              MER_MART.FORECAST_CASES,
              MER_MART.SAFETY_CASES,
              MER_MART.OVER_CASES,
              MER_MART.TRADING_DATE,
              MER_MART.SOURCE_DATA_STATUS_CODE,
              g_date
              )  
      ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;
  
--**************************************************************************************************
-- Do hospital checks
--**************************************************************************************************
   
   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.stg_jdaff_st_ord_HSP hsp
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON POST_DATE, ITEM, LOC',
                            TMP.POST_DATE,
                            TMP.LOCATION_NO,
                            TMP.ITEM_NO,
                            TMP.BOH_QTY1,
                            TMP.BOH_QTY2,
                            TMP.BOH_QTY3,
                            TMP.STORE_ORDER1,
                            TMP.STORE_ORDER2,
                            TMP.STORE_ORDER3,
                            TMP.SAFETY_QTY,
                            TMP.SPECIAL_CASES,
                            TMP.FORECAST_CASES,
                            TMP.SAFETY_CASES,
                            TMP.OVER_CASES,
                            TMP.TRADING_DATE,
                            TMP.SOURCE_DATA_STATUS_CODE,
                            TMP.LAST_UPDATED_DATE,
                            TMP.BOH_1_IND
                            
    from  stg_jdaff_st_ord_cpy  TMP 
    where ( 
 --         (TMP.post_date < (g_date - 730) or  TMP.post_date > (g_date + 7))
 --        or
         not exists
          (select *
           from   fnd_item di
           where  tmp.item_no   = di.item_no )  
         or
         not exists
           (select *
           from   fnd_location dl
           where  tmp.location_no  = dl.location_no )
          )  
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

END IF; 

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
       
End Wh_Fnd_Corp_149u;
