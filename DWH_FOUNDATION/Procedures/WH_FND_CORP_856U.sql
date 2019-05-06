--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_856U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_856U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Aug 2016
--  Author:      Barry K
--  Purpose:     Staging interface for Supplier Plan Analysis data in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - STG_JDAFF_SUP_PLN_ANALYSIS
--               Output - FND_SUP_PLAN_ANALYSIS_DY
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

g_forall_limit        integer       :=  10000;
g_recs_read           integer       :=  0;
g_recs_updated        integer       :=  0;
g_recs_inserted       integer       :=  0;
g_recs_hospital       integer       :=  0;
g_recs_duplicate      integer       :=  0;
g_error_count         number        :=  0;
g_error_index         number        :=  0;
g_hospital            char(1)       := 'N';
g_hospital_text       DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_HSP.sys_process_msg%type;
stg_src               DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY%rowtype;
g_found               boolean;
g_valid               boolean;

g_date                date          := trunc(sysdate);

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_856U';
l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD Supplier Plan Analysis measure data';
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_SUPPLIER_NO         DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY.SUPPLIER_NO%type; 
g_item_no             DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY.item_no%TYPE;
g_post_date           DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY.post_date%TYPE; 
g_TRADING_DATE        DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY.TRADING_DATE%TYPE; 

-- cursor for de-duplication ...
cursor  stg_dup is
        select  * 
        from    DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY
        where  (item_no, SUPPLIER_NO, TRUNC(TRADING_DATE)
        --, post_date
        ) in (select item_no,
                                                                          SUPPLIER_NO,
                                                                          TRUNC(TRADING_DATE)
                                                                          --,
                                                                        --  post_date
                                                                   from   DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY 
                                                                   group by 
                                                                          item_no,
                                                                          SUPPLIER_NO,
                                                                          TRUNC(TRADING_DATE)
                                                                          --,
                                                                        --  post_date
                                                                   having  count(*) > 1
                                         ) 
        order by item_no, SUPPLIER_NO, TRUNC(TRADING_DATE)
        , 
        post_date, 
        sys_source_batch_id desc, sys_source_sequence_no desc;
    
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

    l_text := 'LOAD Supplier Plan Analysis measure data: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    --g_date := g_date -1;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    select count(*)
    into   g_recs_read
    from   DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY                                                                                                                                                                                              
    where  sys_process_code = 'N';
    
    --**************************************************************************************************
    -- De Duplication of the staging table to avoid Bulk insert failures
    --************************************************************************************************** 
    l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    g_item_no        := 0;
    g_SUPPLIER_NO    := 0; 
    g_TRADING_DATE   := null;
    g_POST_DATE      := null;
    
    for dupp_record in stg_dup loop 
        if  dupp_record.ITEM_NO      = g_ITEM_NO
        and dupp_record.SUPPLIER_NO  = g_SUPPLIER_NO 
        and TRUNC(dupp_record.TRADING_DATE) = g_TRADING_DATE
     --   and dupp_record.POST_DATE    = g_POST_DATE 
        then
            update DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id
            and    sys_source_sequence_no = dupp_record.sys_source_sequence_no;
              
            g_recs_duplicate := g_recs_duplicate  + 1;       
        end if;           
     
        g_item_no        := dupp_record.item_no;
        g_SUPPLIER_NO    := dupp_record.SUPPLIER_NO; 
        g_TRADING_DATE   := TRUNC(dupp_record.TRADING_DATE);
     --   g_POST_DATE      := dupp_record.POST_DATE;
      
    end loop;   
    commit;
      
    l_text := 'DEDUP ENDED - RECS='||g_recs_duplicate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --**************************************************************************************************
    -- Bulk Merge controlling main program execution
    --**************************************************************************************************
    l_text := 'MERGE STARTING  ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ parallel (fnd_tgt,4) */ into DWH_FOUNDATION.FND_SUP_PLAN_ANALYSIS_DY fnd_tgt
    using ( select /*+ parallel (stg,4) parallel (di,4) full(stg) full(di) */ 
                   stg.ITEM_NO,
                   stg.SUPPLIER_NO,
                   TRUNC(stg.TRADING_DATE) TRADING_DATE,
                   TRUNC(stg.POST_DATE) POST_DATE,
                   stg.TOTAL_DEMAND_UNITS,
                   stg.IGNORE_DEMAND_UNITS,
                   stg.PLANNED_ORDERS_UNITS,
                   stg.PLAN_SHIP_UNITS,
                   stg.FIRM_PLAN_SHIP_UNITS,
                   stg.REC_SHIP_UNITS
                   --,
             --      stg.LAST_UPDATED_DATE
            from   DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_CPY stg   
            join   fnd_item     di on (stg.item_no = di.item_no)
            join   fnd_supplier ds on (stg.SUPPLIER_NO = ds.SUPPLIER_NO) 
            join   DIM_CALENDAR dC1 on (TRUNC(stg.TRADING_DATE) = dC1.CALENDAR_DATE) 
            join   DIM_CALENDAR dC2 on (TRUNC(stg.POST_DATE) = dC2.CALENDAR_DATE) 
            where  sys_process_code = 'N' 
          ) stg_src
    on    (      stg_src.ITEM_NO = fnd_tgt.ITEM_NO 
             and stg_src.SUPPLIER_NO = fnd_tgt.SUPPLIER_NO 
             and TRUNC(stg_src.TRADING_DATE) = TRUNC(fnd_tgt.TRADING_DATE) 
        --     and stg_src.POST_DATE = fnd_tgt.POST_DATE
             )  
    when matched then
      update
        set   fnd_tgt.TOTAL_DEMAND_UNITS	 = stg_src.TOTAL_DEMAND_UNITS,	
              fnd_tgt.IGNORE_DEMAND_UNITS	 = stg_src.IGNORE_DEMAND_UNITS,	
              fnd_tgt.PLANNED_ORDERS_UNITS = stg_src.PLANNED_ORDERS_UNITS,	
              fnd_tgt.PLAN_SHIP_UNITS	     = stg_src.PLAN_SHIP_UNITS,	
              fnd_tgt.FIRM_PLAN_SHIP_UNITS = stg_src.FIRM_PLAN_SHIP_UNITS,	
              fnd_tgt.REC_SHIP_UNITS	     = stg_src.REC_SHIP_UNITS,
              FND_TGT.POST_DATE     = STG_SRC.POST_DATE,
              fnd_tgt.last_updated_date    = g_date
    WHEN NOT MATCHED THEN
      INSERT
             (ITEM_NO,
              SUPPLIER_NO,
              TRADING_DATE,
              POST_DATE,
              TOTAL_DEMAND_UNITS,
              IGNORE_DEMAND_UNITS,
              PLANNED_ORDERS_UNITS,
              PLAN_SHIP_UNITS,
              FIRM_PLAN_SHIP_UNITS,
              REC_SHIP_UNITS,
              LAST_UPDATED_DATE
             )
      values
             (stg_src.ITEM_NO,
              stg_src.SUPPLIER_NO,
              stg_src.TRADING_DATE,
              stg_src.POST_DATE,
              stg_src.TOTAL_DEMAND_UNITS,
              stg_src.IGNORE_DEMAND_UNITS,
              stg_src.PLANNED_ORDERS_UNITS,
              stg_src.PLAN_SHIP_UNITS,
              stg_src.FIRM_PLAN_SHIP_UNITS,
              stg_src.REC_SHIP_UNITS,
              g_date
             );
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;     
      commit;
      
    l_text := 'MERGE ENDED - RECS='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

      --**************************************************************************************************
      -- Write final log data
      --**************************************************************************************************
      l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS  ';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      insert  /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_hsp hsp 
      select  /*+ FULL(TMP) */  
              TMP.sys_source_batch_id,
              TMP.sys_source_sequence_no,
              sysdate,
             'Y',
             'DWH',
              TMP.sys_middleware_batch_id,
             'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM/SUPPLIER/TRADEDTE/POSTDTE',
             
              TMP.ITEM_NO,
              TMP.SUPPLIER_NO,
              TMP.TRADING_DATE,
              TMP.POST_DATE,
              TMP.TOTAL_DEMAND_UNITS,
              TMP.IGNORE_DEMAND_UNITS,
              TMP.PLANNED_ORDERS_UNITS,
              TMP.PLAN_SHIP_UNITS,
              TMP.FIRM_PLAN_SHIP_UNITS,
              TMP.REC_SHIP_UNITS,
              TMP.LAST_UPDATED_DATE
      from    DWH_FOUNDATION.STG_JDAFF_SUP_PLN_ANALYSIS_cpy  TMP  
      where   sys_process_code = 'N'  
      and    (
              not exists (select *
                          from   fnd_item di
                          where  tmp.item_no = di.item_no 
                         )  
              or      
              not exists (select *
                          from   fnd_supplier ds
                          where  tmp.SUPPLIER_NO = ds.SUPPLIER_NO
                         )
              or      
              not exists (select *
                          from   DIM_CALENDAR dC1
                          where  TRUNC(tmp.TRADING_DATE) = dC1.CALENDAR_DATE
                         )
--              or      
--              not exists (select *
--                          from   DIM_CALENDAR dC2
--                          where  TRUNC(tmp.POST_DATE) = dC2.CALENDAR_DATE
--                         )
             );         
    g_recs_hospital := g_recs_hospital + sql%rowcount; 
    commit;
         
   l_text := 'HOSPITALISATION CHECKS COMPLETE - RECS='||g_recs_hospital;
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
       
end wh_fnd_corp_856u;
