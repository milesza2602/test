--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_460U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_460U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID dense sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_sale
--               Output - RTL_CTRY_PROD_DY_FACTOR
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  13 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--
--  Sept 2017   - Rewrote as a single merge to improve performance - Q. Smit
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_fnd_sale             number(14,2)        :=  0;
g_prf_sale             number(14,2)        :=  0;


g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          number       :=  0;
g_recs_inserted      number       :=  0;
g_recs_updated       number       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            RTL_CTRY_PROD_DY_FACTOR%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_460U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE COUNTRY FACTOR DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    
    g_forall_limit := 10000;
    
--    l_text := 'ARRAY LIMIT - '||g_forall_limit;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_CTRY_PROD_DY_FACTOR EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';
    
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '11/FEB/18';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'MERGE STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE /*+ parallel(rtl_ctryfctr,4) */ INTO DWH_PERFORMANCE.RTL_CTRY_PROD_DY_FACTOR rtl_ctryfctr
    USING
    (
    SELECT DI.SK1_BUSINESS_UNIT_NO, 
       DC.SK1_COUNTRY_CODE, 
       DF.COUNTRY_CODE,
       DF.EFFECTIVE_DATE,
       DI.SK1_GROUP_NO,
       DI.SK1_DEPARTMENT_NO,
       DI.ITEM_PARENT_NO,
       DI.SK1_ITEM_NO,
       DF.PRICING_FACTOR,
       DF.PRICING_FACTOR_STATUS,
       DZ.SK1_ZONE_GROUP_ZONE_NO SK1_ZONE_NO

      FROM DWH_FOUNDATION.FND_CTRY_PROD_DY_FACTOR DF,
           DIM_ITEM DI,
           DIM_COUNTRY DC,
           DIM_ZONE DZ
     
     
     WHERE DF.BUSINESS_UNIT_NO = DI.BUSINESS_UNIT_NO
       AND DF.COUNTRY_CODE     = DC.COUNTRY_CODE
       AND DF.ITEM_NO          = DI.ITEM_NO
       AND DF.ZONE_NO          = DZ.ZONE_NO
       AND DF.LAST_UPDATED_DATE = G_DATE

    ) mer_ctryfctr
    
    ON
    (mer_ctryfctr.SK1_BUSINESS_UNIT_NO = rtl_ctryfctr.SK1_BUSINESS_UNIT_NO
    and mer_ctryfctr.SK1_COUNTRY_CODE  = rtl_ctryfctr.SK1_COUNTRY_CODE
    and mer_ctryfctr.EFFECTIVE_DATE    = rtl_ctryfctr.EFFECTIVE_DATE)
    
    WHEN MATCHED
    THEN
    UPDATE
    SET   SK1_GROUP_NO          = mer_ctryfctr.SK1_GROUP_NO,
          SK1_DEPARTMENT_NO     = mer_ctryfctr.SK1_DEPARTMENT_NO,
          ITEM_PARENT_NO        = mer_ctryfctr.ITEM_PARENT_NO,
          SK1_ITEM_NO           = mer_ctryfctr.SK1_ITEM_NO,
          PRICING_FACTOR        = mer_ctryfctr.PRICING_FACTOR,
          PRICING_FACTOR_STATUS = mer_ctryfctr.PRICING_FACTOR_STATUS,
          SK1_ZONE_NO           = mer_ctryfctr.SK1_ZONE_NO,
          LAST_UPDATED_DATE     = g_date
    
    WHEN NOT MATCHED
    THEN
    INSERT
    (
                  SK1_BUSINESS_UNIT_NO,
                  SK1_COUNTRY_CODE,
                  EFFECTIVE_DATE,
                  SK1_GROUP_NO,
                  SK1_DEPARTMENT_NO,
                  ITEM_PARENT_NO,
                  SK1_ITEM_NO,
                  PRICING_FACTOR,
                  PRICING_FACTOR_STATUS,
                  SK1_ZONE_NO,
                  LAST_UPDATED_DATE
    )
    VALUES
    (
                  mer_ctryfctr.SK1_BUSINESS_UNIT_NO,
                  mer_ctryfctr.SK1_COUNTRY_CODE,
                  mer_ctryfctr.EFFECTIVE_DATE,
                  mer_ctryfctr.SK1_GROUP_NO,
                  mer_ctryfctr.SK1_DEPARTMENT_NO,
                  mer_ctryfctr.ITEM_PARENT_NO,
                  mer_ctryfctr.SK1_ITEM_NO,
                  mer_ctryfctr.PRICING_FACTOR,
                  mer_ctryfctr.PRICING_FACTOR_STATUS,
                  mer_ctryfctr.SK1_ZONE_NO,
                  g_date
    );

   g_recs_inserted := g_recs_inserted + sql%rowcount;
   g_recs_updated  := g_recs_updated  + sql%rowcount;
   
    commit;
    
    
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
        dwh_log.record_error(l_module_name,sqlcode,l_message);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
        rollback;
        p_success := false;
        raise;
    
      when others then
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
        dwh_log.record_error(l_module_name,sqlcode,l_message);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

        rollback;
        p_success := false;
        raise;


end WH_PRF_CORP_460U;
