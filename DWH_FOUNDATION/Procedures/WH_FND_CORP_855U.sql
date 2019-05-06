--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_855U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_855U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        February 2016
--  Author:      Barry K
--  Purpose:     Create CUST_ORDER_CASES  ORDERS new measures dimention table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - STG_JDAFF_CUST_ORD
--               Output - FND_LOC_ITEM_DY_FF_CUST_ORD
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
g_hospital_text       DWH_FOUNDATION.STG_JDAFF_CUST_ORD_HSP.sys_process_msg%type;
stg_src               DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy%rowtype;
g_found               boolean;
g_valid               boolean;

g_date                date          := trunc(sysdate);

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_855U';
l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD CUST_ORDER_CASES + ISO ORDERS new measures';
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no         DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy.location_no%type;
g_item_no             DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy.item_no%TYPE;
g_post_date           DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy.post_date%TYPE;


cursor  stg_dup is
        select  *
        from    DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy
        where  (location_no, item_no, post_date) in (select  location_no,
                                                  item_no,
                                                  post_date
                                          from    DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy
                                          group by
                                                  location_no,
                                                  item_no,
                                                  post_date
                                          having  count(*) > 1
                                         )
        order by location_no, item_no, post_date, sys_source_batch_id desc, sys_source_sequence_no desc;


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

    l_text := 'LOAD OF CUST_ORDER_CASES + ISO ORDERS (new measure) Started at: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    from   DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy
    where  sys_process_code = 'N';

    --**************************************************************************************************
    -- De Duplication of the staging table to avoid Bulk insert failures
    --**************************************************************************************************
    l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_location_no    := 0;
    g_item_no        := 0;
    g_POST_DATE      := null;

    for dupp_record in stg_dup loop
        if  dupp_record.location_no = g_location_no
        and dupp_record.item_no     = g_item_no
        and dupp_record.POST_DATE   = g_POST_DATE then
            update DWH_FOUNDATION.STG_JDAFF_CUST_ORD_CPY stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id
            and    sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate := g_recs_duplicate  + 1;
        end if;

        g_location_no    := dupp_record.location_no;
        g_item_no        := dupp_record.item_no;
        g_POST_DATE      := dupp_record.POST_DATE;

    end loop;
    commit;

    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --**************************************************************************************************
    -- Bulk Merge controlling main program execution
    --**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ parallel (fnd_tgt,6) */ into DWH_FOUNDATION.FND_LOC_ITEM_DY_FF_CUST_ORD fnd_tgt
--    merge into DWH_FOUNDATION.FND_LOC_ITEM_DY_FF_CUST_ORD fnd_tgt
    using ( select *
            from   DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy
            where  sys_process_code = 'N'
          ) stg_src
    on    (stg_src.LOCATION_NO = fnd_tgt.LOCATION_NO and stg_src.ITEM_NO = fnd_tgt.ITEM_NO and stg_src.POST_DATE = fnd_tgt.POST_DATE)
    when matched then
      update
        set   fnd_tgt.EMERGENCY_ORDER_CASES   = stg_src.EMERGENCY_ORDER_CASES,
              fnd_tgt.IN_STORE_ORDER_CASES    = stg_src.IN_STORE_ORDER_CASES,
              fnd_tgt.ZERO_BOH_CASES          = stg_src.ZERO_BOH_CASES,
              fnd_tgt.SCANNED_ORDER_CASES     = stg_src.SCANNED_ORDER_CASES,
              fnd_tgt.CUST_ORDER_CASES        = stg_src.CUST_ORDER_CASES,
              fnd_tgt.source_data_status_code = stg_src.source_data_status_code,
              fnd_tgt.last_updated_date       = g_date
    WHEN NOT MATCHED THEN
      INSERT
             (LOCATION_NO,
              ITEM_NO,
              POST_DATE,
              EMERGENCY_ORDER_CASES,
              IN_STORE_ORDER_CASES,
              ZERO_BOH_CASES,
              SCANNED_ORDER_CASES,
              CUST_ORDER_CASES,
              SOURCE_DATA_STATUS_CODE,
              LAST_UPDATED_DATE
             )
      values
             (stg_src.LOCATION_NO,
              stg_src.ITEM_NO,
              stg_src.POST_DATE,
              stg_src.EMERGENCY_ORDER_CASES,
              stg_src.IN_STORE_ORDER_CASES,
              stg_src.ZERO_BOH_CASES,
              stg_src.SCANNED_ORDER_CASES,
              stg_src.CUST_ORDER_CASES,
              stg_src.SOURCE_DATA_STATUS_CODE,
              g_date
             );
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      commit;

      --**************************************************************************************************
      -- Write final log data
      --**************************************************************************************************
      l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      insert  /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.STG_JDAFF_CUST_ORD_hsp hsp
      select  /*+ FULL(TMP) */
              TMP.sys_source_batch_id,
              TMP.sys_source_sequence_no,
              sysdate,
             'Y',
             'DWH',
              TMP.sys_middleware_batch_id,
             'INVALID INDICATOR OR REFERENCIAL ERROR ON POSTDTE/LOC/ITEM',
              TMP.POST_DATE,
              TMP.LOCATION_NO,
              TMP.ITEM_NO,
              TMP.EMERGENCY_ORDER_CASES,
              TMP.IN_STORE_ORDER_CASES,
              TMP.ZERO_BOH_CASES,
              TMP.SCANNED_ORDER_CASES,
              TMP.CUST_ORDER_CASES,
              TMP.SOURCE_DATA_STATUS_CODE
      from    DWH_FOUNDATION.STG_JDAFF_CUST_ORD_cpy  TMP
      where   sys_process_code = 'N'
      and    (
              not exists (select *
                          from   fnd_item di
                          where  tmp.item_no = di.item_no
                         )
              or
              not exists (select *
                          from   fnd_location dl
                          where  tmp.location_no = dl.location_no
                         )
             );
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

end wh_fnd_corp_855u;
