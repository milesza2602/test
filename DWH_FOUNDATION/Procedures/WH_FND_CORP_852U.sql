--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_852U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_852U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        April 2016
--  Author:      Barry K
--  Purpose:     Create Triceps Allocation table in the foundation layer
--               with input ex staging table from Triceps (rewrite into merge format)
--  Tables:      Input  - stg_triceps_alloc_day
--               Output - fnd_triceps_alloc_day
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

g_forall_limit        integer       :=  10000;
g_recs_read           integer       :=  0;
g_recs_updated        integer       :=  0;
g_recs_inserted       integer       :=  0;
g_recs_hospital       integer       :=  0;
g_recs_duplicate      integer       :=  0;
g_error_count         number        :=  0;
g_error_index         number        :=  0;
g_hospital            char(1)       := 'N';
g_hospital_text       DWH_FOUNDATION.stg_triceps_alloc_day_hsp.sys_process_msg%type;
stg_src               DWH_FOUNDATION.stg_triceps_alloc_day_cpy%rowtype;
g_found               boolean;
g_valid               boolean;

g_date                date          := trunc(sysdate);

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_852U';
l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD THE ALLOCATIONS FACTS EX TRICEPS';
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_alloc_no            DWH_FOUNDATION.stg_triceps_alloc_day_cpy.ALLOC_NO%TYPE;
g_location_no         DWH_FOUNDATION.stg_triceps_alloc_day_cpy.location_no%type;

-- cursor for de-duplication ...
cursor  stg_dup is
        select  *
        from    DWH_FOUNDATION.stg_triceps_alloc_day_cpy
        where  (alloc_no, location_no) in ( select  alloc_no,
                                                    location_no                                                    
                                            from    DWH_FOUNDATION.stg_triceps_alloc_day_cpy
                                            group by
                                                    alloc_no,
                                                    location_no
                                            having  count(*) > 1
                                          )
        order by alloc_no, location_no, sys_source_batch_id desc, sys_source_sequence_no desc;

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

    l_text := 'LOAD OF fnd_triceps_alloc_day EX ALLOCATIONS Started at: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    from   DWH_FOUNDATION.stg_triceps_alloc_day_cpy
    where  sys_process_code = 'N';

    --**************************************************************************************************
    -- De Duplication of the staging table to avoid Bulk insert failures
    --**************************************************************************************************
    l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_location_no    := 0;
    g_alloc_no       := 0;

    for dupp_record in stg_dup loop
        if  dupp_record.location_no = g_location_no
        and dupp_record.alloc_no    = g_alloc_no    then
            update DWH_FOUNDATION.stg_triceps_alloc_day_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id
            and    sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate := g_recs_duplicate  + 1;
        end if;

        g_location_no    := dupp_record.location_no;
        g_alloc_no       := dupp_record.alloc_no;

    end loop;
    commit;

    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --**************************************************************************************************
    -- Bulk Merge controlling main program execution
    --**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ append parallel (fnd_tgt,6) */ into DWH_FOUNDATION.FND_TRICEPS_ALLOC_DAY fnd_tgt
    using ( select /*+ parallel (stg,4) */ *      -- full(stg)
            from   DWH_FOUNDATION.stg_triceps_alloc_day_cpy stg
            where  sys_process_code = 'N'
          ) stg_src
    on    (stg_src.alloc_NO = fnd_tgt.alloc_NO and stg_src.LOCATION_NO = fnd_tgt.LOCATION_NO)
    when matched then
      update
        set   fnd_tgt.RELEASE_QTY             = stg_src.RELEASE_QTY,
              fnd_tgt.ALLOC_QTY               = stg_src.ALLOC_QTY,
              fnd_tgt.TRICEPS_PO_NO           = stg_src.TRICEPS_PO_NO,
              fnd_tgt.TRICEPS_ALLOC_DATE      = stg_src.TRICEPS_ALLOC_DATE,
              fnd_tgt.TRICEPS_ALLOC_TIME      = stg_src.TRICEPS_ALLOC_TIME,
              fnd_tgt.PROCESSED_DATE          = stg_src.PROCESSED_DATE,
              fnd_tgt.PROCESSED_TIME          = stg_src.PROCESSED_TIME,
              fnd_tgt.RELEASE_DATE            = stg_src.RELEASE_DATE,
              fnd_tgt.RELEASE_TIME            = stg_src.RELEASE_TIME,
              fnd_tgt.CLOSED_DATE             = stg_src.CLOSED_DATE,
              fnd_tgt.CLOSED_TIME             = stg_src.CLOSED_TIME,
              fnd_tgt.TRICEPS_PO_GROUPING_IND = stg_src.TRICEPS_PO_GROUPING_IND,
              fnd_tgt.last_updated_date       = g_date,
              fnd_tgt.ALLOC_LAUNCH_CODE       = stg_src.ALLOC_LAUNCH_CODE
    WHEN NOT MATCHED THEN
      INSERT values
             (stg_src.ALLOC_NO,
              stg_src.LOCATION_NO,
              stg_src.RELEASE_QTY,
              stg_src.ALLOC_QTY,
              stg_src.TRICEPS_PO_NO,
              stg_src.TRICEPS_ALLOC_DATE,
              stg_src.TRICEPS_ALLOC_TIME,
              stg_src.PROCESSED_DATE,
              stg_src.PROCESSED_TIME,
              stg_src.RELEASE_DATE,
              stg_src.RELEASE_TIME,
              stg_src.CLOSED_DATE,
              stg_src.CLOSED_TIME,
              stg_src.TRICEPS_PO_GROUPING_IND,
              g_date,
              stg_src.ALLOC_LAUNCH_CODE
             );
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      commit;

      --**************************************************************************************************
      -- Write final log data
      --**************************************************************************************************
      l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      insert /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.stg_triceps_alloc_day_hsp hsp
      select /*+ FULL(TMP) */
              TMP.sys_source_batch_id,
              TMP.sys_source_sequence_no,
              sysdate,
             'Y',
             'DWH',
              TMP.sys_middleware_batch_id,
             'INVALID INDICATOR OR REFERENCIAL ERROR ON ALLOC/LOC',
             
              tmp.SOURCE_DATA_STATUS_CODE,
              TMP.ALLOC_NO,
              TMP.LOCATION_NO,
              TMP.RELEASE_QTY,
              TMP.ALLOC_QTY,
              TMP.TRICEPS_PO_NO,
              TMP.TRICEPS_ALLOC_DATE,
              TMP.TRICEPS_ALLOC_TIME,
              TMP.PROCESSED_DATE,
              TMP.PROCESSED_TIME,
              TMP.RELEASE_DATE,
              TMP.RELEASE_TIME,
              TMP.CLOSED_DATE,
              TMP.CLOSED_TIME,
              TMP.TRICEPS_PO_GROUPING_IND,
              TMP.ALLOC_LAUNCH_CODE
      from    DWH_FOUNDATION.stg_triceps_alloc_day_cpy TMP
      where   sys_process_code = 'N'
      and    (
--              validate only against dim tables - no validation for allocation numbers
--              not exists (select *
--                          from   FND_RTL_ALLOCATION da
--                          where  tmp.alloc_no = da.ALLOC_NO
--                         )
--              or
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

end wh_fnd_corp_852u;
