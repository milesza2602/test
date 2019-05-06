--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_857U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_857U" (p_forall_limit in integer,p_success out boolean) as

--  **************************************************************************************************
--  Date:        Jan 2017
--  Author:      Barry K
--  Purpose:     Staging interface for catchweight audit extract data from Triceps in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - STG_TRICEPS_WEIGHT_AUDIT
--               Output - FND_DC_WEIGHT_AUDIT_DY
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
--  **************************************************************************************************

g_forall_limit        integer       :=  10000;
g_recs_read           integer       :=  0;
g_recs_updated        integer       :=  0;
g_recs_inserted       integer       :=  0;
g_recs_hospital       integer       :=  0;
g_recs_duplicate      integer       :=  0;
g_error_count         number        :=  0;
g_error_index         number        :=  0;
g_hospital            char(1)       := 'N';
g_hospital_text       DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_HSP.sys_process_msg%type;
stg_src               DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY%rowtype;
g_found               boolean;
g_valid               boolean;

g_date                date          := trunc(sysdate);

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_857U';
l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD Triceps Catchweight Audit measure data';
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_PRODUCT_NO          DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY.PRODUCT_NO%type;
g_DC_LOC_NO           DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY.DC_LOC_NO%TYPE;
g_AUDIT_DATE          DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY.AUDIT_DATE%TYPE;
g_RECEIPT_NO          DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY.RECEIPT_NO%TYPE;

-- cursor for de-duplication ...
cursor  stg_dup is
    select  *
    from    DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY
    where  (PRODUCT_NO, DC_LOC_NO, trunc(AUDIT_DATE), RECEIPT_NO)
                  in (select PRODUCT_NO, DC_LOC_NO, trunc(AUDIT_DATE), RECEIPT_NO
                      from   DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY
                      group by
                             PRODUCT_NO, DC_LOC_NO, trunc(AUDIT_DATE), RECEIPT_NO
                      having  count(*) > 1
                     )
    order by PRODUCT_NO, DC_LOC_NO, trunc(AUDIT_DATE), RECEIPT_NO, sys_source_batch_id desc, sys_source_sequence_no desc;

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

    l_text := 'LOAD Triceps Catchweight Audit measure data: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
    from   DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY
    where  sys_process_code = 'N';

    --**************************************************************************************************
    -- De Duplication of the staging table to avoid Bulk insert failures
    --**************************************************************************************************
    l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_PRODUCT_NO   := 0;
    g_DC_LOC_NO    := 0;
    g_AUDIT_DATE   := null;
    g_RECEIPT_NO   := 0;

    for dupp_record in stg_dup loop
        if  dupp_record.PRODUCT_NO        = g_PRODUCT_NO
        and dupp_record.DC_LOC_NO         = g_DC_LOC_NO
        and TRUNC(dupp_record.AUDIT_DATE) = g_AUDIT_DATE
        and dupp_record.RECEIPT_NO        = g_RECEIPT_NO
        then
            update DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id
            and    sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate := g_recs_duplicate  + 1;
        end if;

        g_PRODUCT_NO   := dupp_record.PRODUCT_NO;
        g_DC_LOC_NO    := dupp_record.DC_LOC_NO;
        g_AUDIT_DATE   := TRUNC(dupp_record.AUDIT_DATE);
        g_RECEIPT_NO   := dupp_record.RECEIPT_NO;

    end loop;
    commit;

    l_text := 'DEDUP ENDED - RECS='||g_recs_duplicate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --**************************************************************************************************
    -- Bulk Merge controlling main program execution
    --**************************************************************************************************
    l_text := 'MERGE STARTING  ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ parallel (fnd_tgt,4) */ into DWH_FOUNDATION.FND_DC_WEIGHT_AUDIT_DY fnd_tgt
    using ( select /*+ parallel (stg,4) parallel (di,4) full(stg) full(di) */
                   stg.PRODUCT_NO,
                   stg.DC_LOC_NO,
                   trunc(stg.AUDIT_DATE)        AUDIT_DATE,
                   stg.RECEIPT_NO,
                   stg.AUDIT_EMPLOYEE_ID,
                   stg.AUDIT_EMPLOYEE_NAME,
                   stg.PO_NO,
                   stg.SUPPLIER_NO,
                   stg.RECEIVED_UNITS,
                   stg.SCANNED_UNITS,
                   stg.INVOICED_WEIGHT,
                   stg.ENTERED_WEIGHT,
                   stg.AUDITED_WEIGHT,
                   stg.AUDIT_RSP_INCL_VAT,
                   stg.RECEIVING_EMPLOYEE_ID
            from   DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY stg
            join   dim_location dl  on (stg.DC_LOC_NO         = dl.LOCATION_NO)
            join   fnd_supplier ds  on (stg.SUPPLIER_NO       = ds.SUPPLIER_NO)
            join   DIM_CALENDAR dC1 on (TRUNC(stg.AUDIT_DATE) = dC1.CALENDAR_DATE)
            where  stg.sys_process_code = 'N'
          ) stg_src
    on    (stg_src.PRODUCT_NO         = fnd_tgt.PRODUCT_NO and
           stg_src.DC_LOC_NO          = fnd_tgt.DC_LOC_NO and
           trunc(stg_src.AUDIT_DATE)  = TRUNC(fnd_tgt.AUDIT_DATE) and
           stg_src.RECEIPT_NO         = fnd_tgt.RECEIPT_NO
          )
    when matched then
      update
        set   fnd_tgt.AUDIT_EMPLOYEE_ID	    = stg_src.AUDIT_EMPLOYEE_ID,
              fnd_tgt.AUDIT_EMPLOYEE_NAME	  = stg_src.AUDIT_EMPLOYEE_NAME,
              fnd_tgt.PO_NO	                = stg_src.PO_NO,
              fnd_tgt.SUPPLIER_NO	          = stg_src.SUPPLIER_NO,
              fnd_tgt.RECEIVED_UNITS	      = stg_src.RECEIVED_UNITS,
              fnd_tgt.SCANNED_UNITS	        = stg_src.SCANNED_UNITS,
              fnd_tgt.INVOICED_WEIGHT	      = stg_src.INVOICED_WEIGHT,
              fnd_tgt.ENTERED_WEIGHT	      = stg_src.ENTERED_WEIGHT,
              fnd_tgt.AUDITED_WEIGHT	      = stg_src.AUDITED_WEIGHT,
              fnd_tgt.AUDIT_RSP_INCL_VAT	  = stg_src.AUDIT_RSP_INCL_VAT,
              fnd_tgt.RECEIVING_EMPLOYEE_ID	= stg_src.RECEIVING_EMPLOYEE_ID,
              fnd_tgt.last_updated_date     = g_date
    WHEN NOT MATCHED THEN
      INSERT
             (PRODUCT_NO,
              DC_LOC_NO,
              AUDIT_DATE,
              RECEIPT_NO,

              AUDIT_EMPLOYEE_ID,
              AUDIT_EMPLOYEE_NAME,
              PO_NO,
              SUPPLIER_NO,
              RECEIVED_UNITS,
              SCANNED_UNITS,
              INVOICED_WEIGHT,
              ENTERED_WEIGHT,
              AUDITED_WEIGHT,
              AUDIT_RSP_INCL_VAT,
              RECEIVING_EMPLOYEE_ID,
              LAST_UPDATED_DATE
             )
      values
             (stg_src.PRODUCT_NO,
              stg_src.DC_LOC_NO,
              stg_src.AUDIT_DATE,
              stg_src.RECEIPT_NO,

              stg_src.AUDIT_EMPLOYEE_ID,
              stg_src.AUDIT_EMPLOYEE_NAME,
              stg_src.PO_NO,
              stg_src.SUPPLIER_NO,
              stg_src.RECEIVED_UNITS,
              stg_src.SCANNED_UNITS,
              stg_src.INVOICED_WEIGHT,
              stg_src.ENTERED_WEIGHT,
              stg_src.AUDITED_WEIGHT,
              stg_src.AUDIT_RSP_INCL_VAT,
              stg_src.RECEIVING_EMPLOYEE_ID,
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

      insert  /*+ APPEND parallel (hsp,6) */ into DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_HSP hsp
      select  /*+ FULL(TMP) */
              TMP.sys_source_batch_id,
              TMP.sys_source_sequence_no,
              sysdate,
             'Y',
             'DWH',
              TMP.sys_middleware_batch_id,
             'INVALID INDICATOR OR REFERENCIAL ERROR ON PRODUCT/LOCATION/AUDITDTE/RECEIPTNO',

              SOURCE_DATA_STATUS_CODE,
              PRODUCT_NO,
              DC_LOC_NO,
              AUDIT_DATE,
              RECEIPT_NO,
              AUDIT_EMPLOYEE_ID,
              AUDIT_EMPLOYEE_NAME,
              PO_NO,
              SUPPLIER_NO,
              RECEIVED_UNITS,
              SCANNED_UNITS,
              INVOICED_WEIGHT,
              ENTERED_WEIGHT,
              AUDITED_WEIGHT,
              AUDIT_RSP_INCL_VAT,
              RECEIVING_EMPLOYEE_ID
      from    DWH_FOUNDATION.STG_TRICEPS_WEIGHT_AUDIT_CPY  TMP
      where   sys_process_code = 'N'
      and    (
              not exists (select  /* + parallel (di,8) full(di)  */ *
                          from   dim_item di
                          where  tmp.PRODUCT_NO  = di.FD_PRODUCT_NO
                         )
              or
              not exists (select *
                          from   dim_location ds
                          where  tmp.DC_LOC_NO   = ds.location_no
                         )
              or
              not exists (select *
                          from   fnd_supplier fs
                          where  tmp.SUPPLIER_NO = fs.SUPPLIER_NO
                         )
              or
              not exists (select *
                          from   DIM_CALENDAR dC1
                          where  TRUNC(tmp.AUDIT_DATE) = dC1.CALENDAR_DATE
                         )
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

end wh_fnd_corp_857u;
