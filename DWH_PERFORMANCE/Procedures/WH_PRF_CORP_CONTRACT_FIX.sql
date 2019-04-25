--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_CONTRACT_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_CONTRACT_FIX" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        15 March 2017
--  Author:      Mariska Matthee
--  Purpose:     This procedure will fix the Contract inconsistencies between RMS and BI
--               where the chain code differs
--
--  Tables:      Input  -
--               Output -
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;
g_start_date         date;
g_end_date           date;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_CONTRACT_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'FIX CHAIN_CODE INCONSISTENCIES FOR CONTRACTS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

--**************************************************************************************************
-- Fix started
--**************************************************************************************************

    l_text := 'BACKUP FND AND DIM TABLES STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'create table DWH_FOUNDATION.FND_RTL_CONTRACT_BCK032017
                           as
                       select *
                         from DWH_FOUNDATION.FND_RTL_CONTRACT';

    execute immediate 'create table DWH_PERFORMANCE.DIM_CONTRACT_BCK032017
                           as
                       select *
                         from DWH_PERFORMANCE.DIM_CONTRACT';

    execute immediate 'create table DWH_PERFORMANCE.DIM_DJ_CONTRACT_BCK032017
                           as
                       select *
                         from DWH_PERFORMANCE.DIM_DJ_CONTRACT';

    l_text := 'UPDATE CONTRACT LIST RMS CHAIN CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_foundation.contract_list tmp
       set tmp.rms_chain_code = (select 'CORP' rms_chain_code
                                   from dwh_foundation.contract_list dim
                                  where dim.bi_contract_no = tmp.bi_contract_no
                                    and rms_chain_code = 'null')
     where exists (select 'CORP' rms_chain_code
                     from dwh_foundation.contract_list dim
                    where dim.bi_contract_no = tmp.bi_contract_no
                      and rms_chain_code = 'null');
    commit;

    l_text := 'DELETE MATCHING RECORDS FROM CONTRACT LIST '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete
      from dwh_foundation.contract_list tmp
     where tmp.bi_chain_code = tmp.rms_chain_code;
    commit;

    l_text := 'SET SK1_CONTRACT_NO VALUES '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_foundation.contract_list tmp
       set tmp.bi_corp_sk1_contract_no = (select sk1_contract_no
                                            from dim_contract dim
                                           where dim.contract_no = tmp.bi_contract_no)
     where exists (select sk1_contract_no
                     from dim_contract dim
                    where dim.contract_no = tmp.bi_contract_no);
    commit;

    update dwh_foundation.contract_list tmp
       set tmp.bi_dj_sk1_contract_no = (select sk1_contract_no
                                          from dim_dj_contract dim
                                         where dim.contract_no = tmp.bi_contract_no)
     where exists (select sk1_contract_no
                     from dim_dj_contract dim
                    where dim.contract_no = tmp.bi_contract_no);
    commit;

    l_text := 'MERGE FOUNDATION DATA '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge into fnd_rtl_contract fnd
         using (with stg as (select /*+ parallel(scpy,4) */
                                    SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO,APPROVAL_DATE,
                                    READY_DATE,CONTRACT_TYPE,SUPPLIER_NO,CONTRACT_STATUS_CODE,CREATE_DATE,
                                    CREATE_ID,CANCEL_DATE,COMPLETE_DATE,START_DATE,CONTRACT_END_DATE,
                                    ORDERABLE_IND,PRODUCTION_IND,COMMENT_DESC,ITEM_NO,REF_ITEM_NO,
                                    CONTRACT_QTY,PO_QTY,RECEIVED_QTY,COST_PRICE,DISPLAY_SEQ,
                                    SOURCE_DATA_STATUS_CODE,CHAIN_CODE
                               from stg_rms_rtl_contract_cpy scpy
                              where contract_no in (select bi_contract_no
                                                      from dwh_foundation.contract_list)
                             union
                             select /*+ parallel(sarc,4) */
                                    SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO,APPROVAL_DATE,
                                    READY_DATE,CONTRACT_TYPE,SUPPLIER_NO,CONTRACT_STATUS_CODE,CREATE_DATE,
                                    CREATE_ID,CANCEL_DATE,COMPLETE_DATE,START_DATE,CONTRACT_END_DATE,
                                    ORDERABLE_IND,PRODUCTION_IND,COMMENT_DESC,ITEM_NO,REF_ITEM_NO,
                                    CONTRACT_QTY,PO_QTY,RECEIVED_QTY,COST_PRICE,DISPLAY_SEQ,
                                    SOURCE_DATA_STATUS_CODE,CHAIN_CODE
                               from stg_rms_rtl_contract_arc sarc
                              where contract_no in (select bi_contract_no
                                                      from dwh_foundation.contract_list)
                             union
                             select /*+ parallel(sarc25,4) */
                                    SYS_LOAD_DATE+1 SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO,APPROVAL_DATE,
                                    READY_DATE,CONTRACT_TYPE,SUPPLIER_NO,CONTRACT_STATUS_CODE,CREATE_DATE,
                                    CREATE_ID,CANCEL_DATE,COMPLETE_DATE,START_DATE,CONTRACT_END_DATE,
                                    ORDERABLE_IND,PRODUCTION_IND,COMMENT_DESC,ITEM_NO,REF_ITEM_NO,
                                    CONTRACT_QTY,PO_QTY,RECEIVED_QTY,COST_PRICE,DISPLAY_SEQ,
                                    SOURCE_DATA_STATUS_CODE,CASE WHEN CHAIN_CODE = 'DJ' then null else 'DJ' end CHAIN_CODE
                               from stg_rms_rtl_contract_arc sarc25
                              where contract_no in (520459,520460,520550,520556,520557,520559,520525,520521)
                                and CONTRACT_NO not in (select /*+ parallel(stga25,4) */ contract_no
                                                          from stg_rms_rtl_contract_arc stga25
                                                         where SYS_LOAD_DATE > '25/AUG/16'
                                                        union
                                                        select /*+ parallel(stgc25,4) */ contract_no
                                                          from stg_rms_rtl_contract_cpy stgc25
                                                         where SYS_LOAD_DATE > '25/AUG/16')
                                and SYS_LOAD_DATE = '25/AUG/16')
                select stg.CONTRACT_NO,stg.SEQ_NO,stg.APPROVAL_DATE,
                       stg.READY_DATE,stg.CONTRACT_TYPE,stg.SUPPLIER_NO,stg.CONTRACT_STATUS_CODE,stg.CREATE_DATE,
                       stg.CREATE_ID,stg.CANCEL_DATE,stg.COMPLETE_DATE,stg.START_DATE,stg.CONTRACT_END_DATE,
                       stg.ORDERABLE_IND,stg.PRODUCTION_IND,stg.COMMENT_DESC,stg.ITEM_NO,stg.REF_ITEM_NO,
                       stg.CONTRACT_QTY,stg.PO_QTY,stg.RECEIVED_QTY,stg.COST_PRICE,stg.DISPLAY_SEQ,
                       stg.SOURCE_DATA_STATUS_CODE,g_date LAST_UPDATED_DATE, nvl(FND.REG_RSP,0) REG_RSP_EXCL_VAT,
                       nullif(tmp.RMS_CHAIN_CODE,'CORP') CHAIN_CODE
                 from stg
                 left outer join (select /*+ parallel(fndzi,4) */ item_no, max(reg_rsp) reg_rsp
                                    from fnd_zone_item fndzi
                                   where base_retail_ind  = 1
                                   group by item_no) fnd
                   on stg.item_no = fnd.item_no
                 inner join dwh_foundation.contract_list tmp
                    on stg.contract_no = tmp.bi_contract_no
                 where (stg.contract_no,stg.seq_no,stg.sys_load_date) in (select contract_no, seq_no, max(sys_load_date) sys_load_date
                                                                            from stg
                                                                           group by contract_no, seq_no)) stg
            on (fnd.contract_no = stg.contract_no and
                fnd.seq_no = stg.seq_no)
     when matched then
          update set fnd.APPROVAL_DATE = stg.APPROVAL_DATE,
                     fnd.READY_DATE = stg.READY_DATE,
                     fnd.CONTRACT_TYPE = stg.CONTRACT_TYPE,
                     fnd.SUPPLIER_NO = stg.SUPPLIER_NO,
                     fnd.CONTRACT_STATUS_CODE = stg.CONTRACT_STATUS_CODE,
                     fnd.CREATE_DATE = stg.CREATE_DATE,
                     fnd.CREATE_ID = stg.CREATE_ID,
                     fnd.CANCEL_DATE = stg.CANCEL_DATE,
                     fnd.COMPLETE_DATE = stg.COMPLETE_DATE,
                     fnd.START_DATE = stg.START_DATE,
                     fnd.CONTRACT_END_DATE = stg.CONTRACT_END_DATE,
                     fnd.ORDERABLE_IND = stg.ORDERABLE_IND,
                     fnd.PRODUCTION_IND = stg.PRODUCTION_IND,
                     fnd.COMMENT_DESC = stg.COMMENT_DESC,
                     fnd.ITEM_NO = stg.ITEM_NO,
                     fnd.REF_ITEM_NO = stg.REF_ITEM_NO,
                     fnd.CONTRACT_QTY = stg.CONTRACT_QTY,
                     fnd.PO_QTY = stg.PO_QTY,
                     fnd.RECEIVED_QTY = stg.RECEIVED_QTY,
                     fnd.COST_PRICE = stg.COST_PRICE,
                     fnd.DISPLAY_SEQ = stg.DISPLAY_SEQ,
                     fnd.SOURCE_DATA_STATUS_CODE = 'U',
                     fnd.LAST_UPDATED_DATE = stg.LAST_UPDATED_DATE,
                     fnd.REG_RSP_EXCL_VAT = stg.REG_RSP_EXCL_VAT,
                     fnd.CHAIN_CODE = stg.CHAIN_CODE
     when not matched then
          insert (fnd.CONTRACT_NO,fnd.SEQ_NO,fnd.APPROVAL_DATE,
                  fnd.READY_DATE,fnd.CONTRACT_TYPE,fnd.SUPPLIER_NO,fnd.CONTRACT_STATUS_CODE,fnd.CREATE_DATE,
                  fnd.CREATE_ID,fnd.CANCEL_DATE,fnd.COMPLETE_DATE,fnd.START_DATE,fnd.CONTRACT_END_DATE,
                  fnd.ORDERABLE_IND,fnd.PRODUCTION_IND,fnd.COMMENT_DESC,fnd.ITEM_NO,fnd.REF_ITEM_NO,
                  fnd.CONTRACT_QTY,fnd.PO_QTY,fnd.RECEIVED_QTY,fnd.COST_PRICE,fnd.DISPLAY_SEQ,
                  fnd.SOURCE_DATA_STATUS_CODE,fnd.LAST_UPDATED_DATE,fnd.REG_RSP_EXCL_VAT,fnd.CHAIN_CODE)
          values (stg.CONTRACT_NO,stg.SEQ_NO,stg.APPROVAL_DATE,
                  stg.READY_DATE,stg.CONTRACT_TYPE,stg.SUPPLIER_NO,stg.CONTRACT_STATUS_CODE,stg.CREATE_DATE,
                  stg.CREATE_ID,stg.CANCEL_DATE,stg.COMPLETE_DATE,stg.START_DATE,stg.CONTRACT_END_DATE,
                  stg.ORDERABLE_IND,stg.PRODUCTION_IND,stg.COMMENT_DESC,stg.ITEM_NO,stg.REF_ITEM_NO,
                  stg.CONTRACT_QTY,stg.PO_QTY,stg.RECEIVED_QTY,stg.COST_PRICE,stg.DISPLAY_SEQ,
                  null,stg.LAST_UPDATED_DATE,stg.REG_RSP_EXCL_VAT,stg.CHAIN_CODE);
    commit;

    l_text := 'MARK DELETED LINES FOR FOUNDATION DATA '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge into fnd_rtl_contract fnd
         using (with stg as (select /*+ parallel(scpy,4) */
                                    SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO
                               from stg_rms_rtl_contract_cpy scpy
                              where contract_no in (select bi_contract_no
                                                      from dwh_foundation.contract_list)
                             union
                             select /*+ parallel(sarc,4) */
                                    SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO
                               from stg_rms_rtl_contract_arc sarc
                              where contract_no in (select bi_contract_no
                                                      from dwh_foundation.contract_list)
                             union
                             select /*+ parallel(sarc25,4) */
                                    SYS_LOAD_DATE+1 SYS_LOAD_DATE,CONTRACT_NO,SEQ_NO
                               from stg_rms_rtl_contract_arc sarc25
                              where contract_no in (520459,520460,520550,520556,520557,520559,520525,520521)
                                and CONTRACT_NO not in (select /*+ parallel(stga25,4) */ contract_no
                                                          from stg_rms_rtl_contract_arc stga25
                                                         where SYS_LOAD_DATE > '25/AUG/16'
                                                        union
                                                        select /*+ parallel(stgc25,4) */ contract_no
                                                          from stg_rms_rtl_contract_cpy stgc25
                                                         where SYS_LOAD_DATE > '25/AUG/16')
                                and SYS_LOAD_DATE = '25/AUG/16'),
                     fnd as (select fnd.CONTRACT_NO,fnd.SEQ_NO,'D' SOURCE_DATA_STATUS_CODE,g_date LAST_UPDATED_DATE
                               from fnd_rtl_contract fnd
                              where fnd.contract_no in (select bi_contract_no
                                                          from dwh_foundation.contract_list))
                select CONTRACT_NO,SEQ_NO,SOURCE_DATA_STATUS_CODE,LAST_UPDATED_DATE
                  from fnd
                minus
                select stg.CONTRACT_NO,stg.SEQ_NO,'D' SOURCE_DATA_STATUS_CODE,g_date LAST_UPDATED_DATE
                  from stg
                 where (stg.contract_no,stg.sys_load_date) in (select contract_no,max(sys_load_date) sys_load_date
                                                                 from stg
                                                                group by contract_no)) stg
            on (fnd.contract_no = stg.contract_no and
                fnd.seq_no = stg.seq_no)
     when matched then
          update set fnd.LAST_UPDATED_DATE = stg.LAST_UPDATED_DATE,
                     fnd.SOURCE_DATA_STATUS_CODE = stg.SOURCE_DATA_STATUS_CODE;
    commit;

    l_text := 'MERGE DIMENSION DATA '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge into dim_contract dim
         using (select CONTRACT_NO,
                       APPROVAL_DATE,
                       CONTRACT_TYPE,
                       nvl(cnt.SUPPLIER_NO,0) as supplier_no,
                       ds.sk1_supplier_no,
                       CONTRACT_STATUS_CODE,
                       CREATE_DATE,
                       CREATE_ID,
                       CANCEL_DATE,
                       COMPLETE_DATE,
                       START_DATE,
                       CONTRACT_END_DATE,
                       ORDERABLE_IND,
                       PRODUCTION_IND,
                       MAX(COMMENT_DESC) COMMENT_DESC,
                       'TOTAL' TOTAL,
                       'ALL CONTRACT' TOTAL_DESC,
                       g_date LAST_UPDATED_DATE--,
                       --0 SK1_CHAIN_CODE_IND
                 from  fnd_rtl_contract cnt,
                       dim_supplier ds
                 where nvl(cnt.supplier_no,0) = ds.supplier_no
                   and (chain_code <> 'DJ' or chain_code is null)
                   and contract_no in (select bi_contract_no
                                         from dwh_foundation.contract_list)
                   and nvl(source_data_status_code,'I') <> 'D'
                 group by CONTRACT_NO,
                          APPROVAL_DATE,
                          CONTRACT_TYPE,
                          nvl(cnt.SUPPLIER_NO,0),
                          ds.sk1_supplier_no,
                          CONTRACT_STATUS_CODE,
                          CREATE_DATE,
                          CREATE_ID,
                          CANCEL_DATE,
                          COMPLETE_DATE,
                          START_DATE,
                          CONTRACT_END_DATE,
                          ORDERABLE_IND,
                          PRODUCTION_IND,
                          'TOTAL',
                          'ALL CONTRACT',
                          g_date--,
                          --0
                          ) fnd
            on (dim.contract_no = fnd.contract_no)
     when matched then
          update set dim.APPROVAL_DATE = fnd.APPROVAL_DATE,
                     dim.CONTRACT_TYPE = fnd.CONTRACT_TYPE,
                     dim.SUPPLIER_NO = fnd.SUPPLIER_NO,
                     dim.CONTRACT_STATUS_CODE = fnd.CONTRACT_STATUS_CODE,
                     dim.CREATE_DATE = fnd.CREATE_DATE,
                     dim.CREATE_ID = fnd.CREATE_ID,
                     dim.CANCEL_DATE = fnd.CANCEL_DATE,
                     dim.COMPLETE_DATE = fnd.COMPLETE_DATE,
                     dim.START_DATE = fnd.START_DATE,
                     dim.CONTRACT_END_DATE = fnd.CONTRACT_END_DATE,
                     dim.ORDERABLE_IND = fnd.ORDERABLE_IND,
                     dim.PRODUCTION_IND = fnd.PRODUCTION_IND,
                     dim.COMMENT_DESC = fnd.COMMENT_DESC,
                     dim.TOTAL = fnd.TOTAL,
                     dim.TOTAL_DESC = fnd.TOTAL_DESC,
                     dim.LAST_UPDATED_DATE = fnd.LAST_UPDATED_DATE,
                     dim.SK1_SUPPLIER_NO = fnd.SK1_SUPPLIER_NO--,
                     --dim.SK1_CHAIN_CODE_IND = fnd.SK1_CHAIN_CODE_IND
     when not matched then
          insert (dim.CONTRACT_NO,dim.APPROVAL_DATE,dim.CONTRACT_TYPE,dim.SUPPLIER_NO,dim.CONTRACT_STATUS_CODE,
                  dim.CREATE_DATE,dim.CREATE_ID,dim.CANCEL_DATE,dim.COMPLETE_DATE,dim.START_DATE,
                  dim.CONTRACT_END_DATE,dim.ORDERABLE_IND,dim.PRODUCTION_IND,dim.COMMENT_DESC,dim.TOTAL,
                  dim.TOTAL_DESC,dim.LAST_UPDATED_DATE,dim.SK1_CONTRACT_NO,dim.SK1_SUPPLIER_NO--,dim.SK1_CHAIN_CODE_IND
                  )
          values (fnd.CONTRACT_NO,fnd.APPROVAL_DATE,fnd.CONTRACT_TYPE,fnd.SUPPLIER_NO,fnd.CONTRACT_STATUS_CODE,
                  fnd.CREATE_DATE,fnd.CREATE_ID,fnd.CANCEL_DATE,fnd.COMPLETE_DATE,fnd.START_DATE,
                  fnd.CONTRACT_END_DATE,fnd.ORDERABLE_IND,fnd.PRODUCTION_IND,fnd.COMMENT_DESC,fnd.TOTAL,
                  fnd.TOTAL_DESC,fnd.LAST_UPDATED_DATE,dwh_performance.merch_hierachy_seq.nextval,fnd.SK1_SUPPLIER_NO--,fnd.SK1_CHAIN_CODE_IND
                  );
    commit;

    merge into dim_dj_contract dim
         using (select CONTRACT_NO,
                       APPROVAL_DATE,
                       CONTRACT_TYPE,
                       nvl(cnt.SUPPLIER_NO,0) as supplier_no,
                       ds.sk1_supplier_no,
                       CONTRACT_STATUS_CODE,
                       CREATE_DATE,
                       CREATE_ID,
                       CANCEL_DATE,
                       COMPLETE_DATE,
                       START_DATE,
                       CONTRACT_END_DATE,
                       ORDERABLE_IND,
                       PRODUCTION_IND,
                       MAX(COMMENT_DESC) COMMENT_DESC,
                       'TOTAL' TOTAL,
                       'ALL CONTRACT' TOTAL_DESC,
                       g_date LAST_UPDATED_DATE,
                       1 SK1_CHAIN_CODE_IND
                 from  fnd_rtl_contract cnt,
                       dim_supplier ds
                 where nvl(cnt.supplier_no,0) = ds.supplier_no
                   and cnt.chain_code = 'DJ'
                   and contract_no in (select bi_contract_no
                                         from dwh_foundation.contract_list)
                   and nvl(source_data_status_code,'I') <> 'D'
                 group by CONTRACT_NO,
                          APPROVAL_DATE,
                          CONTRACT_TYPE,
                          nvl(cnt.SUPPLIER_NO,0),
                          ds.sk1_supplier_no,
                          CONTRACT_STATUS_CODE,
                          CREATE_DATE,
                          CREATE_ID,
                          CANCEL_DATE,
                          COMPLETE_DATE,
                          START_DATE,
                          CONTRACT_END_DATE,
                          ORDERABLE_IND,
                          PRODUCTION_IND,
                          'TOTAL',
                          'ALL CONTRACT',
                          g_date,
                          1) fnd
            on (dim.contract_no = fnd.contract_no)
     when matched then
          update set dim.APPROVAL_DATE = fnd.APPROVAL_DATE,
                     dim.CONTRACT_TYPE = fnd.CONTRACT_TYPE,
                     dim.SUPPLIER_NO = fnd.SUPPLIER_NO,
                     dim.CONTRACT_STATUS_CODE = fnd.CONTRACT_STATUS_CODE,
                     dim.CREATE_DATE = fnd.CREATE_DATE,
                     dim.CREATE_ID = fnd.CREATE_ID,
                     dim.CANCEL_DATE = fnd.CANCEL_DATE,
                     dim.COMPLETE_DATE = fnd.COMPLETE_DATE,
                     dim.START_DATE = fnd.START_DATE,
                     dim.CONTRACT_END_DATE = fnd.CONTRACT_END_DATE,
                     dim.ORDERABLE_IND = fnd.ORDERABLE_IND,
                     dim.PRODUCTION_IND = fnd.PRODUCTION_IND,
                     dim.COMMENT_DESC = fnd.COMMENT_DESC,
                     dim.TOTAL = fnd.TOTAL,
                     dim.TOTAL_DESC = fnd.TOTAL_DESC,
                     dim.LAST_UPDATED_DATE = fnd.LAST_UPDATED_DATE,
                     dim.SK1_SUPPLIER_NO = fnd.SK1_SUPPLIER_NO,
                     dim.SK1_CHAIN_CODE_IND = fnd.SK1_CHAIN_CODE_IND
     when not matched then
          insert (dim.CONTRACT_NO,dim.APPROVAL_DATE,dim.CONTRACT_TYPE,dim.SUPPLIER_NO,dim.CONTRACT_STATUS_CODE,
                  dim.CREATE_DATE,dim.CREATE_ID,dim.CANCEL_DATE,dim.COMPLETE_DATE,dim.START_DATE,
                  dim.CONTRACT_END_DATE,dim.ORDERABLE_IND,dim.PRODUCTION_IND,dim.COMMENT_DESC,dim.TOTAL,
                  dim.TOTAL_DESC,dim.LAST_UPDATED_DATE,dim.SK1_CONTRACT_NO,dim.SK1_SUPPLIER_NO,dim.SK1_CHAIN_CODE_IND)
          values (fnd.CONTRACT_NO,fnd.APPROVAL_DATE,fnd.CONTRACT_TYPE,fnd.SUPPLIER_NO,fnd.CONTRACT_STATUS_CODE,
                  fnd.CREATE_DATE,fnd.CREATE_ID,fnd.CANCEL_DATE,fnd.COMPLETE_DATE,fnd.START_DATE,
                  fnd.CONTRACT_END_DATE,fnd.ORDERABLE_IND,fnd.PRODUCTION_IND,fnd.COMMENT_DESC,fnd.TOTAL,
                  fnd.TOTAL_DESC,fnd.LAST_UPDATED_DATE,dwh_performance.merch_hierachy_seq.nextval,fnd.SK1_SUPPLIER_NO,fnd.SK1_CHAIN_CODE_IND);
    commit;

    l_text := 'UPDATE SK1_CONTRACT_NO FOR DIM_CONTRACT FROM DIM_DJ_CONTRACT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dim_contract dim
       set dim.sk1_contract_no = (select sk1_contract_no
                                    from dim_dj_contract dimd
                                   where dimd.contract_no = dim.contract_no)
     where exists (select sk1_contract_no
                     from dim_dj_contract dimd
                    where dimd.contract_no = dim.contract_no)
       and contract_no in (select bi_contract_no
                             from dwh_foundation.contract_list
                            where bi_chain_code = 'DJ'
                              and rms_chain_code = 'CORP'
                              and bi_dj_sk1_contract_no is not null
                              and bi_corp_sk1_contract_no is null);
    commit;

    l_text := 'DELETE FROM DIM_CONTRACT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete
      from dim_contract
     where contract_no in (select bi_contract_no
                              from dwh_foundation.contract_list
                             where bi_chain_code = 'CORP'
                               and rms_chain_code = 'DJ');
    commit;

    l_text := 'MARK ENTRIES ON DIM_DJ_CONTRACT AS FROM DIM_CONTRACT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dim_dj_contract dim
       set dim.sk1_chain_code_ind = 0,
           dim.last_updated_date = g_date
     where contract_no in (select bi_contract_no
                              from dwh_foundation.contract_list
                             where bi_chain_code = 'DJ'
                               and rms_chain_code = 'CORP');
    commit;

    l_text := 'SET SK1_CONTRACT_NO WITH NEW VALUES '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_foundation.contract_list tmp
       set tmp.bi_corp_sk1_contract_no = (select sk1_contract_no
                                            from dim_contract dim
                                           where dim.contract_no = tmp.bi_contract_no)
     where exists (select sk1_contract_no
                     from dim_contract dim
                    where dim.contract_no = tmp.bi_contract_no);
    commit;

    update dwh_foundation.contract_list tmp
       set tmp.bi_dj_sk1_contract_no = (select sk1_contract_no
                                          from dim_dj_contract dim
                                         where dim.contract_no = tmp.bi_contract_no)
     where exists (select sk1_contract_no
                     from dim_dj_contract dim
                    where dim.contract_no = tmp.bi_contract_no);
    commit;

    l_text := 'UPDATE RTL DATA '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge into RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ rtlu
         using (select /*+ parallel(rtl,4) full(rtl) */
                       tmp.BI_DJ_SK1_CONTRACT_NO sk1_contract_no,
                       rtl.sk1_po_no,
                       rtl.sk1_item_no,
                       rtl.tran_date,
                       rtl.sk1_supply_chain_no,
                       rtl.sk1_location_no
                  from RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ rtl
                 inner join dim_dj_purchase_order dim
                    on rtl.sk1_po_no = dim.sk1_po_no
                 inner join dwh_foundation.contract_list tmp
                    on dim.contract_no = tmp.bi_contract_no
                 where rtl.tran_date between '01/JAN/15' and '31/MAR/17'
                   and rtl.sk1_contract_no <> tmp.BI_DJ_SK1_CONTRACT_NO) rtl
            on (rtlu.sk1_po_no = rtl.sk1_po_no and
                rtlu.sk1_item_no = rtl.sk1_item_no and
                rtlu.tran_date = rtl.tran_date and
                rtlu.sk1_supply_chain_no = rtl.sk1_supply_chain_no and
                rtlu.sk1_location_no = rtl.sk1_location_no)
     when matched then
          update set rtlu.sk1_contract_no = rtl.sk1_contract_no;
    commit;

    merge into RTL_PO_SUPCHAIN_LOC_ITEM_DY rtlu
         using (select /*+ parallel(rtl,4) full(rtl) */
                       tmp.BI_CORP_SK1_CONTRACT_NO sk1_contract_no,
                       rtl.sk1_po_no,
                       rtl.sk1_item_no,
                       rtl.tran_date,
                       rtl.sk1_supply_chain_no,
                       rtl.sk1_location_no
                  from RTL_PO_SUPCHAIN_LOC_ITEM_DY rtl
                 inner join dim_purchase_order dim
                    on rtl.sk1_po_no = dim.sk1_po_no
                 inner join dwh_foundation.contract_list tmp
                    on dim.contract_no = tmp.bi_contract_no
                 where rtl.tran_date between '01/JAN/16' and '31/MAR/17'
                   and rtl.sk1_contract_no <> tmp.BI_CORP_SK1_CONTRACT_NO) rtl
            on (rtlu.sk1_po_no = rtl.sk1_po_no and
                rtlu.sk1_item_no = rtl.sk1_item_no and
                rtlu.tran_date = rtl.tran_date and
                rtlu.sk1_supply_chain_no = rtl.sk1_supply_chain_no and
                rtlu.sk1_location_no = rtl.sk1_location_no)
     when matched then
          update set rtlu.sk1_contract_no = rtl.sk1_contract_no;
    commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'FIX COMPLETED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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
end WH_PRF_CORP_CONTRACT_FIX;
