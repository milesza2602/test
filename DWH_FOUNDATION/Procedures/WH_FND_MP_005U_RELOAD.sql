--------------------------------------------------------
--  DDL for Procedure WH_FND_MP_005U_RELOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MP_005U_RELOAD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- reclass of CGM - May 2017 - RELOAD OF TARGET TABLE
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create Weekly Subclass Plan Type table in the foundation layer
--               with input ex staging table from MP.
--  Tables:      Input  - stg_mp_chain_subc_wk_plan_cpy
--               Output - fnd_chain_subclass_wk_plan
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Jan 2008 - defect 443  - Change to stg_mp_chain_subc_wk_plan_cpy structure
--              - defect 444  - Change to fnd_chain_subclass_wk_plan structure
--  28 Feb 2012 - defect 4627 - Total Planning Project - add 4 new store measures
--                            - Remove 2 measures (pln_store_intk_fr_rpl_selling, pln_store_intk_fr_fast_selling)
--  06 Nov 2012 - defect ???? - Commitment - add 4 new store measures

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs         number       :=  0;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_mp_chain_subc_wk_plan_hsp.sys_process_msg%type;
g_rec_out            fnd_chain_subclass_wk_plan%rowtype;
g_rec_in             stg_mp_chain_subc_wk_plan_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MP_005U_RELOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WEEKLY SUBCLASS PLAN TYPE EX MP';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_mp_chain_subc_wk_plan_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_chain_subclass_wk_plan%rowtype index by binary_integer;
type tbl_array_u is table of fnd_chain_subclass_wk_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_mp_chain_subc_wk_plan_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_mp_chain_subc_wk_plan_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_CHAIN_SUBCLASS_WK_PLAN EX MP STARTED AT '||
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

          execute immediate 'alter session enable parallel dml';
        g_recs := 0;

    EXECUTE IMMEDIATE('TRUNCATE TABLE  DWH_FOUNDATION.fnd_chain_subclass_wk_plan');
       l_text := 'Truncate DWH_FOUNDATION.fnd_chain_subclass_wk_plan';
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       
       select count(*) into g_recs from dwh_foundation.stg_mp_chain_subc_wk_plan_cpy;
       g_recs_read := g_recs;
       l_text := 'recs read = '|| g_recs;
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        g_recs  := 0;
        
    insert /*+ append */ inTO DWH_FOUNDATION.fnd_chain_subclass_wk_plan
    SELECT /*+ FULL(A) PARALLEL(A,8) */ 
                   a.CHAIN_NO
                  , a.DEPARTMENT_NO
                  , a.CLASS_NO
                  , a.SUBCLASS_NO
                  , a.PLAN_TYPE_NO
                  , a.FIN_YEAR_NO
                  , a.FIN_WEEK_NO
                  , a.PLN_SALES_QTY
                  , a.PLN_SALES
                  , a.PLN_SALES_COST
                  , a.PLN_NET_MKDN
                  , a.PLN_STORE_OPENING_STK_QTY
                  , a.PLN_STORE_OPENING_STK_SELLING
                  , a.PLN_STORE_OPENING_STK_COST
                  , a.PLN_STORE_CLOSING_STK_QTY
                  , a.PLN_STORE_CLOSING_STK_SELLING
                  , a.PLN_STORE_CLOSING_STK_COST
                  , a.PLN_STORE_INTK_QTY
                  , a.PLN_STORE_INTK_SELLING
                  , a.PLN_STORE_INTK_COST
                  , a.PLN_CHAIN_OPENING_STK_QTY
                  , a.PLN_CHAIN_OPENING_STK_SELLING
                  , a.PLN_CHAIN_OPENING_STK_COST
                  , a.PLN_CHAIN_CLOSING_STK_QTY
                  , a.PLN_CHAIN_CLOSING_STK_SELLING
                  , a.PLN_CHAIN_CLOSING_STK_COST
                  , a.PLN_CHAIN_INTK_SELLING
                  , a.PLN_CHN_LOCAL_COMMIT_RPL_SELL
                  , a.PLN_CHN_COMMIT_WH_ORD_SELLING
                  , a.PLN_CHAIN_RTV_QTY
                  , a.PLN_CHAIN_RTV_SELLING
                  , a.PLN_CHAIN_OTB_SELLING
                  , a.PLN_CHAIN_HOLDBACK_SELLING
                  , a.PLN_CHAIN_RELEASE_PLAN_SELLING
                  , a.PLN_CHN_LOCAL_COMMIT_FAST_SELL
                  , a.SOURCE_DATA_STATUS_CODE
                  , g_date LAST_UPDATED_DATE
                  , a.PLN_STORE_CLR_MKDN_SELLING
                  , a.PLN_CHAIN_INTK_QTY
                  , a.PLN_CHAIN_INTK_COST
                  , a.PLN_CHAIN_HOLDBACK_PRC
                  , a.PLN_CHAIN_TOTAL_COMMITMENT
                  , a.PLN_STORE_INTK_TOT_SELLING
                  , a.PLN_STORE_INTER_AFR_SELLING
                  , a.PLN_CHN_COMMIT_WH_ORD_COST
                  , a.PLN_CHN_LOCAL_COMMIT_FAST_COST
                  , a.PLN_CHN_LOCAL_COMMIT_RPL_COST
                  , a.PLN_CHAIN_TOTAL_COMMIT_COST
                  , a.PLN_SALES_MARGIN
                  , a.PLN_STORE_PROM_MKDN_SELLING
                  , a.PLN_STORE_PRICE_ADJUSTMENT
        from dwh_foundation.stg_mp_chain_subc_wk_plan_cpy A,
              dwh_foundation.fnd_subclass fi,
              dwh_foundation.fnd_chain fc
        where plan_type_no  in (50,51,52,53,54) 
              and fc.chain_no = a.chain_no
              and fi.subclass_no = a.subclass_no
              and fi.class_no = a.class_no
              and fi.department_no = a.department_no
        ;
        g_recs := SQL%ROWCOUNT ;
        COMMIT;

       l_text := 'recs inserted = '|| g_recs;
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       
       g_recs_inserted  := 0;
       
       g_recs_inserted  := g_recs;

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
end wh_fnd_mp_005u_RELOAD;
