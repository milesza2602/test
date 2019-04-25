--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_316U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_316U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_txn_dly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_txn_dly_cpy
--               Output - fnd_wfs_crd_txn_dly
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
--  2018-11-05 N Chauhan - check added for accidental send of an old information_date 
--  2018-11-05 N Chauhan - check added for a double batch load 
--  2018-11-05 N Chauhan - Drop indexes after delete, for efficient delete by information_date
--  2018-11-05 N Chauhan - use lib proc drop_index_if_exists.
--  2018-11-06 N Chauhan - due to priv issues proc drop_index_if_exists created locally.
--
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
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
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0; 
g_recs_deleted       integer       :=  0;   
g_truncate_count     integer       :=  0;
g_delete_date        date;
g_truncate_date      date;
g_delete_ind         integer       :=  1;
g_batch_count        integer       :=  0;

g_date               date          := trunc(sysdate);

g_idx_drop_success boolean:= false;
g_idx_existed  boolean:= false;


L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_316U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD TRANACTION DLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--g_tablespace_idx varchar2(50) := 'FND_MASTER';  -- for uat
g_tablespace_idx varchar2(50) := 'WFS_FND_DATA_01';  -- for prod
g_exec_stmt  varchar2(1000);



--************************************************************************************************** 
-- Drop index if exists
--**************************************************************************************************


 procedure drop_index_if_exists
  (p_owner in varchar2, 
   p_index in varchar2,
   p_existed out boolean,
   p_success out boolean) as

    index_not_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(index_not_exists, -1418);

  begin
       p_success:= false;
       execute immediate 'DROP INDEX '||p_owner||'.'||p_index;
       p_success:= true;
       p_existed:= true;

  exception
    when index_not_exists then
       p_success:= true;
       p_existed:= false;
    when others then
       p_success:= false;
       p_existed:= true;

 end drop_index_if_exists;


-- procedure to manage suitable messaging when dropping indexes

procedure  drop_index( p_index_name in varchar2) as

begin   

    drop_index_if_exists(
         'DWH_WFS_FOUNDATION',
         p_index_name,
         g_idx_existed,
         g_idx_drop_success );

     if g_idx_drop_success = false then
        l_text :=p_index_name||'  index drop failed';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
     else 
        if g_idx_existed = false then
           l_text :=p_index_name||'  index drop skipped as it does not exist';
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        else
        l_text :=p_index_name||'  index dropped';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        end if;
     end if;   

end drop_index;




--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
--      /*+ APPEND parallel (fnd,2) */

      insert  /*+ APPEND */  into fnd_wfs_crd_txn_dly fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
             cpy.	information_date	,
             cpy.	account_number	,
             cpy.	txn_date	,
             nvl(cpy.	card_number_used,' ')	,
             cpy.	card_number_posted	,
             cpy.	primary_or_secondary_card	,
             cpy.	original_posted_date	,
             cpy.	processing_date	,
             cpy.	card_main_txn_group	,
             cpy.	card_sub_txn_group	,
             cpy.	debit_credit_flag	,
             cpy.	card_txn_type_code	,
             cpy.	card_txn_category_code	,
             cpy.	card_txn_status_code	,
             cpy.	card_amt	,
             cpy.	on_us_ind	,
             cpy.	card_txn_type_desc	,
             cpy.	card_product_type_desc	,
             cpy.	authorisation_completion_code	,
             cpy.	authorisation_response_code	,
             cpy.	card_txn_source_code	,
             cpy.	print_on_statement_ind	,
             cpy.	merchant_number	,
             cpy.	merchant_name	,
             cpy.	merchant_fin_institution_code	,
             cpy.	merchant_sic_code	,
             cpy.	merchant_city	,
             cpy.	country_code	,
             cpy.	financial_institution_desc	,
             cpy.	financial_institution_code	,
             cpy.	txn_3rd_level_code	,
             cpy.	txn_4th_level_code	,
             cpy.	txn_5th_level_code	,
             cpy.	chip_app_transaction_code	,
             cpy.	chip_condition_code	,
             cpy.	chip_time_date	,
             cpy.	point_of_sale_entry_mode	,
             g_date as last_updated_date
      from   stg_absa_crd_txn_dly_cpy cpy
      where  sys_process_code = 'N';


      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;



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
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************


    -- check if old data was sent

    select     /*+ parallel(stg,4) full(stg) */ 
      min(information_date)  into      g_delete_date
    from      stg_absa_crd_txn_dly_cpy stg;

    if g_delete_date < trunc(add_months(g_date, -1),'MM') then 
    -- earlier than beginning of previous month
       l_text := 'Load aborted - Old data received, INFORMATION_DATE: '||to_char(g_delete_date,'YYYY-MM-DD');
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := false;
       return;
    end if;


    -- check if more than 1 batch was sent

    select  /*+ parallel(stg,4) full(stg) */ 
       count(distinct sys_source_batch_id)  into      g_batch_count
    from     stg_absa_crd_txn_dly_cpy stg;

    if g_batch_count > 1 then -- multiple source files received
       l_text := 'Load aborted - multiple source files received';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := false;
       return;
    end if;


    -- Delete current month's data

    l_text := 'DELETE CURRENT MONTH FROM TXN_DLY STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    begin

    select    information_date
    into      g_delete_date
    from      stg_absa_crd_txn_dly_cpy stg
    where     rownum = 1; 

    exception
            when no_data_found then
              g_delete_date := G_DATE + 1;
              g_delete_ind  := 0;
    end;    

    if g_delete_ind = 1 then 

/*
        l_text := 'Drop Index Begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE IMMEDIATE('drop index DWH_WFS_FOUNDATION.BI1_FND_ABSA_CRD_TXN_DLY');
        EXECUTE IMMEDIATE('drop index DWH_WFS_FOUNDATION.BI2_FND_ABSA_CRD_TXN_DLY');
        commit;
*/
      -- keep indexes till later for more efficient deletes, based on information_date


       -- delete data from beginning of the month

       g_delete_date := trunc(g_delete_date) - (to_char(g_delete_date,'DD') - 1);
       l_text := 'Data Being deleted from '|| g_delete_date || '  '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    BEGIN
--    SELECT MAX(INFORMATION_DATE) 
--    INTO G_TRUNCATE_DATE 
--    FROM DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY
--    WHERE  INFORMATION_DATE >= G_DELETE_DATE;
--    exception
--            when no_data_found then
--              g_truncate_date = G_DELETE_DATE;
--    END;

--    g_stmt   := 'Alter table  DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY truncate  partition for ('||G_TRUNCATE_DATE||')';
--    l_text   := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    execute immediate g_stmt;


       DELETE /*+ PARALLEL(TXN,4) */ FROM fnd_wfs_crd_txn_dly TXN WHERE INFORMATION_DATE >= G_DELETE_DATE;

       g_recs_deleted := g_recs_deleted + sql%rowcount;
       commit;




        select count(*)
        into   g_recs_read
        from   stg_absa_crd_txn_dly_cpy
        where  sys_process_code = 'N';


        -- Drop indexes -------------------

        drop_index('BI1_FND_ABSA_CRD_TXN_DLY');
        drop_index('BI2_FND_ABSA_CRD_TXN_DLY');




        l_text := 'BULK INSERT STARTED AT '||
        to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        flagged_records_insert;


        commit;


        l_text := 'Create B1 Index Begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--        EXECUTE immediate('CREATE BITMAP INDEX DWH_WFS_FOUNDATION.BI1_FND_ABSA_CRD_TXN_DLY ON DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY
--        (INFORMATION_DATE) noLOGGING TABLESPACE WFS_FND_DATA_01 PARALLEL (degree 8) LOCAL');

        g_exec_stmt:= 'CREATE BITMAP INDEX DWH_WFS_FOUNDATION.BI1_FND_ABSA_CRD_TXN_DLY ON DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY
        (INFORMATION_DATE) noLOGGING TABLESPACE '||g_tablespace_idx||' PARALLEL (degree 8) LOCAL';
        EXECUTE immediate(g_exec_stmt);
        Execute Immediate('ALTER INDEX DWH_WFS_FOUNDATION.BI1_FND_ABSA_CRD_TXN_DLY LOGGING NOPARALLEL') ;

        l_text := 'Create B2 Index Begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--        EXECUTE immediate('CREATE BITMAP INDEX DWH_WFS_FOUNDATION.BI2_FND_ABSA_CRD_TXN_DLY ON DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY
--        (ORIGINAL_POSTED_DATE) LOGGING TABLESPACE WFS_FND_DATA_01 PARALLEL (degree 8) LOCAL');
        g_exec_stmt:= 'CREATE BITMAP INDEX DWH_WFS_FOUNDATION.BI2_FND_ABSA_CRD_TXN_DLY ON DWH_WFS_FOUNDATION.FND_WFS_CRD_TXN_DLY
        (ORIGINAL_POSTED_DATE) LOGGING TABLESPACE '||g_tablespace_idx||' PARALLEL (degree 8) LOCAL';
        EXECUTE immediate(g_exec_stmt);
        Execute Immediate('ALTER INDEX DWH_WFS_FOUNDATION.BI2_FND_ABSA_CRD_TXN_DLY LOGGING NOPARALLEL') ;



    --    EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY ON DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY (LAST_UPDATED_DATE)     
    --    TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
    --    Execute Immediate('ALTER INDEX DWH_WFS_FOUNDATION.BI1_FND_ABSA_CRD_TXN_DLY LOGGING NOPARALLEL') ;



        commit;   

    --   l_text := 'UPDATE STATS ON TXN DAILY'; 
    --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --   DBMS_STATS.gather_table_stats ('DWH_WFS_FOUNDATION','FND_WFS_CRD_TXN_DLY',estimate_percent=>1, DEGREE => 32); 

    ELSE
       l_text := 'NO DATA TODAY FOR TXN DAILY'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    end if;
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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DELETED EX TXN_DLY '||g_recs_deleted;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;  


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

end wh_fnd_wfs_316u;
