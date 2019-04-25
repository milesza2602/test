--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_460U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_460U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Feb 2018
--  Author:      Quentin Smit
--               Create FND_CTRY_PROD_DY_FACTOR dimention table in the foundation layer
--               with input ex staging table from RMS  .
--  Tables:      Input  - stg_ctry_factor_cpy
--               Output - FND_CTRY_PROD_DY_FACTOR
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
--
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
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_460U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE COUNTRY FACTOR MASTER EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   merge  into DWH_FOUNDATION.FND_CTRY_PROD_DY_FACTOR fnd 
   using (
   
    SELECT /*+ PARALLEL(CPY1,8) */ CPY1.*
    FROM stg_ctry_factor_CPY CPY1,  fnd_currency fc 
    WHERE (SYS_SOURCE_BATCH_ID, SYS_SOURCE_SEQUENCE_NO, BUSINESS_UNIT_NO, cpy1.COUNTRY_CODE, EFFECTIVE_DATE,ZONE_NO,DEPARTMENT_NO,ITEM_NO) IN
          (SELECT /*+ PARALLEL(8) */ SYS_SOURCE_BATCH_ID, SYS_SOURCE_SEQUENCE_NO, BUSINESS_UNIT_NO, COUNTRY_CODE, EFFECTIVE_DATE,ZONE_NO,DEPARTMENT_NO,ITEM_NO
           FROM (SELECT /*+ PARALLEL(tmp,8) */ tmp.*,
                 RANK ()
                    OVER (PARTITION BY BUSINESS_UNIT_NO, COUNTRY_CODE, EFFECTIVE_DATE,ZONE_NO,DEPARTMENT_NO,ITEM_NO ORDER BY SYS_SOURCE_BATCH_ID DESC, SYS_SOURCE_SEQUENCE_NO DESC)
                    AS RANK
                 FROM stg_ctry_factor_CPY tmp
           )
           WHERE RANK = 1)
--     and cpy1.item_no       = fi.item_no
--     and cpy1.department_no = fi.department_no
     and cpy1.country_code  = fc.currency_code
--     and cpy1.zone_no       = fz.zone_no
ORDER BY SYS_SOURCE_BATCH_ID DESC, SYS_SOURCE_SEQUENCE_NO

         ) mer_rec
         
   on    (     fnd.BUSINESS_UNIT_NO	    =	mer_rec.BUSINESS_UNIT_NO
           and fnd.COUNTRY_CODE         = mer_rec.COUNTRY_CODE
           and fnd.EFFECTIVE_DATE       = mer_rec.EFFECTIVE_DATE
            and fnd.ZONE_NO             = mer_rec.ZONE_NO
             and fnd.DEPARTMENT_NO      = mer_rec.DEPARTMENT_NO
              and fnd.ITEM_NO           = mer_rec.ITEM_NO
        )
        
   when matched then 
   update set
          fnd.GROUP_NO                = mer_rec.GROUP_NO,
          fnd.ITEM_PARENT_NO          = mer_rec.ITEM_PARENT_NO,
          fnd.PRICING_FACTOR          = mer_rec.PRICING_FACTOR,
          fnd.PRICING_FACTOR_STATUS   = mer_rec.PRICING_FACTOR_STATUS,
          fnd.last_updated_date       = g_date
          
   when not matched then
   insert
        (         
          BUSINESS_UNIT_NO,
          GROUP_NO,
          DEPARTMENT_NO,
          ITEM_PARENT_NO,
          ITEM_NO,
          COUNTRY_CODE,
          EFFECTIVE_DATE,
          PRICING_FACTOR,
          PRICING_FACTOR_STATUS,
          ZONE_NO,
          LAST_UPDATED_DATE
        )
  values
        (         
          MER_REC.BUSINESS_UNIT_NO,
          MER_REC.GROUP_NO,
          MER_REC.DEPARTMENT_NO,
          MER_REC.ITEM_PARENT_NO,
          MER_REC.ITEM_NO,
          MER_REC.COUNTRY_CODE,
          MER_REC.EFFECTIVE_DATE,
          MER_REC.PRICING_FACTOR,
          MER_REC.PRICING_FACTOR_STATUS,
          MER_REC.ZONE_NO,
          g_date
        )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

     commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 

end flagged_records_update;

--=============================================================================

procedure flagged_records_hospital as
begin

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_ctry_factor_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON COUNTRY',
                            TMP.SOURCE_DATA_STATUS_CODE,
                            TMP.BUSINESS_UNIT_NO,
                            TMP.GROUP_NO,
                            TMP.DEPARTMENT_NO,
                            TMP.ITEM_PARENT_NO,
                            TMP.ITEM_NO,
                            TMP.COUNTRY_CODE,
                            TMP.EFFECTIVE_DATE,
                            TMP.PRICING_FACTOR,
                            TMP.PRICING_FACTOR_STATUS,
                            TMP.ZONE_NO
                            
    from  stg_ctry_factor_cpy  TMP 
    where ( 
--         not exists
--          (select *
--           from   fnd_item fi
--           where  tmp.item_no       = fi.item_no )  
--         or
         not exists
           (select *
           from   fnd_currency fc
           where  tmp.country_code  = fc.currency_code)
--         or
--         not exists
--            (select * 
--              from fnd_zone fz
--             where tmp.zone_no      = fz.zone_no) 
          )  
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

end flagged_records_hospital;


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
     
    select count(*)
    into   g_recs_read
    from   stg_ctry_factor_cpy
    where  sys_process_code = 'N';

    l_text := 'SET NULLS TO ZERO ON STAGING STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    UPDATE stg_ctry_factor_cpy
    SET    ZONE_NO       = NVL(ZONE_NO,0),
           ITEM_NO       = NVL(ITEM_NO,0),
           DEPARTMENT_NO = NVL(DEPARTMENT_NO,0)
    WHERE  ZONE_NO IS NULL OR ITEM_NO IS NULL OR DEPARTMENT_NO IS NULL;
    
    

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    flagged_records_hospital;
    

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
       RAISE;
end wh_fnd_corp_460u;
