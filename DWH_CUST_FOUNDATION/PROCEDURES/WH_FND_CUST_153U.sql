--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_153U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_153U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create segmant lifestyle fact table in the foundation layer
--               with input ex staging table from SAS. INSERT ONLY!!
--               FOR RERUN - DROP PARTISION FIRST THEN RERUN
--  Tables:      Input  - stg_sas_cust_seg_lifestyle_cpy
--               Output - fnd_cust_segment_lifestyle
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_stmt               varchar2(200);

g_primary_customer_identifier  stg_sas_cust_seg_lifestyle_cpy.primary_customer_identifier%type;
g_fin_year_no                  stg_sas_cust_seg_lifestyle_cpy.fin_year_no%type;
g_fin_month_no                 stg_sas_cust_seg_lifestyle_cpy.fin_month_no%type;
g_segment_no                   stg_sas_cust_seg_lifestyle_cpy.segment_no%type;
g_segment_type                 stg_sas_cust_seg_lifestyle_cpy.segment_type%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_153U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD cust_segment_lifestyle EX SAS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
SELECT * FROM stg_sas_cust_seg_lifestyle_CPY
where (PRIMARY_CUSTOMER_IDENTIFIER,
FIN_YEAR_NO,
FIN_MONTH_NO,
SEGMENT_NO,
SEGMENT_TYPE)
IN
(select PRIMARY_CUSTOMER_IDENTIFIER,
FIN_YEAR_NO,
FIN_MONTH_NO,
SEGMENT_NO,
SEGMENT_TYPE
FROM stg_sas_cust_seg_lifestyle_CPY
group by PRIMARY_CUSTOMER_IDENTIFIER,
FIN_YEAR_NO,
FIN_MONTH_NO,
SEGMENT_NO,
SEGMENT_TYPE
HAVING COUNT(*) > 1)
order by PRIMARY_CUSTOMER_IDENTIFIER,
FIN_YEAR_NO,
FIN_MONTH_NO,
SEGMENT_NO,
SEGMENT_TYPE,
sys_source_batch_id desc ,sys_source_sequence_no desc;


--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_primary_customer_identifier  := 0;
   g_fin_year_no                  := 1900 ;
   g_fin_month_no                 := 13 ;
   g_segment_no                   := 9999999;
   g_segment_type                 := 'SEGTYPE';

for dupp_record in stg_dup
   loop

    if  dupp_record.primary_customer_identifier = g_primary_customer_identifier and
        dupp_record.fin_year_no                 = g_fin_year_no and
        dupp_record.fin_month_no                = g_fin_month_no and
        dupp_record.segment_no                  = g_segment_no and
        dupp_record.segment_type                = g_segment_type then
        update stg_sas_cust_seg_lifestyle_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_primary_customer_identifier := dupp_record.primary_customer_identifier;
    g_fin_year_no                 := dupp_record.fin_year_no;
    g_fin_month_no                := dupp_record.fin_month_no;
    g_segment_no                  := dupp_record.segment_no;
    g_segment_type                := dupp_record.segment_type;

   end loop;

   commit;

   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end remove_duplicates;



--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,4) */ into fnd_cust_segment_lifestyle fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
              cpy.primary_customer_identifier	,
              cpy.fin_year_no	,
            	cpy.fin_month_no	,
            	cpy.segment_no	,
            	cpy.segment_type	,
              g_date as last_updated_date
       from  stg_sas_cust_seg_lifestyle_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_cust_segment_lifestyle
       where  primary_customer_identifier = cpy.primary_customer_identifier and
              fin_year_no                 = cpy.fin_year_no and
              fin_month_no                = cpy.fin_month_no and
              segment_no                  = cpy.segment_no and
              segment_type                = cpy.segment_type
              )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       and sys_process_code = 'N';


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


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;


    commit;

    select count(*)
    into   g_recs_read
    from   stg_sas_cust_seg_lifestyle_cpy
    where  sys_process_code = 'N';

    commit;

--    g_stmt      := 'Alter table  DWH_CUST_FOUNDATION.FND_CUST_SEGMENT_LIFESTYLE truncate  subpartition for (2015,7)';
--    l_text      := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    execute immediate(g_stmt);
--    commit;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;






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
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
      l_text :=  'THIS IS AN INSERTS ONLY LOAD!!!!!!!  '||TO_CHAR(SYSDATE,('dd mon yyyy hh24:mi:ss'));
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      l_text :=  'YOU ARE RERUNNING INSERTS THAT ARE ALREADY ON THE TABLE - WHY???  '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text :=  '(+-Line 225) DELETE THE DAYS RECS OFF THE fnd_cust_segment_lifestyle TABLE &  RERUN -OR- '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      l_text :=  'IGNORE AND CONTINUE BATCH IF THIS WAS A SIMPLE RERUN MISTAKE  '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance SEE LOG FILE';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - SEE LOG FILES');
   end if;


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
end wh_fnd_cust_153u;
