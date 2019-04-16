--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_291U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_291U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Kgomotso Lehabe
--               Create fnd_cust_db_subgroup_involve  table in the foundation layer
--               with input ex staging table from SAS.
--  Tables:      Input  - stg_sas_subgroup_involve
--               Output - fnd_cust_db_subgroup_involve
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_primary_customer_identifier     stg_sas_subgroup_involve_cpy.primary_customer_identifier%type;
g_subgroup_no                     stg_sas_subgroup_involve_cpy.subgroup_no%type;
g_fin_year_no                     stg_sas_subgroup_involve_cpy.fin_year_no%type;
G_FIN_MONTH_NO                    STG_SAS_SUBGROUP_INVOLVE_CPY.FIN_MONTH_NO%type;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_stmt                varchar(500); 

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_291U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE INVOLVEMENT SCORES INTO FOUNDATION -  SUBGROUP';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is

 select * from dwh_cust_foundation.stg_sas_subgroup_involve_cpy
 where (fin_year_no,
        fin_month_no,  
        primary_customer_identifier, 
        subgroup_no) 
 in 
  (select fin_year_no, 
          fin_month_no,  
          primary_customer_identifier, 
          subgroup_no
   from dwh_cust_foundation.stg_sas_subgroup_involve_cpy
  group by fin_year_no, 
          fin_month_no,
          primary_customer_identifier, 
          subgroup_no
  having count(*) > 1
  )
order by fin_year_no, 
        fin_month_no, 
        primary_customer_identifier,  
        subgroup_no, 
        sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_sas_subgroup_involve is
select /*+ FULL(cpy)  parallel (cpy,4) */
              cpy.*
      from    DWH_CUST_FOUNDATION.STG_SAS_SUBGROUP_INVOLVE_CPY CPY,
              dwh_foundation.fnd_subgroup subg
      where   cpy.sys_process_code = 'N' 
      and     cpy.subgroup_no = subg.subgroup_no
     order by
              cpy.fin_year_no, 
              cpy.fin_month_no, 
              cpy.primary_customer_identifier,  
              cpy.subgroup_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

   g_primary_customer_identifier := 0;
   g_subgroup_no                 := 0;
   g_fin_year_no                 := 0;
   g_fin_month_no                := 0 ;

   g_recs_duplicate  := 0;

 for dupp_record in stg_dup
   loop

    if  dupp_record.primary_customer_identifier = g_primary_customer_identifier and
        dupp_record.subgroup_no                 = g_subgroup_no and              
        dupp_record.fin_year_no                 = g_fin_year_no  and
        dupp_record.fin_month_no                = g_fin_month_no  then
        
        update stg_sas_subgroup_involve_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_primary_customer_identifier := dupp_record.primary_customer_identifier;
    g_subgroup_no                 := dupp_record.subgroup_no;
    g_fin_year_no                 := dupp_record.fin_year_no;
    g_fin_month_no                := dupp_record.fin_month_no ;

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
      g_stmt      := 'TRUNCATE table  DWH_CUST_FOUNDATION.FND_CUST_DB_SUBGROUP_INVOLVE';
    execute immediate g_stmt;  
    
     l_text := 'TABLE  DWH_CUST_FOUNDATION.FND_CUST_DB_SUBGROUP_INVOLVE CLEARED BEFORE LOAD' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
      insert /*+ APPEND parallel (fnd,4) */ into fnd_cust_db_subgroup_involve fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
      
             2014, -- G_LAST_YR,
             8, -- g_last_mn,
              cpy.PRIMARY_CUSTOMER_IDENTIFIER ,
              cpy.SUBGROUP_NO,
              cpy.CUSTOMER_NO,
              cpy.NUM_ITEM_SUM,
              cpy.SALES_SUM	,
              cpy.INVOLVEMENT_SCORE,
              g_date
        from  dwh_cust_foundation.stg_sas_subgroup_involve_cpy cpy,
        dwh_foundation.fnd_subgroup subg
      where  cpy.sys_process_code = 'N' 
      and cpy.subgroup_no = subg.subgroup_no;

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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin

--g_recs_hospital := g_recs_hospital + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

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

    select last_yr_fin_year_no,last_mn_fin_month_no 
    into   g_yr_00,g_mn_00 
    from dim_control;
    
   g_last_mn := g_mn_00 - 1;
   g_last_yr := g_yr_00;
   if g_last_mn = 0 then
      g_last_mn := 12;
      G_LAST_YR := G_LAST_YR - 1;
   end if;   

    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    remove_duplicates;

    select count(*)
    into   g_recs_read
    from   stg_sas_subgroup_involve_cpy
    where  sys_process_code = 'N';

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
    l_text :=  'RECORDS READ '||g_recs_read;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
  
    
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if G_RECS_READ <> G_RECS_INSERTED then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - SOME INVALID SUBGROUPS DROPPED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
end WH_FND_CUST_291U;
