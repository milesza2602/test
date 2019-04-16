--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_167U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_167U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2016
--  Author:      Alastair de Wet
--  Purpose:     Create WOD TIER MONTH DETAIL EX POS fact table in the foundation layer
--               with input ex staging table from POS.
--  Tables:      Input  - stg_pos_wod_tiers_cpy
--               Output - fnd_wod_tier_mth_detail
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


g_fin_year_no                    stg_pos_wod_tiers_cpy.fin_year_no%type;
g_primary_customer_identifier    stg_pos_wod_tiers_cpy.primary_customer_identifier%type;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_167U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WOD TIER MONTH DETAILS EX POS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_pos_wod_tiers_cpy
where (fin_year_no,primary_customer_identifier)
in
(select fin_year_no,primary_customer_identifier
from stg_pos_wod_tiers_cpy
group by fin_year_no,primary_customer_identifier
having count(*) > 1)
order by fin_year_no,primary_customer_identifier,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_pos_wod_tiers is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_pos_wod_tiers_cpy cpy,
              fnd_wod_tier_mth_detail FND
      where   cpy.fin_year_no                  =  fnd.fin_year_no and
              cpy.primary_customer_identifier  =  fnd.primary_customer_identifier and
              cpy.sys_process_code       = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.fin_year_no,cpy.primary_customer_identifier,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_fin_year_no                 := 0;
   g_primary_customer_identifier := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.fin_year_no  = g_fin_year_no and
        dupp_record.primary_customer_identifier  = g_primary_customer_identifier then
        update stg_pos_wod_tiers_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_primary_customer_identifier  := dupp_record.primary_customer_identifier;
    g_fin_year_no                  := dupp_record.fin_year_no;


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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_wod_tier_mth_detail fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	FIN_YEAR_NO	,
              cpy.	PRIMARY_CUSTOMER_IDENTIFIER	,
              cpy.	START_TIER	,
              cpy.	OVERRIDE_TIER	,
              cpy.	YTD_SPEND	,
              cpy.	YTD_GREEN_VALUE	,
              cpy.	YTD_TBC_VALUE	,
              cpy.	YTD_TIER_VALUE	,
              cpy.	YTD_DISCOUNT	,
              cpy.	MONTH_01_SPEND	,
              cpy.	MONTH_01_GREEN_VALUE	,
              cpy.	MONTH_01_TBC_VALUE	,
              cpy.	MONTH_01_TIER_VALUE	,
              cpy.	MONTH_01_DISCOUNT	,
              cpy.	MONTH_01_TIER	,
              cpy.	MONTH_02_SPEND	,
              cpy.	MONTH_02_GREEN_VALUE	,
              cpy.	MONTH_02_TBC_VALUE	,
              cpy.	MONTH_02_TIER_VALUE	,
              cpy.	MONTH_02_DISCOUNT	,
              cpy.	MONTH_02_TIER	,
              cpy.	MONTH_03_SPEND	,
              cpy.	MONTH_03_GREEN_VALUE	,
              cpy.	MONTH_03_TBC_VALUE	,
              cpy.	MONTH_03_TIER_VALUE	,
              cpy.	MONTH_03_DISCOUNT	,
              cpy.	MONTH_03_TIER	,
              cpy.	MONTH_04_SPEND	,
              cpy.	MONTH_04_GREEN_VALUE	,
              cpy.	MONTH_04_TBC_VALUE	,
              cpy.	MONTH_04_TIER_VALUE	,
              cpy.	MONTH_04_DISCOUNT	,
              cpy.	MONTH_04_TIER	,
              cpy.	MONTH_05_SPEND	,
              cpy.	MONTH_05_GREEN_VALUE	,
              cpy.	MONTH_05_TBC_VALUE	,
              cpy.	MONTH_05_TIER_VALUE	,
              cpy.	MONTH_05_DISCOUNT	,
              cpy.	MONTH_05_TIER	,
              cpy.	MONTH_06_SPEND	,
              cpy.	MONTH_06_GREEN_VALUE	,
              cpy.	MONTH_06_TBC_VALUE	,
              cpy.	MONTH_06_TIER_VALUE	,
              cpy.	MONTH_06_DISCOUNT	,
              cpy.	MONTH_06_TIER	,
              cpy.	MONTH_07_SPEND	,
              cpy.	MONTH_07_GREEN_VALUE	,
              cpy.	MONTH_07_TBC_VALUE	,
              cpy.	MONTH_07_TIER_VALUE	,
              cpy.	MONTH_07_DISCOUNT	,
              cpy.	MONTH_07_TIER	,
              cpy.	MONTH_08_SPEND	,
              cpy.	MONTH_08_GREEN_VALUE	,
              cpy.	MONTH_08_TBC_VALUE	,
              cpy.	MONTH_08_TIER_VALUE	,
              cpy.	MONTH_08_DISCOUNT	,
              cpy.	MONTH_08_TIER	,
              cpy.	MONTH_09_SPEND	,
              cpy.	MONTH_09_GREEN_VALUE	,
              cpy.	MONTH_09_TBC_VALUE	,
              cpy.	MONTH_09_TIER_VALUE	,
              cpy.	MONTH_09_DISCOUNT	,
              cpy.	MONTH_09_TIER	,
              cpy.	MONTH_10_SPEND	,
              cpy.	MONTH_10_GREEN_VALUE	,
              cpy.	MONTH_10_TBC_VALUE	,
              cpy.	MONTH_10_TIER_VALUE	,
              cpy.	MONTH_10_DISCOUNT	,
              cpy.	MONTH_10_TIER	,
              cpy.	MONTH_11_SPEND	,
              cpy.	MONTH_11_GREEN_VALUE	,
              cpy.	MONTH_11_TBC_VALUE	,
              cpy.	MONTH_11_TIER_VALUE	,
              cpy.	MONTH_11_DISCOUNT	,
              cpy.	MONTH_11_TIER	,
              cpy.	MONTH_12_SPEND	,
              cpy.	MONTH_12_GREEN_VALUE	,
              cpy.	MONTH_12_TBC_VALUE	,
              cpy.	MONTH_12_TIER_VALUE	,
              cpy.	MONTH_12_DISCOUNT	,
              cpy.	MONTH_12_TIER	,
              g_date as last_updated_date
       from  stg_pos_wod_tiers_cpy cpy
       where  not exists
      (select /*+ nl_aj */ * from fnd_wod_tier_mth_detail
       where fin_year_no   = cpy.fin_year_no and
             primary_customer_identifier = cpy.primary_customer_identifier
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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



FOR upd_rec IN c_stg_pos_wod_tiers
   loop
     update fnd_wod_tier_mth_detail fnd
     set    fnd.	START_TIER	=	upd_rec.	START_TIER	,
            fnd.	OVERRIDE_TIER	=	upd_rec.	OVERRIDE_TIER	,
            fnd.	YTD_SPEND	=	upd_rec.	YTD_SPEND	,
            fnd.	YTD_GREEN_VALUE	=	upd_rec.	YTD_GREEN_VALUE	,
            fnd.	YTD_TBC_VALUE	=	upd_rec.	YTD_TBC_VALUE	,
            fnd.	YTD_TIER_VALUE	=	upd_rec.	YTD_TIER_VALUE	,
            fnd.	YTD_DISCOUNT	=	upd_rec.	YTD_DISCOUNT	,
            fnd.	MONTH_01_SPEND	=	upd_rec.	MONTH_01_SPEND	,
            fnd.	MONTH_01_GREEN_VALUE	=	upd_rec.	MONTH_01_GREEN_VALUE	,
            fnd.	MONTH_01_TBC_VALUE	=	upd_rec.	MONTH_01_TBC_VALUE	,
            fnd.	MONTH_01_TIER_VALUE	=	upd_rec.	MONTH_01_TIER_VALUE	,
            fnd.	MONTH_01_DISCOUNT	=	upd_rec.	MONTH_01_DISCOUNT	,
            fnd.	MONTH_01_TIER	=	upd_rec.	MONTH_01_TIER	,
            fnd.	MONTH_02_SPEND	=	upd_rec.	MONTH_02_SPEND	,
            fnd.	MONTH_02_GREEN_VALUE	=	upd_rec.	MONTH_02_GREEN_VALUE	,
            fnd.	MONTH_02_TBC_VALUE	=	upd_rec.	MONTH_02_TBC_VALUE	,
            fnd.	MONTH_02_TIER_VALUE	=	upd_rec.	MONTH_02_TIER_VALUE	,
            fnd.	MONTH_02_DISCOUNT	=	upd_rec.	MONTH_02_DISCOUNT	,
            fnd.	MONTH_02_TIER	=	upd_rec.	MONTH_02_TIER	,
            fnd.	MONTH_03_SPEND	=	upd_rec.	MONTH_03_SPEND	,
            fnd.	MONTH_03_GREEN_VALUE	=	upd_rec.	MONTH_03_GREEN_VALUE	,
            fnd.	MONTH_03_TBC_VALUE	=	upd_rec.	MONTH_03_TBC_VALUE	,
            fnd.	MONTH_03_TIER_VALUE	=	upd_rec.	MONTH_03_TIER_VALUE	,
            fnd.	MONTH_03_DISCOUNT	=	upd_rec.	MONTH_03_DISCOUNT	,
            fnd.	MONTH_03_TIER	=	upd_rec.	MONTH_03_TIER	,
            fnd.	MONTH_04_SPEND	=	upd_rec.	MONTH_04_SPEND	,
            fnd.	MONTH_04_GREEN_VALUE	=	upd_rec.	MONTH_04_GREEN_VALUE	,
            fnd.	MONTH_04_TBC_VALUE	=	upd_rec.	MONTH_04_TBC_VALUE	,
            fnd.	MONTH_04_TIER_VALUE	=	upd_rec.	MONTH_04_TIER_VALUE	,
            fnd.	MONTH_04_DISCOUNT	=	upd_rec.	MONTH_04_DISCOUNT	,
            fnd.	MONTH_04_TIER	=	upd_rec.	MONTH_04_TIER	,
            fnd.	MONTH_05_SPEND	=	upd_rec.	MONTH_05_SPEND	,
            fnd.	MONTH_05_GREEN_VALUE	=	upd_rec.	MONTH_05_GREEN_VALUE	,
            fnd.	MONTH_05_TBC_VALUE	=	upd_rec.	MONTH_05_TBC_VALUE	,
            fnd.	MONTH_05_TIER_VALUE	=	upd_rec.	MONTH_05_TIER_VALUE	,
            fnd.	MONTH_05_DISCOUNT	=	upd_rec.	MONTH_05_DISCOUNT	,
            fnd.	MONTH_05_TIER	=	upd_rec.	MONTH_05_TIER	,
            fnd.	MONTH_06_SPEND	=	upd_rec.	MONTH_06_SPEND	,
            fnd.	MONTH_06_GREEN_VALUE	=	upd_rec.	MONTH_06_GREEN_VALUE	,
            fnd.	MONTH_06_TBC_VALUE	=	upd_rec.	MONTH_06_TBC_VALUE	,
            fnd.	MONTH_06_TIER_VALUE	=	upd_rec.	MONTH_06_TIER_VALUE	,
            fnd.	MONTH_06_DISCOUNT	=	upd_rec.	MONTH_06_DISCOUNT	,
            fnd.	MONTH_06_TIER	=	upd_rec.	MONTH_06_TIER	,
            fnd.	MONTH_07_SPEND	=	upd_rec.	MONTH_07_SPEND	,
            fnd.	MONTH_07_GREEN_VALUE	=	upd_rec.	MONTH_07_GREEN_VALUE	,
            fnd.	MONTH_07_TBC_VALUE	=	upd_rec.	MONTH_07_TBC_VALUE	,
            fnd.	MONTH_07_TIER_VALUE	=	upd_rec.	MONTH_07_TIER_VALUE	,
            fnd.	MONTH_07_DISCOUNT	=	upd_rec.	MONTH_07_DISCOUNT	,
            fnd.	MONTH_07_TIER	=	upd_rec.	MONTH_07_TIER	,
            fnd.	MONTH_08_SPEND	=	upd_rec.	MONTH_08_SPEND	,
            fnd.	MONTH_08_GREEN_VALUE	=	upd_rec.	MONTH_08_GREEN_VALUE	,
            fnd.	MONTH_08_TBC_VALUE	=	upd_rec.	MONTH_08_TBC_VALUE	,
            fnd.	MONTH_08_TIER_VALUE	=	upd_rec.	MONTH_08_TIER_VALUE	,
            fnd.	MONTH_08_DISCOUNT	=	upd_rec.	MONTH_08_DISCOUNT	,
            fnd.	MONTH_08_TIER	=	upd_rec.	MONTH_08_TIER	,
            fnd.	MONTH_09_SPEND	=	upd_rec.	MONTH_09_SPEND	,
            fnd.	MONTH_09_GREEN_VALUE	=	upd_rec.	MONTH_09_GREEN_VALUE	,
            fnd.	MONTH_09_TBC_VALUE	=	upd_rec.	MONTH_09_TBC_VALUE	,
            fnd.	MONTH_09_TIER_VALUE	=	upd_rec.	MONTH_09_TIER_VALUE	,
            fnd.	MONTH_09_DISCOUNT	=	upd_rec.	MONTH_09_DISCOUNT	,
            fnd.	MONTH_09_TIER	=	upd_rec.	MONTH_09_TIER	,
            fnd.	MONTH_10_SPEND	=	upd_rec.	MONTH_10_SPEND	,
            fnd.	MONTH_10_GREEN_VALUE	=	upd_rec.	MONTH_10_GREEN_VALUE	,
            fnd.	MONTH_10_TBC_VALUE	=	upd_rec.	MONTH_10_TBC_VALUE	,
            fnd.	MONTH_10_TIER_VALUE	=	upd_rec.	MONTH_10_TIER_VALUE	,
            fnd.	MONTH_10_DISCOUNT	=	upd_rec.	MONTH_10_DISCOUNT	,
            fnd.	MONTH_10_TIER	=	upd_rec.	MONTH_10_TIER	,
            fnd.	MONTH_11_SPEND	=	upd_rec.	MONTH_11_SPEND	,
            fnd.	MONTH_11_GREEN_VALUE	=	upd_rec.	MONTH_11_GREEN_VALUE	,
            fnd.	MONTH_11_TBC_VALUE	=	upd_rec.	MONTH_11_TBC_VALUE	,
            fnd.	MONTH_11_TIER_VALUE	=	upd_rec.	MONTH_11_TIER_VALUE	,
            fnd.	MONTH_11_DISCOUNT	=	upd_rec.	MONTH_11_DISCOUNT	,
            fnd.	MONTH_11_TIER	=	upd_rec.	MONTH_11_TIER	,
            fnd.	MONTH_12_SPEND	=	upd_rec.	MONTH_12_SPEND	,
            fnd.	MONTH_12_GREEN_VALUE	=	upd_rec.	MONTH_12_GREEN_VALUE	,
            fnd.	MONTH_12_TBC_VALUE	=	upd_rec.	MONTH_12_TBC_VALUE	,
            fnd.	MONTH_12_TIER_VALUE	=	upd_rec.	MONTH_12_TIER_VALUE	,
            fnd.	MONTH_12_DISCOUNT	=	upd_rec.	MONTH_12_DISCOUNT	,
            fnd.	MONTH_12_TIER	=	upd_rec.	MONTH_12_TIER	,

            fnd.  last_updated_date         = g_date
     where  fnd.	fin_year_no	        =	upd_rec.	fin_year_no and
            fnd.primary_customer_identifier = upd_rec.primary_customer_identifier	;

      g_recs_updated := g_recs_updated + 1;
   end loop;


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

 
    select count(*)
    into   g_recs_read
    from   stg_pos_wod_tiers_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

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
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
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
end wh_fnd_cust_167u;
