--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_450U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_450U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2018
--  Author:      Alastair de Wet
--  Purpose:     Load OFIN GL daily rate into staging.
--  Tables:      Input  - stg_ofin_gl_daily_rate_cpy
--                      Output - fnd_gl_daily_rate
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

G_FROM_CURRENCY_CODE VARCHAR2(15 BYTE); 
G_TO_CURRENCY_CODE   VARCHAR2(15 BYTE);
G_CONVERSION_DATE    DATE;
G_CONVERSION_TYPE    VARCHAR2(30 BYTE);

g_date               date          := trunc(sysdate);

g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_sub                integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_450U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD GL DAILY RATE INTO FND TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_ofin_gl_daily_rate_cpy
where (FROM_CURRENCY_CODE,
       TO_CURRENCY_CODE,
       CONVERSION_DATE,
       CONVERSION_TYPE)
in
(select FROM_CURRENCY_CODE,
        TO_CURRENCY_CODE,
        CONVERSION_DATE,
        CONVERSION_TYPE
from stg_ofin_gl_daily_rate_cpy 
group by FROM_CURRENCY_CODE,
         TO_CURRENCY_CODE,
         CONVERSION_DATE,
         CONVERSION_TYPE
having count(*) > 1) 
order by FROM_CURRENCY_CODE,
         TO_CURRENCY_CODE,
         CONVERSION_DATE,
         CONVERSION_TYPE,
sys_source_batch_id desc ,sys_source_sequence_no desc;

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


G_FROM_CURRENCY_CODE := 'XZY'; 
G_TO_CURRENCY_CODE   := 'XZY';
G_CONVERSION_DATE    := '1 JAN 2000';
G_CONVERSION_TYPE    := 'XYZ';
 

for dupp_record in stg_dup
   loop

 
    if  dupp_record.FROM_CURRENCY_CODE = G_FROM_CURRENCY_CODE  AND
        dupp_record.TO_CURRENCY_CODE   = G_TO_CURRENCY_CODE  AND
        dupp_record.CONVERSION_DATE    = G_CONVERSION_DATE  AND
        dupp_record.CONVERSION_TYPE    = G_CONVERSION_TYPE  THEN
        update stg_ofin_gl_daily_rate_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

  
    G_FROM_CURRENCY_CODE   := dupp_record.FROM_CURRENCY_CODE; 
    G_TO_CURRENCY_CODE     := dupp_record.TO_CURRENCY_CODE;
    G_CONVERSION_DATE      := dupp_record.CONVERSION_DATE;
    G_CONVERSION_TYPE      := dupp_record.CONVERSION_TYPE; 

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   MERGE  INTO FND_GL_DAILY_RATE fnd 
   USING (
         select /*+ FULL(cpy)  parallel (4) */  
              cpy.*
      from    STG_OFIN_GL_DAILY_RATE_cpy cpy,
              dwh_foundation.fnd_currency currf,
              dwh_foundation.fnd_currency currt,
              dwh_foundation.fnd_gl_conv_type glc
      where   cpy.sys_process_code           = 'N'  
       and    cpy.from_currency_code         = currf.currency_code 
       and    cpy.to_currency_code           = currt.currency_code 
       and    upper(cpy.CONVERSION_TYPE)     = glc.user_conversion_type 
      
      order by   sys_source_batch_id,sys_source_sequence_no      
         ) mer_rec
   ON   (fnd.FROM_CURRENCY_CODE = mer_rec.FROM_CURRENCY_CODE  AND
         fnd.TO_CURRENCY_CODE   = mer_rec.TO_CURRENCY_CODE  AND
         fnd.CONVERSION_DATE    = mer_rec.CONVERSION_DATE  AND
         fnd.CONVERSION_TYPE    = UPPER(mer_rec.CONVERSION_TYPE ))
   WHEN MATCHED THEN 
   UPDATE SET
  
          fnd.CONVERSION_RATE   =	mer_rec.CONVERSION_RATE,
          fnd.STATUS_CODE       =	mer_rec.STATUS_CODE,
          fnd.CREATION_DATE     =	mer_rec.CREATION_DATE,
          fnd.CREATED_BY        =	mer_rec.CREATED_BY,
          fnd.LAST_UPDATE_DATE  =	mer_rec.LAST_UPDATE_DATE,
          fnd.LAST_UPDATED_BY   =	mer_rec.LAST_UPDATED_BY,
          fnd.LAST_UPDATE_LOGIN =	mer_rec.LAST_UPDATE_LOGIN,
          fnd.last_updated_date = g_date
   WHEN NOT MATCHED THEN
   INSERT
          (         
          FROM_CURRENCY_CODE,
          TO_CURRENCY_CODE,
          CONVERSION_DATE,
          CONVERSION_TYPE,
          CONVERSION_RATE,
          STATUS_CODE,
          CREATION_DATE,
          CREATED_BY,
          LAST_UPDATE_DATE,
          LAST_UPDATED_BY,
          LAST_UPDATE_LOGIN,
          LAST_UPDATED_DATE
          )
  values
          (         
          mer_rec.FROM_CURRENCY_CODE,
          mer_rec.TO_CURRENCY_CODE,
          mer_rec.CONVERSION_DATE,
          UPPER(mer_rec.CONVERSION_TYPE),
          mer_rec.CONVERSION_RATE,
          mer_rec.STATUS_CODE,
          mer_rec.CREATION_DATE,
          mer_rec.CREATED_BY,
          mer_rec.LAST_UPDATE_DATE,
          mer_rec.LAST_UPDATED_BY,
          mer_rec.LAST_UPDATE_LOGIN,
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

--************************************************************************************************** 
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin
     
      insert /*+ APPEND parallel (hsp,2) */ into STG_OFIN_GL_DAILY_RATE_HSP hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'Invalid CURRENCY OR CONVERSION!!',
              CPY.FROM_CURRENCY_CODE,
              CPY.TO_CURRENCY_CODE,
              CPY.CONVERSION_DATE,
              CPY.CONVERSION_TYPE,
              CPY.CONVERSION_RATE,
              CPY.STATUS_CODE,
              CPY.CREATION_DATE,
              CPY.CREATED_BY,
              CPY.LAST_UPDATE_DATE,
              CPY.LAST_UPDATED_BY,
              CPY.LAST_UPDATE_LOGIN
      from    STG_OFIN_GL_DAILY_RATE_CPY cpy
      where 
        (
        (
         not exists 
           (select user_conversion_type
           from    dwh_foundation.fnd_gl_conv_type glc
           where   UPPER(cpy.conversion_type)  = glc.user_conversion_type )  
       ) 
       or
       (
         not exists 
           (select from_currency_code 
           from    dwh_foundation.fnd_currency curr
           where   cpy.from_currency_code  = curr.currency_code )  
       ) 
       or
       (
         not exists 
           (select to_currency_code 
           from    dwh_foundation.fnd_currency curr
           where   cpy.to_currency_code    = curr.currency_code )  
       )  
       )
      AND sys_process_code = 'N';
 
      g_recs_hospital := g_recs_hospital + sql%rowcount;
      
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
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   STG_OFIN_GL_DAILY_RATE_CPY
    where  sys_process_code = 'N';

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
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;            --Bulk load--
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
end wh_fnd_corp_450u;
