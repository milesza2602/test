--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_032U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_032U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create CAMPAIGN PROMOTION fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_ls_prod_grp
--               Output - dim_ls_prod_grp
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update
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
g_truncate_count     integer       :=  0;



g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_032U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PROD GROUP EX FND MARKETING';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_ls_prod_grp is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_ls_prod_grp fnd,
              dim_ls_prod_grp prf
      where   fnd.ls_prod_grp_id   = prf.ls_prod_grp_id
   --   and     fnd.last_updated_date   = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.ls_prod_grp_id;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (prf,2) */ into dim_ls_prod_grp prf
      select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.	ls_prod_grp_id	,
            	fnd.	ls_seg_prod_cat_id,
              fnd.  prod_grp_descr,
            	fnd.	food_non_food	,
            	fnd.	prod_grp_no	,
            	fnd.	factor1	,
            	fnd.	factor2	,
            	fnd.	factor3	,
            	fnd.	factor4	,
            	fnd.	factor5	,
            	fnd.	factor6	,
            	fnd.	factor7	,
            	fnd.	factor8	,
            	fnd.	factor9	,
            	fnd.	factor10	,
            	fnd.	factor11	,
            	fnd.	factor12	,
            	fnd.	factor13	,
            	fnd.	factor14	,
            	fnd.	factor15	,
            	fnd.	factor16	,
              g_date as last_updated_date
       from  fnd_ls_prod_grp fnd
       where  --fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_ls_prod_grp
       where  ls_prod_grp_id    = fnd.ls_prod_grp_id
       )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       ;


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

FOR upd_rec IN c_fnd_ls_prod_grp
   loop
     update dim_ls_prod_grp prf
     set    prf.	ls_seg_prod_cat_id	=	upd_rec.	ls_seg_prod_cat_id	,
            prf.	prod_grp_descr	=	upd_rec.	prod_grp_descr	,
            prf.	food_non_food	=	upd_rec.	food_non_food	,
            prf.	prod_grp_no	=	upd_rec.	prod_grp_no	,
            prf.	factor1	=	upd_rec.	factor1	,
            prf.	factor2	=	upd_rec.	factor2	,
            prf.	factor3	=	upd_rec.	factor3	,
            prf.	factor4	=	upd_rec.	factor4	,
            prf.	factor5	=	upd_rec.	factor5	,
            prf.	factor6	=	upd_rec.	factor6	,
            prf.	factor7	=	upd_rec.	factor7	,
            prf.	factor8	=	upd_rec.	factor8	,
            prf.	factor9	=	upd_rec.	factor9	,
            prf.	factor10	=	upd_rec.	factor10	,
            prf.	factor11	=	upd_rec.	factor11	,
            prf.	factor12	=	upd_rec.	factor12	,
            prf.	factor13	=	upd_rec.	factor13	,
            prf.	factor14	=	upd_rec.	factor14	,
            prf.	factor15	=	upd_rec.	factor15	,
            prf.	factor16	=	upd_rec.	factor16	,
            prf.  last_updated_date = g_date
     where  prf.	ls_prod_grp_id	      =	upd_rec.	ls_prod_grp_id ;

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


    select count(*)
    into   g_recs_read
    from   fnd_ls_prod_grp
 --   where  last_updated_date = g_date
    ;

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
       raise;
end wh_prf_cust_032u;