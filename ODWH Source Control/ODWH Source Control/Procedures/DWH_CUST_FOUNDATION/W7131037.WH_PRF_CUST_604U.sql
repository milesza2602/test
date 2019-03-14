-- ****** Object: Procedure W7131037.WH_PRF_CUST_604U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_604U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        JUN 2017
--  Author:      Alastair de Wet
--  Purpose:    CREATE CUSTOMER SVOC CAMPAIGN FEEDBACK ROLLUP to apex TABLE
--  Tables:      Input  - cust_svoc_campaign_fb_summ
--               Output - apex_ap3_param_1
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
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
g_recs_last_read     integer       :=  0;
g_recs_last_inserted integer       :=  0;
g_recs_hospital      integer       :=  0;


g_date               date          := trunc(sysdate);




l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_604U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP  TO apex_ap3_param_1';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP OF apex_ap3_param_1  '||
    to_char(sysdate,('dd/MON/yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text      := 'Start of Merge to ROLLUP apex_ap3_param_1 ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session force parallel dml';


--**************************************************************************************************
-- merge--
--**************************************************************************************************

   MERGE  INTO APEX_APP_CUST_01.apex_ap3_param_1 prf USING
   (
    SELECT  EMAIL_SUBJECT,
            EMAIL_NAME,
            EVENT_DATE,
            LAST_UPDATED_DATE
    FROM   W7131037.cust_svoc_campaign_fb_summ
    WHERE last_updated_date = g_date
   ) MER_REC
    ON    (  prf.	CAMPAIGN_DESCRIPTION   =	mer_rec.	EMAIL_SUBJECT and
             prf.	PROMOTION_DESCRIPTION	 =	mer_rec.	EMAIL_NAME)

   WHEN MATCHED THEN
   UPDATE SET
            prf.	COMM_DATE	              =	mer_rec.	EVENT_DATE	,
            prf.	START_MEASUREMENT_DATE	=	mer_rec.	EVENT_DATE	,
            prf.	CREATED_DATE	          =	mer_rec.	LAST_UPDATED_DATE,
            prf.	CREATED_BY	            =	'SYSTEM',
            prf.  last_updated_date       = g_date
   WHEN NOT MATCHED THEN
   INSERT
          (
          CAMPAIGN_DESCRIPTION,
          PROMOTION_DESCRIPTION,
          COMM_DATE,
          START_MEASUREMENT_DATE,
          END_MEASUREMENT_DATE,
          CHANNEL,
          UNIT_COST,
          LAST_UPDATED_DATE,
          CREATED_DATE,
          CREATED_BY,
          UPDATED_BY,
          INCLUDE,
          CAMPAIGN_TYPE,
          CONTROL_GROUP
          )
  values
          (
          MER_REC.EMAIL_SUBJECT,
          MER_REC.EMAIL_NAME,
          MER_REC.EVENT_DATE,
          MER_REC.EVENT_DATE,
          '','','',
          MER_REC.LAST_UPDATED_DATE,
          MER_REC.LAST_UPDATED_DATE,
          'SYSTEM',
          '','','',''
          );


g_recs_read:=g_recs_read+SQL%ROWCOUNT;

commit;





l_text :=  'RECORDS WRITTEN TO prf '||g_recs_last_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END "WH_PRF_CUST_604U";
