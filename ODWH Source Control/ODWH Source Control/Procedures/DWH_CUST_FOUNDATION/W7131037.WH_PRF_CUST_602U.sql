-- ****** Object: Procedure W7131037.WH_PRF_CUST_602U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_602U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        JUN 2017
--  Author:      Alastair de Wet
--  Purpose:    CREATE CUSTOMER SVOC CAMPAIGN FEEDBACK EX FOUNDATION TABLE
--  Tables:      Input  - fnd_svoc_campaign_feedback
--               Output - cust_svoc_campaign_feedback
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_602U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD cust_svoc_campaign_feedback EX FND TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF cust_svoc_campaign_feedback EX FOUNDATION STARTED AT '||
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

   l_text      := 'Start of Merge to LOAD customer SVOC mapping ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session enable parallel dml';


--**************************************************************************************************
-- merge--
--**************************************************************************************************

   MERGE /*+ parallel (prf,8) */ INTO cust_svoc_campaign_feedback prf USING
   (
    SELECT /*+ parallel (fnd,8) full(fnd) */  *
    FROM   W7131037.fnd_svoc_campaign_feedback fnd
    WHERE last_updated_date = g_date
   ) MER_REC
    ON    (  prf.	SUBSCRIBER_KEY	    =	mer_rec.	SUBSCRIBER_KEY and
             prf.	JOB_ID	            =	mer_rec.	JOB_ID and
             prf.	EVENT_TYPE	        =	mer_rec.	EVENT_TYPE and
             prf.	EVENT_DATE	        =	mer_rec.	EVENT_DATE and
             prf. URL                 = mer_rec.  URL)
   WHEN MATCHED THEN
   UPDATE SET
            prf.	ACCOUNT_ID	        =	mer_rec.	ACCOUNT_ID	,
            prf.	OYB_ACCOUNT_ID	    =	mer_rec.	OYB_ACCOUNT_ID	,
            prf.	LIST_ID	            =	mer_rec.	LIST_ID	,
            prf.	BATCH_ID	          =	mer_rec.	BATCH_ID	,
            prf.	SUBSCRIBER_ID	      =	mer_rec.	SUBSCRIBER_ID	,
            prf.	IS_UNIQUE	          =	mer_rec.	IS_UNIQUE	,
            prf.	DOMAIN	            =	mer_rec.	DOMAIN	,
            prf.	BOUNCE_CATEGORY	    =	mer_rec.	BOUNCE_CATEGORY	,
            prf.	BOUNCE_SUB_CATEGORY	=	mer_rec.	BOUNCE_SUB_CATEGORY	,
            prf.	BOUNCE_TYPE	        =	mer_rec.	BOUNCE_TYPE	,
            prf.	LINK_NAME	          =	mer_rec.	LINK_NAME	,
            prf.	LINK_CONTENT	      =	mer_rec.	LINK_CONTENT	,
            prf.	LOAD_DATE	          =	mer_rec.	LOAD_DATE	,
            prf.	EMAIL_NAME	        =	mer_rec.	EMAIL_NAME	,
            prf.	EMAIL_SUBJECT	      =	mer_rec.	EMAIL_SUBJECT	,
            prf.	CONTROL_GROUP_IND	  =	mer_rec.	CONTROL_GROUP_IND	,
            prf.	OPT_OUT_REASON	    =	mer_rec.	OPT_OUT_REASON	,
            prf.  last_updated_date   = g_date
   WHERE    prf.	ACCOUNT_ID	        <>	mer_rec.	ACCOUNT_ID	OR
            prf.	OYB_ACCOUNT_ID	    <>	mer_rec.	OYB_ACCOUNT_ID	OR
            prf.	LIST_ID	            <>	mer_rec.	LIST_ID	OR
            prf.	BATCH_ID	          <>	mer_rec.	BATCH_ID	OR
            prf.	SUBSCRIBER_ID	      <>	mer_rec.	SUBSCRIBER_ID	OR
            NVL(prf.	IS_UNIQUE,9)    <>	mer_rec.	IS_UNIQUE	OR
            prf.	DOMAIN	            <>	mer_rec.	DOMAIN	OR
            NVL(prf.	BOUNCE_CATEGORY,' ')	    <>	mer_rec.	BOUNCE_CATEGORY	OR
            NVL(prf.	BOUNCE_SUB_CATEGORY,' ')	<>	mer_rec.	BOUNCE_SUB_CATEGORY	OR
            NVL(prf.	BOUNCE_TYPE,' ')	        <>	mer_rec.	BOUNCE_TYPE	OR
            NVL(prf.	LINK_NAME,' ')	          <>	mer_rec.	LINK_NAME	OR
            NVL(prf.	LINK_CONTENT,' ')	      <>	mer_rec.	LINK_CONTENT	OR
            prf.	LOAD_DATE	          <>	mer_rec.	LOAD_DATE	OR
            prf.	EMAIL_NAME	        <>	mer_rec.	EMAIL_NAME	OR
            prf.	EMAIL_SUBJECT	      <>	mer_rec.	EMAIL_SUBJECT	OR
            prf.	CONTROL_GROUP_IND	  <>	mer_rec.	CONTROL_GROUP_IND	OR
            prf.	OPT_OUT_REASON	    <>	mer_rec.	OPT_OUT_REASON
   WHEN NOT MATCHED THEN
   INSERT
          (
          ACCOUNT_ID,
          OYB_ACCOUNT_ID,
          JOB_ID,
          LIST_ID,
          BATCH_ID,
          SUBSCRIBER_ID,
          SUBSCRIBER_KEY,
          EVENT_DATE,
          IS_UNIQUE,
          DOMAIN,
          EVENT_TYPE,
          BOUNCE_CATEGORY,
          BOUNCE_SUB_CATEGORY,
          BOUNCE_TYPE,
          URL,
          LINK_NAME,
          LINK_CONTENT,
          LOAD_DATE,
          EMAIL_NAME,
          EMAIL_SUBJECT,
          CONTROL_GROUP_IND,
          LAST_UPDATED_DATE,
          OPT_OUT_REASON
          )
  values
          (
          MER_REC.ACCOUNT_ID,
          MER_REC.OYB_ACCOUNT_ID,
          MER_REC.JOB_ID,
          MER_REC.LIST_ID,
          MER_REC.BATCH_ID,
          MER_REC.SUBSCRIBER_ID,
          MER_REC.SUBSCRIBER_KEY,
          MER_REC.EVENT_DATE,
          MER_REC.IS_UNIQUE,
          MER_REC.DOMAIN,
          MER_REC.EVENT_TYPE,
          MER_REC.BOUNCE_CATEGORY,
          MER_REC.BOUNCE_SUB_CATEGORY,
          MER_REC.BOUNCE_TYPE,
          MER_REC.URL,
          MER_REC.LINK_NAME,
          MER_REC.LINK_CONTENT,
          MER_REC.LOAD_DATE,
          MER_REC.EMAIL_NAME,
          MER_REC.EMAIL_SUBJECT,
          MER_REC.CONTROL_GROUP_IND,
          G_DATE,
          MER_REC.OPT_OUT_REASON
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


END "WH_PRF_CUST_602U";
