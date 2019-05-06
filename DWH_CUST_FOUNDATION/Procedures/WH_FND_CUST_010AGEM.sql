--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_010AGEM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_010AGEM" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        January 2013
--  Author:      Alastair de Wet
--  Purpose:     fIX AGE ON DIM_CUSTOMER
--  Tables:      Input  - DIM_CUSTOMER
--               Output - DIM_CUSTOMER
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_age           integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      INTEGER       :=  0;
g_age                NUMBER        :=  0;


g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_010U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER  DIM EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

--**************************************************************************************************
-- CALCULATE THE AGE OF THE ACCOUNT HOLDER
--**************************************************************************************************
procedure calculate_age as
begin



execute immediate 'alter session ENABLE parallel dml';   

MERGE  /*+ parallel (DC,4) */ INTO DIM_CUSTOMER DC USING
(
   SELECT /*+ FULL(CST) parallel(CST,12) */ CUSTOMER_NO,BIRTH_DATE,NVL(AGE_ACC_HOLDER,0) AGE_ACC_HOLDER
   FROM   DIM_CUSTOMER CST
   WHERE  BIRTH_DATE     IS NOT NULL AND
--          AGE_ACC_HOLDER IS NOT NULL AND
          FLOOR(MONTHS_BETWEEN(SYSDATE,BIRTH_DATE) / 12) <> NVL(AGE_ACC_HOLDER,0)  AND
          BIRTH_DATE BETWEEN '1 Jan 1900' AND '1 JAN 2018'

) AGE
ON  ( DC.CUSTOMER_NO       = AGE.CUSTOMER_NO )
WHEN MATCHED THEN
UPDATE     SET     DC.AGE_ACC_HOLDER    = FLOOR(MONTHS_BETWEEN(SYSDATE,AGE.BIRTH_DATE) / 12);
--                  , DC.LAST_UPDATED_DATE = G_DATE;

g_recs_age  := g_recs_age   + sql%rowcount;

END calculate_age;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'FIX AGE ON DIM_CUSTOMER STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    calculate_age;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'AGE RECORDS UPDATED TO CUSTOMER MASTER '||g_recs_age;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_run_completed ||sysdate;
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

END WH_FND_CUST_010AGEM;
