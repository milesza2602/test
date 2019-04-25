--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_005U_SS_HSP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_005U_SS_HSP" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     script for loading 3 files data . SSLK
--
--  Tables:      AIT load - STG_S4S_EMP_JOB_dy
-- for WH_PRF_S4S_005U_SS
--               Input    - STG_S4S_EMP_JOB_dy_hsp
--                        - FND_S4S_EMP_JOB
--               Output   - FND_S4S_EMP_JOB
-- for WH_PRF_S4S_006U_SS
--               Input    - STG_S4S_LOC_EMP_JB_PYCT_DY_hsp
--                        - FND_S4S_LOC_EMP_JOB_PAYCAT_DY
--               Output   - FND_S4S_LOC_EMP_JOB_PAYCAT_DY
-- for WH_PRF_S4S_063U_SS
--               Input    - STG_S4S_ACTL_XCPTN_EMP_DY_hsp
--                        - FND_S4S_ACTL_XCPTN_EMP_DY
--               Output   - FND_S4S_ACTL_XCPTN_EMP_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
--------------------
--WH_PRF_S4S_005U_SS
--------------------

BEGIN


--STEP 2
insert /* +append */  into DWH_FOUNDATION.FND_S4S_EMP_JOB 
select /*+ PARALLEL(a,6) */ * from DWH_FOUNDATION.FND_S4S_EMP_JOB a ;
commit;

--STEP 3
 DBMS_STATS.gather_table_stats ('dwh_foundation', 'FND_S4S_EMP_JOB', degree => 8, cascade => true);

--STEP 4

MERGE INTO DWH_FOUNDATION.FND_S4S_EMP_JOB e
    USING (with hsp_no_dups as
(
Select EMPLOYEE_ID,JOB_ID,JOB_START_DATE,JOB_END_DATE,EMPLOYEE_RATE,PAYPOLICY_ID,sysdate 
from DWH_FOUNDATION.STG_S4S_EMP_JOB_DY_HSP 
where   sys_source_batch_id||sys_source_sequence_no||sys_load_date in
    (select 
        sys_source_batch_id||sys_source_sequence_no ||sys_load_date
    from 
        (select  
            a.*,       
            --rank() over (partition by sys_source_batch_id,sys_source_sequence_no order by sys_load_date desc) rnk 
            rank() over (partition by EMPLOYEE_ID,JOB_START_DATE order by EMPLOYEE_ID,JOB_START_DATE,sys_load_date desc) rnk 
         from DWH_FOUNDATION.STG_S4S_EMP_JOB_DY_HSP a 
         where SYS_PROCESS_MSG = 'JOB_ID NOT FOUND'
         --and sys_load_date >= ' 25-JUN-2017'
        )
    where rnk =1
    ))
        

select

a.EMPLOYEE_ID,
a.JOB_ID,
a.JOB_START_DATE,
a.JOB_END_DATE,
a.EMPLOYEE_RATE,
a.PAYPOLICY_ID,
sysdate LAST_UPDATED_DATE
from hsp_no_dups a,
     dim_job b,
     dwh_hr_performance.dim_employee c,
     dim_pay_policy d
where a.employee_id = c.employee_id
  and a.job_id  =b.job_id
  and a.paypolicy_id = d.PAYPOLICY_ID
--and b.effective_from_date <= start_date
and JOB_START_DATE between  b.sk1_effective_from_date and  b.sk1_effective_to_date) h
    ON (e.job_id = h.job_id and
        e.employee_id =  h.employee_id --and 
       -- e.job_start_date =   h.job_start_date
    
    )
  --WHEN MATCHED THEN   
    
    
  WHEN NOT MATCHED THEN
    INSERT (E.EMPLOYEE_ID,E.JOB_ID,E.JOB_START_DATE,E.JOB_END_DATE,E.EMPLOYEE_RATE,E.PAYPOLICY_ID,E.LAST_UPDATED_DATE)
    VALUES (H.EMPLOYEE_ID,H.JOB_ID,H.JOB_START_DATE,H.JOB_END_DATE,H.EMPLOYEE_RATE,H.PAYPOLICY_ID,H.LAST_UPDATED_DATE);
    COMMIT;
    
--STEP 5 Kick off PRF Procedure --> WH_PRF_S4S_005U_SS

----------------------------------------------------------------
--PART 2
--FND_S4S_LOC_EMP_JOB_PAYCAT_DY
---------------------------------------------------------------
--STEP 1


--STEP 2
insert /* +append */  into  DWH_FOUNDATION.FND_S4S_LOC_EMP_JOB_PAYCAT_DY 
select  /*+ PARALLEL(a,6) */ * from DWH_FOUNDATION.FND_S4S_LOC_EMP_JOB_PAYCAT_DY a

;
commit;

--STEP 3
DBMS_STATS.gather_table_stats ('dwh_foundation', 'FND_S4S_LOC_EMP_JOB_PAYCAT_DY', degree => 8, cascade => true);

--
--update DWH_FOUNDATION.FND_S4S_EMP_JOB
--set job_end_date ='24-SEP-2017'

--select * from v$database
--
--select * from DWH_FOUNDATION.STG_S4S_EMP_JOB_DY_HSP 
--where employee_id = '7071909'
--and job_id = '1000372';
----and job_start_date = '11-SEP-17'

--STEP 4: Merge

--DWH_FOUNDATION.FND_S4S_LOC_EMP_JOB_PAYCAT_DY 
--DWH_FOUNDATION.FND_S4S_LOC_EMP_JOB_PAYCAT_DY
--DWH_FOUNDATION.STG_S4S_LOC_EMP_JB_PYCT_DY_hsp 

MERGE INTO DWH_FOUNDATION.FND_S4S_LOC_EMP_JOB_PAYCAT_DY e
    USING (
  with hsp_no_dups as
(
Select LOCATION_NO,
        EMPLOYEE_ID,
        JOB_ID,
        PAYMENT_CATEGORY_NO,
        PAY_WEEK_DATE,
        BUSINESS_DATE,
        ACTUAL_HRS,
        LAST_MODIFIED_DATE,
        sysdate 
from DWH_FOUNDATION.STG_S4S_LOC_EMP_JB_PYCT_DY_hsp 
where   sys_source_batch_id||sys_source_sequence_no||sys_load_date in
    (select 
        sys_source_batch_id||sys_source_sequence_no ||sys_load_date
    from 
        (select  
            a.*,       
            --rank() over (partition by sys_source_batch_id,sys_source_sequence_no order by sys_load_date desc) rnk 
            rank() over (partition by   LOCATION_NO 
        , EMPLOYEE_ID 
        , JOB_ID 
        , PAYMENT_CATEGORY_NO 
        , PAY_WEEK_DATE 
        , BUSINESS_DATE  
        order by   LOCATION_NO 
        , EMPLOYEE_ID 
        , JOB_ID 
        , PAYMENT_CATEGORY_NO 
        , PAY_WEEK_DATE 
        , BUSINESS_DATE ,
        sys_load_date desc) rnk 
         from DWH_FOUNDATION.STG_S4S_LOC_EMP_JB_PYCT_DY_hsp a 
         where SYS_PROCESS_MSG = 'JOB_ID NOT FOUND'
         --and sys_load_date >= ' 25-JUN-2017'
        )
    where rnk =1
    ))        

select
a.LOCATION_NO,
a.EMPLOYEE_ID,
a.JOB_ID,
a.PAYMENT_CATEGORY_NO,
a.PAY_WEEK_DATE,
a.BUSINESS_DATE,
a.ACTUAL_HRS,
a.LAST_MODIFIED_DATE,
sysdate LAST_UPDATED_DATE
from hsp_no_dups a,
     dim_job b,
     dwh_hr_performance.dim_employee c--,
--     dim_pay_policy d
where a.employee_id = c.employee_id
  and a.job_id  =b.job_id
  --and a.paypolicy_id = d.PAYPOLICY_ID
--and b.effective_from_date <= start_date
and BUSINESS_DATE between  b.sk1_effective_from_date and  b.sk1_effective_to_date
) h

    ON (E.LOCATION_NO = H.LOCATION_NO and
        e.job_id = h.job_id and
        e.employee_id =  h.employee_id and 
        E.PAYMENT_CATEGORY_NO = H.PAYMENT_CATEGORY_NO and 
        E.PAY_WEEK_DATE = H.PAY_WEEK_DATE and 
        E.BUSINESS_DATE =  H.BUSINESS_DATE    
    )    
  --WHEN MATCHED THEN       
    
  WHEN NOT MATCHED THEN
    INSERT (E.LOCATION_NO,E.EMPLOYEE_ID,E.JOB_ID,E.PAYMENT_CATEGORY_NO, E.PAY_WEEK_DATE, E.BUSINESS_DATE, E.ACTUAL_HRS,E.LAST_MODIFIED_DATE,E.LAST_UPDATED_DATE)
    VALUES (H.LOCATION_NO,H.EMPLOYEE_ID,H.JOB_ID,H.PAYMENT_CATEGORY_NO, H.PAY_WEEK_DATE, H.BUSINESS_DATE, H.ACTUAL_HRS,H.LAST_MODIFIED_DATE,H.LAST_UPDATED_DATE);
    COMMIT;
    
-----PART 3 (063U)
  
  
--STEP 2
insert /* +append */  into  DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY 
select  /*+ PARALLEL(a,6) */ * from DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY a

;
commit;

--STEP 3
DBMS_STATS.gather_table_stats ('dwh_foundation', 'FND_S4S_ACTL_XCPTN_EMP_DY', degree => 8, cascade => true);



MERGE INTO DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY  e
    USING (
  with hsp_no_dups as
(
Select  EMPLOYEE_ID,      
        to_date(EXCEPTION_DATE,'DD-MON-YY') EXCEPTION_DATE,
        to_number(EXCEPTION_TYPE_ID) EXCEPTION_TYPE_ID,
        to_number(LOCATION_NO) LOCATION_NO,
        to_number(JOB_ID) job_id,
        EXCEPTION_START_TIME,
        EXCEPTION_END_TIME,
        sysdate 
from DWH_FOUNDATION.STG_S4S_ACTL_XCPTN_EMP_DY_hsp 
where SYS_PROCESS_MSG = 'JOB_ID NOT FOUND' --SSLK
and sys_source_batch_id||sys_source_sequence_no||sys_load_date in
    (select 
        sys_source_batch_id||sys_source_sequence_no ||sys_load_date
    from 
        (select  
            a.*,       
            --rank() over (partition by sys_source_batch_id,sys_source_sequence_no order by sys_load_date desc) rnk 
            rank() over (partition by     EMPLOYEE_ID 
                                        , EXCEPTION_DATE 
                                        , EXCEPTION_TYPE_ID 
                                        , LOCATION_NO 
                                        , JOB_ID 
                                        --, to_date(sys_load_date,'DD-MON-YY') 
                            order by      EMPLOYEE_ID 
                                        , EXCEPTION_DATE 
                                        , EXCEPTION_TYPE_ID 
                                        , LOCATION_NO 
                                        , JOB_ID
                                        , to_date(sys_load_date,'DD-MON-YY') desc
                                        , sys_source_batch_id desc
                                        ) rnk 
         from DWH_FOUNDATION.STG_S4S_ACTL_XCPTN_EMP_DY_hsp a 
         where SYS_PROCESS_MSG = 'JOB_ID NOT FOUND'
         --and sys_load_date >= ' 25-JUN-2017'           
        )
    where rnk =1
    ))        
  

select
        a.EMPLOYEE_ID,
        a.EXCEPTION_DATE,
        a.EXCEPTION_TYPE_ID,
        a.LOCATION_NO,
        a.JOB_ID,
        a.EXCEPTION_START_TIME,
        a.EXCEPTION_END_TIME,
        sysdate LAST_UPDATED_DATE
from hsp_no_dups a,
     dim_job b,
     dwh_hr_performance.dim_employee c,
     dwh_performance.dim_exception_type d
where a.employee_id = c.employee_id
  and a.job_id  =b.job_id
  and d.exception_type_id = a.exception_type_id
--and  to_date(EXCEPTION_DATE,'DD-MON-YY') between  b.sk1_effective_from_date and  b.sk1_effective_to_date
and b.sk1_effective_to_date = '31/DEC/3999'
) h

    ON (E.EMPLOYEE_ID = H.EMPLOYEE_ID and
        e.EXCEPTION_DATE = h.EXCEPTION_DATE and
        e.EXCEPTION_TYPE_ID =  h.EXCEPTION_TYPE_ID and 
        E.LOCATION_NO = H.LOCATION_NO and 
        E.JOB_ID = H.JOB_ID         
        
    )    
  --WHEN MATCHED THEN       
    
  WHEN NOT MATCHED THEN
    INSERT (E.EMPLOYEE_ID,E.EXCEPTION_DATE,E.EXCEPTION_TYPE_ID,E.LOCATION_NO,E.JOB_ID,E.EXCEPTION_START_TIME,E.EXCEPTION_END_TIME,E.LAST_UPDATED_DATE)
    VALUES (H.EMPLOYEE_ID,H.EXCEPTION_DATE,H.EXCEPTION_TYPE_ID,H.LOCATION_NO,H.JOB_ID,H.EXCEPTION_START_TIME,H.EXCEPTION_END_TIME,H.LAST_UPDATED_DATE);
    COMMIT;
    
END WH_FND_S4S_005U_SS_HSP;
