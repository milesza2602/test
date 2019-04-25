--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_044U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_044U_OLD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load payment_category_WEEK information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_DY
--               Output   - DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------   
--                 Standard cursor bulk insert/update due to complicated calculations done.
--                 Process all recvords in daily table
--
--               Delete process :
--               ----------------   
--                 None
--
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Maintenance:
--  w lyttle  23 march 2016  - as this job should only run once a week when data is sent from source,
--                             we will now do a check on the founda6tion table to see if any data has loaded
--                             in this batch.
--                             ie. condition on job in appworx will no longer be applied to run on a Friday only.
--
-- w lyttle 5 may 2017 - commented out check for processing where foundation was updated.
--                        this is now controlled by appworx schedule where job will only run on a friday morning
--                          as part of Thursday nights batch.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            RTL_LOC_EMP_JOB_PAYCAT_wk%rowtype;

g_rec_cnt            number        := 0;

g_total  number(14,2) :=  0;
g_total_plus_training  number(14,2) :=  0;

g_paid_leave  number(14,2) :=  0;
g_ex_paid_leave  number(14,2) :=  0;

 g_total_plus_paid_leave number(14,2) :=  0;

g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;

g_found              boolean;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_044U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_LOC_EMP_JOB_PAYCAT_WK data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_LOC_EMP_JOB_PAYCAT_wk%rowtype index by binary_integer;
type tbl_array_u is table of RTL_LOC_EMP_JOB_PAYCAT_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_LOCATION is
WITH selext1 AS
  (
  SELECT
          /*+ full(RTL) parallel(RTL,6) */
          distinct 
                    rtl.pay_week_date,
                    case when rtl.business_date < rtl.pay_week_date 
                          and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                          and db.TCOE_ind = 0 
                                  then dc.this_week_start_date
                          else
                               rtl.pay_week_date
                    end derived_pay_week_date,
                     rtl.business_date,
                    rtl.SK1_LOCATION_NO ,
                    rtl.SK1_EMPLOYEE_ID ,
                    dpc.payment_category_NO ,
                    rtl.sk1_JOB_ID,
                    db.TCOE_ind,
                    dpc.payment_category_percent,
                    case when dpc.payment_category_no = 1000251 and rtl.business_date <> rtl.pay_week_date then 0
                           else  
                                case when rtl.business_date < rtl.pay_week_date 
                                     and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                                     and db.TCOE_ind = 0 
                                              then 0
                                     else
                                          NVL(ACTUAL_HRS,0) 
                                     end 
                    end actual_hrs,
                    case when rtl.business_date < rtl.pay_week_date 
                          and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                          and db.TCOE_ind = 0 
                              then NVL(ACTUAL_HRS,0) 
                          else
                               0
                          end ex_actual_hours
FROM dwh_performance.rtl_LOC_EMP_job_PAYCAT_DY rtl,
      dim_payment_category dpc,
      dim_s4s_business_unit db,
      dim_job dj,
      dim_calendar dc
WHERE rtl.sk1_payment_category_NO = Dpc.sk1_payment_category_NO
  and dj.sk1_s4s_business_unit_no = db.sk1_s4s_business_unit_no
  and dj.sk1_job_id = rtl.sk1_job_id
  and dc.calendar_date = rtl.business_date
 -- AND RTL.LAST_UPDATED_DATE = G_DATE
 -- and sk1_employee_id = '190722'
--  and pay_week_date between '23 jun 2014' and '30 june 2014'
),
selext2 as (
  SELECT
                fin_year_no,
                fin_week_no,
                SK1_LOCATION_NO ,
                SK1_EMPLOYEE_ID ,
                payment_category_NO ,
                sk1_JOB_ID,
                TCOE_ind,
                payment_category_percent,
                sum(nvl( actual_hrs,0)) total_actual_hrs,
                sum(nvl(ex_actual_hours,0)) total_ex_actual_hrs
FROM selext1 se1,
      dim_calendar dc
WHERE se1.pay_week_date = dc.calendar_date
group by fin_year_no,
                fin_week_no,
                SK1_LOCATION_NO ,
                SK1_EMPLOYEE_ID ,
                payment_category_NO ,
                sk1_JOB_ID,
                TCOE_ind,
                payment_category_percent
--  and sk1_employee_id = '227140'
),
selext3 as (
 select * from 
selext2 
pivot(SUM(NVL(total_ACTUAL_HRS,0)) as totacthrs
, SUM(NVL(total_ex_actual_hrs,0)) as totexacthrs
,min(payment_category_percent) as paycatperc
for payment_category_NO in(
1000200	Basic
,1000201	Additional
,1000202	Daily_Overtime
,1000203	Public_Holiday
,1000204	Non_Working_Day
,1000205	Night
,1000209	Annual_Leave
,1000210	Non_Stat_Leave
,1000211	Coida_100
,1000212	Coida_75
,1000213	Ext_Illness_33
,1000214	Family_Responsib
,1000215	Flexi_Mat_LV_33
,1000216	Flexi_Mat_LV_50
,1000217	Sick_Leave_Entit
,1000218	Suspension
,1000219	Training
,1000224	Study_Leave
,1000251	Pay_Adj_Basic
,1000254	Weekly_Overtime
)
)
)
SELECT distinct se3.*,
    rc.STRICT_MIN_HRS_PER_WK,
    employee_rate,
    flr.sk1_employee_id rtl_exists
      FROM SELEXT3 se3
left outer    join dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK rc
      on rc.SK1_EMPLOYEE_ID = se3.sk1_employee_id
         and rc.FIN_YEAR_NO = se3.fin_year_no
          and rc.FIN_WEEK_NO = se3.fin_week_no
      left outer join dwh_performance.rtl_LOC_EMP_job_PAYCAT_wk flr
      on  flr.SK1_LOCATION_NO = se3.SK1_LOCATION_NO
          and flr.SK1_EMPLOYEE_ID = se3.SK1_EMPLOYEE_ID
          and flr.SK1_JOB_ID = se3.SK1_JOB_ID
          and flr.FIN_YEAR_NO = se3.FIN_YEAR_NO
          and flr.FIN_WEEK_NO = se3.FIN_WEEK_NO
       join dwh_performance.RTL_EMP_JOB_WK ej
      on   ej.SK1_EMPLOYEE_ID = se3.SK1_EMPLOYEE_ID
          and ej.SK1_JOB_ID = se3.SK1_JOB_ID
          and ej.FIN_YEAR_NO = se3.FIN_YEAR_NO
          and ej.FIN_WEEK_NO = se3.FIN_WEEK_NO
          WHERE TCOE_IND = 1
                      union
SELECT distinct se3.*,
    rc.STRICT_MIN_HRS_PER_WK,
    employee_rate,
    flr.sk1_employee_id rtl_exists
      FROM SELEXT3 se3
    join dwh_performance.RTL_EMP_CONSTR_LOC_JOB_WK rc
      on rc.SK1_EMPLOYEE_ID = se3.sk1_employee_id
         and rc.FIN_YEAR_NO = se3.fin_year_no
          and rc.FIN_WEEK_NO = se3.fin_week_no
      left outer join dwh_performance.rtl_LOC_EMP_job_PAYCAT_wk flr
      on  flr.SK1_LOCATION_NO = se3.SK1_LOCATION_NO
          and flr.SK1_EMPLOYEE_ID = se3.SK1_EMPLOYEE_ID
          and flr.SK1_JOB_ID = se3.SK1_JOB_ID
          and flr.FIN_YEAR_NO = se3.FIN_YEAR_NO
          and flr.FIN_WEEK_NO = se3.FIN_WEEK_NO
       join dwh_performance.RTL_EMP_JOB_WK ej
      on   ej.SK1_EMPLOYEE_ID = se3.SK1_EMPLOYEE_ID
          and ej.SK1_JOB_ID = se3.SK1_JOB_ID
          and ej.FIN_YEAR_NO = se3.FIN_YEAR_NO
          and ej.FIN_WEEK_NO = se3.FIN_WEEK_NO
                    WHERE TCOE_IND = 0
                   ;




type stg_array is table of c_fnd_LOCATION%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_LOCATION%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
        G_REC_OUT.SK1_LOCATION_NO         := G_REC_IN.SK1_LOCATION_NO;
        G_REC_OUT.SK1_EMPLOYEE_ID         := G_REC_IN.SK1_EMPLOYEE_ID;
        G_REC_OUT.SK1_JOB_ID              := G_REC_IN.SK1_JOB_ID;
        G_REC_OUT.fin_year_no             := G_REC_IN.fin_year_no;
        G_REC_OUT.fin_week_no             := G_REC_IN.fin_week_no;
        g_rec_out.LAST_UPDATED_DATE       := g_date;
        
 If G_REC_IN.tcoe_ind = 1
    then g_total := (nvl(g_rec_in.basic_totacthrs,0) * nvl(g_rec_in.basic_paycatperc,0)/100) ;
         g_rec_out.ACTUAL_FTE_HRS_WK := g_total;
         g_rec_out.ACTUAL_cost_HRS_WK := g_total;

         g_rec_out.ACTUAL_FTE_WK := nvl(g_rec_out.ACTUAL_FTE_HRS_WK /40,0);
         g_rec_out.ACTUAL_COST_WK   := nvl(g_rec_out.ACTUAL_cost_HRS_WK,0)  * nvl(g_rec_in.employee_rate,0);
 else
          g_total_plus_training := (nvl(g_rec_in.basic_totacthrs,0) * nvl(g_rec_in.basic_paycatperc,0)/100) 
                                 + (nvl(g_rec_in.Training_totacthrs,0) * nvl(g_rec_in.Training_paycatperc,0)/100);

          g_paid_leave  := (nvl(g_rec_in.Annual_Leave_totacthrs,0) * nvl(g_rec_in.Annual_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Coida_75_totacthrs,0) * nvl(g_rec_in.Coida_75_paycatperc,0)/100)
                         + (nvl(g_rec_in.Coida_100_totacthrs,0) * nvl(g_rec_in.Coida_100_paycatperc,0)/100)
                         + (nvl(g_rec_in.Ext_Illness_33_totacthrs,0) * nvl(g_rec_in.Ext_Illness_33_paycatperc,0)/100)
                         + (nvl(g_rec_in.Family_Responsib_totacthrs,0) * nvl(g_rec_in.Family_Responsib_paycatperc,0)/100)
                         + (nvl(g_rec_in.Flexi_Mat_LV_33_totacthrs,0) * nvl(g_rec_in.Flexi_Mat_LV_33_paycatperc,0)/100)
                         + (nvl(g_rec_in.Flexi_Mat_LV_50_totacthrs,0) * nvl(g_rec_in.Flexi_Mat_LV_50_paycatperc,0)/100)
                         + (nvl(g_rec_in.Non_Stat_Leave_totacthrs,0) * nvl(g_rec_in.Non_Stat_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Sick_Leave_Entit_totacthrs,0) * nvl(g_rec_in.Sick_Leave_Entit_paycatperc,0)/100)
                         + (nvl(g_rec_in.Study_Leave_totacthrs,0) * nvl(g_rec_in.Study_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Suspension_totacthrs,0) * nvl(g_rec_in.Suspension_paycatperc,0)/100)
                         ;

          g_EX_paid_leave  := (nvl(g_rec_in.Annual_Leave_totexacthrs,0) * nvl(g_rec_in.Annual_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Coida_75_totexacthrs,0) * nvl(g_rec_in.Coida_75_paycatperc,0)/100)
                         + (nvl(g_rec_in.Coida_100_totexacthrs,0) * nvl(g_rec_in.Coida_100_paycatperc,0)/100)
                         + (nvl(g_rec_in.Ext_Illness_33_totexacthrs,0) * nvl(g_rec_in.Ext_Illness_33_paycatperc,0)/100)
                         + (nvl(g_rec_in.Family_Responsib_totexacthrs,0) * nvl(g_rec_in.Family_Responsib_paycatperc,0)/100)
                         + (nvl(g_rec_in.Flexi_Mat_LV_33_totexacthrs,0) * nvl(g_rec_in.Flexi_Mat_LV_33_paycatperc,0)/100)
                         + (nvl(g_rec_in.Flexi_Mat_LV_50_totexacthrs,0) * nvl(g_rec_in.Flexi_Mat_LV_50_paycatperc,0)/100)
                         + (nvl(g_rec_in.Non_Stat_Leave_totexacthrs,0) * nvl(g_rec_in.Non_Stat_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Sick_Leave_Entit_totexacthrs,0) * nvl(g_rec_in.Sick_Leave_Entit_paycatperc,0)/100)
                         + (nvl(g_rec_in.Study_Leave_totexacthrs,0) * nvl(g_rec_in.Study_Leave_paycatperc,0)/100)
                         + (nvl(g_rec_in.Suspension_totexacthrs,0) * nvl(g_rec_in.Suspension_paycatperc,0)/100)
                         ;


          if g_total_plus_training < nvl(g_rec_in.STRICT_MIN_HRS_PER_WK,0)
          then
                 if g_total_plus_training +  g_paid_leave > nvl(g_rec_in.STRICT_MIN_HRS_PER_WK,0)
                    then 
                          g_total_plus_paid_leave := nvl(g_rec_in.STRICT_MIN_HRS_PER_WK,0);
                    else 
                          g_total_plus_paid_leave := g_total_plus_training +  g_paid_leave;
                 end if;  
          else
           g_total_plus_paid_leave := g_total_plus_training ;
          end if;   

       g_rec_out.ACTUAL_FTE_HRS_WK := g_total_plus_paid_leave  
                                    + (nvl(g_rec_in.Additional_totacthrs,0)  * nvl(g_rec_in.Additional_paycatperc,0)/100)
                                    + (nvl(g_rec_in.Pay_Adj_Basic_totacthrs,0)  * nvl(g_rec_in.Pay_Adj_Basic_paycatperc,0)/100)	
                                    + g_EX_paid_leave;
       
       g_rec_out.ACTUAL_cost_HRS_WK := nvl(g_rec_out.ACTUAL_FTE_HRS_WK,0)
                                    + (nvl(g_rec_in.Weekly_Overtime_totacthrs,0) * nvl(g_rec_in.Weekly_Overtime_paycatperc,0)/100)
                                    + (nvl(g_rec_in.Daily_Overtime_totacthrs,0) * nvl(g_rec_in.Daily_Overtime_paycatperc,0)/100)
                                    + (nvl(g_rec_in.Non_Working_Day_totacthrs,0) * nvl(g_rec_in.Non_Working_Day_paycatperc,0)/100)
                                    + (nvl(g_rec_in.Public_Holiday_totacthrs,0) * nvl(g_rec_in.Public_Holiday_paycatperc,0)/100)
                                    + (nvl(g_rec_in.Night_totacthrs,0) * nvl(g_rec_in.Night_paycatperc,0) / 100);
         g_rec_out.ACTUAL_FTE_WK := g_rec_out.ACTUAL_FTE_HRS_WK /40;
         g_rec_out.ACTUAL_COST_WK   := g_rec_out.ACTUAL_cost_HRS_WK  * nvl(g_rec_in.employee_rate,0);
      end if;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into RTL_LOC_EMP_JOB_PAYCAT_wk  values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                      ' '||a_tbl_INSERT(g_error_index).sk1_LOCATION_no||
                      ' '||a_tbl_INSERT(g_error_index).sk1_JOB_ID||
                      ' '||a_tbl_INSERT(g_error_index).fin_year_no||
                      ' '||a_tbl_INSERT(g_error_index).fin_week_no||
                       ' '||a_tbl_INSERT(g_error_index).sk1_EMPLOYEE_ID;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
     UPDATE RTL_LOC_EMP_JOB_PAYCAT_wk
              SET 
                   ACTUAL_COST_WK                = a_tbl_update(i).ACTUAL_COST_WK,
                    ACTUAL_COST_HRS_WK                = a_tbl_update(i).ACTUAL_COST_HRS_WK,
                    ACTUAL_FTE_WK                = a_tbl_update(i).ACTUAL_FTE_WK,
                    ACTUAL_FTE_HRS_WK                = a_tbl_update(i).ACTUAL_FTE_HRS_WK,
                    LAST_UPDATED_DATE         = a_tbl_update(i).LAST_UPDATED_DATE
              WHERE sk1_LOCATION_NO       = a_tbl_update(i).sk1_LOCATION_no
              AND sk1_EMPLOYEE_ID         = a_tbl_update(i).sk1_EMPLOYEE_ID
              AND sk1_JOB_ID              = a_tbl_update(i).sk1_JOB_ID
              AND fin_year_no           = a_tbl_update(i).fin_year_no
              AND fin_week_no           = a_tbl_update(i).fin_week_no;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_LOCATION_no||
                       ' '||a_tbl_update(g_error_index).sk1_JOB_ID||
                      ' '||a_tbl_update(g_error_index).fin_year_no||
                      ' '||a_tbl_update(g_error_index).fin_week_no||
                       ' '||a_tbl_update(g_error_index).sk1_EMPLOYEE_ID;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   IF G_REC_IN.RTL_EXISTS IS NOT NULL
   THEN G_COUNT := 1;
   g_found := TRUE;
   END IF;

-- Place record into array for later bulk writing
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_LOC_EMP_JOB_PAYCAT_DY  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
-- hardcoding batch_date for testing
  -- g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --**************************************************************************************************
-- Only process if any data has come through in this batch.
--**************************************************************************************************
--  g_rec_cnt := 0;
    g_rec_cnt := 1;
  -- check commented out - wlyttle - 11 may 2017 - not required anymore as APPWORX
  -- will ensure that this job only runs on a Friday morning as part of Thursday nights batch.
 -- Select count(*) into g_rec_cnt from dwh_foundation.FND_S4S_LOC_EMP_job_PAYCAT_DY where last_updated_date = g_date;
  If g_rec_cnt  > 0 then 

          execute immediate 'alter session set workarea_size_policy=manual';
          execute immediate 'alter session set sort_area_size=100000000';
          execute immediate 'alter session enable parallel dml';
      
          l_text := 'Running GATHER_TABLE_STATS ON RTL_LOC_EMP_JOB_PAYCAT_DY';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
           DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                         'RTL_LOC_EMP_JOB_PAYCAT_DY', DEGREE => 8);
      
              l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_wk';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
           EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_wk');


                    ---------------------------------------------------------------
                    --
                    -- delete process
                    --
                    ---------------------------------------------------------------
                    /*
                    BEGIN
                             delete from DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_wk
                             where (SK1_employee_id, FIN_YEAR_NO, FIN_WEEK_NO) in (SELECT
                              /*+ full(RTL) parallel(RTL,6) */
                     /*         distinct 
                                                  rtl.SK1_EMPLOYEE_ID ,
                                        case when rtl.business_date < rtl.pay_week_date 
                                              and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                                              and db.TCOE_ind = 0 
                                                      then dc.FIN_YEAR_NO
                                              else
                                                   DC2.FIN_YEAR_NO
                                        end FIN_YEAR_NO,
                                         case when rtl.business_date < rtl.pay_week_date 
                                              and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                                              and db.TCOE_ind = 0 
                                                      then dc.FIN_WEEK_NO
                                              else
                                                   DC2.FIN_WEEK_NO
                                        end FIN_WEEK_NO
                    FROM dwh_performance.rtl_LOC_EMP_job_PAYCAT_DY rtl,
                          dim_payment_category dpc,
                          dim_s4s_business_unit db,
                          dim_job dj,
                          dim_calendar dc,
                          DIM_CALENDAR DC2
                    WHERE rtl.sk1_payment_category_NO = Dpc.sk1_payment_category_NO
                      and dj.sk1_s4s_business_unit_no = db.sk1_s4s_business_unit_no
                      and dj.sk1_job_id = rtl.sk1_job_id
                      and dc.calendar_date = rtl.business_date
                     -- AND SK1_EMPLOYEE_ID = 206438
                      AND RTL.LAST_UPDATED_DATE = G_DATE);
                        
                       
                         
                              g_recs :=SQL%ROWCOUNT ;
                              COMMIT;
                              g_recs_deleted := g_recs;
                                    
                          l_text := 'Deleted from DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_WK recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
                          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                      exception
                             when no_data_found then
                                    l_text := 'No deletions done for DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_WK ';
                          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                       end;          
                    
                       g_recs_inserted  :=0;
                       g_recs_deleted := 0;
                       */
                    --**************************************************************************************************
                    -- Bulk fetch loop controlling main program execution
                    --**************************************************************************************************
                        open c_fnd_LOCATION;
                        fetch c_fnd_LOCATION bulk collect into a_stg_input limit g_forall_limit;
                        while a_stg_input.count > 0
                        loop
                          for i in 1 .. a_stg_input.count
                          loop
                             g_recs_read := g_recs_read + 1;
                             if g_recs_read mod 100000 = 0 then
                                l_text := dwh_constants.vc_log_records_processed||
                                to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
                                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                             end if;
                    
                             g_rec_in                := a_stg_input(i);
                    
                            local_address_variables;
                            local_write_output;
                    
                          end loop;
                        fetch c_fnd_LOCATION bulk collect into a_stg_input limit g_forall_limit;
                        end loop;
                        close c_fnd_LOCATION;
                    
                    --**************************************************************************************************
                    -- At end write out what remains in the arrays at end of program
                    --**************************************************************************************************
                       local_bulk_insert;
                       local_bulk_update;
       end if;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
       raise;




END WH_PRF_S4S_044U_OLD;
