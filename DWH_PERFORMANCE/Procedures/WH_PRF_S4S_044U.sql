--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_044U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_044U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        22 Feb 2019
--  Author:      Shuaib Salie ; Lisa Kriel
--  Purpose:     Load payment_category_WEEK information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - DWH_PERFORMANCE.RTL_LOC_EMP_JOB_PAYCAT_DY
--               Output   - DWH_PERFORMANCE.rtl_loc_emp_job_paycat_wk
--  Packages:    dwh_constants, dwh_log, dwh_valid, dwh_s4s
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------   
--                 Standard cursor bulk insert/update due to complicated calculations done.
--                 Process all records in daily table
--
--               Delete process :
--               ----------------   
--                 None
--
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
g_rec_out            RTL_LOC_EMP_JOB_PAYCAT_WK%rowtype;

g_rec_cnt            number        := 0;

g_total                 number(14,2) :=  0;
g_total_plus_training   number(14,2) :=  0;

g_paid_leave     number(14,2) :=  0;
g_ex_paid_leave  number(14,2) :=  0;

g_total_plus_paid_leave number(14,2) :=  0;

g_date               date         ;
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
g_part_inserted   number          :=0;

g_found              boolean;
g_new_partition      boolean;
g_loop_fin_year_no   number        :=  0;
g_loop_fin_week_no   number        :=  0;
g_sub                integer       :=  0;
g_loop_cnt           integer       :=  30; -- Number of partitions to be truncated/replaced   !!!TODO 30

g_loop_start_date       date;

g_subpart_type       dba_part_tables.SUBPARTITIONING_TYPE%type; 
g_subpart_column_name dba_subpart_key_columns.column_name%type;

l_message            sys_dwh_errlog.log_text%type;
l_procedure_name     sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_044U';
l_table_name         all_tables.table_name%type                := 'RTL_LOC_EMP_JOB_PAYCAT_WK';
l_table_owner        all_tables.owner%type                     := 'DWH_PERFORMANCE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE '||l_table_name||'  data EX PERFORMANCE DY';
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
WITH selext1 AS (
        SELECT /*+ full(rtl) parallel(rtl,8) */
 --          distinct 
               rtl.pay_week_date,
               case when rtl.business_date < rtl.pay_week_date 
                    and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                    and db.tcoe_ind = 0 
                  then dc.this_week_start_date
                 else rtl.pay_week_date
               end derived_pay_week_date,
               rtl.business_date,
               rtl.sk1_location_no,
               rtl.sk1_employee_id,
               rtl.fin_week_no,
               rtl.fin_year_no,
               dpc.payment_category_no ,
               rtl.sk1_job_id,
               db.tcoe_ind,
               dpc.payment_category_percent,
               case when dpc.payment_category_no = 1000251 and rtl.business_date <> rtl.pay_week_date then 0
                 else  
                    case when rtl.business_date < rtl.pay_week_date 
                          and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                          and db.tcoe_ind = 0 
                    then 0
                    else NVL(ACTUAL_HRS,0) 
                    end 
               end actual_hrs,
               case when rtl.business_date < rtl.pay_week_date 
                     and dpc.payment_category_no in (1000209,1000211,1000212,1000213,1000214,1000215,1000216,1000210,1000217,1000224,1000218) 
                     and db.tcoe_ind = 0 
                  then NVL(ACTUAL_HRS,0) 
                  else 0
               end ex_actual_hours
          FROM dwh_performance.RTL_LOC_EMP_JOB_PAYCAT_DY rtl
              ,dim_payment_category dpc
              ,dim_s4s_business_unit db
              ,dim_job dj
              ,dim_calendar dc
         WHERE rtl.sk1_payment_category_NO = Dpc.sk1_payment_category_NO
           and dj.sk1_s4s_business_unit_no = db.sk1_s4s_business_unit_no
           and dj.sk1_job_id = rtl.sk1_job_id
           and rtl.business_date between dj.sk1_effective_from_date  and dj.sk1_effective_to_date 
           and rtl.business_date between db.sk1_effective_from_date  and db.sk1_effective_to_date 
          and dc.calendar_date = rtl.business_date         
           -- and dc.calendar_date = rtl.pay_week_date         
--           and rtl.fin_year_no = g_loop_fin_year_no
--           and rtl.fin_week_no = g_loop_fin_week_no
           and rtl.pay_week_date between g_loop_start_date and g_date
        ),
     selext2 as (
        SELECT 
               fin_year_no,
               fin_week_no,
               sk1_location_no,
               sk1_employee_id,
               payment_category_no,
               sk1_job_id,
               TCOE_ind,
               payment_category_percent,
               sum(nvl( actual_hrs,0)) total_actual_hrs,
               sum(nvl(ex_actual_hours,0)) total_ex_actual_hrs
          FROM selext1 se1
            --  ,dim_calendar dc
         --WHERE se1.pay_week_date = dc.calendar_date
--           and se1.fin_year_no = g_loop_fin_year_no
--           and se1.fin_week_no = g_loop_fin_week_no
where se1.pay_week_date  between g_loop_start_date and g_date
         group by fin_year_no,
                  fin_week_no,
                  sk1_location_no ,
                  sk1_employee_id ,
                  payment_category_no,
                  sk1_job_id,
                  TCOE_ind,
                  payment_category_percent
        ),
     selext3 as (
        select * from selext2 
           pivot(
                 SUM(NVL(total_ACTUAL_HRS,0))    as totacthrs
                ,SUM(NVL(total_ex_actual_hrs,0)) as totexacthrs
                ,min(payment_category_percent)   as paycatperc
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
                    ,1000254	Weekly_Overtime)
                )
        )
        SELECT              
                distinct se3.*,
                rc.STRICT_MIN_HRS_PER_WK,
                employee_rate,
                flr.sk1_employee_id rtl_exists
          FROM SELEXT3 se3
          left outer join dwh_performance.rtl_emp_constr_loc_job_wk rc
                  on rc.sk1_employee_id = se3.sk1_employee_id
                 and rc.fin_year_no     = se3.fin_year_no
                 and rc.fin_week_no     = se3.fin_week_no
          left outer join dwh_performance.rtl_loc_emp_job_paycat_wk flr
                  on flr.sk1_location_no = se3.sk1_location_no
                 and flr.sk1_employee_id = se3.sk1_employee_id
                 and flr.sk1_job_id      = se3.sk1_job_id
                 and flr.fin_year_no     = se3.fin_year_no
                 and flr.fin_week_no     = se3.fin_week_no
          join dwh_performance.rtl_emp_job_wk ej
            on ej.sk1_employee_id = se3.sk1_employee_id
           and ej.sk1_job_id      = se3.sk1_job_id
           and ej.fin_year_no     = se3.fin_year_no
           and ej.fin_week_no     = se3.fin_week_no
--        WHERE TCOE_IND = 1
--     union
--        SELECT              
--                se3.*,
--                rc.STRICT_MIN_HRS_PER_WK,
--                employee_rate,
--                flr.sk1_employee_id rtl_exists
--          FROM SELEXT3 se3
--          join dwh_performance.rtl_emp_constr_loc_job_wk rc
--            on rc.sk1_employee_id = se3.sk1_employee_id
--           and rc.fin_year_no = se3.fin_year_no
--           and rc.FIN_WEEK_NO = se3.fin_week_no
--          left outer join dwh_performance.rtl_loc_emp_job_paycat_wk flr
--                  on flr.sk1_location_no = se3.sk1_location_no
--                 and flr.sk1_employee_id = se3.sk1_employee_id
--                 and flr.sk1_job_id = se3.sk1_job_id
--                 and flr.fin_year_no = se3.fin_year_no
--                 and flr.fin_week_no = se3.fin_week_no
--          join dwh_performance.rtl_emp_job_wk ej
--            on ej.sk1_employee_id = se3.sk1_employee_id
--           and ej.sk1_job_id = se3.sk1_job_id
--           and ej.fin_year_no = se3.fin_year_no
--           and ej.fin_week_no = se3.fin_week_no
--         WHERE TCOE_IND = 0                          
     ORDER by se3.fin_year_no, se3.fin_week_no, TCOE_IND;

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
                         + (nvl(g_rec_in.Suspension_totexacthrs,0) * nvl(g_rec_in.Suspension_paycatperc,0)/100)                         ;

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
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
--dbms_output.put_line('Insert');
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_loc_emp_job_paycat_wk  values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
    g_part_inserted := g_part_inserted + a_tbl_insert.count;
--    L_TEXT := L_table_name||' : inserts = '|| a_tbl_insert.count ||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := 'Insert '|| dwh_constants.vc_err_lb_loop||i||
                      ' '||g_error_index||
                      ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                      ' '||a_tbl_INSERT(g_error_index).sk1_LOCATION_no||
                      ' '||a_tbl_INSERT(g_error_index).sk1_JOB_ID||
                      ' '||a_tbl_INSERT(g_error_index).fin_year_no||
                      ' '||a_tbl_INSERT(g_error_index).fin_week_no||
                      ' '||a_tbl_INSERT(g_error_index).sk1_EMPLOYEE_ID;
          dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;
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
   if a_count_i = 0 THEN
      g_loop_fin_year_no :=  g_rec_in.fin_year_no;
      g_loop_fin_week_no :=  g_rec_in.fin_week_no;
   END IF;
   IF (g_rec_in.fin_year_no = g_loop_fin_year_no
       and g_rec_in.fin_week_no = g_loop_fin_week_no) THEN 
       g_new_partition := false;
   else
       g_new_partition := true;
   end if;
-- Place record into array for later bulk writing
   if not g_found then    
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit or g_new_partition then
      local_bulk_insert;     
      if g_new_partition then
          L_TEXT := L_table_name||' : inserts = '|| g_part_inserted ||' for Fin '||g_loop_fin_year_no||'w'||g_loop_fin_week_no;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         g_part_inserted :=0;
      end if;
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
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
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
    dwh_performance.dwh_s4s.write_initial_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
  dwh_lookup.dim_control(g_date);
-- hardcoding batch_date for testing
  -- g_date := trunc(sysdate);

  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --**************************************************************************************************
-- Only process if any data has come through in this batch.
--**************************************************************************************************
  g_rec_cnt := 0;
  --Select count(*) into g_rec_cnt from dwh_foundation.FND_S4S_LOC_EMP_job_PAYCAT_DY where last_updated_date >= g_date;
  Select count(*) into g_rec_cnt from dwh_foundation.FND_S4S_LOC_EMP_job_PAYCAT_DY where last_updated_date >= g_date-7;
  l_text := 'Records for '||g_date||':- '||g_rec_cnt;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  If g_rec_cnt  > 0 then 

       --**************************************************************************************************
      -- Prepare environment
      --**************************************************************************************************
      EXECUTE immediate 'alter session enable parallel dml';
      execute immediate 'alter session set nls_date_format="dd-mm-yyyy hh24:mi:ss"';

      --**************************************************************************************************
      -- Disabling of FK constraints
      --**************************************************************************************************
      DWH_PERFORMANCE.DWH_S4S.disable_foreign_keys (l_table_name, L_table_owner);

--*************************************************************************************************
-- Remove existing data by truncating partitions
--*************************************************************************************************        

    begin

       for g_sub in 0 .. g_loop_cnt-1
         loop 
           select distinct this_week_start_date, fin_year_no, fin_week_no
           into   g_loop_start_date, g_loop_fin_year_no, g_loop_fin_week_no
           from   dwh_performance.dim_calendar
           where  calendar_date = (g_date) - (g_sub * 7);  

           -- truncate subpartition
           DWH_PERFORMANCE.DWH_S4S.remove_subpartition_of_year (l_name,l_system_name,l_script_name,l_procedure_name,
                                                l_table_name, l_table_owner,G_LOOP_FIN_YEAR_NO, G_LOOP_FIN_WEEK_NO);
       end loop;   
       
    end;   
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

   end if;
 --**************************************************************************************************
  -- Enabling of FK constraints Novalidate
  --**************************************************************************************************
   DWH_PERFORMANCE.DWH_S4S.enable_foreign_keys  (l_table_name, L_table_owner, true);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    DWH_PERFORMANCE.DWH_S4S.write_final_log_data(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                                 l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_procedure_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_S4S_044U;
