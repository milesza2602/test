--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_168U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_168U_OLD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        February 2013
--  Author:      Quentin Smit
--  Purpose:     Create FND_JDAFF_ST_PLAN_ANALYSIS_DY table in the foundation layer
--               with input ex staging table from JDA.
--  Tables:      Input  - stg_jda_st_plan_anlysis_dy_cpy
--               Output - dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

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
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_21d_bck_start_date  date;
g_21d_fwd_end_date    date;

g_sub                integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_168U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STORE PLANNING ANALYSIS DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
   l_text := 'LOAD OF FND_JDAFF_ST_PLAN_ANALYSIS_DY STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   --g_date := '11/JAN/15';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   SELECT min(trading_date), max(trading_date)
   into g_21d_bck_start_date, g_21d_fwd_end_date
   FROM stg_jda_st_plan_anlysis_dy_cpy;

   l_text := 'MIN TRADING DATE ON STAGING - '||g_21d_bck_start_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'MAX TRADING DATE ON STAGING - '||g_21d_fwd_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

   l_text := 'Running GATHER_TABLE_STATS ON FND_JDAFF_ST_PLAN_ANALYSIS_DY - Needed due to the truncate that preceeded this job';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_JDAFF_ST_PLAN_ANALYSIS_DY', DEGREE => 32);

   l_text := 'First GATHER_TABLE_STATS ON FND_JDAFF_ST_PLAN_ANALYSIS_DY completed';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Processing data ..';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   INSERT /*+ APPEND parallel (jdast,4) */
     INTO  dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_DY jdast
   SELECT /*+ full(stpln) parallel(stpln,4) */
          stpln.ITEM_NO,
          stpln.LOCATION_NO,
          stpln.TRADING_DATE,
          stpln.POST_DATE,
          stpln.TOTAL_DEMAND_UNIT,
          stpln.INVENTORY_UNIT,
          stpln.PLANNED_ARRIVALS_UNIT,
          stpln.PLANNED_ARRIVALS_CASE,
          stpln.REC_ARRIVAL_UNIT,
          stpln.REC_ARRIVAL_CASE,
          stpln.IN_TRANSIT_UNIT,
          stpln.IN_TRANSIT_CASE,
          stpln.CONSTRAINT_POH_UNIT,
          stpln.safety_stock_unit,
          stpln.CONSTRAINED_EXPIRED_STOCK,
          stpln.constr_store_cover_day,
           g_date AS last_updated_date,
  -- SS Added 7 new columns 07/may/2015
          stpln.ALT_CONSTRAINT_UNUSED_SOH_UNIT,
          stpln.ALT_CONSTRAINT_POH_UNIT,
          stpln.CONSTRAINT_UNMET_DEMAND_UNIT,
          stpln.CONSTRAINT_UNUSED_SOH_UNIT,
          stpln.EXPIRED_SOH_UNIT,
          stpln.IGNORED_DEMAND_UNIT,
          stpln.projected_stock_available_unit
     FROM
            dwh_foundation.stg_jda_st_plan_anlysis_dy_cpy stpln,
            dwh_foundation.fnd_location fl,
            dwh_foundation.fnd_item fi
    WHERE stpln.item_no = fi.item_no AND stpln.location_no = fl.location_no
      --and stpln.sys_process_code = 'N'

       ;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

   l_text := 'Processing completed ..';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Running GATHER_TABLE_STATS ON FND_JDAFF_ST_PLAN_ANALYSIS_DY';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_JDAFF_ST_PLAN_ANALYSIS_DY', DEGREE => 32);

   l_text := 'Second GATHER_TABLE_STATS ON FND_JDAFF_ST_PLAN_ANALYSIS_DY completed';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --  Update process code on the staging table - this method was used due to the volume of records to be updated
    --  ***********************************************************************************************************
    --    drop index on sys_process_code
    --    update sys_process_code
    --    create index in parallel with  nologging
    --    set index to noparallel / logging

    --l_text := 'Updating sys_process_code..';
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --execute immediate 'DROP INDEX "DWH_FOUNDATION"."BS_S_JDF_ST_PLN_ANL_DC"';

    --update /*+ parallel (t,16) full(t)  */ dwh_foundation.STG_JDA_ST_PLAN_ANLYSIS_DY_CPY t
    --set t.sys_process_code = 'Y' where t.sys_process_code = 'N';

    --commit;

    --execute immediate 'CREATE INDEX "DWH_FOUNDATION"."BS_S_JDF_ST_PLN_ANL_DC" ON "DWH_FOUNDATION"."STG_JDA_ST_PLAN_ANLYSIS_DY_CPY" ("SYS_PROCESS_CODE")
    --                  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS PARALLEL 8 NOLOGGING
    --                  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
    --                  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
    --                  TABLESPACE "FND_MASTER"' ;

    --execute immediate 'alter index "DWH_FOUNDATION"."BS_S_JDF_ST_PLN_ANL_DC" NOPARALLEL LOGGING';

    --l_text := 'DONE Updating sys_process_code..';
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end wh_fnd_corp_168u_old;
