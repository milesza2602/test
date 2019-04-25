--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_005U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_005U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Update employee_job_day information for Scheduling for Staff(S4S)
--               Note that the location_no is not correct - hence left out of rollup into foundation.
--
--
--  Tables:      AIT load - STG_S4S_EMP_JOB_dy
--               Input    - STG_S4S_EMP_JOB_dy_cpy
--               Output   - FND_S4S_EMP_JOB
--  Packages:    dwh_constants, dwh_log, dwh_valid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------   
--                 Standard insert/update, and write to hospital of invalid records.
--
--               Delete process :
--               ----------------   
--                 The records are written to an audit table before they are deleted from FOUNDATION.
--                 Any changes at source result in the employee's full history being sent.
--                          and therefore we have to delete the current records for the employee (based upon employee_id only)
--                              the current data and load the new data.
--                 The audit table = dwh_foundation.FND_S4S_EMP_JOB_del_list  -- NOT used anywhere else.
--
--                Validation :
--                ------------
--                  Normal validation done on dimension data.
--                  We should not be sent any records for an employee where the dates overlap for the job periods.
--                  The job_start_date can be any day but the job_end_date will be sent as the job_start_date of the next period 
--                   This all depends on the derivation criteria.
--                  eg. RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015'
--                      RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : job_start_date = '1 jan 2015'  job_end_date = '12 january 2015' 
--                            RECORD 2 : job_start_date = '12 jan 2015'  job_end_date = NULL
--
--                Multiple batches :
--                -----------------
--                  Regardless of how many batches are sent, we derive the latest sys_source_batch_id for an employee_id 
--                  and then process the latest sys_source_sequence_no for the employee_id,  job_start_date.
--                 
--
--                Data Takeon option :
--                -----------------
--                  The code in this procedure can be changed to allow for a data takeon.
--                    ie. TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB_DEL_LIST
--                        TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB
--                        TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_JOB_DY_HSP
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--
--  Maintenance:
--   28 NOV 2014 -  W LYTTLE - add in processing for multiple batches
--   22 jan 2015 - w lyttle - coding added for open_ended processing when no update received :
--                 1. Records need to be appended to the 'delete list table' where the 'end_date' is null.
--                     This will ensure that from this point forward the performance records for these will be deleted and reloaded.
--                 2. The foundation fact table will have LAST_UPDATED_DATE =batch_date where any records have a null 'end_date'.
--                     This is to ensure that from this point onwards, forecasting will be applied.
--  4 march 2015 w lyttle - added data takeon option
--
--  19 Sept 2018 S Salie - Remove start date's reference to dim_calender as this table only stores data 
--                         as far back as Woolworths Financial Year 2000.   

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      DWH_FOUNDATION.STG_S4S_EMP_JOB_dy_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_S4S_EMP_JOB%rowtype;

g_JOB_end_date       date ;
g_found              boolean;
g_valid              boolean;
g_data_takeon        varchar2(1);
--g_date             date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_005U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE employee_job_day data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_S4S_EMP_JOB%rowtype index by binary_integer;
type tbl_array_u is table of FND_S4S_EMP_JOB%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_S4S_EMP_JOB_dy_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_S4S_EMP_JOB_dy_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_S4S_EMP_JOB is
WITH 
 selbat AS
  (SELECT 
       MAX(sys_source_batch_id) maxbat,
       employee_id
   FROM dwh_foundation.STG_S4S_EMP_JOB_DY_cpy
   GROUP BY employee_id
   )
   ,
   selseq AS
   (SELECT 
        MAX(sys_source_sequence_no) maxseq,
        maxbat,
        STG.EMPLOYEE_ID ,
        STG.job_START_DATE 
    FROM selbat sb,
         dwh_foundation.STG_S4S_EMP_JOB_DY_cpy stg
    WHERE stg.EMPLOYEE_ID = sb.employee_id
      AND SB.MAXBAT       = STG.sys_source_batch_id
  GROUP BY maxbat,
           STG.EMPLOYEE_ID ,
           STG.job_START_DATE
    )  ,
  selall AS
  (SELECT 
      SYS_SOURCE_BATCH_ID,
      sys_source_sequence_no,
      STG.EMPLOYEE_ID ,
      STG.job_START_DATE ,
      STG.job_end_DATE 
  FROM selseq ss,
       dwh_foundation.STG_S4S_EMP_JOB_DY_cpy stg
  WHERE stg.EMPLOYEE_ID    = sS.employee_id
    AND stg.job_START_DATE = sS.job_START_DATE
    AND SS.MAXBAT          = STG.sys_source_batch_id
    AND SS.MAXSEQ          = STG.sys_source_sequence_no
  GROUP BY SYS_SOURCE_BATCH_ID,
           sys_source_sequence_no,
           STG.EMPLOYEE_ID ,
           STG.job_START_DATE ,
           STG.job_end_DATE 
  )

   select stg.SYS_SOURCE_BATCH_ID,
          stg.SYS_SOURCE_SEQUENCE_NO,
          stg.SYS_LOAD_DATE,
          stg.SYS_PROCESS_CODE,
          stg.SYS_LOAD_SYSTEM_NAME,
          stg.SYS_MIDDLEWARE_BATCH_ID,
          stg.SYS_PROCESS_MSG,
          STG.SOURCE_DATA_STATUS_CODE,
          STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
          STG.JOB_ID stg_JOB_ID,
          STG.JOB_START_DATE stg_JOB_START_DATE,
          STG.JOB_END_DATE stg_JOB_END_DATE,
          STG.EMPLOYEE_RATE  stg_EMPLOYEE_RATE ,
          STG.PAYPOLICY_ID stg_PAYPOLICY_ID,
          fe.EMPloyee_id fe_EMPLOYEE_ID,
          fJ.JOB_id fJ_JOB_ID,
          -- dc2.calendar_date dc2_JOB_START_DATE, SS - remove start date reference to dim_calendar table
          --STG.JOB_START_DATE dc2_JOB_START_DATE, --SS   -- take from DWH_FOUNDATION.STG_S4S_EMP_JOB_dy_cpy 
          fp.PAYPOLICY_ID fp_PAYPOLICY_ID    
   from DWH_FOUNDATION.STG_S4S_EMP_JOB_dy_cpy stg,
        DWH_HR_performance.DIM_EMPLOYEE fe,
        fnd_S4S_job fJ,
        --dim_calendar dc2,
        FND_S4S_PAY_POLICY fp, 
        SELALL SA
   where stg.EMPLOYEE_ID = fe.employee_id(+)
     and stg.job_id = fj.job_id(+)
     --and stg.JOB_START_DATE = dc2.calendar_date(+)
     and stg.PAYPOLICY_ID   = fp.PAYPOLICY_ID(+)
     AND stg.EMPLOYEE_ID    = sa.employee_id
     AND stg.JOB_START_DATE = sa.JOB_START_DATE
     ----------------------------------------------------------------------
     -- Comments By Wendy Lyttle ...
     ---   The coding was ....AND stg.JOB_END_DATE = sa.JOB_END_DATE
     --    but had to change to an arbitrary value when doing the join
     --     as it won't join a null value to a null value 
     --     which is quite a valid scenario for this extract
     ---------------------------------------------------------------------
     AND nvl(stg.JOB_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy')) = nvl(sa.JOB_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy'))
     AND stg.SYS_SOURCE_BATCH_ID    = sa.SYS_SOURCE_BATCH_ID 
     AND stg.sys_source_sequence_no = sa.sys_source_sequence_no
   order by sys_source_batch_id,sys_source_sequence_no;

g_rec_in                   c_STG_S4S_EMP_JOB%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_S4S_EMP_JOB%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Delete records from Foundation
-- based on employee_id  as we will get full history for an employee each time from source
--**************************************************************************************************
procedure delete_fnd as
begin

      g_recs_inserted := 0;

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_EMP_JOB_del_list;


      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;

      g_run_date := trunc(sysdate);
      BEGIN
           insert /*+ append */ into dwh_foundation.FND_S4S_EMP_JOB_del_list
           with selstg
                  as (select distinct employee_id from STG_S4S_EMP_JOB_dy_cpy)
           select g_run_date, g_date, g_run_seq_no, f.*
              from DWH_FOUNDATION.FND_S4S_EMP_JOB f, selstg s
              where f.employee_id = s.employee_id;

                g_recs :=SQL%ROWCOUNT ;
                COMMIT;
                g_recs_inserted := g_recs;

                l_text := 'Insert into FND_S4S_EMP_JOB_del_list recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

               delete from DWH_FOUNDATION.FND_S4S_EMP_JOB
               where employee_id IN  (select distinct employee_id from dwh_foundation.STG_S4S_EMP_JOB_dy_cpy);

                g_recs :=SQL%ROWCOUNT ;
                COMMIT;
                g_recs_deleted := g_recs;

            l_text := 'Deleted from DWH_FOUNDATION.FND_S4S_EMP_JOB recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
          exception
               when no_data_found then
                      l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_EMP_JOB ';
            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
         end;          

         g_recs_inserted  :=0;
         g_recs_deleted := 0;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_fnd;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital               := 'N';
   g_rec_out.EMPLOYEE_ID    :=  g_rec_in.stg_EMPLOYEE_ID;
   g_rec_out.JOB_ID         :=  g_rec_in.STG_JOB_ID;
   g_rec_out.JOB_START_DATE :=  g_rec_in.STG_JOB_START_DATE;
   g_rec_out.JOB_END_DATE   :=  g_rec_in.STG_JOB_END_DATE;
   g_rec_out.EMPLOYEE_RATE  :=  g_rec_in.STG_EMPLOYEE_RATE ;
   g_rec_out.PAYPOLICY_ID   :=  g_rec_in.STG_PAYPOLICY_ID;

   g_rec_out.last_updated_date               := g_date;

   if G_REC_IN.fe_EMPLOYEE_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'EMPLOYEE_ID NOT FOUND';
     return;
   end if;

   --   if G_REC_IN.dc2_JOB_START_DATE IS NULL then
   --     g_hospital      := 'Y';
   --     g_hospital_text := 'JOB_START_DATE NOT FOUND';
   --     return;
   --   end if;

   if G_REC_IN.STG_JOB_END_DATE IS NOT NULL
      AND  G_REC_IN.STG_JOB_END_DATE < G_REC_IN.STG_JOB_START_DATE then
     g_hospital      := 'Y';
     g_hospital_text := 'JOB_END_DATE < JOB_START_DATE';
     return;
   end if;

   if G_REC_IN.fj_job_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'JOB_ID NOT FOUND';
     return;
   end if;

   if G_REC_IN.fp_paypolicy_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'PAYPOLICY_ID NOT FOUND';
     return;
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into dwh_foundation.STG_S4S_EMP_JOB_dy_hsp values
       (  g_rec_in.SYS_SOURCE_BATCH_ID
         ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
         ,g_rec_in.SYS_LOAD_DATE
         ,g_rec_in.SYS_PROCESS_CODE
         ,g_rec_in.SYS_LOAD_SYSTEM_NAME
         ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
         ,g_rec_in.SYS_PROCESS_MSG
         ,g_rec_in.SOURCE_DATA_STATUS_CODE
         ,g_rec_in.stg_EMPLOYEE_ID
         ,g_rec_in.STG_JOB_id
         ,g_rec_in.STG_JOB_START_DATE
         ,g_rec_in.STG_JOB_END_DATE
         ,g_rec_in.STG_EMPLOYEE_RATE
         ,g_rec_in.STG_PAYPOLICY_ID
       );

   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into FND_S4S_EMP_JOB  values a_tbl_insert(i);

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
                      ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID||
                      ' '||a_tbl_insert(g_error_index).job_ID||
                      ' '||a_tbl_insert(g_error_index).JOB_START_DATE;

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
       update FND_S4S_EMP_JOB
        set JOB_END_DATE      =  a_tbl_update(i).JOB_END_DATE,
            EMPLOYEE_RATE     =  a_tbl_update(i).EMPLOYEE_RATE ,
            PAYPOLICY_ID      =  a_tbl_update(i).PAYPOLICY_ID,
            LAST_UPDATED_DATE =  a_tbl_update(i).LAST_UPDATED_DATE
       where EMPLOYEE_ID     =  a_tbl_update(i).EMPLOYEE_ID
         AND JOB_ID          =  a_tbl_update(i).JOB_ID
         AND JOB_START_DATE  =  a_tbl_update(i).JOB_START_DATE;

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
                        ' '||a_tbl_update(g_error_index).EMPLOYEE_ID||
                        ' '||a_tbl_update(g_error_index).JOB_ID||
                        ' '||a_tbl_update(g_error_index).JOB_START_DATE;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

v_count integer := 0;

begin

   g_found := false;

   select count(1)
    into   v_count
   from   DWH_FOUNDATION.FND_S4S_EMP_JOB
   where  EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
     AND JOB_ID =  g_rec_out.JOB_ID
     AND JOB_START_DATE =  g_rec_out.JOB_START_DATE;

   if v_count = 1 then
      g_found := TRUE;
   end if;

   -- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if   a_tbl_insert(i).EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
           AND a_tbl_insert(i).JOB_ID =  g_rec_out.JOB_ID
           AND a_tbl_insert(i).JOB_START_DATE =  g_rec_out.JOB_START_DATE
           then
            g_found := TRUE;
         end if;
      end loop;
   end if;

   -- Place data into and array for later writing to table in bulk
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
   --if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;

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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD THE LOC_EMP_JOB data ex S4S STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    -- hardcoding batch_date for testing
 --   g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    SELECT 
       distinct THIS_WEEK_END_DATE into G_JOB_end_date
    FROM DIM_CALENDAR 
    WHERE CALENDAR_DATE = g_date + 20;

    l_text := 'G_JOB_end_date= '||G_JOB_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Delete Process *** set g_data_takeon indicator
--**************************************************************************************************

  G_DATA_TAKEON := 'N';

  l_text := 'Data takeon = '||G_DATA_TAKEON;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   If g_data_takeon <> 'Y' OR g_data_takeon IS NULL
   THEN 
        delete_fnd;
   ELSE
        L_Text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB_DEL_LIST';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB_DEL_LIST');
        L_Text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_JOB');
        L_Text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_JOB';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Dbms_Stats.Gather_Table_Stats ('DWH_FOUNDATION',
                                       'FND_S4S_EMP_JOB', Degree => 8);
        L_Text := 'TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_JOB_DY_HSP';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_JOB_DY_HSP');
   END IF;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_S4S_EMP_JOB;
    fetch c_STG_S4S_EMP_JOB bulk collect into a_stg_input limit g_forall_limit;
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
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_STG_S4S_EMP_JOB bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_EMP_JOB;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_JOB';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_JOB', DEGREE => 8);

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

END WH_FND_S4S_005U;
