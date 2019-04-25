--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_050U_BATSEQ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_050U_BATSEQ" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     LOAD Employee job schedule information for Scheduling for Staff(S4S)
--
--               Delete process :
--                 Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and trunc(shift_clock_in)
--
--                The delete lists are used in the rollups as well.
--                ie. FND_S4S_SCHLOCEMPJBDY_del_list
--
--
  --  Tables:      AIT load - STG_S4S_EMP_LOC_JOB_SHED
  --               Input    - STG_S4S_EMP_LOC_JOB_SHED_CPY
  --               Output   - FND_S4S_SCH_LOC_EMP_JB_DY
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := 10000;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_hospital_text DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_hsp.sys_process_msg%type;
  g_rec_out DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY%rowtype;
  g_found BOOLEAN;
  g_valid BOOLEAN;
g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_S4S_050U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD Employee job schedule data ex S4S';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF FND_S4S_SCH_LOC_EMP_JB_DY%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF FND_S4S_SCH_LOC_EMP_JB_DY%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  a_count_u INTEGER := 0;
  -- For arrays used to update the staging table process_code --
type staging_array1
IS
  TABLE OF STG_S4S_EMP_LOC_JOB_SHED_cpy.sys_source_batch_id%type INDEX BY binary_integer;
type staging_array2
IS
  TABLE OF STG_S4S_EMP_LOC_JOB_SHED_cpy.sys_source_sequence_no%type INDEX BY binary_integer;
  a_staging1 staging_array1;
  a_staging2 staging_array2;
  a_empty_set_s1 staging_array1;
  a_empty_set_s2 staging_array2;
  a_count_stg INTEGER := 0;
 CURSOR c_STG_S4S_EMP_LOC_JOB_SHED
  IS
  WITH  
 selbat AS
  (SELECT MAX(sys_source_batch_id) maxbat,
  employee_id,location_no,
    trunc(SHIFT_CLOCK_IN) trunc_SHIFT_CLOCK_IN
  FROM dwh_foundation.STG_S4S_EMP_LOC_JOB_SHED_cpy
  GROUP BY employee_id,location_no,
    trunc(SHIFT_CLOCK_IN)
  )
  ,
  selseq AS
  (
  SELECT MAX(sys_source_sequence_no) maxseq,
  maxbat,
    STG.EMPLOYEE_ID ,stg.location_no,
    trunc(stg.SHIFT_CLOCK_IN) trunc_SHIFT_CLOCK_IN
  FROM selbat sb,
    dwh_foundation.STG_S4S_EMP_LOC_JOB_SHED_cpy stg
  WHERE stg.EMPLOYEE_ID           = sb.employee_id
  AND trunc(stg.SHIFT_CLOCK_IN) = trunc(sb.trunc_SHIFT_CLOCK_IN)
  AND stg.location_no = sb.location_no
  AND SB.MAXBAT = STG.sys_source_batch_id
  GROUP BY maxbat,
    STG.EMPLOYEE_ID ,stg.location_no ,
   trunc(stg.SHIFT_CLOCK_IN)
  )
   ,
  selall AS
  (
  SELECT distinct SYS_SOURCE_BATCH_ID,
  sys_source_sequence_no,
    STG.EMPLOYEE_ID ,stg.location_no,
    stg.SHIFT_CLOCK_IN
  FROM selseq ss,
    dwh_foundation.STG_S4S_EMP_LOC_JOB_SHED_cpy stg
  WHERE stg.EMPLOYEE_ID           = sS.employee_id
  AND stg.location_no = sS.location_no
  AND trunc(stg.SHIFT_CLOCK_IN) = ss.trunc_SHIFT_CLOCK_IN
   AND SS.MAXBAT = STG.sys_source_batch_id
   AND SS.MAXSEQ = STG.sys_source_sequence_no
  )
    SELECT STG.SYS_SOURCE_BATCH_ID,
            STG.SYS_SOURCE_SEQUENCE_NO,
            STG.SYS_LOAD_DATE,
            STG.SYS_PROCESS_CODE,
            STG.SYS_LOAD_SYSTEM_NAME,
            STG.SYS_MIDDLEWARE_BATCH_ID,
            STG.SOURCE_DATA_STATUS_CODE,
            SYS_PROCESS_MSG,
            STG.LOCATION_no stg_LOCATION_NO,
            STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
            STG.JOB_ID stg_JOB_ID,
            STG.SHIFT_CLOCK_IN stg_SHIFT_CLOCK_IN,
            STG.SHIFT_CLOCK_OUT stg_SHIFT_CLOCK_OUT,
            STG.MEAL_BREAK_MINUTES stg_MEAL_BREAK_MINUTES,
            STG.TEA_BREAK_MINUTES stg_TEA_BREAK_MINUTES,
            fl.LOCATION_NO fl_LOCATION_NO,
            fe.EMPloyee_id fe_EMPLOYEE_ID,
            fJ.JOB_ID fJ_JOB_ID
    FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_cpy stg,
      fnd_location fl,
      DWH_HR_PERFORMANCE.DIM_EMPLOYEE fe,
      fnd_S4S_job fJ,
      selall sa
    WHERE stg.location_NO      = fl.location_no(+)
    AND stg.EMPLOYEE_ID        = fe.employee_id(+)
    AND stg.JOB_ID             = fj.JOB_ID(+)
    and stg.location_no = sa.location_no
    and stg.employee_id = sa.employee_id
    and stg.shift_clock_in = sa.shift_clock_in
      --   AND SYS_SOURCE_BATCH_ID = 193
    ORDER BY sys_source_batch_id,
      sys_source_sequence_no;
  g_rec_in c_STG_S4S_EMP_LOC_JOB_SHED%rowtype;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF c_STG_S4S_EMP_LOC_JOB_SHED%rowtype;
  a_stg_input stg_array;
 
 
--**************************************************************************************************
-- Delete records from Foundation
-- based on employee_id and SHIFT_CLOCK_IN
-- before loading from staging
--**************************************************************************************************
procedure delete_fnd as
begin 
 
      g_recs_inserted := 0;

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list;
       
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);
begin
     insert /*+ append */ into dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list
           WITH selstg AS
            (SELECT DISTINCT employee_id,
              fin_year_no, fin_week_no
            FROM STG_S4S_EMP_LOC_JOB_SHED_cpy cp, dim_calendar dc
            where TRUNC(cp.SHIFT_CLOCK_IN) = dc.calendar_date
            and employee_id = 7072787	
                and TRUNC(SHIFT_CLOCK_IN) >= '2 march 2015'
                            )
                SELECT g_run_date,
                        g_date,
                        g_run_seq_no,
                        f.*
                FROM DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY f,
                      selstg s,
                      dim_calendar dc
                WHERE f.employee_id         = s.employee_id
                AND TRUNC(f.SHIFT_CLOCK_IN) = dc.calendar_date
                and dc.fin_year_no = s.fin_year_no
                and dc.fin_week_no = s.fin_week_no
                 ;
          
          
          
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
          l_text := 'Insert into FND_S4S_SCHLOCEMPJBDY_del_list recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

         delete from DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY b
         where exists   (select distinct employee_id, trunc(SHIFT_CLOCK_IN) from dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list a
         where run_seq_no = g_run_seq_no
         and a.employee_id = b.employee_id
         and trunc(A.SHIFT_CLOCK_IN) = trunc(B.shift_clock_in));
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
                
      l_text := 'Deleted from DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    exception
         when no_data_found then
                l_text := 'No deletions done for WH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY ';
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
PROCEDURE local_address_variables
AS
BEGIN
  g_hospital                    := 'N';
  g_rec_out.LOCATION_NO         := g_rec_in.STG_LOCATION_NO;
  g_rec_out.EMPLOYEE_ID         := g_rec_in.stg_EMPLOYEE_ID;
  g_rec_out.JOB_ID              := g_rec_in.STG_JOB_ID;
  g_rec_out.SHIFT_CLOCK_IN := g_rec_in.STG_SHIFT_CLOCK_IN;
  g_rec_out.SHIFT_CLOCK_OUT  := g_rec_in.STG_SHIFT_CLOCK_OUT;
  g_rec_out.MEAL_break_minutes          := g_rec_in.STG_MEAL_BREAK_MINUTES;
    g_rec_out.TEA_break_minutes          := g_rec_in.STG_TEA_BREAK_MINUTES;
    
  g_rec_out.last_updated_date   := g_date;
  
  IF G_REC_IN.fl_LOCATION_NO    IS NULL THEN
    g_hospital                  := 'Y';
    g_hospital_text             := 'LOCATION_NO NOT FOUND';
    RETURN;
  END IF;
  IF G_REC_IN.fe_EMPLOYEE_ID IS NULL THEN
    g_hospital               := 'Y';
    g_hospital_text          := 'EMPLOYEE_ID NOT FOUND';
    RETURN;
  END IF;
  IF G_REC_IN.fj_JOB_ID IS NULL THEN
    g_hospital          := 'Y';
    g_hospital_text     := 'JOB_ID NOT FOUND';
    RETURN;
  END IF;
    IF G_REC_IN.STG_SHIFT_CLOCK_IN IS NULL THEN
    g_hospital          := 'Y';
    g_hospital_text     := 'SHIFT_CLOCK_IN IS NULL';
    RETURN;
  END IF;
    IF G_REC_IN.STG_SHIFT_CLOCK_OUT IS NULL THEN
    g_hospital          := 'Y';
    g_hospital_text     := 'SHIFT_CLOCK_OUT IS NULL';
    RETURN;
  END IF;

  
  
EXCEPTION
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_address_variables;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
PROCEDURE local_write_hospital
AS
BEGIN
  g_rec_in.sys_load_date        := sysdate;
  g_rec_in.sys_load_system_name := 'DWH';
  g_rec_in.sys_process_code     := 'Y';
  g_rec_in.sys_process_msg      := g_hospital_text;
  INSERT
  INTO dwh_foundation.STG_S4S_EMP_LOC_JOB_SHED_hsp VALUES
    (
      g_rec_in.SYS_SOURCE_BATCH_ID ,
      g_rec_in.SYS_SOURCE_SEQUENCE_NO ,
      g_rec_in.SYS_LOAD_DATE ,
      g_rec_in.SYS_PROCESS_CODE ,
      g_rec_in.SYS_LOAD_SYSTEM_NAME ,
      g_rec_in.SYS_MIDDLEWARE_BATCH_ID ,
      g_rec_in.SYS_PROCESS_MSG ,
      g_rec_in.SOURCE_DATA_STATUS_CODE ,
      g_rec_in.stg_EMPLOYEE_ID ,
      g_rec_in.stg_LOCATION_NO ,

      g_rec_in.stg_JOB_ID ,
      g_rec_in.stg_SHIFT_CLOCK_IN ,
      g_rec_in.stg_SHIFT_CLOCK_OUT,

      g_rec_in.stg_MEAL_BREAK_MINUTES ,
      g_rec_in.stg_TEA_BREAK_MINUTES 
    );
  g_recs_hospital := g_recs_hospital + sql%rowcount;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lh_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lh_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_write_hospital;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
PROCEDURE local_bulk_insert
AS
BEGIN
  forall i IN a_tbl_insert.first .. a_tbl_insert.last
  SAVE exceptions
  INSERT
  INTO DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY VALUES a_tbl_insert
    (
      i
    );
  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| 
    ' '||g_error_index|| 
    ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| 
    ' '||a_tbl_insert(g_error_index).LOCATION_NO|| 
    ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID|| 
    ' '||a_tbl_insert(g_error_index).JOB_ID|| 
    ' '||a_tbl_insert(g_error_index).SHIFT_CLOCK_IN
;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_insert;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_update
AS
BEGIN
  forall i IN a_tbl_update.first .. a_tbl_update.last
  SAVE exceptions
  
  UPDATE DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY
  SET 
        SHIFT_CLOCK_OUT     = a_tbl_update(i).SHIFT_CLOCK_OUT,
        MEAL_break_minutes            = a_tbl_update(i).MEAL_break_minutes,
        TEA_break_minutes            = a_tbl_update(i).TEA_break_minutes,
        LAST_UPDATED_DATE     = a_tbl_update(i).LAST_UPDATED_DATE
  WHERE LOCATION_NO       = a_tbl_update(i).LOCATION_NO
  AND EMPLOYEE_ID         = a_tbl_update(i).EMPLOYEE_ID
  AND JOB_ID              = a_tbl_update(i).JOB_ID
  AND SHIFT_CLOCK_IN = a_tbl_update(i).SHIFT_CLOCK_IN;
  
  g_recs_updated         := g_recs_updated + a_tbl_update.count;
  
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| 
    ' '||g_error_index|| 
    ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| 
    ' '||a_tbl_update(g_error_index).LOCATION_NO|| 
    ' '||a_tbl_update(g_error_index).EMPLOYEE_ID|| 
    ' '||a_tbl_update(g_error_index).JOB_ID|| 
    ' '||a_tbl_update(g_error_index).SHIFT_CLOCK_IN;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
  v_count INTEGER := 0;
BEGIN
  g_found := false;
  SELECT COUNT(1)
  INTO v_count
  FROM DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY
  WHERE LOCATION_NO = g_rec_out.LOCATION_NO
  AND EMPLOYEE_ID   = g_rec_out.EMPLOYEE_ID
  AND JOB_ID        = g_rec_out.JOB_ID
  AND SHIFT_CLOCK_IN = g_rec_out.SHIFT_CLOCK_IN;
  IF v_count        = 1 THEN
    g_found        := TRUE;
  END IF;
  -- Check if insert of item already in insert array and change to put duplicate in update array
  IF a_count_i > 0 AND NOT g_found THEN
    FOR i IN a_tbl_insert.first .. a_tbl_insert.last
    LOOP
      IF a_tbl_insert(i).LOCATION_NO = g_rec_out.LOCATION_NO 
      AND a_tbl_insert(i).EMPLOYEE_ID = g_rec_out.EMPLOYEE_ID 
      AND a_tbl_insert(i).JOB_ID = g_rec_out.JOB_ID 
      AND a_tbl_insert(i).SHIFT_CLOCK_IN = g_rec_out.SHIFT_CLOCK_IN THEN
        g_found                     := TRUE;
      END IF;
    END LOOP;
  END IF;
  -- Place data into and array for later writing to table in bulk
  IF NOT g_found THEN
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  ELSE
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  END IF;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  --   if a_count > 1000 then
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_staging1   := a_empty_set_s1;
    a_staging2   := a_empty_set_s2;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
    a_count_stg  := 0;
    COMMIT;
  END IF;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_write_output;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD Employee job schedule data ex S4S STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);

-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  
    --**************************************************************************************************
  -- Delete process
  --**************************************************************************************************

    delete_fnd;
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  OPEN c_STG_S4S_EMP_LOC_JOB_SHED;
  FETCH c_STG_S4S_EMP_LOC_JOB_SHED bulk collect
  INTO a_stg_input limit g_forall_limit;
  WHILE a_stg_input.count > 0
  LOOP
    FOR i IN 1 .. a_stg_input.count
    LOOP
      g_recs_read              := g_recs_read + 1;
      IF g_recs_read mod 50000 = 0 THEN
        l_text                 := dwh_constants.vc_log_records_processed|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;
      g_rec_in                := a_stg_input(i);
      a_count_stg             := a_count_stg + 1;
      a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
      a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
      local_address_variables;
      IF g_hospital = 'Y' THEN
        local_write_hospital;
      ELSE
        local_write_output;
      END IF;
    END LOOP;
    FETCH c_STG_S4S_EMP_LOC_JOB_SHED bulk collect
    INTO a_stg_input limit g_forall_limit;
  END LOOP;
  CLOSE c_STG_S4S_EMP_LOC_JOB_SHED;
  --**************************************************************************************************
  -- At end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  
  
      l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCH_LOC_EMP_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCH_LOC_EMP_JB_DY', DEGREE => 8);
  
  
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;


END WH_FND_S4S_050U_BATSEQ;
