--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_302U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_302U" (
    p_forall_limit IN integer,
    p_success OUT boolean)
AS
  --**************************************************************************************************
  --  date:        November 2012
  --  author:      wendy lyttle
  --  purpose:
  --  tables:      input  - stg_sims_waste_adherence_cpy
  --               output - fnd_sims_loc_dy
  --  packages:    constants, dwh_log, dwh_valid
  --

  --
  --  naming conventions
  --  g_  -  global variable
  --  l_  -  log table variable
  --  a_  -  array variable
  --  v_  -  local variable as found in packages
  --  p_  -  parameter
  --  c_  -  prefix to cursor
  --**************************************************************************************************
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_recs_hospital integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_hospital      char(1) := 'N';
  g_hospital_text stg_sims_waste_adherence_hsp.sys_process_msg%TYPE;
  g_rec_out fnd_sims_loc_dy%ROWTYPE;
  g_rec_in stg_sims_waste_adherence_cpy%ROWTYPE;
  g_found      boolean;
  g_insert_rec boolean;
  g_date date := TRUNC(SYSDATE);
  l_message sys_dwh_errlog.log_text%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_FND_CORP_302U';
  l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
  l_text sys_dwh_log.log_text%TYPE ;
  l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD THE WAST adherence EX SIMS';
  l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
  -- for input bulk collect --
TYPE stg_array
IS
  TABLE OF stg_sims_waste_adherence_cpy%ROWTYPE;
  a_stg_input stg_array;
  -- for output arrays into bulk load forall statements --
TYPE tbl_array_i
IS
  TABLE OF fnd_sims_loc_dy%ROWTYPE INDEX BY binary_integer;
TYPE tbl_array_u
IS
  TABLE OF fnd_sims_loc_dy%ROWTYPE INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
  a_count_i integer := 0;
  a_count_u integer := 0;
  -- for arrays used to update the staging table process_code --
TYPE staging_array1
IS
  TABLE OF stg_sims_waste_adherence_cpy.sys_source_batch_id%TYPE INDEX BY binary_integer;
TYPE staging_array2
IS
  TABLE OF stg_sims_waste_adherence_cpy.sys_source_sequence_no%TYPE INDEX BY binary_integer;
  a_staging1 staging_array1;
  a_staging2 staging_array2;
  a_empty_set_s1 staging_array1;
  a_empty_set_s2 staging_array2;
  a_count_stg integer := 0;
  CURSOR stg_sims_waste_adherence
  IS
    SELECT *
    FROM stg_sims_waste_adherence_cpy
    WHERE sys_process_code = 'N'
    ORDER BY sys_source_batch_id,
      sys_source_sequence_no;
  -- order by only where sequencing is essential to the correct loading of data
  --**************************************************************************************************
  -- process, transform and validate the data read from the input interface
  --**************************************************************************************************
PROCEDURE local_address_variables
AS
BEGIN
  g_hospital                        := 'N';
  g_rec_out.location_no             := g_rec_in.location_no;
  g_rec_out.post_date               := g_rec_in.post_date;
  g_rec_out.waste_adherence_qty    := g_rec_in.waste_adherence_qty;
  g_rec_out.waste_adherence_mass      := g_rec_in.waste_adherence_mass;
  g_rec_out.waste_not_in_adherence_qty  := g_rec_in.waste_not_in_adherence_qty;
  g_rec_out.waste_not_in_adherence_mass       := g_rec_in.waste_not_in_adherence_mass;
  g_rec_out.total_waste_qty := g_rec_in.total_waste_qty;
  g_rec_out.total_waste_mass        := g_rec_in.total_waste_mass;
  g_rec_out.percentage_diff_qty := g_rec_in.percentage_diff_qty;
  g_rec_out.percentage_diff_mass        := g_rec_in.percentage_diff_mass;

  IF NOT dwh_valid.fnd_location(g_rec_out.location_no) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_location_not_found;
    l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  END IF;
  -- rest of the pk validation tbc
EXCEPTION
WHEN others THEN
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  RAISE;
END local_address_variables;
--**************************************************************************************************
-- write invalid data out to the hostpital table
--**************************************************************************************************
PROCEDURE local_write_hospital
AS
BEGIN
  g_rec_in.sys_load_date        := SYSDATE;
  g_rec_in.sys_load_system_name := 'DWH';
  g_rec_in.sys_process_code     := 'Y';
  g_rec_in.sys_process_msg      := g_hospital_text;
  INSERT INTO stg_sims_waste_adherence_hsp VALUES g_rec_in;
  g_recs_hospital := g_recs_hospital + SQL%ROWCOUNT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lh_insert||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_lh_other||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  RAISE;
END local_write_hospital;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
PROCEDURE local_bulk_insert
AS
BEGIN
  FORALL i IN a_tbl_insert.FIRST .. a_tbl_insert.LAST
  SAVE EXCEPTIONS
  INSERT INTO fnd_sims_loc_dy VALUES a_tbl_insert
    (i
    );
  g_recs_inserted := g_recs_inserted + a_tbl_insert.COUNT;
EXCEPTION
WHEN others THEN
  g_error_count := SQL%bulk_exceptions.COUNT;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := SQL%bulk_exceptions
    (
      i
    )
    .error_index;
    L_Message := Dwh_Constants.Vc_Err_Lb_Loop||I|| ' '||G_Error_Index
    || ' '||Sqlerrm(-Sql%Bulk_Exceptions(I).Error_Code)
    || ' '||A_Tbl_Insert(G_Error_Index).Location_No
    || ' '||A_Tbl_Insert(G_Error_Index).post_date;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  RAISE;
END local_bulk_insert;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_update
AS
BEGIN
  FORALL i IN a_tbl_update.FIRST .. a_tbl_update.LAST
  SAVE EXCEPTIONS
  UPDATE fnd_sims_loc_dy
  SET waste_adherence_qty = a_tbl_update(i).waste_adherence_qty,
    waste_adherence_mass     = a_tbl_update(i).waste_adherence_mass,
    waste_not_in_adherence_qty = a_tbl_update(i).waste_not_in_adherence_qty,
    waste_not_in_adherence_mass      = a_tbl_update(i).waste_not_in_adherence_mass,
    total_waste_qty= a_tbl_update(i).total_waste_qty,
    total_waste_mass       = a_tbl_update(i).total_waste_mass,
     percentage_diff_qty= a_tbl_update(i).percentage_diff_qty,
     percentage_diff_mass       = a_tbl_update(i).percentage_diff_mass,
    last_updated_date      = a_tbl_update(i).last_updated_date
  WHERE location_no        = a_tbl_update(i).location_no
  AND post_date            = a_tbl_update(i).post_date ;
  g_recs_updated          := g_recs_updated + a_tbl_update.COUNT;
EXCEPTION
WHEN others THEN
  g_error_count := SQL%bulk_exceptions.COUNT;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := SQL%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||SQLERRM(-SQL%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).location_no|| ' '||a_tbl_update(g_error_index).post_date;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  RAISE;
END local_bulk_update;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_staging_update
AS
BEGIN
  FORALL i IN a_staging1.FIRST .. a_staging1.LAST
  SAVE EXCEPTIONS
  UPDATE stg_sims_waste_adherence_cpy
  SET sys_process_code       = 'Y'
  WHERE sys_source_batch_id  = a_staging1(i)
  AND sys_source_sequence_no = a_staging2(i);
EXCEPTION
WHEN others THEN
  g_error_count := SQL%bulk_exceptions.COUNT;
  l_message     := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := SQL%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||SQLERRM(-SQL%bulk_exceptions(i).error_code)|| ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  RAISE;
END local_bulk_staging_update;
--**************************************************************************************************
-- write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  g_found := FALSE;
  -- check to see if item is present on table and update/insert accordingly
  SELECT COUNT(1)
  INTO g_count
  FROM fnd_sims_loc_dy
  WHERE location_no = g_rec_out.location_no
  AND post_date     = g_rec_out.post_date;
  IF g_count        = 1 THEN
    g_found        := TRUE;
  END IF;
  -- check if insert of item already in insert array and change to put duplicate in update array
  IF a_count_i > 0 AND NOT g_found THEN
    FOR i     IN a_tbl_insert.FIRST .. a_tbl_insert.LAST
    LOOP
      If (A_Tbl_Insert(I).Location_No = G_Rec_Out.Location_No
      And A_Tbl_Insert(I).post_date = G_Rec_Out.post_date) then
           g_found := TRUE;
      END IF;
    END LOOP;
  END IF;
  -- place data into and array for later writing to table in bulk
  IF NOT g_found THEN
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  ELSE
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  END IF;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;
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
  l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  RAISE;
END local_write_output;
--**************************************************************************************************
-- main process
--**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.PUT_LINE('BULK WRITE LIMIT '||p_forall_limit||' '||g_forall_limit);
  p_success := FALSE;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF fnd_sims_loc_dy EX POS STARTED AT '|| TO_CHAR(SYSDATE,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- bulk fetch loop controlling main program execution
  --**************************************************************************************************
  OPEN stg_sims_waste_adherence;
  FETCH stg_sims_waste_adherence BULK COLLECT INTO a_stg_input LIMIT g_forall_limit;
  WHILE a_stg_input.COUNT > 0
  LOOP
    FOR i IN 1 .. a_stg_input.COUNT
    LOOP
      g_recs_read              := g_recs_read + 1;
      IF g_recs_read MOD 100000 = 0 THEN
        l_text                 := dwh_constants.vc_log_records_processed|| TO_CHAR(SYSDATE,('DD MON YYYY HH24:MI:SS'))||'  '||g_recs_read ;
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
    FETCH stg_sims_waste_adherence BULK COLLECT INTO a_stg_input LIMIT g_forall_limit;
  END LOOP;
  CLOSE stg_sims_waste_adherence;
  --**************************************************************************************************
  -- at end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  local_bulk_staging_update;
  --**************************************************************************************************
  -- write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(SYSDATE,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||SYSDATE;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := TRUE;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := FALSE;
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||SQLERRM;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := FALSE;
  Raise;

END WH_FND_CORP_302U;
