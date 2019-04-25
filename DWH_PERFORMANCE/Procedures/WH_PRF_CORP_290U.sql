--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_290U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_290U" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN,
   p_start_date     IN     DATE,
   p_end_date       IN     DATE
)
AS
   --**************************************************************************************************
   --  Date:        Sept 2008
   --  Author:      Alastair de Wet
   --  Purpose:     Generate the loc dept dy debtors comm for 5 days
   --               batch date, 2 days back and 2 days forward.
   --               The procedure can also generate debtors comm records for a specified period,
   --               when supplying the start- and end-date input parameters.
   --  Tables:      Input  - fnd_fin_debtors_comm
   --               Output - rtl_loc_dept_dy
   --  Packages:    constants, dwh_log,
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
   --  c_  -  Prefix to cursor followed by table name
   --**************************************************************************************************
   g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read        INTEGER := 0;
   g_recs_inserted    INTEGER := 0;
   g_recs_updated     INTEGER := 0;
   g_error_count      NUMBER := 0;
   g_error_index      NUMBER := 0;
   g_count            INTEGER := 0;
   g_found            BOOLEAN;
   g_date             DATE;
   g_start_date       DATE;
   g_end_date         DATE;
   g_rec_out          rtl_loc_dept_dy%ROWTYPE;
   l_message          sys_dwh_errlog.log_text%TYPE;
   l_module_name sys_dwh_errlog.log_procedure_name%TYPE
         := 'WH_PRF_CORP_290U' ;
   l_name sys_dwh_log.log_name%TYPE
         := dwh_constants.vc_log_name_rtl_facts ;
   l_system_name sys_dwh_log.log_system_name%TYPE
         := dwh_constants.vc_log_system_name_rtl_prf ;
   l_script_name sys_dwh_log.log_script_name%TYPE
         := dwh_constants.vc_log_script_rtl_prf_facts ;
   l_procedure_name   sys_dwh_log.log_procedure_name%TYPE := l_module_name;
   l_text             sys_dwh_log.log_text%TYPE;
   l_description sys_dwh_log_summary.log_description%TYPE
         := 'EXPLODE DEBTORS COMMISSION TO LOC DEPT DY' ;
   l_process_type sys_dwh_log_summary.log_process_type%TYPE
         := dwh_constants.vc_log_process_type_n ;

   -- For output arrays into bulk load forall statements --
   TYPE tbl_array_i
   IS
      TABLE OF rtl_loc_dept_dy%ROWTYPE
         INDEX BY BINARY_INTEGER;

   TYPE tbl_array_u
   IS
      TABLE OF rtl_loc_dept_dy%ROWTYPE
         INDEX BY BINARY_INTEGER;

   a_tbl_insert       tbl_array_i;
   a_tbl_update       tbl_array_u;
   a_empty_set_i      tbl_array_i;
   a_empty_set_u      tbl_array_u;

   a_count            INTEGER := 0;
   a_count_i          INTEGER := 0;
   a_count_u          INTEGER := 0;

   -- For debtors_business_id = 'FRFMS', join to all departments for business_unit_no = 50
   -- For debtors_business_id = 'FRIMS', join to all departments for business_unit_no = 53
   -- For debtors_business_id = 'FRCMS', join to all departments for business_unit_no = 51, 52 or 54
   -- Do cartesian join to dim_calendar to get 5 dates for each location/department combination.
   CURSOR c_fnd_fin_debtors_comm
   IS
      SELECT   dbtc.sk1_location_no,
               dd.sk1_department_no,
               dbtc.post_date,
               dbtc.sk2_location_no,
               dbtc.debtors_commission_perc
        FROM      (SELECT   dl.sk1_location_no,
                            fdc.post_date,
                            dlh.sk2_location_no,
                            CASE fdc.debtors_business_id
                               WHEN 'FRFMS' THEN 50
                               WHEN 'FRIMS' THEN 53
                               ELSE 51
                            END
                               business_unit_no,
                            CASE fdc.debtors_business_id
                               WHEN 'FRCMS' THEN 52
                            END
                               business_unit_no2,
                            CASE fdc.debtors_business_id
                               WHEN 'FRCMS' THEN 54
                            END
                               business_unit_no3,
                            fdc.debtors_commission_perc
                     FROM   fnd_of_fin_debtors_comm fdc,
                            dim_location dl,
                            dim_location_hist dlh
                    WHERE   fdc.debtors_business_id IN
                                  ('FRCMS', 'FRFMS', 'FRIMS')
                            AND fdc.customer_billing_no = dl.location_no
                            AND fdc.customer_billing_no = dlh.location_no
                            AND dlh.sk2_active_to_date =
                                  dwh_constants.sk_to_date
                            AND fdc.post_date BETWEEN g_start_date
                                                  AND  g_end_date) dbtc
               JOIN
                  dim_department dd
               ON    dd.business_unit_no = dbtc.business_unit_no
                  OR dd.business_unit_no = dbtc.business_unit_no2
                  OR dd.business_unit_no = dbtc.business_unit_no3
               where dd.HIERARCHY_HAS_CHILDREN_IND = 1;

   g_rec_in           c_fnd_fin_debtors_comm%ROWTYPE;

   -- For input bulk collect --
   TYPE stg_array IS TABLE OF c_fnd_fin_debtors_comm%ROWTYPE;

   a_stg_input        stg_array;

   --**************************************************************************************************
   -- Process, transform and validate the data read from the input interface
   --**************************************************************************************************
   PROCEDURE local_address_variables
   AS
   BEGIN
      g_rec_out := NULL;

      g_rec_out.sk1_location_no := g_rec_in.sk1_location_no;
      g_rec_out.sk1_department_no := g_rec_in.sk1_department_no;
      g_rec_out.post_date := g_rec_in.post_date;
      g_rec_out.sk2_location_no := g_rec_in.sk2_location_no;
      g_rec_out.debtors_commission_perc := g_rec_in.debtors_commission_perc;
      g_rec_out.last_updated_date := g_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_message :=
               dwh_constants.vc_err_av_other
            || SQLCODE
            || ' '
            || SQLERRM
            || ' '
            || g_rec_out.sk1_location_no
            || ' '
            || g_rec_out.sk1_department_no
            || ' '
            || g_rec_out.post_date;
         dwh_log.record_error (l_module_name, SQLCODE, l_message);
         RAISE;
   END local_address_variables;

   --**************************************************************************************************
   -- Bulk 'write from array' loop controlling bulk inserts to output table
   --**************************************************************************************************
   PROCEDURE local_bulk_insert
   AS
   BEGIN
      FORALL i IN a_tbl_insert.FIRST .. a_tbl_insert.LAST
      SAVE EXCEPTIONS
         INSERT INTO rtl_loc_dept_dy
           VALUES   a_tbl_insert (i);

      g_recs_inserted := g_recs_inserted + a_tbl_insert.COUNT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_error_count := sql%BULK_EXCEPTIONS.COUNT;
         l_message :=
               dwh_constants.vc_err_lb_insert
            || g_error_count
            || ' '
            || SQLCODE
            || ' '
            || SQLERRM;
         dwh_log.record_error (l_module_name, SQLCODE, l_message);

         FOR i IN 1 .. g_error_count
         LOOP
            g_error_index := sql%BULK_EXCEPTIONS(i).ERROR_INDEX;
            l_message :=
                  dwh_constants.vc_err_lb_loop
               || i
               || ' '
               || g_error_index
               || ' '
               || SQLERRM (-sql%BULK_EXCEPTIONS(i).ERROR_CODE)
               || ' '
               || a_tbl_insert (g_error_index).sk1_location_no
               || ' '
               || a_tbl_insert (g_error_index).sk1_department_no
               || ' '
               || a_tbl_insert (g_error_index).post_date;
            dwh_log.record_error (l_module_name, SQLCODE, l_message);
         END LOOP;

         RAISE;
   END local_bulk_insert;

   --**************************************************************************************************
   -- Bulk 'write from array' loop controlling bulk updates to output table
   --**************************************************************************************************
   PROCEDURE local_bulk_update
   AS
   BEGIN
      FORALL i IN a_tbl_update.FIRST .. a_tbl_update.LAST
      SAVE EXCEPTIONS
         UPDATE   rtl_loc_dept_dy
            SET   debtors_commission_perc =
                     a_tbl_update (i).debtors_commission_perc,
                  last_updated_date = a_tbl_update (i).last_updated_date
          WHERE       sk1_location_no = a_tbl_update (i).sk1_location_no
                  AND sk1_department_no = a_tbl_update (i).sk1_department_no
                  AND post_date = a_tbl_update (i).post_date;

      g_recs_updated := g_recs_updated + a_tbl_update.COUNT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_error_count := sql%BULK_EXCEPTIONS.COUNT;
         l_message :=
               dwh_constants.vc_err_lb_update
            || g_error_count
            || ' '
            || SQLCODE
            || ' '
            || SQLERRM;
         dwh_log.record_error (l_module_name, SQLCODE, l_message);

         FOR i IN 1 .. g_error_count
         LOOP
            g_error_index := sql%BULK_EXCEPTIONS(i).ERROR_INDEX;
            l_message :=
                  dwh_constants.vc_err_lb_loop
               || i
               || ' '
               || g_error_index
               || ' '
               || SQLERRM (-sql%BULK_EXCEPTIONS(i).ERROR_CODE)
               || ' '
               || a_tbl_update (g_error_index).sk1_location_no
               || ' '
               || a_tbl_update (g_error_index).sk1_department_no
               || ' '
               || a_tbl_update (g_error_index).post_date;
            dwh_log.record_error (l_module_name, SQLCODE, l_message);
         END LOOP;

         RAISE;
   END local_bulk_update;

   --**************************************************************************************************
   -- Write valid data out to output table
   --**************************************************************************************************
   PROCEDURE local_write_output
   AS
   BEGIN
      g_found := FALSE;

      -- Check to see if present on table and update/insert accordingly
      SELECT   COUNT (1)
        INTO   g_count
        FROM   rtl_loc_dept_dy
       WHERE       sk1_location_no = g_rec_out.sk1_location_no
               AND sk1_department_no = g_rec_out.sk1_department_no
               AND post_date = g_rec_out.post_date;

      IF g_count = 1
      THEN
         g_found := TRUE;
      END IF;

      -- Place data into array for later writing to table in bulk
      IF NOT g_found
      THEN
         a_count_i := a_count_i + 1;
         a_tbl_insert (a_count_i) := g_rec_out;
      ELSE
         a_count_u := a_count_u + 1;
         a_tbl_update (a_count_u) := g_rec_out;
      END IF;

      a_count := a_count + 1;

      --**************************************************************************************************
      -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
      --**************************************************************************************************
      IF a_count > g_forall_limit
      THEN
         local_bulk_insert;
         local_bulk_update;
         a_tbl_insert := a_empty_set_i;
         a_tbl_update := a_empty_set_u;
         a_count_i := 0;
         a_count_u := 0;
         a_count := 0;
         COMMIT;
      END IF;
   EXCEPTION
      WHEN dwh_errors.e_insert_error
      THEN
         l_message :=
            dwh_constants.vc_err_lw_insert || SQLCODE || ' ' || SQLERRM;
         dwh_log.record_error (l_module_name, SQLCODE, l_message);
         RAISE;
      WHEN OTHERS
      THEN
         l_message :=
            dwh_constants.vc_err_lw_other || SQLCODE || ' ' || SQLERRM;
         dwh_log.record_error (l_module_name, SQLCODE, l_message);
         RAISE;
   END local_write_output;
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
BEGIN
   IF p_forall_limit IS NOT NULL
      AND p_forall_limit > dwh_constants.vc_forall_minimum
   THEN
      g_forall_limit := p_forall_limit;
   END IF;

   p_success := FALSE;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text :=
      'GENERATE DEBTORS COMM STARTED '
      || TO_CHAR (SYSDATE, ('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   dwh_log.insert_log_summary (l_name,
                               l_system_name,
                               l_script_name,
                               l_procedure_name,
                               l_description,
                               l_process_type,
                               dwh_constants.vc_log_started,
                               '',
                               '',
                               '',
                               '',
                               '');
   --**************************************************************************************************
   -- Look up batch date from dim_control
   --**************************************************************************************************
   dwh_lookup.dim_control (g_date);

   IF p_start_date IS NOT NULL AND p_end_date IS NOT NULL
   THEN
      g_start_date := p_start_date;
      g_end_date := p_end_date;
   ELSE
      g_start_date := g_date - 2;
      g_end_date := g_date + 2;
   END IF;

   l_text := 'BATCH DATE BEING PROCESSED - ' || g_date;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := 'GENERATE RECORDS FOR - ' || g_start_date || ' to ' || g_end_date;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);

   --**************************************************************************************************
   -- Bulk fetch loop controlling main program execution
   --**************************************************************************************************
   OPEN c_fnd_fin_debtors_comm;

   FETCH c_fnd_fin_debtors_comm
      BULK COLLECT INTO   a_stg_input
      LIMIT g_forall_limit;

   WHILE a_stg_input.COUNT > 0
   LOOP
      FOR i IN 1 .. a_stg_input.COUNT
      LOOP
         g_recs_read := g_recs_read + 1;

         IF g_recs_read MOD 100000 = 0
         THEN
            l_text :=
                  dwh_constants.vc_log_records_processed
               || TO_CHAR (SYSDATE, ('dd mon yyyy hh24:mi:ss'))
               || '  '
               || g_recs_read;
            dwh_log.write_log (l_name,
                               l_system_name,
                               l_script_name,
                               l_procedure_name,
                               l_text);
         END IF;

         g_rec_in := NULL;
         g_rec_in := a_stg_input (i);

         local_address_variables;
         local_write_output;
      END LOOP;

      FETCH c_fnd_fin_debtors_comm
         BULK COLLECT INTO   a_stg_input
         LIMIT g_forall_limit;
   END LOOP;

   CLOSE c_fnd_fin_debtors_comm;

   --**************************************************************************************************
   -- At end write out what remains in the arrays at end of program
   --**************************************************************************************************
   local_bulk_insert;
   local_bulk_update;
   COMMIT;

   --**************************************************************************************************
   -- Write final log data
   --**************************************************************************************************
   dwh_log.update_log_summary (l_name,
                               l_system_name,
                               l_script_name,
                               l_procedure_name,
                               l_description,
                               l_process_type,
                               dwh_constants.vc_log_ended,
                               g_recs_read,
                               g_recs_inserted,
                               g_recs_updated,
                               '',
                               '');
   l_text :=
      dwh_constants.vc_log_time_completed
      || TO_CHAR (SYSDATE, ('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := dwh_constants.vc_log_records_read || g_recs_read;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := dwh_constants.vc_log_records_inserted || g_recs_inserted;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := dwh_constants.vc_log_records_updated || g_recs_updated;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := dwh_constants.vc_log_run_completed || SYSDATE;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   l_text := ' ';
   dwh_log.write_log (l_name,
                      l_system_name,
                      l_script_name,
                      l_procedure_name,
                      l_text);
   COMMIT;
   p_success := TRUE;
EXCEPTION
   WHEN dwh_errors.e_insert_error
   THEN
      l_message :=
            dwh_constants.vc_err_mm_insert
         || SQLCODE
         || ' '
         || SQLERRM
         || ' '
         || g_rec_out.sk1_location_no
         || ' '
         || g_rec_out.sk1_department_no
         || ' '
         || g_rec_out.post_date;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      ROLLBACK;
      p_success := FALSE;
      RAISE;
   WHEN OTHERS
   THEN
      l_message :=
            dwh_constants.vc_err_mm_other
         || SQLCODE
         || ' '
         || SQLERRM
         || ' '
         || g_rec_out.sk1_location_no
         || ' '
         || g_rec_out.sk1_department_no
         || ' '
         || g_rec_out.post_date;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      ROLLBACK;
      p_success := FALSE;
      RAISE;
END wh_prf_corp_290u;
