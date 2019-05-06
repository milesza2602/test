--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_126U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_126U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        July 2008
  --  Author:      Alastair de Wet
  --  Purpose:     Create WW online fact table in the performance layer
  --               with input ex WW online sale table from foundation layer.
  --  Tables:      Input  - fnd_rtl_loc_item_dy_atg_sale
  --               Output - rtl_loc_item_dy_wwo_sale
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  --  11 May 2011 - Quentin Smit / Skylight Project
  --                Source of online sales changed to fnd_rtl_loc_item_dy_atg_sale
  --                Removed online_total_sales_cost from rtl_loc_item_dy_wwo_sale
  --                as it will be calculated in the cube.
  --  12 AUG 2011 QC4425 - change to recalc for all data based upon key
  --                         where last_updated_date = g_date
  --                     - change name from fnd_rtl_loc_item_dy_atg_sale
  --                                to fnd_loc_item_dy_atg_sale_dtl
  --
  --  20 Apr 2016 - BarryK
  --                Re-instate online_total_sales_cost as online_sales_cost:
  --                Add additional table in a join (rtl_location_item) to get 'WAC' column for calculation of 'online sales cost'
  --                to re-insert into target table (rtl_loc_item_dy_wwo_sale)
  --                Ref: BK20/04/2016
  --  Naming conventions
  --  g_  -  Global variable'wac' value
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_rec_out rtl_loc_item_dy_wwo_sale%rowtype;
  g_found BOOLEAN;
  g_date DATE      := TRUNC(sysdate);
  g_yesterday DATE := TRUNC(sysdate) - 1;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_126U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE WW ONLINE SALES FACTS EX FOUNDATION';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF rtl_loc_item_dy_wwo_sale%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF rtl_loc_item_dy_wwo_sale%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  a_count_u INTEGER := 0;

  CURSOR c_fnd_rtl_loc_item_dy_wwo_sale
  IS
  WITH wwo AS
    (SELECT selkey.location_no,
      selkey.item_no,
      selkey.post_date,
      SUM(itb.online_total_sales_qty) online_total_sales_qty,
      SUM(itb.online_total_sales) online_total_sales
    FROM fnd_loc_item_dy_atg_sale_dtl             itb ,
      (SELECT sald.location_no                                AS location_no,
        dwh_performance.dwh_lookup.item_convert(sald.item_no) AS item_no,
        sald.post_date as  post_date
      FROM fnd_loc_item_dy_atg_sale_dtl sald
      WHERE sald.last_updated_date = g_date
      GROUP BY sald.location_no,
        dwh_performance.dwh_lookup.item_convert(sald.item_no) ,
        sald.post_date
      ) selkey
  WHERE selkey.item_no   = itb.item_no
  AND selkey.location_no = itb.location_no
  AND selkey.post_date   = itb.post_date
  GROUP BY selkey.location_no,
      selkey.item_no,
      selkey.post_date
    )
  SELECT wwo.location_no,
    wwo.item_no,
    wwo.post_date,
    wwo.online_total_sales_qty,
    wwo.online_total_sales,
    nvl(wwo.online_total_sales_qty * nvl(rli.wac,0),0) as online_sales_cost,           -- Ref: BK20/04/2016
    di.department_no,
    di.tran_ind,
    di.sk1_item_no,
    dd.jv_dept_ind,
    dd.packaging_dept_ind,
    dl.sk1_location_no,
    dih.sk2_item_no,
    dlh.sk2_location_no
  FROM  wwo                                                                                                         -- Ref: BK20/04/2016
  join  dim_item di            on (wwo.item_no      = di.item_no)                                                   -- Ref: BK20/04/2016
  join  dim_department dd      on (di.department_no = dd.department_no)                                             -- Ref: BK20/04/2016
  join  dim_location dl        on (wwo.location_no  = dl.location_no)                                               -- Ref: BK20/04/2016
  join  dim_item_hist dih      on (wwo.item_no      = dih.item_no)                                                  -- Ref: BK20/04/2016
  join  dim_location_hist dlh  on (wwo.location_no  = dlh.location_no)                                              -- Ref: BK20/04/2016
  left join                                                                                                         -- Ref: BK20/04/2016
        rtl_location_item rli  on (dl.SK1_LOCATION_NO = rli.SK1_LOCATION_NO AND di.sk1_item_no = rli.sk1_item_no)   -- Ref: BK20/04/2016
  WHERE
        wwo.post_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date
  AND   wwo.location_no = dlh.location_no
  AND   wwo.post_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date ;
  --   where  last_updated_date >= g_yesterday;
  -- order by only where sequencing is essential to the correct loading of data
  g_rec_in c_fnd_rtl_loc_item_dy_wwo_sale%rowtype;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF c_fnd_rtl_loc_item_dy_wwo_sale%rowtype;
  a_stg_input stg_array;
  --**************************************************************************************************
  -- Process, transform and validate the data read from the input interface
  --**************************************************************************************************
PROCEDURE local_address_variables
AS
BEGIN
  g_rec_out.post_date              := g_rec_in.post_date;
  g_rec_out.online_total_sales_qty := g_rec_in.online_total_sales_qty ;
  g_rec_out.online_total_sales     := g_rec_in.online_total_sales ;
--  g_rec_out.online_sales_cost   := g_rec_in.online_total_sales_cost ;
  g_rec_out.last_updated_date := g_date;
  g_rec_out.sk1_item_no       := g_rec_in.sk1_item_no;
  g_rec_out.sk1_location_no   := g_rec_in.sk1_location_no;
  g_rec_out.sk2_item_no       := g_rec_in.sk2_item_no;
  g_rec_out.sk2_location_no   := g_rec_in.sk2_location_no;
  -- Convert Items not at RMS transaction level to RMS transaction level
  --   if g_rec_in.tran_ind = 0 then
  --      dwh_lookup.dim_item_convert(g_rec_in.item_no,g_rec_in.item_no);
  -- Look up the Surrogate keys from dimensions for output onto the Fact record
  --      dwh_lookup.dim_item_sk1(g_rec_in.item_no,g_rec_out.sk1_item_no);
  --      dwh_lookup.dim_item_sk2(g_rec_in.item_no,g_rec_out.sk2_item_no);
  --   end if;
  -- Value add and calculated fields added to performance layer
  g_rec_out.online_sales_qty           := '';
  g_rec_out.online_sales               := '';
  g_rec_out.online_sales_cost          := '';
  g_rec_out.online_sales_margin        := '';
  g_rec_out.spec_dept_online_rvnu_qty  := '';
  g_rec_out.spec_dept_online_rvnu      := '';
  g_rec_out.spec_dept_online_rvnu_cost := '';
  IF g_rec_in.jv_dept_ind              <> 1 AND g_rec_in.packaging_dept_ind <> 1 THEN
    g_rec_out.online_sales_qty         := g_rec_in.online_total_sales_qty ;
    g_rec_out.online_sales             := g_rec_in.online_total_sales ;
    g_rec_out.online_sales_cost   := g_rec_in.online_sales_cost ;                               -- Ref: BK20/04/2016
    g_rec_out.online_sales_margin := g_rec_out.online_sales - g_rec_out.online_sales_cost;
  END IF;
  IF g_rec_in.jv_dept_ind                = 1 OR g_rec_in.packaging_dept_ind = 1 THEN
    g_rec_out.spec_dept_online_rvnu_qty := g_rec_in.online_total_sales_qty ;
    g_rec_out.spec_dept_online_rvnu     := g_rec_in.online_total_sales ;
    g_rec_out.spec_dept_online_rvnu_cost   := g_rec_in.online_sales_cost ;                       -- Ref: BK20/04/2016
  END IF;
EXCEPTION
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END local_address_variables;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
PROCEDURE local_bulk_insert
AS
BEGIN
  forall i IN a_tbl_insert.first .. a_tbl_insert.last
  SAVE exceptions
  INSERT INTO rtl_loc_item_dy_wwo_sale VALUES a_tbl_insert
    (i
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
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_insert(g_error_index).sk1_location_no|| ' '||a_tbl_insert(g_error_index).sk1_item_no|| ' '||a_tbl_insert(g_error_index).post_date;
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
  UPDATE rtl_loc_item_dy_wwo_sale
  SET online_total_sales_qty = a_tbl_update(i).online_total_sales_qty,
    online_total_sales       = a_tbl_update(i).online_total_sales ,
--    online_total_sales_cost         = a_tbl_update(i).online_total_sales_cost,
    online_sales_qty           = a_tbl_update(i).online_sales_qty,
    online_sales               = a_tbl_update(i).online_sales ,
    online_sales_cost          = a_tbl_update(i).online_sales_cost,
    online_sales_margin        = a_tbl_update(i).online_sales_margin,
    spec_dept_online_rvnu_qty  = a_tbl_update(i).spec_dept_online_rvnu_qty,
    spec_dept_online_rvnu      = a_tbl_update(i).spec_dept_online_rvnu,
    spec_dept_online_rvnu_cost = a_tbl_update(i).spec_dept_online_rvnu_cost,
    last_updated_date          = a_tbl_update(i).last_updated_date
  WHERE sk1_location_no        = a_tbl_update(i).sk1_location_no
  AND sk1_item_no              = a_tbl_update(i).sk1_item_no
  AND post_date                = a_tbl_update(i).post_date;
  g_recs_updated              := g_recs_updated + a_tbl_update.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).sk1_location_no|| ' '||a_tbl_update(g_error_index).sk1_item_no|| ' '||a_tbl_update(g_error_index).post_date;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
END local_bulk_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  g_found := FALSE;
  -- Check to see if item is present on table and update/insert accordingly
  SELECT COUNT(1)
  INTO g_count
  FROM rtl_loc_item_dy_wwo_sale
  WHERE sk1_location_no = g_rec_out.sk1_location_no
  AND sk1_item_no       = g_rec_out.sk1_item_no
  AND post_date         = g_rec_out.post_date;
  IF g_count            = 1 THEN
    g_found            := TRUE;
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
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
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
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF RTL_LOC_ITEM_DY_WWO_SALE EX FOUNDATION STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);

--g_date := '15/FEB/2016';

  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  OPEN c_fnd_rtl_loc_item_dy_wwo_sale;
  FETCH c_fnd_rtl_loc_item_dy_wwo_sale bulk collect
  INTO a_stg_input limit g_forall_limit;
  WHILE a_stg_input.count > 0
  LOOP
    FOR i IN 1 .. a_stg_input.count
    LOOP
      g_recs_read              := g_recs_read + 1;
      IF g_recs_read mod 100000 = 0 THEN
        l_text                 := dwh_constants.vc_log_records_processed|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;
      g_rec_in := a_stg_input(i);
      local_address_variables;
      local_write_output;
    END LOOP;
    FETCH c_fnd_rtl_loc_item_dy_wwo_sale bulk collect
    INTO a_stg_input limit g_forall_limit;
  END LOOP;
  CLOSE c_fnd_rtl_loc_item_dy_wwo_sale;
  --**************************************************************************************************
  -- At end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
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
END wh_prf_corp_126u;
