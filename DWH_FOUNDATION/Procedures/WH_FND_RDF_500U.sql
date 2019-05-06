--------------------------------------------------------
--  DDL for Procedure WH_FND_RDF_500U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_RDF_500U" (
        p_forall_limit IN INTEGER,
        p_success OUT BOOLEAN)
    AS
      --**************************************************************************************************
      --  Date:        February 2015
      --  Author:      Quentin Smit
      --  Purpose:     Create RDF Daily Foods Forecast LEVEL 1(LOCATION LEVEL) table in the foundation layer
      --               with input ex staging table from RDF.
      --               21 - days extract
      --  Tables:      Input  - STG_RDF_DYFCST_L1_CPY
      --               Output - FND_LOC_ITEM_RDF_DYFCST_L1
      --  Packages:    dwh_constants, dwh_log, dwh_valid
      --
      --------------------------------NEW VERSION--------------------------------------------------------------------
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

      g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
      g_recs_read     INTEGER := 0;
      g_recs_updated  INTEGER := 0;
      g_recs_inserted INTEGER := 0;
      g_recs_hospital INTEGER := 0;
      g_error_count   NUMBER  := 0;
      g_error_index   NUMBER  := 0;
      g_count         NUMBER  := 0;
      g_hospital      CHAR(1) := 'N';
      g_hospital_text STG_RDF_DYFCST_L1_HSP.sys_process_msg%type;
      g_rec_out FND_LOC_ITEM_RDF_DYFCST_L1%rowtype;

      g_found BOOLEAN;
      g_valid BOOLEAN;

      g_date DATE := TRUNC(sysdate);

      l_message sys_dwh_errlog.log_text%type;
      l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_RDF_500U';
      l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rdf;
      l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_pln_fnd;
      l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_rdf;
      l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
      l_text sys_dwh_log.log_text%type ;
      l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE DAILY FOODS FORECAST LEVEL1 EX RDF';
      l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

      -- For output arrays into bulk load forall statements --
    type tbl_array_i
    IS
      TABLE OF FND_LOC_ITEM_RDF_DYFCST_L1%rowtype INDEX BY binary_integer;

    type tbl_array_u
    IS
      TABLE OF FND_LOC_ITEM_RDF_DYFCST_L1%rowtype INDEX BY binary_integer;

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
      TABLE OF STG_RDF_DYFCST_L1_CPY.sys_source_batch_id%type INDEX BY binary_integer;

    type staging_array2
    IS
      TABLE OF STG_RDF_DYFCST_L1_CPY.sys_source_sequence_no%type INDEX BY binary_integer;
      a_staging1 staging_array1;
      a_staging2 staging_array2;
      a_empty_set_s1 staging_array1;
      a_empty_set_s2 staging_array2;
      a_count_stg INTEGER := 0;

      CURSOR c_stg_rdf_rtl_dyfcstl1
      is
        select
          /*+ full(st) */
          st.*,
          fi.item_no fi_item_no,
          fl.location_no fl_location_no,
          dc.calendar_date dc_calendar_date
        from STG_RDF_DYFCST_L1_CPY st
        left outer join fnd_item fi     on fi.item_no = st.item_no
        left outer join fnd_location fl on fl.location_no = st.location_no
        left outer join dim_calendar dc on dc.calendar_date = st.post_date
        where st.sys_process_code = 'N'
        order by sys_source_batch_id,
          sys_source_sequence_no;


      g_rec_in c_stg_rdf_rtl_dyfcstl1%rowtype;
    type stg_array
    IS
      TABLE OF c_stg_rdf_rtl_dyfcstl1%rowtype;
      a_stg_input stg_array;

      --**************************************************************************************************
      -- Process, transform and validate the data read from the input interface
      --**************************************************************************************************
    PROCEDURE local_address_variables
    AS
    BEGIN
      g_hospital                        := 'N';
      g_rec_out.location_no             := g_rec_in.location_no;
      g_rec_out.item_no                 := g_rec_in.item_no;
      g_rec_out.post_date               := g_rec_in.post_date;
      g_rec_out.dy_01_sys_fcst_qty      := g_rec_in.dy_01_sys_fcst_qty;
      g_rec_out.dy_02_sys_fcst_qty      := g_rec_in.dy_02_sys_fcst_qty;
      g_rec_out.dy_03_sys_fcst_qty      := g_rec_in.dy_03_sys_fcst_qty;
      g_rec_out.dy_04_sys_fcst_qty      := g_rec_in.dy_04_sys_fcst_qty;
      g_rec_out.dy_05_sys_fcst_qty      := g_rec_in.dy_05_sys_fcst_qty;
      g_rec_out.dy_06_sys_fcst_qty      := g_rec_in.dy_06_sys_fcst_qty;
      g_rec_out.dy_07_sys_fcst_qty      := g_rec_in.dy_07_sys_fcst_qty;
      g_rec_out.dy_08_sys_fcst_qty      := g_rec_in.dy_08_sys_fcst_qty;
      g_rec_out.dy_09_sys_fcst_qty      := g_rec_in.dy_09_sys_fcst_qty;
      g_rec_out.dy_10_sys_fcst_qty      := g_rec_in.dy_10_sys_fcst_qty;
      g_rec_out.dy_11_sys_fcst_qty      := g_rec_in.dy_11_sys_fcst_qty;
      g_rec_out.dy_12_sys_fcst_qty      := g_rec_in.dy_12_sys_fcst_qty;
      g_rec_out.dy_13_sys_fcst_qty      := g_rec_in.dy_13_sys_fcst_qty;
      g_rec_out.dy_14_sys_fcst_qty      := g_rec_in.dy_14_sys_fcst_qty;
      g_rec_out.dy_15_sys_fcst_qty      := g_rec_in.dy_15_sys_fcst_qty;
      g_rec_out.dy_16_sys_fcst_qty      := g_rec_in.dy_16_sys_fcst_qty;
      g_rec_out.dy_17_sys_fcst_qty      := g_rec_in.dy_17_sys_fcst_qty;
      g_rec_out.dy_18_sys_fcst_qty      := g_rec_in.dy_18_sys_fcst_qty;
      g_rec_out.dy_19_sys_fcst_qty      := g_rec_in.dy_19_sys_fcst_qty;
      g_rec_out.dy_20_sys_fcst_qty      := g_rec_in.dy_20_sys_fcst_qty;
      g_rec_out.dy_21_sys_fcst_qty      := g_rec_in.dy_21_sys_fcst_qty;
      g_rec_out.dy_01_app_fcst_qty      := g_rec_in.dy_01_app_fcst_qty;
      g_rec_out.dy_02_app_fcst_qty      := g_rec_in.dy_02_app_fcst_qty;
      g_rec_out.dy_03_app_fcst_qty      := g_rec_in.dy_03_app_fcst_qty;
      g_rec_out.dy_04_app_fcst_qty      := g_rec_in.dy_04_app_fcst_qty;
      g_rec_out.dy_05_app_fcst_qty      := g_rec_in.dy_05_app_fcst_qty;
      g_rec_out.dy_06_app_fcst_qty      := g_rec_in.dy_06_app_fcst_qty;
      g_rec_out.dy_07_app_fcst_qty      := g_rec_in.dy_07_app_fcst_qty;
      g_rec_out.dy_08_app_fcst_qty      := g_rec_in.dy_08_app_fcst_qty;
      g_rec_out.dy_09_app_fcst_qty      := g_rec_in.dy_09_app_fcst_qty;
      g_rec_out.dy_10_app_fcst_qty      := g_rec_in.dy_10_app_fcst_qty;
      g_rec_out.dy_11_app_fcst_qty      := g_rec_in.dy_11_app_fcst_qty;
      g_rec_out.dy_12_app_fcst_qty      := g_rec_in.dy_12_app_fcst_qty;
      g_rec_out.dy_13_app_fcst_qty      := g_rec_in.dy_13_app_fcst_qty;
      g_rec_out.dy_14_app_fcst_qty      := g_rec_in.dy_14_app_fcst_qty;
      g_rec_out.dy_15_app_fcst_qty      := g_rec_in.dy_15_app_fcst_qty;
      g_rec_out.dy_16_app_fcst_qty      := g_rec_in.dy_16_app_fcst_qty;
      g_rec_out.dy_17_app_fcst_qty      := g_rec_in.dy_17_app_fcst_qty;
      g_rec_out.dy_18_app_fcst_qty      := g_rec_in.dy_18_app_fcst_qty;
      g_rec_out.dy_19_app_fcst_qty      := g_rec_in.dy_19_app_fcst_qty;
      g_rec_out.dy_20_app_fcst_qty      := g_rec_in.dy_20_app_fcst_qty;
      g_rec_out.dy_21_app_fcst_qty      := g_rec_in.dy_21_app_fcst_qty;
      g_rec_out.source_data_status_code := g_rec_in.source_data_status_code;
      g_rec_out.last_updated_date       := g_date;
      /* TRYING TO IMPROVE PERFORMANCE
      if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_invalid_source_code;
      l_text := dwh_constants.vc_invalid_source_code||' '||g_rec_out.source_data_status_code;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      if not dwh_valid.fnd_location(g_rec_out.location_no) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_location_not_found;
      l_text := dwh_constants.vc_location_not_found||' '||g_rec_out.location_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      if not  dwh_valid.fnd_item(g_rec_out.item_no) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_item_not_found;
      l_text := dwh_constants.vc_item_not_found||' '||g_rec_out.item_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      if not dwh_valid.fnd_calendar(g_rec_out.post_date) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_date_not_found;
      l_text := dwh_constants.vc_date_not_found||' '||g_rec_out.post_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      */
      IF g_rec_in.FL_LOCATION_NO IS NULL THEN
        g_hospital               := 'Y';
        g_hospital_text          := dwh_constants.vc_location_not_found;
        l_text                   := dwh_constants.vc_location_not_found||' '||g_rec_out.location_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;
      IF g_rec_in.FI_item_no IS NULL THEN
        g_hospital           := 'Y';
        g_hospital_text      := dwh_constants.vc_item_not_found;
        l_text               := dwh_constants.vc_item_not_found||' '||g_rec_out.item_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;
      IF g_rec_in.DC_CALENDAR_DATE IS NULL THEN
        g_hospital                 := 'Y';
        g_hospital_text            := dwh_constants.vc_date_not_found;
        l_text                     := dwh_constants.vc_date_not_found||' '||g_rec_out.post_date;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
      INTO STG_RDF_DYFCST_L1_HSP VALUES
        (
          G_REC_IN.SYS_SOURCE_BATCH_ID ,
          G_REC_IN.SYS_SOURCE_SEQUENCE_NO ,
          G_REC_IN.SYS_LOAD_DATE ,
          G_REC_IN.SYS_PROCESS_CODE ,
          G_REC_IN.SYS_LOAD_SYSTEM_NAME ,
          G_REC_IN.SYS_MIDDLEWARE_BATCH_ID ,
          G_REC_IN.SYS_PROCESS_MSG ,
          G_REC_IN.SOURCE_DATA_STATUS_CODE ,
          G_REC_IN.LOCATION_NO ,
          G_REC_IN.ITEM_NO ,
          G_REC_IN.POST_DATE ,
          G_REC_IN.DY_01_SYS_FCST_QTY ,
          G_REC_IN.DY_02_SYS_FCST_QTY ,
          G_REC_IN.DY_03_SYS_FCST_QTY ,
          G_REC_IN.DY_04_SYS_FCST_QTY ,
          G_REC_IN.DY_05_SYS_FCST_QTY ,
          G_REC_IN.DY_06_SYS_FCST_QTY ,
          G_REC_IN.DY_07_SYS_FCST_QTY ,
          G_REC_IN.DY_08_SYS_FCST_QTY ,
          G_REC_IN.DY_09_SYS_FCST_QTY ,
          G_REC_IN.DY_10_SYS_FCST_QTY ,
          G_REC_IN.DY_11_SYS_FCST_QTY ,
          G_REC_IN.DY_12_SYS_FCST_QTY ,
          G_REC_IN.DY_13_SYS_FCST_QTY ,
          G_REC_IN.DY_14_SYS_FCST_QTY ,
          G_REC_IN.DY_15_SYS_FCST_QTY ,
          G_REC_IN.DY_16_SYS_FCST_QTY ,
          G_REC_IN.DY_17_SYS_FCST_QTY ,
          G_REC_IN.DY_18_SYS_FCST_QTY ,
          G_REC_IN.DY_19_SYS_FCST_QTY ,
          G_REC_IN.DY_20_SYS_FCST_QTY ,
          G_REC_IN.DY_21_SYS_FCST_QTY ,
          G_REC_IN.DY_01_APP_FCST_QTY ,
          G_REC_IN.DY_02_APP_FCST_QTY ,
          G_REC_IN.DY_03_APP_FCST_QTY ,
          G_REC_IN.DY_04_APP_FCST_QTY ,
          G_REC_IN.DY_05_APP_FCST_QTY ,
          G_REC_IN.DY_06_APP_FCST_QTY ,
          G_REC_IN.DY_07_APP_FCST_QTY ,
          G_REC_IN.DY_08_APP_FCST_QTY ,
          G_REC_IN.DY_09_APP_FCST_QTY ,
          G_REC_IN.DY_10_APP_FCST_QTY ,
          G_REC_IN.DY_11_APP_FCST_QTY ,
          G_REC_IN.DY_12_APP_FCST_QTY ,
          G_REC_IN.DY_13_APP_FCST_QTY ,
          G_REC_IN.DY_14_APP_FCST_QTY ,
          G_REC_IN.DY_15_APP_FCST_QTY ,
          G_REC_IN.DY_16_APP_FCST_QTY ,
          G_REC_IN.DY_17_APP_FCST_QTY ,
          G_REC_IN.DY_18_APP_FCST_QTY ,
          G_REC_IN.DY_19_APP_FCST_QTY ,
          G_REC_IN.DY_20_APP_FCST_QTY ,
          G_REC_IN.DY_21_APP_FCST_QTY
        ) ;
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
      INSERT INTO FND_LOC_ITEM_RDF_DYFCST_L1 VALUES a_tbl_insert
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
        l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_insert(g_error_index).location_no|| ' '||a_tbl_insert(g_error_index).item_no|| ' '||a_tbl_insert(g_error_index).post_date;
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
      UPDATE FND_LOC_ITEM_RDF_DYFCST_L1
      SET dy_01_sys_fcst_qty    = a_tbl_update(i).dy_01_sys_fcst_qty,
        dy_02_sys_fcst_qty      = a_tbl_update(i).dy_02_sys_fcst_qty,
        dy_03_sys_fcst_qty      = a_tbl_update(i).dy_03_sys_fcst_qty,
        dy_04_sys_fcst_qty      = a_tbl_update(i).dy_04_sys_fcst_qty,
        dy_05_sys_fcst_qty      = a_tbl_update(i).dy_05_sys_fcst_qty,
        dy_06_sys_fcst_qty      = a_tbl_update(i).dy_06_sys_fcst_qty,
        dy_07_sys_fcst_qty      = a_tbl_update(i).dy_07_sys_fcst_qty,
        dy_08_sys_fcst_qty      = a_tbl_update(i).dy_08_sys_fcst_qty,
        dy_09_sys_fcst_qty      = a_tbl_update(i).dy_09_sys_fcst_qty,
        dy_10_sys_fcst_qty      = a_tbl_update(i).dy_10_sys_fcst_qty,
        dy_11_sys_fcst_qty      = a_tbl_update(i).dy_11_sys_fcst_qty,
        dy_12_sys_fcst_qty      = a_tbl_update(i).dy_12_sys_fcst_qty,
        dy_13_sys_fcst_qty      = a_tbl_update(i).dy_13_sys_fcst_qty,
        dy_14_sys_fcst_qty      = a_tbl_update(i).dy_14_sys_fcst_qty,
        dy_15_sys_fcst_qty      = a_tbl_update(i).dy_15_sys_fcst_qty,
        dy_16_sys_fcst_qty      = a_tbl_update(i).dy_16_sys_fcst_qty,
        dy_17_sys_fcst_qty      = a_tbl_update(i).dy_17_sys_fcst_qty,
        dy_18_sys_fcst_qty      = a_tbl_update(i).dy_18_sys_fcst_qty,
        dy_19_sys_fcst_qty      = a_tbl_update(i).dy_19_sys_fcst_qty,
        dy_20_sys_fcst_qty      = a_tbl_update(i).dy_20_sys_fcst_qty,
        dy_21_sys_fcst_qty      = a_tbl_update(i).dy_21_sys_fcst_qty,
        dy_01_app_fcst_qty      = a_tbl_update(i).dy_01_app_fcst_qty,
        dy_02_app_fcst_qty      = a_tbl_update(i).dy_02_app_fcst_qty,
        dy_03_app_fcst_qty      = a_tbl_update(i).dy_03_app_fcst_qty,
        dy_04_app_fcst_qty      = a_tbl_update(i).dy_04_app_fcst_qty,
        dy_05_app_fcst_qty      = a_tbl_update(i).dy_05_app_fcst_qty,
        dy_06_app_fcst_qty      = a_tbl_update(i).dy_06_app_fcst_qty,
        dy_07_app_fcst_qty      = a_tbl_update(i).dy_07_app_fcst_qty,
        dy_08_app_fcst_qty      = a_tbl_update(i).dy_08_app_fcst_qty,
        dy_09_app_fcst_qty      = a_tbl_update(i).dy_09_app_fcst_qty,
        dy_10_app_fcst_qty      = a_tbl_update(i).dy_10_app_fcst_qty,
        dy_11_app_fcst_qty      = a_tbl_update(i).dy_11_app_fcst_qty,
        dy_12_app_fcst_qty      = a_tbl_update(i).dy_12_app_fcst_qty,
        dy_13_app_fcst_qty      = a_tbl_update(i).dy_13_app_fcst_qty,
        dy_14_app_fcst_qty      = a_tbl_update(i).dy_14_app_fcst_qty,
        dy_15_app_fcst_qty      = a_tbl_update(i).dy_15_app_fcst_qty,
        dy_16_app_fcst_qty      = a_tbl_update(i).dy_16_app_fcst_qty,
        dy_17_app_fcst_qty      = a_tbl_update(i).dy_17_app_fcst_qty,
        dy_18_app_fcst_qty      = a_tbl_update(i).dy_18_app_fcst_qty,
        dy_19_app_fcst_qty      = a_tbl_update(i).dy_19_app_fcst_qty,
        dy_20_app_fcst_qty      = a_tbl_update(i).dy_20_app_fcst_qty,
        dy_21_app_fcst_qty      = a_tbl_update(i).dy_21_app_fcst_qty,
        source_data_status_code = a_tbl_update(i).source_data_status_code,
        last_updated_date       = a_tbl_update(i).last_updated_date
      WHERE location_no         = a_tbl_update(i).location_no
      AND item_no               = a_tbl_update(i).item_no
      AND post_date             = a_tbl_update(i).post_date ;
      g_recs_updated           := g_recs_updated + a_tbl_update.count;

    EXCEPTION
    WHEN OTHERS THEN
      g_error_count := sql%bulk_exceptions.count;
      l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);
      FOR i IN 1 .. g_error_count
      LOOP
        g_error_index := sql%bulk_exceptions(i).error_index;
        l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).location_no || ' '||a_tbl_update(g_error_index).item_no || ' '||a_tbl_update(g_error_index).post_date ;
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
      FROM FND_LOC_ITEM_RDF_DYFCST_L1
      WHERE location_no = g_rec_out.location_no
      AND item_no       = g_rec_out.item_no
      AND post_date     = g_rec_out.post_date ;
      IF g_count        = 1 THEN
        g_found        := TRUE;
      END IF;
      -- Check if insert of item already in insert array and change to put duplicate in update array
      IF a_count_i > 0 AND NOT g_found THEN
        FOR i IN a_tbl_insert.first .. a_tbl_insert.last
        LOOP
          IF a_tbl_insert(i).location_no = g_rec_out.location_no AND a_tbl_insert(i).item_no = g_rec_out.item_no AND a_tbl_insert(i).post_date = g_rec_out.post_date THEN
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
      IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
        g_forall_limit  := p_forall_limit;
      END IF;
      dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
      p_success := false;
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'LOAD OF FND_LOC_ITEM_RDF_DYFCST_L1 EX RDF STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
      --**************************************************************************************************
      -- Look up batch date from dim_control
      --**************************************************************************************************
      dwh_lookup.dim_control(g_date);
      l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      --**************************************************************************************************
      -- Bulk fetch loop controlling main program execution
      --**************************************************************************************************
      OPEN c_stg_rdf_rtl_dyfcstl1;
      FETCH c_stg_rdf_rtl_dyfcstl1 bulk collect
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
        FETCH c_stg_rdf_rtl_dyfcstl1 bulk collect
        INTO a_stg_input limit g_forall_limit;
      END LOOP;
      CLOSE c_stg_rdf_rtl_dyfcstl1;
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


END WH_FND_RDF_500U;
